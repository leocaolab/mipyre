//! Runs the channel between real processes, as the lanes do: the test process is the host, and the peer is a forked
//! child that maps the channel from the fd the host made, so every system call is the real one. The peer is not
//! trusted: it dies at any point, writes anything anywhere in the channel, and forks children of its own.
//!
//! A forked child runs only its body and ends with `_exit`: it never returns into the test harness, and a panic in it
//! ends it with [`CHILD_PANICKED`] before anything is printed under a lock another test thread may have held at the
//! fork.

// Miri runs no system call, and the Loom build (`--cfg shm_model`) has no OS half.
#![cfg(not(any(miri, shm_model)))]
// `allow-unwrap-in-tests` and its kin do not reach the helpers, which are not `#[test]`s.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "the test's helpers fail the test by panicking, and in a forked child by ending it"
)]

use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::sync::Arc;

use bytes::BytesMut;
use mipyre_shm::adapter::doorbell::{
    drain_once, open_host_doorbell, DrainOutcome, FutexDoorbell, HostBell, HostBellOutcome, PipeDoorbell,
};
use mipyre_shm::adapter::mapped::MappedMemory;
use mipyre_shm::adapter::parker::FutexParker;
use mipyre_shm::channel::{
    sleep_all, wake_all, Attempt, Channel, DeadPeerAnswer, HeapMemory, HostEnd, PeerEnd, PeerFault, SleepVerdict,
    StartOutcome, Wait, NOTE_BYTES,
};
use mipyre_shm::plan::geometry::{Geometry, SlotBytes};
use mipyre_shm::plan::ring::{RingDepth, Seq};
use mipyre_shm::plan::spin_window::SpinWindow;

/// The status a forked child ends with when its body panicked.
const CHILD_PANICKED: i32 = 101;
/// A peer sleeps a minute per slice, so a wake that never came shows as the host's wait running out.
const PEER_SLICE_NS: u64 = 60_000_000_000;
/// How long the host waits for its wake pipe before it calls the wake lost; far below [`PEER_SLICE_NS`].
const HOST_WAIT_MS: i32 = 20_000;
/// The peer parks as soon as it finds no request, so every wait goes through the OS.
const NO_SPIN: SpinWindow = SpinWindow::until(0);

type Host = HostEnd<MappedMemory, FutexDoorbell>;
type Peer = PeerEnd<MappedMemory, PipeDoorbell, FutexParker>;

fn geometry(depth: u32) -> Geometry {
    Geometry::new(RingDepth::new(depth).unwrap(), SlotBytes::new(256).unwrap()).unwrap()
}

/// One channel the test made as a lane's host does, and the two fds its peers are handed.
struct Lane {
    geometry: Geometry,
    host: Host,
    bell: HostBell,
    shm: OwnedFd,
    bell_write: OwnedFd,
}

impl Lane {
    fn new(depth: u32) -> Self {
        let geometry = geometry(depth);
        let (memory, shm) = MappedMemory::create(&geometry).unwrap();
        let (bell, bell_write) = open_host_doorbell().unwrap();
        let host = HostEnd::new(Arc::new(Channel::new(memory)), FutexDoorbell);
        Lane {
            geometry,
            host,
            bell,
            shm,
            bell_write,
        }
    }

    /// Forks a peer that maps the channel from the fds it inherited, as a worker maps them from the template's, and
    /// runs `body` on its end.
    fn fork_peer(&self, body: impl FnOnce(&mut Peer) -> i32) -> libc::pid_t {
        fork_child(|| {
            let mut peer = attach(&self.geometry, &self.shm, &self.bell_write);
            body(&mut peer)
        })
    }

    fn send(&mut self, seq: u32) {
        let mut slot = self
            .host
            .request_slot()
            .expect("the test sends only while the ring has room");
        assert_eq!(slot.seq(), Seq::new(seq));
        slot.write(|w| w.put(&request(seq))).unwrap();
        slot.publish().unwrap_or_else(|never| match never {});
    }

    /// Returns the next answer the peer publishes, sleeping on the wake pipe as the host does whenever none is
    /// there.
    fn next_answer(&mut self) -> (Seq, BytesMut) {
        let mut message = BytesMut::new();
        loop {
            if let Some(seq) = self.host.answers().unwrap().take_next(&mut message).unwrap() {
                return (seq, message);
            }
            if sleep_all(std::slice::from_mut(&mut self.host)) == SleepVerdict::Park {
                wait_for_ring(&self.bell);
                drain_once(&self.bell).unwrap();
            }
            wake_all(std::slice::from_mut(&mut self.host));
        }
    }
}

fn attach(geometry: &Geometry, shm: &OwnedFd, bell_write: &OwnedFd) -> Peer {
    let memory = MappedMemory::attach(geometry, shm.as_fd()).unwrap();
    PeerEnd::new(
        Arc::new(Channel::new(memory)),
        PipeDoorbell::new(bell_write.try_clone().unwrap()),
        FutexParker,
    )
}

fn request(seq: u32) -> Vec<u8> {
    format!("request {seq}").into_bytes()
}

fn now_ns() -> u64 {
    let ts = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// Where a peer that dies on purpose stops.
#[derive(Clone, Copy, Debug)]
enum Death {
    /// It took this request and died before it wrote a byte of the answer.
    AfterTaking(u32),
    /// It wrote half of this request's answer and died before it published it.
    MidAnswer(u32),
    /// It published this request's answer and died.
    AfterAnswering(u32),
}

/// Answers each request with its own bytes, up to request `last` or until `death`.
fn echo(peer: &mut Peer, death: Option<Death>, last: u32) -> i32 {
    loop {
        let request = match peer.wait_for_request(NO_SPIN, now_ns, PEER_SLICE_NS) {
            Ok(Wait::Request(request)) => request,
            Ok(Wait::SliceOver) => continue,
            Err(refused) => panic!("the peer's wait was refused: {refused}"),
        };
        let seq = request.seq().get();
        if matches!(death, Some(Death::AfterTaking(at)) if at == seq) {
            die();
        }
        let message = request.bytes().unwrap().to_vec();
        let rung = request.answer(|w| {
            if matches!(death, Some(Death::MidAnswer(at)) if at == seq) {
                w.put(&message[..message.len() / 2]).unwrap();
                die();
            }
            w.put(&message).unwrap();
        });
        match rung {
            Ok(Some(HostBellOutcome::HostGone)) => return 0,
            Ok(_) => {}
            Err(broken) => panic!("the host's wake pipe broke: {broken}"),
        }
        if matches!(death, Some(Death::AfterAnswering(at)) if at == seq) {
            die();
        }
        if seq == last {
            return 0;
        }
    }
}

fn die() -> ! {
    // SAFETY: SIGKILL to this process, which does not return.
    unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
    unreachable!("SIGKILL ends the process");
}

/// Forks a child that runs `body` and ends with its status.
fn fork_child(body: impl FnOnce() -> i32) -> libc::pid_t {
    // SAFETY: the child runs `body` and `_exit`s; it touches no lock the test harness holds (malloc's are made safe
    // across fork by the C library) and never returns into the harness.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork: {}", std::io::Error::last_os_error());
    if pid == 0 {
        // SAFETY: `_exit` runs nothing of this process's.
        std::panic::set_hook(Box::new(|_| unsafe { libc::_exit(CHILD_PANICKED) }));
        let status = body();
        // SAFETY: as above.
        unsafe { libc::_exit(status) };
    }
    pid
}

/// How a child ended.
#[derive(Debug, PartialEq, Eq)]
enum End {
    Exited(i32),
    Killed(i32),
}

fn wait_end(pid: libc::pid_t) -> End {
    let mut status = 0;
    // SAFETY: `status` is a writable int and `pid` is this process's child.
    assert_eq!(
        unsafe { libc::waitpid(pid, &mut status, 0) },
        pid,
        "waitpid: {}",
        std::io::Error::last_os_error()
    );
    if libc::WIFSIGNALED(status) {
        End::Killed(libc::WTERMSIG(status))
    } else {
        End::Exited(libc::WEXITSTATUS(status))
    }
}

fn kill(pid: libc::pid_t) {
    // SAFETY: `pid` is this process's child, not yet waited for.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
}

/// Waits until the peer has written into the host's wake pipe; one that never does fails the test.
fn wait_for_ring(bell: &HostBell) {
    let mut watched = libc::pollfd {
        fd: bell.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one valid pollfd for the call.
    let ready = unsafe { libc::poll(&mut watched, 1, HOST_WAIT_MS) };
    assert_eq!(
        ready,
        1,
        "the peer never rang the host's wake pipe: {}",
        std::io::Error::last_os_error()
    );
}

/// Each doorbell is proved once on its own before the bulk: the host is asleep before the peer answers, so the peer
/// must ring the pipe; the peer is asleep before the host publishes, so the host must wake its futex. A ring lost
/// either way leaves the waiting side asleep past [`HOST_WAIT_MS`].
#[test]
fn a_forked_peer_answers_through_a_mapped_channel() {
    const REQUESTS: u32 = 10_000;
    let mut lane = Lane::new(4);
    let peer = lane.fork_peer(|peer| echo(peer, None, REQUESTS - 1));

    assert_eq!(sleep_all(std::slice::from_mut(&mut lane.host)), SleepVerdict::Park);
    lane.send(0);
    wait_for_ring(&lane.bell);
    wake_all(std::slice::from_mut(&mut lane.host));
    assert_eq!(lane.next_answer(), (Seq::new(0), BytesMut::from(&request(0)[..])));

    while lane.host.channel().peer_waits().parks == 0 {
        std::thread::yield_now();
    }
    lane.send(1);
    assert_eq!(lane.next_answer(), (Seq::new(1), BytesMut::from(&request(1)[..])));

    let (mut sent, mut answered) = (2, 2);
    while answered < REQUESTS {
        while sent < REQUESTS && lane.host.window().has_room(lane.geometry.depth()) {
            lane.send(sent);
            sent += 1;
        }
        let (seq, message) = lane.next_answer();
        assert_eq!((seq, &message[..]), (Seq::new(answered), &request(answered)[..]));
        answered += 1;
    }
    let waits = lane.host.channel().peer_waits();
    assert!(waits.parks > 0 && waits.wakes > 0, "{waits:?}");

    drop(lane);
    assert_eq!(wait_end(peer), End::Exited(0));
}

/// A child forked by either end has no mapping at the channel's address, so its first read there is a fault; the
/// same read of a channel on the heap, which a fork does copy, goes through.
#[test]
fn a_child_of_either_end_cannot_touch_the_mapping() {
    let lane = Lane::new(2);
    let heap = Channel::new(HeapMemory::new(&lane.geometry));
    let reads = |channel: &Channel<MappedMemory>| {
        fork_child(|| {
            std::hint::black_box(channel.peer_waits());
            0
        })
    };

    assert_eq!(
        wait_end(reads(lane.host.channel())),
        End::Killed(libc::SIGSEGV),
        "a child of the host"
    );
    let peer = fork_child(|| {
        let mapped = Channel::new(MappedMemory::attach(&lane.geometry, lane.shm.as_fd()).unwrap());
        std::hint::black_box(mapped.peer_waits());
        let child = fork_child(|| {
            std::hint::black_box(mapped.peer_waits());
            0
        });
        let control = fork_child(|| {
            std::hint::black_box(heap.peer_waits());
            0
        });
        match (wait_end(child), wait_end(control)) {
            (End::Killed(libc::SIGSEGV), End::Exited(0)) => 0,
            ends => panic!("the peer's children ended {ends:?}"),
        }
    });
    assert_eq!(wait_end(peer), End::Exited(0), "a child of the peer");
}

/// A peer that dies on purpose at each place in its work leaves the window split where it died: the answers it
/// published are copied out whole, and the request it was running and those it never took are given up. None of them
/// runs on the next peer, which could otherwise run bytes the dead one rewrote; it serves only what is sent after.
#[test]
fn a_peer_that_dies_anywhere_leaves_its_answers_and_no_request_for_the_next() {
    for death in (0..4).flat_map(|at| [Death::AfterTaking(at), Death::MidAnswer(at), Death::AfterAnswering(at)]) {
        let mut lane = Lane::new(8);
        for seq in 0..6 {
            lane.send(seq);
        }
        let peer = lane.fork_peer(|peer| echo(peer, Some(death), 5));
        assert_eq!(wait_end(peer), End::Killed(libc::SIGKILL), "{death:?}");

        let mut answered = Vec::new();
        let split = lane.host.take_over(&mut answered).unwrap();
        let (published, running) = match death {
            Death::AfterTaking(at) | Death::MidAnswer(at) => (at, Some(Seq::new(at))),
            Death::AfterAnswering(at) => (at + 1, None),
        };
        assert_eq!(answered, (0..published).map(echoed).collect::<Vec<_>>(), "{death:?}");
        assert_eq!(split.running(), running, "{death:?}");
        let untaken = published + u32::from(running.is_some());
        assert_eq!(
            (split.handed().start(), split.handed().end()),
            (Seq::new(untaken), Seq::new(6)),
            "{death:?}"
        );
        assert_eq!(lane.host.in_flight(), 0, "{death:?}");

        let replacement = lane.fork_peer(|peer| echo(peer, None, 6));
        lane.send(6);
        assert_eq!(
            lane.next_answer(),
            (Seq::new(6), BytesMut::from(&request(6)[..])),
            "{death:?}"
        );
        drop(lane);
        assert_eq!(wait_end(replacement), End::Exited(0));
    }
}

fn echoed(seq: u32) -> DeadPeerAnswer {
    DeadPeerAnswer {
        seq: Seq::new(seq),
        message: Ok(request(seq).into()),
    }
}

/// A peer killed from outside at whatever point it reached leaves every request in exactly one place: answered once
/// with its own bytes, or given up at its death (the one it was running and those it never took).
#[test]
fn a_peer_killed_at_any_moment_loses_no_request_and_answers_none_twice() {
    const REQUESTS: u32 = 2_000;
    const KILLS: u32 = 16;
    let mut lane = Lane::new(4);
    let mut times_answered = vec![0u32; REQUESTS as usize];
    let mut given_up = 0;
    let mut peer = lane.fork_peer(|peer| echo(peer, None, REQUESTS - 1));
    let (mut sent, mut read) = (0, 0);
    let mut kills = (1..=KILLS).map(|k| k * REQUESTS / (KILLS + 1));
    let mut next_kill = kills.next();

    while read < REQUESTS {
        while sent < REQUESTS && lane.host.window().has_room(lane.geometry.depth()) {
            lane.send(sent);
            sent += 1;
        }
        if next_kill.is_some_and(|at| read >= at) {
            next_kill = kills.next();
            kill(peer);
            assert_eq!(wait_end(peer), End::Killed(libc::SIGKILL));
            let mut answered = Vec::new();
            let split = lane.host.take_over(&mut answered).unwrap();
            for answer in answered {
                assert_eq!(answer, echoed(answer.seq.get()));
                times_answered[answer.seq.get() as usize] += 1;
            }
            given_up += u32::from(split.running().is_some()) + split.handed().len();
            read = split.handed().end().get();
            peer = lane.fork_peer(|peer| echo(peer, None, REQUESTS - 1));
            continue;
        }
        let (seq, message) = lane.next_answer();
        assert_eq!(&message[..], &request(seq.get())[..]);
        times_answered[seq.get() as usize] += 1;
        read = seq.get() + 1;
    }

    assert!(times_answered.iter().all(|&n| n <= 1), "an answer came twice");
    assert_eq!(times_answered.iter().filter(|&&n| n == 0).count() as u32, given_up);
    drop(lane);
    assert_eq!(wait_end(peer), End::Exited(0));
}

/// Writes `writes` words of garbage into the channel's memory: small numbers near the ring's counters, the largest
/// numbers and anything at all, each at a word anywhere in the memory, chosen by `seed`.
fn scribble(shm: &OwnedFd, total_bytes: usize, seed: u64, writes: u32) {
    // SAFETY: a new shared mapping of the whole object at an address the kernel picks.
    let base = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            total_bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            shm.as_raw_fd(),
            0,
        )
    };
    assert_ne!(base, libc::MAP_FAILED, "mmap: {}", std::io::Error::last_os_error());
    let words = total_bytes / 4;
    let mut state = seed | 1;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for _ in 0..writes {
        let at = (next() % words as u64) as usize;
        let value = match next() % 3 {
            0 => (next() % 16) as u32,
            1 => u32::MAX - (next() % 4) as u32,
            _ => next() as u32,
        };
        // SAFETY: `at` is a word inside the mapping; the host reads these words only as atomics or by copying.
        unsafe { base.cast::<u32>().add(at).write_volatile(value) };
    }
}

/// Exercises every way the host reads the channel, while the peer may be writing garbage into it, and checks that
/// nothing it believed is out of the host's own bounds.
fn read_everything(lane: &mut Lane, fault: &mut Option<PeerFault>) {
    let max = lane.geometry.max_message() as usize;
    let mut message = BytesMut::new();
    match lane.host.answers() {
        Ok(mut ready) => {
            assert_eq!(*fault, None, "a fault the host kept was forgotten");
            loop {
                message.clear();
                match ready.take_next(&mut message) {
                    Ok(Some(_)) => assert!(message.len() <= max, "{} bytes believed", message.len()),
                    Ok(None) => break,
                    Err(found) => {
                        *fault = Some(found);
                        break;
                    }
                }
            }
        }
        Err(found) => assert!(fault.is_none_or(|kept| kept == found), "{fault:?} then {found:?}"),
    }
    if let Err(found) = lane.host.answers() {
        *fault = Some(found);
    }
    let window = lane.host.window();
    if let Ok(Some(running)) = lane.host.running() {
        assert!(
            running.since(window.read()) < window.in_flight(),
            "{running} outside {window:?}"
        );
    }
    std::hint::black_box(lane.host.has_news());
    if let Ok(Some(StartOutcome::Failed(note))) = lane.host.start_report(Attempt::new(1)) {
        assert!(note.len() <= NOTE_BYTES as usize);
    }
    std::hint::black_box(sleep_all(std::slice::from_mut(&mut lane.host)));
    wake_all(std::slice::from_mut(&mut lane.host));
    if lane.host.request_slot().is_some() {
        assert_eq!(*fault, None, "a peer at fault was sent a request");
    }
    let window = lane.host.window();
    assert!(window.in_flight() <= lane.geometry.depth().get(), "{window:?}");
}

/// A peer that writes garbage anywhere in the channel, while the host reads it and after, never makes the host read
/// past its own bounds, forget a fault it found, or misplace a request when it takes over; and once it is dead, no
/// request it could have rewritten reaches the next peer, which is served as on a clean channel.
#[test]
fn a_peer_writing_garbage_anywhere_cannot_crash_mislead_or_outlive_itself() {
    for seed in 0..16u64 {
        let mut lane = Lane::new(4);
        for seq in 0..3 {
            lane.send(seq);
        }
        let total = lane.geometry.total_bytes();
        let shm = lane.shm.try_clone().unwrap();
        let writer = fork_child(move || {
            scribble(&shm, total, seed, 200_000);
            0
        });
        let mut fault = None;
        loop {
            read_everything(&mut lane, &mut fault);
            let mut status = 0;
            // SAFETY: `status` is a writable int and `writer` is this process's child.
            match unsafe { libc::waitpid(writer, &mut status, libc::WNOHANG) } {
                0 => continue,
                pid => {
                    assert_eq!(pid, writer);
                    assert!(
                        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
                        "the writer ended {status}"
                    );
                    break;
                }
            }
        }
        read_everything(&mut lane, &mut fault);

        let before = lane.host.window();
        let mut answered = Vec::new();
        match lane.host.take_over(&mut answered) {
            Ok(split) => {
                assert_eq!(
                    (split.answered().start(), split.handed().end()),
                    (before.read(), before.sent())
                );
                assert_eq!(answered.len() as u32, split.answered().len());
                assert!(answered
                    .iter()
                    .all(|a| a.message.as_ref().is_ok_and(|m| m.len() <= total)));
            }
            Err(dead) => assert_eq!(dead.abandoned, before.outstanding()),
        }

        assert_eq!(lane.host.in_flight(), 0, "seed {seed}");
        let next = before.sent().get();
        let peer = lane.fork_peer(|peer| echo(peer, None, next + 7));
        for seq in next..next + 8 {
            lane.send(seq);
            assert_eq!(
                lane.next_answer(),
                (Seq::new(seq), BytesMut::from(&request(seq)[..])),
                "seed {seed}"
            );
        }
        drop(lane);
        assert_eq!(wait_end(peer), End::Exited(0), "seed {seed}");
    }
}

/// A peer that keeps writing into the host's wake pipe cannot hold the host in a read: each drain reads once.
#[test]
fn a_peer_flooding_the_wake_pipe_cannot_hold_the_host_in_a_read() {
    let (bell, bell_write) = open_host_doorbell().unwrap();
    let flooder = fork_child(|| loop {
        // A full pipe answers EAGAIN, which a flooder ignores; a reader gone ends it.
        if rustix::io::write(&bell_write, &[1u8; 4096]) == Err(rustix::io::Errno::PIPE) {
            return 0;
        }
    });
    wait_for_ring(&bell);
    let mut more_left = 0;
    for _ in 0..1_000 {
        if drain_once(&bell).unwrap().outcome == DrainOutcome::MoreLeft {
            more_left += 1;
        }
    }
    assert!(more_left > 0);
    kill(flooder);
    assert_eq!(wait_end(flooder), End::Killed(libc::SIGKILL));
}
