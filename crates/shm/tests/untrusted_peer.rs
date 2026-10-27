//! Holds the channel to what it promises about a peer it does not trust: every value the peer can write in
//! the header, at each edge of what the host believes, through a real shared mapping the test writes as the peer
//! does; the ring's wrap at 2^32 at every depth; a peer that drops requests unanswered; a dead peer's
//! leftovers and the next attempt; and memory the OS will not give.
//!
//! The peer's words are written through a second mapping of the object, word by word, as a peer process would; the
//! word offsets are checked against what the channel itself writes before any test relies on them.

// Miri runs no system call, and the Loom build (`--cfg shm_model`) has no OS half.
#![cfg(not(any(miri, shm_model)))]
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "the test's helpers fail the test by panicking"
)]

use std::convert::Infallible;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use mipyre_shm::adapter::doorbell::FutexDoorbell;
use mipyre_shm::adapter::mapped::{AttachError, MappedMemory};
use mipyre_shm::adapter::parker::FutexParker;
use mipyre_shm::channel::{Attempt, Channel, HeapMemory, HostEnd, PeerEnd, PeerFault, StartOutcome, Wait, NOTE_BYTES};
use mipyre_shm::plan::geometry::{Geometry, SlotBytes, HEADER_PAGE};
use mipyre_shm::plan::ring::{count_ready_answers, exit_split, running, PeerCounterError, RingDepth, Seq, Window};
use mipyre_shm::plan::slot_len::SlotLengthError;
use mipyre_shm::plan::spin_window::SpinWindow;
use mipyre_shm::plan::start::StartReportError;
use mipyre_shm::protocol::doorbell::{AtomicU32, Doorbell, Parker};
use mipyre_shm::Syscall;

/// A peer's doorbell to a host that never sleeps in these tests.
struct NoBell;

impl Doorbell for NoBell {
    type Outcome = ();
    type Error = Infallible;
    fn ring(&self, _: &AtomicU32) -> Result<(), Infallible> {
        Ok(())
    }
}

/// The peer parks as soon as it finds no request.
const NO_SPIN: SpinWindow = SpinWindow::until(0);

/// Bytes from the header page's start to each word the peer can write, as `ChannelHeader` lays them out: each word on
/// a 128-byte line of its own (`mipyre_cpu::CacheLine`). [`Rig::new`] checks them against what the channel writes.
mod at {
    pub const REQ_HEAD: usize = 0;
    pub const TAKEN: usize = 128;
    pub const RESP_HEAD: usize = 256;
    pub const PEER_SLEEPING: usize = 384;
    pub const HOST_SLEEPING: usize = 512;
    pub const ATTEMPT: usize = 640;
    pub const STATE: usize = 768;
    pub const NOTE_LEN: usize = 896;
    pub const WAITS: usize = 1024;
}

/// The start report's states, as the peer writes them.
const READY: u32 = 1;
const FAILED: u32 = 2;

type Host = HostEnd<MappedMemory, FutexDoorbell>;
type Peer = PeerEnd<MappedMemory, NoBell, FutexParker>;

fn geometry(depth: u32, slot_bytes: usize) -> Geometry {
    Geometry::new(RingDepth::new(depth).unwrap(), SlotBytes::new(slot_bytes).unwrap()).unwrap()
}

/// A second shared mapping of the channel's object, through which the test writes as the peer does.
struct PeerView {
    base: *mut u8,
    len: usize,
}

impl PeerView {
    fn new(shm: &OwnedFd, len: usize) -> Self {
        // SAFETY: a new shared mapping of the whole object at an address the kernel picks.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                shm.as_raw_fd(),
                0,
            )
        };
        assert_ne!(base, libc::MAP_FAILED, "mmap: {}", std::io::Error::last_os_error());
        PeerView { base: base.cast(), len }
    }

    fn put_u32(&self, at: usize, value: u32) {
        assert!(at.is_multiple_of(4) && at + 4 <= self.len);
        // SAFETY: an aligned word inside the mapping; the host reads it only as an atomic.
        unsafe { self.base.add(at).cast::<u32>().write_volatile(value) };
    }

    fn u32_at(&self, at: usize) -> u32 {
        assert!(at.is_multiple_of(4) && at + 4 <= self.len);
        // SAFETY: as in `put_u32`.
        unsafe { self.base.add(at).cast::<u32>().read_volatile() }
    }

    fn put_u64(&self, at: usize, value: u64) {
        assert!(at.is_multiple_of(8) && at + 8 <= self.len);
        // SAFETY: as in `put_u32`, a word of 8 bytes.
        unsafe { self.base.add(at).cast::<u64>().write_volatile(value) };
    }
}

// SAFETY: the view is a range of shared addresses read and written only by volatile word accesses, from any thread.
unsafe impl Send for PeerView {}

impl Drop for PeerView {
    fn drop(&mut self) {
        // SAFETY: the whole of the mapping `new` made.
        unsafe { libc::munmap(self.base.cast(), self.len) };
    }
}

/// One channel in a mapping, its host's end, and a view to write into it as the peer.
struct Rig {
    geometry: Geometry,
    host: Host,
    shm: OwnedFd,
    view: PeerView,
}

impl Rig {
    fn new(depth: u32) -> Self {
        let geometry = geometry(depth, 64);
        let (memory, shm) = MappedMemory::create(&geometry).unwrap();
        let host = HostEnd::new(Arc::new(Channel::new(memory)), FutexDoorbell);
        let view = PeerView::new(&shm, geometry.total_bytes());
        let mut rig = Rig {
            geometry,
            host,
            shm,
            view,
        };
        rig.check_offsets();
        rig
    }

    /// Fails the test unless the offsets in [`at`] are where the channel writes its words.
    fn check_offsets(&mut self) {
        self.host.begin_attempt(Attempt::new(0xA77E));
        assert_eq!(self.view.u32_at(at::ATTEMPT), 0xA77E, "attempt");
        self.view.put_u32(at::NOTE_LEN, 3);
        self.view.put_u32(at::STATE, FAILED);
        assert_eq!(
            self.host
                .start_report(Attempt::new(0xA77E))
                .unwrap()
                .map(|o| matches!(o, StartOutcome::Failed(n) if n.len() == 3)),
            Some(true)
        );
        self.host.begin_attempt(Attempt::new(0));
        self.view.put_u64(at::WAITS, 11);
        self.view.put_u64(at::WAITS + 24, 7);
        let waits = self.host.channel().peer_waits();
        assert_eq!((waits.waits, waits.wakes), (11, 7), "peer waits");
        self.view.put_u64(at::WAITS, 0);
        self.view.put_u64(at::WAITS + 24, 0);
        mipyre_shm::channel::sleep_all(std::slice::from_mut(&mut self.host));
        assert_eq!(self.view.u32_at(at::HOST_SLEEPING), 1, "host_sleeping");
        mipyre_shm::channel::wake_all(std::slice::from_mut(&mut self.host));
        assert_eq!(self.view.u32_at(at::HOST_SLEEPING), 0, "host_sleeping");
    }

    fn peer(&self) -> Peer {
        let memory = MappedMemory::attach(&self.geometry, self.shm.as_fd()).unwrap();
        PeerEnd::new(Arc::new(Channel::new(memory)), NoBell, FutexParker)
    }

    fn send(&mut self, n: u32) {
        for _ in 0..n {
            let mut slot = self.host.request_slot().expect("the ring has room");
            let seq = slot.seq();
            slot.write(|w| w.put(&seq.get().to_le_bytes())).unwrap();
            slot.publish().unwrap_or_else(|never| match never {});
        }
    }

    /// Takes every answer ready, as the host does.
    fn take_all(&mut self) -> Result<Vec<(Seq, Vec<u8>)>, PeerFault> {
        let mut ready = self.host.answers()?;
        let mut out = Vec::new();
        loop {
            let mut buf = BytesMut::new();
            match ready.take_next(&mut buf)? {
                Some(seq) => out.push((seq, buf.to_vec())),
                None => return Ok(out),
            }
        }
    }

    /// Leaves the window at `[2, 6)`: six requests sent, the first two answered by a real peer and read.
    fn window_2_6() -> Self {
        let mut rig = Rig::new(8);
        rig.send(6);
        let mut peer = rig.peer();
        for _ in 0..2 {
            echo(&mut peer);
        }
        assert_eq!(rig.view.u32_at(at::REQ_HEAD), 6, "req_head");
        assert_eq!(
            (rig.view.u32_at(at::TAKEN), rig.view.u32_at(at::RESP_HEAD)),
            (2, 2),
            "taken, resp_head"
        );
        assert_eq!(rig.take_all().unwrap().len(), 2);
        assert_eq!(rig.host.window(), Window::new(Seq::new(2), Seq::new(6)));
        rig
    }
}

/// Takes the next request, which must be there, and answers it with its own bytes.
fn echo(peer: &mut Peer) -> Seq {
    let Ok(Wait::Request(request)) = peer.wait_for_request(NO_SPIN, || 0, 1) else {
        panic!("a request was published");
    };
    let (seq, bytes) = (request.seq(), request.bytes().unwrap().to_vec());
    request
        .answer(|w| w.put(&bytes).unwrap())
        .unwrap_or_else(|never| match never {});
    seq
}

/// Runs `body` on a thread of its own and returns what it returned, or fails the test once `limit` passes: a body
/// that never returns is a hang.
fn within<T: Send + 'static>(limit: Duration, what: &str, body: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    std::thread::Builder::new()
        .name(what.into())
        .spawn(move || {
            match tx.send(body()) {
                Ok(()) => {}
                // The receiver is gone only once the limit passed, and the test has failed on that already.
                Err(mpsc::SendError(_late)) => {}
            }
        })
        .unwrap();
    rx.recv_timeout(limit)
        .unwrap_or_else(|e| panic!("{what} did not finish within {limit:?}: {e}"))
}

/// At every power-of-two depth, a window that straddles the wrap of the u32 sequence keeps each request in a
/// slot of its own inside the mapping, counts every answer up to what was sent, and refuses one past either edge.
#[test]
fn every_depth_keeps_the_requests_in_flight_in_distinct_slots_across_the_wrap() {
    for raw in [3, 5, 6, 7, 9, 12, u32::MAX] {
        assert_eq!(
            RingDepth::new(raw),
            None,
            "depth {raw} would put two requests in one slot at the wrap"
        );
    }
    for log2 in 0..=31u32 {
        let depth = RingDepth::new(1 << log2).unwrap();
        let g = Geometry::new(depth, SlotBytes::new(8).unwrap()).unwrap();
        let read = Seq::new(u32::MAX - depth.get() / 2);
        let sent = read.after(depth.get());
        let w = Window::new(read, sent);
        assert!(
            !w.has_room(depth) && Window::new(read, sent.after(u32::MAX)).has_room(depth),
            "depth {}",
            depth.get()
        );
        // Up to 2^16 slots are checked one by one; past that, the slots at both ends of the window and on both
        // sides of the wrap.
        let ks: Vec<u32> = if log2 <= 16 {
            (0..depth.get()).collect()
        } else {
            let d = depth.get();
            let wrap = d / 2 + 1;
            vec![0, 1, wrap - 2, wrap - 1, wrap, wrap + 1, d - 2, d - 1]
        };
        let mut taken = std::collections::HashSet::new();
        for &k in &ks {
            let seq = read.after(k);
            let (req, ans) = (g.request_slot(seq).get(), g.answer_slot(seq).get());
            assert!(
                req >= HEADER_PAGE && req + 8 <= g.answer_slot(Seq::new(0)).get(),
                "depth {} seq {seq}",
                depth.get()
            );
            assert!(ans + 8 <= g.total_bytes(), "depth {} seq {seq}", depth.get());
            assert!(
                taken.insert(seq.slot_index(depth)),
                "depth {}: seq {seq} shares a slot",
                depth.get()
            );
            assert_eq!(count_ready_answers(&w, seq), Ok(k), "depth {}", depth.get());
        }
        assert_eq!(count_ready_answers(&w, sent), Ok(depth.get()));
        assert!(
            count_ready_answers(&w, sent.after(1)).is_err(),
            "one past what was sent, depth {}",
            depth.get()
        );
        assert!(
            count_ready_answers(&w, read.after(u32::MAX)).is_err(),
            "one behind what was read, depth {}",
            depth.get()
        );
        assert_eq!(
            running(&w, read, sent),
            Ok(Some(read)),
            "took every request sent, depth {}",
            depth.get()
        );
        assert!(running(&w, read, sent.after(1)).is_err());
        assert!(exit_split(&w, read, read.after(1))
            .is_ok_and(|s| s.running() == Some(read) && s.handed().len() == depth.get() - 1));
    }
}

/// The two counters a live peer writes, at every value from two behind what was read to two past what was sent:
/// the host believes exactly `read ≤ published ≤ taken ≤ sent`, keeps anything else as the peer's fault with the
/// value it read, and from then on reads nothing of the peer's and sends it nothing.
#[test]
fn a_live_peers_counters_at_each_edge_are_believed_or_kept_as_its_fault() {
    let (read, sent) = (2u32, 6u32);
    for published in 0..=8u32 {
        for taken in 0..=8u32 {
            let mut rig = Rig::window_2_6();
            rig.view.put_u32(at::RESP_HEAD, published);
            rig.view.put_u32(at::TAKEN, taken);
            let believed = (read..=sent).contains(&published) && (published..=sent).contains(&taken);
            let case = format!("published {published}, taken {taken}");
            match rig.host.running() {
                Ok(run) => {
                    assert!(believed, "{case}: believed");
                    assert_eq!(run, (taken != published).then_some(Seq::new(published)), "{case}");
                    assert!(rig.host.request_slot().is_some(), "{case}");
                }
                Err(fault) => {
                    assert!(!believed, "{case}: refused {fault:?}");
                    let PeerFault::Counter(e) = fault else {
                        panic!("{case}: {fault:?}")
                    };
                    let said = match e {
                        PeerCounterError::AnswersPastSent { published: p, .. } => p.get() == published,
                        PeerCounterError::TakenOutOfRange {
                            published: p, taken: t, ..
                        } => p.get() == published && t.get() == taken,
                    };
                    assert!(said, "{case}: the fault says what was read: {e}");
                    rig.view.put_u32(at::RESP_HEAD, sent);
                    rig.view.put_u32(at::TAKEN, sent);
                    assert_eq!(rig.host.answers().err(), Some(fault), "{case}: the fault is kept");
                    assert!(
                        rig.host.request_slot().is_none(),
                        "{case}: a peer at fault is sent nothing"
                    );
                    assert!(!rig.host.has_answers(), "{case}");
                }
            }
        }
    }
}

/// A dead peer's two counters at every value around the window: the host splits the window by them exactly when a
/// standing peer could have left them, else gives up every request sent; either way the next peer takes only what is
/// sent after.
#[test]
fn a_dead_peers_counters_at_each_edge_split_the_window_or_give_it_up() {
    let (read, sent) = (2u32, 6u32);
    for published in 0..=8u32 {
        for taken in 0..=8u32 {
            let mut rig = Rig::window_2_6();
            rig.view.put_u32(at::RESP_HEAD, published);
            rig.view.put_u32(at::TAKEN, taken);
            rig.view.put_u32(at::REQ_HEAD, 0xdead);
            let case = format!("published {published}, taken {taken}");
            let split_ok =
                (read..=sent).contains(&published) && (taken == published || (taken == published + 1 && taken <= sent));
            let mut answered = Vec::new();
            match rig.host.take_over(&mut answered) {
                Ok(split) => {
                    assert!(split_ok, "{case}: split");
                    assert_eq!(answered.len() as u32, published - read, "{case}");
                    assert_eq!(
                        split.running(),
                        (taken != published).then_some(Seq::new(published)),
                        "{case}"
                    );
                    assert_eq!(
                        (split.handed().start().get(), split.handed().end().get()),
                        (taken.max(published), sent),
                        "{case}"
                    );
                }
                Err(dead) => {
                    assert!(!split_ok, "{case}: given up: {dead}");
                    assert_eq!(
                        (dead.abandoned.start().get(), dead.abandoned.end().get()),
                        (read, sent),
                        "{case}"
                    );
                    assert!(answered.is_empty(), "{case}");
                }
            }
            assert_eq!(rig.host.in_flight(), 0, "{case}");
            assert_eq!(
                (
                    rig.view.u32_at(at::REQ_HEAD),
                    rig.view.u32_at(at::TAKEN),
                    rig.view.u32_at(at::RESP_HEAD)
                ),
                (sent, sent, sent),
                "{case}: the counters are put back"
            );
            let mut next = rig.peer();
            assert!(
                matches!(next.wait_for_request(NO_SPIN, || 0, 1), Ok(Wait::SliceOver)),
                "{case}: the next peer takes nothing old"
            );
            rig.send(1);
            assert_eq!(echo(&mut next), Seq::new(sent), "{case}");
            assert_eq!(
                rig.take_all(),
                Ok(vec![(Seq::new(sent), sent.to_le_bytes().to_vec())]),
                "{case}"
            );
        }
    }
}

/// A slot's length word at each edge of the slot's room, in both rings: the host takes an answer up to the room and
/// keeps one past it as the fault, with the value; the peer refuses a request past it with the value.
#[test]
fn a_slot_length_at_each_edge_is_taken_or_refused_with_its_value() {
    let max = geometry(8, 64).max_message();
    for len in [0, 1, max - 1, max, max + 1, u32::MAX] {
        let mut rig = Rig::window_2_6();
        let mut peer = rig.peer();
        echo(&mut peer);
        rig.view.put_u32(rig.geometry.answer_slot(Seq::new(2)).get(), len);
        let mut buf = BytesMut::new();
        let taken = rig.host.answers().unwrap().take_next(&mut buf);
        if len <= max {
            assert_eq!(
                (taken, buf.len()),
                (Ok(Some(Seq::new(2))), len as usize),
                "answer of {len}"
            );
        } else {
            assert_eq!(
                taken,
                Err(PeerFault::Length(SlotLengthError::PastSlot { len, max })),
                "answer of {len}"
            );
            assert!(buf.is_empty(), "answer of {len}: nothing copied");
        }

        rig.view.put_u32(rig.geometry.request_slot(Seq::new(3)).get(), len);
        let Ok(Wait::Request(request)) = peer.wait_for_request(NO_SPIN, || 0, 1) else {
            panic!("request 3 was sent")
        };
        match request.bytes() {
            Ok(view) => assert!(len <= max && view.len() == len as usize, "request of {len}"),
            Err(e) => assert!(
                len > max && e == SlotLengthError::PastSlot { len, max },
                "request of {len}: {e}"
            ),
        }
    }
}

/// A peer that dies before the next attempt can leave that attempt's number and a ready state behind; beginning the
/// attempt clears them, so only the next peer's own report is read. The note's length at each edge of its area is
/// read or refused with the value, and a state no peer writes is refused with it.
#[test]
fn a_start_report_is_the_attempts_own_and_its_note_is_held_to_its_area() {
    let mut rig = Rig::new(1);
    let next = Attempt::new(2);
    rig.view.put_u32(at::ATTEMPT, next.get());
    rig.view.put_u32(at::STATE, READY);
    rig.view.put_u32(at::NOTE_LEN, u32::MAX);
    rig.host.begin_attempt(next);
    assert_eq!(
        rig.host.start_report(next),
        Ok(None),
        "the dead peer's report is cleared"
    );
    let peer = rig.peer();
    peer.report_ready(Attempt::new(1)).unwrap();
    assert_eq!(
        rig.host.start_report(next),
        Ok(None),
        "a peer of another attempt writes nothing"
    );
    peer.report_ready(next).unwrap();
    assert_eq!(rig.host.start_report(next), Ok(Some(StartOutcome::Ready)));

    for len in [0, 1, NOTE_BYTES - 1, NOTE_BYTES, NOTE_BYTES + 1, u32::MAX] {
        rig.view.put_u32(at::NOTE_LEN, len);
        rig.view.put_u32(at::STATE, FAILED);
        match rig.host.start_report(next) {
            Ok(Some(StartOutcome::Failed(note))) => {
                assert!(len <= NOTE_BYTES && note.len() == len as usize, "note of {len}")
            }
            Err(StartReportError::NoteLength(SlotLengthError::PastSlot { len: said, max })) => {
                assert!(len > NOTE_BYTES && said == len && max == NOTE_BYTES, "note of {len}")
            }
            other => panic!("note of {len}: {other:?}"),
        }
    }
    for state in [3, 0x100, u32::MAX] {
        rig.view.put_u32(at::STATE, state);
        assert_eq!(rig.host.start_report(next), Err(StartReportError::UnknownState(state)));
    }
}

/// A dead peer that left every word of the header it can write set wrong (its sleep flag down, the host's up, the
/// host's own `req_head` ahead, its wait counts at the top) does not stop the next peer, in another thread on real
/// futex sleeps of a minute per slice, from being woken by the host's publish and answering.
///
/// The limit is 20 s: no step here waits on a clock except the peer's minute-long sleep slices, so a lost wake shows
/// as the limit passing, and a working round trip takes microseconds.
#[test]
fn a_dead_peers_leftover_flags_do_not_cost_the_next_peer_its_wake() {
    let mut rig = Rig::window_2_6();
    rig.view.put_u32(at::PEER_SLEEPING, 0);
    rig.view.put_u32(at::HOST_SLEEPING, 1);
    rig.view.put_u32(at::REQ_HEAD, 1000);
    for k in 0..4 {
        rig.view.put_u64(at::WAITS + 8 * k, u64::MAX);
    }
    rig.host.take_over(&mut Vec::new()).unwrap();
    let mut next = rig.peer();
    let (parked_tx, parked_rx) = mpsc::channel();
    let view_parks = PeerView::new(&rig.shm, rig.geometry.total_bytes());
    let peer = std::thread::Builder::new()
        .name("next peer".into())
        .spawn(move || {
            parked_tx.send(()).unwrap();
            loop {
                match next.wait_for_request(NO_SPIN, || 0, 60_000_000_000) {
                    Ok(Wait::Request(request)) => {
                        let (seq, bytes) = (request.seq(), request.bytes().unwrap().to_vec());
                        request
                            .answer(|w| w.put(&bytes).unwrap())
                            .unwrap_or_else(|never| match never {});
                        return seq;
                    }
                    Ok(Wait::SliceOver) => continue,
                    Err(e) => panic!("the peer's sleep was refused: {e}"),
                }
            }
        })
        .unwrap();
    parked_rx.recv().unwrap();
    within(Duration::from_secs(20), "the next peer parks", move || {
        while view_parks.u32_at(at::PEER_SLEEPING) == 0 {
            std::thread::yield_now();
        }
    });
    rig.send(1);
    let mut rig = within(Duration::from_secs(20), "the answer of the woken peer", move || {
        while !rig.host.has_answers() {
            std::thread::yield_now();
        }
        rig
    });
    assert_eq!(rig.take_all(), Ok(vec![(Seq::new(6), 6u32.to_le_bytes().to_vec())]));
    assert_eq!(peer.join().unwrap(), Seq::new(6));
}

/// A request the peer drops unanswered is taken again, by the same peer: across a thousand requests on two threads,
/// each one dropped once before it is answered, every answer reaches the host once, in order, with its own bytes.
///
/// The limit is 20 s: neither side waits on a clock (the peer's parker yields), so only a lost request ends there.
#[test]
fn a_request_dropped_unanswered_is_taken_again_and_answered_once() {
    struct Yield;
    impl Parker for Yield {
        type Error = Infallible;
        fn park(&self, _: &AtomicU32, _: u32, _: u64) -> Result<(), Infallible> {
            std::thread::yield_now();
            Ok(())
        }
    }
    const REQUESTS: u32 = 1000;
    let channel = Arc::new(Channel::new(HeapMemory::new(&geometry(4, 64))));
    let mut host = HostEnd::new(Arc::clone(&channel), FutexDoorbell);
    let mut peer = PeerEnd::new(channel, NoBell, Yield);
    within(
        Duration::from_secs(20),
        "a thousand requests each dropped once",
        move || {
            std::thread::scope(|s| {
                s.spawn(move || {
                    let mut dropped = None;
                    let mut answered = 0;
                    while answered < REQUESTS {
                        let Ok(Wait::Request(request)) = peer.wait_for_request(NO_SPIN, || 0, 1) else {
                            continue;
                        };
                        let seq = request.seq();
                        if dropped != Some(seq) {
                            dropped = Some(seq);
                            drop(request);
                            continue;
                        }
                        let bytes = request.bytes().unwrap().to_vec();
                        request
                            .answer(|w| w.put(&bytes).unwrap())
                            .unwrap_or_else(|never| match never {});
                        answered += 1;
                    }
                });
                let (mut sent, mut read) = (0u32, 0u32);
                while read < REQUESTS {
                    while sent < REQUESTS {
                        let Some(mut slot) = host.request_slot() else { break };
                        slot.write(|w| w.put(&sent.to_le_bytes())).unwrap();
                        slot.publish().unwrap_or_else(|never| match never {});
                        sent += 1;
                    }
                    let mut ready = host.answers().unwrap();
                    let mut buf = BytesMut::new();
                    while let Some(seq) = ready.take_next(&mut buf).unwrap() {
                        assert_eq!((seq, &buf[..]), (Seq::new(read), &read.to_le_bytes()[..]));
                        buf.clear();
                        read += 1;
                    }
                    std::thread::yield_now();
                }
            });
        },
    );
}

/// A layout larger than the OS will back is refused by the call that refused it, never a crash; an fd too small for
/// the layout, or not a shared memory object at all, is refused before it is mapped.
#[test]
fn memory_the_os_will_not_give_or_an_fd_too_small_is_refused_not_a_crash() {
    let huge = Geometry::new(RingDepth::new(1 << 30).unwrap(), SlotBytes::new(1 << 31).unwrap()).unwrap();
    assert_eq!(huge.total_bytes(), (1 << 62) + HEADER_PAGE);
    let refused = MappedMemory::create(&huge).err().expect("2^62 bytes are refused");
    assert!(matches!(refused.call, Syscall::Ftruncate | Syscall::Mmap), "{refused}");

    let small = geometry(2, 64);
    let (_memory, shm) = MappedMemory::create(&small).unwrap();
    let bigger = geometry(4, 4096);
    assert_eq!(
        MappedMemory::attach(&bigger, shm.as_fd()).err(),
        Some(AttachError::TooSmall {
            file_bytes: small.total_bytes() as i64,
            need: bigger.total_bytes()
        })
    );
    assert!(matches!(
        MappedMemory::attach(&huge, shm.as_fd()).err(),
        Some(AttachError::TooSmall { .. })
    ));
    let (read, _write) = rustix::pipe::pipe().unwrap();
    match MappedMemory::attach(&small, read.as_fd()) {
        Err(AttachError::TooSmall { file_bytes: 0, .. } | AttachError::NotMapped(_)) => {}
        other => panic!("a pipe attached as a channel: {:?}", other.err()),
    }
}
