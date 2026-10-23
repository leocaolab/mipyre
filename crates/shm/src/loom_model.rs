//! The Loom models of the channel: a host and a peer run the shipping ends ([`HostEnd`], [`PeerEnd`], `sleep_all`,
//! `wake_all`) over a [`ModelMemory`], every interleaving within three preemptions. The peer sleeps on the model's
//! futex and the host on the model's wake pipe (`model.rs`), neither with a timeout, so a wake the doorbell's
//! protocol loses leaves a side asleep for good, which Loom reports as a deadlock; every slot's bytes are a cell
//! Loom checks, so the two sides touching one slot without the counters ordering them is reported too. Run with
//! `RUSTFLAGS="--cfg shm_model" cargo test -p mipyre_shm --lib --profile model --target-dir target/loom loom_model`.
//!
//! It proves: every request published is taken, and every answer published is read, though either side may have
//! gone to sleep (the doorbell's fences, `protocol::doorbell::publish`); the host's one fence in `sleep_all` covers
//! an answer on any of its channels; a slot is reused only after the side before it is done with it, the request's
//! bytes held by the peer included; every answer a dead peer published reaches the host once, by a read or by the
//! take-over, and a slot it left stale is read only as the next peer rewrote it; a start report is read whole or
//! not at all.

use std::sync::Arc;

use bytes::BytesMut;
use loom::thread;

use crate::channel::{
    sleep_all, wake_all, Attempt, Channel, HostEnd, ModelMemory, PeerEnd, SleepVerdict, StartOutcome, Wait, MODEL_SLOT,
};
use crate::model::{FutexModel, PipeModel};
use crate::plan::geometry::{Geometry, SlotBytes};
use crate::plan::ring::{RingDepth, Seq};
use crate::plan::spin_window::SpinWindow;

/// The host rings the peer's futex; the peer rings the host's pipe and sleeps on its futex.
type Host = HostEnd<ModelMemory, FutexModel>;
type Peer = PeerEnd<ModelMemory, PipeModel, FutexModel>;

/// An answer as the host read it: the channel, the request's number and the answer's one byte.
type Answer = (usize, Seq, u8);

/// Runs `model` over every interleaving within three preemptions (design §4.3: the bound is part of what is proved).
fn check(model: impl Fn() + Sync + Send + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(model);
}

fn channel(depth: u32) -> Arc<Channel<ModelMemory>> {
    let slot = SlotBytes::new(16).unwrap();
    assert!(slot.get() <= MODEL_SLOT);
    let geometry = Geometry::new(RingDepth::new(depth).unwrap(), slot).unwrap();
    Arc::new(Channel::new(ModelMemory::new(&geometry).unwrap()))
}

/// Publishes a request of one byte, ringing the peer when it said it sleeps.
fn send(host: &mut Host, byte: u8) {
    let mut slot = host.request_slot().expect("the ring has room");
    slot.write(|w| w.put(&[byte])).unwrap();
    let Ok(_) = slot.publish();
}

/// Waits for the next request and answers it with its byte plus one; with `hold`, the peer keeps the request's
/// bytes borrowed across a yield, as a handler holds them while it runs.
fn answer_next(peer: &mut Peer, hold: bool) -> (Seq, u8) {
    loop {
        let Ok(wait) = peer.wait_for_request(SpinWindow::until(0), || 0, 0);
        let Wait::Request(request) = wait else { continue };
        let seq = request.seq();
        let view = request.bytes().unwrap();
        let byte = view[0];
        if hold {
            thread::yield_now();
        }
        drop(view);
        let Ok(_) = request.answer(|w| w.put(&[byte + 1]).unwrap());
        return (seq, byte);
    }
}

/// Reads answers off `ends` into `read` until it holds `want`, sleeping as the host's park hook does when there are
/// none: say awake, say asleep and look once more ([`sleep_all`]), and wait on the pipe only on `Park`.
fn read_until(ends: &mut [Host], pipe: &PipeModel, read: &mut Vec<Answer>, want: usize) {
    loop {
        take_ready(ends, read);
        if read.len() >= want {
            return;
        }
        wake_all(ends);
        if sleep_all(ends) == SleepVerdict::Park {
            pipe.wait_and_drain();
        }
    }
}

/// Reads every answer ready on `ends` into `read`, without sleeping.
fn take_ready(ends: &mut [Host], read: &mut Vec<Answer>) {
    let mut buf = BytesMut::new();
    for (i, end) in ends.iter_mut().enumerate() {
        let mut ready = end.answers().expect("a correct peer");
        while let Some(seq) = ready.take_next(&mut buf).expect("a correct peer") {
            read.push((i, seq, buf[0]));
            buf.clear();
        }
    }
}

fn spawn_peer(
    channel: &Arc<Channel<ModelMemory>>,
    pipe: &PipeModel,
    futex: &FutexModel,
    n: usize,
    hold: bool,
) -> thread::JoinHandle<Vec<(Seq, u8)>> {
    let mut peer = PeerEnd::new(Arc::clone(channel), pipe.clone(), futex.clone());
    thread::spawn(move || (0..n).map(|_| answer_next(&mut peer, hold)).collect())
}

/// Three requests through a ring of two: the third reuses the first's slots once its answer is read. The peer
/// sleeps whenever it finds nothing, so a request published past its sleep is taken only if the bell wakes it.
#[test]
fn a_published_request_is_always_taken() {
    check(|| {
        let (futex, pipe, channel) = (FutexModel::default(), PipeModel::default(), channel(2));
        let peer = spawn_peer(&channel, &pipe, &futex, 3, false);
        let mut ends = [HostEnd::new(channel, futex)];
        let mut read = Vec::new();

        send(&mut ends[0], 10);
        send(&mut ends[0], 20);
        read_until(&mut ends, &pipe, &mut read, 1);
        send(&mut ends[0], 30);
        read_until(&mut ends, &pipe, &mut read, 3);

        let (s0, s1, s2) = (Seq::new(0), Seq::new(1), Seq::new(2));
        assert_eq!(
            peer.join().unwrap(),
            [(s0, 10), (s1, 20), (s2, 30)],
            "taken once each, in order"
        );
        assert_eq!(read, [(0, s0, 11), (0, s1, 21), (0, s2, 31)]);
    });
}

/// The host sleeps whenever it finds no answer, so an answer published past its `sleep_all` is read only if the
/// peer sees the flag and writes the pipe (b/781); it writes at most one byte per answer.
#[test]
fn a_published_answer_always_wakes_a_sleeping_host() {
    check(|| {
        let (futex, pipe, channel) = (FutexModel::default(), PipeModel::default(), channel(2));
        let mut ends = [HostEnd::new(Arc::clone(&channel), futex.clone())];
        send(&mut ends[0], 1);
        send(&mut ends[0], 2);
        let peer = spawn_peer(&channel, &pipe, &futex, 2, false);
        let mut read = Vec::new();

        read_until(&mut ends, &pipe, &mut read, 2);

        assert_eq!(read, [(0, Seq::new(0), 2), (0, Seq::new(1), 3)]);
        assert_eq!(peer.join().unwrap().len(), 2);
        assert!(pipe.written() <= 2, "{} bytes for 2 answers", pipe.written());
    });
}

/// One slot each way, two requests: the host rewrites the request slot and the peer the answer slot only after the
/// other side is done with it, while the peer holds the request's bytes borrowed across a yield. A write the
/// counters do not order after the other side's access is a race on the slot's cell, which Loom reports.
#[test]
fn a_slot_is_never_touched_by_both_sides() {
    check(|| {
        let (futex, pipe, channel) = (FutexModel::default(), PipeModel::default(), channel(1));
        let peer = spawn_peer(&channel, &pipe, &futex, 2, true);
        let mut ends = [HostEnd::new(channel, futex)];
        let mut read = Vec::new();

        send(&mut ends[0], 5);
        assert!(
            ends[0].request_slot().is_none(),
            "the only slot is the peer's until its answer is read"
        );
        read_until(&mut ends, &pipe, &mut read, 1);
        send(&mut ends[0], 7);
        read_until(&mut ends, &pipe, &mut read, 2);

        assert_eq!(read, [(0, Seq::new(0), 6), (0, Seq::new(1), 8)]);
        drop(peer.join().unwrap());
    });
}

/// A host asleep on two channels, with a request in flight on each and an answer coming on the second only: the one
/// fence in `sleep_all` covers every end, so the answer wakes the host or is seen before it sleeps.
#[test]
fn an_answer_on_the_second_channel_wakes_a_host_asleep_on_both() {
    check(|| {
        let (futex, pipe) = (FutexModel::default(), PipeModel::default());
        let (first, second) = (channel(1), channel(1));
        let mut ends = [
            HostEnd::new(first, futex.clone()),
            HostEnd::new(Arc::clone(&second), futex.clone()),
        ];
        send(&mut ends[0], 1);
        send(&mut ends[1], 2);
        let peer = spawn_peer(&second, &pipe, &futex, 1, false);
        let mut read = Vec::new();

        read_until(&mut ends, &pipe, &mut read, 1);

        assert_eq!(read, [(1, Seq::new(0), 3)]);
        assert_eq!(
            ends[0].in_flight(),
            1,
            "the first channel's request is still the peer's that never came"
        );
        drop(peer.join().unwrap());
    });
}

/// A peer answers the first of two requests and dies running the second, while the host reads what it finds; the
/// join stands for the host seeing the peer exit, which is when the lane takes over. The dead peer's answer reaches
/// the host once, by the read or by the take-over, and the request it died running is given up. The next peer's
/// first answer lies in the slot where the dead peer's answer still is: the host reads it only as the next peer
/// wrote it, and a slot read before the next peer's publish is a race on the slot's cell, which Loom reports.
#[test]
fn a_dead_peers_answer_reaches_the_host_once_and_its_stale_slot_is_never_read_as_an_answer() {
    check(|| {
        let (futex, pipe, channel) = (FutexModel::default(), PipeModel::default(), channel(2));
        let mut ends = [HostEnd::new(Arc::clone(&channel), futex.clone())];
        send(&mut ends[0], 1);
        send(&mut ends[0], 2);
        let mut dying: Peer = PeerEnd::new(Arc::clone(&channel), pipe.clone(), futex.clone());
        let dying = thread::spawn(move || {
            answer_next(&mut dying, false);
            let Ok(Wait::Request(running)) = dying.wait_for_request(SpinWindow::until(0), || 0, 0) else {
                panic!("the second request was published before the peer started");
            };
            drop(running);
        });
        let mut read = Vec::new();

        take_ready(&mut ends, &mut read);
        dying.join().unwrap();
        let mut answered = Vec::new();
        let split = ends[0]
            .take_over(&mut answered)
            .expect("a dead peer's counters fit the window");
        read.extend(
            answered
                .iter()
                .map(|a| (0, a.seq, a.message.as_ref().expect("a length that fits")[0])),
        );

        assert_eq!(read, [(0, Seq::new(0), 2)], "the dead peer's answer, once");
        assert_eq!(split.running(), Some(Seq::new(1)));
        assert!(split.handed().is_empty());

        let next = spawn_peer(&channel, &pipe, &futex, 2, false);
        send(&mut ends[0], 30);
        send(&mut ends[0], 40);
        let mut after = Vec::new();
        read_until(&mut ends, &pipe, &mut after, 2);

        assert_eq!(after, [(0, Seq::new(2), 31), (0, Seq::new(3), 41)]);
        assert_eq!(
            next.join().unwrap(),
            [(Seq::new(2), 30), (Seq::new(3), 40)],
            "the given-up request is never taken"
        );
    });
}

/// The host looks at the start report while the peer it started writes a failure: it sees nothing yet or the note
/// whole, never a state without its note or a note being written (a read of the note's cell the state's publish
/// does not order is a race Loom reports). Spawning the peer after `begin_attempt` stands for the lane starting it.
#[test]
fn a_start_report_is_read_whole_or_not_at_all() {
    check(|| {
        const NOTE: &[u8] = b"Traceback";
        let (futex, pipe, channel) = (FutexModel::default(), PipeModel::default(), channel(1));
        let mut host: Host = HostEnd::new(Arc::clone(&channel), futex.clone());
        let attempt = Attempt::new(1);
        host.begin_attempt(attempt);
        let peer: Peer = PeerEnd::new(channel, pipe, futex);
        let peer = thread::spawn(move || peer.report_failed(attempt, NOTE));
        let failed = Some(StartOutcome::Failed(NOTE.to_vec()));

        let early = host.start_report(attempt).expect("a report a peer writes");
        peer.join().unwrap();
        let late = host.start_report(attempt).expect("a report a peer writes");

        assert!(early.is_none() || early == failed, "{early:?}");
        assert_eq!(late, failed);
    });
}

/// The check itself: a report whose state is stored without `Release`, read without `Acquire`, lets the host read the
/// note while the peer may still be writing it, and the models report the race on the note's cell. It runs a copy
/// of the shape on bare words and a bare cell, not the shipping code.
#[test]
#[should_panic(expected = "Causality violation")]
fn a_report_state_published_relaxed_lets_the_host_read_a_note_being_written() {
    use crate::sync::{AtomicU32, Ordering::Relaxed, SlotCell};

    /// A start report's state and a one-byte note.
    struct Report {
        state: AtomicU32,
        note: SlotCell<u8>,
    }
    // SAFETY: both threads reach the cell on purpose; Loom checks every access, and reporting the overlap is the test.
    unsafe impl Sync for Report {}

    check(|| {
        let report = Arc::new(Report {
            state: AtomicU32::new(0),
            note: SlotCell::new(0),
        });
        let peer = thread::spawn({
            let report = Arc::clone(&report);
            move || {
                // SAFETY: as above.
                report.note.with_mut(|b| unsafe { *b = 7 });
                report.state.store(1, Relaxed);
            }
        });
        if report.state.load(Relaxed) == 1 {
            // SAFETY: as above.
            let byte = report.note.with(|b| unsafe { *b });
            assert_eq!(byte, 7);
        }
        peer.join().unwrap();
    });
}

/// The check itself: the host's look before it sleeps without the fence, as it was before b/781 (the flag stored
/// `SeqCst`, the counter read `Acquire`), loses a wake in some interleaving, and the models report it. It runs a
/// copy of the shape on bare words, not the shipping code.
#[test]
#[should_panic(expected = "deadlock")]
fn a_look_before_sleeping_without_the_fence_loses_a_wake() {
    use crate::sync::{
        AtomicU32,
        Ordering::{Acquire, SeqCst},
    };
    check(|| {
        let pipe = PipeModel::default();
        let words = Arc::new((AtomicU32::new(0), AtomicU32::new(0)));
        let peer = thread::spawn({
            let (pipe, words) = (pipe.clone(), Arc::clone(&words));
            move || {
                let (published, host_sleeping) = &*words;
                published.store(1, SeqCst);
                if host_sleeping.load(SeqCst) == 1 {
                    let Ok(()) = crate::protocol::doorbell::Doorbell::ring(&pipe, published);
                }
            }
        });
        let (published, host_sleeping) = &*words;
        host_sleeping.store(1, SeqCst);
        if published.load(Acquire) == 0 {
            pipe.wait_and_drain();
        }
        peer.join().unwrap();
    });
}
