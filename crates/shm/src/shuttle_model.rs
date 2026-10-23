//! The channel's Shuttle model (b/744, design §4.3): one host thread runs four channels, each with a peer thread of
//! its own, through the shipping ends ([`HostEnd`], [`PeerEnd`], `sleep_all`, `wake_all`) over a [`ModelMemory`] and
//! the same kernel as the Loom models (`model.rs`: the peer sleeps on a futex, the host on its one wake pipe, neither
//! with a timeout). Loom enumerates two threads and a few steps; this runs five threads and 800 requests under
//! seeded PCT schedules, so it checks the protocol at scale: nothing is lost, duplicated or reordered, and a wake is
//! never lost (a lost one leaves every thread asleep, which Shuttle reports as a deadlock). Shuttle runs every
//! atomic as `SeqCst` and checks no cell, so memory ordering and slot races stay the Loom models' (`loom_model.rs`).
//!
//! The merge gate runs [`SCHEDULES`] schedules of [`SEED`] (dev/proofs.toml):
//! `RUSTFLAGS="--cfg shm_model --cfg shm_shuttle" cargo test -p mipyre_shm --lib --profile model --target-dir target/shuttle shuttle_model`.
//! The nightly run adds `--cfg shm_shuttle_random` for random schedules; a schedule that fails prints itself, and goes
//! into [`REGRESSIONS`], replayed on every run.

use std::sync::Arc;

use bytes::BytesMut;
use shuttle::scheduler::PctScheduler;
use shuttle::{thread, Config, Runner};

use crate::channel::{sleep_all, wake_all, Channel, HostEnd, ModelMemory, PeerEnd, SleepVerdict, Wait, MODEL_SLOT};
use crate::model::{FutexModel, PipeModel};
use crate::plan::geometry::{Geometry, SlotBytes};
use crate::plan::ring::{RingDepth, Seq};
use crate::plan::spin_window::SpinWindow;

type Host = HostEnd<ModelMemory, FutexModel>;
type Peer = PeerEnd<ModelMemory, PipeModel, FutexModel>;

const CHANNELS: usize = 4;
const REQUESTS: usize = 200;
const DEPTH: u32 = 2;
/// How many priority changes a PCT schedule makes: bugs that need at most this many to show are found with a known
/// probability per run.
const PCT_DEPTH: usize = 3;

/// The merge gate's schedules: [`SCHEDULES`] PCT schedules drawn from this seed, the same on every run. One seed and
/// many schedules, not many seeds of one schedule each: PCT picks where to change priorities from the run lengths it
/// has seen, so the first schedule of a seed changes them nowhere useful, and 32 one-schedule seeds missed the lost
/// wake that [`a_host_that_says_it_sleeps_after_its_last_look_loses_a_wake`] plants (8 schedules of one seed miss it
/// too, 16 find it).
const SEED: u64 = 0;
const SCHEDULES: usize = 32;

/// Schedules a random run found failing, as Shuttle printed them; each is replayed on every run.
const REGRESSIONS: &[&str] = &[];

/// What the request numbered `n` on a channel carries, and what its answer must carry: one byte each.
fn request_byte(n: usize) -> u8 {
    (n % 251) as u8
}

fn answer_byte(request: u8) -> u8 {
    request.wrapping_add(1)
}

fn channel() -> Arc<Channel<ModelMemory>> {
    let slot = SlotBytes::new(16).unwrap();
    assert!(slot.get() <= MODEL_SLOT);
    let geometry = Geometry::new(RingDepth::new(DEPTH).unwrap(), slot).unwrap();
    Arc::new(Channel::new(ModelMemory::new(&geometry).unwrap()))
}

/// Answers `n` requests, each with its byte plus one, and returns the numbers and bytes it took, in order.
fn peer(mut peer: Peer, n: usize) -> Vec<(Seq, u8)> {
    let mut taken = Vec::with_capacity(n);
    while taken.len() < n {
        let Ok(wait) = peer.wait_for_request(SpinWindow::until(0), || 0, 0);
        let Wait::Request(request) = wait else { continue };
        let seq = request.seq();
        let byte = request.bytes().unwrap()[0];
        let Ok(_) = request.answer(|w| w.put(&[answer_byte(byte)]).unwrap());
        taken.push((seq, byte));
    }
    taken
}

/// The host: keeps every channel's ring full until each has sent [`REQUESTS`], reads every answer, and sleeps as the
/// host's park hook does when there is nothing to read (say awake, say asleep and look once more, wait on the pipe
/// only on `Park`). Returns each channel's answers in the order read.
fn host(ends: &mut [Host; CHANNELS], pipe: &PipeModel) -> [Vec<(Seq, u8)>; CHANNELS] {
    let mut sent = [0usize; CHANNELS];
    let mut read: [Vec<(Seq, u8)>; CHANNELS] = Default::default();
    let mut buf = BytesMut::new();
    loop {
        for (i, end) in ends.iter_mut().enumerate() {
            while sent[i] < REQUESTS {
                let Some(mut slot) = end.request_slot() else { break };
                slot.write(|w| w.put(&[request_byte(sent[i])])).unwrap();
                let Ok(_) = slot.publish();
                sent[i] += 1;
            }
            let mut ready = end.answers().expect("a correct peer");
            while let Some(seq) = ready.take_next(&mut buf).expect("a correct peer") {
                read[i].push((seq, buf[0]));
                buf.clear();
            }
        }
        if read.iter().map(Vec::len).sum::<usize>() == CHANNELS * REQUESTS {
            return read;
        }
        let room = ends
            .iter_mut()
            .zip(&sent)
            .any(|(end, &n)| n < REQUESTS && end.request_slot().is_some());
        if room {
            continue;
        }
        wake_all(ends);
        if sleep_all(ends) == SleepVerdict::Park {
            pipe.wait_and_drain();
        }
    }
}

/// One run: every request is taken once, in order, by its channel's peer, and its answer read once, in order, with
/// what the peer answered; the pipe holds at most one byte per answer.
fn model() {
    let (futex, pipe) = (FutexModel::default(), PipeModel::default());
    let channels: Vec<_> = (0..CHANNELS).map(|_| channel()).collect();
    let peers: Vec<_> = channels
        .iter()
        .map(|c| {
            let end = PeerEnd::new(Arc::clone(c), pipe.clone(), futex.clone());
            thread::spawn(move || peer(end, REQUESTS))
        })
        .collect();
    let mut ends: [Host; CHANNELS] = std::array::from_fn(|i| HostEnd::new(Arc::clone(&channels[i]), futex.clone()));

    let read = host(&mut ends, &pipe);

    let expected: Vec<(Seq, u8)> = (0..REQUESTS).map(|n| (Seq::new(n as u32), request_byte(n))).collect();
    for (i, taken) in peers.into_iter().map(|p| p.join().unwrap()).enumerate() {
        assert_eq!(taken, expected, "channel {i}: every request taken once, in order");
        let answered: Vec<_> = expected.iter().map(|&(seq, byte)| (seq, answer_byte(byte))).collect();
        assert_eq!(read[i], answered, "channel {i}: every answer read once, in order");
        assert_eq!(ends[i].in_flight(), 0, "channel {i}: nothing left in flight");
    }
    let answers = (CHANNELS * REQUESTS) as u32;
    assert!(
        pipe.written() <= answers,
        "{} wake bytes for {answers} answers",
        pipe.written()
    );
}

fn config() -> Config {
    let mut config = Config::new();
    // Shuttle warns, once per process, that it runs weaker orderings as `SeqCst`: this module's doc says so.
    config.silence_warnings = true;
    config
}

/// The merge gate's schedules ([`SEED`], [`SCHEDULES`]), the same on every run; each runs the model to its end.
#[test]
fn every_request_is_taken_and_answered_once_in_order_under_the_fixed_schedules() {
    // Counts the runs that got to the model's end, outside Shuttle's view: the schedules ran the model whole.
    let ended = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&ended);
    Runner::new(PctScheduler::new_from_seed(SEED, PCT_DEPTH, SCHEDULES), config()).run(move || {
        model();
        counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });
    assert_eq!(ended.load(std::sync::atomic::Ordering::Relaxed), SCHEDULES);
}

/// The check itself: a host that says it sleeps only after its last look, as a host with no `sleep_all` would, loses
/// a wake in some schedule the merge gate runs (the peer publishes between the look and the flag, sees no flag and
/// rings nothing), and Shuttle reports the deadlock. It runs that shape on bare words, not the shipping code.
#[test]
#[should_panic(expected = "deadlock")]
fn a_host_that_says_it_sleeps_after_its_last_look_loses_a_wake() {
    use crate::sync::{AtomicU32, Ordering::SeqCst};
    let lost = || {
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
        let seen = published.load(SeqCst);
        host_sleeping.store(1, SeqCst);
        if seen == 0 {
            pipe.wait_and_drain();
        }
        peer.join().unwrap();
    };
    Runner::new(PctScheduler::new_from_seed(SEED, PCT_DEPTH, SCHEDULES), config()).run(lost);
}

/// The schedules a random run found failing, replayed.
#[test]
fn the_schedules_found_failing_pass() {
    for schedule in REGRESSIONS {
        shuttle::replay(model, schedule);
    }
}

/// The nightly run (b/744): random PCT schedules, many of them, built only under `--cfg shm_shuttle_random` so the
/// merge gate's run stays the fixed [`SCHEDULES`]; a failing one prints its schedule for [`REGRESSIONS`].
#[cfg(shm_shuttle_random)]
#[test]
fn every_request_is_taken_and_answered_once_in_order_under_random_schedules() {
    Runner::new(PctScheduler::new(PCT_DEPTH, 1_000), config()).run(model);
}
