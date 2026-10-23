//! The job queue's Shuttle model (b/744): the shipping queue (its lock, condition variable and
//! stop flag are `crate::sync`'s, Shuttle's here) under seeded PCT schedules, at a scale the Loom
//! model (`loom_model.rs`, two or three threads) cannot enumerate: four senders, four receivers
//! and a receiver that is stopped while they run, on a queue of two places. Every job is taken
//! once, every receiver ends, and no wake is lost: a receiver left asleep with nothing coming to
//! wake it leaves every thread blocked, which Shuttle reports as a deadlock.
//!
//! The merge gate runs [`SCHEDULES`] schedules of [`SEED`] (dev/proofs.toml):
//! `RUSTFLAGS="--cfg sync_model --cfg sync_shuttle" cargo test -p mipyre_pyext --lib --profile model --target-dir target/shuttle job_queue::shuttle_model`.
//! The nightly run adds `--cfg sync_shuttle_random` for random schedules; a schedule that fails
//! prints itself, and goes into [`REGRESSIONS`], replayed on every run.

use std::sync::Arc;

use shuttle::scheduler::PctScheduler;
use shuttle::{thread, Config, Runner};

use super::{job_queue, JobReceiver, JobSendError};
use crate::sync::{AtomicBool, Ordering};

const SENDERS: usize = 4;
const RECEIVERS: usize = 4;
const JOBS_PER_SENDER: usize = 50;
const CAPACITY: usize = 2;
/// The bug depth a PCT schedule targets: bugs that need at most this many ordering constraints to
/// show are found with a known probability per run. It is 2, not the shm channel's 3: the lost
/// wake that [`a_receiver_that_counts_itself_asleep_after_letting_go_of_the_lock_loses_a_wake`]
/// plants is found in the first 8 schedules of [`SEED`] at depth 2 and in none of 128 at depth 3.
const PCT_DEPTH: usize = 2;

/// The merge gate's schedules: [`SCHEDULES`] PCT schedules drawn from this seed, the same on every
/// run (one seed and many schedules, as the shm channel's model explains: the first schedule of a
/// seed changes priorities nowhere useful). A schedule of the model takes about 140 µs on an M5
/// (release), so the gate runs a thousand.
const SEED: u64 = 0;
const SCHEDULES: usize = 1024;

/// Schedules a random run found failing, as Shuttle printed them; each is replayed on every run.
const REGRESSIONS: &[&str] = &[];

/// Takes jobs until `recv` says nothing more comes, and returns them in the order taken.
fn drain(recv: impl FnMut() -> Option<usize>) -> Vec<usize> {
    std::iter::from_fn(recv).collect()
}

/// One run: the senders retry a full queue until each job is in, the receivers take until the
/// last sender is gone, and one more receiver is stopped and woken while they run. Every job
/// sent is taken exactly once, by some receiver, the stopped one included.
fn model() {
    let (tx, rx) = job_queue::<usize>(CAPACITY);
    let senders: Vec<_> = (0..SENDERS)
        .map(|s| {
            let tx = tx.clone();
            thread::spawn(move || {
                for job in s * JOBS_PER_SENDER..(s + 1) * JOBS_PER_SENDER {
                    while tx.try_send(job) == Err(JobSendError::QueueFull) {
                        thread::yield_now();
                    }
                }
            })
        })
        .collect();
    drop(tx);
    let receivers: Vec<_> = (0..RECEIVERS)
        .map(|_| {
            let rx = rx.clone();
            thread::spawn(move || drain(|| rx.recv()))
        })
        .collect();
    let stop = Arc::new(AtomicBool::new(false));
    let stopped = spawn_stopped(&rx, &stop);

    stop.store(true, Ordering::SeqCst);
    rx.wake_all();

    for sender in senders {
        sender.join().unwrap();
    }
    let mut taken = stopped.join().unwrap();
    for receiver in receivers {
        taken.extend(receiver.join().unwrap());
    }
    taken.sort_unstable();
    let sent: Vec<usize> = (0..SENDERS * JOBS_PER_SENDER).collect();
    assert_eq!(taken, sent, "every job sent is taken exactly once");
}

fn spawn_stopped(rx: &JobReceiver<usize>, stop: &Arc<AtomicBool>) -> thread::JoinHandle<Vec<usize>> {
    let (rx, stop) = (rx.clone(), Arc::clone(stop));
    thread::spawn(move || drain(|| rx.recv_unless(&stop)))
}

fn config() -> Config {
    let mut config = Config::new();
    // Shuttle warns, once per process, that it runs weaker orderings as `SeqCst`; the queue's
    // protocol is under its lock, which Shuttle runs as it is.
    config.silence_warnings = true;
    config
}

/// The merge gate's schedules ([`SEED`], [`SCHEDULES`]), the same on every run; each runs the
/// model to its end.
#[test]
fn every_job_is_taken_once_and_every_receiver_ends_under_the_fixed_schedules() {
    // Counts the runs that got to the model's end, outside Shuttle's view: the schedules ran the
    // model whole.
    let ended = Arc::new(std::sync::Mutex::new(0usize));
    let counted = Arc::clone(&ended);
    Runner::new(PctScheduler::new_from_seed(SEED, PCT_DEPTH, SCHEDULES), config()).run(move || {
        model();
        *counted.lock().unwrap() += 1;
    });
    assert_eq!(*ended.lock().unwrap(), SCHEDULES);
}

/// The check itself: a receiver that counts itself a sleeper only after it let go of the lock it
/// found the queue empty under loses a wake in some schedule the merge gate runs (the sender
/// queues and sees no sleeper in between, so it wakes nobody), and Shuttle reports the deadlock.
/// It runs that shape on a bare lock and condition variable, not the shipping queue.
#[test]
#[should_panic(expected = "deadlock")]
fn a_receiver_that_counts_itself_asleep_after_letting_go_of_the_lock_loses_a_wake() {
    use crate::sync::poisoning::{Condvar, Mutex};

    /// The queued jobs and the receivers counted asleep.
    #[derive(Default)]
    struct Queue {
        jobs: usize,
        sleepers: usize,
    }

    let lost = || {
        let queue = Arc::new((Mutex::new(Queue::default()), Condvar::new()));
        let receiver = thread::spawn({
            let queue = Arc::clone(&queue);
            move || {
                let (state, ready) = &*queue;
                let empty = state.lock().unwrap().jobs == 0;
                if empty {
                    let mut held = state.lock().unwrap();
                    held.sleepers += 1;
                    held = ready.wait(held).unwrap();
                    held.sleepers -= 1;
                }
            }
        });
        let (state, ready) = &*queue;
        let mut held = state.lock().unwrap();
        held.jobs += 1;
        let wake = held.sleepers > 0;
        drop(held);
        if wake {
            ready.notify_one();
        }
        receiver.join().unwrap();
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

/// The nightly run (b/744): random PCT schedules, many of them, built only under
/// `--cfg sync_shuttle_random` so the merge gate's run stays the fixed [`SCHEDULES`]; a failing
/// one prints its schedule for [`REGRESSIONS`].
#[cfg(sync_shuttle_random)]
#[test]
fn every_job_is_taken_once_and_every_receiver_ends_under_random_schedules() {
    Runner::new(PctScheduler::new(PCT_DEPTH, 100_000), config()).run(model);
}
