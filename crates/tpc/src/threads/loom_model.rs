//! The Loom model of a run's stop ([`RunStop`], [`stop_on_failure`]): threads that fail on their
//! own and threads that fail because the stop reached them, over every interleaving, running the
//! shipping code (the cause's lock is Loom's here, `crate::sync`). The token is the model's own
//! [`Cancellable`]: a flag stored with Release and read with Acquire, the least a
//! `CancellationToken` gives (tokio_util's sets it under a lock that its readers take). Run with
//! `RUSTFLAGS="--cfg sync_model" cargo test -p mipyre_tpc --lib --release --target-dir target/loom loom`.
//!
//! It proves what the run's cause rests on: a companion (a failure that came after the stop
//! reached its thread) never becomes the cause, because a cause is recorded only while the token
//! is not cancelled, the root's before it cancels, and a companion fails only once it sees the
//! cancel.

use std::sync::Arc;

use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;

use super::{stop_on_failure, Cancellable, RunStop, StopCause, ThreadError, ThreadStop};

#[derive(Clone, Debug, Default)]
struct StopFlag(Arc<AtomicBool>);

impl Cancellable for StopFlag {
    fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

type ModelRun = RunStop<(), StopFlag>;

fn failure() -> Result<(), ThreadError<()>> {
    Err(ThreadError::Panicked("failed".to_string()))
}

/// A thread that fails on its own.
fn root(stop: &ModelRun, name: &'static str) -> thread::JoinHandle<()> {
    let stop = stop.clone();
    thread::spawn(move || {
        drop(stop_on_failure(&ThreadStop::new(stop, name.into()), |_| failure()));
    })
}

/// A thread that fails only once the stop has reached it (what a server's thread does when its
/// stop ends its serve).
fn companion(stop: &ModelRun) -> thread::JoinHandle<()> {
    let stop = stop.clone();
    thread::spawn(move || {
        while !stop.is_stopped() {
            thread::yield_now();
        }
        drop(stop_on_failure(&ThreadStop::new(stop, "companion".into()), |_| failure()));
    })
}

fn failed(thread: &str) -> Option<StopCause<()>> {
    Some(StopCause::ThreadFailed { thread: thread.into() })
}

#[test]
fn a_companion_is_never_the_cause() {
    loom::model(|| {
        let stop = ModelRun::new(StopFlag::default());
        let companion = companion(&stop);
        let root = root(&stop, "root");
        root.join().unwrap();
        companion.join().unwrap();
        assert_eq!(stop.cause(), failed("root"));
    });
}

/// Two threads fail on their own, a third because the stop reached it: whichever root is first,
/// the companion is never the cause.
#[test]
fn of_two_roots_and_a_companion_the_cause_is_a_roots() {
    loom::model(|| {
        let stop = ModelRun::new(StopFlag::default());
        let companion = companion(&stop);
        let one = root(&stop, "one");
        let two = root(&stop, "two");
        one.join().unwrap();
        two.join().unwrap();
        companion.join().unwrap();
        let cause = stop.cause();
        assert!(cause == failed("one") || cause == failed("two"), "the cause is {cause:?}");
    });
}
