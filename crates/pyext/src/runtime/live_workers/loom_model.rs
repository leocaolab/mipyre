//! The Loom model of a kind's live workers ([`WorkerLife::died`], the run's [`ServerRunStop`]): workers that
//! die at once, over every interleaving, running the shipping code (the count and the first cause
//! are `crate::sync`'s, the run's cause `mipyre_tpc`'s, Loom's here, and so are the run's
//! counters). The run's
//! stop is the model's own [`Cancellable`], which counts its cancels: tokio_util's token runs its
//! own lock, which Loom cannot see. Run with
//! `RUSTFLAGS="--cfg sync_model" cargo test -p mipyre_pyext --lib --profile model --target-dir target/loom loom`.
//!
//! It proves: of a kind's workers dying at once, exactly the last one stops the run, once, and
//! hands back the kind's first cause, which every caller reads the same; of two kinds that share
//! a run, each kind's last stops it and the run's cause is one of theirs, the same for both. That
//! a kind is taken down at most once per worker (b/595) is the type's: a [`WorkerLife`] is taken
//! by its death, and there is one per worker.

use std::sync::Arc;

use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::thread;
use mipyre_tpc::{Cancellable, StopCause};

use super::{LiveWorkers, ServerRunStop, ServerStop, WorkerKind, WorkerLife};
use crate::telemetry::RunCounters;

#[derive(Clone, Default)]
struct StopCounter(Arc<AtomicUsize>);

impl Cancellable for StopCounter {
    fn cancel(&self) {
        self.0.fetch_add(1, Ordering::Release);
    }

    fn is_cancelled(&self) -> bool {
        self.cancels() > 0
    }
}

impl StopCounter {
    fn cancels(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

type ModelStop = ServerRunStop<StopCounter>;

/// `workers` lives of `kind` in the run `stop` stops.
fn kind(
    kind: WorkerKind,
    workers: usize,
    stop: &ModelStop,
) -> (LiveWorkers<StopCounter>, Vec<WorkerLife<StopCounter>>) {
    LiveWorkers::new(kind, workers, stop.clone(), Arc::new(RunCounters::new(false)))
}

/// Returns the first worker's cause the run was stopped with, and the kind it was of.
fn first_cause(stop: &ModelStop) -> Option<(WorkerKind, Arc<str>)> {
    match stop.cause() {
        Some(StopCause::Caller(ServerStop::NoWorkerLeft { kind, first })) => Some((kind, first)),
        _ => None,
    }
}

fn die(life: WorkerLife<StopCounter>, cause: &'static str) -> thread::JoinHandle<Option<Arc<str>>> {
    thread::spawn(move || life.died(cause.to_string()))
}

#[test]
fn of_two_workers_dying_at_once_the_last_stops_the_run_once_with_the_first_cause() {
    loom::model(|| {
        let signal = StopCounter::default();
        let stop = ModelStop::new(signal.clone());
        let (kind, lives) = kind(WorkerKind::TpcThread, 2, &stop);
        let mut lives = lives.into_iter();
        let (Some(one), Some(two)) = (lives.next(), lives.next()) else {
            panic!("two workers, two lives");
        };
        let other = die(two, "two");
        let mine = one.died("one".to_string());
        let theirs = other.join().unwrap();

        assert!(
            mine.is_some() != theirs.is_some(),
            "one death is the last: {mine:?}, {theirs:?}"
        );
        assert_eq!(signal.cancels(), 1, "the run is stopped once");
        let first = kind.first_death().expect("a worker died");
        assert_eq!(
            mine.or(theirs).as_deref(),
            Some(&*first),
            "the last hands back the first cause"
        );
        let (_, cause) = first_cause(&stop).expect("the run was stopped for a cause");
        assert_eq!(cause, first);
    });
}

#[test]
fn of_two_kinds_whose_last_workers_die_at_once_the_run_has_one_cause() {
    loom::model(|| {
        let signal = StopCounter::default();
        let stop = ModelStop::new(signal.clone());
        let (_, sync) = kind(WorkerKind::PoolSync, 1, &stop);
        let (_, r#async) = kind(WorkerKind::PoolAsync, 1, &stop);
        let (Some(sync), Some(r#async)) = (sync.into_iter().next(), r#async.into_iter().next()) else {
            panic!("one worker of each kind, one life each");
        };
        let other = thread::spawn({
            let stop = stop.clone();
            move || {
                let last = r#async.died("the engine stopped".to_string());
                (last, first_cause(&stop))
            }
        });
        let mine = sync.died("gc.collect() raised".to_string());
        let seen_here = first_cause(&stop);
        let (theirs, seen_there) = other.join().unwrap();

        assert_eq!(
            mine.as_deref(),
            Some("gc.collect() raised"),
            "the sync kind's last hands back its cause"
        );
        assert_eq!(
            theirs.as_deref(),
            Some("the engine stopped"),
            "the async kind's last hands back its cause"
        );
        assert_eq!(signal.cancels(), 2, "each kind's last stops the run");
        let cause = seen_here.expect("stopped");
        assert_eq!(Some(&cause), seen_there.as_ref(), "both read the one cause that stands");
        assert!(
            cause == (WorkerKind::PoolSync, "gc.collect() raised".into())
                || cause == (WorkerKind::PoolAsync, "the engine stopped".into()),
            "{cause:?}"
        );
    });
}
