//! Counts a run's workers by kind and tells the run once a kind has none left.
//!
//! A worker that stops on its own is not rebuilt; the others take its share. When a kind has none
//! left its routes can never be served again, so the last of them tells whoever assembled the run
//! ([`WorkerKindLoss`]); what that does to the server is not decided here.

use std::sync::Arc;

use super::Retirement;
use crate::sync::{AtomicUsize, Mutex, OnceLock, Ordering};

/// A kind of workers has none left serving; `first` is why the first of them stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerKindLoss {
    pub kind: WorkerKind,
    pub first: Arc<str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerKind {
    PoolSync,
    PoolAsync,
    TpcThread,
}

impl std::fmt::Display for WorkerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            WorkerKind::PoolSync => "def (sync) pool worker",
            WorkerKind::PoolAsync => "async def pool worker",
            WorkerKind::TpcThread => "TPC thread worker",
        })
    }
}

/// Answers on behalf of a kind of workers, such as a queue whose workers are gone; each worker
/// holds its own [`WorkerLife`].
#[derive(Clone)]
pub struct LiveWorkers(Arc<Workers>);

/// Holds one worker's place among the serving workers of its kind. Each way out of serving takes
/// it by value, so a worker is counted as died or stopped at most once. A life dropped without
/// one, from an aborted start, counts nothing.
pub struct WorkerLife(Arc<Workers>);

struct Workers {
    kind: WorkerKind,
    counts: Arc<KindCounts>,
    first_death: OnceLock<Arc<str>>,
    tell_loss: Arc<dyn Fn(WorkerKindLoss) + Send + Sync>,
    run: Arc<WorkerCounters>,
}

/// How many workers of one kind started, died and were stopped by the server; `died` and
/// `stopped` never add up past `started`, because each life is taken once.
struct KindCounts {
    started: usize,
    died: AtomicUsize,
    stopped: AtomicUsize,
}

/// The worker counts of one server run. It holds each kind's counts but not the kind's
/// `tell_loss`, so whoever reads it keeps no stop callback alive.
pub struct WorkerCounters {
    kinds: Mutex<Vec<Arc<KindCounts>>>,
    retired: AtomicUsize,
    last_retirement: Mutex<Option<String>>,
    process_workers_died: AtomicUsize,
}

/// A run's worker counts, read at one moment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkerTally {
    pub workers_serving: usize,
    /// The workers that stopped serving on their own (their thread state could not be bound, their
    /// engine failed, they retired), not at shutdown.
    pub workers_died: usize,
    /// The `runtime="process"` workers that died (crashed, were killed, timed out) while the run
    /// served; their lane forks a replacement.
    pub process_workers_died: usize,
    /// Each retirement is a `gc.collect()` that raised or panicked.
    pub workers_retired: usize,
    pub last_worker_retirement: Option<String>,
}

impl WorkerTally {
    /// Returns how many `gc.collect()` calls failed, which is how many workers retired.
    pub fn gc_failures(&self) -> usize {
        self.workers_retired
    }
}

impl WorkerCounters {
    #[expect(
        clippy::new_without_default,
        reason = "one run builds its counts once, through `new`"
    )]
    pub fn new() -> Self {
        WorkerCounters {
            kinds: Mutex::new(Vec::new()),
            retired: AtomicUsize::new(0),
            last_retirement: Mutex::new(None),
            process_workers_died: AtomicUsize::new(0),
        }
    }

    /// Counts a `runtime="process"` worker that died while the run served.
    pub fn process_worker_died(&self) {
        self.process_workers_died.fetch_add(1, Ordering::Relaxed);
    }

    fn retired(&self, why: String) {
        self.retired.fetch_add(1, Ordering::Relaxed);
        *self.last_retirement.lock() = Some(why);
    }

    pub fn read(&self) -> WorkerTally {
        let (workers_serving, workers_died) = self.kinds.lock().iter().fold((0, 0), |(serving, died), kind| {
            let (kind_died, stopped) = (kind.died.load(Ordering::Relaxed), kind.stopped.load(Ordering::Relaxed));
            (serving + kind.started - kind_died - stopped, died + kind_died)
        });
        WorkerTally {
            workers_serving,
            workers_died,
            process_workers_died: self.process_workers_died.load(Ordering::Relaxed),
            workers_retired: self.retired.load(Ordering::Relaxed),
            last_worker_retirement: self.last_retirement.lock().clone(),
        }
    }
}

impl LiveWorkers {
    /// Returns the kind's tracker and one life per worker, counted in `run`; the last of them to
    /// die calls `tell_loss`.
    pub fn new(
        kind: WorkerKind,
        workers: usize,
        tell_loss: Arc<dyn Fn(WorkerKindLoss) + Send + Sync>,
        run: &Arc<WorkerCounters>,
    ) -> (Self, Vec<WorkerLife>) {
        let counts = Arc::new(KindCounts {
            started: workers,
            died: AtomicUsize::new(0),
            stopped: AtomicUsize::new(0),
        });
        run.kinds.lock().push(Arc::clone(&counts));
        let shared = Arc::new(Workers {
            kind,
            counts,
            first_death: OnceLock::new(),
            tell_loss,
            run: Arc::clone(run),
        });
        let lives = (0..workers).map(|_| WorkerLife(Arc::clone(&shared))).collect();
        (LiveWorkers(shared), lives)
    }

    pub(crate) fn first_death(&self) -> Option<Arc<str>> {
        self.0.first_death.get().cloned()
    }
}

impl WorkerLife {
    /// Records a worker that stopped because the server stops, which is not a death.
    pub fn stopped(self) {
        self.0.counts.stopped.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts a retirement and then records it as a death ([`Self::died`]).
    pub fn retired(self, retirement: &Retirement) -> Option<Arc<str>> {
        let why = retirement.to_string();
        self.0.run.retired(why.clone());
        self.died(why)
    }

    /// Records a worker that stopped on its own while the server runs.
    ///
    /// When it was the last of its kind to die, it tells the kind's loss and returns the first
    /// cause, for the caller to answer the requests still waiting on this kind with. A kind whose
    /// other workers were stopped by the server is not lost by this death.
    pub fn died(self, cause: String) -> Option<Arc<str>> {
        let workers = &self.0;
        let first = Arc::clone(workers.first_death.get_or_init(|| cause.into()));
        if workers.counts.died.fetch_add(1, Ordering::AcqRel) + 1 != workers.counts.started {
            return None;
        }
        (workers.tell_loss)(WorkerKindLoss {
            kind: workers.kind,
            first: Arc::clone(&first),
        });
        Some(first)
    }
}

#[cfg(test)]
#[cfg(all(sync_model, not(sync_shuttle)))]
mod loom_model;

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// The losses a `tell_loss` was told, in order.
    #[derive(Clone, Default)]
    struct Told(Arc<Mutex<Vec<WorkerKindLoss>>>);

    impl Told {
        fn tell(&self) -> Arc<dyn Fn(WorkerKindLoss) + Send + Sync> {
            let told = self.clone();
            Arc::new(move |loss| told.0.lock().expect("no test thread panicked").push(loss))
        }

        fn losses(&self) -> Vec<WorkerKindLoss> {
            self.0.lock().expect("no test thread panicked").clone()
        }
    }

    #[test]
    fn the_last_worker_of_a_kind_tells_the_loss_with_the_first_cause() {
        let told = Told::default();
        let counters = Arc::new(WorkerCounters::new());
        let (_, lives) = LiveWorkers::new(WorkerKind::TpcThread, 2, told.tell(), &counters);
        let mut lives = lives.into_iter();

        assert_eq!(
            lives
                .next()
                .expect("worker 0's life")
                .died("worker 0 retired: gc.collect() raised A".into()),
            None
        );
        assert_eq!(told.losses(), [], "one worker of two is left serving");

        let first = lives
            .next()
            .expect("worker 1's life")
            .died("worker 1 retired: gc.collect() raised B".into());
        assert_eq!(first.as_deref(), Some("worker 0 retired: gc.collect() raised A"));
        assert_eq!(
            told.losses(),
            [WorkerKindLoss {
                kind: WorkerKind::TpcThread,
                first: "worker 0 retired: gc.collect() raised A".into(),
            }]
        );
        let counted = counters.read();
        assert_eq!((counted.workers_serving, counted.workers_died), (0, 2));
    }

    #[test]
    fn a_worker_stopped_by_the_server_is_no_longer_serving_and_did_not_die() {
        let told = Told::default();
        let counters = Arc::new(WorkerCounters::new());
        let (_, lives) = LiveWorkers::new(WorkerKind::PoolSync, 3, told.tell(), &counters);
        assert_eq!(lives.len(), 3, "one life per worker");
        lives.into_iter().next().expect("a worker's life").stopped();
        let counted = counters.read();
        assert_eq!((counted.workers_serving, counted.workers_died), (2, 0));
        assert_eq!(told.losses(), []);
    }

    #[test]
    fn a_worker_that_stops_on_its_own_is_counted_as_died_and_no_longer_serving() {
        let told = Told::default();
        let counters = Arc::new(WorkerCounters::new());
        let (_, lives) = LiveWorkers::new(WorkerKind::PoolSync, 2, told.tell(), &counters);
        let mut lives = lives.into_iter();
        lives.next().expect("worker 0's life").stopped();
        assert_eq!(
            lives.next().expect("worker 1's life").died("engine failed".into()),
            None
        );
        let counted = counters.read();
        assert_eq!((counted.workers_serving, counted.workers_died), (0, 1));
        assert_eq!(told.losses(), [], "the other worker was stopped, not lost");
    }

    #[test]
    fn each_kind_tells_its_own_loss_with_its_own_first_cause() {
        let told = Told::default();
        let counters = Arc::new(WorkerCounters::new());
        let (_, sync) = LiveWorkers::new(WorkerKind::PoolSync, 1, told.tell(), &counters);
        let (_, r#async) = LiveWorkers::new(WorkerKind::PoolAsync, 1, told.tell(), &counters);
        for (lives, cause) in [(sync, "sync gone"), (r#async, "async gone")] {
            for life in lives {
                life.died(cause.into());
            }
        }
        assert_eq!(
            told.losses(),
            [
                WorkerKindLoss {
                    kind: WorkerKind::PoolSync,
                    first: "sync gone".into(),
                },
                WorkerKindLoss {
                    kind: WorkerKind::PoolAsync,
                    first: "async gone".into(),
                },
            ]
        );
    }
}
