//! Counts the workers still serving and stops the server once a kind has none left.
//!
//! A worker that stops on its own is not rebuilt; the others take its share. When a kind has none
//! left its routes can never be served again, so the server stops with the first cause rather
//! than answer 503 forever, and `run()` raises it so the process exits non-zero for its
//! supervisor to restart.

use std::sync::Arc;

use mipyre_tpc::{Cancellable, RunStop};
use tokio_util::sync::CancellationToken;

use super::Retirement;
use crate::sync::{AtomicUsize, OnceLock, Ordering};
use crate::telemetry::RunCounters;

/// Why a server stopped, for the reasons the server tells its run's stop ([`RunStop`]) beyond its
/// threads': each part that stops the server records its own here as it stops it. It lives here,
/// below the lanes and the servers, because the workers here stop the server too.
#[derive(Clone, Debug)]
pub(crate) enum ServerStop {
    /// A kind of workers has none left serving, so its routes can never be served again; `first`
    /// is why the first of them stopped.
    NoWorkerLeft { kind: WorkerKind, first: Arc<str> },
    /// The process lanes failed in a way they cannot serve past (`lane::LaneRunError`, which
    /// sits above this layer).
    LanesFailed(Arc<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for ServerStop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerStop::NoWorkerLeft { kind, first } => {
                write!(
                    f,
                    "the server stopped: no {kind} is left serving; the first stopped: {first}"
                )
            }
            ServerStop::LanesFailed(why) => write!(f, "the process lanes failed, so the server stopped: {why}"),
        }
    }
}

/// A server run's stop, with the server's own causes.
pub(crate) type ServerRunStop<S = CancellationToken> = RunStop<ServerStop, S>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WorkerKind {
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
pub(crate) struct LiveWorkers<S = CancellationToken>(Arc<Workers<S>>);

/// Holds one worker's place among the serving workers of its kind. Each way out of serving takes
/// it by value, so a kind's serving count drops at most once per worker it started with. A life
/// dropped without one, from an aborted start, counts nothing.
pub(crate) struct WorkerLife<S = CancellationToken>(Arc<Workers<S>>);

struct Workers<S> {
    kind: WorkerKind,
    serving: AtomicUsize,
    first_death: OnceLock<Arc<str>>,
    stop: ServerRunStop<S>,
    counters: Arc<RunCounters>,
}

impl<S> LiveWorkers<S> {
    /// Returns the kind's tracker and one life per worker; the last of them to die stops `stop`.
    pub(crate) fn new(
        kind: WorkerKind,
        workers: usize,
        stop: ServerRunStop<S>,
        counters: Arc<RunCounters>,
    ) -> (Self, Vec<WorkerLife<S>>) {
        counters.workers_started(workers);
        let shared = Arc::new(Workers {
            kind,
            serving: AtomicUsize::new(workers),
            first_death: OnceLock::new(),
            stop,
            counters,
        });
        let lives = (0..workers).map(|_| WorkerLife(Arc::clone(&shared))).collect();
        (LiveWorkers(shared), lives)
    }

    pub(crate) fn first_death(&self) -> Option<Arc<str>> {
        self.0.first_death.get().cloned()
    }
}

impl<S: Cancellable> WorkerLife<S> {
    /// Records a worker that stopped because the server stops, which is not a death.
    pub(crate) fn stopped(self) {
        self.0.counters.worker_stopped(false);
    }

    /// Counts a retirement and then records it as a death ([`Self::died`]).
    pub(crate) fn retired(self, retirement: &Retirement) -> Option<Arc<str>> {
        let why = retirement.to_string();
        self.0.counters.worker_retired(why.clone());
        self.died(why)
    }

    /// Records a worker that stopped on its own while the server runs.
    ///
    /// When it was the last of its kind, it stops the run and returns the first cause, for the
    /// caller to answer the requests still waiting on this kind with.
    pub(crate) fn died(self, cause: String) -> Option<Arc<str>> {
        let workers = &self.0;
        workers.counters.worker_stopped(true);
        let first = Arc::clone(workers.first_death.get_or_init(|| cause.into()));
        if workers.serving.fetch_sub(1, Ordering::AcqRel) != 1 {
            return None;
        }
        let cause = ServerStop::NoWorkerLeft {
            kind: workers.kind,
            first: Arc::clone(&first),
        };
        tracing::error!(target: "mipyre::server", error = %cause, "the server stops");
        workers.stop.stop(cause);
        Some(first)
    }
}

#[cfg(test)]
#[cfg(all(sync_model, not(sync_shuttle)))]
mod loom_model;

#[cfg(test)]
mod tests {
    use super::*;
    use mipyre_tpc::StopCause;

    /// Returns the first worker's cause if `stop` was stopped because no worker of `kind` is left.
    fn no_worker_left(stop: &ServerRunStop, kind: WorkerKind) -> Option<Arc<str>> {
        match stop.cause() {
            Some(StopCause::Caller(ServerStop::NoWorkerLeft { kind: stopped, first })) if stopped == kind => {
                Some(first)
            }
            _ => None,
        }
    }

    #[test]
    fn the_last_worker_of_a_kind_stops_the_run_with_the_first_cause() {
        let stop = ServerRunStop::default();
        let counters = Arc::new(RunCounters::new(false));
        let (_, lives) = LiveWorkers::new(WorkerKind::TpcThread, 2, stop.clone(), Arc::clone(&counters));
        let mut lives = lives.into_iter();

        assert_eq!(
            lives
                .next()
                .expect("worker 0's life")
                .died("worker 0 retired: gc.collect() raised A".into()),
            None
        );
        assert!(!stop.is_stopped(), "one worker of two is left serving");
        assert!(stop.cause().is_none());

        let first = lives
            .next()
            .expect("worker 1's life")
            .died("worker 1 retired: gc.collect() raised B".into());
        assert_eq!(first.as_deref(), Some("worker 0 retired: gc.collect() raised A"));
        assert!(stop.is_stopped());
        let cause =
            no_worker_left(&stop, WorkerKind::TpcThread).expect("the run was stopped for no TPC thread worker left");
        assert_eq!(&*cause, "worker 0 retired: gc.collect() raised A");
        let counted = counters.read();
        assert_eq!((counted.workers_serving, counted.workers_died), (0, 2));
    }

    #[test]
    fn a_worker_stopped_by_the_server_is_no_longer_serving_and_did_not_die() {
        let counters = Arc::new(RunCounters::new(false));
        let (_, lives) = LiveWorkers::new(
            WorkerKind::PoolSync,
            3,
            <ServerRunStop>::default(),
            Arc::clone(&counters),
        );
        assert_eq!(lives.len(), 3, "one life per worker");
        lives.into_iter().next().expect("a worker's life").stopped();
        let counted = counters.read();
        assert_eq!((counted.workers_serving, counted.workers_died), (2, 0));
    }

    #[test]
    fn the_first_fatal_cause_stands() {
        let stop = ServerRunStop::default();
        let counters = Arc::new(RunCounters::new(false));
        let (_, sync) = LiveWorkers::new(WorkerKind::PoolSync, 1, stop.clone(), Arc::clone(&counters));
        let (_, r#async) = LiveWorkers::new(WorkerKind::PoolAsync, 1, stop.clone(), counters);
        for (lives, cause) in [(sync, "sync gone"), (r#async, "async gone")] {
            for life in lives {
                life.died(cause.into());
            }
        }
        assert_eq!(
            no_worker_left(&stop, WorkerKind::PoolSync).as_deref(),
            Some("sync gone"),
            "{:?}",
            stop.cause()
        );
    }
}
