//! Holds and joins a pool's worker threads, and names the ones a shutdown or the watchdog
//! abandons.

use std::sync::Arc;
use std::time::{Duration, Instant};

use mipyre_subinterp::PYTHON_THREAD_STACK;
use mipyre_tpc::{OsThread, ShutdownDeadline, ThreadJoinError};

use pyo3::prelude::*;

use super::watchdog::{Supervision, SyncLane};
use crate::sync::{AtomicU64, Ordering};

/// Builds a pool thread with the stack C extensions expect, since it runs Python.
pub(super) fn python_thread(name: String) -> std::thread::Builder {
    std::thread::Builder::new()
        .name(name)
        .stack_size(PYTHON_THREAD_STACK)
}

/// A worker thread still running after a pool shutdown's grace period. Its interpreter is still
/// alive, and finalizing with a live sub-interpreter aborts, so `Mipyre.run()` exits non-zero
/// instead.
#[pyclass(module = "mipyre.engine", frozen)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AbandonedWorker {
    #[pyo3(get)]
    pub(crate) thread: String,
    pub(crate) route: Option<mipyre_http::plan::route::RouteKey>,
}

#[pymethods]
impl AbandonedWorker {
    /// Returns the route it was running as `METHOD path`, or `None`.
    #[getter]
    fn route(&self) -> Option<String> {
        self.route.as_ref().map(ToString::to_string)
    }

    fn __str__(&self) -> String {
        self.to_string()
    }

    fn __repr__(&self) -> String {
        format!("AbandonedWorker({self})")
    }
}

impl std::fmt::Display for AbandonedWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.route {
            Some(route) => write!(f, "{} (running {route})", self.thread),
            None => f.write_str(&self.thread),
        }
    }
}

/// What a worker is running, as the number of the route it serves, or nothing. The worker writes
/// it, and the shutdown reads it when it abandons the worker.
///
/// It is one atomic word with idle stored as `IDLE`, so a reader never pairs a flag with a
/// number from different steps, and `Relaxed` suffices because no other write needs ordering
/// with it. The word has its cache line to itself because the worker writes it on every request.
#[derive(Clone, Debug)]
pub(super) struct RunningSlot(Arc<mipyre_cpu::CacheLine<AtomicU64>>);

/// The value of an idle [`RunningSlot`]; no `u32` converts to it.
const IDLE: u64 = u64::MAX;

impl RunningSlot {
    pub(super) fn idle() -> Self {
        Self(Arc::new(mipyre_cpu::CacheLine(AtomicU64::new(IDLE))))
    }

    pub(super) fn start(&self, number: u32) {
        self.0 .0.store(u64::from(number), Ordering::Relaxed);
    }

    pub(super) fn finish(&self) {
        self.0 .0.store(IDLE, Ordering::Relaxed);
    }

    pub(super) fn current(&self) -> Option<u32> {
        u32::try_from(self.0 .0.load(Ordering::Relaxed)).ok()
    }
}

/// A pool worker's thread with what it is running.
pub(super) struct WorkerThread {
    pub(super) handle: OsThread<()>,
    pub(super) running: RunningSlot,
}

/// Holds a pool's threads until its server stops. Sync workers' threads live in their
/// [`Supervision`] lanes because the watchdog replaces them; async workers' threads are here, with
/// `WorkerThread::running` left idle.
pub(crate) struct PoolThreads {
    pub(super) threads: Vec<WorkerThread>,
    pub(super) sup: Arc<Supervision>,
    /// Dropping it stops the watchdog.
    pub(super) stop: crossbeam_channel::Sender<()>,
}

/// Joins the threads of a pool whose start failed, `threads` and the sync lanes', by
/// `shutdown_timeout`; their workers end because the start was aborted, and one still running then
/// is logged.
pub(super) fn join_failed_start(mut threads: PoolThreads, sync_lanes: &[Arc<SyncLane>], shutdown_timeout: Duration) {
    threads.threads.extend(sync_lanes.iter().filter_map(|lane| lane.take_thread()));
    for abandoned in threads.join(&ShutdownDeadline::new(shutdown_timeout)) {
        tracing::error!(
            target: "mipyre::server",
            "worker thread {abandoned} did not stop after a failed pool start"
        );
    }
}

impl PoolThreads {
    /// Waits until `deadline` for the worker threads to end, and returns the ones still running
    /// together with those the watchdog abandoned. Every [`InterpreterPool`] reference must be
    /// dropped first, or the workers never finish.
    ///
    /// A handler can block forever (a `requests.get` with no timeout), so a thread still running
    /// is leaked instead of awaited, and the caller must not finalize the runtime while it lives.
    pub(crate) fn join(self, deadline: &ShutdownDeadline) -> Vec<AbandonedWorker> {
        let PoolThreads { threads, sup, stop } = self;
        drop(stop);
        let mut to_join = threads;
        to_join.extend(sup.close());
        let mut abandoned: Vec<AbandonedWorker> = Vec::new();
        let deadline_at = deadline.fix(Instant::now());
        for WorkerThread { handle, running } in to_join {
            let thread = handle.name();
            match handle.join_by(deadline_at) {
                Ok(()) => {}
                Err(ThreadJoinError::Panicked(message)) => tracing::error!(
                    target: "mipyre::server",
                    thread = %thread,
                    message = %message,
                    "worker thread panicked during shutdown",
                ),
                Err(ThreadJoinError::StillRunning) => {
                    let abandoned_worker = sup.abandoned_worker(thread, &running);
                    tracing::error!(
                        target: "mipyre::server",
                        "worker thread {abandoned_worker} did not exit within {:?}; abandoning it",
                        deadline.grace(),
                    );
                    abandoned.push(abandoned_worker);
                }
            }
        }
        abandoned.extend(sup.take_abandoned());
        abandoned
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mipyre_http::plan::route::RouteKey;
    use mipyre_tpc::spawn;

    #[test]
    fn a_running_slot_holds_every_work_number_and_nothing_when_idle() {
        let slot = RunningSlot::idle();
        assert_eq!(slot.current(), None);
        slot.start(u32::MAX);
        assert_eq!(slot.current(), Some(u32::MAX));
        slot.start(0);
        assert_eq!(slot.current(), Some(0));
        slot.finish();
        assert_eq!(slot.current(), None);
    }

    #[test]
    fn a_slot_read_during_restarts_is_always_a_value_the_writer_stored() {
        const ROUNDS: u32 = 20_000;
        let slot = RunningSlot::idle();
        let writer = slot.clone();
        let writing = std::thread::spawn(move || {
            for _ in 0..ROUNDS {
                writer.start(111);
                writer.finish();
                writer.start(222);
                writer.finish();
            }
        });
        for _ in 0..ROUNDS {
            match slot.current() {
                None | Some(111) | Some(222) => {}
                other => panic!("the slot read {other:?}, which the writer never stored"),
            }
        }
        writing.join().expect("the writer ends");
    }

    /// Abandons a running worker at once; tests wait for a thread's end with [`ended`] rather
    /// than for a time.
    fn no_wait() -> ShutdownDeadline {
        ShutdownDeadline::new(Duration::ZERO)
    }

    /// Returns `thread` once it has finished.
    fn ended(thread: WorkerThread) -> WorkerThread {
        while !thread.handle.is_finished() {
            std::thread::yield_now();
        }
        thread
    }

    fn get(path: &str) -> mipyre_http::plan::route::RouteKey {
        mipyre_http::plan::route::RouteKey {
            method: hyper::Method::GET,
            path: path.into(),
        }
    }

    fn thread_running(
        name: &str,
        release: crossbeam_channel::Receiver<()>,
        route: Option<usize>,
    ) -> WorkerThread {
        let handle = spawn(std::thread::Builder::new().name(name.to_string()), move || {
            release.recv().unwrap_err();
        })
        .unwrap();
        let running = RunningSlot::idle();
        if let Some(route) = route {
            running.start(u32::try_from(route).unwrap());
        }
        WorkerThread { handle, running }
    }

    fn pool_threads(threads: Vec<WorkerThread>, routes: Vec<RouteKey>) -> PoolThreads {
        let (stop, _stopped) = crossbeam_channel::bounded::<()>(0);
        PoolThreads {
            threads,
            sup: Supervision::new(routes),
            stop,
        }
    }

    #[test]
    fn join_returns_the_abandoned_workers_to_its_caller() {
        let (release, blocked) = crossbeam_channel::bounded::<()>(0);
        let (_done, finished) = crossbeam_channel::bounded::<()>(0);
        drop(_done);
        let threads = pool_threads(
            vec![
                ended(thread_running("w-finished", finished, None)),
                thread_running("w-stuck", blocked, Some(1)),
            ],
            vec![get("/a"), get("/stuck")],
        );
        let abandoned = threads.join(&no_wait());
        assert_eq!(
            abandoned,
            vec![AbandonedWorker {
                thread: "w-stuck".into(),
                route: Some(get("/stuck")),
            }]
        );
        assert_eq!(abandoned[0].to_string(), "w-stuck (running GET /stuck)");
        drop(release);
    }

    #[test]
    fn two_pools_report_their_own_abandoned_workers() {
        // Nothing is shared through a process-wide list, so one server's stuck worker never
        // reaches another's run.
        let (release, blocked) = crossbeam_channel::bounded::<()>(0);
        let stuck = pool_threads(vec![thread_running("pool-a", blocked, None)], Vec::new());
        let (_done, finished) = crossbeam_channel::bounded::<()>(0);
        drop(_done);
        let clean = pool_threads(vec![ended(thread_running("pool-b", finished, None))], Vec::new());
        assert_eq!(clean.join(&no_wait()), vec![]);
        assert_eq!(stuck.join(&no_wait()).len(), 1);
        drop(release);
    }


}
