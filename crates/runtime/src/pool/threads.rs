//! Holds and joins a pool's worker threads, and names the ones a shutdown or the watchdog
//! abandons.

use std::sync::Arc;
use std::time::{Duration, Instant};

use mipyre_subinterp::PYTHON_THREAD_STACK;
use mipyre_tpc::{OsThread, ShutdownDeadline, ThreadJoinError};

use pyo3::prelude::*;

use super::watchdog::{Supervision, SyncLane};

/// Builds a pool thread with the stack C extensions expect, since it runs Python.
pub(super) fn python_thread(name: String) -> std::thread::Builder {
    std::thread::Builder::new().name(name).stack_size(PYTHON_THREAD_STACK)
}

/// A worker thread still running after a pool shutdown's grace period. Its interpreter is still
/// alive, and finalizing with a live sub-interpreter aborts, so `Mipyre.run()` exits non-zero
/// instead.
#[pyclass(module = "mipyre.engine", frozen)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbandonedWorker {
    #[pyo3(get)]
    pub thread: String,
    pub route: Option<mipyre_http::plan::route::RouteKey>,
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

/// A pool worker's thread.
pub(super) struct WorkerThread {
    pub(super) handle: OsThread<()>,
    /// It is the sync lane the thread serves, which says the route it is running; an async
    /// worker's thread has none.
    pub(super) lane: Option<Arc<SyncLane>>,
}

/// Holds a pool's threads until its server stops. Sync workers' threads live in their
/// [`Supervision`] lanes because the watchdog replaces them; async workers' threads are here.
pub struct PoolThreads {
    pub(super) threads: Vec<WorkerThread>,
    pub(super) sup: Arc<Supervision>,
    /// Dropping it stops the watchdog.
    pub(super) stop: crossbeam_channel::Sender<()>,
}

/// Joins the threads of a pool whose start failed, `threads` and the sync lanes', by
/// `shutdown_timeout`, and returns the ones still running then. Their workers end because the
/// start was aborted; one that has not is still running Python in its interpreter, so the
/// process must not finalize while it lives.
#[must_use]
pub(super) fn join_failed_start(
    mut threads: PoolThreads,
    sync_lanes: &[Arc<SyncLane>],
    shutdown_timeout: Duration,
) -> Vec<AbandonedWorker> {
    threads
        .threads
        .extend(sync_lanes.iter().filter_map(|lane| lane.take_thread()));
    threads.join(&ShutdownDeadline::new(shutdown_timeout))
}

impl PoolThreads {
    /// Waits until `deadline` for the worker threads to end, and returns the ones still running
    /// together with those the watchdog abandoned. Every [`InterpreterPool`] reference must be
    /// dropped first, or the workers never finish.
    ///
    /// A handler can block forever (a `requests.get` with no timeout), so a thread still running
    /// is leaked instead of awaited, and the caller must not finalize the runtime while it lives.
    pub fn join(self, deadline: &ShutdownDeadline) -> Vec<AbandonedWorker> {
        let PoolThreads { threads, sup, stop } = self;
        drop(stop);
        let mut to_join = threads;
        to_join.extend(sup.close());
        let mut abandoned: Vec<AbandonedWorker> = Vec::new();
        let deadline_at = deadline.fix(Instant::now());
        for WorkerThread { handle, lane } in to_join {
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
                    let route = lane.as_deref().and_then(SyncLane::running_route);
                    let abandoned_worker = sup.abandoned_worker(thread, route);
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
#[cfg(not(sync_model))]
mod tests {
    use super::*;
    use mipyre_http::plan::route::RouteKey;
    use mipyre_tpc::spawn;

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

    fn thread_running(name: &str, release: crossbeam_channel::Receiver<()>, route: Option<usize>) -> WorkerThread {
        let handle = spawn(std::thread::Builder::new().name(name.to_string()), move || {
            release.recv().unwrap_err();
        })
        .unwrap();
        let lane = SyncLane::new(0, None);
        if let Some(route) = route {
            lane.begin(route);
        }
        WorkerThread {
            handle,
            lane: Some(lane),
        }
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

    #[test]
    fn a_failed_start_returns_the_threads_still_running_to_its_caller() {
        let (release, blocked) = crossbeam_channel::bounded::<()>(0);
        let (_done, finished) = crossbeam_channel::bounded::<()>(0);
        drop(_done);
        let threads = pool_threads(
            vec![
                ended(thread_running("w-ended", finished, None)),
                thread_running("w-stuck", blocked, None),
            ],
            Vec::new(),
        );
        let abandoned = join_failed_start(threads, &[], Duration::ZERO);
        assert_eq!(
            abandoned,
            vec![AbandonedWorker {
                thread: "w-stuck".into(),
                route: None,
            }]
        );
        drop(release);
    }

    #[test]
    fn a_failed_start_whose_threads_all_ended_returns_none() {
        let (_done, finished) = crossbeam_channel::bounded::<()>(0);
        drop(_done);
        let threads = pool_threads(vec![ended(thread_running("w-ended", finished, None))], Vec::new());
        assert_eq!(join_failed_start(threads, &[], Duration::ZERO), vec![]);
    }
}
