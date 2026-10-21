//! Runs a pyre worker in one sub-interpreter.
//!
//! A worker runs the same program as main ([`program`], read on main): its bootstrap sets up
//! logging and the GC policy, then the user's script runs as a real module, and the routes it
//! registers are checked index by index against main's table ([`start`]). It then serves with a
//! request runner: [`sync_runner`] drives the hook chain from Rust, [`async_engine`] runs the
//! Python engine on its own loop. The interpreter's lifecycle belongs to `mipyre_subinterp`; a
//! runner's references belong to the worker's interpreter and are released only by
//! `SubInterpreter::end`, with its thread state current.

use mipyre_subinterp::gc::GcPolicy;
use mipyre_subinterp::SubInterpreter;
use pyo3::prelude::*;

use self::start::{WorkerSpec, WorkerStartError};
use self::sync_runner::SyncRunner;
use super::worker_app::WorkerRoutes;

pub(super) mod async_engine;
mod exec;
pub(super) mod program;
pub(super) mod start;
pub(super) mod sync_runner;

#[cfg(test)]
mod objects_per_request;

/// A worker's index among the workers built together, as its logs, errors and `WORKER_ID` name it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkerIndex(usize);

impl WorkerIndex {
    pub(crate) fn new(index: usize) -> Self {
        WorkerIndex(index)
    }

    pub(crate) fn get(self) -> usize {
        self.0
    }
}

impl std::fmt::Display for WorkerIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Runs a worker's requests in its interpreter, and releases its references with that
/// interpreter current.
pub(crate) trait RequestRunner: mipyre_subinterp::Releasable {
    /// Builds the runner last in the worker's init. `routes` and everything built before an
    /// error are `Bound`, so an error releases them at once.
    fn build(
        py: Python<'_>,
        worker: WorkerIndex,
        routes: WorkerRoutes<'_>,
        gc: GcPolicy,
    ) -> Result<Self, WorkerStartError>;
}

pub(crate) struct SubInterpreterWorker<R: RequestRunner = SyncRunner> {
    index: WorkerIndex,
    interp: SubInterpreter<R>,
}

impl<R: RequestRunner> SubInterpreterWorker<R> {
    /// Creates a sub-interpreter on the calling thread, which then serves it, and runs
    /// [`start::prepare`] in it; on failure the interpreter is ended before the error returns. Another
    /// thread state current on this thread aborts the process.
    pub(crate) fn create(
        worker: WorkerIndex,
        spec: &WorkerSpec<'_>,
    ) -> Result<Self, WorkerStartError> {
        SubInterpreter::create_on_this_thread(
            worker.to_string(),
            |source| WorkerStartError::CreationFailed { worker, source },
            |py| start::prepare::<R>(py, worker, spec),
        )
        .map(|interp| SubInterpreterWorker {
            index: worker,
            interp,
        })
    }

    pub(crate) fn worker_id(&self) -> WorkerIndex {
        self.index
    }

    pub(crate) fn end_on_own_thread(self) {
        self.interp.end_on_own_thread()
    }

    fn with_gil<T>(
        &mut self,
        f: impl for<'py> FnOnce(&mut R, Python<'py>) -> T,
    ) -> std::thread::Result<T> {
        self.interp.with_gil(f)
    }
}
