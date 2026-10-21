//! Runs a worker's async engine (`_async_engine.py`): it takes the handlers and hooks from the
//! app itself and serves on its own loop, fed by an [`AsyncInbox`].

use pyo3::prelude::*;

use super::exec::{bundled, exec_in, new_module};
use super::start::WorkerStartError;
use super::{RequestRunner, SubInterpreterWorker, WorkerIndex};
use crate::exchange::error::PyException;
use crate::runtime::worker_api::AsyncInbox;
use crate::runtime::worker_app::WorkerRoutes;
use mipyre_subinterp::gc::GcPolicy;
use mipyre_tpc::panic_message;

const ASYNC_ENGINE_MODULE: &std::ffi::CStr = c"__mipyre_async_engine__";

/// A runner for the async engine, which holds nothing here.
pub(crate) struct AsyncEngine;

impl RequestRunner for AsyncEngine {
    fn build(
        _py: Python<'_>,
        _worker: WorkerIndex,
        _routes: WorkerRoutes<'_>,
        _gc: GcPolicy,
    ) -> Result<Self, WorkerStartError> {
        Ok(AsyncEngine)
    }
}

impl mipyre_subinterp::Releasable for AsyncEngine {
    fn release(self, _py: Python<'_>) {}
}

/// The engine runs for the worker's whole life, so an exception it ends with happened at run
/// time, not while the worker started.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AsyncEngineError {
    #[error("worker {worker}: creating the async engine module failed: {exception}\n{}", .exception.traceback())]
    ModuleSetup {
        worker: WorkerIndex,
        exception: PyException,
    },
    #[error("worker {worker}: the async engine stopped: {exception}\n{}", .exception.traceback())]
    RunRaised {
        worker: WorkerIndex,
        exception: PyException,
    },
    #[error("worker {worker}: a Rust panic in the async engine: {payload}")]
    Panicked {
        worker: WorkerIndex,
        payload: String,
    },
}

impl SubInterpreterWorker<AsyncEngine> {
    /// Runs the async engine until `inbox` closes or the engine stops, and returns what it ended
    /// with: its exception with its traceback, or a Rust panic's payload.
    pub(crate) fn run_async_engine(
        &mut self,
        inbox: AsyncInbox,
    ) -> Result<(), AsyncEngineError> {
        let worker = self.worker_id();
        self.with_gil(|_, py| run_engine(py, worker, inbox))
            .unwrap_or_else(|payload| {
                Err(AsyncEngineError::Panicked {
                    worker,
                    payload: panic_message(&*payload),
                })
            })
    }
}

fn run_engine(
    py: Python<'_>,
    worker: WorkerIndex,
    inbox: AsyncInbox,
) -> Result<(), AsyncEngineError> {
    let setup = |e: PyErr| AsyncEngineError::ModuleSetup {
        worker,
        exception: PyException::capture(py, &e),
    };
    let engine = new_module(py, ASYNC_ENGINE_MODULE, None)
        .and_then(|m| {
            let task_timeout = inbox.task_budget().as_secs_f64();
            m.setattr("WORKER_ID", worker.get())?;
            m.setattr("CHANNEL", Bound::new(py, inbox)?)?;
            m.setattr("TASK_TIMEOUT", task_timeout)?;
            Ok(m)
        })
        .map_err(setup)?;
    exec_in(py, &bundled!("mipyre/_async_engine.py"), &engine).map_err(|e| {
        AsyncEngineError::RunRaised {
            worker,
            exception: PyException::capture(py, &e.raised),
        }
    })
}
