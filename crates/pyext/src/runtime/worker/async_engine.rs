//! Runs a worker's async engine (`_worker/async_engine.py`): it takes the handlers and hooks from the
//! app itself and serves on its own loop, fed by an [`AsyncInbox`].

use pyo3::prelude::*;
use pyo3::types::PyTuple;

use super::exec::{bundled, exec_in, new_module, ExtensionLoader};
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
    Panicked { worker: WorkerIndex, payload: String },
}

impl SubInterpreterWorker<AsyncEngine> {
    /// Runs the async engine until `inbox` closes or the engine stops, and returns what it ended
    /// with: its exception with its traceback, or a Rust panic's payload.
    pub(crate) fn run_async_engine(&mut self, inbox: AsyncInbox) -> Result<(), AsyncEngineError> {
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

fn run_engine(py: Python<'_>, worker: WorkerIndex, inbox: AsyncInbox) -> Result<(), AsyncEngineError> {
    let routes = crate::runtime::worker_api::worker_engine_routes(py).map_err(|e| AsyncEngineError::ModuleSetup {
        worker,
        exception: PyException::capture(py, &e),
    })?;
    run_engine_module(py, &format!("worker-{}", worker.get()), inbox, routes).map_err(|failure| match failure {
        EngineFailure::Setup(exception) => AsyncEngineError::ModuleSetup { worker, exception },
        EngineFailure::Raised(exception) => AsyncEngineError::RunRaised { worker, exception },
    })
}

/// The handlers an async engine serves (`None` for one it does not serve), the app's hooks, and
/// whether a handler may answer with a `Stream`, which only main can serve.
pub(crate) struct EngineRoutes {
    pub(crate) handlers: Vec<Option<Py<PyAny>>>,
    pub(crate) before: Vec<Py<PyAny>>,
    pub(crate) after: Vec<Py<PyAny>>,
    pub(crate) streams: bool,
}

/// How an async engine ended other than by its inbox closing.
#[derive(Debug)]
pub(crate) enum EngineFailure {
    /// Its module could not be made.
    Setup(PyException),
    /// It raised while it served.
    Raised(PyException),
}

/// Runs `_worker/async_engine.py` on this thread's interpreter as the engine `engine`, serving `routes`
/// until `inbox` closes or the engine stops.
pub(crate) fn run_engine_module(
    py: Python<'_>,
    engine: &str,
    inbox: AsyncInbox,
    routes: EngineRoutes,
) -> Result<(), EngineFailure> {
    let setup = |e: PyErr| EngineFailure::Setup(PyException::capture(py, &e));
    let EngineRoutes {
        handlers,
        before,
        after,
        streams,
    } = routes;
    let module = new_module(py, ASYNC_ENGINE_MODULE, None)
        .and_then(|m| {
            let task_timeout = inbox.task_budget().as_secs_f64();
            m.setattr("ENGINE", engine)?;
            m.setattr("CHANNEL", Bound::new(py, inbox)?)?;
            m.setattr("TASK_TIMEOUT", task_timeout)?;
            m.setattr("HANDLERS", handlers)?;
            m.setattr("BEFORE_HOOKS", before)?;
            m.setattr("AFTER_HOOKS", after)?;
            let stream = py.get_type::<crate::exchange::stream::ResponseStream>();
            let unhooked = if streams {
                PyTuple::new(py, [stream])?
            } else {
                PyTuple::empty(py)
            };
            m.setattr("UNHOOKED", unhooked)?;
            Ok(m)
        })
        .map_err(setup)?;
    exec_in(
        py,
        &bundled!("mipyre/_worker/async_engine.py"),
        &module,
        ExtensionLoader::CPython,
    )
    .map_err(|e| EngineFailure::Raised(PyException::capture(py, &e.raised)))
}
