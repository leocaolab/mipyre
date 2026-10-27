//! Runs a worker's async engine (`_worker/async_engine.py`): it takes the handlers and hooks from the
//! app itself and serves on its own loop, fed by an [`EngineInbox`].

use std::collections::VecDeque;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyTuple;

use super::exec::{bundled, exec_in, new_module, ExtensionLoader};
use super::start::WorkerStartError;
use super::{RequestRunner, SubInterpreterWorker, WorkerIndex};
use crate::engine_queue::{EngineInbox, EngineJob};
use crate::exchange::error::PyException;
use crate::exchange::loop_wait::LoopMailbox;
use crate::websocket::WsConnection;
use crate::worker_api::AsyncInbox;
use crate::worker_api::ConnectionInbox;
use crate::worker_app::WorkerRoutes;
use mipyre_subinterp::gc::GcPolicy;
use mipyre_util::panic_message;

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
    /// Runs the async engine until its queue closes or the engine stops, and returns what it ended
    /// with (its exception with its traceback, or a Rust panic's payload) and the jobs still queued
    /// when it was the last engine to stop.
    pub(crate) fn run_async_engine(
        &mut self,
        inbox: EngineInbox,
        connections: Option<Arc<LoopMailbox<WsConnection>>>,
    ) -> (Result<(), AsyncEngineError>, VecDeque<EngineJob>) {
        let worker = self.worker_id();
        self.with_gil(|_, py| run_engine(py, worker, inbox, connections))
            .unwrap_or_else(|payload| {
                let panicked = AsyncEngineError::Panicked {
                    worker,
                    payload: panic_message(&*payload),
                };
                (Err(panicked), VecDeque::new())
            })
    }
}

fn run_engine(
    py: Python<'_>,
    worker: WorkerIndex,
    inbox: EngineInbox,
    connections: Option<Arc<LoopMailbox<WsConnection>>>,
) -> (Result<(), AsyncEngineError>, VecDeque<EngineJob>) {
    let routes = match crate::worker_api::worker_engine_routes(py, connections) {
        Ok(routes) => routes,
        Err(e) => {
            let exception = PyException::capture(py, &e);
            return (Err(AsyncEngineError::ModuleSetup { worker, exception }), inbox.leave());
        }
    };
    let (ended, left) = run_engine_module(py, inbox, routes);
    let ended = ended.map_err(|failure| match failure {
        EngineFailure::Setup(exception) => AsyncEngineError::ModuleSetup { worker, exception },
        EngineFailure::Raised(exception) => AsyncEngineError::RunRaised { worker, exception },
    });
    (ended, left)
}

/// The handlers an async engine serves (`None` for one it does not serve), the app's hooks, and
/// whether a handler may answer with a `Stream`, which only main can serve.
pub(crate) struct EngineRoutes {
    pub(crate) handlers: Vec<Option<Py<PyAny>>>,
    pub(crate) before: Vec<Py<PyAny>>,
    pub(crate) after: Vec<Py<PyAny>>,
    pub(crate) streams: bool,
    /// Is `None` for an engine that serves no WebSocket route.
    pub(crate) websockets: Option<EngineWebSockets>,
}

/// The WebSocket handlers an engine serves, indexed by route (`None` for one it does not serve),
/// and where their connections are handed to it.
pub(crate) struct EngineWebSockets {
    pub(crate) handlers: Vec<Option<Py<PyAny>>>,
    pub(crate) connections: Arc<LoopMailbox<WsConnection>>,
}

/// How an async engine ended other than by its inbox closing.
#[derive(Debug)]
pub(crate) enum EngineFailure {
    /// Its module could not be made.
    Setup(PyException),
    /// It raised while it served.
    Raised(PyException),
}

/// Runs `_worker/async_engine.py` on this thread's interpreter, serving `routes` until `inbox`'s
/// queue closes or the engine stops, then leaves the queue and returns the jobs still queued when
/// this was the last engine.
pub(crate) fn run_engine_module(
    py: Python<'_>,
    inbox: EngineInbox,
    routes: EngineRoutes,
) -> (Result<(), EngineFailure>, VecDeque<EngineJob>) {
    let inbox = match Py::new(py, AsyncInbox(inbox)) {
        Ok(inbox) => inbox,
        // The inbox was dropped with the failed allocation, and left the queue then.
        Err(e) => return (Err(EngineFailure::Setup(PyException::capture(py, &e))), VecDeque::new()),
    };
    let ended = run_module(py, &inbox, routes);
    (ended, inbox.get().0.leave())
}

fn run_module(py: Python<'_>, inbox: &Py<AsyncInbox>, routes: EngineRoutes) -> Result<(), EngineFailure> {
    let setup = |e: PyErr| EngineFailure::Setup(PyException::capture(py, &e));
    let EngineRoutes {
        handlers,
        before,
        after,
        streams,
        websockets,
    } = routes;
    let websockets = match websockets {
        Some(EngineWebSockets { handlers, connections }) => {
            Some((Py::new(py, ConnectionInbox(connections)).map_err(setup)?, handlers))
        }
        None => None,
    };
    let module = new_module(py, ASYNC_ENGINE_MODULE, None)
        .and_then(|m| {
            m.setattr("INBOX", inbox.clone_ref(py))?;
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
            m.setattr("WEBSOCKETS", websockets)?;
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
