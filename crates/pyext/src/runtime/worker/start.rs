//! Starts one worker in its new interpreter: the bootstrap, the user's script, the routes it
//! registered checked against main's table, then the runner; and the errors a start fails with.

use std::ffi::CStr;
use std::path::{Path, PathBuf};

use mipyre_subinterp::gc::GcPolicy;
use mipyre_subinterp::NewInterpreterError;
use pyo3::exceptions::{PyImportError, PyModuleNotFoundError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyModule};

use super::exec::{bundled, exec_in, new_module, ExecFailure, PythonSource, ScriptFailure};
use super::program::WorkerProgram;
use super::{RequestRunner, WorkerIndex};
use crate::exchange::error::PyException;
use crate::runtime::worker_app::{self, WorkerRoutes};
use mipyre_http::plan::route::RouteManifest;

/// The script's module in a worker is not `__main__`, so a script's
/// `if __name__ == "__main__": app.run()` does not run in workers.
const SCRIPT_MODULE: &CStr = c"__mipyre_worker__";
const BOOTSTRAP_MODULE: &CStr = c"__mipyre_bootstrap__";

pyo3::create_exception!(
    mipyre.engine,
    WorkerException,
    pyo3::exceptions::PyException,
    "An exception a sub-interpreter worker raised, as the traceback it printed there: the \
     exception object itself belongs to the worker's interpreter. The `__cause__` of the \
     error a failed worker start raises on the main interpreter."
);

/// The classes main re-raises a worker's start failure as; anything else is a `RuntimeError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExceptionClass {
    ModuleNotFound,
    Import,
    Other,
}

impl ExceptionClass {
    fn of(py: Python<'_>, err: &PyErr) -> Self {
        if err.is_instance_of::<PyModuleNotFoundError>(py) {
            ExceptionClass::ModuleNotFound
        } else if err.is_instance_of::<PyImportError>(py) {
            ExceptionClass::Import
        } else {
            ExceptionClass::Other
        }
    }
}

/// A Python exception a worker raised while starting, kept as text because the object belongs to
/// the worker's interpreter.
#[derive(Debug, thiserror::Error)]
#[error("{exception}")]
pub(crate) struct StartException {
    exception: PyException,
    class: ExceptionClass,
}

impl StartException {
    pub(super) fn capture(py: Python<'_>, err: &PyErr) -> Self {
        StartException {
            exception: PyException::capture(py, err),
            class: ExceptionClass::of(py, err),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WorkerStartError {
    #[error("worker {worker}: its sub-interpreter could not be created: {source}")]
    CreationFailed {
        worker: WorkerIndex,
        source: NewInterpreterError,
    },
    #[error(
        "worker {worker}: {} raised while the worker started: {raised}{}",
        .file.display(),
        .failure.as_ref().map(ToString::to_string).unwrap_or_default()
    )]
    ScriptRaised {
        worker: WorkerIndex,
        file: Box<Path>,
        raised: StartException,
        /// It is boxed because the explanation text would make every start error large.
        failure: Option<Box<ScriptFailure>>,
    },
    #[error("worker {worker}: {step} failed: {raised}")]
    SetupRaised {
        worker: WorkerIndex,
        step: &'static str,
        raised: StartException,
    },
    #[error(
        "worker {worker}: {} can't be compiled: it contains a NUL byte ({source})",
        .file.display()
    )]
    NulInSource {
        worker: WorkerIndex,
        file: PathBuf,
        source: std::ffi::NulError,
    },
    #[error(
        "worker {worker}: the script registered no routes in the worker, but the main \
         interpreter has {expected} route(s). A worker executes the whole script and serves \
         the app it registers routes on; don't register routes only in the main interpreter."
    )]
    NoRoutes {
        worker: WorkerIndex,
        expected: usize,
    },
    #[error(
        "worker {worker}: its interpreter already had {state} before the worker set it; a new \
         worker interpreter has none"
    )]
    StateAlreadySet {
        worker: WorkerIndex,
        state: InterpreterState,
    },
    /// The panic is caught on the worker's thread, and its interpreter is ended before the unwind
    /// goes on.
    #[error("worker {worker}: its start panicked: {message}")]
    Panicked {
        worker: WorkerIndex,
        message: String,
    },
    /// The thread could not be given its turn, ended without reporting, or could not take its
    /// verdict. This is not expected, and the start fails rather than wait.
    #[error("start {position}: the thread ended before its start was done")]
    ReportLost { position: usize },
}

/// A per-interpreter value a worker sets once, as it starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum InterpreterState {
    #[error("the running app's shared-state map")]
    SharedMap,
    #[error("main's route table")]
    RouteManifest,
    #[error("its routers")]
    Routers,
}

impl WorkerStartError {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            WorkerStartError::CreationFailed { .. } => "creation_failed",
            WorkerStartError::ScriptRaised { .. }
            | WorkerStartError::SetupRaised { .. }
            | WorkerStartError::NulInSource { .. }
            | WorkerStartError::NoRoutes { .. }
            | WorkerStartError::StateAlreadySet { .. } => "prepare_failed",
            WorkerStartError::Panicked { .. } => "panicked",
            WorkerStartError::ReportLost { .. } => "report_lost",
        }
    }

    fn exec(py: Python<'_>, worker: WorkerIndex, file: &Path, error: ExecFailure) -> Self {
        let ExecFailure { raised, failure } = error;
        WorkerStartError::ScriptRaised {
            worker,
            file: file.into(),
            raised: StartException::capture(py, &raised),
            failure: failure.map(Box::new),
        }
    }

    fn raised(&self) -> Option<&StartException> {
        match self {
            WorkerStartError::ScriptRaised { raised, .. }
            | WorkerStartError::SetupRaised { raised, .. } => Some(raised),
            _ => None,
        }
    }

    /// Converts this error into the exception main raises: an import failure stays an
    /// `ImportError` (or `ModuleNotFoundError`) and anything else is a `RuntimeError`, with the
    /// worker's traceback as its `__cause__` ([`WorkerException`]).
    pub(crate) fn into_pyerr(self, py: Python<'_>) -> PyErr {
        let text = self.to_string();
        let Some(raised) = self.raised() else {
            return PyRuntimeError::new_err(text);
        };
        let err = match raised.class {
            ExceptionClass::ModuleNotFound => PyModuleNotFoundError::new_err(text),
            ExceptionClass::Import => PyImportError::new_err(text),
            ExceptionClass::Other => PyRuntimeError::new_err(text),
        };
        let traceback = raised.exception.traceback().to_owned();
        err.set_cause(py, Some(WorkerException::new_err(traceback)));
        err
    }
}

#[derive(Clone, Copy)]
pub(crate) struct WorkerSpec<'a> {
    pub(crate) program: &'a WorkerProgram,
    /// It is main's route table up to its seal, which the script must register.
    pub(crate) expected: &'a RouteManifest,
    pub(crate) store: &'a crate::runtime::AppStore,
    pub(crate) gc: GcPolicy,
    /// A worker whose script set other limits warns that they are ignored.
    pub(crate) limits: mipyre_http::plan::limits::Limits,
}

/// Owns what [`WorkerSpec`] borrows, so the server shares it by `Arc` with every worker's thread
/// rather than lending it.
pub(crate) struct WorkerRecipe {
    program: WorkerProgram,
    expected: RouteManifest,
    store: crate::runtime::AppStore,
    gc: GcPolicy,
    limits: mipyre_http::plan::limits::Limits,
}

impl WorkerRecipe {
    pub(crate) fn new(
        program: &WorkerProgram,
        expected: &RouteManifest,
        store: &crate::runtime::AppStore,
        gc: GcPolicy,
        limits: mipyre_http::plan::limits::Limits,
    ) -> Self {
        WorkerRecipe {
            program: program.clone(),
            expected: expected.clone(),
            store: store.clone(),
            gc,
            limits,
        }
    }

    pub(crate) fn spec(&self) -> WorkerSpec<'_> {
        WorkerSpec {
            program: &self.program,
            expected: &self.expected,
            store: &self.store,
            gc: self.gc,
            limits: self.limits,
        }
    }

    pub(crate) fn expected(&self) -> &RouteManifest {
        &self.expected
    }

    /// Logs the per-process isolation report once `workers` workers of this recipe started.
    pub(crate) fn report_started(&self, workers: usize) {
        if self.program.isolation {
            crate::runtime::report_startup(workers);
        }
    }
}

/// Runs `src` in `module` as a step of `worker`'s start.
fn run(
    py: Python<'_>,
    worker: WorkerIndex,
    src: &PythonSource<'_>,
    module: &Bound<'_, PyModule>,
) -> Result<(), WorkerStartError> {
    exec_in(py, src, module).map_err(|e| WorkerStartError::exec(py, worker, src.file(), e))
}

pub(super) fn prepare<R: RequestRunner>(
    py: Python<'_>,
    worker: WorkerIndex,
    spec: &WorkerSpec<'_>,
) -> Result<R, WorkerStartError> {
    let setup = |step| {
        move |e: PyErr| WorkerStartError::SetupRaised {
            worker,
            step,
            raised: StartException::capture(py, &e),
        }
    };
    let program = spec.program;

    // This comes before the script, because a `MipyreApp` or `SharedState` it creates must see
    // the running app's map.
    crate::runtime::hand_to_worker(py, spec.store)
        .map_err(|state| WorkerStartError::StateAlreadySet { worker, state })?;

    // This comes before any import, so the bootstrap and the script resolve imports as on main.
    PyList::new(py, program.import_path.iter().map(|p| p.as_os_str()))
        .and_then(|path| py.import("sys")?.setattr("path", path))
        .map_err(setup("setting sys.path to main's"))?;

    let bootstrap = new_module(py, BOOTSTRAP_MODULE, None)
        .and_then(|m| {
            use mipyre_subinterp::isolation::{SOURCE, SOURCE_FILE};
            m.setattr("WORKER_ID", worker.get())?;
            m.setattr("ISOLATION_SOURCE", program.isolation.then_some(SOURCE))?;
            m.setattr("ISOLATION_FILE", SOURCE_FILE)?;
            Ok(m)
        })
        .map_err(setup("creating the bootstrap module"))?;
    // The log bridge runs first in the same namespace, because the bootstrap installs it before
    // the package can be imported.
    run(py, worker, &bundled!("mipyre/_log_bridge.py"), &bootstrap)?;
    // The isolation adapter that the bootstrap runs makes its private copies with these
    // functions; like the log bridge they must be there before the package can be imported.
    if program.isolation {
        run(py, worker, &bundled!("mipyre/_clone.py"), &bootstrap)?;
    }
    run(py, worker, &bundled!("mipyre/_bootstrap.py"), &bootstrap)?;

    // This comes before the script, because its app is matched by key against main's table and
    // its `Mipyre(routers=...)` imports these routers.
    worker_app::expect(py, spec.expected.clone(), program.routers.clone())
        .map_err(|state| WorkerStartError::StateAlreadySet { worker, state })?;

    // The script is a real module compiled with its own path, so tracebacks point at it,
    // `from __future__` imports work and `typing.get_type_hints` finds it in `sys.modules`.
    let script_module = new_module(py, SCRIPT_MODULE, Some(&program.script_path))
        .map_err(setup("creating the script module"))?;
    let script = PythonSource::new(&program.script, &program.script_path).map_err(|source| {
        WorkerStartError::NulInSource {
            worker,
            file: program.script_path.clone(),
            source,
        }
    })?;
    run(py, worker, &script, &script_module)?;

    crate::exchange::to_response::require_json(py).map_err(setup("loading the JSON serializer"))?;

    let routes = registered_routes(py, worker, spec)?;
    R::build(py, worker, routes, spec.gc)
}

/// Returns the routes of the app the script registered on, which must be main's table.
fn registered_routes<'py>(
    py: Python<'py>,
    worker: WorkerIndex,
    spec: &WorkerSpec<'_>,
) -> Result<WorkerRoutes<'py>, WorkerStartError> {
    let registered = worker_app::worker_routes(py).map_err(|e| WorkerStartError::SetupRaised {
        worker,
        step: "reading the app the script registered",
        raised: StartException::capture(py, &e),
    })?;
    let expected = spec.expected;
    let routes = match registered {
        Some(r) => r,
        None if expected.is_empty() => WorkerRoutes::empty(),
        None => {
            return Err(WorkerStartError::NoRoutes {
                worker,
                expected: expected.routes.len(),
            })
        }
    };
    if let Some(limits) = &routes.limits {
        for ignored in spec.limits.ignored_in_worker(limits) {
            tracing::warn!(target: "mipyre::server", worker = worker.get(), "{ignored}");
        }
    }
    Ok(routes)
}
