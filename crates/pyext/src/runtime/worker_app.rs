//! What `mipyre.engine` holds in one interpreter (its [`EngineModule`]): what the main
//! interpreter assigned it, and the one app its script registers on, whose routes are matched to
//! main's table.
//!
//! The app type records itself together with the function that reads its routes, so the worker
//! does not depend on the app type.

use std::sync::{Arc, OnceLock, Weak};

use pyo3::prelude::*;

use mipyre_http::plan::limits::Limits;
use mipyre_http::plan::route::{ExpectedRoute, RouteKey, RouteManifest};

use super::pg::{PgHandle, RunPools};
use super::shared_map::AppStore;

/// The module whose state an interpreter's [`EngineModule`] is.
const ENGINE_MODULE: &str = "mipyre.engine";

/// `mipyre.engine`'s PEP 489 module state in one interpreter (b/335 D13): it lives in that
/// interpreter's module object and ends with it, and code reaches it through the interpreter's
/// own `sys.modules`, not through a process-wide table. The main interpreter has no assignment.
#[derive(Default)]
pub(crate) struct EngineModule {
    assignment: OnceLock<WorkerAssignment>,
    /// It is the app the script registered on, from its first registration until the worker
    /// ends ([`EngineModule::release_app`]): the module's state is not visited by the garbage
    /// collector, so an app still held when the interpreter ends would outlive it.
    app: crate::sync::Mutex<Option<WorkerApp>>,
    /// Every `PgPool.connect()` this interpreter made that is still held, in call order: a run
    /// sealing on the main interpreter opens its pools from them (b/335 D17).
    pg_handles: crate::sync::Mutex<Vec<Weak<PgHandle>>>,
}

/// What the main interpreter gives an interpreter it serves through, once, before its script
/// runs: a sub-interpreter worker gets main's route table, its routers and the app's shared
/// map; a process-lane worker gets only its routers.
pub(crate) enum WorkerAssignment {
    Subinterpreter {
        expected: RouteManifest,
        routers: Vec<String>,
        store: AppStore,
        /// The run's Postgres pools, which this worker's `PgPool.connect()` binds to; `None` when
        /// the run has none.
        pg: Option<Arc<RunPools>>,
    },
    LaneWorker {
        routers: Vec<String>,
    },
}

impl WorkerAssignment {
    fn routers(&self) -> &[String] {
        match self {
            WorkerAssignment::Subinterpreter { routers, .. } | WorkerAssignment::LaneWorker { routers } => routers,
        }
    }
}

/// An interpreter is assigned once; a second assignment is refused and the first kept.
#[derive(Debug, thiserror::Error)]
#[error(
    "this interpreter was already assigned what it serves; an interpreter is assigned once, before its script runs"
)]
pub(crate) struct SecondAssignmentError;

impl EngineModule {
    /// Runs `f` on this interpreter's `mipyre.engine` state, importing the module if this
    /// interpreter has not yet.
    ///
    /// # Errors
    ///
    /// Returns the Python error when the module cannot be imported or holds no state.
    pub(crate) fn with<R>(py: Python<'_>, f: impl FnOnce(&EngineModule) -> R) -> PyResult<R> {
        let module = py.import(ENGINE_MODULE)?;
        Ok(f(module.state::<EngineModule>()?))
    }

    /// Runs `f` on this interpreter's `mipyre.engine` state when the module is loaded in it, and
    /// returns `None` when it is not: an interpreter that never imported the module was assigned
    /// nothing and recorded no app, so a reader has nothing to find and imports nothing.
    ///
    /// # Errors
    ///
    /// Returns the Python error when `sys.modules` cannot be read or the module holds no state.
    pub(crate) fn if_loaded<R>(py: Python<'_>, f: impl FnOnce(&EngineModule) -> R) -> PyResult<Option<R>> {
        let modules = py.import("sys")?.getattr("modules")?;
        let Some(module) = modules.cast::<pyo3::types::PyDict>()?.get_item(ENGINE_MODULE)? else {
            return Ok(None);
        };
        Ok(Some(f(module
            .cast::<pyo3::types::PyModule>()?
            .state::<EngineModule>()?)))
    }

    /// Records what this interpreter serves.
    ///
    /// # Errors
    ///
    /// Returns [`SecondAssignmentError`] when it was assigned before.
    pub(crate) fn assign(&self, assignment: WorkerAssignment) -> Result<(), SecondAssignmentError> {
        self.assignment.set(assignment).map_err(|_| SecondAssignmentError)
    }

    /// Lets go of the recorded app, as the worker ends, with its interpreter attached.
    ///
    /// # Errors
    ///
    /// Returns the Python error when this interpreter's `mipyre.engine` cannot be reached.
    pub(crate) fn release_app(py: Python<'_>) -> PyResult<()> {
        // The app drops here, attached, outside the lock.
        let released = EngineModule::if_loaded(py, |module| module.app.lock().take())?.flatten();
        drop(released);
        Ok(())
    }

    /// Records a `PgPool.connect()` of this interpreter, for the next run that seals here.
    pub(crate) fn record_pg(&self, handle: &Arc<PgHandle>) {
        let mut handles = self.pg_handles.lock();
        handles.retain(|held| held.strong_count() > 0);
        handles.push(Arc::downgrade(handle));
    }

    /// Every `PgPool.connect()` of this interpreter still held, in call order.
    pub(crate) fn pg_handles(&self) -> Vec<Arc<PgHandle>> {
        self.pg_handles.lock().iter().filter_map(Weak::upgrade).collect()
    }

    /// The pools of the run this interpreter serves as a worker, if the run has any.
    pub(crate) fn run_pools(&self) -> Option<Arc<RunPools>> {
        match self.assignment.get() {
            Some(WorkerAssignment::Subinterpreter { pg, .. }) => pg.clone(),
            Some(WorkerAssignment::LaneWorker { .. }) | None => None,
        }
    }

    /// The app's shared map inside a sub-interpreter worker, and a fresh one elsewhere, so
    /// `TestClient` apps in one process keep separate maps.
    pub(crate) fn store(&self) -> AppStore {
        match self.assignment.get() {
            Some(WorkerAssignment::Subinterpreter { store, .. }) => store.clone(),
            Some(WorkerAssignment::LaneWorker { .. }) | None => AppStore::default(),
        }
    }
}

/// Holds what a worker serves, indexed like main's table.
///
/// The handlers are `Bound` so that dropping them during the worker's init, when its thread state
/// is current but not registered with PyO3, releases them at once.
pub(crate) struct WorkerRoutes<'py> {
    pub(crate) routes: Vec<WorkerRoute<'py>>,
    pub(crate) before_hooks: Vec<Bound<'py, PyAny>>,
    pub(crate) after_hooks: Vec<Bound<'py, PyAny>>,
    /// Holds the limits the script set, which are never served because main's app is; it is
    /// `None` when the script made no app.
    pub(crate) limits: Option<Limits>,
}

pub(crate) struct WorkerRoute<'py> {
    pub(crate) key: RouteKey,
    /// Is `None` for a route that runs elsewhere; main never sends it to this worker.
    pub(crate) handler: Option<Bound<'py, PyAny>>,
}

impl WorkerRoutes<'_> {
    pub(crate) fn empty() -> Self {
        WorkerRoutes {
            routes: Vec::new(),
            before_hooks: Vec::new(),
            after_hooks: Vec::new(),
            limits: None,
        }
    }
}

/// Holds what a worker's script registered, in the script's order, before `align` matches it to
/// main's table.
pub(crate) struct ScriptRegistrations<'py> {
    pub(crate) routes: Vec<AppRoute<'py>>,
    pub(crate) before_hooks: Vec<Bound<'py, PyAny>>,
    pub(crate) after_hooks: Vec<Bound<'py, PyAny>>,
    pub(crate) limits: Limits,
}

pub(crate) struct AppRoute<'py> {
    pub(crate) route: ExpectedRoute,
    pub(crate) handler: Bound<'py, PyAny>,
}

impl ScriptRegistrations<'_> {
    fn manifest(&self) -> RouteManifest {
        RouteManifest {
            routes: self.routes.iter().map(|r| r.route.clone()).collect(),
            before_hooks: self.before_hooks.len(),
            after_hooks: self.after_hooks.len(),
        }
    }
}

pub(crate) type RoutesReader = for<'py> fn(&Bound<'py, PyAny>) -> PyResult<ScriptRegistrations<'py>>;

/// The app a worker's script registered on, with the function that reads its routes.
pub(crate) struct WorkerApp {
    app: Py<PyAny>,
    read: RoutesReader,
}

/// Returns the routers this interpreter imports, or `None` on main, which imports all of them.
#[pyfunction]
pub(crate) fn _routers_to_load(py: Python<'_>) -> PyResult<Option<Vec<String>>> {
    EngineModule::with(py, |module| module.assignment.get().map(|a| a.routers().to_vec()))
}

/// Assigns a process-lane worker its routers; `_process_lane` calls it before importing the app.
#[pyfunction]
pub(crate) fn _set_routers_to_load(py: Python<'_>, routers: Vec<String>) -> PyResult<()> {
    EngineModule::with(py, |module| module.assign(WorkerAssignment::LaneWorker { routers }))?
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

/// Records `app` as the one this worker serves; it does nothing on main.
///
/// # Errors
///
/// Fails when the script registers on a second app, since one app is what is checked against
/// main's.
pub(crate) fn record_worker_app(app: &Bound<'_, PyAny>, read: RoutesReader) -> PyResult<()> {
    let py = app.py();
    if mipyre_subinterp::attach::on_main(py) {
        return Ok(());
    }
    EngineModule::with(py, |module| {
        let mut slot = module.app.lock();
        let recorded = slot.get_or_insert_with(|| WorkerApp {
            app: app.clone().unbind(),
            read,
        });
        if recorded.app.bind(py).is(app) {
            Ok(())
        } else {
            Err(pyo3::exceptions::PyRuntimeError::new_err(
                "the script registers routes or hooks on a second app; a worker serves exactly \
                 one app per script (create one Mipyre()/MipyreApp() and register everything \
                 on it)",
            ))
        }
    })?
}

/// Returns the recorded app's routes aligned to main's table, or `None` if the script registered
/// nothing; it is meaningful only in a worker after its script ran.
pub(crate) fn worker_routes(py: Python<'_>) -> PyResult<Option<WorkerRoutes<'_>>> {
    let recorded = EngineModule::if_loaded(py, |module| {
        let app = module
            .app
            .lock()
            .as_ref()
            .map(|recorded| (recorded.app.clone_ref(py), recorded.read));
        let expected = match module.assignment.get() {
            Some(WorkerAssignment::Subinterpreter { expected, .. }) => Some(expected.clone()),
            Some(WorkerAssignment::LaneWorker { .. }) | None => None,
        };
        (app, expected)
    })?;
    let Some((Some((app, read)), expected)) = recorded else {
        return Ok(None);
    };
    let Some(expected) = expected else {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "this interpreter's app was recorded, but it was never given the main \
             interpreter's route table to serve (only an engine worker is)",
        ));
    };
    let own = read(app.bind(py))?;
    align(own, &expected).map(Some)
}

/// Reorders `own` to main's indices by `(METHOD, path)`, failing when a route or the hook counts
/// differ.
fn align<'py>(own: ScriptRegistrations<'py>, expected: &RouteManifest) -> PyResult<WorkerRoutes<'py>> {
    let mut routes = Vec::with_capacity(expected.routes.len());
    let mut missing = Vec::new();
    for ExpectedRoute { key, on_worker } in &expected.routes {
        if !on_worker {
            routes.push(WorkerRoute {
                key: key.clone(),
                handler: None,
            });
            continue;
        }
        let found = own.routes.iter().find(|r| r.route.key == *key && r.route.on_worker);
        match found {
            Some(r) => routes.push(WorkerRoute {
                key: key.clone(),
                handler: Some(r.handler.clone()),
            }),
            None => missing.push(key.to_string()),
        }
    }
    // A route only this worker has means the app diverged between interpreters, for example
    // under `if _in_worker()`.
    for AppRoute { route, .. } in &own.routes {
        if !expected.routes.iter().any(|r| r.key == route.key) {
            missing.push(format!("{} (only in this worker)", route.key));
        }
    }
    let hooks_differ = own.before_hooks.len() != expected.before_hooks || own.after_hooks.len() != expected.after_hooks;
    if !missing.is_empty() || hooks_differ {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "this worker's app and the main interpreter's differ. Routes that don't match: \
             {}. {}\n{}",
            if missing.is_empty() {
                "none".to_string()
            } else {
                missing.join(", ")
            },
            if hooks_differ {
                "The before/after_request hooks differ too."
            } else {
                ""
            },
            RouteManifest::describe_mismatch(expected, &own.manifest())
        )));
    }
    Ok(WorkerRoutes {
        routes,
        before_hooks: own.before_hooks,
        after_hooks: own.after_hooks,
        limits: Some(own.limits),
    })
}
