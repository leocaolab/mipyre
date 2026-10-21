//! Records the one app a worker's script registers on and matches its routes to main's table.
//!
//! The app type records itself together with the function that reads its routes, so the worker
//! does not depend on the app type.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;

use mipyre_http::plan::limits::Limits;
use mipyre_http::plan::route::{ExpectedRoute, RouteKey, RouteManifest};

use super::worker::start::InterpreterState;

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

struct WorkerApp {
    app: Py<PyAny>,
    read: RoutesReader,
}

/// Holds one value per interpreter, as every `PyOnceLock` here does under the PyO3 fork.
static WORKER_APP: PyOnceLock<WorkerApp> = PyOnceLock::new();

/// Holds main's table, set before the app is imported.
static EXPECTED: PyOnceLock<RouteManifest> = PyOnceLock::new();

/// Holds the routers (`module:attr`) this interpreter imports; it is unset on main, which
/// imports every router.
static ROUTERS: PyOnceLock<Vec<String>> = PyOnceLock::new();

/// Sets what this worker serves; it must run before the app is imported.
///
/// # Errors
///
/// Returns the value this interpreter already had set.
pub(crate) fn expect(
    py: Python<'_>,
    expected: RouteManifest,
    routers: Vec<String>,
) -> Result<(), InterpreterState> {
    EXPECTED
        .set(py, expected)
        .map_err(|_| InterpreterState::RouteManifest)?;
    ROUTERS
        .set(py, routers)
        .map_err(|_| InterpreterState::Routers)
}

/// Returns the routers this interpreter imports, or `None` on main, which imports all of them.
#[pyfunction]
pub(crate) fn _routers_to_load(py: Python<'_>) -> Option<Vec<String>> {
    ROUTERS.get(py).cloned()
}

/// Sets a process-lane worker's routers; `_process_lane` calls it before importing the app.
#[pyfunction]
pub(crate) fn _set_routers_to_load(py: Python<'_>, routers: Vec<String>) -> PyResult<()> {
    ROUTERS.set(py, routers).map_err(|_| {
        pyo3::exceptions::PyRuntimeError::new_err("this interpreter's routers are already set")
    })
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
    match WORKER_APP.get(py) {
        Some(recorded) if recorded.app.bind(py).is(app) => Ok(()),
        Some(_) => Err(pyo3::exceptions::PyRuntimeError::new_err(
            "the script registers routes or hooks on a second app; a worker serves exactly \
             one app per script (create one Mipyre()/MipyreApp() and register everything \
             on it)",
        )),
        None => WORKER_APP
            .set(
                py,
                WorkerApp {
                    app: app.clone().unbind(),
                    read,
                },
            )
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("worker app already recorded")),
    }
}

/// Returns the recorded app's routes aligned to main's table, or `None` if the script registered
/// nothing; it is meaningful only in a worker after its script ran.
pub(crate) fn worker_routes(py: Python<'_>) -> PyResult<Option<WorkerRoutes<'_>>> {
    let Some(recorded) = WORKER_APP.get(py) else {
        return Ok(None);
    };
    let Some(expected) = EXPECTED.get(py) else {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "this interpreter's app was recorded, but it was never given the main \
             interpreter's route table to serve (only an engine worker is)",
        ));
    };
    let own = (recorded.read)(recorded.app.bind(py))?;
    align(own, expected).map(Some)
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
        let found = own
            .routes
            .iter()
            .find(|r| r.route.key == *key && r.route.on_worker);
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
    let hooks_differ = own.before_hooks.len() != expected.before_hooks
        || own.after_hooks.len() != expected.after_hooks;
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
