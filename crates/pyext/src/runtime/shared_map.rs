//! Holds `app.state` in Rust memory, so every interpreter of an app sees it without a Python
//! object crossing interpreters.

use std::sync::Arc;

use dashmap::DashMap;
use pyo3::Python;

/// A value of `app.state`, kept as the type it was set with so a text read never revalidates
/// UTF-8.
pub(crate) enum StateValue {
    Text(String),
    Bytes(Vec<u8>),
}

impl StateValue {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        match self {
            StateValue::Text(text) => text.as_bytes(),
            StateValue::Bytes(bytes) => bytes,
        }
    }

    /// Returns the value as text; bytes count only if they are UTF-8.
    pub(crate) fn as_text(&self) -> Option<&str> {
        match self {
            StateValue::Text(text) => Some(text),
            StateValue::Bytes(bytes) => std::str::from_utf8(bytes).ok(),
        }
    }
}

/// One app's `app.state`; a clone is the same store.
#[derive(Clone, Default)]
pub(crate) struct AppStore(Arc<DashMap<String, StateValue>>);

impl std::ops::Deref for AppStore {
    type Target = DashMap<String, StateValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// The running app's map in this interpreter; it is unset on main and in interpreters mipyre did
/// not create.
static WORKER_MAP: pyo3::sync::PyOnceLock<AppStore> = pyo3::sync::PyOnceLock::new();

/// Gives the attached worker interpreter the running app's map.
///
/// It must run once, right after the interpreter is created and before its script runs: the
/// PyO3 fork's `PyOnceLock` holds one value per interpreter and starts empty in a new one.
pub(crate) fn hand_to_worker(
    py: Python<'_>,
    map: &AppStore,
) -> Result<(), crate::runtime::worker::start::InterpreterState> {
    WORKER_MAP
        .set(py, map.clone())
        .map_err(|_| crate::runtime::worker::start::InterpreterState::SharedMap)
}

/// Returns the running app's map inside a worker, and a fresh one elsewhere so TestClient apps
/// in one process keep separate maps.
pub(crate) fn map_for_new(py: Python<'_>) -> AppStore {
    match WORKER_MAP.get(py) {
        Some(map) => map.clone(),
        None => AppStore::default(),
    }
}
