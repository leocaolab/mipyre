//! Holds `app.state` in Rust memory, so every interpreter of an app sees it without a Python
//! object crossing interpreters.

use std::sync::Arc;

use dashmap::DashMap;
use pyo3::Python;

/// A value of `app.state`, kept as the type it was set with so a text read never revalidates
/// UTF-8.
pub enum StateValue {
    Text(String),
    Bytes(Vec<u8>),
}

impl StateValue {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            StateValue::Text(text) => text.as_bytes(),
            StateValue::Bytes(bytes) => bytes,
        }
    }

    /// Returns the value as text; bytes count only if they are UTF-8.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            StateValue::Text(text) => Some(text),
            StateValue::Bytes(bytes) => std::str::from_utf8(bytes).ok(),
        }
    }
}

/// One app's `app.state`; a clone is the same store.
#[derive(Clone, Default)]
pub struct AppStore(Arc<DashMap<String, StateValue>>);

impl std::ops::Deref for AppStore {
    type Target = DashMap<String, StateValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Returns the running app's map inside a worker, and a fresh one elsewhere so TestClient apps
/// in one process keep separate maps.
///
/// # Errors
///
/// Returns the Python error when this interpreter's `mipyre.engine` cannot be reached.
pub fn map_for_new(py: Python<'_>) -> pyo3::PyResult<AppStore> {
    // The main interpreter is never assigned a map (a process-lane worker's is assigned only
    // its routers), so it does not look.
    if mipyre_subinterp::attach::on_main(py) {
        return Ok(AppStore::default());
    }
    super::worker_app::EngineModule::with(py, super::worker_app::EngineModule::store)
}
