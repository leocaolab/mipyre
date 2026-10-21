//! Defines `SharedState` (`app.state`), a key-value store of strings and bytes that every
//! interpreter of one app sees; it lives in Rust memory so no Python object crosses
//! interpreters.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyString};

use crate::runtime::{map_for_new, AppStore, StateValue};

/// Copies a stored value into a Python `str`; a value set with `set_bytes` that is not UTF-8
/// is a `TypeError` naming the key.
fn text<'py>(py: Python<'py>, key: &str, value: &StateValue) -> PyResult<Bound<'py, PyString>> {
    value.as_text().map(|s| PyString::new(py, s)).ok_or_else(|| {
        pyo3::exceptions::PyTypeError::new_err(format!(
            "state[{key:?}]: value is not valid UTF-8; use get_bytes() for raw access"
        ))
    })
}

/// Refuses access from a process-lane worker, which holds only a forked copy of the state.
fn guard() -> PyResult<()> {
    crate::lane::refuse_in_lane_worker("app.state (SharedState)")
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

/// A view of an app's [`AppStore`]; every method raises `RuntimeError` in a process-lane
/// worker.
#[pyclass(name = "SharedState", module = "mipyre.engine")]
pub(crate) struct AppStateView {
    store: AppStore,
}

impl AppStateView {
    pub(crate) fn of(store: AppStore) -> Self {
        AppStateView { store }
    }
}

#[pymethods]
impl AppStateView {
    #[new]
    fn new(py: Python<'_>) -> Self {
        AppStateView {
            store: map_for_new(py),
        }
    }

    fn set(&self, key: String, value: String) -> PyResult<()> {
        guard()?;
        self.store.insert(key, StateValue::Text(value));
        Ok(())
    }

    /// Return the string value of `key`, or `default` when it is absent; a value that is not
    /// UTF-8 raises `TypeError`.
    #[pyo3(signature = (key, default=None))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        key: &str,
        default: Option<Bound<'py, PyString>>,
    ) -> PyResult<Option<Bound<'py, PyString>>> {
        guard()?;
        match self.store.get(key) {
            Some(v) => text(py, key, v.value()).map(Some),
            None => Ok(default),
        }
    }

    fn set_bytes(&self, key: String, value: Vec<u8>) -> PyResult<()> {
        guard()?;
        self.store.insert(key, StateValue::Bytes(value));
        Ok(())
    }

    fn get_bytes<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Option<Bound<'py, PyBytes>>> {
        guard()?;
        Ok(self.store.get(key).map(|v| PyBytes::new(py, v.value().as_bytes())))
    }

    /// Delete `key` and return whether it existed.
    fn delete(&self, key: &str) -> PyResult<bool> {
        guard()?;
        Ok(self.store.remove(key).is_some())
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        guard()?;
        PyList::new(py, self.store.iter().map(|e| PyString::new(py, e.key())))
    }

    /// Return all string values; a value that is not UTF-8 raises `TypeError` naming its key.
    fn values<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyString>>> {
        guard()?;
        self.store
            .iter()
            .map(|e| text(py, e.key(), e.value()))
            .collect()
    }

    /// Return all `(key, value)` pairs; a value that is not UTF-8 raises `TypeError` naming
    /// its key.
    fn items<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Vec<(Bound<'py, PyString>, Bound<'py, PyString>)>> {
        guard()?;
        self.store
            .iter()
            .map(|e| Ok((PyString::new(py, e.key()), text(py, e.key(), e.value())?)))
            .collect()
    }

    fn __len__(&self) -> PyResult<usize> {
        guard()?;
        Ok(self.store.len())
    }

    /// Return whether `key` exists, even when its value is not UTF-8.
    fn __contains__(&self, key: &str) -> PyResult<bool> {
        guard()?;
        Ok(self.store.contains_key(key))
    }

    fn __setitem__(&self, key: String, value: String) -> PyResult<()> {
        self.set(key, value)
    }

    /// Return the string value of `key`; an absent key raises `KeyError` and a value that is
    /// not UTF-8 raises `TypeError`.
    fn __getitem__<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Bound<'py, PyString>> {
        guard()?;
        match self.store.get(key) {
            None => Err(pyo3::exceptions::PyKeyError::new_err(key.to_string())),
            Some(v) => text(py, key, v.value()),
        }
    }

    fn __delitem__(&self, key: &str) -> PyResult<()> {
        if self.delete(key)? {
            Ok(())
        } else {
            Err(pyo3::exceptions::PyKeyError::new_err(key.to_string()))
        }
    }

    /// Atomically add `amount` to the integer under `key`, starting from 0, and return the
    /// new value.
    ///
    /// An existing value that is not an integer raises `TypeError` rather than being reset,
    /// because it may be someone else's data; overflow raises `OverflowError`.
    fn incr(&self, key: String, amount: i64) -> PyResult<i64> {
        guard()?;
        let mut entry = self
            .store
            .entry(key.clone())
            .or_insert_with(|| StateValue::Text("0".to_owned()));
        let raw = entry.value().as_text().ok_or_else(|| {
            pyo3::exceptions::PyTypeError::new_err(format!(
                "incr({key:?}): existing value is not valid UTF-8"
            ))
        })?;
        let current: i64 = raw.parse().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err(format!(
                "incr({key:?}): existing value {raw:?} is not an integer"
            ))
        })?;
        let new_val = current.checked_add(amount).ok_or_else(|| {
            pyo3::exceptions::PyOverflowError::new_err(format!("incr({key:?}): i64 overflow"))
        })?;
        *entry = StateValue::Text(new_val.to_string());
        Ok(new_val)
    }

    /// Atomically subtract `amount` from the integer under `key` and return the new value.
    fn decr(&self, key: String, amount: i64) -> PyResult<i64> {
        let neg = amount.checked_neg().ok_or_else(|| {
            pyo3::exceptions::PyOverflowError::new_err("decr: amount i64::MIN cannot be negated")
        })?;
        self.incr(key, neg)
    }

    fn __repr__(&self) -> String {
        format!("SharedState({} keys)", self.store.len())
    }
}
