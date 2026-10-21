//! A serving thread's own copy of an app's rules.

use mipyre_http::plan::site::SiteRules;
use pyo3::{Py, PyAny, Python};

/// Copies `rules` with its own reference to every handler, so one serving thread can keep it.
pub(crate) fn copy_rules<M: Clone>(
    rules: &SiteRules<Py<PyAny>, M>,
    py: Python<'_>,
) -> SiteRules<Py<PyAny>, M> {
    rules.copy_with(|handler| handler.clone_ref(py))
}
