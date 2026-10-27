//! A serving thread's own copy of an app's rules, and the handler type the server's tables hold.

use mipyre_http::io::static_files::StaticMount;
use mipyre_http::plan::route::RouteTable;
use mipyre_http::plan::site::SiteRules;
use pyo3::{Py, PyAny, Python};

/// A Python callable the app registered: a route's handler, a hook or the fallback.
pub type Handler = Py<PyAny>;

pub type Routes = RouteTable<Handler, StaticMount>;

/// Copies `rules` with its own reference to every handler, so one serving thread can keep it.
pub fn copy_rules<M: Clone>(rules: &SiteRules<Handler, M>, py: Python<'_>) -> SiteRules<Handler, M> {
    rules.copy_with(|handler| handler.clone_ref(py))
}
