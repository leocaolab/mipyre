//! Serves the metrics page of one server run as a main-interpreter route, so the app's
//! `before_request` hooks guard it like any other route.

use std::sync::Arc;

use hyper::header::HeaderValue;
use hyper::StatusCode;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::telemetry::{exposition, read_counters};
use mipyre_http::io::static_files::StaticMount;
use mipyre_runtime::exchange::Response;
use mipyre_runtime::WorkerCounters;
use mipyre_server::ServerCounters;

/// The handler of the run's metrics route; each run binds its own, so a page shows only the run
/// that serves it.
#[pyclass(frozen, module = "mipyre.engine")]
pub(crate) struct MetricsPage {
    run: Arc<ServerCounters>,
    workers: Arc<WorkerCounters>,
    mounts: Vec<StaticMount>,
}

impl MetricsPage {
    pub(crate) fn new(run: Arc<ServerCounters>, workers: Arc<WorkerCounters>, mounts: Vec<StaticMount>) -> Self {
        MetricsPage { run, workers, mounts }
    }
}

#[pymethods]
impl MetricsPage {
    fn __call__(&self, py: Python<'_>, _req: &Bound<'_, PyAny>) -> Response {
        let static_bytes_in_flight = self.mounts.iter().map(StaticMount::bytes_in_flight).sum();
        let page = exposition::render(
            &self.run.read(),
            &self.workers.read(),
            static_bytes_in_flight,
            &read_counters(),
        );
        Response {
            body: PyBytes::new(py, page.as_bytes()).into_any().unbind(),
            status: StatusCode::OK,
            content_type: Some(HeaderValue::from_static(exposition::CONTENT_TYPE)),
            header_type: None,
            headers: Default::default(),
            validation_errors: None,
        }
    }
}
