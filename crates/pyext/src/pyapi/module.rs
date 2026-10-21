//! Defines the extension module `mipyre.engine`, which registers every class and function
//! Python sees, and the module-level functions that belong to no class.

use pyo3::prelude::*;

#[cfg(feature = "leak_detect")]
#[pyo3::pyfunction]
fn leak_detect_dump() {
    crate::telemetry::dump_to_stderr();
}

/// Panic with `message`, so the chaos tests can drive a real Rust panic through the dispatch
/// path; a `PyErr` would exercise a different path.
///
/// PyO3 raises the panic as `PanicException` in the calling handler, and the dispatcher resumes
/// it in Rust on the path that ran the handler.
#[cfg(feature = "fault_injection")]
#[pyo3::pyfunction]
#[expect(
    clippy::panic,
    reason = "this function exists to panic: the fault_injection feature (test-only) drives a real panic through the dispatch path"
)]
fn _fault_panic(message: String) {
    panic!("{message}");
}

#[pyo3::pyfunction]
fn workrequest_counts() -> Option<(u64, u64)> {
    crate::runtime::PoolJob::counts()
}

/// Return the parameter names of the route path `path` in order (`{*rest}` gives `rest`).
///
/// Raises `ValueError` for a path the router would reject, such as `:name`. It is the only
/// parser of route templates, shared by `app.py`'s parameter injection and route registration.
#[pyo3::pyfunction]
fn _route_params(path: &str) -> PyResult<Vec<String>> {
    mipyre_http::plan::route::template_params(path)
        .map(|names| names.into_iter().map(str::to_owned).collect())
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("route {path}: {e}")))
}

/// Return whether this code runs in a sub-interpreter worker.
///
/// It asks the interpreter rather than an environment variable, which child processes would
/// inherit.
#[pyo3::pyfunction]
fn _in_worker(py: Python<'_>) -> bool {
    !mipyre_subinterp::attach::on_main(py)
}

/// Wait up to the time a stopped worker thread needs to end its interpreter, and return how
/// many sub-interpreters threads still own then; the process must not finalize while any is.
#[pyo3::pyfunction]
fn _owned_interpreters_at_exit(py: Python<'_>) -> usize {
    let deadline = std::time::Instant::now() + crate::serve::TEARDOWN_MARGIN;
    py.detach(|| mipyre_subinterp::wait_for_owned_interpreters(deadline))
}

/// Return the physical cores as the server counts them, which is the default number of
/// process-lane workers; on Apple Silicon only performance cores count.
#[pyo3::pyfunction]
fn _physical_cores() -> usize {
    crate::boot::host_cpus().cpus.physical().get()
}

#[pymodule]
fn engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(
        crate::pyapi::alloc::_mimalloc_purge_delay,
        m
    )?)?;
    m.add_class::<crate::pyapi::App>()?;
    m.add_class::<crate::pyapi::Server>()?;
    m.add_class::<crate::lane::LaneSet>()?;
    m.add_class::<crate::lane::StartStrategy>()?;
    m.add("BUILD_ID", env!("MIPYRE_BUILD_ID"))?;
    m.add_function(pyo3::wrap_pyfunction!(crate::lane::_driver_name, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        crate::lane::_driver_start_template,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        crate::lane::_driver_start_import_worker,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::lane::_driver_fork_worker, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_routers_to_load, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        crate::runtime::_set_routers_to_load,
        m
    )?)?;
    m.add("NoWorkerScript", m.py().get_type::<crate::runtime::NoWorkerScript>())?;
    m.add(
        "RegistrationSealed",
        m.py().get_type::<crate::pyapi::RegistrationSealed>(),
    )?;
    m.add_class::<crate::boot::InterpreterMode>()?;
    m.add_class::<crate::boot::SizeOrigin>()?;
    m.add_class::<crate::pyapi::config::PyGivenSize>()?;
    m.add_class::<crate::pyapi::config::PyLogLevel>()?;
    m.add_class::<crate::pyapi::config::Compression>()?;
    m.add_class::<crate::exchange::Request>()?;
    m.add_class::<crate::exchange::Response>()?;
    m.add_class::<crate::exchange::RequestHeaders>()?;
    m.add_class::<crate::exchange::WebSocket>()?;
    m.add_class::<crate::pyapi::state::AppStateView>()?;
    m.add_class::<crate::exchange::ResponseStream>()?;
    m.add_class::<crate::exchange::BodyStream>()?;
    m.add_class::<crate::runtime::AbandonedWorker>()?;
    m.add(
        "WorkerException",
        m.py().get_type::<crate::runtime::WorkerException>(),
    )?;
    m.add(
        "BodyRejected",
        m.py().get_type::<crate::exchange::BodyRejected>(),
    )?;
    crate::db::register(m)?;
    m.add_class::<crate::pyapi::config::Metrics>()?;
    m.add_function(pyo3::wrap_pyfunction!(
        crate::pyapi::config::get_gil_metrics,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::telemetry::reset_peaks, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        crate::pyapi::config::init_logger,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        crate::pyapi::config::flush_logs,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        crate::telemetry::emit_python_log,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        crate::telemetry::_python_log_level,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(workrequest_counts, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(_in_worker, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(_owned_interpreters_at_exit, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(_physical_cores, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(_route_params, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_worker_recv, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_worker_close, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_worker_send, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_worker_fail, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_worker_timed_out, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        crate::runtime::_worker_to_response,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        crate::runtime::_worker_app_handlers,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_worker_app_hooks, m)?)?;
    #[cfg(feature = "leak_detect")]
    m.add_function(pyo3::wrap_pyfunction!(leak_detect_dump, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_note_isolated, m)?)?;
    #[cfg(feature = "fault_injection")]
    m.add_function(pyo3::wrap_pyfunction!(_fault_panic, m)?)?;
    Ok(())
}
