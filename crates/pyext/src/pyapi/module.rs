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

/// Return how many threads that own a sub-interpreter run Python, which the process must not
/// finalize under; when none does, wait without a deadline until every one has ended its
/// interpreter (0) or one enters Python.
#[pyo3::pyfunction]
fn _interpreters_running_python_at_exit(py: Python<'_>) -> usize {
    match py.detach(mipyre_subinterp::decide_exit) {
        mipyre_subinterp::ExitDecision::Finalize => 0,
        mipyre_subinterp::ExitDecision::ExitWithoutFinalizing { running_python } => running_python.get(),
    }
}

/// Return the physical cores as the server counts them, which is the default number of
/// process-lane workers; on Apple Silicon only performance cores count.
#[pyo3::pyfunction]
fn _physical_cores() -> usize {
    crate::boot::host_cpus().cpus.physical().get()
}

/// Return whether calling `handler` gives an awaitable, which sends its route to the async
/// workers: a coroutine function, or a class-based view whose `__call__` is one.
///
/// Raises whatever `inspect.iscoroutinefunction` or reading `__call__` raises.
#[pyo3::pyfunction]
fn _is_async_handler(handler: &Bound<'_, PyAny>) -> PyResult<bool> {
    crate::pyapi::is_async_handler(handler)
}

/// Return `value` encoded as JSON bytes, exactly as a handler's returned `dict` or `list` is.
///
/// Sets and frozensets become lists, `UUID` and `Decimal` strings, dates and datetimes ISO 8601
/// strings, and a non-finite float `null`.
///
/// An integer of any width is written in full, as `json.dumps` writes it.
///
/// Raises `TypeError` for a value it cannot encode: another type or a non-`str` dict key; and
/// `ValueError`, as `json.dumps` does, for an integer with more digits than
/// `sys.get_int_max_str_digits()` allows.
#[pyo3::pyfunction]
fn encode_json<'py>(py: Python<'py>, value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
    crate::exchange::json::encode(py, value)
}

#[pymodule]
fn engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Each interpreter's `mipyre.engine` holds its own (PEP 489 module state, b/335 D13).
    m.set_state(crate::runtime::EngineModule::default())?;
    m.add_function(wrap_pyfunction!(crate::pyapi::alloc::_mimalloc_purge_delay, m)?)?;
    m.add_class::<crate::pyapi::App>()?;
    m.add_class::<crate::pyapi::PyRuntimeChoice>()?;
    m.add_class::<crate::pyapi::Server>()?;
    m.add_class::<crate::runtime::process::LaneSet>()?;
    m.add_class::<crate::runtime::CommandLine>()?;
    m.add_class::<crate::runtime::process::StartStrategy>()?;
    m.add_class::<crate::runtime::process::FailurePolicy>()?;
    m.add("BUILD_ID", env!("MIPYRE_BUILD_ID"))?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::process::_driver_name, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        crate::runtime::process::_driver_start_template,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        crate::runtime::process::_driver_spawn_template,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        crate::runtime::process::_driver_start_import_worker,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::process::_driver_fork_worker, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_routers_to_load, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_set_routers_to_load, m)?)?;
    m.add(
        "DeclarationError",
        m.py().get_type::<crate::runtime::DeclarationError>(),
    )?;
    m.add(
        "MissingAppFileError",
        m.py().get_type::<crate::runtime::MissingAppFileError>(),
    )?;
    m.add(
        "LateRegistrationError",
        m.py().get_type::<crate::pyapi::LateRegistrationError>(),
    )?;
    m.add_class::<crate::pyapi::config::PyLogLevel>()?;
    m.add_class::<crate::pyapi::config::Compression>()?;
    m.add_class::<crate::exchange::Request>()?;
    m.add_class::<crate::exchange::Response>()?;
    m.add_class::<crate::exchange::Cookie>()?;
    m.add_class::<crate::exchange::SameSite>()?;
    m.add_class::<crate::exchange::RequestHeaders>()?;
    m.add_class::<crate::exchange::WebSocket>()?;
    m.add_class::<crate::pyapi::state::AppStateView>()?;
    m.add_class::<crate::exchange::ResponseStream>()?;
    m.add_class::<crate::exchange::BodyStream>()?;
    m.add_class::<crate::runtime::AbandonedWorker>()?;
    m.add("WorkerException", m.py().get_type::<crate::runtime::WorkerException>())?;
    m.add("BodyReadError", m.py().get_type::<crate::exchange::PyBodyReadError>())?;
    m.add_class::<crate::exchange::HttpException>()?;
    crate::db::register(m)?;
    m.add_class::<crate::pyapi::config::Metrics>()?;
    m.add_class::<crate::pyapi::config::RunMetrics>()?;
    m.add_class::<crate::boot::Flag>()?;
    m.add_class::<crate::boot::Deployment>()?;
    m.add_class::<crate::boot::ServerFlags>()?;
    m.add_class::<crate::runtime::process::LaneRecipe>()?;
    m.add_class::<crate::boot::EntryPoint>()?;
    m.add("ConfigError", m.py().get_type::<crate::boot::ConfigErrorException>())?;
    m.add_function(pyo3::wrap_pyfunction!(crate::pyapi::config::_flags, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::pyapi::config::get_gil_metrics, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::telemetry::reset_peaks, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::pyapi::config::init_logger, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::pyapi::config::flush_logs, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::pyapi::config::_stop_log_writer, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::pyapi::config::_start_log_writer, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::pyapi::config::_user_cache_dir, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::pyapi::config::_log_writer_thread, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::telemetry::emit_python_log, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::telemetry::_python_log_level, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(workrequest_counts, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(_in_worker, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(_interpreters_running_python_at_exit, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(_physical_cores, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(_route_params, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(encode_json, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(_is_async_handler, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_worker_recv, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_worker_close, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_worker_send, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_worker_fail, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_worker_timed_out, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_worker_to_response, m)?)?;
    #[cfg(feature = "leak_detect")]
    m.add_function(pyo3::wrap_pyfunction!(leak_detect_dump, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(crate::runtime::_note_isolated, m)?)?;
    #[cfg(feature = "fault_injection")]
    m.add_function(pyo3::wrap_pyfunction!(_fault_panic, m)?)?;
    Ok(())
}
