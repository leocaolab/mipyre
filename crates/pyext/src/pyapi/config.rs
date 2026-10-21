//! Converts the engine's configuration values to and from Python: `Mode`, `LogLevel`,
//! `GivenSize`, `Compression` and the `Metrics` snapshot.
//!
//! Each value belongs to the layer that uses it. A Python class with the owner's exact shape is
//! the owner's type with its Python methods here; one with a shape of its own is a separate type
//! converted once at this edge.

use pyo3::prelude::*;

use mipyre_http::plan::compression::{
    BrotliQuality, CompressionSettings, CompressionSettingsError, EnabledCodecs, GzipLevel,
};

use crate::boot;
use crate::telemetry;

/// Returns the name of `arg`'s type for a `TypeError`, or what reading it raised.
fn type_name(arg: &Bound<'_, PyAny>) -> String {
    arg.get_type().name().map_or_else(
        |e| format!("an object whose type name could not be read ({e})"),
        |n| n.to_string(),
    )
}

/// A count `start()` is given, with where it came from.
///
/// The value stays signed and unchecked so that `start()` raises the same `ValueError` for
/// `-1` as for `0`, instead of pyo3's `OverflowError`.
#[pyclass(module = "mipyre.engine", name = "GivenSize", frozen, from_py_object)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PyGivenSize {
    #[pyo3(get)]
    pub(crate) value: i64,
    #[pyo3(get)]
    pub(crate) origin: boot::SizeOrigin,
}

#[pymethods]
impl PyGivenSize {
    #[new]
    fn new(value: i64, origin: boot::SizeOrigin) -> Self {
        Self { value, origin }
    }
}

#[pymethods]
impl boot::InterpreterMode {
    /// Return the mode `name` spells: `"gil"` or `"default"`, `"subinterp"` or `"auto"`;
    /// anything else raises `ValueError`.
    #[staticmethod]
    fn parse(name: &str) -> PyResult<Self> {
        Ok(name.parse::<boot::InterpreterMode>()?)
    }

    /// Return whether this mode runs handlers in sub-interpreter workers.
    #[getter]
    fn uses_workers(&self) -> bool {
        *self == boot::InterpreterMode::Subinterp
    }

    fn __str__(&self) -> &'static str {
        self.as_str()
    }
}

/// Parses a `mode=` argument, which is a `Mode` or its name.
pub(crate) fn mode_from_arg(arg: &Bound<'_, PyAny>) -> PyResult<boot::InterpreterMode> {
    if let Ok(mode) = arg.cast::<boot::InterpreterMode>() {
        return Ok(*mode.get());
    }
    let name: &str = arg.extract().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(format!(
            "mode must be a mipyre.engine.Mode or a str, got {}",
            type_name(arg)
        ))
    })?;
    Ok(name.parse::<boot::InterpreterMode>()?)
}

/// The minimum level that is logged.
#[pyclass(
    module = "mipyre.engine",
    name = "LogLevel",
    eq,
    eq_int,
    frozen,
    hash
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PyLogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl From<telemetry::LogLevel> for PyLogLevel {
    fn from(level: telemetry::LogLevel) -> Self {
        match level {
            telemetry::LogLevel::Off => PyLogLevel::Off,
            telemetry::LogLevel::At(telemetry::RecordLevel::Error) => PyLogLevel::Error,
            telemetry::LogLevel::At(telemetry::RecordLevel::Warn) => PyLogLevel::Warn,
            telemetry::LogLevel::At(telemetry::RecordLevel::Info) => PyLogLevel::Info,
            telemetry::LogLevel::At(telemetry::RecordLevel::Debug) => PyLogLevel::Debug,
            telemetry::LogLevel::At(telemetry::RecordLevel::Trace) => PyLogLevel::Trace,
        }
    }
}

impl From<PyLogLevel> for telemetry::LogLevel {
    fn from(level: PyLogLevel) -> Self {
        match level {
            PyLogLevel::Off => telemetry::LogLevel::Off,
            PyLogLevel::Error => telemetry::LogLevel::At(telemetry::RecordLevel::Error),
            PyLogLevel::Warn => telemetry::LogLevel::At(telemetry::RecordLevel::Warn),
            PyLogLevel::Info => telemetry::LogLevel::At(telemetry::RecordLevel::Info),
            PyLogLevel::Debug => telemetry::LogLevel::At(telemetry::RecordLevel::Debug),
            PyLogLevel::Trace => telemetry::LogLevel::At(telemetry::RecordLevel::Trace),
        }
    }
}

#[pymethods]
impl PyLogLevel {
    /// Return the level `name` spells in any case, where `WARNING` also means `WARN`;
    /// anything else raises `ValueError`.
    #[staticmethod]
    fn parse(name: &str) -> PyResult<Self> {
        Ok(name.parse::<telemetry::LogLevel>()?.into())
    }

    fn __str__(&self) -> &'static str {
        match self {
            Self::Off => "OFF",
            Self::Error => "ERROR",
            Self::Warn => "WARN",
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
            Self::Trace => "TRACE",
        }
    }
}

/// Parses a `level=` argument, which is a `LogLevel` or its name.
fn level_from_arg(arg: &Bound<'_, PyAny>) -> PyResult<telemetry::LogLevel> {
    if let Ok(level) = arg.cast::<PyLogLevel>() {
        return Ok((*level.get()).into());
    }
    let name: &str = arg.extract().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(format!(
            "level must be a mipyre.engine.LogLevel or a str, got {}",
            type_name(arg)
        ))
    })?;
    Ok(name.parse::<telemetry::LogLevel>()?)
}

/// Initialize the Rust logger, or reconfigure it if already initialized.
///
/// `level` is a `LogLevel` or its name, `access_log=False` suppresses the access log, and
/// `format` is `"text"` or `"json"`. Raises `ValueError` on an unknown level or format, and
/// `RuntimeError` when another global subscriber is already installed.
#[pyfunction]
#[pyo3(signature = (level, access_log, format))]
pub(crate) fn init_logger(
    level: &Bound<'_, PyAny>,
    access_log: bool,
    format: &str,
) -> PyResult<()> {
    Ok(telemetry::init_logger(
        level_from_arg(level)?,
        access_log,
        format,
    )?)
}

/// Block until every line logged before the call is on stderr; return at once when no logger
/// is installed.
///
/// The logger writes from a background thread that process exit does not wait for, so
/// `Mipyre.run()` calls this as it ends. Raises `RuntimeError` when the writer thread has
/// stopped and the lines may never be written.
#[pyfunction]
pub(crate) fn flush_logs(py: Python<'_>) -> PyResult<()> {
    Ok(telemetry::flush_logs(py)?)
}

/// Bodies smaller than this many bytes cost more to compress than the bytes they save.
const DEFAULT_MIN_SIZE: u32 = 512;
const DEFAULT_GZIP_LEVEL: i64 = 6;
const DEFAULT_BROTLI_QUALITY: i64 = 4;

fn settings_error_to_py(e: CompressionSettingsError) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(e.to_string())
}

/// An app's compression settings as Python gives them.
///
/// A level out of its codec's range, or neither codec, raises `ValueError` at construction
/// rather than being clamped later. It keeps every level as given, so Python reads back the
/// level of a codec it turned off.
#[pyclass(frozen, name = "Compression", module = "mipyre.engine")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Compression {
    min_size: u32,
    codings: Codings,
    gzip_level: GzipLevel,
    brotli_quality: BrotliQuality,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Codings {
    Gzip,
    Brotli,
    Both,
}

impl Codings {
    fn of(codecs: EnabledCodecs) -> Self {
        match codecs {
            EnabledCodecs::Gzip(_) => Codings::Gzip,
            EnabledCodecs::Brotli(_) => Codings::Brotli,
            EnabledCodecs::Both(..) => Codings::Both,
        }
    }

    fn gzip(self) -> bool {
        matches!(self, Codings::Gzip | Codings::Both)
    }

    fn brotli(self) -> bool {
        matches!(self, Codings::Brotli | Codings::Both)
    }
}

impl Compression {
    /// Returns the settings the engine compresses with, holding only the codecs turned on.
    pub(crate) fn settings(&self) -> CompressionSettings {
        let codecs = match self.codings {
            Codings::Gzip => EnabledCodecs::Gzip(self.gzip_level),
            Codings::Brotli => EnabledCodecs::Brotli(self.brotli_quality),
            Codings::Both => EnabledCodecs::Both(self.gzip_level, self.brotli_quality),
        };
        CompressionSettings {
            min_size: self.min_size,
            codecs,
        }
    }
}

#[pymethods]
impl Compression {
    #[new]
    #[pyo3(signature = (
        *,
        min_size = i64::from(DEFAULT_MIN_SIZE),
        gzip = true,
        brotli = true,
        gzip_level = DEFAULT_GZIP_LEVEL,
        brotli_quality = DEFAULT_BROTLI_QUALITY,
    ))]
    fn py_new(
        min_size: i64,
        gzip: bool,
        brotli: bool,
        gzip_level: i64,
        brotli_quality: i64,
    ) -> PyResult<Self> {
        CompressionSettings::parse(min_size, gzip, brotli, gzip_level, brotli_quality)
            .and_then(|settings| {
                Ok(Self {
                    min_size: settings.min_size,
                    codings: Codings::of(settings.codecs),
                    gzip_level: GzipLevel::new(gzip_level)?,
                    brotli_quality: BrotliQuality::new(brotli_quality)?,
                })
            })
            .map_err(settings_error_to_py)
    }

    #[getter]
    fn min_size(&self) -> u32 {
        self.min_size
    }

    #[getter]
    fn gzip(&self) -> bool {
        self.codings.gzip()
    }

    #[getter]
    fn brotli(&self) -> bool {
        self.codings.brotli()
    }

    #[getter]
    fn gzip_level(&self) -> u8 {
        self.gzip_level.get()
    }

    #[getter]
    fn brotli_quality(&self) -> u8 {
        self.brotli_quality.get()
    }

    fn __repr__(&self) -> String {
        format!(
            "Compression(min_size={}, gzip={}, brotli={}, gzip_level={}, brotli_quality={})",
            self.min_size,
            py_bool(self.codings.gzip()),
            py_bool(self.codings.brotli()),
            self.gzip_level.get(),
            self.brotli_quality.get()
        )
    }
}

fn py_bool(b: bool) -> &'static str {
    if b {
        "True"
    } else {
        "False"
    }
}

/// A snapshot of the engine's counters; reading it clears nothing, and only `reset_peaks()`
/// clears the peaks.
#[pyclass(frozen, name = "Metrics", module = "mipyre.engine")]
#[derive(Debug, Clone)]
pub(crate) struct Metrics(telemetry::Counters);

#[pymethods]
impl Metrics {
    /// Return the latest GIL acquisition wait in microseconds.
    #[getter]
    fn gil_wait_last_us(&self) -> u64 {
        self.0.gil_wait_last_us
    }

    /// Return the longest GIL acquisition wait since `reset_peaks()`, in microseconds.
    #[getter]
    fn gil_wait_peak_us(&self) -> u64 {
        self.0.gil_wait_peak_us
    }

    /// Return how many handler invocations had their GIL wait measured.
    #[getter]
    fn gil_wait_count(&self) -> u64 {
        self.0.gil_wait_count
    }

    /// Return the sum of all measured GIL waits in microseconds.
    #[getter]
    fn gil_wait_total_us(&self) -> u64 {
        self.0.gil_wait_total_us
    }

    /// Return how many threads are waiting for the main GIL now.
    #[getter]
    fn gil_queue_length(&self) -> isize {
        self.0.gil_queue_length
    }

    /// Return the longest handler GIL hold since `reset_peaks()`, in microseconds.
    #[getter]
    fn gil_hold_peak_us(&self) -> u64 {
        self.0.gil_hold_peak_us
    }

    /// Return the sampled process RSS in bytes, or `None` before a successful read.
    #[getter]
    fn rss_bytes(&self) -> Option<u64> {
        self.0.rss_bytes
    }

    /// Return what runs degraded as `key: reason` lines; the list is empty while everything
    /// runs as configured.
    #[getter]
    fn degraded(&self) -> Vec<String> {
        self.0.degraded.clone()
    }

    /// Return how many requests the server answered itself because Python could not: 503
    /// overloaded or shutting down, 504 past the budget, 502 a dead process-lane worker, 500 a
    /// dropped request.
    #[getter]
    fn dropped_requests(&self) -> u64 {
        self.0.dropped_requests
    }

    /// Return how many requests were served, or `None` while `MIPYRE_METRICS` is unset.
    #[getter]
    fn total_requests(&self) -> Option<u64> {
        self.0.total_requests
    }

    /// Return how many workers are serving now.
    #[getter]
    fn workers_serving(&self) -> i64 {
        self.0.workers_serving
    }

    /// Return how many workers stopped serving on their own while the server ran.
    #[getter]
    fn workers_died(&self) -> u64 {
        self.0.workers_died
    }

    /// Return how many `gc.collect()` calls raised or panicked; each retired its worker.
    #[getter]
    fn gc_failures(&self) -> u64 {
        self.0.gc_failures
    }

    /// Return how many workers were retired because their `gc.collect()` failed.
    #[getter]
    fn workers_retired(&self) -> u64 {
        self.0.workers_retired
    }

    /// Return why the last worker retired, in its own words, or `None` while none has.
    #[getter]
    fn last_worker_retirement(&self) -> Option<String> {
        self.0.last_worker_retirement.clone()
    }

    /// Return how many pre-compressed static files could not be read, so the original was
    /// served.
    #[getter]
    fn precompressed_fallbacks(&self) -> u64 {
        self.0.precompressed_fallbacks
    }

    /// Return how many static mounts are served uncached because their file watch failed.
    #[getter]
    fn static_unwatched_mounts(&self) -> u64 {
        self.0.static_unwatched_mounts
    }

    /// Return how many async database results were dropped because their asyncio loop had
    /// closed.
    #[getter]
    fn db_results_undeliverable(&self) -> u64 {
        self.0.db_results_undeliverable
    }

    /// Return how many GIL acquisition waits exceeded 50 ms.
    #[getter]
    fn gil_congested_waits(&self) -> u64 {
        self.0.gil_congested_waits
    }

    /// Return why the server is sized from a fallback CPU count, or `None` when the OS gave
    /// the counts.
    #[getter]
    fn cpu_sizing_fallback(&self) -> Option<String> {
        self.0.cpu_sizing_fallback.clone()
    }

    /// Return how many times a client reset or closed its connection mid-IO; one connection
    /// can count twice, on its body read and its response write.
    #[getter]
    fn client_disconnects(&self) -> u64 {
        self.0.client_disconnects
    }

    /// Return how many `accept()` calls failed for reasons other than exhaustion or a peer
    /// going away.
    #[getter]
    fn accept_failures(&self) -> u64 {
        self.0.accept_failures
    }

    /// Return how many `accept()` calls failed for lack of file descriptors, sockets or kernel
    /// buffers.
    #[getter]
    fn accept_exhausted(&self) -> u64 {
        self.0.accept_exhausted
    }

    /// Return how many TLS handshakes failed, including probes and slow clients timing out.
    #[getter]
    fn tls_handshake_failures(&self) -> u64 {
        self.0.tls_handshake_failures
    }

    /// Return how many connections were closed with a 400 because the client sent no valid
    /// HTTP/1 request.
    #[getter]
    fn malformed_requests(&self) -> u64 {
        self.0.malformed_requests
    }

    /// Return how many connections were closed with a 414 or 431 because the request head
    /// exceeded the limits.
    #[getter]
    fn oversized_request_heads(&self) -> u64 {
        self.0.oversized_request_heads
    }

    /// Return how many connections were closed because no first request arrived within the
    /// request-head timeout.
    #[getter]
    fn first_request_timeouts(&self) -> u64 {
        self.0.first_request_timeouts
    }

    /// Return how many kept-alive HTTP/1 connections were closed because no next request head
    /// arrived within the request-head timeout.
    #[getter]
    fn request_head_timeouts(&self) -> u64 {
        self.0.request_head_timeouts
    }

    /// Return how many connections could not set TCP_QUICKACK and keep delayed ACKs.
    #[getter]
    fn quickack_failures(&self) -> u64 {
        self.0.quickack_failures
    }

    /// Return how many listeners could not set TCP_DEFER_ACCEPT.
    #[getter]
    fn defer_accept_failures(&self) -> u64 {
        self.0.defer_accept_failures
    }

    /// Return how many connections could not set TCP_NODELAY and keep Nagle's algorithm.
    #[getter]
    fn nodelay_failures(&self) -> u64 {
        self.0.nodelay_failures
    }

    /// Return how many threads could not be pinned to their core and run unpinned.
    #[getter]
    fn cpu_pin_failures(&self) -> u64 {
        self.0.cpu_pin_failures
    }

    /// Return how many macOS threads could not raise their QoS class and may run on
    /// efficiency cores.
    #[getter]
    fn cpu_qos_failures(&self) -> u64 {
        self.0.cpu_qos_failures
    }

    /// Return how many reads of the process's CPU affinity failed, leaving threads unpinned.
    #[getter]
    fn cpu_affinity_unreadable(&self) -> u64 {
        self.0.cpu_affinity_unreadable
    }

    /// Return how many reads of the physical core topology failed, leaving threads unpinned.
    #[getter]
    fn cpu_topology_unreadable(&self) -> u64 {
        self.0.cpu_topology_unreadable
    }

    /// Return how many starts had more TPC threads than CPUs, leaving the extra threads
    /// unpinned.
    #[getter]
    fn cpu_threads_beyond_cpus(&self) -> u64 {
        self.0.cpu_threads_beyond_cpus
    }

    /// Return how many drains of stopped accept loops ran out of time.
    #[getter]
    fn drain_timeouts(&self) -> u64 {
        self.0.drain_timeouts
    }

    /// Return how many connections were still open, and were closed, when such a drain gave
    /// up.
    #[getter]
    fn drain_closed_connections(&self) -> u64 {
        self.0.drain_closed_connections
    }

    /// Return how many stop watchers, the threads that stop a TPC run on SIGINT or SIGTERM,
    /// panicked or were still running when their run ended.
    #[getter]
    fn stop_watcher_failures(&self) -> u64 {
        self.0.stop_watcher_failures
    }

    /// Return how many main-interpreter handlers were still running when their TPC thread's
    /// runtime shut down, and were left running.
    #[getter]
    fn blocking_calls_left(&self) -> u64 {
        self.0.blocking_calls_left
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.0).replacen("Counters", "Metrics", 1)
    }
}

/// Return a snapshot of every counter without resetting any.
#[pyfunction]
pub(crate) fn get_gil_metrics() -> Metrics {
    Metrics(telemetry::read_counters())
}
