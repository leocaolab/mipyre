//! Converts the engine's configuration values to and from Python: `LogLevel`, `Compression`,
//! the `Metrics` snapshot, and the flags (`Flag`, `Deployment`, `ServerFlags`).
//!
//! Each value belongs to the layer that uses it. A Python class with the owner's exact shape is
//! the owner's type with its Python methods here; one with a shape of its own is a separate type
//! converted once at this edge.

use pyo3::prelude::*;

use mipyre_http::plan::compression::{
    BrotliQuality, CompressionSettings, CompressionSettingsError, EnabledCodecs, GzipLevel,
};

use crate::boot::{cli_option, env_variable, CodeCall, Deployment, EntryPoint, Flag, FlagScope, ServerFlags, FLAGS};
use crate::telemetry;

/// Returns the name of `arg`'s type for a `TypeError`, or what reading it raised.
fn type_name(arg: &Bound<'_, PyAny>) -> String {
    arg.get_type().name().map_or_else(
        |e| format!("an object whose type name could not be read ({e})"),
        |n| n.to_string(),
    )
}

/// The minimum level that is logged.
#[pyclass(module = "mipyre.engine", name = "LogLevel", eq, eq_int, frozen, hash)]
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
pub(crate) fn init_logger(level: &Bound<'_, PyAny>, access_log: bool, format: &str) -> PyResult<()> {
    Ok(telemetry::init_logger(level_from_arg(level)?, access_log, format)?)
}

/// Block until every line logged before the call is on stderr; return at once when no logger
/// is installed.
///
/// The logger writes from a background thread that process exit does not wait for, so
/// `Mipyre.run()` calls this as it ends. Raises `RuntimeError` when the writer thread has
/// stopped and the lines may never be written.
/// The user's cache directory by the OS's convention (`~/Library/Caches` on macOS; elsewhere an
/// absolute `$XDG_CACHE_HOME`, else `~/.cache`), or `None` when this user has no home directory.
#[pyfunction]
pub(crate) fn _user_cache_dir() -> Option<std::path::PathBuf> {
    crate::boot::user_cache_dir()
}

#[pyfunction]
pub(crate) fn flush_logs(py: Python<'_>) -> PyResult<()> {
    Ok(telemetry::flush_logs(py)?)
}

/// Stop the log's writer thread, so this process runs no thread of the logger's (it is about to
/// fork), and return whether one was running. Until `_start_log_writer()` each thread that logs
/// writes its lines itself; the lines logged before are written out first.
#[pyfunction]
pub(crate) fn _stop_log_writer(py: Python<'_>) -> PyResult<bool> {
    Ok(telemetry::stop_log_writer(py)?)
}

/// Start the log's writer thread again after `_stop_log_writer()`; a no-op with no logger
/// installed or its thread running.
#[pyfunction]
pub(crate) fn _start_log_writer() -> PyResult<()> {
    Ok(telemetry::start_log_writer()?)
}

/// The name of the log's writer thread, as a process's thread list shows it.
#[pyfunction]
pub(crate) fn _log_writer_thread() -> &'static str {
    telemetry::WRITER_THREAD
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
    fn py_new(min_size: i64, gzip: bool, brotli: bool, gzip_level: i64, brotli_quality: i64) -> PyResult<Self> {
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

    /// Return how many static answers sent from disk were cut off because the file changed or
    /// failed while it was sent.
    #[getter]
    fn static_answers_cut(&self) -> u64 {
        self.0.static_answers_cut
    }

    /// Return how many async database results were dropped because their asyncio loop had
    /// closed.
    #[getter]
    fn db_results_undeliverable(&self) -> u64 {
        self.0.db_results_undeliverable
    }

    /// Return how many log lines were dropped, because the log's writer could not keep up or
    /// because stderr refused them.
    #[getter]
    fn log_lines_dropped(&self) -> u64 {
        let dropped = self.0.log_lines_dropped;
        dropped.queue_full + dropped.write_failed
    }

    /// Return how many of the dropped log lines stderr refused (EPIPE, a closed or full disk).
    #[getter]
    fn log_lines_write_failed(&self) -> u64 {
        self.0.log_lines_dropped.write_failed
    }

    /// Return how many GIL acquisition waits exceeded 50 ms.
    #[getter]
    fn gil_congested_waits(&self) -> u64 {
        self.0.gil_congested_waits
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

    /// Return how many trusted proxies' `X-Forwarded-For` headers were ignored because the entry
    /// where the client was looked for is no IP address.
    #[getter]
    fn forwarded_for_unreadable(&self) -> u64 {
        self.0.forwarded_for_unreadable
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

    /// Return how many sync workers the pools abandoned are still running their requests.
    #[getter]
    fn workers_abandoned(&self) -> u64 {
        self.0.workers_abandoned
    }

    /// Return how many pools are at their limit of abandoned workers; `/readyz` is not ready
    /// while one is.
    #[getter]
    fn pools_at_abandon_limit(&self) -> u64 {
        self.0.pools_at_abandon_limit
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.0).replacen("Counters", "Metrics", 1)
    }
}

/// One server run's counts, read at one moment (`Server.metrics()`): what this server did,
/// apart from any other server of the process.
#[pyclass(frozen, name = "RunMetrics", module = "mipyre.engine")]
#[derive(Debug, Clone)]
pub(crate) struct RunMetrics {
    pub(crate) counts: mipyre_server::ServerCounts,
    pub(crate) workers: mipyre_runtime::WorkerTally,
    pub(crate) static_bytes_in_flight: u64,
    /// What the run runs without, as `component: reason`, from each part that holds it.
    pub(crate) degraded: Vec<String>,
}

#[pymethods]
impl RunMetrics {
    /// Return how many requests the server answered itself because Python could not: 503
    /// overloaded or shutting down, 504 past the budget, 502 a dead process-lane worker, 500 a
    /// dropped request.
    #[getter]
    fn dropped_requests(&self) -> u64 {
        self.counts.refused
    }

    /// Return why the server was sized from a fallback CPU count, or `None` when the OS gave
    /// the counts.
    #[getter]
    fn cpu_sizing_fallback(&self) -> Option<String> {
        self.counts.cpu_sizing_fallback.clone()
    }

    /// Return how many requests were served, or `None` while the run does not count its answers (its `metrics` flag is off).
    #[getter]
    fn total_requests(&self) -> Option<u64> {
        self.counts.answers.as_ref().map(mipyre_server::AnswerCounts::total)
    }

    /// Return how many workers are serving now.
    #[getter]
    fn workers_serving(&self) -> usize {
        self.workers.workers_serving
    }

    /// Return how many workers stopped serving on their own while the server ran.
    #[getter]
    fn workers_died(&self) -> usize {
        self.workers.workers_died
    }

    /// Return how many `runtime="process"` worker processes died (crashed, were killed, timed
    /// out) and were replaced while the server ran.
    #[getter]
    fn process_workers_died(&self) -> usize {
        self.workers.process_workers_died
    }

    /// Return how many `gc.collect()` calls raised or panicked; each retired its worker.
    #[getter]
    fn gc_failures(&self) -> usize {
        self.workers.gc_failures()
    }

    /// Return how many workers were retired because their `gc.collect()` failed.
    #[getter]
    fn workers_retired(&self) -> usize {
        self.workers.workers_retired
    }

    /// Return why the last worker retired, in its own words, or `None` while none has.
    #[getter]
    fn last_worker_retirement(&self) -> Option<String> {
        self.workers.last_worker_retirement.clone()
    }

    /// Return how many bytes the server's static answers sent from disk hold now, read and not
    /// yet written out.
    #[getter]
    fn static_bytes_in_flight(&self) -> u64 {
        self.static_bytes_in_flight
    }

    /// Return how many requests were answered 422 because their `body=` model or `query=`
    /// string failed validation.
    #[getter]
    fn validation_failures(&self) -> u64 {
        self.counts.validation_failures
    }

    /// Return how many WebSocket clients were pinged and closed with 1011 because no frame came
    /// in time.
    #[getter]
    fn ws_keepalive_timeouts(&self) -> u64 {
        self.counts.ws_keepalive_timeouts
    }

    /// Return how many threads could not be pinned to their core and run unpinned.
    #[getter]
    fn cpu_pin_failures(&self) -> u64 {
        self.counts.cpu_pin_failures
    }

    /// Return how many macOS threads could not raise their QoS class and may run on
    /// efficiency cores.
    #[getter]
    fn cpu_qos_failures(&self) -> u64 {
        self.counts.cpu_qos_failures
    }

    /// Return how many reads of the process's CPU affinity failed, leaving threads unpinned.
    #[getter]
    fn cpu_affinity_unreadable(&self) -> u64 {
        self.counts.cpu_affinity_unreadable
    }

    /// Return how many reads of the physical core topology failed, leaving threads unpinned.
    #[getter]
    fn cpu_topology_unreadable(&self) -> u64 {
        self.counts.cpu_topology_unreadable
    }

    /// Return how many starts had more TPC threads than CPUs, leaving the extra threads
    /// unpinned.
    #[getter]
    fn cpu_threads_beyond_cpus(&self) -> u64 {
        self.counts.cpu_threads_beyond_cpus
    }

    /// Return how many drains of stopped accept loops ran out of time.
    #[getter]
    fn drain_timeouts(&self) -> u64 {
        self.counts.drain_timeouts
    }

    /// Return how many connections were still open, and were closed, when such a drain gave
    /// up.
    #[getter]
    fn drain_closed_connections(&self) -> u64 {
        self.counts.drain_closed_connections
    }

    /// Return how many stop watchers, the threads that stop a server run on SIGINT or SIGTERM,
    /// panicked or were still running when their run ended.
    #[getter]
    fn stop_watcher_failures(&self) -> u64 {
        self.counts.stop_watcher_failures
    }

    /// Return how many main-interpreter handlers were still running when their TPC thread's
    /// runtime shut down, and were left running.
    #[getter]
    fn blocking_calls_left(&self) -> u64 {
        self.counts.blocking_calls_left
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }

    /// Return what this server runs without, as ``component: reason`` lines: CPU pinning, a
    /// crash-looping process lane, a static mount served uncached; empty while it runs as
    /// configured.
    #[getter]
    fn degraded(&self) -> Vec<String> {
        self.degraded.clone()
    }
}

/// Return a snapshot of the process's counters without resetting any: the main GIL, RSS, the
/// connection-level events and what runs degraded. A server's own counts are its
/// `Server.metrics()`.
#[pyfunction]
pub(crate) fn get_gil_metrics() -> Metrics {
    Metrics(telemetry::read_counters())
}

/// The face Python reads of one setting of the declaration table (`boot/settings.rs`).
#[pymethods]
impl Flag {
    #[getter]
    fn name(&self) -> &'static str {
        self.name
    }

    /// The `mipyre run` option, `--threads`.
    #[getter]
    fn cli(&self) -> String {
        cli_option(self.name)
    }

    /// The environment variable, `MIPYRE_THREADS`.
    #[getter]
    fn env(&self) -> String {
        env_variable(self.name)
    }

    /// The `pyproject.toml` key in its table, `[tool.mipyre] threads`.
    #[getter]
    fn file(&self) -> String {
        self.file.to_string()
    }

    /// Every place app code gives it, `ServerFlags(threads=)`.
    #[getter]
    fn code(&self) -> Vec<String> {
        self.code.iter().map(ToString::to_string).collect()
    }

    /// The shape of its value in one word: `bool`, `count`, `positive`, `duration`,
    /// `positive_duration`, `port`, `ports`, `host`, `hosts`, `path`, `slot_bytes`, `choice`.
    #[getter]
    fn shape(&self) -> &'static str {
        self.shape.word()
    }

    /// What its value must be, as an error says it.
    #[getter]
    fn expected(&self) -> String {
        self.shape.expected()
    }

    #[getter]
    fn default(&self) -> &'static str {
        self.default
    }

    /// Whether it is about the whole process (`reload`, `log`) rather than one server.
    #[getter]
    fn process_wide(&self) -> bool {
        self.scope == FlagScope::Process
    }

    #[getter]
    fn doc(&self) -> &'static str {
        self.doc
    }

    fn __repr__(&self) -> String {
        format!(
            "Flag({:?}, {}, {})",
            self.name,
            cli_option(self.name),
            env_variable(self.name)
        )
    }
}

/// Every mipyre setting, in the order `--help` and the docs list them.
#[pyfunction]
pub(crate) fn _flags() -> Vec<Flag> {
    FLAGS.to_vec()
}

/// What the deployment gave, resolved once at an entry point; Python makes it and hands it to the
/// server it starts.
#[pymethods]
impl Deployment {
    /// No deployment at all: what an embedded engine (`TestClient`, a raw `start()`) uses.
    #[new]
    fn new() -> Self {
        Deployment::default()
    }

    /// Reads this process's environment and the `pyproject.toml` in its working directory, once,
    /// with `flags`, the command line's values by flag name as typed.
    ///
    /// # Errors
    ///
    /// Raises `ConfigError` with every value that cannot be used, and `ValueError` for a name in
    /// `flags` that is no flag.
    #[staticmethod]
    #[pyo3(name = "from_process")]
    fn from_process_py(flags: std::collections::BTreeMap<String, String>, entry: EntryPoint) -> PyResult<Self> {
        let flags = flags
            .into_iter()
            .map(|(name, raw)| match FLAGS.iter().find(|flag| flag.name == name) {
                Some(flag) => Ok((flag.name, raw)),
                None => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "{name:?} is not a mipyre flag"
                ))),
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Deployment::from_process(&flags, entry)?)
    }

    #[getter]
    fn entry(&self) -> EntryPoint {
        self.entry
    }

    /// One line per value the deployment gave: the flag, the value and where it came from.
    #[pyo3(name = "summary")]
    fn summary_lines(&self) -> Vec<String> {
        self.summary()
    }

    /// Whether Python logging is on: the deployment's `log`, else the app's `debug=` (`code`),
    /// else `default` (on under `mipyre dev`).
    #[pyo3(name = "log", signature = (code, default))]
    fn log_on(&self, code: Option<bool>, default: bool) -> bool {
        self.log.as_ref().map_or(code.unwrap_or(default), |given| given.value)
    }

    /// The app's process lanes: this deployment laid over `config` (with the code's
    /// `ProcessLanes`), workers defaulting to `physical_cores`.
    #[pyo3(name = "lanes")]
    fn lanes_py(
        &self,
        config: PyRef<'_, ServerFlags>,
        physical_cores: usize,
    ) -> PyResult<super::process::PyLaneRecipe> {
        let cores = std::num::NonZeroUsize::new(physical_cores)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("physical_cores must be at least 1"))?;
        Ok(Deployment::lanes(self, &config, cores).into())
    }

    /// Whether to reload on file changes: the deployment's `reload`, else `run(reload=)`.
    #[pyo3(name = "reload", signature = (code))]
    fn reload_on(&self, code: Option<bool>) -> bool {
        self.reload.as_ref().map_or(code.unwrap_or(false), |given| given.value)
    }
}

/// Writes a value Python code gave a flag as the text a flag of its shape is written in: a
/// bool as `true` / `false`, a list or tuple as its items joined by commas, a path as its
/// `os.fspath`, anything else as its `str`.
fn flag_text(value: &Bound<'_, PyAny>) -> PyResult<String> {
    use pyo3::types::{PyBool, PyList, PyTuple};
    if let Ok(b) = value.cast::<PyBool>() {
        return Ok(if b.is_true() { "true" } else { "false" }.to_string());
    }
    if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
        let items: PyResult<Vec<String>> = value.try_iter()?.map(|item| flag_text(&item?)).collect();
        return Ok(items?.join(","));
    }
    let os = value.py().import("os")?;
    if value.is_instance(&os.getattr("PathLike")?)? {
        return os.call_method1("fspath", (value,))?.str()?.extract();
    }
    value.str()?.extract()
}

/// Reads the keywords Python code passed `call` into `flags`; a keyword given `None` is a
/// keyword not given.
fn give_all(flags: &mut ServerFlags, call: CodeCall, given: Option<&Bound<'_, pyo3::types::PyDict>>) -> PyResult<()> {
    for (keyword, value) in given.into_iter().flatten() {
        if value.is_none() {
            continue;
        }
        let keyword: String = keyword.extract()?;
        flags.give(call, &keyword, &flag_text(&value)?)?;
    }
    Ok(())
}

/// The flag values app code gives one server: `ServerFlags(threads=2)`, passed to `run()`,
/// `start(config=)` or `TestClient(config=)`.
#[pymethods]
impl ServerFlags {
    /// # Errors
    ///
    /// Raises `ConfigError` for a value of the wrong shape and `TypeError` for a keyword that
    /// is no flag.
    #[new]
    #[pyo3(signature = (**given))]
    fn new(given: Option<&Bound<'_, pyo3::types::PyDict>>) -> PyResult<Self> {
        let mut flags = ServerFlags::default();
        give_all(&mut flags, CodeCall::ServerFlags, given)?;
        Ok(flags)
    }

    /// These flags with `given` set as well, each named as `ServerFlags(name=)`: what a
    /// `TestServer` adds its host and port with.
    #[pyo3(signature = (**given))]
    fn _with(&self, given: Option<&Bound<'_, pyo3::types::PyDict>>) -> PyResult<Self> {
        let mut flags = self.clone();
        give_all(&mut flags, CodeCall::ServerFlags, given)?;
        Ok(flags)
    }

    /// These flags with the app's `ProcessLanes` fields (`given`), each named as
    /// `ProcessLanes(name=)`.
    #[pyo3(signature = (**given))]
    fn _with_lanes(&self, given: Option<&Bound<'_, pyo3::types::PyDict>>) -> PyResult<Self> {
        let mut flags = self.clone();
        give_all(&mut flags, CodeCall::ProcessLanes, given)?;
        Ok(flags)
    }
}
