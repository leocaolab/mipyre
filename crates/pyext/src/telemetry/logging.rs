//! Installs the process's tracing subscriber and bridges Python `logging` records into it.
//!
//! A background thread writes every line, because at hundreds of thousands of access-log lines a
//! second, Tokio workers writing stderr themselves would serialize on its lock.

use std::io::{IsTerminal, Write};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Layer, Registry};

use super::log_writer::{FenceError, LogChannel, LogDrops, LogQueue, LogWriter};
use mipyre_server::{JsonLines, ACCESS_TARGET};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    Text,
    Json,
}

impl FromStr for LogFormat {
    type Err = LoggerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            _ => Err(LoggerError::UnknownFormat(s.to_string())),
        }
    }
}

/// The minimum level of what is logged. Its `FromStr` is the one parser of a level's name, which
/// Python's `enable_access_log(level=)` and `LogConfig["level"]` reach through `pyapi`'s `PyLogLevel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LogLevel {
    Off,
    At(RecordLevel),
}

/// The level of one log record, most severe first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum RecordLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

/// Accepts the levels `LogConfig["level"]` documents plus Python's spelling `WARNING`. It is
/// stricter than `LevelFilter::from_str`, which also takes `""` and `0`–`5`, and than an
/// `EnvFilter` directive, where any unknown word is a target name.
impl FromStr for LogLevel {
    type Err = LoggerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "OFF" => Ok(Self::Off),
            "ERROR" => Ok(Self::At(RecordLevel::Error)),
            "WARN" | "WARNING" => Ok(Self::At(RecordLevel::Warn)),
            "INFO" => Ok(Self::At(RecordLevel::Info)),
            "DEBUG" => Ok(Self::At(RecordLevel::Debug)),
            "TRACE" => Ok(Self::At(RecordLevel::Trace)),
            _ => Err(LoggerError::UnknownLevel(s.to_string())),
        }
    }
}

impl LogLevel {
    fn filter(self) -> LevelFilter {
        match self {
            Self::Off => LevelFilter::OFF,
            Self::At(RecordLevel::Error) => LevelFilter::ERROR,
            Self::At(RecordLevel::Warn) => LevelFilter::WARN,
            Self::At(RecordLevel::Info) => LevelFilter::INFO,
            Self::At(RecordLevel::Debug) => LevelFilter::DEBUG,
            Self::At(RecordLevel::Trace) => LevelFilter::TRACE,
        }
    }

    /// Returns the Python `logging` level that lets through what this level logs.
    fn python_level(self) -> i64 {
        match self {
            Self::Off => py_level::CRITICAL + 10,
            Self::At(RecordLevel::Error) => py_level::ERROR,
            Self::At(RecordLevel::Warn) => py_level::WARNING,
            Self::At(RecordLevel::Info) => py_level::INFO,
            // Python has no TRACE, so DEBUG records are the finest it emits.
            Self::At(RecordLevel::Debug | RecordLevel::Trace) => py_level::DEBUG,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LoggerConfig {
    level: LogLevel,
    access_log: bool,
    format: LogFormat,
}

impl std::fmt::Display for LoggerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let format = match self.format {
            LogFormat::Text => "text",
            LogFormat::Json => "json",
        };
        write!(
            f,
            "level {}, access log {}, format {format}",
            self.level.filter(),
            if self.access_log { "on" } else { "off" }
        )
    }
}

impl LoggerConfig {
    fn new(level: LogLevel, access_log: bool, format: &str) -> Result<Self, LoggerError> {
        Ok(Self {
            level,
            access_log,
            format: format.parse()?,
        })
    }

    /// Builds the filter from the level, turning the access log's target off unless it is on.
    /// The directives always parse, but a failure is still an error rather than a panic.
    fn filter(&self) -> Result<EnvFilter, LoggerError> {
        let level = self.level.filter();
        let directives = if self.access_log {
            level.to_string()
        } else {
            format!("{level},{ACCESS_TARGET}=off")
        };
        EnvFilter::try_new(&directives).map_err(|source| LoggerError::InvalidFilter { directives, source })
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LoggerError {
    #[error("invalid log level {0:?}: expected one of OFF, ERROR, WARN, INFO, DEBUG, TRACE")]
    UnknownLevel(String),
    #[error("invalid log format {0:?}: expected \"text\" or \"json\"")]
    UnknownFormat(String),
    #[error(
        "could not install the mipyre tracing subscriber; another global subscriber \
         or `log` logger is already set: {0}"
    )]
    SubscriberAlreadySet(#[from] tracing_subscriber::util::TryInitError),
    /// The log is configured once per process: a second, different configuration
    /// is refused rather than silently ignored or applied.
    #[error(
        "the log is already configured in this process ({installed}); a different configuration \
         ({wanted}) cannot replace it: one process has one log"
    )]
    AlreadyConfigured {
        installed: LoggerConfig,
        wanted: LoggerConfig,
    },
    #[error("invalid tracing filter {directives:?}: {source}")]
    InvalidFilter {
        directives: String,
        #[source]
        source: tracing_subscriber::filter::ParseError,
    },
    #[error(transparent)]
    LogWriterStopped(#[from] FenceError),
    #[error("could not start the log's writer thread: {0}")]
    WriterThread(#[source] std::io::Error),
}

impl From<LoggerError> for PyErr {
    fn from(e: LoggerError) -> Self {
        match e {
            LoggerError::UnknownLevel(_) | LoggerError::UnknownFormat(_) => PyValueError::new_err(e.to_string()),
            LoggerError::SubscriberAlreadySet(_)
            | LoggerError::AlreadyConfigured { .. }
            | LoggerError::InvalidFilter { .. }
            | LoggerError::LogWriterStopped(_)
            | LoggerError::WriterThread(_) => PyRuntimeError::new_err(e.to_string()),
        }
    }
}

/// The process-wide subscriber, installed by the first `init_logger` call: tracing allows one
/// global subscriber per process, and this process has one log configuration.
struct GlobalSubscriber {
    config: LoggerConfig,
    queue: LogQueue,
    /// The thread that writes the queue to stderr; `None` while the process must run no thread of
    /// the logger's (it is about to fork), and in the child of that fork.
    writer: Option<LogWriter>,
    /// Whether each logging thread writes its lines to stderr itself (`writer` is `None`).
    direct: Arc<AtomicBool>,
}

/// Where the formatted lines go: into the queue the writer thread drains, or, while there is no
/// writer thread, straight to stderr from the thread that logs. One atomic load per line.
#[derive(Clone)]
struct LogSink {
    queue: LogChannel,
    direct: Arc<AtomicBool>,
}

/// One line's way out, as [`LogSink`] picked it.
enum SinkWriter {
    Queue(LogChannel),
    Stderr(std::io::Stderr),
}

impl Write for SinkWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            SinkWriter::Queue(queue) => queue.write(buf),
            SinkWriter::Stderr(stderr) => stderr.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            SinkWriter::Queue(queue) => queue.flush(),
            SinkWriter::Stderr(stderr) => stderr.flush(),
        }
    }
}

impl<'a> MakeWriter<'a> for LogSink {
    type Writer = SinkWriter;

    fn make_writer(&'a self) -> SinkWriter {
        if self.direct.load(Ordering::Acquire) {
            SinkWriter::Stderr(std::io::stderr())
        } else {
            SinkWriter::Queue(self.queue.clone())
        }
    }
}

static LOGGER: Mutex<Option<GlobalSubscriber>> = Mutex::new(None);

/// The format layer, boxed so the text and JSON formats are one type.
type FmtLayer = Box<dyn Layer<tracing_subscriber::layer::Layered<EnvFilter, Registry>> + Send + Sync>;

fn fmt_layer<W>(format: LogFormat, writer: W, ansi: bool) -> FmtLayer
where
    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
{
    match format {
        LogFormat::Json => fmt::layer()
            .with_writer(writer)
            .json()
            .event_format(JsonLines::new(SystemTime))
            .boxed(),
        LogFormat::Text => fmt::layer()
            .with_writer(writer)
            .with_target(true)
            .with_ansi(ansi)
            .boxed(),
    }
}

fn install(config: &LoggerConfig) -> Result<GlobalSubscriber, LoggerError> {
    // The process's exit does not wait for the thread; `flush_logs` does.
    let stderr = std::io::stderr();
    let ansi = stderr.is_terminal();
    let queue = LogQueue::new();
    let writer = queue.start_writer(stderr).map_err(LoggerError::WriterThread)?;
    let direct = Arc::new(AtomicBool::new(false));
    let sink = LogSink {
        queue: queue.channel().clone(),
        direct: Arc::clone(&direct),
    };
    tracing_subscriber::registry()
        .with(config.filter()?)
        .with(fmt_layer(config.format, sink, ansi))
        .try_init()?;
    Ok(GlobalSubscriber {
        config: *config,
        queue,
        writer: Some(writer),
        direct,
    })
}

/// Installs the log once; the same configuration again does nothing, and a different one is
/// refused.
fn apply(config: &LoggerConfig) -> Result<(), LoggerError> {
    let mut slot = LOGGER.lock();
    match slot.as_ref() {
        Some(installed) if installed.config == *config => Ok(()),
        Some(installed) => Err(LoggerError::AlreadyConfigured {
            installed: installed.config,
            wanted: *config,
        }),
        None => {
            *slot = Some(install(config)?);
            Ok(())
        }
    }
}

/// Installs the tracing subscriber once per process; `access_log: false` turns off the
/// `mipyre::access` target, and `format` is `"text"` or `"json"`. A later call with the same
/// configuration does nothing.
///
/// An event that passes the filter takes one atomic load (where its line goes) on top of
/// formatting and the queue send; a filtered-out event is rejected by the cached callsite
/// interest.
///
/// # Errors
///
/// Fails on an unknown format, when a foreign global subscriber already holds the slot, and
/// when the log is already configured differently.
pub(crate) fn init_logger(level: LogLevel, access_log: bool, format: &str) -> Result<(), LoggerError> {
    let config = LoggerConfig::new(level, access_log, format)?;
    apply(&config)?;
    tracing::info!(
        target: "mipyre::server",
        level = %config.level.filter(),
        access_log = config.access_log,
        format = ?config.format,
        "Mipyre tracing engine initialized"
    );
    Ok(())
}

/// Blocks until every line logged before the call is written to stderr, and returns at once when
/// no logger is installed. The process's exit does not wait for the writer thread, so
/// `Mipyre.run()` calls this as it ends.
///
/// # Errors
///
/// Fails when the writer thread has stopped.
pub(crate) fn flush_logs(py: Python<'_>) -> Result<(), LoggerError> {
    let channel = match LOGGER.lock().as_ref() {
        Some(installed) if installed.writer.is_some() => installed.queue.channel().clone(),
        // Each line was written by the call that logged it.
        Some(_) | None => return Ok(()),
    };
    py.detach(move || channel.fence())?;
    Ok(())
}

/// Returns how many lines the log dropped, by cause, and none before it is installed.
pub(crate) fn log_lines_dropped() -> LogDrops {
    LOGGER
        .lock()
        .as_ref()
        .map_or(LogDrops::default(), |installed| installed.queue.channel().dropped())
}

/// Stops the log's writer thread, so the process runs no thread of the logger's, and returns
/// whether one was running. Until [`start_log_writer`], each thread that logs writes its lines
/// itself; the lines queued before are written out first.
///
/// # Errors
///
/// Fails when the writer thread had already stopped (it panicked on a line).
pub(crate) fn stop_log_writer(py: Python<'_>) -> Result<bool, LoggerError> {
    let mut slot = LOGGER.lock();
    let Some(installed) = slot.as_mut() else {
        return Ok(false);
    };
    let Some(writer) = installed.writer.take() else {
        return Ok(false);
    };
    // Lines from here on go to stderr directly; the ones already queued are written before the
    // thread ends, and one that raced the switch waits for the next writer.
    installed.direct.store(true, Ordering::Release);
    py.detach(|| writer.stop())?;
    Ok(true)
}

/// Starts the log's writer thread again after [`stop_log_writer`]; with no logger installed, or
/// its thread running, it does nothing.
///
/// # Errors
///
/// Fails when the thread cannot be started; the log stays written by the logging threads.
pub(crate) fn start_log_writer() -> Result<(), LoggerError> {
    let mut slot = LOGGER.lock();
    let Some(installed) = slot.as_mut() else {
        return Ok(());
    };
    if installed.writer.is_some() {
        return Ok(());
    }
    installed.writer = Some(
        installed
            .queue
            .start_writer(std::io::stderr())
            .map_err(LoggerError::WriterThread)?,
    );
    installed.direct.store(false, Ordering::Release);
    Ok(())
}

/// Return the Python `logging` level matching what `init_logger` applied, or None before it ran.
///
/// Every interpreter sets its root logger to it, so records below the threshold are dropped
/// before formatting or crossing into Rust.
#[pyfunction]
pub(crate) fn _python_log_level() -> Option<i64> {
    Some(LOGGER.lock().as_ref()?.config.level.python_level())
}

mod py_level {
    pub(crate) const DEBUG: i64 = 10;
    pub(crate) const INFO: i64 = 20;
    pub(crate) const WARNING: i64 = 30;
    pub(crate) const ERROR: i64 = 40;
    pub(crate) const CRITICAL: i64 = 50;
}

impl RecordLevel {
    /// Maps a Python level to the highest standard threshold it reaches, because Python levels
    /// are an open integer scale (`logging.addLevelName(25, "NOTICE")`): 25 is INFO, 45 is ERROR,
    /// 5 is TRACE.
    fn from_levelno(levelno: i64) -> Self {
        match levelno {
            n if n >= py_level::ERROR => Self::Error,
            n if n >= py_level::WARNING => Self::Warn,
            n if n >= py_level::INFO => Self::Info,
            n if n >= py_level::DEBUG => Self::Debug,
            _ => Self::Trace,
        }
    }
}

/// Dispatches a Python log record to the tracing macro of its level. A macro keeps each level a
/// distinct static callsite, so `EnvFilter` keeps its near-zero-cost skip; a `None` `$wid`
/// records no `worker` field.
macro_rules! dispatch_python_log {
    ($level:expr, $wid:expr, $lane:expr, $name:expr, $pathname:expr, $lineno:expr, $message:expr $(,)?) => {
        match $level {
            RecordLevel::Trace => {
                tracing::trace!(
                    target: "mipyre::app",
                    worker = $wid,
                    lane = $lane,
                    logger = %$name,
                    file = %$pathname,
                    line = $lineno,
                    "{}", $message
                );
            }
            RecordLevel::Debug => {
                tracing::debug!(
                    target: "mipyre::app",
                    worker = $wid,
                    lane = $lane,
                    logger = %$name,
                    file = %$pathname,
                    line = $lineno,
                    "{}", $message
                );
            }
            RecordLevel::Info => {
                tracing::info!(
                    target: "mipyre::app",
                    worker = $wid,
                    lane = $lane,
                    logger = %$name,
                    file = %$pathname,
                    line = $lineno,
                    "{}", $message
                );
            }
            RecordLevel::Warn => {
                tracing::warn!(
                    target: "mipyre::app",
                    worker = $wid,
                    lane = $lane,
                    logger = %$name,
                    file = %$pathname,
                    line = $lineno,
                    "{}", $message
                );
            }
            RecordLevel::Error => {
                tracing::error!(
                    target: "mipyre::app",
                    worker = $wid,
                    lane = $lane,
                    logger = %$name,
                    file = %$pathname,
                    line = $lineno,
                    "{}", $message
                );
            }
        }
    };
}

/// Route a Python logging record through Rust tracing.
///
/// `mipyre._log.bridge.RustLogHandler.emit()` calls it in every interpreter and every
/// process-lane worker. A sub-interpreter worker passes its `worker_id`, a process-lane worker
/// its `lane` (the server's own lines about that worker name it the same way); the main
/// interpreter passes neither and records neither field, so its line can't pass for a worker's.
#[pyfunction]
#[pyo3(signature = (levelno, name, message, pathname, lineno, worker_id=None, lane=None))]
pub(crate) fn emit_python_log(
    levelno: i64,
    name: &str,
    message: &str,
    pathname: &str,
    lineno: u32,
    worker_id: Option<usize>,
    lane: Option<usize>,
) {
    dispatch_python_log!(
        RecordLevel::from_levelno(levelno),
        worker_id,
        lane,
        name,
        pathname,
        lineno,
        message
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levelno_maps_by_threshold() {
        assert_eq!(RecordLevel::from_levelno(0), RecordLevel::Trace);
        assert_eq!(RecordLevel::from_levelno(9), RecordLevel::Trace);
        assert_eq!(RecordLevel::from_levelno(10), RecordLevel::Debug);
        assert_eq!(RecordLevel::from_levelno(20), RecordLevel::Info);
        assert_eq!(RecordLevel::from_levelno(25), RecordLevel::Info);
        assert_eq!(RecordLevel::from_levelno(30), RecordLevel::Warn);
        assert_eq!(RecordLevel::from_levelno(40), RecordLevel::Error);
        assert_eq!(RecordLevel::from_levelno(50), RecordLevel::Error);
        assert_eq!(RecordLevel::from_levelno(1000), RecordLevel::Error);
    }

    #[test]
    fn level_accepts_documented_names_only() {
        assert_eq!("warning".parse::<LogLevel>().unwrap(), LogLevel::At(RecordLevel::Warn));
        assert_eq!("Off".parse::<LogLevel>().unwrap(), LogLevel::Off);
        for bad in ["", "3", "verbose", "mipyre=debug"] {
            assert!(
                matches!(bad.parse::<LogLevel>(), Err(LoggerError::UnknownLevel(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn python_level_lets_through_what_the_filter_logs() {
        assert_eq!(LogLevel::Off.python_level(), 60);
        assert_eq!(LogLevel::At(RecordLevel::Error).python_level(), 40);
        assert_eq!(LogLevel::At(RecordLevel::Warn).python_level(), 30);
        assert_eq!(LogLevel::At(RecordLevel::Info).python_level(), 20);
        assert_eq!(LogLevel::At(RecordLevel::Debug).python_level(), 10);
        assert_eq!(LogLevel::At(RecordLevel::Trace).python_level(), 10);
    }

    #[test]
    fn format_is_closed() {
        assert_eq!("JSON".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("text".parse::<LogFormat>().unwrap(), LogFormat::Text);
        assert!(matches!(
            "yaml".parse::<LogFormat>(),
            Err(LoggerError::UnknownFormat(_))
        ));
    }

    /// The child process of
    /// [`a_subscriber_slot_another_one_holds_is_reported_not_silently_lost`] has this set.
    const SLOT_TAKEN_CHILD: &str = "LOGGER_SLOT_TAKEN_CHILD";

    /// tracing's one slot is per process and can't be emptied, so the test runs itself again as
    /// a child process, which fills the slot and then calls `init_logger`.
    #[test]
    fn a_subscriber_slot_another_one_holds_is_reported_not_silently_lost() {
        let name = concat!(
            module_path!(),
            "::a_subscriber_slot_another_one_holds_is_reported_not_silently_lost"
        );
        // The harness filters on the test's name without the crate.
        let name = name.split_once("::").map_or(name, |(_crate, path)| path);
        if std::env::var_os(SLOT_TAKEN_CHILD).is_some() {
            tracing::subscriber::set_global_default(tracing_subscriber::registry())
                .expect("the child's slot is empty until here");
            let err = init_logger(LogLevel::At(RecordLevel::Info), false, "text").expect_err("the slot is taken");
            assert!(matches!(err, LoggerError::SubscriberAlreadySet(_)), "{err:?}");
            let said = err.to_string();
            assert!(said.contains("already set"), "{said}");
            // Python gets it as a RuntimeError with those words.
            Python::initialize();
            mipyre_subinterp::attach::with_main(|py| {
                let raised = PyErr::from(err);
                assert!(raised.is_instance_of::<PyRuntimeError>(py), "{raised}");
                assert_eq!(raised.value(py).to_string(), said);
            });
            // Nothing was installed: the level stays unset for every interpreter.
            assert_eq!(_python_log_level(), None);
            return;
        }
        let exe = std::env::current_exe().expect("the test binary's path");
        let child = std::process::Command::new(exe)
            .args([name, "--exact", "--nocapture", "--test-threads=1"])
            .env(SLOT_TAKEN_CHILD, "1")
            .output()
            .expect("the test binary runs again");
        let out = String::from_utf8_lossy(&child.stdout);
        let err = String::from_utf8_lossy(&child.stderr);
        assert!(child.status.success(), "child failed:\n{out}\n{err}");
        // The filter matched: the child ran this test, not none.
        assert!(out.contains("1 passed"), "the child ran no test:\n{out}");
    }

    #[derive(Clone, Default)]
    struct Lines(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Lines {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Lines {
        type Writer = Lines;
        fn make_writer(&'a self) -> Lines {
            self.clone()
        }
    }

    fn text_line(ansi: bool) -> String {
        let lines = Lines::default();
        let subscriber = tracing_subscriber::registry()
            .with(EnvFilter::new("info"))
            .with(fmt_layer(LogFormat::Text, lines.clone(), ansi));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "mipyre::app", "started");
        });
        let written = lines.0.lock().unwrap().clone();
        String::from_utf8(written).unwrap()
    }

    #[test]
    fn the_text_format_writes_colour_codes_only_when_asked() {
        let plain = text_line(false);
        assert!(plain.contains("started") && !plain.contains('\x1b'), "{plain:?}");
        let coloured = text_line(true);
        assert!(
            coloured.contains("started") && coloured.contains('\x1b'),
            "{coloured:?}"
        );
    }

    fn config(level: &str, format: &str) -> LoggerConfig {
        LoggerConfig::new(level.parse().unwrap(), false, format).unwrap()
    }

    #[test]
    fn a_configuration_differing_in_any_part_is_not_the_installed_one() {
        assert_eq!(config("INFO", "text"), config("INFO", "text"));
        assert_ne!(config("INFO", "text"), config("INFO", "json"));
        assert_ne!(config("INFO", "text"), config("WARN", "text"));
        let refused = LoggerError::AlreadyConfigured {
            installed: config("ERROR", "text"),
            wanted: config("INFO", "json"),
        };
        let text = refused.to_string();
        assert!(
            text.contains("(level error, access log off, format text)")
                && text.contains("(level info, access log off, format json)"),
            "{text}"
        );
    }

    #[test]
    fn access_log_off_adds_its_directive() {
        let config = LoggerConfig::new(LogLevel::At(RecordLevel::Info), false, "text").unwrap();
        assert!(config.filter().unwrap().to_string().contains("mipyre::access=off"));
    }
}
