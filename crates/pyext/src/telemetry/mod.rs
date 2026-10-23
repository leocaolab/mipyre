//! Holds the process's counters, the GIL monitor, log output and leak detection.

mod access_line;
mod degraded;
pub(crate) mod exposition;
#[cfg(feature = "leak_detect")]
mod leak_detect;
mod log_writer;
mod logging;
mod monitor;
mod run_counters;

pub(crate) use self::access_line::log_access;
pub(crate) use self::degraded::{admit_closed_loop_warning, count_db_result_undeliverable};
#[cfg(feature = "leak_detect")]
pub(crate) use self::leak_detect::{dump_to_stderr, record_drop};
pub(crate) use self::log_writer::WRITER_THREAD;
pub(crate) use self::logging::{
    _python_log_level, emit_python_log, flush_logs, init_logger, start_log_writer, stop_log_writer, LogLevel,
    RecordLevel,
};
pub(crate) use self::monitor::{read_counters, reset_peaks, Counters, RssSamplingGuard, MAIN_GIL};
pub(crate) use self::run_counters::{AnswerCounts, RunCounters, RunCounts};
