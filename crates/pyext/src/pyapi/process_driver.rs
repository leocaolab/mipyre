//! Gives the Python launcher (`mipyre._process_lane`) the process lanes' ways to start a template or a worker, as
//! the `_driver_*` functions.
//!
//! A fork here is `os.fork()`'s, at-fork callbacks included, but without its multi-thread check: on macOS that
//! check in CPython 3.14.7 leaks a 16 KiB page per call, which grew a template by 91 MB over 5435 forks. The launcher
//! and the template fork only while they have one thread. A failed fork or spawn is raised as `OSError` with the OS
//! errno, which the template sends to the server as its answer to a fork request.

use std::io;
use std::os::fd::RawFd;

use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::prelude::*;

use mipyre_process::{spawn_python, start_new_process, ProcessStartError, StartedProcess, DRIVER_NAME};

/// Forks this Python process as `os.fork()` does, at-fork callbacks included, but without its
/// multi-thread check (the caller says why). The caller is the process's only thread: a child
/// forked beside another thread inherits whatever locks that thread held.
fn fork_python(_py: Python<'_>) -> io::Result<StartedProcess> {
    // SAFETY: attached (`_py`), as the three hooks require. They bracket `fork` exactly as
    // `os.fork()` does: before it, then the child's or the parent's after it, so CPython's
    // locks and thread state are consistent on both sides. The callers fork only while this
    // process has one thread (the launcher and the template are single-threaded, see
    // `spawn_python`), which is the condition the skipped check warns about. errno is read
    // before `PyOS_AfterFork_Parent`, which may change it.
    unsafe {
        pyo3::ffi::PyOS_BeforeFork();
        let pid = libc::fork();
        if pid == 0 {
            pyo3::ffi::PyOS_AfterFork_Child();
            return Ok(StartedProcess::Child);
        }
        let err = io::Error::last_os_error();
        pyo3::ffi::PyOS_AfterFork_Parent();
        if pid < 0 {
            return Err(err);
        }
        Ok(StartedProcess::Parent { pid })
    }
}

fn start_error(err: ProcessStartError) -> PyErr {
    let text = err.to_string();
    match &err {
        ProcessStartError::NulInArgument(_) | ProcessStartError::NulInEnvironment(_) => PyValueError::new_err(text),
        ProcessStartError::Spawn { source, .. } | ProcessStartError::Fork(source) => match source.raw_os_error() {
            Some(errno) => PyOSError::new_err((errno, text)),
            None => PyOSError::new_err(text),
        },
    }
}

/// Returns `"fork"` or `"spawn"`.
#[pyfunction]
pub(crate) fn _driver_name() -> &'static str {
    DRIVER_NAME
}

/// Starts the template and returns its pid, or 0 in a forked child, which becomes the template.
#[pyfunction]
#[pyo3(signature = (python, module, args, keep_fds, env))]
pub(crate) fn _driver_start_template(
    py: Python<'_>,
    python: &str,
    module: &str,
    args: Vec<String>,
    keep_fds: Vec<RawFd>,
    env: Vec<(String, String)>,
) -> PyResult<libc::pid_t> {
    start_new_process(|| fork_python(py), python, module, &args, &keep_fds, &env)
        .map(StartedProcess::fork_answer)
        .map_err(start_error)
}

/// Starts the template in a new Python on every platform and returns its pid: a test server's,
/// whose process runs the test runner's threads, which a forked template would not survive.
#[pyfunction]
#[pyo3(signature = (python, module, args, keep_fds, env))]
pub(crate) fn _driver_spawn_template(
    python: &str,
    module: &str,
    args: Vec<String>,
    keep_fds: Vec<RawFd>,
    env: Vec<(String, String)>,
) -> PyResult<libc::pid_t> {
    spawn_python(python, module, &args, &keep_fds, &env)
        .map(StartedProcess::fork_answer)
        .map_err(start_error)
}

/// Starts a `start="import"` worker and returns its pid, or 0 in a forked child, which becomes
/// the worker.
#[pyfunction]
#[pyo3(signature = (python, module, args, keep_fds, env))]
pub(crate) fn _driver_start_import_worker(
    py: Python<'_>,
    python: &str,
    module: &str,
    args: Vec<String>,
    keep_fds: Vec<RawFd>,
    env: Vec<(String, String)>,
) -> PyResult<libc::pid_t> {
    start_new_process(|| fork_python(py), python, module, &args, &keep_fds, &env)
        .map(StartedProcess::fork_answer)
        .map_err(start_error)
}

/// Forks a `start="fork"` worker and returns its pid, or 0 in the child.
#[pyfunction]
pub(crate) fn _driver_fork_worker(py: Python<'_>) -> PyResult<libc::pid_t> {
    fork_python(py)
        .map(StartedProcess::fork_answer)
        .map_err(|e| start_error(ProcessStartError::Fork(e)))
}
