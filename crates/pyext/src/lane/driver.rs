//! Starts the lane's template and worker processes, by fork on Linux and by spawning a new
//! Python on macOS.
//!
//! Which strategy to use is decided in the launcher (`mipyre._process_lane.template`), not here.

use std::os::fd::RawFd;

use pyo3::prelude::*;

use super::os::StartedProcess;

/// Says how a worker gets the app.
#[pyclass(module = "mipyre.engine", eq, eq_int, frozen)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StartStrategy {
    /// The worker imports the app itself, so forking stays safe whatever the app's libraries do
    /// at import.
    Import,
    /// The template imports the app and forks workers, which replaces them faster and shares
    /// memory; the template must be single-threaded when it forks.
    Fork,
}

#[pymethods]
impl StartStrategy {
    /// Parses `"import"` or `"fork"`.
    ///
    /// # Errors
    ///
    /// Raises `ValueError` naming any other text.
    #[staticmethod]
    fn parse(text: &str) -> PyResult<Self> {
        match text {
            "import" => Ok(StartStrategy::Import),
            "fork" => Ok(StartStrategy::Fork),
            other => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "start={other:?} is not one of \"import\", \"fork\""
            ))),
        }
    }

    #[getter]
    fn name(&self) -> &'static str {
        match self {
            StartStrategy::Import => "import",
            StartStrategy::Fork => "fork",
        }
    }
}

#[cfg(target_os = "macos")]
const DRIVER_NAME: &str = "spawn";
#[cfg(not(target_os = "macos"))]
const DRIVER_NAME: &str = "fork";

/// Starts the template or a `start="import"` worker; off macOS it forks, which takes about a
/// millisecond and ignores every argument but `py`.
///
/// On macOS it spawns `python -m <module> <args>` instead: numpy 2.5.x fails to import at random
/// in any process that has forked, parent or child (numpy gh-32124), so nothing that may import
/// numpy forks there until numpy ships the fix.
#[cfg(not(target_os = "macos"))]
fn start_new_process(
    py: Python<'_>,
    _python: &str,
    _module: &str,
    _args: &[String],
    _keep_fds: &[RawFd],
    _env: &[(String, String)],
) -> PyResult<StartedProcess> {
    fork_without_thread_check(py)
}

#[cfg(target_os = "macos")]
fn start_new_process(
    _py: Python<'_>,
    python: &str,
    module: &str,
    args: &[String],
    keep_fds: &[RawFd],
    env: &[(String, String)],
) -> PyResult<StartedProcess> {
    spawn_python(python, module, args, keep_fds, env)
}

/// Forks as `os.fork()` does, at-fork callbacks included, but without its multi-thread check: on
/// macOS that check in CPython 3.14.7 leaks a 16 KiB page per call, which grew a template by
/// 91 MB over 5435 forks.
fn fork_without_thread_check(py: Python<'_>) -> PyResult<StartedProcess> {
    super::os::fork_python(py).map_err(|e| os_error("fork", &e))
}

/// Converts `err` to `OSError(errno, "<what>: <err>")`, keeping the OS errno because the template
/// sends it to the server as its answer to a fork request.
fn os_error(what: &str, err: &std::io::Error) -> PyErr {
    let text = format!("{what}: {err}");
    match err.raw_os_error() {
        Some(errno) => pyo3::exceptions::PyOSError::new_err((errno, text)),
        None => pyo3::exceptions::PyOSError::new_err(text),
    }
}

/// Spawns `python -m <module> <args>`, which inherits `keep_fds`, the standard streams, and this
/// environment with `env` overriding it.
#[cfg(target_os = "macos")]
fn spawn_python(
    python: &str,
    module: &str,
    args: &[String],
    keep_fds: &[RawFd],
    env: &[(String, String)],
) -> PyResult<StartedProcess> {
    use std::ffi::CString;

    let cstr = |s: &str| {
        CString::new(s)
            .map_err(|_| pyo3::exceptions::PyValueError::new_err(format!("NUL in {s:?}")))
    };
    let mut argv: Vec<CString> = vec![cstr(python)?, cstr("-m")?, cstr(module)?];
    for a in args {
        argv.push(cstr(a)?);
    }
    let mut envp: Vec<CString> = std::env::vars_os()
        .filter(|(k, _)| {
            !env.iter()
                .any(|(name, _)| k.to_str() == Some(name.as_str()))
        })
        .map(|(k, v)| {
            use std::os::unix::ffi::OsStrExt;
            let entry = [k.as_bytes(), b"=", v.as_bytes()].concat();
            CString::new(entry).map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "NUL in the environment entry {:?}",
                    e.into_vec()
                ))
            })
        })
        .collect::<PyResult<_>>()?;
    for (k, v) in env {
        envp.push(cstr(&format!("{k}={v}"))?);
    }
    // Clearing close-on-exec here does not race another thread's spawn, because the launcher
    // and the template are single-threaded.
    for &fd in keep_fds {
        super::os::keep_across_exec(fd)
            .map_err(|e| os_error(&format!("keeping fd {fd} for the new process"), &e))?;
    }
    let pid = super::os::spawn(&argv, &envp)
        .map_err(|e| os_error(&format!("posix_spawn {python} -m {module}"), &e))?;
    Ok(StartedProcess::Parent { pid })
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
    start_new_process(py, python, module, &args, &keep_fds, &env).map(StartedProcess::fork_answer)
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
    start_new_process(py, python, module, &args, &keep_fds, &env).map(StartedProcess::fork_answer)
}

/// Forks a `start="fork"` worker and returns its pid, or 0 in the child.
#[pyfunction]
pub(crate) fn _driver_fork_worker(py: Python<'_>) -> PyResult<libc::pid_t> {
    fork_without_thread_check(py).map(StartedProcess::fork_answer)
}
