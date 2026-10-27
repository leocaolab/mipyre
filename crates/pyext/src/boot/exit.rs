//! Ends the process without finalizing when, at exit, a thread still runs Python in its
//! sub-interpreter: `Py_Finalize` would free the interpreter under it.
//!
//! The decision is the sub-interpreters' (`mipyre_subinterp::decide_exit`); this only asks for it
//! at exit, once a run has left workers running, and carries it out. It does not know which
//! workers those were: each was logged when it was abandoned.

use mipyre_subinterp::ExitDecision;
use pyo3::prelude::*;
use pyo3::types::PyCFunction;

/// Makes the process decide at exit, before finalization begins, whether a thread still runs
/// Python in its interpreter, and exit with status 1 without finalizing if one does.
///
/// It registers an `atexit` callback each time. `atexit` runs the latest first, and the callback
/// that lets finalization begin was registered when the first interpreter was made, so these run
/// before it; after the first lets the exit go on, the rest find no owner left and return at once.
///
/// # Errors
///
/// Returns the Python error when the callback cannot be built or `atexit.register` fails.
pub(crate) fn decide_exit_before_finalizing(py: Python<'_>) -> PyResult<()> {
    let check = PyCFunction::new_closure(
        py,
        Some(c"mipyre_decide_exit"),
        Some(c"Exits without finalizing while a thread runs Python in its sub-interpreter."),
        |args, _kwargs| exit_unless_finalizable(args.py()),
    )?;
    py.import("atexit")?.call_method1("register", (check,))?;
    Ok(())
}

fn exit_unless_finalizable(py: Python<'_>) {
    match py.detach(mipyre_subinterp::decide_exit) {
        ExitDecision::Finalize => {}
        ExitDecision::ExitWithoutFinalizing { running_python } => exit_without_finalizing(
            py,
            &format!(
                "{running_python} sub-interpreter(s) still in use at exit, running Python, after worker(s) \
                 outlived the shutdown grace period"
            ),
            None,
        ),
    }
}

/// Ends the process with status 1 and no finalization, after the log lines so far, `error`'s
/// whole chain (causes, notes, each of a group) when there is one, and `why`.
pub(crate) fn exit_without_finalizing(py: Python<'_>, why: &str, error: Option<PyErr>) -> ! {
    if let Err(e) = crate::telemetry::flush_logs(py) {
        eprintln!("mipyre: log lines may be missing above: {e}");
    }
    if let Some(error) = error {
        error.display(py);
    }
    if let Err(e) = flush_python_streams(py) {
        e.display(py);
    }
    eprintln!("mipyre: {why}; exiting without finalization");
    mipyre_process::exit_now(1)
}

fn flush_python_streams(py: Python<'_>) -> PyResult<()> {
    let sys = py.import("sys")?;
    sys.getattr("stdout")?.call_method0("flush")?;
    sys.getattr("stderr")?.call_method0("flush")?;
    Ok(())
}
