//! Hands an error that no caller can take to the loop's exception handler, as asyncio does for an
//! exception in a callback, and logs one the handler cannot take.
//!
//! It does not decide what goes on after the error: the turn, or the Task, does.

use std::os::fd::RawFd;
use std::time::Duration;

use mipyre_util::throttle::LogThrottle;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::EventLoop;

/// Names what raised, as asyncio's handlers read it: a callback under `handle`, a Task under
/// `task`, an fd under `fd`.
pub(super) enum Subject<'a, 'py> {
    Handle(&'a Bound<'py, PyAny>),
    Task(&'a Bound<'py, PyAny>),
    Fd(RawFd),
}

/// Builds the context asyncio's handlers read — `message`, `exception` and the subject — and hands
/// it to the loop's handler.
pub(super) fn report(event_loop: &Bound<'_, EventLoop>, message: &str, raised: &PyErr, subject: Subject<'_, '_>) {
    let py = event_loop.py();
    let context = PyDict::new(py);
    let built = context
        .set_item(intern!(py, "message"), message)
        .and_then(|()| context.set_item(intern!(py, "exception"), raised.value(py)))
        .and_then(|()| match subject {
            Subject::Handle(handle) => context.set_item(intern!(py, "handle"), handle),
            Subject::Task(task) => context.set_item(intern!(py, "task"), task),
            Subject::Fd(fd) => context.set_item(intern!(py, "fd"), fd),
        });
    match built {
        Ok(()) => call_handler(event_loop, context),
        Err(also) => tracing::error!(target: "mipyre::server", error = %raised, %also, "{message}"),
    }
}

/// Calls the handler set with `set_exception_handler`, or logs `context` as asyncio's default
/// does; a handler that raises is logged, never raised to the caller.
pub(super) fn call_handler(event_loop: &Bound<'_, EventLoop>, context: Bound<'_, PyDict>) {
    let py = event_loop.py();
    let this = event_loop.get();
    let handler = this.exception_handler.lock().as_ref().map(|h| h.clone_ref(py));
    if let Some(handler) = handler {
        match handler.bind(py).call1((event_loop, &context)) {
            Ok(_) => return,
            Err(raised) => tracing::error!(
                target: "mipyre::server",
                error = %raised,
                "the event loop's exception handler raised"
            ),
        }
    }
    log_context(&this.callback_errors, this.elapsed(), &context);
}

/// Logs what an event loop error's `context` says, at most once per period while it repeats.
pub(super) fn log_context(throttle: &LogThrottle, now: Duration, context: &Bound<'_, PyDict>) {
    let Some(suppressed) = throttle.admit(now) else {
        return;
    };
    let message = match context.get_item("message") {
        Ok(Some(message)) => message.to_string(),
        Ok(None) => "an event loop error".to_owned(),
        Err(e) => format!("an event loop error whose message could not be read ({e})"),
    };
    let exception = match context.get_item("exception") {
        Ok(Some(exception)) => exception.repr().map_or_else(|e| e.to_string(), |r| r.to_string()),
        Ok(None) => String::new(),
        Err(e) => e.to_string(),
    };
    tracing::error!(target: "mipyre::server", exception, suppressed, "{message}");
}
