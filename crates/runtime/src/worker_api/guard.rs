//! Defines worker API functions so that none lets a Rust panic out as PyO3's `PanicException`.
//!
//! `PanicException` is a `BaseException`, and the engine's fetcher thread catches only
//! `Exception`, so one escaping panic would kill that thread. Every `#[pyfunction]` in
//! `worker_api.rs` is written through this macro, never by hand.

use pyo3::prelude::*;

use super::{EnginePanic, JobReply};
use crate::exchange::error::HandlerError;
use mipyre_util::panic_message;

/// Runs `f` and raises a Rust panic in it as an unlogged [`EnginePanic`]; whoever catches it logs
/// it once.
#[inline]
pub(super) fn no_panic<T>(context: &'static str, py: Python<'_>, f: impl FnOnce() -> PyResult<T>) -> PyResult<T> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => Err(EnginePanic::raise(py, panicked(context, &*payload))),
    }
}

/// Runs `f` and records a Rust panic in it as the failure of the request `reply` answers.
///
/// The call then returns `Ok`, because the request is finished and nobody further up could act on
/// the panic.
#[inline]
pub(super) fn no_panic_answering(
    context: &'static str,
    reply: &JobReply,
    f: impl FnOnce() -> PyResult<()>,
) -> PyResult<()> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => {
            reply.fail(panicked(context, &*payload));
            Ok(())
        }
    }
}

#[cold]
pub(super) fn panicked(context: &'static str, payload: &(dyn std::any::Any + Send)) -> HandlerError {
    HandlerError::Panicked {
        payload: format!("in {context}: {}", panic_message(payload)),
    }
}

/// Writes a `pub` `#[pyfunction]` whose body runs under [`no_panic`] (`raising through`)
/// or, for a call that answers a request, [`no_panic_answering`] (`answering`).
macro_rules! worker_api_function {
    (
        $(#[$attr:meta])*
        fn $name:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty;
        raising through $py:expr;
        $body:block
    ) => {
        $(#[$attr])*
        #[pyo3::pyfunction]
        pub fn $name($($arg: $ty),*) -> pyo3::PyResult<$ret> {
            $crate::worker_api::guard::no_panic(stringify!($name), $py, || $body)
        }
    };
    (
        $(#[$attr:meta])*
        fn $name:ident($($arg:ident: $ty:ty),* $(,)?);
        answering $reply:expr;
        $body:block
    ) => {
        $(#[$attr])*
        #[pyo3::pyfunction]
        pub fn $name($($arg: $ty),*) -> pyo3::PyResult<()> {
            $crate::worker_api::guard::no_panic_answering(stringify!($name), $reply, || $body)
        }
    };
}
pub(super) use worker_api_function;
