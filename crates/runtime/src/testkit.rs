//! Test helpers this package's tests share with the engine's: a log capture, and the readings of a
//! thread's stack. One test binary has one global tracing subscriber, so the engine's tests capture
//! through these rather than installing their own.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test helpers: a failed expectation is how a test using them fails"
)]

pub mod thread_stack;

use std::sync::{Arc, Mutex};

use mipyre_http::io::request_head::RequestLabel;
use mipyre_http::plan::request_id::RequestId;
use mipyre_subinterp::context::{ContextEntry, PyContext, SpareContext};
use pyo3::prelude::*;

use crate::exchange::error::{HandlerError, RequestStage};
use crate::request_context::in_reused_context;

/// Installs, once per test binary, a global subscriber that wants every event, for a test that
/// captures events with `tracing::subscriber::with_default`.
///
/// A callsite caches its interest the first time any thread reaches it. While the capture is the
/// only subscriber alive, tracing-core asks only the reaching thread's default, so a parallel test
/// reaching the callsite first, with no subscriber of its own, caches `never` and the capture
/// loses the event. The global subscriber keeps a second one alive, so the interest is
/// asked of every live subscriber, the capture too.
pub fn ask_every_subscriber() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        tracing::subscriber::set_global_default(tracing_subscriber::registry())
            .expect("no other test in this binary sets the global subscriber");
    });
}

pub fn captured_log(f: impl FnOnce()) -> String {
    captured_log_from(tracing::Level::INFO, f)
}

pub fn captured_log_from(level: tracing::Level, f: impl FnOnce()) -> String {
    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Buf {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let buf = Buf::default();
    let sink = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(level)
        .with_writer(move || sink.clone())
        .finish();
    ask_every_subscriber();
    tracing::subscriber::with_default(subscriber, f);
    let bytes = buf.0.lock().unwrap().clone();
    String::from_utf8(bytes).unwrap()
}

/// Runs `f` with its log captured and discarded.
pub fn captured_log_value<T>(f: impl FnOnce() -> T) -> T {
    let mut out = None;
    captured_log(|| out = Some(f()));
    out.unwrap()
}

pub fn label(id: &RequestId) -> RequestLabel {
    RequestLabel {
        id: id.clone(),
        method: hyper::Method::GET,
        uri: hyper::Uri::from_static("/p"),
    }
}

/// Runs `in_reused_context` with an `f` that enters another context and never exits it,
/// so the request's context cannot be left.
///
/// The request's context then stays entered on this thread state, so callers run it on a
/// thread of their own.
pub fn with_a_context_left_entered<'py, T>(
    py: Python<'py>,
    f: impl FnOnce(Python<'py>) -> Result<T, HandlerError>,
) -> Result<T, HandlerError> {
    let mut other: Option<ContextEntry<'py>> = None;
    let outcome = in_reused_context(py, &mut SpareContext::default(), |_entered| {
        other = Some(PyContext::new(py).expect("a new context").enter());
        f(py)
    });
    other.expect("f ran").exit().expect("the context f entered is current");
    outcome
}

pub fn raised(py: Python<'_>, message: &str) -> HandlerError {
    let err = pyo3::exceptions::PyKeyError::new_err(message.to_owned());
    HandlerError::python(py, RequestStage::Handler, &err)
}
