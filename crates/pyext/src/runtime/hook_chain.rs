//! Runs one request's before hooks, handler and after hooks in the request's own
//! `contextvars.Context`, for every synchronous path on main and in workers.
//!
//! The async engine repeats this order in Python (`_async_engine.py`), because there a request is
//! a task on the engine's already running loop.

use pyo3::call::PyCallArgs;
use pyo3::exceptions::PyRuntimeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PerInterpreterCell;
use pyo3::types::PyType;

use super::request_context::{AwaitableKind, RequestContext};
use crate::exchange::error::{HandlerError, RequestStage, ResponseError};
use crate::exchange::request::Request;
use crate::exchange::response::Response;
use mipyre_http::plan::response::HandlerResponse;
use mipyre_http::plan::route::RouteKey;

/// Says how an exception from a hook or handler is reported.
#[derive(Clone, Copy)]
pub(crate) enum ExceptionMapping<'a> {
    AsRaised,
    /// Turns asyncio's "no current event loop" error into a `NoEventLoopError` naming the route,
    /// for a worker's `def` handler.
    NoLoopHint {
        key: &'a RouteKey,
        handler: &'a Py<PyAny>,
    },
}

/// Supplies the event loop that drives an awaitable a hook or handler returned.
pub(crate) trait EventLoopProvider {
    fn event_loop<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>, HandlerError>;
}

pub(crate) type ResponseMapping =
    for<'py> fn(Bound<'py, PyAny>) -> Result<HandlerResponse, ResponseError>;

pub(crate) fn main_response(value: Bound<'_, PyAny>) -> Result<HandlerResponse, ResponseError> {
    crate::exchange::to_response::extract_response_data(value.py(), value)
}

/// Maps a worker's return value like [`main_response`], but refuses a `Stream`, because a worker
/// cannot stream.
pub(crate) fn worker_response(value: Bound<'_, PyAny>) -> Result<HandlerResponse, ResponseError> {
    if value.is_instance_of::<crate::exchange::stream::ResponseStream>() {
        return Err(ResponseError::StreamInWorker);
    }
    crate::exchange::to_response::extract_response_data(value.py(), value)
}

pub(crate) struct Chain<'a, 'py, L: EventLoopProvider> {
    pub(crate) py: Python<'py>,
    pub(crate) rc: &'a mut RequestContext<'py>,
    pub(crate) event_loop: &'a L,
    pub(crate) to_response: ResponseMapping,
}

impl<'py, L: EventLoopProvider> Chain<'_, 'py, L> {
    /// Runs the before hooks until one returns something other than `None`, which becomes the
    /// response.
    ///
    /// A hook that raises fails the request: running the handler anyway would bypass an auth
    /// hook that denies by raising.
    pub(crate) fn before(
        &mut self,
        hooks: &[Py<PyAny>],
        req: &Bound<'py, Request>,
    ) -> Result<Option<HandlerResponse>, HandlerError> {
        for hook in hooks {
            let value = self.call(
                hook,
                (req,),
                RequestStage::BeforeHook,
                ExceptionMapping::AsRaised,
            )?;
            if !value.is_none() {
                return Ok(Some((self.to_response)(value)?));
            }
        }
        Ok(None)
    }

    /// Calls the handler and returns its value, with any awaitable already run.
    pub(crate) fn handler(
        &mut self,
        handler: &Py<PyAny>,
        req: &Bound<'py, Request>,
        meaning: ExceptionMapping<'_>,
    ) -> Result<Bound<'py, PyAny>, HandlerError> {
        self.call(handler, (req,), RequestStage::Handler, meaning)
    }

    /// Runs the after hooks; each sees the response so far and may replace it by returning
    /// something other than `None`.
    ///
    /// One Python `Response` is shared by every hook until one replaces the response: it is
    /// frozen, so reusing it saves a body copy per hook.
    pub(crate) fn after(
        &mut self,
        hooks: &[Py<PyAny>],
        req: &Bound<'py, Request>,
        mut response: HandlerResponse,
    ) -> Result<HandlerResponse, HandlerError> {
        let mut shown: Option<Bound<'py, Response>> = None;
        for hook in hooks {
            let current = match &shown {
                Some(current) => current.clone(),
                None => shown
                    .insert(crate::exchange::to_response::response_to_py(&response, self.py)?)
                    .clone(),
            };
            let value = self.call(
                hook,
                (req, current),
                RequestStage::AfterHook,
                ExceptionMapping::AsRaised,
            )?;
            if !value.is_none() {
                response = (self.to_response)(value)?;
                shown = None;
            }
        }
        Ok(response)
    }

    /// Calls `f(*args)` and runs any awaitable it returns; only an exception from the call
    /// itself goes through `meaning`, since by the time an awaitable runs there is a loop.
    fn call<A: PyCallArgs<'py>>(
        &mut self,
        f: &Py<PyAny>,
        args: A,
        stage: RequestStage,
        meaning: ExceptionMapping<'_>,
    ) -> Result<Bound<'py, PyAny>, HandlerError> {
        let py = self.py;
        let raised = |e: PyErr| HandlerError::python(py, stage, &e);
        let value = f.bind(py).call1(args).map_err(|e| {
            raised(match meaning {
                ExceptionMapping::AsRaised => e,
                ExceptionMapping::NoLoopHint { key, handler } => {
                    explain_no_current_loop(py, key, handler, e)
                }
            })
        })?;
        match AwaitableKind::of(&value).map_err(raised)? {
            None => Ok(value),
            // The task runs in the request's context so its `ContextVar` writes reach the
            // rest of the request.
            Some(AwaitableKind::InTask) => {
                let event_loop = self.event_loop.event_loop(py)?;
                self.rc.run_in_task(&event_loop, &value, stage)
            }
            // A `Future` is already scheduled in its own context, so it is only waited for.
            Some(AwaitableKind::Future) => {
                let event_loop = self.event_loop.event_loop(py)?;
                event_loop
                    .call_method1(intern!(py, "run_until_complete"), (value,))
                    .map_err(raised)
            }
        }
    }
}

/// Turns asyncio's no-current-loop `RuntimeError` from the `def` handler of `key` into a
/// `mipyre.engine.NoEventLoopError` naming the route and what works instead, with asyncio's error
/// as its cause; any other error comes back unchanged. A `def` handler has no current loop because
/// the worker's loop was set on the thread that built the worker, not on the one serving.
fn explain_no_current_loop(
    py: Python<'_>,
    key: &RouteKey,
    handler: &Py<PyAny>,
    raised: PyErr,
) -> PyErr {
    if !is_no_current_loop(py, &raised) {
        return raised;
    }
    let RouteKey { method, path } = key;
    let handler = handler.bind(py);
    let name = match handler.getattr(intern!(py, "__qualname__")) {
        Ok(name) => name.to_string(),
        // A callable with no `__qualname__` (a `functools.partial`, an instance with
        // `__call__`) is named by its repr; any other failure of the lookup is the error.
        Err(e) if e.is_instance_of::<pyo3::exceptions::PyAttributeError>(py) => handler.to_string(),
        Err(e) => {
            e.set_cause(py, Some(raised));
            return e;
        }
    };
    let message = format!(
        "{method} {path} (handler {name}) asked asyncio for the current event loop, but \
         a def handler has none: mipyre does not support calls that need the current \
         loop (asyncio.get_event_loop() and the like) in a def route. Make the route \
         `async def`, or run the coroutine in the handler with asyncio.run(...) or on a \
         loop of its own from asyncio.new_event_loop()."
    );
    match no_event_loop_error_type(py) {
        Ok(ty) => {
            let err = PyErr::from_type(ty.clone(), message);
            err.set_cause(py, Some(raised));
            err
        }
        Err(failed) => {
            failed.set_cause(py, Some(raised));
            failed
        }
    }
}

/// Tells whether `err` is exactly a `RuntimeError`, not a subclass, with asyncio's
/// "no current event loop" text.
fn is_no_current_loop(py: Python<'_>, err: &PyErr) -> bool {
    const NO_CURRENT_LOOP: &str = "There is no current event loop";
    if !err.get_type(py).is(py.get_type::<PyRuntimeError>()) {
        return false;
    }
    err.value(py)
        .str()
        .is_ok_and(|text| text.to_string_lossy().starts_with(NO_CURRENT_LOOP))
}

/// Returns this interpreter's `mipyre.engine.NoEventLoopError` class, a `RuntimeError` subclass.
fn no_event_loop_error_type(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    static TYPE_OBJECT: PerInterpreterCell<Py<PyType>> = PerInterpreterCell::new();
    let ty = TYPE_OBJECT.get_or_try_init(py, || {
        PyErr::new_type(
            py,
            c"mipyre.engine.NoEventLoopError",
            Some(
                c"A def handler in a worker asked asyncio for the current event loop, which a \
                  def handler does not have: not supported. A RuntimeError.",
            ),
            Some(&py.get_type::<PyRuntimeError>()),
            None,
        )
    })?;
    Ok(ty.bind(py))
}
