//! Runs a handler and its hooks on the main interpreter, on the calling thread's persistent
//! asyncio event loop.

use bytes::Bytes;
use pyo3::prelude::*;

use crate::exchange::error::{catch_panic, ErrorRecord, HandlerError, RequestStage, ResponseError};
use crate::exchange::request::Request;
use crate::exchange::stream::ResponseStream;
use crate::exchange::to_response::extract_response_data;
use crate::runtime::hook_chain::{main_response, Chain, EventLoopProvider, ExceptionMapping};
use crate::runtime::request_context::{in_request_context, RequestContext};
use mipyre_http::plan::response::HandlerResponse;
use mipyre_http::plan::route::HandlerTarget;
use mipyre_http::plan::site::SiteRules;

pub(crate) enum MainReply {
    Response(HandlerResponse),
    Stream(StreamReply),
}

pub(crate) struct StreamReply {
    pub(crate) rx: tokio::sync::mpsc::Receiver<Result<Bytes, std::convert::Infallible>>,
    pub(crate) head: crate::exchange::stream::StreamHead,
}

/// Holds a thread's asyncio event loop and closes it when the thread dies, so the loop's FDs and
/// tasks do not leak.
///
/// The close attaches to the loop's own interpreter explicitly, because by then the thread may
/// have no thread state and a bare `Python::attach` is refused once several interpreters have run.
/// If the interpreter is already finalized, the loop is leaked instead of touched.
struct LoopGuard(Option<(Py<PyAny>, pyo3::sync::InterpreterHandle)>);

impl Drop for LoopGuard {
    /// [arc:intentional-handle] reason: we are in Drop with no error
    /// channel to propagate to and no caller to defer to. A failed
    /// close() can still leak FDs/tasks, so rather than silently dropping
    /// the error we log it (with the error object) so the leak is
    /// diagnosable.
    fn drop(&mut self) {
        if let Some((loop_obj, interp)) = self.0.take() {
            // SAFETY: `Py_IsInitialized` may be called at any time. Tokio's blocking pool can
            // tear threads down after `Py_Finalize`, when attaching would be a use-after-free.
            if unsafe { pyo3::ffi::Py_IsInitialized() } != 0 {
                mipyre_subinterp::attach::with_interpreter(interp, move |py| close_loop(py, loop_obj));
            } else {
                // Dropping the loop would `Py_DECREF` on a finalized interpreter and crash, and
                // the process is exiting anyway.
                std::mem::forget(loop_obj);
            }
        }
    }
}

fn close_loop(py: Python<'_>, loop_obj: Py<PyAny>) {
    if let Err(e) = loop_obj.call_method0(py, "close") {
        tracing::warn!(
            target: "mipyre::server",
            error = %e,
            "event loop close() failed; loop FDs/tasks may have leaked"
        );
    }
}

thread_local! {
    static LOOP: std::cell::RefCell<LoopGuard> =
        const { std::cell::RefCell::new(LoopGuard(None)) };
}

/// Closes this thread's event loop now instead of at thread exit, for a long-lived thread that
/// must drop every `Py<T>` it owns before releasing its thread state.
pub(crate) fn close_thread_event_loop(py: Python<'_>) {
    let taken = LOOP.with(|tl| tl.borrow_mut().0.take());
    if let Some((loop_obj, _interp)) = taken {
        close_loop(py, loop_obj);
    }
}

/// Provides this thread's cached asyncio event loop, so no loop is made and torn down per request.
pub(crate) struct ThreadLoop;

impl EventLoopProvider for ThreadLoop {
    fn event_loop<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>, HandlerError> {
        LOOP.with(|tl| {
            let mut guard = tl.borrow_mut();

            let (loop_obj, loop_interp) = match &mut guard.0 {
                Some(existing) => &*existing,
                empty @ None => {
                    let new_loop = new_event_loop(py).map_err(|e| HandlerError::python(py, RequestStage::Setup, &e))?;
                    &*empty.insert((new_loop.unbind(), pyo3::sync::InterpreterHandle::current(py)))
                }
            };
            // Running the loop from another interpreter would mix two interpreters' objects.
            let current = pyo3::sync::InterpreterHandle::current(py);
            if *loop_interp != current {
                return Err(HandlerError::ForeignEventLoop {
                    loop_interp: loop_interp.id(),
                    request_interp: current.id(),
                });
            }
            Ok(loop_obj.bind(py).clone())
        })
    }
}

pub(crate) fn main_chain<'a, 'py>(py: Python<'py>, rc: &'a mut RequestContext<'py>) -> Chain<'a, 'py, ThreadLoop> {
    Chain {
        py,
        rc,
        event_loop: &ThreadLoop,
        to_response: main_response,
    }
}

/// Creates an asyncio event loop and sets it as this thread's current one.
fn new_event_loop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let asyncio = py.import("asyncio")?;
    let new_loop = asyncio.call_method0("new_event_loop")?;
    asyncio.call_method1("set_event_loop", (&new_loop,))?;
    Ok(new_loop)
}

/// Runs `target`'s handler and hooks on the main interpreter in a fresh `contextvars.Context`.
///
/// A failure, a panic included, is logged here on the thread that ran it, and the thread survives
/// it.
pub(crate) fn call_handler_with_hooks<M>(
    rules: &SiteRules<Py<PyAny>, M>,
    target: HandlerTarget,
    sky_req: Request,
) -> Result<MainReply, ErrorRecord> {
    let gil = &crate::telemetry::MAIN_GIL;
    gil.queued();
    let gil_wait_start = std::time::Instant::now();

    let label = sky_req.label();

    mipyre_subinterp::attach::with_main(|py| {
        gil.acquired(gil_wait_start.elapsed().as_micros() as u64);
        let hold_start = std::time::Instant::now();

        let result = catch_panic(|| in_request_context(py, |rc| run_with_hooks(py, rc, rules, target, sky_req)));

        gil.released(hold_start.elapsed().as_micros() as u64);
        result.map_err(|e| e.log(&label))
    })
}

pub(crate) fn run_with_hooks<'py, M>(
    py: Python<'py>,
    rc: &mut RequestContext<'py>,
    rules: &SiteRules<Py<PyAny>, M>,
    target: HandlerTarget,
    sky_req: Request,
) -> Result<MainReply, HandlerError> {
    let routes = &rules.routes;
    let mut chain = main_chain(py, rc);
    let req = Bound::new(py, sky_req).map_err(|e| HandlerError::python(py, RequestStage::Setup, &e))?;

    if let Some(short_circuit) = chain.before(routes.before_hooks(), &req)? {
        return Ok(MainReply::Response(short_circuit));
    }
    let handler = routes.handler(target);
    let value = chain.handler(handler, &req, ExceptionMapping::AsRaised)?;

    if value.is_instance_of::<ResponseStream>() {
        return Ok(MainReply::Stream(stream_info(&value)?));
    }
    let data = extract_response_data(py, value)?;
    let data = chain.after(routes.after_hooks(), &req, data)?;
    Ok(MainReply::Response(data))
}

/// Maps what a main-interpreter handler or hook returned: a `Stream` is streamed, anything else is
/// a response.
pub(crate) fn main_reply(value: Bound<'_, PyAny>) -> Result<MainReply, ResponseError> {
    if value.is_instance_of::<ResponseStream>() {
        return Ok(MainReply::Stream(stream_info(&value)?));
    }
    main_response(value).map(MainReply::Response)
}

fn stream_info(bound: &Bound<'_, PyAny>) -> Result<StreamReply, ResponseError> {
    let stream = bound
        .cast::<ResponseStream>()
        .map_err(|_| ResponseError::StreamConsumed)?
        .get();
    let rx = stream.take_rx().ok_or(ResponseError::StreamConsumed)?;
    Ok(StreamReply {
        rx,
        head: stream.head.clone(),
    })
}
