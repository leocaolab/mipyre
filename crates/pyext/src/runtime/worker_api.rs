//! Provides the functions an async engine (`_worker/async_engine.py`) calls to take requests and answer
//! them: a sub-interpreter worker's, and main's for its `async def` routes on the thread-per-core
//! server.
//!
//! Each engine gets its own [`AsyncInbox`] rather than a process-global channel, so two pools in
//! one process never see each other's requests.

use std::time::Duration;

use crate::sync::{AtomicBool, Mutex, Ordering};
use pyo3::exceptions::{PyBaseException, PyException as PyExceptionType, PyRuntimeError};
use pyo3::prelude::*;

use super::hook_chain::worker_response;
use super::job_queue::JobReceiver;
use super::main_bridge::{BridgeJob, BridgeReplySender};
use super::main_call::main_reply;
use super::pool::{PoolJob, PoolReplySender, ReplySender};
use super::worker::async_engine::EngineRoutes;
use super::worker_app;
use crate::exchange::error::{
    refuse, HandlerError, ReplyHolder, RequestLabel, RequestStage, ResponseError, UnansweredError,
};
use crate::exchange::request::Request;
use mipyre_http::plan::limits::RequestBudget;

mod guard;
use guard::worker_api_function;

/// Raises a Rust panic to the engine as a `RuntimeError`, which the engine handles, rather than
/// PyO3's `PanicException`.
///
/// It carries the panic unlogged, so `_worker_fail` answers with it instead of logging it a
/// second time as an exception.
#[pyclass(module = "mipyre.engine", extends = PyRuntimeError, frozen, name = "_EnginePanic")]
struct EnginePanic {
    text: String,
    error: Mutex<Option<HandlerError>>,
}

#[pymethods]
impl EnginePanic {
    fn __str__(&self) -> &str {
        &self.text
    }
}

impl EnginePanic {
    fn raise(py: Python<'_>, error: HandlerError) -> PyErr {
        let text = error.to_string();
        let carrier = EnginePanic {
            text: text.clone(),
            error: Mutex::new(Some(error)),
        };
        raise_carrier(py, carrier, text)
    }
}

/// Raises `carrier`, an exception that takes a typed error through Python to whoever catches it;
/// when its type cannot be made, the error's `text` survives as a `RuntimeError`, with why as the
/// cause.
fn raise_carrier<T>(py: Python<'_>, carrier: T, text: String) -> PyErr
where
    T: pyo3::PyClass,
    pyo3::PyClassInitializer<T>: From<T>,
{
    match Bound::new(py, carrier) {
        Ok(exception) => PyErr::from_value(exception.into_any()),
        Err(e) => {
            let err = PyRuntimeError::new_err(text);
            err.set_cause(py, Some(e));
            err
        }
    }
}

/// Holds one async engine's view of its queue and its own stop flag, which the engine sets when it
/// stops so its fetcher thread returns.
#[pyclass(module = "mipyre.engine", frozen, name = "_AsyncInbox")]
pub(crate) struct AsyncInbox {
    requests: EngineQueue,
    /// Is set by `_worker_close`, which then wakes the queue's sleeping receivers to see it.
    closed: AtomicBool,
    task_budget: Duration,
}

/// The async engine gives up on a request's task this much before the caller does, so it cancels
/// the task and answers 504 itself instead of computing a result nobody takes.
const ASYNC_TASK_MARGIN: Duration = Duration::from_secs(2);

/// Returns the async engine's per-task budget: [`ASYNC_TASK_MARGIN`] less than the request
/// budget, or half of it when that is smaller, so a short budget still leaves the task most of it.
fn async_task_budget(request: RequestBudget) -> Duration {
    let budget = request.get();
    budget - ASYNC_TASK_MARGIN.min(budget / 2)
}

/// The queue an async engine takes its requests from: an async worker's is the pool's, main's is
/// the bridge's own.
enum EngineQueue {
    Pool(JobReceiver<PoolJob>),
    Main(JobReceiver<BridgeJob>),
}

impl EngineQueue {
    /// Wakes the queue's sleeping receivers, so a closed inbox is seen.
    fn wake_all(&self) {
        match self {
            EngineQueue::Pool(requests) => requests.wake_all(),
            EngineQueue::Main(requests) => requests.wake_all(),
        }
    }
}

/// Where an async engine's answer to one request goes, which decides what the answer may be: only
/// main can answer with a `Stream`.
enum EngineReply {
    Pool(PoolReplySender),
    Main(BridgeReplySender),
}

impl EngineReply {
    fn is_closed(&self) -> bool {
        match self {
            EngineReply::Pool(reply) => reply.is_closed(),
            EngineReply::Main(reply) => reply.is_closed(),
        }
    }
}

impl AsyncInbox {
    /// Gives each task of the engine [`async_task_budget`] of `request`.
    pub(crate) fn new(requests: JobReceiver<PoolJob>, request: RequestBudget) -> Self {
        Self::of(EngineQueue::Pool(requests), request)
    }

    /// Takes main's `async def` routes' requests from the bridge's queue, each job's target
    /// indexing the engine's handlers.
    pub(crate) fn for_main(requests: JobReceiver<BridgeJob>, request: RequestBudget) -> Self {
        Self::of(EngineQueue::Main(requests), request)
    }

    fn of(requests: EngineQueue, request: RequestBudget) -> Self {
        AsyncInbox {
            requests,
            closed: AtomicBool::new(false),
            task_budget: async_task_budget(request),
        }
    }

    pub(crate) fn task_budget(&self) -> Duration {
        self.task_budget
    }

    /// Returns the next request whose caller still waits, with its handler's index and where its
    /// answer goes, dropping unrun the ones whose caller gave up, or `None` once the channel or
    /// this inbox is closed.
    fn next_live(&self) -> Option<(usize, Request, EngineReply)> {
        loop {
            let (index, request, reply) = match &self.requests {
                EngineQueue::Pool(requests) => {
                    let (route, request, reply) = requests.recv_unless(&self.closed)?.into_request();
                    (route.index(), request, EngineReply::Pool(reply))
                }
                EngineQueue::Main(requests) => {
                    let BridgeJob { target, request, reply } = requests.recv_unless(&self.closed)?;
                    (target.index(), request, EngineReply::Main(reply))
                }
            };
            if !reply.is_closed() {
                return Some((index, request, reply));
            }
            PoolJob::inc_completed();
        }
    }
}

/// Carries one request to the async engine; it is answered exactly once, by its drop with an
/// error if the engine lets go of it unanswered.
#[pyclass(module = "mipyre.engine", frozen, name = "_AsyncJob")]
pub(crate) struct AsyncJob {
    /// Indexes the engine's handlers: a worker's by route, main's by handler.
    route: usize,
    #[pyo3(get)]
    request: Py<Request>,
    reply: JobReply,
    task_budget: Duration,
}

/// Holds where a request's answer goes; the label outlives the answer so a later failure still
/// names the request.
struct JobReply {
    label: RequestLabel,
    reply: Mutex<Option<EngineReply>>,
}

#[pymethods]
impl AsyncJob {
    #[getter]
    fn route(&self) -> usize {
        self.route
    }
}

impl JobReply {
    fn new(label: RequestLabel, reply: EngineReply) -> Self {
        JobReply {
            label,
            reply: Mutex::new(Some(reply)),
        }
    }

    /// Takes where the answer goes.
    ///
    /// # Errors
    ///
    /// Fails when the request was already answered, which is a bug in the engine.
    fn take(&self) -> PyResult<EngineReply> {
        self.reply
            .lock()
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("this request was already answered"))
    }

    /// Answers this request with what its handler or a hook returned, mapped as its caller takes
    /// it: a worker's as a response, main's also as a `Stream`.
    fn answer(&self, value: Bound<'_, PyAny>) -> PyResult<()> {
        match self.take()? {
            EngineReply::Pool(reply) => {
                send_answer(reply, &self.label, worker_response(value).map_err(HandlerError::from));
            }
            EngineReply::Main(reply) => {
                send_answer(reply, &self.label, main_reply(value).map_err(HandlerError::from));
            }
        }
        Ok(())
    }

    /// Answers this request with `error`.
    ///
    /// # Errors
    ///
    /// Fails when the request was already answered, which is a bug in the engine.
    fn answer_error(&self, error: HandlerError) -> PyResult<()> {
        let reply = self.take()?;
        self.send_error(reply, error);
        Ok(())
    }

    /// Answers this request with `error`, or only logs it if the request was already answered.
    fn fail(&self, error: HandlerError) {
        let pending = self.reply.lock().take();
        match pending {
            Some(reply) => self.send_error(reply, error),
            None => {
                let _logged = error.log(&self.label);
            }
        }
    }

    fn send_error(&self, reply: EngineReply, error: HandlerError) {
        match reply {
            EngineReply::Pool(reply) => send_answer(reply, &self.label, Err(error)),
            EngineReply::Main(reply) => send_answer(reply, &self.label, Err(error)),
        }
    }
}

/// Logs an error with the request's label, then sends `result` to the waiting caller.
///
/// A caller that stopped waiting already said why, so the dropped result is noted only at debug.
fn send_answer<T>(reply: ReplySender<T>, label: &RequestLabel, result: Result<T, HandlerError>) {
    let result = result.map_err(|e| e.log(label));
    if reply.send(result).is_err() {
        tracing::debug!(target: "mipyre::server", request_id = %label.id, "nobody waits any more (the caller's budget ran out, or its client went away); dropping the result");
    }
    PoolJob::inc_completed();
}

impl Drop for JobReply {
    fn drop(&mut self) {
        if let Some(reply) = self.reply.get_mut().take() {
            self.send_error(reply, refuse(UnansweredError::WorkerLost(ReplyHolder::AsyncEngine)));
        }
    }
}

/// Carries the typed error for a return value that is not a response from `_worker_to_response`
/// to `_worker_fail`.
#[pyclass(module = "mipyre.engine", extends = PyExceptionType, frozen, name = "_InvalidResponse")]
struct InvalidResponse {
    text: String,
    error: Mutex<Option<ResponseError>>,
}

#[pymethods]
impl InvalidResponse {
    fn __str__(&self) -> &str {
        &self.text
    }
}

impl InvalidResponse {
    fn raise(py: Python<'_>, error: ResponseError) -> PyErr {
        let text = error.to_string();
        let carrier = InvalidResponse {
            text: text.clone(),
            error: Mutex::new(Some(error)),
        };
        raise_carrier(py, carrier, text)
    }
}

worker_api_function! {
/// Waits with the GIL released for the next request, returning `None` once the pool is gone or
/// the inbox is closed.
fn _worker_recv(py: Python<'_>, inbox: &Bound<'_, AsyncInbox>) -> Option<AsyncJob>;
raising through py;
{
        let inbox = inbox.get();
        let Some((route, request, reply)) = py.detach(|| inbox.next_live()) else {
            return Ok(None);
        };
        let reply = JobReply::new(request.label(), reply);
        match Py::new(py, request) {
            Ok(request) => Ok(Some(AsyncJob {
                route,
                request,
                reply,
                task_budget: inbox.task_budget,
            })),
            Err(e) => {
                reply.fail(HandlerError::python(py, RequestStage::Setup, &e));
                Err(e)
            }
        }
    }
}

worker_api_function! {
/// Closes `inbox` so the fetcher thread, which the interpreter waits for at its end, exits.
fn _worker_close(inbox: &Bound<'_, AsyncInbox>) -> ();
raising through inbox.py();
{
        let inbox = inbox.get();
        inbox.closed.store(true, Ordering::SeqCst);
        inbox.requests.wake_all();
        Ok(())
    }
}

worker_api_function! {
/// Answers `job` with anything a handler may return; what it raises goes to `_worker_fail`.
fn _worker_send(job: &Bound<'_, AsyncJob>, response: Bound<'_, PyAny>) -> ();
raising through job.py();
{
        job.get().reply.answer(response)
    }
}

worker_api_function! {
/// Answers `job` with the exception its task or `_worker_send` raised, or only logs it if the
/// request was already answered.
fn _worker_fail(py: Python<'_>, job: &Bound<'_, AsyncJob>, exception: Bound<'_, PyBaseException>);
answering &job.get().reply;
{
        let reply = &job.get().reply;
        // The engine's own exceptions carry their typed error, not yet logged.
        let carried = if let Ok(invalid) = exception.cast::<InvalidResponse>() {
            invalid
                .get()
                .error
                .lock()
                .take()
                .map(HandlerError::Response)
        } else if let Ok(panicked) = exception.cast::<EnginePanic>() {
            panicked.get().error.lock().take()
        } else {
            None
        };
        let error = carried.unwrap_or_else(|| {
            HandlerError::python(
                py,
                RequestStage::AsyncTask,
                &PyErr::from_value(exception.into_any()),
            )
        });
        reply.fail(error);
        Ok(())
    }
}

worker_api_function! {
/// Answers `job` with a 504 after its task ran past the budget and was cancelled.
fn _worker_timed_out(job: &Bound<'_, AsyncJob>);
answering &job.get().reply;
{
        let job = job.get();
        job.reply
            .answer_error(refuse(UnansweredError::TimedOut(job.task_budget)))
    }
}

worker_api_function! {
/// Turns a return value into a `Response`, so after hooks see one on every path; a value that is
/// no response raises [`InvalidResponse`].
fn _worker_to_response(py: Python<'_>, value: Bound<'_, PyAny>) -> Py<PyAny>;
raising through py;
{
        if value.is_instance_of::<crate::exchange::response::Response>() {
            return Ok(value.unbind());
        }
        worker_response(value)
            .and_then(|data| crate::exchange::to_response::response_to_py(&data, py))
            .map(|response| response.into_any().unbind())
            .map_err(|e| InvalidResponse::raise(py, e))
    }
}

/// Returns the routes of the app the worker's script registered on. A worker without one fails its
/// engine's start: an empty table would instead fail every request with a KeyError and lose the
/// cause.
fn registered_routes(py: Python<'_>) -> PyResult<worker_app::WorkerRoutes<'_>> {
    worker_app::worker_routes(py)?.ok_or_else(|| {
        PyRuntimeError::new_err(format!(
            "worker {:?} has no app: its script did not register routes on an app, \
             so the worker cannot serve any",
            std::thread::current().name().unwrap_or("<unnamed>")
        ))
    })
}

/// Returns what the worker's async engine serves: its app's handlers, indexed like main's route
/// table, and its hooks.
pub(crate) fn worker_engine_routes(py: Python<'_>) -> PyResult<EngineRoutes> {
    let routes = registered_routes(py)?;
    let unbind = |hooks: Vec<Bound<'_, PyAny>>| hooks.into_iter().map(Bound::unbind).collect();
    Ok(EngineRoutes {
        before: unbind(routes.before_hooks),
        after: unbind(routes.after_hooks),
        handlers: routes
            .routes
            .into_iter()
            .map(|r| r.handler.map(Bound::unbind))
            .collect(),
        streams: false,
    })
}

#[cfg(test)]
mod tests {
    use super::guard::{no_panic, no_panic_answering, panicked};
    use super::*;
    use crate::exchange::{captured_log_from, captured_log_value, label};
    use mipyre_http::io::request_id::RequestId;
    use mipyre_http::plan::response::HandlerResponse;

    /// The main interpreter records no worker app: a worker engine's routes are then the startup
    /// error (b/135), never an empty table that fails every request with a KeyError.
    #[test]
    fn a_worker_engine_without_an_app_fails_its_start() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let Err(e) = worker_engine_routes(py) else {
                panic!("the main interpreter has no worker app");
            };
            assert!(e.to_string().contains("has no app"), "{e}");
        });
    }

    #[test]
    fn the_async_task_budget_ends_before_the_callers() {
        let default = mipyre_http::plan::limits::Limits::DEFAULT.request_budget;
        assert_eq!(async_task_budget(default) + ASYNC_TASK_MARGIN, default.get());
        for seconds in [0.001, 1.0, 3.0, 4.0, 30.0, 600.0] {
            let request = RequestBudget::from_secs(seconds).unwrap();
            let task = async_task_budget(request);
            assert!(task < request.get(), "{seconds}: {task:?}");
            assert!(task >= request.get() / 2, "{seconds}: {task:?}");
        }
    }

    fn panicking() -> PyResult<()> {
        std::panic::panic_any("boom")
    }

    #[test]
    fn a_panic_in_a_call_is_the_request_error_naming_the_call() {
        match panicked("_worker_send", &"boom") {
            HandlerError::Panicked { payload } => assert_eq!(payload, "in _worker_send: boom"),
            other => panic!("not the panic: {other:?}"),
        }
        pyo3::Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            assert!(matches!(no_panic("_worker_send", py, || Ok(7)), Ok(7)));
        });
    }

    #[test]
    fn a_panic_in_a_call_that_answers_nothing_is_raised_as_the_engines_runtime_error() {
        pyo3::Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let raised =
                no_panic("_worker_to_response", py, panicking).expect_err("the panic comes out as an exception");
            let value = raised.value(py);
            // PyO3's PanicException is a BaseException the fetcher thread would die on.
            assert!(raised.is_instance_of::<PyRuntimeError>(py), "{raised}");
            assert_eq!(value.get_type().name().unwrap().to_string(), "_EnginePanic");
            let carried = value.cast::<EnginePanic>().unwrap().get().error.lock().take();
            assert!(
                matches!(&carried, Some(HandlerError::Panicked { payload }) if payload == "in _worker_to_response: boom"),
                "{carried:?}"
            );
        });
    }

    #[test]
    fn a_job_dropped_unanswered_answers_itself_and_an_answered_one_does_not() {
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let dropped = JobReply::new(label(&RequestId::mint()), EngineReply::Pool(tx));
        captured_log_value(|| drop(dropped));
        match rx.try_recv().expect("the dropped job was answered") {
            Err(logged) => assert!(
                matches!(
                    logged.into_error_and_id().0,
                    HandlerError::Refused(UnansweredError::WorkerLost(ReplyHolder::AsyncEngine))
                ),
                "not WorkerLost(AsyncEngine)"
            ),
            Ok(_) => panic!("a dropped job answered with a response"),
        }

        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let answered = JobReply::new(label(&RequestId::mint()), EngineReply::Pool(tx));
        captured_log_value(|| {
            answered
                .answer_error(HandlerError::Panicked {
                    payload: "its own answer".into(),
                })
                .expect("the first answer")
        });
        drop(answered);
        match rx.try_recv().expect("answered once") {
            Err(logged) => assert!(
                matches!(logged.into_error_and_id().0, HandlerError::Panicked { .. }),
                "the drop replaced the answer"
            ),
            Ok(_) => panic!("not the answer it was given"),
        }
    }

    #[test]
    fn a_panic_in_a_call_that_answers_a_request_fails_it_once_and_returns() {
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let reply = JobReply::new(label(&RequestId::mint()), EngineReply::Pool(tx));
        let returned = captured_log_value(|| no_panic_answering("_worker_fail", &reply, panicking));
        assert!(returned.is_ok(), "the call returns normally");
        let answered = rx.try_recv().expect("the request was answered");
        match answered.map_err(|logged| logged.into_error_and_id().0) {
            Err(HandlerError::Panicked { payload }) => assert_eq!(payload, "in _worker_fail: boom"),
            other => panic!("not the panic: {:?}", other.err()),
        }
        let again = captured_log_value(|| no_panic_answering("_worker_fail", &reply, panicking));
        assert!(again.is_ok());
        assert!(reply.reply.lock().is_none());
    }

    /// Pins that the engine may skip making a `Response` when there is no after hook.
    #[test]
    fn a_value_sent_as_returned_answers_what_its_response_answers() {
        pyo3::Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let globals = pyo3::types::PyDict::new(py);
            globals
                .set_item("Response", py.get_type::<crate::exchange::response::Response>())
                .unwrap();
            globals
                .set_item("Stream", py.get_type::<crate::exchange::stream::ResponseStream>())
                .unwrap();
            let to_response = pyo3::wrap_pyfunction!(_worker_to_response, py).unwrap();
            for value in [
                c"'text'",
                c"b'ok'",
                c"b'\\xff\\x00'",
                c"bytearray(b'raw')",
                c"None",
                c"42",
                c"Response('made', status_code=201, content_type='text/html', headers={'x-a': ['1', '2']})",
                c"Response(b'\\xfe', status_code=202)",
                c"Response('t', headers={'content-type': 'text/csv'})",
                c"'\\ud800'",
                c"Stream()",
            ] {
                let value = py.eval(value, Some(&globals), None).unwrap();
                let as_returned = worker_response(value.clone());
                let as_response = match to_response.call1((value.clone(),)) {
                    Ok(response) => worker_response(response),
                    Err(raised) => Err(raised
                        .value(py)
                        .cast::<InvalidResponse>()
                        .expect("a value that is no response raises _InvalidResponse")
                        .get()
                        .error
                        .lock()
                        .take()
                        .expect("carrying its error")),
                };
                match (as_returned, as_response) {
                    (Ok(a), Ok(b)) => assert_eq!(
                        (a.status, &a.body, a.content_type(), a.headers()),
                        (b.status, &b.body, b.content_type(), b.headers()),
                        "{value}"
                    ),
                    (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string(), "{value}"),
                    (a, b) => panic!("{value}: as returned {a:?}, as its Response {b:?}"),
                }
            }
        });
    }

    /// Pins the debug level, because a client that hangs up must not be able to fill the log.
    #[test]
    fn an_answer_whose_caller_is_gone_is_dropped_at_debug_and_the_worker_goes_on() {
        let response = || {
            Ok(HandlerResponse::new(
                hyper::StatusCode::OK,
                bytes::Bytes::from_static(b"late"),
                hyper::header::HeaderValue::from_static("text/plain"),
                mipyre_http::plan::response::HeaderLines::new(),
            ))
        };
        let id = RequestId::mint();

        let (tx, rx) = tokio::sync::oneshot::channel();
        drop(rx);
        let log = captured_log_from(tracing::Level::TRACE, || {
            send_answer(tx, &label(&id), response());
        });
        let lines: Vec<&str> = log.lines().collect();
        assert_eq!(lines.len(), 1, "{log}");
        assert!(lines[0].contains("\"level\":\"DEBUG\""), "{log}");
        assert!(lines[0].contains("dropping the result"), "{log}");
        assert!(lines[0].contains(&id.to_string()), "{log}");

        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let log = captured_log_from(tracing::Level::TRACE, || {
            send_answer(tx, &label(&id), response());
        });
        assert_eq!(log, "", "{log}");
        let answered = rx.try_recv().expect("the caller got its answer");
        assert_eq!(answered.map(|r| r.body).ok(), Some(bytes::Bytes::from_static(b"late")));
    }
}
