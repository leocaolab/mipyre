//! Provides the functions an async engine (`_worker/async_engine.py`) calls to take requests and answer
//! them: an async sub-interpreter worker's, and main's for its `async def` routes.
//!
//! Each engine gets its own [`AsyncInbox`] rather than a process-global channel, so two pools in
//! one process never see each other's requests.

use std::os::fd::RawFd;
use std::sync::Arc;

use crate::sync::Mutex;
use pyo3::exceptions::{PyBaseException, PyException as PyExceptionType, PyRuntimeError};
use pyo3::prelude::*;

use super::engine_queue::{send_answer, EngineInbox, EngineReply, TaskPlace};
use super::hook_chain::worker_response;
use super::main_call::main_reply;
use super::worker::async_engine::{EngineRoutes, EngineWebSockets};
use super::worker_app;
use crate::exchange::error::{refuse, HandlerError, ReplyHolder, RequestStage, ResponseError, UnansweredError};
use crate::exchange::loop_wait::{LoopMailbox, LoopWaits};
use crate::exchange::request::Request;
use crate::exchange::{HandlerEnds, PyException, WebSocket};
use crate::pool::answer_if_awaited;
use crate::websocket::{ConnectionHeld, HooksAnswer, WsConnection};

mod guard;
use guard::worker_api_function;
use mipyre_http::io::request_head::RequestLabel;

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

/// Is one async engine's end of its queue, as the engine's Python sees it.
#[pyclass(module = "mipyre.engine", frozen, name = "_AsyncInbox")]
pub struct AsyncInbox(pub(crate) EngineInbox);

#[pymethods]
impl AsyncInbox {
    /// Returns the fd the engine's loop waits on: readable when the engine should take.
    #[getter]
    fn doorbell(&self) -> RawFd {
        self.0.doorbell()
    }
}

/// Carries one request to the async engine; it is answered exactly once, by its drop with an
/// error if the engine lets go of it unanswered.
#[pyclass(module = "mipyre.engine", frozen, name = "_AsyncJob")]
pub struct AsyncJob {
    /// Indexes the engine's handlers: a worker's by route, main's by handler.
    route: usize,
    #[pyo3(get)]
    request: Py<Request>,
    reply: JobReply,
    /// Is dropped after `reply`, so the request is answered before its place in flight is given
    /// back; the job is dropped when its task ends.
    _place: TaskPlace,
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

    /// Answers this request with `error`, or only logs it if the request was already answered.
    fn fail(&self, error: HandlerError) {
        let pending = self.reply.lock().take();
        match pending {
            Some(reply) => reply.fail(&self.label, error),
            None => {
                let _logged = error.log(&self.label);
            }
        }
    }
}

impl Drop for JobReply {
    fn drop(&mut self) {
        if let Some(reply) = self.reply.get_mut().take() {
            reply.fail(
                &self.label,
                refuse(UnansweredError::WorkerLost(ReplyHolder::AsyncEngine)),
            );
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
/// Takes the requests queued for `inbox`'s engine, each as a job, or returns `None` once the queue is
/// closed. A request whose `Request` cannot be made fails alone.
fn _worker_take(py: Python<'_>, inbox: &Bound<'_, AsyncInbox>) -> Option<Vec<AsyncJob>>;
raising through py;
{
        let Some(taken) = inbox.get().0.take()? else {
            return Ok(None);
        };
        let jobs = taken
            .into_iter()
            .filter_map(|(job, place)| {
                let reply = JobReply::new(job.request.label(), job.reply);
                match Py::new(py, job.request) {
                    Ok(request) => Some(AsyncJob {
                        route: job.handler,
                        request,
                        reply,
                        _place: place,
                    }),
                    Err(e) => {
                        reply.fail(HandlerError::python(py, RequestStage::Setup, &e));
                        None
                    }
                }
            })
            .collect();
        Ok(Some(jobs))
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
/// table, its hooks, and its WebSocket handlers when `connections` is where main hands it
/// connections.
pub(crate) fn worker_engine_routes(
    py: Python<'_>,
    connections: Option<Arc<LoopMailbox<WsConnection>>>,
) -> PyResult<EngineRoutes> {
    let routes = registered_routes(py)?;
    let unbind = |hooks: Vec<Bound<'_, PyAny>>| hooks.into_iter().map(Bound::unbind).collect();
    let websockets = connections.map(|connections| EngineWebSockets {
        handlers: routes.websockets.iter().map(|h| h.clone().map(Bound::unbind)).collect(),
        connections,
    });
    Ok(EngineRoutes {
        before: unbind(routes.before_hooks),
        after: unbind(routes.after_hooks),
        handlers: routes
            .routes
            .into_iter()
            .map(|r| r.handler.map(Bound::unbind))
            .collect(),
        streams: false,
        websockets,
    })
}

/// Is one engine's end of its connections, as the engine's Python sees it; dropping it stops the
/// engine taking more.
#[pyclass(frozen, module = "mipyre.engine", name = "_ConnectionInbox")]
pub struct ConnectionInbox(pub(crate) Arc<LoopMailbox<WsConnection>>);

#[pymethods]
impl ConnectionInbox {
    /// Returns the fd the engine's loop waits on: readable when connections were handed to it.
    #[getter]
    fn doorbell(&self) -> std::os::fd::RawFd {
        self.0.doorbell()
    }

    /// Takes the connections handed so far, each as a job.
    fn take(&self, py: Python<'_>) -> PyResult<Vec<WsJob>> {
        let taken = self.0.take()?;
        Ok(taken
            .into_iter()
            .filter_map(|connection| WsJob::new(py, connection))
            .collect())
    }
}

impl Drop for ConnectionInbox {
    fn drop(&mut self) {
        drop(self.0.close());
    }
}

/// One connection as the engine's task runs it: its hooks answer the upgrade, then its handler
/// gets the connection. Dropping it ends the connection's hold on the server's slot.
#[pyclass(frozen, module = "mipyre.engine", name = "_WsJob")]
pub struct WsJob {
    route: usize,
    #[pyo3(get)]
    request: Py<Request>,
    label: RequestLabel,
    /// Holds the connection's queues and the hooks' answer until the hooks have run.
    upgrade: Mutex<Option<(HandlerEnds, HooksAnswer)>>,
    _held: ConnectionHeld,
}

impl WsJob {
    /// Returns `None` when the request cannot be made, after answering the upgrade with why.
    fn new(py: Python<'_>, connection: WsConnection) -> Option<Self> {
        let WsConnection {
            route,
            request,
            ends,
            hooks_answer,
            ended,
        } = connection;
        let label = request.label();
        match Py::new(py, request) {
            Ok(request) => Some(WsJob {
                route: route.index(),
                request,
                label,
                upgrade: Mutex::new(Some((ends, hooks_answer))),
                _held: ended,
            }),
            Err(e) => {
                let error = HandlerError::python(py, RequestStage::Setup, &e).log(&label);
                answer_if_awaited(hooks_answer, Err(error));
                None
            }
        }
    }

    fn take_upgrade(&self) -> PyResult<(HandlerEnds, HooksAnswer)> {
        self.upgrade
            .lock()
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("this WebSocket upgrade was already answered"))
    }
}

#[pymethods]
impl WsJob {
    #[getter]
    fn route(&self) -> usize {
        self.route
    }

    /// Sends the 101 and returns the connection, or `None` when the handshake already gave up
    /// waiting (its request budget), when no 101 was sent.
    fn accept(&self, py: Python<'_>, waits: Py<LoopWaits>) -> PyResult<Option<WebSocket>> {
        let (ends, hooks_answer) = self.take_upgrade()?;
        if hooks_answer.send(Ok(None)).is_err() {
            return Ok(None);
        }
        Ok(Some(WebSocket::new(ends, self.request.clone_ref(py), waits)))
    }

    /// Answers the upgrade with what a `before_request` hook returned.
    fn refuse(&self, value: Bound<'_, PyAny>) -> PyResult<()> {
        let (_, hooks_answer) = self.take_upgrade()?;
        let answer = worker_response(value).map_err(|e| HandlerError::from(e).log(&self.label));
        answer_if_awaited(hooks_answer, answer.map(Some));
        Ok(())
    }

    /// Answers the upgrade with the exception a `before_request` hook raised.
    fn fail(&self, exception: Bound<'_, PyAny>) -> PyResult<()> {
        let (_, hooks_answer) = self.take_upgrade()?;
        let py = exception.py();
        let error = HandlerError::python(py, RequestStage::BeforeHook, &PyErr::from_value(exception)).log(&self.label);
        answer_if_awaited(hooks_answer, Err(error));
        Ok(())
    }

    /// Logs the exception the handler raised on the upgraded connection, which has no response
    /// left to carry it.
    fn raised(&self, exception: Bound<'_, PyAny>) {
        let py = exception.py();
        let exception = PyException::capture(py, &PyErr::from_value(exception));
        let label = &self.label;
        tracing::error!(target: "mipyre::handler", request_id = %label.id,
            method = label.method.as_str(), path = label.uri.path(), error = %exception,
            traceback = exception.traceback(), "WebSocket handler raised");
    }
}

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    use super::guard::{no_panic, no_panic_answering, panicked};
    use super::*;
    use crate::testkit::{captured_log_from, captured_log_value, label};
    use mipyre_http::plan::request_id::RequestId;
    use mipyre_http::plan::response::HandlerResponse;

    /// The main interpreter records no worker app: a worker engine's routes are then the startup
    /// error, never an empty table that fails every request with a KeyError.
    #[test]
    fn a_worker_engine_without_an_app_fails_its_start() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let Err(e) = worker_engine_routes(py, None) else {
                panic!("the main interpreter has no worker app");
            };
            assert!(e.to_string().contains("has no app"), "{e}");
        });
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
            answered.fail(HandlerError::Panicked {
                payload: "its own answer".into(),
            })
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
