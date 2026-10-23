//! Carries why a request failed, as one typed error, from where it happens to the response.
//!
//! [`HandlerError::log`] consumes the error into an [`ErrorRecord`], and only an `ErrorRecord`
//! can become a response, so every failure is logged exactly once, where it happens, and
//! rendered exactly once, at the pipeline edge.
//!
//! This is the bottom layer: the worker, response and pipeline modules build on it, and it
//! imports none of them.

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyString;

use crate::exchange::body_stream;
use crate::exchange::http_exception::HttpException;
use crate::exchange::lane_failure::{LaneDecodeError, LaneIndex, WorkerExit, WorkerFailure};
use mipyre_http::io::body::BodyReadError;
use mipyre_http::io::request_id::RequestId;
use mipyre_http::plan::response::HandlerResponse;
use mipyre_shm::channel::PeerFault;
use mipyre_tpc::panic_message;
use mipyre_util::clock;
use mipyre_util::throttle::LogThrottle;

/// This throttle allows one overload line a minute, since under sustained overload every refused
/// request would log.
static OVERLOAD_THROTTLE: LogThrottle = LogThrottle::new(Duration::from_secs(60));
/// This throttle allows one "workers gone" line a minute during shutdown, apart from the overload
/// throttle so an overload line does not hold it back.
static WORKERS_GONE_THROTTLE: LogThrottle = LogThrottle::new(Duration::from_secs(60));

/// This throttle allows one crash-loop refusal line a minute; the lane's own alert says why it waits.
static CRASH_LOOP_THROTTLE: LogThrottle = LogThrottle::new(Duration::from_secs(60));

#[derive(Debug, Clone)]
pub(crate) struct PyException {
    type_name: String,
    message: String,
    traceback: String,
}

impl PyException {
    /// Captures `err`, formatting its traceback, so it belongs only on the error path.
    pub(crate) fn capture(py: Python<'_>, err: &PyErr) -> Self {
        let value = err.value(py);
        let type_name = match value.get_type().name() {
            Ok(name) => py_text(&name),
            Err(e) => format!("<type name unavailable: {e}>"),
        };
        let message = match value.str() {
            Ok(text) => py_text(&text),
            Err(e) => format!("<str() of the exception raised {e}>"),
        };
        PyException {
            type_name,
            message,
            traceback: format_traceback(py, value),
        }
    }

    pub(crate) fn traceback(&self) -> &str {
        &self.traceback
    }
}

impl fmt::Display for PyException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.type_name, self.message)
    }
}

/// Returns the formatting failure itself as the traceback if formatting fails.
fn format_traceback(py: Python<'_>, value: &Bound<'_, pyo3::exceptions::PyBaseException>) -> String {
    let formatted = py
        .import("traceback")
        .and_then(|tb| tb.call_method1("format_exception", (value,)))
        .and_then(|lines| PyString::new(py, "").call_method1("join", (lines,)))
        .and_then(|joined| Ok(py_text(joined.cast::<PyString>()?)));
    formatted.unwrap_or_else(|e| format!("<traceback unavailable: {e}>"))
}

/// Keeps a lone surrogate as its `\udXXX` escape rather than dropping it.
fn py_text(s: &Bound<'_, PyString>) -> String {
    if let Ok(text) = s.to_str() {
        return text.to_owned();
    }
    s.call_method1("encode", ("utf-8", "backslashreplace"))
        .and_then(|b| b.extract::<Vec<u8>>())
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_else(|e| {
            tracing::debug!(target: "mipyre::server", error = %e,
                "backslashreplace could not encode a str; rendering it lossily");
            s.to_string_lossy().into_owned()
        })
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ResponseError {
    #[error("the returned value could not be serialized as JSON: {0}")]
    NotJsonSerializable(PyException),
    /// The environment is at fault, not the value: isojson did not load or did not return bytes.
    #[error("the JSON serializer is unavailable: {0}")]
    SerializerUnavailable(PyException),
    #[error("response text is not valid Unicode: {0}")]
    NotUnicode(PyException),
    #[error("str() of the returned {type_name} raised {exception}")]
    StrRaised { type_name: String, exception: PyException },
    #[error(
        "a sub-interpreter handler returned a Stream; a Stream response is only served \
         from the main interpreter, so register the route with runtime='main'"
    )]
    StreamInWorker,
    #[error("the returned Stream was already consumed")]
    StreamConsumed,
    #[error("could not build the Response an after_request hook receives: {0}")]
    ResponseNotBuilt(PyException),
}

impl ResponseError {
    pub(crate) fn exception(&self) -> Option<&PyException> {
        match self {
            ResponseError::NotJsonSerializable(e)
            | ResponseError::SerializerUnavailable(e)
            | ResponseError::NotUnicode(e)
            | ResponseError::ResponseNotBuilt(e) => Some(e),
            ResponseError::StrRaised { exception, .. } => Some(exception),
            ResponseError::StreamInWorker | ResponseError::StreamConsumed => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestStage {
    Setup,
    ContextExit,
    BeforeHook,
    Handler,
    AfterHook,
    AsyncTask,
}

impl fmt::Display for RequestStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            RequestStage::Setup => "setting up the request",
            RequestStage::ContextExit => "leaving the request's contextvars.Context",
            RequestStage::BeforeHook => "a before_request hook",
            RequestStage::Handler => "the handler",
            RequestStage::AfterHook => "an after_request hook",
            RequestStage::AsyncTask => "the request's async task",
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum HandlerError {
    #[error("{stage} raised {exception}")]
    Raised {
        stage: RequestStage,
        exception: PyException,
    },
    #[error("{handler}; then leaving the request's contextvars.Context raised {exit}")]
    ContextNotExited {
        exit: PyException,
        handler: Box<HandlerError>,
    },
    /// The hook chain never reaches this, because it stops at the earlier error.
    #[error(
        "{stage}'s awaitable could not run: the request's contextvars.Context was not entered \
         again after an earlier awaitable's task"
    )]
    ContextLost { stage: RequestStage },
    #[error("a Rust panic while running the request: {payload}")]
    Panicked { payload: String },
    #[error(transparent)]
    Response(#[from] ResponseError),
    #[error(transparent)]
    Refused(UnansweredError),
    #[error(transparent)]
    BodyRejected(#[from] BodyReadError),
    /// The answer its raiser chose: not a server failure, so it is logged at debug, without a
    /// traceback. Boxed so the error, which every request's `Result` is as large as, stays small.
    #[error("{stage} raised HTTPException({})", .answer.status.as_u16())]
    Http {
        stage: RequestStage,
        answer: Box<HandlerResponse>,
    },
    #[error("could not start the thread that runs the handler: {0}")]
    ThreadNotSpawned(#[source] std::io::Error),
    /// The awaitable is not run, because that would mix two interpreters' objects.
    #[error(
        "this thread's event loop belongs to interpreter {loop_interp}, but the request runs \
         on interpreter {request_interp}"
    )]
    ForeignEventLoop {
        /// The id follows CPython's numbering, where main is 0.
        loop_interp: i64,
        request_interp: i64,
    },
    #[error("in the process-lane worker: {}", .0.error)]
    LaneWorkerFailed(WorkerFailure),
    /// This is a bug on one side of the lane.
    #[error("reading the process-lane answer: {0}")]
    LaneAnswerMalformed(#[source] LaneDecodeError),
}

/// This part of the server dropped a request's reply without answering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplyHolder {
    PoolWorker,
    MainBridge,
    Lane,
    AsyncEngine,
    WebsocketHandler,
    TaskCancelled,
}

impl fmt::Display for ReplyHolder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ReplyHolder::PoolWorker => "the sub-interpreter worker dropped the request",
            ReplyHolder::MainBridge => "the runtime='main' bridge dropped the request",
            ReplyHolder::Lane => "the process lane dropped the request without an answer",
            ReplyHolder::AsyncEngine => "the async engine dropped the request without answering it",
            ReplyHolder::WebsocketHandler => "the websocket handler thread exited before the upgrade",
            ReplyHolder::TaskCancelled => "the handler's task was cancelled",
        })
    }
}

/// Each group of workers is fed by its own queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WorkerGroup {
    Subinterp,
    MainBridge,
    Lanes,
}

impl WorkerGroup {
    fn queue(self) -> &'static str {
        match self {
            WorkerGroup::Subinterp => "sub-interpreter work queue",
            WorkerGroup::MainBridge => "runtime='main' bridge queue",
            WorkerGroup::Lanes => "process lane queue",
        }
    }
}

impl fmt::Display for WorkerGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            WorkerGroup::Subinterp => "sub-interpreter pool",
            WorkerGroup::MainBridge => "runtime='main' bridge",
            WorkerGroup::Lanes => "process lanes",
        })
    }
}

/// This ran out when a request was refused as overloaded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Capacity {
    WorkQueue(WorkerGroup),
    /// These are the sub-interpreter pool's permits for large bodies.
    AdmissionPermits,
    WebsocketConnections,
}

impl fmt::Display for Capacity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Capacity::WorkQueue(group) => group.queue(),
            Capacity::AdmissionPermits => "admission permits",
            Capacity::WebsocketConnections => "websocket connections",
        })
    }
}

/// The server gave up on the request without an answer from Python; the run counts each one as
/// a dropped request where it is answered.
#[derive(Debug, thiserror::Error)]
pub(crate) enum UnansweredError {
    #[error("no capacity for the request: {0}")]
    Overloaded(Capacity),
    #[error("{0}: the server is shutting down")]
    WorkersGone(WorkerGroup),
    /// Every worker stopped serving while the server runs; `cause` is why the first did.
    #[error("{group}: its workers stopped serving: {cause}")]
    WorkersDied { group: WorkerGroup, cause: Arc<str> },
    /// This concerns only the host that took the request, unlike [`UnansweredError::WorkersDied`]:
    /// another host can still serve, and the run goes on; `cause` is why the first lane broke.
    #[error("{group}: no lane of the host that took the request is usable: {cause}")]
    NoUsableLane { group: WorkerGroup, cause: Arc<str> },
    /// The server was built wrong; the request gets a 500 and an error line, and every other
    /// route keeps serving.
    #[error("{0}: the server has none of these workers, but its route table sent it a request")]
    NoWorkers(WorkerGroup),
    /// This is the same kind of build mistake as [`UnansweredError::NoWorkers`], answered the same
    /// way; the route is numbered by registration order.
    #[error("route #{0} has no handler on this worker, but main sent it here")]
    NotOnWorker(usize),
    /// The thread takes no more connections while the others serve; the text is the worker's own
    /// words for why it retired.
    #[error("this thread's sub-interpreter worker retired: {0}")]
    WorkerRetired(Arc<str>),
    #[error("{0}")]
    WorkerLost(ReplyHolder),
    /// The duration is the app's `request_timeout`.
    #[error("the handler did not answer within {0:?}")]
    TimedOut(Duration),
    /// The request is answered 502. `ran` is how long the host had seen the request running, `None` when the worker
    /// died before the host looked.
    #[error(
        "the worker running the request (pid {pid}, lane {lane}) died{}: {exit}",
        ran.map(|ran| format!(" after {ran:?}")).unwrap_or_default()
    )]
    WorkerDied {
        pid: i32,
        lane: LaneIndex,
        ran: Option<Duration>,
        exit: WorkerExit,
    },
    /// The request is answered 502 and is not run on another worker: it waited in the lane's ring for a worker that
    /// died before taking it, and that worker could have rewritten its bytes in the shared memory.
    #[error(
        "the worker of process lane {lane} (pid {pid}) died before it took the request ({exit}); the request is not \
         run on another worker, since the dead one could have changed it"
    )]
    WorkerDiedBeforeTaking {
        pid: i32,
        lane: LaneIndex,
        exit: WorkerExit,
    },
    /// The request is answered 504.
    #[error(
        "the handler ran {ran:?}, past the process-lane timeout of {timeout:?}; its worker \
         (pid {pid}, lane {lane}) was killed ({exit})"
    )]
    LaneTimedOut {
        pid: i32,
        lane: LaneIndex,
        ran: Duration,
        timeout: Duration,
        exit: WorkerExit,
    },
    /// The request is answered 502 and is not run again: its worker wrote a counter it cannot
    /// have written honestly, so whether it ran the request is unknown. The worker is killed.
    #[error(
        "the worker of process lane {lane} (pid {pid}) corrupted the lane: {}",
        fault_said(fault)
    )]
    PeerCorrupted {
        pid: i32,
        lane: LaneIndex,
        fault: PeerFault,
    },
    /// The request is answered 503 with `Retry-After` from `retry_in`; the other lanes and hosts
    /// keep serving, and a lane that answers ends the wait for good.
    #[error(
        "process lane {lane} is waiting {retry_in:?} before it forks a new worker: its last \
         {crashes} workers died without answering a request, the last one {exit}"
    )]
    LaneCrashLooping {
        lane: LaneIndex,
        crashes: u32,
        exit: WorkerExit,
        retry_in: Duration,
    },
    /// The request is answered 502, with the lane's own words for why it has no worker.
    #[error("no worker for lane {lane}: {cause}")]
    LaneLost { lane: LaneIndex, cause: Arc<str> },
    /// The late result of an inline handler that nothing could preempt is dropped, and what it
    /// raised rides along so the request logs once.
    #[error(
        "handler {handler} ran past the {budget:?} request budget ({took:?}), \
         blocking its TPC thread the whole time; answered 504 only once it returned. A sync \
         `def` runs inline and cannot be preempted; make slow work `async def` or runtime='main', \
         where the 504 is sent on time{}",
        raised.as_ref().map(|e| format!("; it also raised: {e}")).unwrap_or_default()
    )]
    Overran {
        handler: String,
        budget: Duration,
        took: Duration,
        raised: Option<Box<HandlerError>>,
    },
    /// The request is answered 504, as when the caller's own timer wins the race, because `raised`
    /// came from the watchdog's interruption; a handler that raised by itself stays a 500
    /// [`HandlerError::Raised`].
    #[error("the handler was interrupted at its {budget:?} request budget; {raised}")]
    Interrupted {
        budget: Duration,
        raised: Box<HandlerError>,
    },
}

impl UnansweredError {
    /// Returns the traceback of what the handler raised, which only a request given up on while
    /// its handler ran can carry.
    fn traceback(&self) -> Option<&str> {
        match self {
            UnansweredError::Overran { raised, .. } => raised.as_deref()?.traceback(),
            UnansweredError::Interrupted { raised, .. } => raised.traceback(),
            UnansweredError::Overloaded(_)
            | UnansweredError::WorkersGone(_)
            | UnansweredError::WorkersDied { .. }
            | UnansweredError::NoUsableLane { .. }
            | UnansweredError::NoWorkers(_)
            | UnansweredError::NotOnWorker(_)
            | UnansweredError::WorkerRetired(_)
            | UnansweredError::WorkerLost(_)
            | UnansweredError::TimedOut(_)
            | UnansweredError::WorkerDied { .. }
            | UnansweredError::WorkerDiedBeforeTaking { .. }
            | UnansweredError::LaneTimedOut { .. }
            | UnansweredError::PeerCorrupted { .. }
            | UnansweredError::LaneCrashLooping { .. }
            | UnansweredError::LaneLost { .. } => None,
        }
    }

    /// Returns the throttle of a refusal that repeats on every request while its condition
    /// lasts; every other refusal is one request's own failure and is logged as an error.
    fn throttle(&self) -> Option<&'static LogThrottle> {
        match self {
            UnansweredError::Overloaded(_) => Some(&OVERLOAD_THROTTLE),
            UnansweredError::WorkersGone(_) => Some(&WORKERS_GONE_THROTTLE),
            UnansweredError::LaneCrashLooping { .. } => Some(&CRASH_LOOP_THROTTLE),
            UnansweredError::WorkersDied { .. }
            | UnansweredError::NoUsableLane { .. }
            | UnansweredError::NoWorkers(_)
            | UnansweredError::NotOnWorker(_)
            | UnansweredError::WorkerRetired(_)
            | UnansweredError::WorkerLost(_)
            | UnansweredError::TimedOut(_)
            | UnansweredError::WorkerDied { .. }
            | UnansweredError::WorkerDiedBeforeTaking { .. }
            | UnansweredError::LaneTimedOut { .. }
            | UnansweredError::PeerCorrupted { .. }
            | UnansweredError::LaneLost { .. }
            | UnansweredError::Overran { .. }
            | UnansweredError::Interrupted { .. } => None,
        }
    }
}

/// Says what a worker wrote that no correct worker writes: the error the lane's end measured it with.
fn fault_said(fault: &PeerFault) -> &dyn std::fmt::Display {
    match fault {
        PeerFault::Counter(e) => e,
        PeerFault::Length(e) => e,
        PeerFault::Wakes(e) => e,
    }
}

pub(crate) fn refuse(refusal: UnansweredError) -> HandlerError {
    HandlerError::Refused(refusal)
}

impl HandlerError {
    /// Turns a `BodyReadError` from `req.stream` that the handler let through back into that
    /// rejection, answered 4xx like a buffered body's, and an `HTTPException` into the answer it
    /// asks for.
    pub(crate) fn python(py: Python<'_>, stage: RequestStage, err: &PyErr) -> Self {
        if let Some(reject) = body_stream::rejection_of(py, err) {
            return HandlerError::BodyRejected(reject);
        }
        if let Ok(raised) = err.value(py).cast::<HttpException>() {
            return match raised.get().answer(py, err) {
                Ok(answer) => HandlerError::Http {
                    stage,
                    answer: Box::new(answer),
                },
                Err(unanswerable) => HandlerError::Raised {
                    stage,
                    exception: PyException::capture(py, &unanswerable),
                },
            };
        }
        HandlerError::Raised {
            stage,
            exception: PyException::capture(py, err),
        }
    }

    pub(crate) fn panic(payload: Box<dyn Any + Send>) -> Self {
        HandlerError::Panicked {
            payload: panic_message(&*payload),
        }
    }

    pub(crate) fn traceback(&self) -> Option<&str> {
        match self {
            HandlerError::Raised { exception, .. } => Some(exception.traceback()),
            HandlerError::ContextNotExited { exit, handler } => handler.traceback().or(Some(exit.traceback())),
            HandlerError::LaneWorkerFailed(failure) => failure.traceback.as_deref(),
            HandlerError::Response(e) => e.exception().map(PyException::traceback),
            HandlerError::Refused(refusal) => refusal.traceback(),
            HandlerError::ContextLost { .. }
            | HandlerError::Panicked { .. }
            | HandlerError::BodyRejected(_)
            | HandlerError::Http { .. }
            | HandlerError::ThreadNotSpawned(_)
            | HandlerError::ForeignEventLoop { .. }
            | HandlerError::LaneAnswerMalformed(_) => None,
        }
    }

    /// Logs the error with its request; call it on the thread that ran the handler, so an error
    /// nobody waits for any more, because the request timed out, is still logged.
    pub(crate) fn log(self, request: &RequestLabel) -> ErrorRecord {
        let id = &request.id;
        let (method, path) = (request.method.as_str(), request.uri.path());
        match treatment(&self) {
            LogTreatment::Throttled(throttle) => log_refused_throttled(throttle, request, &self),
            LogTreatment::PeerGone => {
                mipyre_transport::stream::events::count(mipyre_transport::stream::events::StreamEvent::PeerGone);
                tracing::debug!(
                    target: "mipyre::handler", request_id = %id, method, path,
                    error = %self, "request body rejected: the client went away"
                )
            }
            LogTreatment::Warn => tracing::warn!(
                target: "mipyre::handler", request_id = %id, method, path,
                error = %self, "request body rejected"
            ),
            LogTreatment::Debug => tracing::debug!(
                target: "mipyre::handler", request_id = %id, method, path,
                error = %self, "request body rejected"
            ),
            LogTreatment::Answered => tracing::debug!(
                target: "mipyre::handler", request_id = %id, method, path,
                error = %self, "request answered with the HTTPException it raised"
            ),
            LogTreatment::Error => tracing::error!(
                target: "mipyre::handler", request_id = %id, method, path,
                error = %self, traceback = self.traceback(), "request failed"
            ),
        }
        ErrorRecord {
            error: Box::new(self),
            request_id: id.clone(),
        }
    }
}

#[derive(Clone, Copy)]
enum LogTreatment {
    Throttled(&'static LogThrottle),
    /// The client went away mid-body, which counts as [`StreamEvent::PeerGone`] and logs at
    /// debug.
    ///
    /// [`StreamEvent::PeerGone`]: mipyre_transport::stream::events::StreamEvent::PeerGone
    PeerGone,
    Warn,
    Debug,
    /// An `HTTPException`'s answer: its raiser decided it, and it is logged at debug.
    Answered,
    Error,
}

/// Throttles the refusals that repeat on every request while their condition lasts, since each
/// is counted in `dropped_requests` anyway, and warns only for a body read the client did not
/// cause to fail.
fn treatment(err: &HandlerError) -> LogTreatment {
    match err {
        HandlerError::Refused(refusal) => match refusal.throttle() {
            Some(throttle) => LogTreatment::Throttled(throttle),
            None => LogTreatment::Error,
        },
        HandlerError::BodyRejected(BodyReadError::ConnectionLost(source))
            if mipyre_http::io::events::peer_went_away(source.as_ref()) =>
        {
            LogTreatment::PeerGone
        }
        HandlerError::BodyRejected(BodyReadError::ConnectionLost(_)) => LogTreatment::Warn,
        HandlerError::BodyRejected(BodyReadError::TooLarge | BodyReadError::TimedOut(_)) => LogTreatment::Debug,
        HandlerError::Http { .. } => LogTreatment::Answered,
        HandlerError::Raised { .. }
        | HandlerError::ContextNotExited { .. }
        | HandlerError::ContextLost { .. }
        | HandlerError::Panicked { .. }
        | HandlerError::Response(_)
        | HandlerError::ThreadNotSpawned(_)
        | HandlerError::ForeignEventLoop { .. }
        | HandlerError::LaneWorkerFailed(_)
        | HandlerError::LaneAnswerMalformed(_) => LogTreatment::Error,
    }
}

/// Warns once per period with how many were held back since the last, and logs the rest at debug.
fn log_refused_throttled(throttle: &LogThrottle, request: &RequestLabel, error: &HandlerError) {
    let (id, method, path) = (&request.id, request.method.as_str(), request.uri.path());
    if let Some(held) = throttle.admit(clock::now()) {
        tracing::warn!(
            target: "mipyre::handler", request_id = %id, method, path,
            error = %error, held, "request refused"
        )
    } else {
        tracing::debug!(
            target: "mipyre::handler", request_id = %id, method, path,
            error = %error, "request refused"
        )
    }
}

/// The label names a request in its log lines after the request moved into the handler's
/// `Request`; making one only bumps reference counts and never allocates.
#[derive(Clone, Debug)]
pub(crate) struct RequestLabel {
    pub(crate) id: RequestId,
    pub(crate) method: hyper::Method,
    pub(crate) uri: hyper::Uri,
}

/// A logged [`HandlerError`] is the only form an error takes on its way to the response; only
/// [`HandlerError::log`] makes one, and the error is boxed so the `Result` stays small.
pub(crate) struct ErrorRecord {
    error: Box<HandlerError>,
    request_id: RequestId,
}

impl ErrorRecord {
    pub(crate) fn into_error_and_id(self) -> (HandlerError, RequestId) {
        (*self.error, self.request_id)
    }
}

impl fmt::Debug for ErrorRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Logged({} [{}])", self.error, self.request_id)
    }
}

/// Turns a panic in `f` into [`HandlerError::Panicked`]; `f` must leave nothing half-updated that
/// the caller reads after a panic.
pub(crate) fn catch_panic<T>(f: impl FnOnce() -> Result<T, HandlerError>) -> Result<T, HandlerError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
        .unwrap_or_else(|payload| Err(HandlerError::panic(payload)))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    pub(crate) fn captured_log(f: impl FnOnce()) -> String {
        captured_log_from(tracing::Level::INFO, f)
    }

    pub(crate) fn captured_log_from(level: tracing::Level, f: impl FnOnce()) -> String {
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
        tracing::subscriber::with_default(subscriber, f);
        let bytes = buf.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    pub(crate) fn label(id: &RequestId) -> RequestLabel {
        RequestLabel {
            id: id.clone(),
            method: hyper::Method::GET,
            uri: hyper::Uri::from_static("/p"),
        }
    }

    #[test]
    fn a_panic_keeps_its_payload() {
        let from_str = catch_panic::<()>(|| panic!("static payload"));
        let from_string = catch_panic::<()>(|| panic!("formatted {}", 42));
        let from_other = catch_panic::<()>(|| std::panic::panic_any(7_u8));
        let text = |r: Result<(), HandlerError>| r.unwrap_err().to_string();
        assert!(text(from_str).ends_with("static payload"));
        assert!(text(from_string).ends_with("formatted 42"));
        assert!(text(from_other).contains("neither &str nor String"));
    }

    #[test]
    fn a_panic_payload_is_logged_with_the_request_id() {
        let id = RequestId::mint();
        let log = captured_log(|| {
            let err = catch_panic::<()>(|| panic!("worker exploded: m4")).unwrap_err();
            let _ = err.log(&label(&id));
        });
        let line = log.lines().find(|l| l.contains("worker exploded: m4")).expect(&log);
        assert!(line.contains(&id.to_string()), "{line}");
        assert!(line.contains("\"level\":\"ERROR\""), "{line}");
    }

    #[test]
    fn a_logged_refusal_keeps_its_kind() {
        let err = refuse(UnansweredError::TimedOut(Duration::from_secs(30)));
        let id = RequestId::mint();
        let logged = captured_log_value(|| err.log(&label(&id)));
        assert!(matches!(
            logged.into_error_and_id().0,
            HandlerError::Refused(UnansweredError::TimedOut(_))
        ));
    }

    #[test]
    fn an_overrun_logs_once_with_what_the_handler_raised() {
        let id = RequestId::mint();
        let raised = HandlerError::Panicked {
            payload: "late and broken".into(),
        };
        let log = captured_log(|| {
            let err = refuse(UnansweredError::Overran {
                handler: "slow".into(),
                budget: Duration::from_secs(30),
                took: Duration::from_secs(31),
                raised: Some(Box::new(raised)),
            });
            let _ = err.log(&label(&id));
        });
        let lines: Vec<_> = log.lines().collect();
        assert_eq!(lines.len(), 1, "{log}");
        assert!(lines[0].contains("ran past"), "{log}");
        assert!(lines[0].contains("late and broken"), "{log}");
    }

    /// Leaves the lost-connection split to `peer_went_away`, which `mipyre_http::io::events` tests.
    #[test]
    fn each_error_gets_its_own_line_treatment() {
        let throttled_by = |err: HandlerError, throttle: &'static LogThrottle| match treatment(&err) {
            LogTreatment::Throttled(by) => assert!(std::ptr::eq(by, throttle), "{err}: another throttle"),
            _ => panic!("{err}: not throttled"),
        };
        for capacity in [
            Capacity::WorkQueue(WorkerGroup::Subinterp),
            Capacity::AdmissionPermits,
            Capacity::WebsocketConnections,
        ] {
            throttled_by(refuse(UnansweredError::Overloaded(capacity)), &OVERLOAD_THROTTLE);
        }
        for group in [WorkerGroup::Subinterp, WorkerGroup::MainBridge, WorkerGroup::Lanes] {
            throttled_by(refuse(UnansweredError::WorkersGone(group)), &WORKERS_GONE_THROTTLE);
        }
        throttled_by(
            refuse(UnansweredError::LaneCrashLooping {
                lane: LaneIndex(0),
                crashes: 3,
                exit: WorkerExit::Exited(1),
                retry_in: Duration::from_secs(1),
            }),
            &CRASH_LOOP_THROTTLE,
        );

        for rejection in [BodyReadError::TooLarge, BodyReadError::TimedOut(Duration::from_secs(5))] {
            let err = HandlerError::BodyRejected(rejection);
            assert!(matches!(treatment(&err), LogTreatment::Debug), "{err}");
        }

        for err in [
            HandlerError::Panicked { payload: "boom".into() },
            refuse(UnansweredError::TimedOut(Duration::from_secs(30))),
            refuse(UnansweredError::NoWorkers(WorkerGroup::Subinterp)),
            refuse(UnansweredError::WorkersDied {
                group: WorkerGroup::Subinterp,
                cause: "all gone".into(),
            }),
        ] {
            assert!(matches!(treatment(&err), LogTreatment::Error), "{err}");
        }
    }

    /// Runs `f` with its log captured and discarded.
    pub(crate) fn captured_log_value<T>(f: impl FnOnce() -> T) -> T {
        let mut out = None;
        captured_log(|| out = Some(f()));
        out.unwrap()
    }
}
