//! A worker's entries to its TPC thread's event loop: open it, start a request's Task on it, turn
//! it, cancel what is live at the end, close it. A request's Task answers it through a
//! [`LoopReply`] the caller awaits.
//!
//! It does not know budgets, limits or when the thread blocks: the server decides whether to take
//! a request, when to turn the loop and how long to wait for the bell. It decides only that a
//! worker whose loop keeps failing its turns retires, as it decides for a failed `gc.collect()`.

use std::future::Future;
use std::io;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use mipyre_subinterp::context::{await_kind, AwaitKind, ContextSuspension, PyContext};
use mipyre_util::panic_message;
use mipyre_util::throttle::LogThrottle;
use pyo3::exceptions::PyBaseException;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use tokio::sync::oneshot;

use super::start::{StartException, WorkerStartError};
use super::sync_runner::{Retirement, RetirementCause, SyncRunner};
use super::SubInterpreterWorker;
use crate::engine_queue::EngineReply;
use crate::event_loop::{EventLoop, LiveTasks, LoopBell, NextWait, PollerReadiness, TaskSink, TurnFailure};
use crate::exchange::error::{
    refuse, ErrorRecord, HandlerError, PyException, ReplyHolder, RequestStage, UnansweredError,
};
use crate::exchange::request::Request;
use crate::pool::{PoolJob, ReplySender};
use crate::worker_api::{JobReply, LoopJob};
use mipyre_http::plan::response::HandlerResponse;
use mipyre_http::plan::route::RouteId;

/// Sets how many failed turns in a row retire the worker: it retires on that one.
const RETIRING_TURN_FAILURES: u32 = 3;

/// Holds back repeats of a failed turn's log line.
const TURN_FAILURE_LOG_PERIOD: Duration = Duration::from_secs(10);

/// Returns whether a worker whose loop failed `consecutive_failures` turns in a row retires.
fn retires(consecutive_failures: u32) -> bool {
    consecutive_failures >= RETIRING_TURN_FAILURES
}

/// Records why one turn of the loop failed.
#[derive(Debug, thiserror::Error)]
pub enum TurnError {
    #[error("the event loop raised {0}")]
    Raised(PyException),
    #[error("the event loop's poller could not be scanned, or refused to set the loop's timer: {0}")]
    Poller(io::Error),
    #[error("a Rust panic in the event loop: {0}")]
    Panicked(String),
}

impl TurnError {
    pub(super) fn traceback(&self) -> &str {
        match self {
            TurnError::Raised(exception) => exception.traceback(),
            TurnError::Poller(_) | TurnError::Panicked(_) => "",
        }
    }
}

/// Records why a call into the loop outside a turn failed: cancelling its live Tasks, or starting
/// its finalizers. Neither scans the poller.
#[derive(Debug, thiserror::Error)]
pub enum LoopCallError {
    #[error("the event loop raised {0}")]
    Raised(PyException),
    #[error("a Rust panic in the event loop: {0}")]
    Panicked(String),
}

/// Routes a request to its handler on the loop.
enum Delivery {
    /// The app has no hooks: the handler is called at once, indexed by route, `None` for a
    /// route that does not run on a worker.
    Direct(Vec<Option<Py<PyAny>>>),
    /// The app has hooks: a Task runs `job_runner`'s coroutine for the job.
    Hooked(Py<PyAny>),
}

/// Holds what a TPC thread's loop keeps in the worker's interpreter. These references belong to
/// that interpreter and cannot be let go of without its thread state, so only
/// [`SubInterpreterWorker::close_event_loop`] lets go of them, with that interpreter current; a loop
/// dropped without being closed leaks them.
struct LoopObjects {
    event_loop: Py<EventLoop>,
    delivery: Delivery,
    /// Closing the loop puts these back: left set, the loop's own hooks keep the closed loop alive
    /// and call into it.
    replaced_asyncgen_hooks: Py<PyTuple>,
}

/// Logs, when dropped, that a loop was dropped without being closed; closing discharges it.
struct CloseObligation;

impl CloseObligation {
    fn discharge(self) {
        std::mem::forget(self);
    }
}

impl Drop for CloseObligation {
    fn drop(&mut self) {
        tracing::error!(
            target: "mipyre::server",
            "a TPC thread's event loop was dropped without being closed, so its references leaked"
        );
    }
}

/// Holds one TPC thread's event loop and what it takes to start requests on it.
///
/// It belongs to the caller, which takes it from [`SubInterpreterWorker::open_event_loop`] and
/// passes it to every entry, so no call can be written without a loop. The caller must close it
/// with [`SubInterpreterWorker::close_event_loop`], which releases what the loop holds in the
/// worker's interpreter; a loop dropped unclosed leaks those references and logs an error.
pub struct RequestLoop {
    objects: ManuallyDrop<LoopObjects>,
    unclosed: CloseObligation,
    bell: Arc<LoopBell>,
    turn_failures: u32,
    turn_failure_log: LogThrottle,
    opened: Instant,
}

impl RequestLoop {
    /// Returns the bell that wakes the loop's driver.
    pub fn bell(&self) -> Arc<LoopBell> {
        Arc::clone(&self.bell)
    }

    /// Registers a duplicate of the loop's poller fd with this thread's tokio reactor; the driver
    /// waits on what it returns as well as on the bell.
    ///
    /// # Panics
    ///
    /// tokio panics when the calling thread is not in a tokio runtime with I/O enabled.
    ///
    /// # Errors
    ///
    /// Returns the OS's refusal to duplicate the fd, or the reactor's to register it.
    pub fn register_poller(&self) -> io::Result<PollerReadiness> {
        self.objects.event_loop.get().register_poller()
    }

    /// Returns how many request Tasks are live, and how many of them have a large body; it takes
    /// no interpreter.
    pub fn live(&self) -> LiveTasks {
        self.objects.event_loop.get().live()
    }
}

/// A worker's response to a request it ran on its loop. It is a type of its own, not the pool's
/// `HandlerResponse`, so the loop's reply channel is a channel of its own: sharing the pool's would
/// give the pool's wait a second caller and take it out of line on the pool's path.
pub(crate) struct LoopAnswer(pub(crate) HandlerResponse);

pub(crate) type LoopReplySender = ReplySender<LoopAnswer>;

/// Waits for the answer to one request started on a loop. Dropping it lets the request run to its
/// end and drops the answer.
pub struct LoopReply(oneshot::Receiver<Result<LoopAnswer, ErrorRecord>>);

impl Future for LoopReply {
    type Output = Result<Result<HandlerResponse, ErrorRecord>, HandlerError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|reply| {
            reply
                .map(|answer| answer.map(|LoopAnswer(response)| response))
                .map_err(|_| refuse(UnansweredError::WorkerLost(ReplyHolder::RequestLoop)))
        })
    }
}

impl SubInterpreterWorker<SyncRunner> {
    /// Opens this thread's event loop in the worker's interpreter, with what it takes to start
    /// the app's requests on it. The thread must be in a tokio runtime with timers.
    ///
    /// # Errors
    ///
    /// Fails when the loop cannot be made (no tokio runtime on this thread, among others), or
    /// `job_runner` cannot be set up, or the opening panicked.
    pub fn open_event_loop(&mut self) -> Result<RequestLoop, WorkerStartError> {
        let worker = self.worker_id();
        let opened = self.with_gil(|runner, py| {
            open(py, runner).map_err(|raised| WorkerStartError::SetupRaised {
                worker,
                step: "opening the TPC thread's event loop",
                raised: StartException::capture(py, &raised),
            })
        });
        let objects = match opened {
            Ok(objects) => objects?,
            Err(payload) => {
                return Err(WorkerStartError::Panicked {
                    worker,
                    message: panic_message(&*payload),
                })
            }
        };
        let bell = objects.event_loop.get().bell();
        Ok(RequestLoop {
            objects: ManuallyDrop::new(objects),
            unclosed: CloseObligation,
            bell,
            turn_failures: 0,
            turn_failure_log: LogThrottle::new(TURN_FAILURE_LOG_PERIOD),
            opened: Instant::now(),
        })
    }

    /// Starts `request` for `route` on the loop `on`: the handler is called and, when it returns
    /// an awaitable, a Task takes its first step now; `large` marks the Task as holding a large
    /// body. The reply comes through the returned [`LoopReply`].
    ///
    /// # Errors
    ///
    /// Fails when the request cannot be set up or its Task cannot be started; what the handler
    /// raises is the reply.
    pub fn start_task(
        &mut self,
        on: &mut RequestLoop,
        route: RouteId,
        request: Request,
        large: bool,
    ) -> Result<LoopReply, HandlerError> {
        let (reply, answered) = tokio::sync::oneshot::channel();
        let job = JobReply::new(request.label(), EngineReply::Loop(reply));
        PoolJob::inc_created();
        let objects = &on.objects;
        let started = self.with_gil(|_, py| match &objects.delivery {
            Delivery::Direct(handlers) => start_direct(py, &objects.event_loop, handlers, route, request, job, large),
            Delivery::Hooked(run_job) => start_hooked(py, &objects.event_loop, run_job, route, request, job, large),
        });
        match started {
            Ok(started) => started.map(|()| LoopReply(answered)),
            Err(payload) => Err(HandlerError::panic(payload)),
        }
    }

    /// Turns the loop `on` once and returns how its driver waits next. A failed turn is counted,
    /// logged at most once per throttle period, and the driver turns again, until three fail in a
    /// row, which retires the worker.
    ///
    /// # Errors
    ///
    /// Returns the worker's retirement when its loop failed three turns in a row.
    pub fn turn(&mut self, on: &mut RequestLoop) -> Result<NextWait, Retirement> {
        let worker = self.worker_id();
        let event_loop = &on.objects.event_loop;
        let turned = self.with_gil(|_, py| {
            EventLoop::turn(event_loop.bind(py)).map_err(|failure| match failure {
                TurnFailure::Raised(raised) => TurnError::Raised(PyException::capture(py, &raised)),
                TurnFailure::Poller(failed) => TurnError::Poller(failed),
            })
        });
        let failure = match turned {
            Ok(Ok(wait)) => {
                on.turn_failures = 0;
                return Ok(wait);
            }
            Ok(Err(failure)) => failure,
            Err(payload) => TurnError::Panicked(panic_message(&*payload)),
        };
        on.turn_failures += 1;
        if retires(on.turn_failures) {
            return Err(Retirement::new(worker, RetirementCause::LoopTurns(failure)));
        }
        if let Some(suppressed) = on.turn_failure_log.admit(on.opened.elapsed()) {
            tracing::error!(
                target: "mipyre::server",
                worker_id = worker.get(),
                error = %failure,
                in_a_row = on.turn_failures,
                suppressed,
                "a turn of the TPC thread's event loop failed; it turns again"
            );
        }
        Ok(NextWait::Now)
    }

    /// Cancels every live request Task on the loop `on`, as the loop's life ends at the stop or
    /// the worker's retirement, and returns how many were live; each ends on a later turn.
    ///
    /// # Errors
    ///
    /// Returns what a cancel raised or a panic in it, after trying them all.
    pub fn cancel_live_tasks(&mut self, on: &RequestLoop) -> Result<usize, LoopCallError> {
        let event_loop = &on.objects.event_loop;
        let cancelled = self.with_gil(|_, py| {
            event_loop
                .get()
                .cancel_all(py)
                .map_err(|raised| LoopCallError::Raised(PyException::capture(py, &raised)))
        });
        match cancelled {
            Ok(cancelled) => cancelled,
            Err(payload) => Err(LoopCallError::Panicked(panic_message(&*payload))),
        }
    }

    /// Starts the loop `on`'s finalizers as a Task that is not a request: it closes the async
    /// generators left open, then waits at most `timeout` for the default executor's threads. The
    /// returned receiver hears when it ends, however it ends; an exception it ends with is logged.
    ///
    /// # Errors
    ///
    /// Returns what starting the Task raised or a panic in it.
    pub fn start_finalizers(
        &mut self,
        on: &RequestLoop,
        timeout: Duration,
    ) -> Result<oneshot::Receiver<()>, LoopCallError> {
        let worker = self.worker_id();
        let event_loop = &on.objects.event_loop;
        let started = self.with_gil(|_, py| {
            let (done, finished) = oneshot::channel();
            let sink: TaskSink = Box::new(move |py, ended| {
                if let Err(raised) = ended {
                    tracing::warn!(
                        target: "mipyre::server",
                        worker_id = worker.get(),
                        error = %PyException::capture(py, &PyErr::from_value(raised.into_any())),
                        "the TPC thread's event loop's finalizers raised"
                    );
                }
                if done.send(()).is_err() {
                    tracing::debug!(
                        target: "mipyre::server",
                        worker_id = worker.get(),
                        "the TPC thread's event loop's finalizers ended after its driver stopped waiting"
                    );
                }
            });
            py.import(intern!(py, "mipyre._worker.tpc_loop"))
                .and_then(|tpc_loop| tpc_loop.getattr(intern!(py, "finalize")))
                .and_then(|finalize| finalize.call1((event_loop, timeout.as_secs_f64())))
                .and_then(|coro| EventLoop::schedule_task(event_loop.bind(py), coro, sink))
                .map(|()| finished)
                .map_err(|raised| LoopCallError::Raised(PyException::capture(py, &raised)))
        });
        match started {
            Ok(started) => started,
            Err(payload) => Err(LoopCallError::Panicked(panic_message(&*payload))),
        }
    }

    /// Closes the loop `on` and lets go of everything it holds, in the worker's interpreter, after
    /// putting back the async generator hooks it replaced.
    pub fn close_event_loop(&mut self, on: RequestLoop) {
        let RequestLoop { objects, unclosed, .. } = on;
        unclosed.discharge();
        let objects = ManuallyDrop::into_inner(objects);
        let released = self.with_gil(|_, py| {
            let LoopObjects {
                event_loop,
                delivery,
                replaced_asyncgen_hooks,
            } = objects;
            if let Err(raised) = restore_asyncgen_hooks(replaced_asyncgen_hooks.bind(py)) {
                tracing::error!(
                    target: "mipyre::server",
                    error = %PyException::capture(py, &raised),
                    "putting back the async generator hooks a TPC thread's event loop replaced failed"
                );
            }
            replaced_asyncgen_hooks.drop_ref(py);
            event_loop.get().release(py);
            event_loop.drop_ref(py);
            match delivery {
                Delivery::Direct(handlers) => handlers.into_iter().flatten().for_each(|h| h.drop_ref(py)),
                Delivery::Hooked(run_job) => run_job.drop_ref(py),
            }
        });
        if let Err(payload) = released {
            tracing::error!(
                target: "mipyre::server",
                panic = %panic_message(&*payload),
                "a Rust panic closing a TPC thread's event loop; what it held stays allocated"
            );
        }
    }
}

/// Makes the loop, an instance of `tpc_loop`'s subclass of the Rust loop, with how requests reach
/// their handlers on it, and sets the thread's async generator hooks to the loop's. A loop made
/// and then not returned, its class's `__init__` failing included, is released first: dropped
/// open, the inherited `__del__` would call the Rust `close`, which raises, and print that on
/// stderr. So the loop is made in two steps, `__new__` and then `__init__`, as calling the class
/// does, to hold the loop when `__init__` fails.
fn open(py: Python<'_>, runner: &SyncRunner) -> PyResult<LoopObjects> {
    let class = py
        .import(intern!(py, "mipyre._worker.tpc_loop"))?
        .getattr(intern!(py, "make_loop_class"))?
        .call1((py.get_type::<EventLoop>(),))?;
    let event_loop = class
        .call_method1(intern!(py, "__new__"), (&class,))?
        .cast_into::<EventLoop>()?;
    let rest = event_loop
        .call_method0(intern!(py, "__init__"))
        .and_then(|_| delivery(py, runner))
        .and_then(|delivery| Ok((delivery, hook_asyncgens(&event_loop)?)));
    match rest {
        Ok((delivery, replaced_asyncgen_hooks)) => Ok(LoopObjects {
            event_loop: event_loop.unbind(),
            delivery,
            replaced_asyncgen_hooks: replaced_asyncgen_hooks.unbind(),
        }),
        Err(raised) => {
            event_loop.get().release(py);
            Err(raised)
        }
    }
}

/// Returns how requests reach their handlers on the loop.
fn delivery(py: Python<'_>, runner: &SyncRunner) -> PyResult<Delivery> {
    let handlers = || -> Vec<Option<Py<PyAny>>> {
        runner
            .routes
            .iter()
            .map(|route| route.handler.as_ref().map(|h| h.clone_ref(py)))
            .collect()
    };
    if runner.before_hooks.is_empty() && runner.after_hooks.is_empty() {
        return Ok(Delivery::Direct(handlers()));
    }
    let run_job = py
        .import(intern!(py, "mipyre._worker.async_requests"))?
        .getattr(intern!(py, "job_runner"))?
        .call1((
            handlers(),
            PyTuple::new(py, runner.before_hooks.iter().map(|h| h.bind(py)))?,
            PyTuple::new(py, runner.after_hooks.iter().map(|h| h.bind(py)))?,
            wrap_pyfunction!(crate::worker_api::_worker_to_response, py)?,
            wrap_pyfunction!(crate::worker_api::_loop_send, py)?,
            wrap_pyfunction!(crate::worker_api::_loop_fail, py)?,
        ))?;
    Ok(Delivery::Hooked(run_job.unbind()))
}

/// Sets the thread's async generator hooks to the loop's, as asyncio's `run_forever` does, and
/// returns the ones they replace.
fn hook_asyncgens<'py>(event_loop: &Bound<'py, EventLoop>) -> PyResult<Bound<'py, PyTuple>> {
    let py = event_loop.py();
    let sys = py.import(intern!(py, "sys"))?;
    let replaced = sys
        .call_method0(intern!(py, "get_asyncgen_hooks"))?
        .cast_into::<PyTuple>()?;
    let hooks = PyDict::new(py);
    hooks.set_item(
        intern!(py, "firstiter"),
        event_loop.getattr(intern!(py, "_asyncgen_firstiter_hook"))?,
    )?;
    hooks.set_item(
        intern!(py, "finalizer"),
        event_loop.getattr(intern!(py, "_asyncgen_finalizer_hook"))?,
    )?;
    sys.call_method(intern!(py, "set_asyncgen_hooks"), (), Some(&hooks))?;
    Ok(replaced)
}

/// Puts back the async generator hooks `hook_asyncgens` replaced.
fn restore_asyncgen_hooks(replaced: &Bound<'_, PyTuple>) -> PyResult<()> {
    let py = replaced.py();
    py.import(intern!(py, "sys"))?
        .call_method1(intern!(py, "set_asyncgen_hooks"), replaced)?;
    Ok(())
}

/// Calls the route's handler in the request's own context and starts a Task of what it returned
/// when that is awaitable; a plain value or what the call raised is the reply at once.
fn start_direct(
    py: Python<'_>,
    event_loop: &Py<EventLoop>,
    handlers: &[Option<Py<PyAny>>],
    route: RouteId,
    request: Request,
    job: JobReply,
    large: bool,
) -> Result<(), HandlerError> {
    let Some(Some(handler)) = handlers.get(route.index()) else {
        return Err(refuse(UnansweredError::NotOnWorker(route.index())));
    };
    let setup = |raised: PyErr| HandlerError::python(py, RequestStage::Setup, &raised);
    let request = Bound::new(py, request).map_err(setup)?;
    let entry = PyContext::new(py).map_err(setup)?.enter();
    let called = handler.bind(py).call1((request,));
    let context = entry.suspend().map_err(|failure| setup(failure.error))?;
    let value = match called {
        Ok(value) => value,
        Err(raised) => {
            job.fail_raised(py, raised.into_value(py).into_bound(py));
            return Ok(());
        }
    };
    if await_kind(&value) == AwaitKind::Plain {
        answer(py, &job, Ok(value));
        return Ok(());
    }
    let sink: TaskSink = Box::new(move |py, ended| answer(py, &job, ended));
    EventLoop::start_task(event_loop.bind(py), value, context, Some(sink), large).map_err(setup)
}

/// Starts a Task of `job_runner`'s coroutine for the request, which answers it through
/// `_loop_send` / `_loop_fail`.
fn start_hooked(
    py: Python<'_>,
    event_loop: &Py<EventLoop>,
    run_job: &Py<PyAny>,
    route: RouteId,
    request: Request,
    job: JobReply,
    large: bool,
) -> Result<(), HandlerError> {
    let setup = |raised: PyErr| HandlerError::python(py, RequestStage::Setup, &raised);
    let request = Py::new(py, request).map_err(setup)?;
    let job = Py::new(py, LoopJob::new(route.index(), request, job)).map_err(setup)?;
    let coro = run_job.bind(py).call1((job,)).map_err(setup)?;
    let context = ContextSuspension::from(PyContext::new(py).map_err(setup)?);
    EventLoop::start_task(event_loop.bind(py), coro, context, None, large).map_err(setup)
}

/// Answers the request with how its handler's Task ended.
fn answer(py: Python<'_>, job: &JobReply, ended: Result<Bound<'_, PyAny>, Bound<'_, PyBaseException>>) {
    let raised = match ended {
        Ok(value) => match job.answer(value) {
            Ok(()) => return,
            Err(raised) => raised.into_value(py).into_bound(py),
        },
        Err(raised) => raised,
    };
    job.fail_raised(py, raised);
}

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests;
