//! Gives each request its own `contextvars.Context`.
//!
//! A sync handler runs on a long-lived thread whose current context outlives the request, so a
//! `ContextVar` set by one request would be seen by the next. An awaitable runs as a task in the
//! request's context, because a task created the default way runs in a copy and its writes are
//! lost when it ends. The task starts eagerly on a loop marked running only for the call, so one
//! that never suspends never turns the loop and the thread's sync handlers can still call
//! `asyncio.run`.

use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;

use mipyre_subinterp::context::{await_slot, is_coroutine, AwaitSlot, ContextEntry, PyContext, SpareContext};

use crate::exchange::error::{HandlerError, PyException, RequestStage};

/// Says whether the request's context is entered on this thread; a panic out of the request
/// leaves it through the entry's drop.
pub(crate) enum RequestContext<'py> {
    Entered(ContextEntry<'py>),
    /// Means the context is not entered here, either while an awaitable's task runs in it or
    /// because entering it again failed; the thread is back in its pre-request context, so
    /// nothing is left to exit.
    Lost,
}

// It must stay one pointer, passed in a register along the chain; `Lost` uses the entry's null
// niche.
const _: () = assert!(std::mem::size_of::<RequestContext<'static>>() == std::mem::size_of::<usize>());

/// Runs `f` in an empty `contextvars.Context` entered on this thread for the call: the one in
/// `spare` when the last request left it empty and unshared, or a new one.
///
/// It costs two pointer swaps on the thread state, and one context object when none is reused.
/// The caller must release `spare` with its interpreter's thread state current.
///
/// # Errors
///
/// Returns `f`'s error, or the failure to make or leave the context. When leaving fails, `f`'s
/// value is dropped, and `f`'s own error, if any, is kept beside the exit's.
pub(crate) fn in_reused_context<'py, T>(
    py: Python<'py>,
    spare: &mut SpareContext,
    f: impl FnOnce(&mut RequestContext<'py>) -> Result<T, HandlerError>,
) -> Result<T, HandlerError> {
    let ctx = spare
        .take(py)
        .map_err(|e| HandlerError::python(py, RequestStage::Setup, &e))?;
    let mut entered = RequestContext::enter(ctx);
    let result = f(&mut entered);
    // A context that cannot be left fails the request; otherwise the thread would go on in a
    // context the request's code entered and never exited.
    if let Err(exit) = entered.exit_keeping(spare) {
        return Err(match result {
            Ok(_) => HandlerError::python(py, RequestStage::ContextExit, &exit),
            Err(handler) => HandlerError::ContextNotExited {
                exit: PyException::capture(py, &exit),
                handler: Box::new(handler),
            },
        });
    }
    result
}

/// Says how to run an awaitable a hook or handler returned.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum AwaitableKind {
    /// Is an asyncio `Future` or `Task`, already scheduled in the context it was made in.
    Future,
    /// Is any other awaitable, run with [`RequestContext::run_in_task`].
    InTask,
}

impl AwaitableKind {
    /// Classifies `obj`, returning `None` for a plain value.
    ///
    /// It runs on every hook and handler result, so a plain value costs two C-level slot
    /// probes and no attribute lookup; only an awaitable that is not a coroutine is asked
    /// whether it is a `Future`.
    ///
    /// # Errors
    ///
    /// Fails when that `Future` check raises.
    pub(crate) fn of(obj: &Bound<'_, PyAny>) -> PyResult<Option<Self>> {
        match await_slot(obj) {
            AwaitSlot::Coroutine => return Ok(Some(AwaitableKind::InTask)),
            AwaitSlot::Plain => return Ok(None),
            AwaitSlot::Awaitable => {}
        }
        Ok(Some(if is_future(obj)? {
            AwaitableKind::Future
        } else {
            AwaitableKind::InTask
        }))
    }
}

/// Checks the duck-typed marker every asyncio-compatible future sets, as `asyncio.isfuture` does.
fn is_future(obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    let py = obj.py();
    let marker = obj.getattr_opt(intern!(py, "_asyncio_future_blocking"))?;
    Ok(marker.is_some_and(|marker| !marker.is_none()))
}

impl<'py> RequestContext<'py> {
    /// Enters `ctx` on this thread, for a caller that runs its request in several steps.
    #[inline(always)]
    pub(crate) fn enter(ctx: PyContext<'py>) -> Self {
        RequestContext::Entered(ctx.enter())
    }

    /// Runs `awaitable` to completion on `event_loop` as a task in this context.
    ///
    /// The context is left for the run, because a context cannot be entered twice, and entered
    /// again however the run ends. A task takes only a coroutine, so any other awaitable is first
    /// wrapped in one.
    ///
    /// # Errors
    ///
    /// Fails when the run raises or the context cannot be entered again; in the latter case
    /// `self` stays `Lost`.
    pub(crate) fn run_in_task(
        &mut self,
        event_loop: &Bound<'py, PyAny>,
        awaitable: &Bound<'py, PyAny>,
        stage: RequestStage,
    ) -> Result<Bound<'py, PyAny>, HandlerError> {
        let py = event_loop.py();
        let raised = |e: PyErr| HandlerError::python(py, stage, &e);
        let entry = match std::mem::replace(self, RequestContext::Lost) {
            RequestContext::Entered(entry) => entry,
            // The chain stops at the error that lost the context, so this is unreachable from it.
            RequestContext::Lost => return Err(HandlerError::ContextLost { stage }),
        };
        let coro = if is_coroutine(awaitable) {
            awaitable.clone()
        } else {
            match drive(py, awaitable) {
                Ok(coro) => coro,
                Err(e) => {
                    *self = RequestContext::Entered(entry);
                    return Err(raised(e));
                }
            }
        };
        let suspension = match entry.suspend() {
            Ok(suspension) => suspension,
            Err(failure) => {
                *self = RequestContext::Entered(failure.entry);
                return Err(raised(failure.error));
            }
        };
        let ran = run(py).and_then(|run| run.call1((event_loop, coro, suspension.context())));
        // Failing to enter again is the error, since the rest of the request would run outside
        // the context; the run's own error becomes its cause.
        match suspension.enter_again() {
            Ok(entry) => {
                *self = RequestContext::Entered(entry);
                ran.map_err(raised)
            }
            Err(reentry) => {
                reentry.set_cause(py, ran.err());
                Err(raised(reentry))
            }
        }
    }

    /// Exits the context, a lost one being already left, and keeps it in `spare` if it can be
    /// reused.
    #[inline(always)]
    fn exit_keeping(self, spare: &mut SpareContext) -> PyResult<()> {
        match self {
            RequestContext::Entered(entry) => entry.exit_keeping(spare),
            RequestContext::Lost => Ok(()),
        }
    }
}

/// Looked up once per interpreter, because an import on every request costs more than running a
/// coroutine that returns at once.
static RUN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn run(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    RUN.get_or_try_init(py, || {
        py.import(intern!(py, "mipyre._worker.awaitable"))?
            .getattr(intern!(py, "run"))
            .map(Bound::unbind)
    })
    .map(|run| run.bind(py).clone())
}

/// Wraps `awaitable` in a native coroutine that awaits it.
fn drive<'py>(py: Python<'py>, awaitable: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    py.import(intern!(py, "mipyre._worker.awaitable"))?
        .getattr(intern!(py, "drive"))?
        .call1((awaitable,))
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    /// Registers `mipyre._worker.awaitable` from its source, because importing the `mipyre` package in a
    /// test binary would load the extension module again.
    pub(crate) fn register_awaitable_module(py: Python<'_>) {
        let source = std::ffi::CString::new(include_str!("../../../python/mipyre/_worker/awaitable.py"))
            .expect("no NUL in the source");
        let module = PyModule::from_code(py, &source, c"awaitable.py", c"mipyre._worker.awaitable")
            .expect("mipyre._worker.awaitable compiles");
        let modules = py
            .import("sys")
            .and_then(|sys| sys.getattr("modules"))
            .expect("sys.modules");
        if !modules.contains("mipyre").expect("sys.modules answers") {
            let package = PyModule::new(py, "mipyre").expect("an empty package");
            modules.set_item("mipyre", package).expect("mipyre registered");
        }
        modules
            .set_item("mipyre._worker.awaitable", module)
            .expect("mipyre._worker.awaitable registered");
    }
}

/// Counts instructions per request context; each test runs its cycle `BENCH_N` times (default 1),
/// and the count is the `perf stat` total minus the `BENCH_N=0` run, divided by `BENCH_N`:
///
///   BENCH_N=1000000 perf stat -x, -e instructions:u -- target/release/deps/mipyre_runtime-* \
///       --exact request_context::bench::a_reused_request_context --test-threads 1
#[cfg(test)]
mod bench {
    use std::hint::black_box;

    use pyo3::prelude::*;

    use super::tests::register_awaitable_module;
    use super::{in_reused_context, SpareContext};
    use crate::exchange::error::HandlerError;

    fn count() -> u64 {
        std::env::var("BENCH_N").ok().and_then(|n| n.parse().ok()).unwrap_or(1)
    }

    #[test]
    fn a_reused_request_context() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let mut spare = SpareContext::default();
            for _ in 0..count() {
                let served = in_reused_context(py, &mut spare, |_rc| Ok::<_, HandlerError>(black_box(py)));
                assert!(served.is_ok());
            }
            spare.release(py);
        });
    }

    #[test]
    fn a_coroutine_run_in_the_request_context() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            register_awaitable_module(py);
            let asyncio = py.import("asyncio").expect("asyncio");
            let event_loop = asyncio.call_method0("new_event_loop").expect("a loop");
            let code = c"async def noop():\n    return None\n";
            let globals = pyo3::types::PyDict::new(py);
            py.run(code, Some(&globals), None).expect("noop defined");
            let noop = globals.get_item("noop").expect("lookup").expect("noop");
            let mut spare = SpareContext::default();
            for _ in 0..count() {
                let coro = noop.call0().expect("a coroutine");
                let served = in_reused_context(py, &mut spare, |rc| {
                    rc.run_in_task(&event_loop, &coro, super::RequestStage::Handler)
                });
                assert!(served.is_ok());
            }
            event_loop.call_method0("close").expect("closed");
            spare.release(py);
        });
    }

    /// Uses a stub loop whose task factory only closes the coroutine, because asyncio's scheduling
    /// makes the count move with time.
    #[test]
    fn a_coroutine_handed_to_a_stub_loop() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            register_awaitable_module(py);
            let code = c"async def noop():\n    return None\n\nclass Done:\n    def done(self):\n        return True\n    def result(self):\n        return None\n\nclass Loop:\n    _ready = ()\n    _scheduled = ()\n    _stopping = False\n    def _run_forever_setup(self):\n        pass\n    def _run_forever_cleanup(self):\n        pass\n    def _task_factory(self, loop, coro, context):\n        coro.close()\n        return Done()\n    def create_task(self, coro, context):\n        return self._task_factory(self, coro, context=context)\n";
            let globals = pyo3::types::PyDict::new(py);
            py.run(code, Some(&globals), None).expect("noop and Loop defined");
            let noop = globals.get_item("noop").expect("lookup").expect("noop");
            let event_loop = globals
                .get_item("Loop")
                .expect("lookup")
                .expect("Loop")
                .call0()
                .expect("a stub loop");
            let mut spare = SpareContext::default();
            for _ in 0..count() {
                let coro = noop.call0().expect("a coroutine");
                let served = in_reused_context(py, &mut spare, |rc| {
                    rc.run_in_task(&event_loop, &coro, super::RequestStage::Handler)
                });
                assert!(served.is_ok());
            }
            spare.release(py);
        });
    }
}
