//! asyncio's Task on a TPC thread's loop, kept in Rust: it steps a coroutine in its own context,
//! waits on the loop's Futures and Tasks without a callback object, and hands how it ended to
//! whoever started it.
//!
//! It does not know requests, replies or budgets: it is given a coroutine and, for a request, a
//! sink for its end. It is registered with asyncio's
//! `_register_task`, so `asyncio.all_tasks()` and `current_task()` see it.

use mipyre_subinterp::context::ContextSuspension;
use pyo3::exceptions::{
    PyBaseException, PyKeyboardInterrupt, PyRuntimeError, PyStopIteration, PySystemExit, PyTypeError,
};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::iterator::PySendResult;
use pyo3::types::{PyDict, PyIterator, PyType};

use super::future::{Future, FutureCore, FutureEnd};
use super::report::{report, Subject};
use super::{caught, EventLoop, LiveKey};
use crate::sync::Mutex;

/// Receives a request Task's end: the value it returned, or what it raised (a cancelled Task hands
/// its `CancelledError`).
pub(crate) type TaskSink = Box<dyn FnOnce(Python<'_>, Result<Bound<'_, PyAny>, Bound<'_, PyBaseException>>) + Send>;

/// Holds what stepping the coroutine needs; how the Task ended is in its `FutureCore`, under a lock
/// of its own that is never taken with this one.
struct TaskState {
    coro: Py<PyAny>,
    /// It is the iterator of the coroutine's `__await__`, which each step sends into, until the
    /// Task ends.
    steps: Option<Py<PyIterator>>,
    /// It is what the coroutine is suspended on.
    waiter: Option<Py<PyAny>>,
    /// It is a cancel asked for and not yet thrown into the coroutine or passed to what it waits on.
    cancel: Option<CancelRequest>,
    cancel_requests: u32,
    sink: Option<TaskSink>,
    live: Option<LiveKey>,
    name: Option<Py<PyAny>>,
    /// It is asyncio's flag for logging a Task destroyed while pending, which `gather` clears;
    /// this Task logs nothing when destroyed, so only Python reads it.
    log_destroy_pending: bool,
}

/// Holds a pending cancel's message (asyncio's `_must_cancel` with its `_cancel_message`).
struct CancelRequest {
    message: Option<Py<PyAny>>,
}

impl CancelRequest {
    fn message(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.message.as_ref().map(|m| m.clone_ref(py))
    }
}

/// Implements asyncio's `Task` on a TPC thread's loop (`mipyre.engine._Task`).
#[pyclass(module = "mipyre.engine", name = "_Task", frozen, weakref)]
pub(crate) struct Task {
    core: FutureCore,
    context: Py<PyAny>,
    state: Mutex<TaskState>,
}

/// Records what one step of the coroutine did.
enum StepOutcome<'py> {
    Returned(Bound<'py, PyAny>),
    Raised(PyErr),
    Yielded(Bound<'py, PyAny>),
}

/// Returns the iterator a Task steps `awaitable` with: the one its `__await__` returns.
///
/// # Errors
///
/// Raises `TypeError` when `awaitable` cannot be awaited.
pub(super) fn steps_of<'py>(awaitable: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyIterator>> {
    let py = awaitable.py();
    let not_awaitable = || PyTypeError::new_err(format!("a coroutine was expected, got {}", repr(awaitable)));
    let Some(await_method) = awaitable.getattr_opt(intern!(py, "__await__"))? else {
        return Err(not_awaitable());
    };
    await_method
        .call0()?
        .cast_into::<PyIterator>()
        .map_err(|_| not_awaitable())
}

impl Task {
    /// Makes a Task of `owner` that steps `coro` in `context` and registers it with asyncio; it
    /// takes no step until woken.
    ///
    /// # Errors
    ///
    /// Raises `TypeError` when `coro` cannot be awaited, or what registering raised.
    pub(super) fn new<'py>(
        owner: &Bound<'py, EventLoop>,
        coro: Bound<'py, PyAny>,
        context: ContextSuspension<'py>,
        sink: Option<TaskSink>,
        name: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, Self>> {
        let steps = steps_of(&coro)?;
        let task = Bound::new(
            owner.py(),
            Task {
                core: FutureCore::new(owner.clone().unbind()),
                context: context.context().clone().unbind(),
                state: Mutex::new(TaskState {
                    coro: coro.unbind(),
                    steps: Some(steps.unbind()),
                    waiter: None,
                    cancel: None,
                    cancel_requests: 0,
                    sink,
                    live: None,
                    name: name.map(Bound::unbind),
                    log_destroy_pending: true,
                }),
            },
        )?;
        owner.get().register_task(&task)?;
        Ok(task)
    }

    pub(super) fn is_pending(&self) -> bool {
        self.core.is_pending()
    }

    fn owner(&self) -> &EventLoop {
        self.core.owner().get()
    }

    pub(super) fn set_live(&self, key: LiveKey) {
        self.state.lock().live = Some(key);
    }

    /// Takes a step of the coroutine inside the Task's context, because `from` ended (its
    /// exception thrown in) or to go on (`None`), as asyncio's `Task.__step`.
    pub(super) fn wake(slf: &Bound<'_, Self>, from: Option<&Bound<'_, PyAny>>) {
        let py = slf.py();
        let this = slf.get();
        let thrown = match from.map(ended_with) {
            Some(Err(raised)) => Some(raised),
            Some(Ok(())) | None => None,
        };
        let Some((steps, thrown)) = this.take_step(py, thrown) else {
            return;
        };

        let entry = match ContextSuspension::new(this.context.bind(py).clone()).enter_again() {
            Ok(entry) => entry,
            Err(raised) => return Self::finish(slf, FutureEnd::Raised(raised.into_value(py))),
        };
        let owner = this.core.owner().bind(py);
        match EventLoop::enter_task(owner, slf) {
            Ok(()) => {
                let stepped = step(steps.bind(py), thrown);
                Self::after_step(slf, stepped);
                if let Err(raised) = EventLoop::leave_task(owner, slf) {
                    Self::report(slf, "a task could not leave the running task", &raised);
                }
            }
            Err(raised) => Self::finish(slf, FutureEnd::Raised(raised.into_value(py))),
        }
        if let Err(raised) = entry.exit() {
            Self::report(slf, "a task could not leave its context", &raised);
        }
    }

    /// Returns what the next step sends, with a pending cancel thrown in, or `None` when the Task
    /// ended meanwhile.
    fn take_step(&self, py: Python<'_>, thrown: Option<PyErr>) -> Option<(Py<PyIterator>, Option<PyErr>)> {
        let (steps, cancel, waiter) = {
            let mut state = self.state.lock();
            let steps = state.steps.as_ref()?.clone_ref(py);
            (steps, state.cancel.take(), state.waiter.take())
        };
        drop(waiter);
        let thrown = match (cancel, thrown) {
            (None, thrown) => thrown,
            (Some(_), Some(raised)) if raised.is_instance(py, self.owner().cancelled_error_type(py)) => Some(raised),
            (Some(cancel), _) => Some(self.owner().cancelled_error(py, cancel.message.as_ref())),
        };
        Some((steps, thrown))
    }

    fn after_step(slf: &Bound<'_, Self>, stepped: StepOutcome<'_>) {
        let py = slf.py();
        let this = slf.get();
        match stepped {
            StepOutcome::Returned(value) => {
                let cancel = this.state.lock().cancel.take();
                let end = match cancel {
                    Some(cancel) => FutureEnd::Cancelled {
                        message: cancel.message,
                        raised: None,
                    },
                    None => FutureEnd::Value(value.unbind()),
                };
                Self::finish(slf, end);
            }
            StepOutcome::Raised(raised) => {
                let end = if raised.is_instance(py, this.owner().cancelled_error_type(py)) {
                    FutureEnd::Cancelled {
                        message: None,
                        raised: Some(raised.into_value(py)),
                    }
                } else {
                    if raised.is_instance_of::<PyKeyboardInterrupt>(py) || raised.is_instance_of::<PySystemExit>(py) {
                        Self::report(
                            slf,
                            "a task raised to stop the process, which a TPC thread's loop does not",
                            &raised,
                        );
                    }
                    FutureEnd::Raised(raised.into_value(py))
                };
                Self::finish(slf, end);
            }
            StepOutcome::Yielded(yielded) => {
                if let Err(refused) = Self::suspend(slf, &yielded) {
                    Self::retry_with(slf, refused);
                }
            }
        }
    }

    /// Waits on what the coroutine yielded, as asyncio's Task does; a yield asyncio refuses is
    /// returned, to be thrown into the coroutine on its next step.
    fn suspend(slf: &Bound<'_, Self>, yielded: &Bound<'_, PyAny>) -> PyResult<()> {
        let py = slf.py();
        let this = slf.get();
        if yielded.is_none() {
            this.owner().queue_wake(py, slf.clone().unbind(), None);
            return Ok(());
        }
        let ours = match (yielded.cast::<Future>(), yielded.cast::<Task>()) {
            (Ok(future), _) => Some(future.get().core()),
            (_, Ok(task)) if task.is(slf) => {
                return Err(PyRuntimeError::new_err(format!(
                    "Task cannot await on itself: {}",
                    repr(slf.as_any())
                )));
            }
            (_, Ok(task)) => Some(&task.get().core),
            _ => None,
        };
        if let Some(core) = ours {
            if !core.owner().is(this.core.owner()) {
                return Err(wrong_loop(slf, yielded));
            }
            if !core.take_blocking() {
                return Err(yield_not_await(slf, yielded));
            }
            core.wake_on_end(yielded, slf.clone().unbind());
            return this.wait_on(yielded);
        }
        Self::suspend_on_foreign(slf, yielded)?;
        this.wait_on(yielded)
    }

    /// Waits on a future that is not the loop's own, through its `add_done_callback`, as
    /// asyncio's Task does with any future.
    fn suspend_on_foreign(slf: &Bound<'_, Self>, yielded: &Bound<'_, PyAny>) -> PyResult<()> {
        let py = slf.py();
        let blocking = match yielded.getattr_opt(intern!(py, "_asyncio_future_blocking"))? {
            Some(blocking) if !blocking.is_none() => blocking,
            _ => {
                let generator = slf.get().owner().is_generator(yielded)?;
                let text = if generator {
                    format!(
                        "yield was used instead of yield from for generator in task {} with {}",
                        repr(slf.as_any()),
                        repr(yielded)
                    )
                } else {
                    format!("Task got bad yield: {}", repr(yielded))
                };
                return Err(PyRuntimeError::new_err(text));
            }
        };
        let its_loop = slf.get().owner().get_loop_of(yielded)?;
        if !its_loop.is(slf.get().core.owner()) {
            return Err(wrong_loop(slf, yielded));
        }
        if !blocking.is_truthy()? {
            return Err(yield_not_await(slf, yielded));
        }
        yielded.setattr(intern!(py, "_asyncio_future_blocking"), false)?;
        // The wakeup is registered without the Task's context: `_wakeup` enters it itself, and a
        // context cannot be entered twice.
        let wakeup = slf.getattr(intern!(py, "_wakeup"))?;
        yielded.call_method1(intern!(py, "add_done_callback"), (wakeup,))?;
        Ok(())
    }

    /// Records that the coroutine waits on `future`, and cancels that at once when a cancel came
    /// while the coroutine ran.
    fn wait_on(&self, future: &Bound<'_, PyAny>) -> PyResult<()> {
        let py = future.py();
        let (replaced, message) = {
            let mut state = self.state.lock();
            let replaced = state.waiter.replace(future.clone().unbind());
            let message = state.cancel.as_ref().map(|cancel| cancel.message(py));
            (replaced, message)
        };
        drop(replaced);
        let Some(message) = message else {
            return Ok(());
        };
        if cancel(future, message)? {
            let passed = self.state.lock().cancel.take();
            drop(passed);
        }
        Ok(())
    }

    /// Takes the next step on a later turn with `error` thrown in, as asyncio does for a yield it
    /// refuses: the error reaches the coroutine through a Future that ended with it.
    fn retry_with(slf: &Bound<'_, Self>, error: PyErr) {
        let py = slf.py();
        let this = slf.get();
        let failed = match Bound::new(py, Future::new(this.core.owner().clone_ref(py))) {
            Ok(failed) => failed,
            Err(raised) => return Self::finish(slf, FutureEnd::Raised(raised.into_value(py))),
        };
        Future::fail(&failed, error.into_value(py));
        this.owner()
            .queue_wake(py, slf.clone().unbind(), Some(failed.into_any().unbind()));
    }

    /// Ends the Task: unregisters it, hands its end to its sink, leaves the loop's count of live
    /// Tasks and wakes who waits on it.
    fn finish(slf: &Bound<'_, Self>, end: FutureEnd) {
        let py = slf.py();
        let this = slf.get();
        let handed = end
            .outcome(py, this.owner())
            .map(|value| value.into_bound(py))
            .map_err(|raised| raised.into_value(py).into_bound(py));
        let Some(waiters) = this.core.settle(end) else { return };
        let (sink, live, steps, waiter) = {
            let mut state = this.state.lock();
            (
                state.sink.take(),
                state.live.take(),
                state.steps.take(),
                state.waiter.take(),
            )
        };
        drop((steps, waiter));
        if let Err(raised) = this.owner().unregister_task(slf) {
            Self::report(slf, "a task could not leave asyncio's tasks", &raised);
        }
        if let Some(sink) = sink {
            sink(py, handed);
        }
        if let Some(key) = live {
            this.owner().forget_live(py, key);
        }
        this.core.wake(slf.as_any(), waiters);
    }

    /// Asks the Task to cancel, as asyncio's `Task.cancel`: what it waits on is cancelled, or the
    /// cancel is thrown into its next step; `false` when it ended already.
    ///
    /// # Errors
    ///
    /// Returns what cancelling the future it waits on raised.
    pub(super) fn cancel_now(slf: &Bound<'_, Self>, message: Option<Py<PyAny>>) -> PyResult<bool> {
        let py = slf.py();
        if !slf.get().core.is_pending() {
            return Ok(false);
        }
        let waiter = {
            let mut state = slf.get().state.lock();
            state.cancel_requests += 1;
            state.waiter.as_ref().map(|w| w.clone_ref(py))
        };
        if let Some(waiter) = waiter {
            let message = message.as_ref().map(|m| m.clone_ref(py));
            if cancel(waiter.bind(py), message)? {
                return Ok(true);
            }
        }
        let replaced = slf.get().state.lock().cancel.replace(CancelRequest { message });
        drop(replaced);
        Ok(true)
    }

    /// Hands an error of the Task's that no caller can take to the loop's exception handler.
    pub(super) fn report(slf: &Bound<'_, Self>, message: &str, raised: &PyErr) {
        let owner = slf.get().core.owner().bind(slf.py());
        report(owner, message, raised, Subject::Task(slf.as_any()));
    }

    fn name(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(name) = &self.state.lock().name {
            return Ok(name.clone_ref(py));
        }
        let number = self.owner().next_task_number(py)?;
        let name = pyo3::types::PyString::new(py, &format!("Task-{number}"))
            .into_any()
            .unbind();
        let replaced = self.state.lock().name.replace(name.clone_ref(py));
        drop(replaced);
        Ok(name)
    }
}

/// Returns `Err` with what `ended` raised, or `Ok` when it has a value.
fn ended_with(ended: &Bound<'_, PyAny>) -> PyResult<()> {
    let py = ended.py();
    if let Ok(future) = ended.cast::<Future>() {
        return future.get().core().result(py).map(drop);
    }
    if let Ok(task) = ended.cast::<Task>() {
        return task.get().core.result(py).map(drop);
    }
    ended.call_method0(intern!(py, "result")).map(drop)
}

/// Cancels `future`, the loop's own or another, with `message`, and returns whether it took.
fn cancel(future: &Bound<'_, PyAny>, message: Option<Py<PyAny>>) -> PyResult<bool> {
    if let Ok(ours) = future.cast::<Future>() {
        return Ok(Future::cancel_now(ours, message));
    }
    if let Ok(task) = future.cast::<Task>() {
        return Task::cancel_now(task, message);
    }
    let py = future.py();
    let kwargs = PyDict::new(py);
    kwargs.set_item(intern!(py, "msg"), message)?;
    future
        .call_method(intern!(py, "cancel"), (), Some(&kwargs))?
        .is_truthy()
}

/// Sends `None` into the coroutine, or throws `thrown` into it.
fn step<'py>(steps: &Bound<'py, PyIterator>, thrown: Option<PyErr>) -> StepOutcome<'py> {
    let py = steps.py();
    let Some(thrown) = thrown else {
        return match caught(|| steps.send(&py.None().into_bound(py))) {
            Ok(PySendResult::Next(yielded)) => StepOutcome::Yielded(yielded),
            Ok(PySendResult::Return(value)) => StepOutcome::Returned(value),
            Err(raised) => StepOutcome::Raised(raised),
        };
    };
    let throw = match steps.getattr_opt(intern!(py, "throw")) {
        Ok(Some(throw)) => throw,
        Ok(None) => return StepOutcome::Raised(thrown),
        Err(raised) => return StepOutcome::Raised(raised),
    };
    match caught(|| throw.call1((thrown.into_value(py),))) {
        Ok(yielded) => StepOutcome::Yielded(yielded),
        Err(raised) if raised.is_instance_of::<PyStopIteration>(py) => {
            match raised.value(py).getattr(intern!(py, "value")) {
                Ok(value) => StepOutcome::Returned(value),
                Err(unreadable) => StepOutcome::Raised(unreadable),
            }
        }
        Err(raised) => StepOutcome::Raised(raised),
    }
}

fn repr(obj: &Bound<'_, PyAny>) -> String {
    match obj.repr() {
        Ok(text) => text.to_string(),
        Err(raised) => format!("<{} whose repr raised {raised}>", obj.get_type()),
    }
}

fn wrong_loop(slf: &Bound<'_, Task>, yielded: &Bound<'_, PyAny>) -> PyErr {
    PyRuntimeError::new_err(format!(
        "Task {} got Future {} attached to a different loop",
        repr(slf.as_any()),
        repr(yielded)
    ))
}

fn yield_not_await(slf: &Bound<'_, Task>, yielded: &Bound<'_, PyAny>) -> PyErr {
    PyRuntimeError::new_err(format!(
        "yield was used instead of yield from in task {} with {}",
        repr(slf.as_any()),
        repr(yielded)
    ))
}

#[pymethods]
impl Task {
    #[getter]
    fn _asyncio_future_blocking(&self) -> bool {
        self.core.blocking()
    }

    #[setter(_asyncio_future_blocking)]
    fn set_blocking(&self, blocking: bool) {
        self.core.set_blocking(blocking);
    }

    #[getter]
    fn _log_destroy_pending(&self) -> bool {
        self.state.lock().log_destroy_pending
    }

    #[setter(_log_destroy_pending)]
    fn set_log_destroy_pending(&self, log: bool) {
        self.state.lock().log_destroy_pending = log;
    }

    /// Steps the Task because a future that is not the loop's own ended; it is the callback the
    /// Task registers on such a future.
    fn _wakeup(slf: &Bound<'_, Self>, future: Bound<'_, PyAny>) {
        Self::wake(slf, Some(&future));
    }

    fn get_loop(&self, py: Python<'_>) -> Py<EventLoop> {
        self.core.owner().clone_ref(py)
    }

    fn get_coro(&self, py: Python<'_>) -> Py<PyAny> {
        self.state.lock().coro.clone_ref(py)
    }

    fn get_context(&self, py: Python<'_>) -> Py<PyAny> {
        self.context.clone_ref(py)
    }

    fn get_name(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.name(py)
    }

    fn set_name(&self, name: Bound<'_, PyAny>) -> PyResult<()> {
        let name = name.str()?.into_any().unbind();
        let replaced = self.state.lock().name.replace(name);
        drop(replaced);
        Ok(())
    }

    /// Returns no frames: the stack of a Task is not kept.
    #[pyo3(signature = (*, limit = None))]
    fn get_stack(&self, py: Python<'_>, limit: Option<Bound<'_, PyAny>>) -> Py<PyAny> {
        drop(limit);
        pyo3::types::PyList::empty(py).into_any().unbind()
    }

    fn done(&self) -> bool {
        !self.is_pending()
    }

    fn cancelled(&self) -> bool {
        self.core.is_cancelled()
    }

    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.core.result(py)
    }

    fn exception(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.core.exception(py)
    }

    #[pyo3(signature = (callback, *, context = None))]
    fn add_done_callback(slf: &Bound<'_, Self>, callback: Py<PyAny>, context: Option<Py<PyAny>>) -> PyResult<()> {
        slf.get().core.add_done_callback(slf.as_any(), callback, context)
    }

    fn remove_done_callback(&self, callback: Bound<'_, PyAny>) -> PyResult<usize> {
        self.core.remove_done_callback(&callback)
    }

    #[pyo3(signature = (msg = None))]
    fn cancel(slf: &Bound<'_, Self>, msg: Option<Py<PyAny>>) -> PyResult<bool> {
        slf.get().core.check_thread()?;
        Self::cancel_now(slf, msg)
    }

    fn cancelling(&self) -> u32 {
        self.state.lock().cancel_requests
    }

    fn uncancel(&self) -> u32 {
        let (left, withdrawn) = {
            let mut state = self.state.lock();
            let withdrawn = match state.cancel_requests {
                0 => None,
                1 => {
                    state.cancel_requests = 0;
                    state.cancel.take()
                }
                _ => {
                    state.cancel_requests -= 1;
                    None
                }
            };
            (state.cancel_requests, withdrawn)
        };
        drop(withdrawn);
        left
    }

    fn set_result(&self, _result: Bound<'_, PyAny>) -> PyResult<()> {
        Err(PyRuntimeError::new_err("Task does not support set_result operation"))
    }

    fn set_exception(&self, _exception: Bound<'_, PyAny>) -> PyResult<()> {
        Err(PyRuntimeError::new_err("Task does not support set_exception operation"))
    }

    fn _make_cancelled_error(&self, py: Python<'_>) -> Py<PyAny> {
        let message = self.state.lock().cancel.as_ref().and_then(|cancel| cancel.message(py));
        self.core.make_cancelled_error(py, message.as_ref())
    }

    fn __await__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    fn __iter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    /// Yields the Task to the Task awaiting it while it runs, and returns its result once it
    /// ended.
    fn __next__<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, Self>> {
        slf.get().core.next(slf.py()).map(|()| slf.clone())
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let name = self.name(py)?;
        let word = self.core.state_word().to_lowercase();
        Ok(format!("<_Task {word} name={}>", repr(name.bind(py))))
    }

    #[classmethod]
    fn __class_getitem__(cls: &Bound<'_, PyType>, _item: Bound<'_, PyAny>) -> Py<PyType> {
        cls.clone().unbind()
    }
}
