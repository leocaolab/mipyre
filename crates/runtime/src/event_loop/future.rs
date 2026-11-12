//! asyncio's Future on a TPC thread's loop, kept in Rust: a resolved Future queues its done
//! callbacks on the loop, and wakes a Task of the loop that awaits it without a callback object.
//!
//! It does not drive coroutines: a Task does (the loop's own, `task.rs`, or asyncio's), through
//! the Future protocol (`__await__`, `_asyncio_future_blocking`, `add_done_callback`). It is
//! resolved only on its loop's thread, which its Python methods check.
//!
//! No lock here is held while Python runs or a Python object is let go of: either can call back
//! into the same Future, whose lock is not reentrant.

use pyo3::exceptions::{PyBaseException, PyStopIteration, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyType;

use super::task::Task;
use super::EventLoop;
use crate::sync::Mutex;

/// Records how a Future, or a Task, ended.
pub(super) enum FutureEnd {
    Value(Py<PyAny>),
    Raised(Py<PyBaseException>),
    /// It was cancelled with `message`, or, for a Task, its coroutine `raised` a
    /// `CancelledError`, which `result()` raises again, as asyncio keeps `_cancelled_exc`.
    Cancelled {
        message: Option<Py<PyAny>>,
        raised: Option<Py<PyBaseException>>,
    },
}

impl FutureEnd {
    fn clone_ref(&self, py: Python<'_>) -> FutureEnd {
        match self {
            FutureEnd::Value(value) => FutureEnd::Value(value.clone_ref(py)),
            FutureEnd::Raised(raised) => FutureEnd::Raised(raised.clone_ref(py)),
            FutureEnd::Cancelled { message, raised } => FutureEnd::Cancelled {
                message: message.as_ref().map(|m| m.clone_ref(py)),
                raised: raised.as_ref().map(|r| r.clone_ref(py)),
            },
        }
    }

    /// Returns what awaiting this end gives: its value, or the exception it raises.
    pub(super) fn outcome(&self, py: Python<'_>, owner: &EventLoop) -> PyResult<Py<PyAny>> {
        match self {
            FutureEnd::Value(value) => Ok(value.clone_ref(py)),
            FutureEnd::Raised(raised)
            | FutureEnd::Cancelled {
                raised: Some(raised), ..
            } => Err(PyErr::from_value(raised.bind(py).clone().into_any())),
            FutureEnd::Cancelled { message, raised: None } => Err(owner.cancelled_error(py, message.as_ref())),
        }
    }
}

/// Holds a callback `add_done_callback` registered, with the context it runs in.
pub(super) struct DoneCallback {
    pub(super) callback: Py<PyAny>,
    pub(super) context: Py<PyAny>,
}

/// Names who is told when a Future, or a Task, ends.
pub(super) enum Waiter {
    /// It is one of the loop's Tasks awaiting it, woken without a callback object.
    Task(Py<Task>),
    Callback(DoneCallback),
}

/// Holds how a Future or a Task ended (`None` while it has not), who waits for that, and asyncio's
/// `_asyncio_future_blocking` flag.
#[derive(Default)]
struct Completion {
    end: Option<FutureEnd>,
    waiters: Vec<Waiter>,
    blocking: bool,
}

impl Completion {
    /// Ends it with `end` and returns who to wake, or hands `end` back when it had ended already.
    fn settle(&mut self, end: FutureEnd) -> Result<Vec<Waiter>, FutureEnd> {
        if self.end.is_some() {
            return Err(end);
        }
        self.end = Some(end);
        Ok(std::mem::take(&mut self.waiters))
    }

    /// Registers `waiter`, or hands it back when this ended already and the waiter is due now.
    fn add(&mut self, waiter: Waiter) -> Option<Waiter> {
        if self.end.is_none() {
            self.waiters.push(waiter);
            return None;
        }
        Some(waiter)
    }

    fn callbacks(&self, py: Python<'_>) -> Vec<Py<PyAny>> {
        self.waiters
            .iter()
            .filter_map(|waiter| match waiter {
                Waiter::Callback(done) => Some(done.callback.clone_ref(py)),
                Waiter::Task(_) => None,
            })
            .collect()
    }

    /// Takes out the registrations of the callbacks in `matched`, the very objects, and returns
    /// them.
    fn remove(&mut self, matched: &[Py<PyAny>]) -> Vec<Waiter> {
        let (removed, kept) = std::mem::take(&mut self.waiters)
            .into_iter()
            .partition(|waiter| match waiter {
                Waiter::Callback(done) => matched.iter().any(|m| m.is(&done.callback)),
                Waiter::Task(_) => false,
            });
        self.waiters = kept;
        removed
    }
}

/// Holds what a Future and a Task share — the loop it belongs to and its `Completion` — and
/// carries the Future protocol for both, so the two classes cannot drift apart.
pub(super) struct FutureCore {
    owner: Py<EventLoop>,
    done: Mutex<Completion>,
}

impl FutureCore {
    pub(super) fn new(owner: Py<EventLoop>) -> Self {
        FutureCore {
            owner,
            done: Mutex::new(Completion::default()),
        }
    }

    pub(super) fn owner(&self) -> &Py<EventLoop> {
        &self.owner
    }

    pub(super) fn is_pending(&self) -> bool {
        self.done.lock().end.is_none()
    }

    pub(super) fn is_cancelled(&self) -> bool {
        matches!(self.done.lock().end, Some(FutureEnd::Cancelled { .. }))
    }

    pub(super) fn state_word(&self) -> &'static str {
        match &self.done.lock().end {
            None => "PENDING",
            Some(FutureEnd::Cancelled { .. }) => "CANCELLED",
            Some(FutureEnd::Value(_) | FutureEnd::Raised(_)) => "FINISHED",
        }
    }

    /// Ends it with `end` and returns who waits on it, or `None` when it ended already; the
    /// refused `end` is let go of after the lock.
    pub(super) fn settle(&self, end: FutureEnd) -> Option<Vec<Waiter>> {
        let settled = self.done.lock().settle(end);
        match settled {
            Ok(waiters) => Some(waiters),
            Err(refused) => {
                drop(refused);
                None
            }
        }
    }

    /// Returns a copy of how it ended, taken under the lock, for Python to read after it.
    fn end_copy(&self, py: Python<'_>) -> Option<FutureEnd> {
        self.done.lock().end.as_ref().map(|end| end.clone_ref(py))
    }

    /// Ends `me` (the Python object this is the core of) with `end` and queues who waits on it;
    /// `false` says it ended already and keeps its end.
    pub(super) fn end(&self, me: &Bound<'_, PyAny>, end: FutureEnd) -> bool {
        let Some(waiters) = self.settle(end) else { return false };
        self.wake(me, waiters);
        true
    }

    /// Queues `waiters` on the loop, each told that `me` ended.
    pub(super) fn wake(&self, me: &Bound<'_, PyAny>, waiters: Vec<Waiter>) {
        let py = me.py();
        let owner = self.owner.get();
        for waiter in waiters {
            match waiter {
                Waiter::Callback(done) => owner.queue_call(py, done, me.clone().unbind()),
                Waiter::Task(task) => owner.queue_wake(py, task, Some(me.clone().unbind())),
            }
        }
    }

    /// Makes `task` one woken when `me` ends; it is woken now when `me` ended already.
    pub(super) fn wake_on_end(&self, me: &Bound<'_, PyAny>, task: Py<Task>) {
        let due = self.done.lock().add(Waiter::Task(task));
        if let Some(due) = due {
            self.wake(me, vec![due]);
        }
    }

    /// Registers a done callback on `me`, in a copy of the current context unless `context` is
    /// given; on one that ended already it is queued now.
    pub(super) fn add_done_callback(
        &self,
        me: &Bound<'_, PyAny>,
        callback: Py<PyAny>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        let py = me.py();
        let context = match context {
            Some(context) => context,
            None => self.owner.get().copied_context(py)?.unbind(),
        };
        let due = self
            .done
            .lock()
            .add(Waiter::Callback(DoneCallback { callback, context }));
        if let Some(due) = due {
            self.wake(me, vec![due]);
        }
        Ok(())
    }

    /// Takes every registration of a callback equal to `callback` off and returns how many there
    /// were. The comparison runs Python, so it runs outside the lock; when it raises, nothing is
    /// taken off.
    pub(super) fn remove_done_callback(&self, callback: &Bound<'_, PyAny>) -> PyResult<usize> {
        let py = callback.py();
        let registered = self.done.lock().callbacks(py);
        let mut matched = Vec::new();
        for candidate in registered {
            if candidate.bind(py).eq(callback)? {
                matched.push(candidate);
            }
        }
        if matched.is_empty() {
            return Ok(0);
        }
        let removed = self.done.lock().remove(&matched);
        Ok(removed.len())
    }

    pub(super) fn blocking(&self) -> bool {
        self.done.lock().blocking
    }

    pub(super) fn set_blocking(&self, blocking: bool) {
        self.done.lock().blocking = blocking;
    }

    /// Returns whether it was yielded by `await`, and clears that, as asyncio's Task does before
    /// it waits on it.
    pub(super) fn take_blocking(&self) -> bool {
        std::mem::replace(&mut self.done.lock().blocking, false)
    }

    pub(super) fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.end_copy(py) {
            None => Err(self.owner.get().invalid_state(py, "Result is not set.".to_owned())),
            Some(end) => end.outcome(py, self.owner.get()),
        }
    }

    /// Returns `None` for a value and the exception for a raise; a cancelled end raises as
    /// `result()` does.
    pub(super) fn exception(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match self.end_copy(py) {
            None => Err(self.owner.get().invalid_state(py, "Exception is not set.".to_owned())),
            Some(FutureEnd::Raised(raised)) => Ok(Some(raised.into_any())),
            Some(end) => end.outcome(py, self.owner.get()).map(|_| None),
        }
    }

    /// Returns the `CancelledError` a cancelled end raises, or, before the end, one carrying
    /// `message`.
    pub(super) fn make_cancelled_error(&self, py: Python<'_>, message: Option<&Py<PyAny>>) -> Py<PyAny> {
        let raised = match self.end_copy(py) {
            Some(end @ FutureEnd::Cancelled { .. }) => end.outcome(py, self.owner.get()).err(),
            _ => None,
        };
        raised
            .unwrap_or_else(|| self.owner.get().cancelled_error(py, message))
            .into_value(py)
            .into_any()
    }

    /// Steps `await` on it: while it is pending it marks itself yielded and the caller yields it;
    /// once it ended this raises `StopIteration` with its result, or what it raised.
    pub(super) fn next(&self, py: Python<'_>) -> PyResult<()> {
        {
            let mut done = self.done.lock();
            if done.end.is_none() {
                done.blocking = true;
                return Ok(());
            }
        }
        let value = self.result(py)?;
        Err(PyStopIteration::new_err((value,)))
    }

    /// Refuses a resolution from a thread other than the loop's.
    pub(super) fn check_thread(&self) -> PyResult<()> {
        self.owner.get().check_thread()
    }
}

/// Implements asyncio's `Future` on a TPC thread's loop (`mipyre.engine._Future`).
#[pyclass(module = "mipyre.engine", name = "_Future", frozen, weakref)]
pub(crate) struct Future {
    core: FutureCore,
}

impl Future {
    pub(super) fn new(owner: Py<EventLoop>) -> Self {
        Future {
            core: FutureCore::new(owner),
        }
    }

    pub(super) fn core(&self) -> &FutureCore {
        &self.core
    }

    /// Resolves the Future with `value` unless it ended meanwhile, as
    /// `futures._set_result_unless_cancelled` does.
    pub(super) fn resolve(slf: &Bound<'_, Self>, value: Py<PyAny>) {
        slf.get().core.end(slf.as_any(), FutureEnd::Value(value));
    }

    /// Ends a Future just made with `raised`, which a Task throws into its coroutine next.
    pub(super) fn fail(slf: &Bound<'_, Self>, raised: Py<PyBaseException>) {
        slf.get().core.end(slf.as_any(), FutureEnd::Raised(raised));
    }

    /// Cancels the Future with `message` and returns whether it was still pending.
    pub(super) fn cancel_now(slf: &Bound<'_, Self>, message: Option<Py<PyAny>>) -> bool {
        slf.get()
            .core
            .end(slf.as_any(), FutureEnd::Cancelled { message, raised: None })
    }

    /// Ends the Future with `end` when it is pending, and raises asyncio's `InvalidStateError`
    /// when it is not.
    fn set(slf: &Bound<'_, Self>, end: FutureEnd) -> PyResult<()> {
        let core = &slf.get().core;
        core.check_thread()?;
        if core.end(slf.as_any(), end) {
            return Ok(());
        }
        let word = core.state_word();
        Err(core
            .owner
            .get()
            .invalid_state(slf.py(), format!("invalid state: {word}")))
    }
}

#[pymethods]
impl Future {
    #[getter]
    fn _asyncio_future_blocking(&self) -> bool {
        self.core.blocking()
    }

    #[setter(_asyncio_future_blocking)]
    fn set_blocking(&self, blocking: bool) {
        self.core.set_blocking(blocking);
    }

    fn get_loop(&self, py: Python<'_>) -> Py<EventLoop> {
        self.core.owner.clone_ref(py)
    }

    fn done(&self) -> bool {
        !self.core.is_pending()
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
        Ok(Self::cancel_now(slf, msg))
    }

    fn set_result(slf: &Bound<'_, Self>, result: Py<PyAny>) -> PyResult<()> {
        Self::set(slf, FutureEnd::Value(result))
    }

    fn set_exception(slf: &Bound<'_, Self>, exception: Bound<'_, PyAny>) -> PyResult<()> {
        let exception = match exception.cast::<PyType>() {
            Ok(class) => class.call0()?,
            Err(_) => exception,
        };
        let Ok(exception) = exception.cast_into::<PyBaseException>() else {
            return Err(PyTypeError::new_err("invalid exception object"));
        };
        if exception.is_instance_of::<PyStopIteration>() {
            return Err(PyTypeError::new_err(
                "StopIteration interacts badly with generators and cannot be raised into a Future",
            ));
        }
        Self::set(slf, FutureEnd::Raised(exception.unbind()))
    }

    fn _make_cancelled_error(&self, py: Python<'_>) -> Py<PyAny> {
        self.core.make_cancelled_error(py, None)
    }

    fn __await__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    fn __iter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    /// Yields the Future to the Task awaiting it while it is pending, and returns its result once
    /// it ended.
    fn __next__<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, Self>> {
        slf.get().core.next(slf.py()).map(|()| slf.clone())
    }

    fn __repr__(&self) -> String {
        format!("<_Future {}>", self.core.state_word().to_lowercase())
    }

    #[classmethod]
    fn __class_getitem__(cls: &Bound<'_, PyType>, _item: Bound<'_, PyAny>) -> Py<PyType> {
        cls.clone().unbind()
    }
}
