//! asyncio's `Handle` and `TimerHandle` for the loop, and the one way a scheduled call runs in its
//! context.
//!
//! A handle does not run itself: the loop runs it on a turn. A timer handle knows its key in the
//! loop's timers only to take itself out when cancelled on the loop's thread.

use mipyre_subinterp::context::ContextSuspension;
use pyo3::call::PyCallArgs;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3::PyClassInitializer;

use super::{caught, chained, EventLoop};
use crate::sync::{AtomicBool, Mutex, Ordering};

/// Holds one call scheduled on the loop, cancellable until it runs (asyncio's `Handle`).
#[pyclass(module = "mipyre.engine", name = "_Handle", frozen, subclass)]
pub(crate) struct Handle {
    callback: Py<PyAny>,
    args: Py<PyTuple>,
    context: Py<PyAny>,
    cancelled: AtomicBool,
}

impl Handle {
    pub(super) fn new(callback: Py<PyAny>, args: Bound<'_, PyTuple>, context: Py<PyAny>) -> Self {
        Handle {
            callback,
            args: args.unbind(),
            context,
            cancelled: AtomicBool::new(false),
        }
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    pub(super) fn run(&self, py: Python<'_>) -> PyResult<()> {
        call_in_context(self.callback.bind(py), self.args.bind(py), self.context.bind(py))
    }
}

#[pymethods]
impl Handle {
    pub(super) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    fn cancelled(&self) -> bool {
        self.is_cancelled()
    }

    fn get_context(&self, py: Python<'_>) -> Py<PyAny> {
        self.context.clone_ref(py)
    }
}

/// Holds a call scheduled on the loop for a time (asyncio's `TimerHandle`).
#[pyclass(module = "mipyre.engine", name = "_TimerHandle", frozen, extends = Handle)]
pub(crate) struct TimerHandle {
    /// It is the loop time the call is due.
    when: f64,
    owner: Py<EventLoop>,
    /// It is the timer's key while the timer is queued, and `None` once it came due or was
    /// taken out.
    pub(super) key: Mutex<Option<super::timers::Key>>,
}

impl TimerHandle {
    pub(super) fn new(py: Python<'_>, call: Handle, when: f64, owner: Py<EventLoop>) -> PyResult<Py<Self>> {
        let timer = TimerHandle {
            when,
            owner,
            key: Mutex::new(None),
        };
        Py::new(py, PyClassInitializer::from(call).add_subclass(timer))
    }

    /// Forgets the timer's key, as its timer came due and left the loop's timers.
    pub(super) fn came_due(&self) {
        self.key.lock().take();
    }
}

#[pymethods]
impl TimerHandle {
    /// Cancels the call; cancelled on its loop's thread it also leaves the loop's timers at once,
    /// and cancelled elsewhere it is skipped when it comes due.
    fn cancel(slf: &Bound<'_, Self>) {
        slf.as_super().get().cancel();
        let owner = slf.get().owner.get();
        if !owner.on_its_thread() {
            return;
        }
        let Some(key) = slf.get().key.lock().take() else { return };
        owner.remove_timer(&key);
    }

    fn when(&self) -> f64 {
        self.when
    }
}

/// Calls `callback` with `args` inside `context`, as `Context.run` does. When the call and leaving
/// the context both fail, the error of leaving is returned with the call's as its cause.
pub(super) fn call_in_context<'py>(
    callback: &Bound<'py, PyAny>,
    args: impl PyCallArgs<'py>,
    context: &Bound<'py, PyAny>,
) -> PyResult<()> {
    let entry = ContextSuspension::new(context.clone()).enter_again()?;
    let called = caught(|| callback.call1(args).map(drop));
    chained(callback.py(), called, entry.exit())
}
