//! Gives each unit of work its own `contextvars.Context` on the thread state.
//!
//! A thread state that serves unit after unit keeps its current context across them, so without
//! this a `ContextVar` one unit sets would be seen by the next. Each state of a context is its
//! own type and each step consumes the one it leaves, so an owner never enters a context twice:
//!
//! ```no_run
//! # use mipyre_subinterp::context::SpareContext;
//! # fn unit(py: pyo3::Python<'_>, spare: &mut SpareContext) -> pyo3::PyResult<()> {
//! let entry = spare.take(py)?.enter();
//! let suspension = entry.suspend().map_err(|failure| failure.error)?;
//! let entry = suspension.enter_again()?;
//! entry.exit_keeping(spare)
//! # }
//! ```
//!
//! A context is entered once:
//!
//! ```compile_fail
//! # use mipyre_subinterp::context::PyContext;
//! # fn unit(py: pyo3::Python<'_>) -> pyo3::PyResult<()> {
//! let ctx = PyContext::new(py)?;
//! let entry = ctx.enter();
//! let again = ctx.enter();
//! # Ok(())
//! # }
//! ```
//!
//! A suspended entry can't be left (or used) until it is entered again:
//!
//! ```compile_fail
//! # use mipyre_subinterp::context::PyContext;
//! # fn unit(py: pyo3::Python<'_>) -> pyo3::PyResult<()> {
//! let entry = PyContext::new(py)?.enter();
//! let suspension = entry.suspend().map_err(|failure| failure.error)?;
//! entry.exit()?;
//! # Ok(())
//! # }
//! ```

use std::mem::ManuallyDrop;

use pyo3::ffi;
use pyo3::prelude::*;

/// A `contextvars.Context` that no one has entered and no one else holds.
pub struct PyContext<'py>(Bound<'py, PyAny>);

/// A context entered on this thread state; dropping it on a panic leaves the context.
pub struct ContextEntry<'py>(Bound<'py, PyAny>);

/// A context left for a while so that something else, such as a task, can enter it.
///
/// The thread state is back in its previous context, so dropping a suspension undoes nothing.
pub struct ContextSuspension<'py>(Bound<'py, PyAny>);

/// Reports that Python code entered another context and did not exit it; `entry` is still
/// entered.
pub struct SuspendError<'py> {
    pub entry: ContextEntry<'py>,
    pub error: PyErr,
}

/// The context the last unit left reusable, if any.
///
/// Reuse is invisible to Python: a context is kept only when it is empty and this is its sole
/// reference, and anything that could still write into it (a scheduled task, a `Token`) holds a
/// reference. The caller must release or drop it with its interpreter's thread state current.
#[derive(Default)]
pub struct SpareContext(Option<Py<PyAny>>);

impl<'py> PyContext<'py> {
    /// Creates an empty context, which costs one object because CPython shares the empty map.
    ///
    /// # Errors
    ///
    /// Returns the Python error when CPython cannot allocate the context.
    #[inline]
    pub fn new(py: Python<'py>) -> PyResult<Self> {
        // SAFETY: `py` proves the thread is attached, and `PyContext_New` returns a new reference
        // or NULL with an exception set.
        unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyContext_New()) }.map(PyContext)
    }

    /// Enters this context on this thread state.
    #[inline(always)]
    pub fn enter(self) -> ContextEntry<'py> {
        // SAFETY: The `Bound` proves the thread is attached, and it holds a context object.
        let status = unsafe { ffi::PyContext_Enter(self.0.as_ptr()) };
        // `PyContext_Enter` fails only for a context already entered, and a `PyContext` is
        // either new or a spare whose sole reference was the spare's.
        assert!(
            status == 0,
            "PyContext_Enter refused a contextvars.Context that no one has entered"
        );
        ContextEntry(self.0)
    }
}

impl SpareContext {
    /// Returns the kept context, or a new one when none is kept.
    ///
    /// # Errors
    ///
    /// Fails as [`PyContext::new`] does.
    #[inline(always)]
    pub fn take<'py>(&mut self, py: Python<'py>) -> PyResult<PyContext<'py>> {
        match self.0.take() {
            Some(ctx) => Ok(PyContext(ctx.into_bound(py))),
            None => PyContext::new(py),
        }
    }

    /// Drops the kept context; `py` must be attached to its interpreter.
    pub fn release(self, py: Python<'_>) {
        if let Some(ctx) = self.0 {
            ctx.drop_ref(py);
        }
    }
}

impl<'py> ContextEntry<'py> {
    #[inline]
    pub fn py(&self) -> Python<'py> {
        self.0.py()
    }

    #[inline]
    pub fn context(&self) -> &Bound<'py, PyAny> {
        &self.0
    }

    /// Leaves this context until [`ContextSuspension::enter_again`].
    ///
    /// # Errors
    ///
    /// Hands the entry back when Python code entered another context and did not exit it.
    #[inline]
    pub fn suspend(self) -> Result<ContextSuspension<'py>, SuspendError<'py>> {
        match exit(&self.0) {
            Ok(()) => Ok(ContextSuspension(self.into_context())),
            Err(error) => Err(SuspendError { entry: self, error }),
        }
    }

    /// Exits the context.
    ///
    /// # Errors
    ///
    /// Fails when Python code entered another context and did not exit it.
    #[inline]
    pub fn exit(self) -> PyResult<()> {
        exit(&self.into_context())
    }

    /// Exits the context and keeps it in `spare` if it is empty and nothing else holds it.
    ///
    /// # Errors
    ///
    /// Fails as [`ContextEntry::exit`] does.
    #[inline(always)]
    pub fn exit_keeping(self, spare: &mut SpareContext) -> PyResult<()> {
        let ctx = self.into_context();
        exit(&ctx)?;
        // SAFETY: The `Bound` proves the thread is attached, and it holds a live context.
        let reusable = unsafe { ffi::Py_REFCNT(ctx.as_ptr()) == 1 && ffi::PyObject_Length(ctx.as_ptr()) == 0 };
        if reusable {
            spare.0 = Some(ctx.unbind());
        }
        Ok(())
    }

    /// Takes the context out without running the drop, which is only for the panic path.
    #[inline(always)]
    fn into_context(self) -> Bound<'py, PyAny> {
        let this = ManuallyDrop::new(self);
        // SAFETY: `this` is never used or dropped again, so the `Bound` is moved out once.
        unsafe { std::ptr::read(&this.0) }
    }
}

impl<'py> ContextSuspension<'py> {
    #[inline]
    pub fn context(&self) -> &Bound<'py, PyAny> {
        &self.0
    }

    /// Enters the context again.
    ///
    /// # Errors
    ///
    /// Fails if something else entered it meanwhile and has not exited, such as code that got it
    /// through `Task.get_context()`; the suspension is spent and the thread state stays in its
    /// previous context.
    #[inline]
    pub fn enter_again(self) -> PyResult<ContextEntry<'py>> {
        // SAFETY: The `Bound` proves the thread is attached, and it holds a context object.
        if unsafe { ffi::PyContext_Enter(self.0.as_ptr()) } != 0 {
            return Err(PyErr::fetch(self.0.py()));
        }
        Ok(ContextEntry(self.0))
    }
}

/// Exits `ctx`, which fails only if Python code entered another context and never exited it.
#[inline(always)]
fn exit(ctx: &Bound<'_, PyAny>) -> PyResult<()> {
    // SAFETY: The `Bound` proves the thread is attached, and the context was entered on this
    // thread state.
    if unsafe { ffi::PyContext_Exit(ctx.as_ptr()) } != 0 {
        return Err(PyErr::fetch(ctx.py()));
    }
    Ok(())
}

impl Drop for ContextEntry<'_> {
    /// Runs only on a panic, since every step that ends an entry skips it, and logs the error
    /// because no caller can take it.
    fn drop(&mut self) {
        if let Err(err) = exit(&self.0) {
            tracing::error!(
                error = %err,
                "leaving a unit's contextvars.Context failed: code in it entered a context it \
                 did not exit"
            );
        }
    }
}

/// How an object is awaited, read from its type's C slots in two compares without a Python call.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AwaitSlot {
    /// The object's type is exactly the native coroutine type.
    Coroutine,
    /// The type has `am_await`, as a Future or a Cython coroutine does.
    Awaitable,
    Plain,
}

#[inline]
pub fn await_slot(obj: &Bound<'_, PyAny>) -> AwaitSlot {
    if is_coroutine(obj) {
        return AwaitSlot::Coroutine;
    }
    let ptr = obj.as_ptr();
    // SAFETY: `ptr` is borrowed from a live `obj`, its type is never NULL, and `tp_as_async` is
    // NULL or points to the type's async slots.
    let awaitable = unsafe {
        let slots = (*ffi::Py_TYPE(ptr)).tp_as_async;
        !slots.is_null() && (*slots).am_await.is_some()
    };
    if awaitable {
        AwaitSlot::Awaitable
    } else {
        AwaitSlot::Plain
    }
}

/// Returns whether `obj`'s type is exactly the native coroutine type.
#[inline]
pub fn is_coroutine(obj: &Bound<'_, PyAny>) -> bool {
    // SAFETY: `obj` is a live object.
    unsafe { ffi::PyCoro_CheckExact(obj.as_ptr()) == 1 }
}
