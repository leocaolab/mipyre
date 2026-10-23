//! Creates, serves and ends one isolated sub-interpreter with its own GIL, all on one thread.

use std::cell::Cell;
use std::ffi::CStr;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::Arc;

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::InterpreterHandle;

use crate::entry::{marked, ThreadAtExit, IN_PYTHON, WATCHED};
use crate::finalization::{FinalizationHold, Ownership, FINALIZATION};
use crate::sync::AtomicPtr;
use crate::sync::{AtomicU64, Condvar, Mutex, Ordering};

#[derive(Debug, thiserror::Error)]
pub enum NewInterpreterError {
    /// The runtime refused `Py_NewInterpreterFromConfig` or a step a creation takes before it.
    #[error(
        "Py_NewInterpreterFromConfig failed{}: {}",
        .func.as_deref().map(|f| format!(" in {f}")).unwrap_or_default(),
        .message.as_deref().unwrap_or("it returned no error message and no thread state")
    )]
    Refused {
        func: Option<String>,
        message: Option<String>,
    },
    /// No interpreter is created once finalization begins, because the main interpreter a
    /// creation starts from is going away.
    #[error("the process is finalizing: no sub-interpreter is created once Py_Finalize has begun")]
    Finalizing,
}

impl NewInterpreterError {
    /// Builds a refusal from `PyStatus.func` and `PyStatus.err_msg`, each if CPython gave one.
    pub fn new(func: Option<String>, message: Option<String>) -> Self {
        NewInterpreterError::Refused { func, message }
    }

    fn no_main_thread_state() -> Self {
        NewInterpreterError::Refused {
            func: Some("PyThreadState_New".into()),
            message: Some("could not make a thread state for the main interpreter".into()),
        }
    }

    /// # Safety
    ///
    /// `status.func` and `status.err_msg` must be NULL or NUL-terminated, as CPython guarantees.
    unsafe fn from_status(status: &ffi::PyStatus) -> Self {
        let text =
            |p: *const std::ffi::c_char| (!p.is_null()).then(|| CStr::from_ptr(p).to_string_lossy().into_owned());
        NewInterpreterError::Refused {
            func: text(status.func),
            message: text(status.err_msg),
        }
    }
}

/// The references a worker holds inside its interpreter, which must be released while that
/// interpreter is alive and its thread state is current.
pub trait Releasable: Sized {
    /// Releases every reference it holds; this interpreter's thread state must be current.
    fn release(self, py: Python<'_>);
}

/// A PEP 684 sub-interpreter with its own GIL, which only [`Self::end_on_own_thread`] or
/// [`Self::abandon`] may dispose of.
///
/// It is neither `Send` nor `Sync`, so every method runs on the thread that created it and the
/// thread state they restore is that thread's.
pub struct SubInterpreter<R: Releasable> {
    /// The thread state is saved while nothing runs on it.
    tstate: NonNull<ffi::PyThreadState>,
    label: String,
    /// A worker dropped without `end` leaks these rather than decref them into whatever
    /// interpreter is current there.
    held: ManuallyDrop<R>,
    /// The slot is kept here rather than by the caller so that a unit is only ever begun in the
    /// interpreter its slot names; it is made by the first [`Self::with_unit`].
    units: Option<UnitSlot>,
    /// Is taken only by what ends or gives up the interpreter, which happens once.
    ownership: Option<Ownership>,
}

impl<R: Releasable> SubInterpreter<R> {
    /// Tells finalization this thread no longer enters the interpreter.
    fn give_up_ownership(&mut self) {
        if let Some(owned) = self.ownership.take() {
            FINALIZATION.give_up(owned);
        }
    }
}

impl<R: Releasable> Drop for SubInterpreter<R> {
    fn drop(&mut self) {
        self.give_up_ownership();
        tracing::error!(
            target: "mipyre_subinterp",
            worker = %self.label,
            "sub-interpreter worker dropped without being ended; leaking its interpreter"
        );
    }
}

/// The main-interpreter thread state one creation needs current, deleted when it is done.
///
/// Deleting it needs the main GIL, so a creation takes that GIL twice: once to start, which
/// serializes the process's creations, and once briefly to delete it.
struct TemporaryMainThreadState(NonNull<ffi::PyThreadState>);

impl TemporaryMainThreadState {
    /// Creates the thread state on the calling thread without attaching it.
    ///
    /// # Safety
    ///
    /// No thread state may be current on the calling thread.
    unsafe fn make() -> Result<TemporaryMainThreadState, NewInterpreterError> {
        // SAFETY: The main interpreter lives for the process, and `PyThreadState_New` does not
        // need the GIL.
        let tstate = ffi::PyThreadState_New(ffi::PyInterpreterState_Main());
        NonNull::new(tstate)
            .map(TemporaryMainThreadState)
            .ok_or_else(NewInterpreterError::no_main_thread_state)
    }

    /// Attaches the thread state, taking the main GIL.
    ///
    /// # Safety
    ///
    /// No thread state may be current on the calling thread.
    unsafe fn attach(&self) {
        ffi::PyEval_RestoreThread(self.0.as_ptr());
    }

    /// Clears and deletes the thread state, leaving none current.
    ///
    /// # Safety
    ///
    /// This thread state must be current on the calling thread.
    unsafe fn dispose_attached(self) {
        ffi::PyThreadState_Clear(self.0.as_ptr());
        let detached = ffi::PyEval_SaveThread();
        debug_assert_eq!(detached, self.0.as_ptr());
        ffi::PyThreadState_Delete(self.0.as_ptr());
    }

    /// # Safety
    ///
    /// No thread state may be current on the calling thread.
    unsafe fn dispose(self) {
        self.attach();
        self.dispose_attached();
    }
}

/// Ends the half-built sub-interpreter and disposes of the main thread state when `prepare`
/// panics.
struct EndOnUnwind {
    main: ManuallyDrop<TemporaryMainThreadState>,
    armed: bool,
}

impl Drop for EndOnUnwind {
    fn drop(&mut self) {
        if self.armed {
            // SAFETY: The panic came out of `prepare`, which runs with the sub-interpreter's
            // thread state current. Ending it leaves none current, as `dispose` needs, and `main`
            // is taken once because the guard drops once.
            unsafe {
                ffi::Py_EndInterpreter(ffi::PyThreadState_Get());
                ManuallyDrop::take(&mut self.main).dispose();
            }
        }
    }
}

/// Prints `label` with the time since the first call and the thread name to stderr; only a
/// build with the `startup_trace` feature has it, and that build is the one that measures.
#[cfg(feature = "startup_trace")]
pub fn trace(label: &str) {
    static T0: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let t0 = *T0.get_or_init(std::time::Instant::now);
    eprintln!(
        "TRACE {:>9.3}ms {:<22} {}",
        t0.elapsed().as_secs_f64() * 1e3,
        std::thread::current().name().unwrap_or("?"),
        label
    );
}

impl<R: Releasable> SubInterpreter<R> {
    /// Creates a sub-interpreter on the calling thread, which then serves it, and runs `prepare`
    /// in it to build what the worker holds.
    ///
    /// On return no thread state is current and the main GIL is not held. Creations on different
    /// threads are serialized only around the creation calls; `prepare` runs under the new
    /// interpreter's own GIL.
    ///
    /// # Errors
    ///
    /// Returns `refused`'s error when the runtime refuses or finalization has begun, and
    /// `prepare`'s error after ending the interpreter. A panic in `prepare` also ends it.
    ///
    /// # Aborts
    ///
    /// CPython aborts the process when another thread state is current on the calling thread,
    /// such as inside a `Python::attach`.
    pub fn create_on_this_thread<E>(
        label: String,
        refused: impl FnOnce(NewInterpreterError) -> E,
        prepare: impl for<'py> FnOnce(Python<'py>) -> Result<R, E>,
    ) -> Result<Self, E> {
        let Some(_not_finalizing) = FINALIZATION.not_begun() else {
            return Err(refused(NewInterpreterError::Finalizing));
        };
        detach_for_idle();
        // SAFETY: Finalization cannot begin while `_not_finalizing` is held, and no thread state
        // of ours is current after the detach; anyone else's makes CPython abort.
        unsafe { Self::create_here(label, refused, prepare) }
    }

    /// # Safety
    ///
    /// The main interpreter must not be finalized, nor begin to be, before this returns.
    unsafe fn create_here<E>(
        label: String,
        refused: impl FnOnce(NewInterpreterError) -> E,
        prepare: impl for<'py> FnOnce(Python<'py>) -> Result<R, E>,
    ) -> Result<Self, E> {
        #[cfg(feature = "startup_trace")]
        trace("create_begin");
        let main = match TemporaryMainThreadState::make() {
            Ok(main) => main,
            Err(e) => return Err(refused(e)),
        };
        main.attach();
        #[cfg(feature = "startup_trace")]
        trace("main_attached");
        // SAFETY: The main interpreter's thread state was attached above.
        let armed = with_assumed_attached(|py| {
            FINALIZATION.arm(py).map_err(|e| {
                NewInterpreterError::new(
                    Some("atexit.register".into()),
                    Some(format!(
                        "could not register the finalization hook (b/496): {}: {}",
                        e.get_type(py),
                        e.value(py)
                    )),
                )
            })
        });
        if let Err(error) = armed {
            main.dispose_attached();
            return Err(refused(error));
        }

        let mut new_tstate: *mut ffi::PyThreadState = std::ptr::null_mut();
        let config = ffi::PyInterpreterConfig {
            use_main_obmalloc: 0,
            allow_fork: 0,
            allow_exec: 0,
            allow_threads: 1,
            allow_daemon_threads: 0,
            check_multi_interp_extensions: 1,
            gil: ffi::PyInterpreterConfig_OWN_GIL,
        };

        // On success the main thread state is detached and the new one is current; on failure
        // the main one is attached again.
        let status = ffi::Py_NewInterpreterFromConfig(&mut new_tstate, &config);
        let new_tstate = match NonNull::new(new_tstate) {
            Some(tstate) if ffi::PyStatus_IsError(status) == 0 => tstate,
            _ => {
                let error = NewInterpreterError::from_status(&status);
                main.dispose_attached();
                return Err(refused(error));
            }
        };

        #[cfg(feature = "startup_trace")]
        trace("interp_created");
        let mut unwind_guard = EndOnUnwind {
            main: ManuallyDrop::new(main),
            armed: true,
        };
        let built = with_assumed_attached(prepare);
        unwind_guard.armed = false;
        // SAFETY: The guard is disarmed, so nothing else takes `main` out of it.
        let main = unsafe { ManuallyDrop::take(&mut unwind_guard.main) };
        #[cfg(feature = "startup_trace")]
        trace("prepared");
        let result = match built {
            Ok(held) => {
                let saved = ffi::PyEval_SaveThread();
                debug_assert_eq!(saved, new_tstate.as_ptr());
                debug_assert!(
                    crate::entry::aligned(saved),
                    "a thread state's address has a bit the entry word uses"
                );
                Ok(SubInterpreter {
                    tstate: new_tstate,
                    label,
                    held: ManuallyDrop::new(held),
                    units: None,
                    ownership: Some(FINALIZATION.own()),
                })
            }
            Err(e) => {
                ffi::Py_EndInterpreter(ffi::PyThreadState_Get());
                Err(e)
            }
        };
        main.dispose();
        #[cfg(feature = "startup_trace")]
        trace("main_disposed");
        result
    }

    /// Returns what the worker holds without its GIL, for plain Rust fields only, never a `Py<T>`.
    pub fn held(&self) -> &R {
        &self.held
    }

    /// Returns whether [`Self::with_unit`] has run on this worker.
    pub fn unit_slot_made(&self) -> bool {
        self.units.is_some()
    }

    /// Ends a leaked unit, releases what the worker holds, and ends the interpreter.
    ///
    /// Another interpreter's thread state current on this thread is saved and restored around
    /// it.
    fn end(self, _not_finalizing: &FinalizationHold<'_>) {
        let mut this = ManuallyDrop::new(self);
        // `this` is never dropped (its `Drop` reports a worker that was not ended), so what it
        // owns is moved out to be freed.
        drop(std::mem::take(&mut this.label));
        // Otherwise `previous` could be this worker's kept thread state, and the restore after
        // `Py_EndInterpreter` would bring it back destroyed.
        detach_for_idle();
        // SAFETY: The worker's thread state was made on this thread and is saved, and the
        // interpreter is alive because only this ends it while finalization is held off.
        unsafe {
            let previous = ffi::PyThreadState_GetUnchecked();
            if !previous.is_null() {
                ffi::PyEval_SaveThread();
            }
            ffi::PyEval_RestoreThread(this.tstate.as_ptr());
            let held = ManuallyDrop::take(&mut this.held);
            let units = this.units.take();
            with_assumed_attached(|py| {
                // Otherwise an interruption still aimed at a leaked unit could land in the release
                // below or enter the interpreter after it is gone.
                if let Some(units) = units {
                    units.end_leaked_run(py);
                }
                held.release(py);
            });
            ffi::Py_EndInterpreter(ffi::PyThreadState_Get());
            if !previous.is_null() {
                ffi::PyEval_RestoreThread(previous);
            }
        }
        this.give_up_ownership();
    }

    /// Gives up on this worker without touching Python, for use once finalization has begun.
    ///
    /// `Py_Finalize` then ends the interpreter itself, and restoring its thread state could be a
    /// use-after-free; the OS reclaims the memory at exit.
    pub fn abandon(mut self) {
        self.give_up_ownership();
        std::mem::forget(self);
    }

    /// Ends the worker, or abandons it to `Py_Finalize` once finalization has begun.
    pub fn end_on_own_thread(self) {
        match FINALIZATION.not_begun() {
            Some(ending) => self.end(&ending),
            None => self.abandon(),
        }
    }

    /// Runs `f` with this worker's thread state current.
    ///
    /// After [`keep_attached_until_idle`] the thread state stays current after `f` until
    /// [`detach_for_idle`]; otherwise it is saved again before this returns.
    ///
    /// # Errors
    ///
    /// Returns the panic payload when `f` panics.
    ///
    /// # Aborts
    ///
    /// CPython aborts the process when another thread state is current on the calling thread,
    /// such as inside a `Python::attach` or another worker's `with_gil`.
    pub fn with_gil<T>(&mut self, f: impl for<'py> FnOnce(&mut R, Python<'py>) -> T) -> std::thread::Result<T> {
        let held = &mut *self.held;
        // SAFETY: `self.tstate` was made on this thread, which the worker never leaves, and lives
        // until `end` consumes the worker.
        unsafe { enter(self.tstate, |py| f(held, py)) }
    }

    /// Runs `f` as [`Self::with_gil`] does, with a unit of work another thread may interrupt.
    ///
    /// Only the first unit allocates.
    ///
    /// # Errors
    ///
    /// Returns the panic payload when `f` panics.
    ///
    /// # Aborts
    ///
    /// Aborts as [`Self::with_gil`] does.
    pub fn with_unit<T>(
        &mut self,
        f: impl for<'py> FnOnce(&mut R, WorkUnit<'py>, Python<'py>) -> T,
    ) -> std::thread::Result<T> {
        let held = &mut *self.held;
        let units = &mut self.units;
        // SAFETY: The same holds as in `with_gil`.
        unsafe {
            enter(self.tstate, |py| {
                let unit = units.get_or_insert_with(|| UnitSlot::new(py)).begin(py);
                f(held, unit, py)
            })
        }
    }
}

/// Runs `f` with `tstate` current, as [`SubInterpreter::with_gil`] describes.
///
/// No drop guard is needed for the save, because a panic is caught inside.
///
/// # Safety
///
/// `tstate` must be a live worker's thread state made on the calling thread and current nowhere
/// else, either saved or kept current on this thread.
// TODO(s2a-v1): `Python::attach_current` exists only in the PyO3 fork, so building on upstream
// PyO3 needs the keep-attached path rewritten or moved out.
#[inline]
unsafe fn enter<T>(
    tstate: NonNull<ffi::PyThreadState>,
    f: impl for<'py> FnOnce(Python<'py>) -> T,
) -> std::thread::Result<T> {
    let tstate = tstate.as_ptr();
    // While `f` runs the thread state is in use, not kept, so `detach_for_idle` leaves it alone.
    let mut kept = ENTRY.with(|e| e.swap(IN_PYTHON, Ordering::Relaxed));
    if kept != tstate {
        // Null, or this thread already in a unit: CPython aborts on the restore below then.
        if kept.addr() & !IN_PYTHON.addr() == 0 {
            ffi::PyEval_RestoreThread(tstate);
        } else {
            if kept.addr() & WATCHED != 0 {
                kept = python_entered_while_watched(kept);
            }
            if kept != tstate {
                // Another worker kept on this thread is saved first, though a TPC thread has one.
                if !kept.is_null() {
                    ffi::PyEval_SaveThread();
                }
                ffi::PyEval_RestoreThread(tstate);
            }
        }
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: This worker's thread state is current and stays current for `f`.
        Python::attach_current(f)
    }));
    if KEEP.with(Cell::get) {
        ENTRY.with(|e| e.store(tstate, Ordering::Relaxed));
    } else {
        let saved = ffi::PyEval_SaveThread();
        debug_assert_eq!(saved, tstate);
        ENTRY.with(|e| e.store(std::ptr::null_mut(), Ordering::Relaxed));
    }
    result
}

/// Tells the exit watching this thread that Python runs on it again, and returns the word
/// `kept` without the exit's mark.
#[cold]
#[inline(never)]
fn python_entered_while_watched(kept: *mut ffi::PyThreadState) -> *mut ffi::PyThreadState {
    FINALIZATION.python_entered_while_watched();
    kept.with_addr(kept.addr() & !WATCHED)
}

/// A thread's entry word ([`crate::entry`]), for the exit to mark from another thread.
///
/// [`Finalization::own`](crate::finalization) makes it on the owning thread and keeps it in the
/// owners' list until that thread gives its interpreter up, which the thread does before it ends
/// and its thread-locals are freed: so while one is held its word is alive.
pub(crate) struct ThreadEntry(*const AtomicPtr<ffi::PyThreadState>);

// SAFETY: The word is an atomic, alive while the entry is held (above).
unsafe impl Send for ThreadEntry {}

impl ThreadEntry {
    /// The calling thread's word.
    pub(crate) fn this_thread() -> Self {
        ThreadEntry(ENTRY.with(std::ptr::from_ref))
    }

    /// Marks the word for the exit unless a unit runs on the thread, and says which.
    pub(crate) fn mark(&self) -> ThreadAtExit {
        // SAFETY: The word is alive while the entry is held (the type's invariant).
        let word = unsafe { &*self.0 };
        let mut seen = word.load(Ordering::Relaxed);
        loop {
            let Some(marking) = marked(seen.addr()) else {
                return ThreadAtExit::RunningPython;
            };
            match word.compare_exchange_weak(seen, seen.with_addr(marking), Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return ThreadAtExit::OutsidePython,
                Err(now) => seen = now,
            }
        }
    }
}

thread_local! {
    static KEEP: Cell<bool> = const { Cell::new(false) };
    pub(crate) static ENTRY: AtomicPtr<ffi::PyThreadState> = const { AtomicPtr::new(std::ptr::null_mut()) };
}

/// Keeps this thread's worker thread state current between units of work from now on.
///
/// The thread must call [`detach_for_idle`] before it blocks, so Python threads the app started
/// in that interpreter can run while the server is idle.
pub fn keep_attached_until_idle() {
    KEEP.with(|k| k.set(true));
}

/// Saves the worker thread state this thread kept current, if any, leaving none current.
pub fn detach_for_idle() {
    #[expect(
        clippy::let_underscore_must_use,
        reason = "`try_with` fails only while this thread's thread-locals are being destroyed: the thread is ending, and there is no kept state left to read"
    )]
    let _ = ENTRY.try_with(|entry| {
        let word = entry.load(Ordering::Relaxed);
        // Above `IN_PYTHON` is a thread state, marked or not: null, `WATCHED` alone and
        // `IN_PYTHON` keep none.
        if word.addr() > IN_PYTHON.addr() {
            // SAFETY: `enter` keeps a thread state in the word only while it is current, and only
            // this takes it out; the exit only adds `WATCHED`, which the clear below keeps.
            unsafe { ffi::PyEval_SaveThread() };
            entry.fetch_and(WATCHED, Ordering::Relaxed);
        }
    });
}

/// One unit of work of a worker, which another thread may interrupt through its
/// [`InterruptTarget`].
///
/// It cannot leave [`SubInterpreter::with_unit`]'s closure and is neither `Send` nor `Sync`, so
/// wherever it is ended or dropped the thread is attached to the unit's own interpreter. Ending
/// or dropping it takes back an interruption that was set and not yet raised, and none lands
/// after.
///
/// Leaving the closure does not compile:
///
/// ```compile_fail,E0521
/// # use mipyre_subinterp::{Releasable, SubInterpreter};
/// # use pyo3::Python;
/// # struct Nothing;
/// # impl Releasable for Nothing { fn release(self, _py: Python<'_>) {} }
/// fn escape(worker: &mut SubInterpreter<Nothing>) {
///     let mut kept = None;
///     let _ = worker.with_unit(|_held, unit, _py| kept = Some(unit));
/// }
/// ```
///
/// ```compile_fail
/// # use mipyre_subinterp::{Releasable, SubInterpreter};
/// # use pyo3::Python;
/// # struct Nothing;
/// # impl Releasable for Nothing { fn release(self, _py: Python<'_>) {} }
/// fn escape(worker: &mut SubInterpreter<Nothing>) {
///     let unit = worker.with_unit(|_held, unit, _py| unit);
/// }
/// ```
///
/// Nor does taking it into another interpreter from a `detach`:
///
/// ```compile_fail,E0277
/// # use mipyre_subinterp::{Releasable, SubInterpreter};
/// # use pyo3::sync::InterpreterHandle;
/// # use pyo3::Python;
/// # struct Nothing;
/// # impl Releasable for Nothing { fn release(self, _py: Python<'_>) {} }
/// fn into_main(worker: &mut SubInterpreter<Nothing>) {
///     let _ = worker.with_unit(|_held, unit, py| {
///         py.detach(move || InterpreterHandle::main().attach(move |_main| unit.end()))
///     });
/// }
/// ```
///
/// Ending it in its own attach compiles:
///
/// ```no_run
/// # use mipyre_subinterp::{Releasable, SubInterpreter};
/// # use pyo3::Python;
/// # struct Nothing;
/// # impl Releasable for Nothing { fn release(self, _py: Python<'_>) {} }
/// fn served(worker: &mut SubInterpreter<Nothing>) -> bool {
///     let ran = worker.with_unit(|_held, unit, py| {
///         let target = unit.target();
///         py.detach(|| ());
///         unit.end();
///         target.interrupted()
///     });
///     ran.is_ok_and(|interrupted| !interrupted)
/// }
/// ```
#[must_use = "a unit of work ends when it is ended or dropped"]
pub struct WorkUnit<'py> {
    state: Arc<UnitState>,
    run: RunId,
    py: Python<'py>,
}

// This compiles only while `WorkUnit`, `UnitSlot` and `SubInterpreter` are neither `Send` nor
// `Sync`: for a `Send` type both `NotSend` impls apply and the path is ambiguous.
const _: () = {
    trait NotSend<Which> {
        const CHECK: () = ();
    }
    impl<T: ?Sized> NotSend<()> for T {}
    struct IsSend;
    impl<T: ?Sized + Send> NotSend<IsSend> for T {}
    trait NotSync<Which> {
        const CHECK: () = ();
    }
    impl<T: ?Sized> NotSync<()> for T {}
    struct IsSync;
    impl<T: ?Sized + Sync> NotSync<IsSync> for T {}
    let () = <WorkUnit<'static> as NotSend<_>>::CHECK;
    let () = <WorkUnit<'static> as NotSync<_>>::CHECK;
    let () = <UnitSlot as NotSend<_>>::CHECK;
    let () = <UnitSlot as NotSync<_>>::CHECK;
    struct Nothing;
    impl Releasable for Nothing {
        fn release(self, _py: Python<'_>) {}
    }
    let () = <SubInterpreter<Nothing> as NotSend<_>>::CHECK;
    <SubInterpreter<Nothing> as NotSync<_>>::CHECK
};

/// A handle another thread uses to interrupt one run of a [`WorkUnit`] and read its state.
#[derive(Clone, Debug)]
pub struct InterruptTarget {
    state: Arc<UnitState>,
    run: RunId,
}

/// The state of one worker's slot, whose `host` is fixed while the worker is served.
#[derive(Debug)]
struct UnitState<H = SlotThread> {
    host: H,
    /// Packs the run in the high bits, the interruptions in flight in the middle and the
    /// [`UnitPhase`] in the low bits, so every change is one atomic update.
    ///
    /// Every write is a read-modify-write on this one word, so all threads agree on a single
    /// order without `SeqCst`. If an interruption counts in first, the end waits for it; if the
    /// end or the next begin comes first, the interruption sees that and leaves without entering
    /// the interpreter. `AcqRel` and `Acquire` carry the writes made under the GIL.
    state: mipyre_cpu::CacheLine<AtomicU64>,
    /// A waiter checks the count under this lock before it sleeps, and the last visit to leave
    /// takes it before notifying, so no wake-up is lost.
    idle_lock: Mutex<()>,
    idle: Condvar,
}

#[derive(Debug)]
struct SlotThread {
    interp: InterpreterHandle,
    /// The `PyThread_get_thread_ident()` value that `PyThreadState_SetAsyncExc` takes.
    thread: std::ffi::c_ulong,
}

/// The interpreter operations the slot protocol needs, abstracted so the tests and the Loom model
/// run the protocol code itself rather than a copy of it.
trait UnitHost {
    /// The value proves that the calling thread holds the unit's GIL.
    type Gil<'py>: Copy;
    type Exception: Copy;

    /// Runs `f` holding the unit's GIL from another thread, or returns `None` without running it
    /// when no thread state could be made.
    fn visit<R>(&self, f: impl for<'py> FnOnce(Self::Gil<'py>) -> R) -> Option<R>;

    /// Sets `exc` pending on the unit's thread, or clears it on `None`, and returns whether the
    /// thread was found; nothing changes when it was not.
    fn set_raise(&self, gil: Self::Gil<'_>, exc: Option<Self::Exception>) -> bool;

    /// Runs `f` with the GIL released and takes it back after.
    fn without_gil(&self, gil: Self::Gil<'_>, f: impl FnOnce());

    /// Builds the [`InterruptError::UnitThreadGone`] that says where the unit is.
    fn thread_gone(&self, gil: Self::Gil<'_>) -> InterruptError;
}

impl UnitHost for SlotThread {
    type Gil<'py> = Python<'py>;
    type Exception = *mut ffi::PyObject;

    fn visit<R>(&self, f: impl for<'py> FnOnce(Python<'py>) -> R) -> Option<R> {
        self.interp.try_attach(f)
    }

    fn set_raise(&self, _attached: Python<'_>, exc: Option<*mut ffi::PyObject>) -> bool {
        // SAFETY: The token proves a thread state is current, NULL clears, and the only exception
        // set is the static `KeyboardInterrupt` type.
        let reached = unsafe {
            ffi::PyThreadState_SetAsyncExc(self.thread as std::ffi::c_long, exc.unwrap_or(std::ptr::null_mut()))
        };
        reached != 0
    }

    fn without_gil(&self, _attached: Python<'_>, f: impl FnOnce()) {
        // SAFETY: The token proves a thread state is current, and it is restored below.
        let saved = unsafe { ffi::PyEval_SaveThread() };
        f();
        // SAFETY: `saved` is this thread's, saved above.
        unsafe { ffi::PyEval_RestoreThread(saved) };
    }

    fn thread_gone(&self, _attached: Python<'_>) -> InterruptError {
        InterruptError::UnitThreadGone {
            thread: self.thread,
            interp: self.interp.id(),
        }
    }
}

/// The count of runs begun on a slot, which an interruption must match to land.
///
/// It wraps after `2^RUN_BITS` runs, which is safe because the watchdog holds a target for at
/// most one request, far short of `2^53`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RunId(u64);

const RUN_BITS: u32 = 64 - PHASE_BITS - COUNT_BITS;
const RUN_MASK: u64 = (1 << RUN_BITS) - 1;

impl RunId {
    fn next(self) -> RunId {
        RunId((self.0 + 1) & RUN_MASK)
    }
}

const PHASE_BITS: u32 = 3;
const PHASE_MASK: u64 = (1 << PHASE_BITS) - 1;
const COUNT_BITS: u32 = 8;
const COUNT_MASK: u64 = (1 << COUNT_BITS) - 1;
const VISIT: u64 = 1 << PHASE_BITS;
const RUN_SHIFT: u32 = PHASE_BITS + COUNT_BITS;

const _: () = assert!(UnitPhase::ALL.len() as u64 <= PHASE_MASK + 1);

fn run_of(word: u64) -> RunId {
    RunId(word >> RUN_SHIFT)
}

fn in_flight_of(word: u64) -> u64 {
    (word >> PHASE_BITS) & COUNT_MASK
}

/// Counts one interruption in flight until dropped; the last drop wakes a waiting end.
struct VisitPass<'a, H: UnitHost>(&'a UnitState<H>);

impl<H: UnitHost> Drop for VisitPass<'_, H> {
    fn drop(&mut self) {
        let before = self.0.state.fetch_sub(VISIT, Ordering::AcqRel);
        if in_flight_of(before) == 1 {
            self.0.wake_waiters();
        }
    }
}

/// Says whether a unit is running or ended and whether it was interrupted, with no value for an
/// outstanding interruption on a unit never interrupted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum UnitPhase {
    Running = 0,
    /// The interruption set on the thread may or may not have been raised since.
    RunningInterruptOutstanding = 1,
    RunningInterruptTakenBack = 2,
    Ended = 3,
    EndedInterrupted = 4,
}

impl UnitPhase {
    /// Every phase is indexed by its discriminant, which a const assertion checks.
    const ALL: [UnitPhase; 5] = [
        UnitPhase::Running,
        UnitPhase::RunningInterruptOutstanding,
        UnitPhase::RunningInterruptTakenBack,
        UnitPhase::Ended,
        UnitPhase::EndedInterrupted,
    ];

    fn from_word(word: u64) -> UnitPhase {
        UnitPhase::ALL[(word & PHASE_MASK) as usize]
    }

    fn has_ended(self) -> bool {
        matches!(self, UnitPhase::Ended | UnitPhase::EndedInterrupted)
    }

    fn was_interrupted(self) -> bool {
        !matches!(self, UnitPhase::Running | UnitPhase::Ended)
    }

    /// Only a unit ended without the GIL while the interruption was being set is ended here.
    fn interrupt(self) -> UnitPhase {
        if self.has_ended() {
            UnitPhase::EndedInterrupted
        } else {
            UnitPhase::RunningInterruptOutstanding
        }
    }

    /// Returns `None` when no interruption is outstanding.
    fn take_back(self) -> Option<UnitPhase> {
        (self == UnitPhase::RunningInterruptOutstanding).then_some(UnitPhase::RunningInterruptTakenBack)
    }

    /// Returns whether an exception set on the unit's thread may still be there, including one set
    /// on a unit that had ended without the GIL.
    fn exception_may_be_pending(self) -> bool {
        matches!(
            self,
            UnitPhase::RunningInterruptOutstanding | UnitPhase::EndedInterrupted
        )
    }

    fn end(self) -> UnitPhase {
        if self.was_interrupted() {
            UnitPhase::EndedInterrupted
        } else {
            UnitPhase::Ended
        }
    }
}

const _: () = {
    let mut i = 0;
    while i < UnitPhase::ALL.len() {
        assert!(
            UnitPhase::ALL[i] as usize == i,
            "UnitPhase::ALL is not in discriminant order"
        );
        i += 1;
    }
};

/// One worker's reusable unit, made once on the serving thread and begun for each request without
/// allocating.
#[derive(Debug)]
struct UnitSlot(
    Arc<UnitState>,
    // The take-back relies on every run being begun on the thread the slot was made on.
    PhantomData<*const ()>,
);

impl UnitSlot {
    fn new(py: Python<'_>) -> UnitSlot {
        UnitSlot(
            Arc::new(UnitState::new(SlotThread {
                interp: InterpreterHandle::current(py),
                thread: this_thread(),
            })),
            PhantomData,
        )
    }

    /// Begins the next run; the previous run must have ended, so any count kept is stale visits
    /// that will see another run and leave.
    #[inline]
    fn begin<'py>(&self, py: Python<'py>) -> WorkUnit<'py> {
        let (run, before) = self.0.begin_run();
        debug_assert!(
            UnitPhase::from_word(before).has_ended(),
            "a slot began a run while its previous run had not ended"
        );
        WorkUnit {
            state: Arc::clone(&self.0),
            run,
            py,
        }
    }

    /// Ends the last run as a drop would, if a leaked [`WorkUnit`] left it unended.
    fn end_leaked_run(self, py: Python<'_>) {
        if !self.0.phase().has_ended() {
            self.0.end_attached(py);
        }
    }
}

impl std::fmt::Debug for WorkUnit<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkUnit")
            .field("state", &self.state)
            .field("run", &self.run)
            .finish_non_exhaustive()
    }
}

impl WorkUnit<'_> {
    pub fn target(&self) -> InterruptTarget {
        InterruptTarget {
            state: Arc::clone(&self.state),
            run: self.run,
        }
    }

    /// Returns whether the unit was interrupted since it last asked, clearing the unraised
    /// exception.
    ///
    /// A worker waiting outside bytecode uses this to raise its own exception, since the pending
    /// one would land only after the wait.
    pub fn take_interruption(&self) -> bool {
        self.state.take_interruption(self.py)
    }

    /// Ends this unit and takes back an unraised interruption, so it cannot land in whatever the
    /// thread runs next.
    #[inline]
    pub fn end(self) {
        let unit = std::mem::ManuallyDrop::new(self);
        // SAFETY: `unit` is never used or dropped again, so the `Arc` is moved out once, and the
        // other fields are `Copy`.
        let state = unsafe { std::ptr::read(&unit.state) };
        state.end_attached(unit.py);
    }
}

impl Drop for WorkUnit<'_> {
    fn drop(&mut self) {
        let unit = &*self.state;
        let py = self.py;
        // Only the worker ends its unit, so an unended phase stays unended while this runs.
        let before = unit.end_phase();
        // Not finding this thread breaks an invariant that `end` panics on, but a drop may run
        // during an unwind, where a second panic aborts, so this logs it instead.
        if before.exception_may_be_pending() && !unit.clear_raise(py) {
            let host = &unit.host;
            tracing::error!(
                thread = host.thread,
                interp = ?host.interp,
                attached_interp = InterpreterHandle::current(py).id(),
                "a unit of work dropped on its own thread in its interpreter, where CPython does \
                 not find that thread: an exception set on it could not be cleared"
            );
            debug_assert!(
                std::thread::panicking(),
                "a unit of work of thread {} in interpreter {:?} was dropped there, but CPython \
                 does not find that thread in it",
                host.thread,
                host.interp,
            );
        }
        unit.wait_for_visits(py);
    }
}

impl InterruptTarget {
    /// Raises `KeyboardInterrupt` at the unit's next bytecode boundary if it is still running,
    /// and marks it [interrupted][Self::interrupted] in the same GIL.
    ///
    /// `PyThreadState_SetAsyncExc` only finds threads of the current interpreter, so this enters
    /// the unit's interpreter and waits for its GIL; a worker inside one long C call keeps it
    /// waiting, as that call would ignore a signal. The visit is counted before it enters, and
    /// both the run's end and finalization wait for it, so the interpreter stays alive.
    ///
    /// A caller detached from an interpreter of its own briefly takes that interpreter's GIL on
    /// the way out, so it must not hold a lock that a thread of that interpreter may wait on
    /// with that GIL held.
    ///
    /// # Errors
    ///
    /// Returns an [`InterruptError`] when no thread state could be made, when a leaked unit's
    /// thread state is gone, or once finalization has begun; nothing was set in any case.
    ///
    /// # Panics
    ///
    /// Panics if the calling thread is attached, not merely detached, to another interpreter.
    pub fn interrupt(&self) -> Result<(), InterruptError> {
        // The hold is taken before the visit is counted, so a beginning finalization never waits
        // on a visit that waits on this hold.
        let Some(_not_finalizing) = FINALIZATION.not_begun() else {
            return Err(InterruptError::Finalizing);
        };
        // SAFETY: This reads the address of a built-in type object, static for the process.
        let exc = unsafe { ffi::PyExc_KeyboardInterrupt };
        self.state.interrupt(self.run, exc)
    }

    /// Returns whether the targeted run has not ended, which is false once the slot began another.
    pub fn is_running(&self) -> bool {
        self.state
            .phase_of_run(self.run)
            .is_some_and(|phase| !phase.has_ended())
    }

    /// Returns whether an interruption was set during the targeted run.
    ///
    /// It stays true after the worker takes the interruption back, and becomes false once the
    /// slot begins another run, since earlier runs' state is not kept.
    pub fn interrupted(&self) -> bool {
        self.state
            .phase_of_run(self.run)
            .is_some_and(UnitPhase::was_interrupted)
    }
}

impl<H: UnitHost> UnitState<H> {
    fn new(host: H) -> Self {
        UnitState {
            host,
            state: mipyre_cpu::CacheLine(AtomicU64::new(UnitPhase::Ended as u64)),
            idle_lock: Mutex::new(()),
            idle: Condvar::new(),
        }
    }

    /// Ends the run, takes back an unraised interruption, and waits for visits in flight to leave.
    fn end_attached(&self, gil: H::Gil<'_>) {
        // `interrupt` sets under the same GIL, so nothing is set between the phase step and the
        // take-back.
        let before = self.end_phase();
        if before.exception_may_be_pending() {
            self.take_back_raise(gil);
        }
        // Visits in flight need this GIL, and the interpreter may end right after this returns.
        self.wait_for_visits(gil);
    }

    /// Takes back an interruption set since the last call and returns whether there was one.
    fn take_interruption(&self, gil: H::Gil<'_>) -> bool {
        // `interrupt` sets under the same GIL, so nothing is set between the step and the clear.
        if !self.step(UnitPhase::take_back) {
            return false;
        }
        self.take_back_raise(gil);
        true
    }

    /// Interrupts `run` with `exc` from another thread, as [`InterruptTarget::interrupt`] does.
    fn interrupt(&self, run: RunId, exc: H::Exception) -> Result<(), InterruptError> {
        // Counting in before entering keeps the run's end, and so the interpreter, waiting.
        let Some(_pass) = self.enter_visit(run) else {
            return Ok(());
        };
        self.host
            .visit(|gil| {
                // A counted visit fixes the run, so only its end can have happened.
                if self.phase().has_ended() {
                    return Ok(());
                }
                if !self.host.set_raise(gil, Some(exc)) {
                    return Err(self.host.thread_gone(gil));
                }
                self.step(|phase| Some(phase.interrupt()));
                Ok(())
            })
            .ok_or(InterruptError::NoThreadState(ThreadStateCreationFailure))?
    }

    /// Waits with the GIL released until no interruption is in flight, without locking if none is.
    fn wait_for_visits(&self, gil: H::Gil<'_>) {
        if self.in_flight() == 0 {
            return;
        }
        self.host.without_gil(gil, || self.wait_until_idle());
    }

    fn phase(&self) -> UnitPhase {
        UnitPhase::from_word(self.state.load(Ordering::Acquire))
    }

    /// Returns the phase of `run`, or `None` if it is no longer the current run.
    fn phase_of_run(&self, run: RunId) -> Option<UnitPhase> {
        let word = self.state.load(Ordering::Acquire);
        (run_of(word) == run).then(|| UnitPhase::from_word(word))
    }

    fn in_flight(&self) -> u64 {
        in_flight_of(self.state.load(Ordering::Acquire))
    }

    /// Begins the next run, keeping the count, and returns it with the word before.
    fn begin_run(&self) -> (RunId, u64) {
        let before = match self.state.fetch_update(Ordering::AcqRel, Ordering::Acquire, |word| {
            let next = run_of(word).next().0;
            Some((next << RUN_SHIFT) | (word & (COUNT_MASK << PHASE_BITS)) | UnitPhase::Running as u64)
        }) {
            Ok(before) | Err(before) => before,
        };
        (run_of(before).next(), before)
    }

    /// Ends the run, keeping the run and the count, and returns the phase before.
    fn end_phase(&self) -> UnitPhase {
        let before = match self.state.fetch_update(Ordering::AcqRel, Ordering::Acquire, |word| {
            Some((word & !PHASE_MASK) | UnitPhase::from_word(word).end() as u64)
        }) {
            Ok(before) | Err(before) => before,
        };
        UnitPhase::from_word(before)
    }

    /// Counts an interruption in on `run`, which keeps the interpreter alive until the pass drops.
    ///
    /// Returns `None` when the run has ended or is not current, or when the count is full;
    /// one interruption per run is sent, so a full count is refused rather than wrapped.
    fn enter_visit(&self, run: RunId) -> Option<VisitPass<'_, H>> {
        // The run and the phase are read from the same update that counts in.
        let before = self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |word| {
                (in_flight_of(word) < COUNT_MASK).then_some(word + VISIT)
            })
            .ok()?;
        let pass = VisitPass(self);
        if run_of(before) != run || UnitPhase::from_word(before).has_ended() {
            return None;
        }
        Some(pass)
    }

    /// Waits until no interruption is in flight; the caller must not hold the unit's GIL, which a
    /// visit needs to finish.
    fn wait_until_idle(&self) {
        let mut guard = self.idle_lock.lock();
        while self.in_flight() > 0 {
            self.idle.wait(&mut guard);
        }
    }

    /// Notifies under the lock, so a waiter that checked the count and has not slept yet is woken.
    fn wake_waiters(&self) {
        let _guard = self.idle_lock.lock();
        self.idle.notify_all();
    }

    /// Applies `step` to the phase atomically and returns `false`, unchanged, when it gives none.
    fn step(&self, step: impl Fn(UnitPhase) -> Option<UnitPhase>) -> bool {
        self.state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |word| {
                let next = step(UnitPhase::from_word(word))?;
                Some((word & !PHASE_MASK) | next as u64)
            })
            .is_ok()
    }

    /// Clears an unraised exception and returns whether the unit's thread was found.
    #[must_use]
    fn clear_raise(&self, gil: H::Gil<'_>) -> bool {
        self.host.set_raise(gil, None)
    }

    /// Clears as [`Self::clear_raise`] does and panics if the unit's own thread was not found.
    fn take_back_raise(&self, gil: H::Gil<'_>) {
        let found = self.clear_raise(gil);
        assert!(
            found,
            "a take-back runs on the unit's own thread (`UnitSlot` and `WorkUnit` are neither \
             Send nor Sync: a unit is begun, ended and asked on the thread its slot was made \
             on), attached to an interpreter; that interpreter holds the thread's current \
             thread state, which CPython bound to the thread that made it (this one), so \
             PyThreadState_SetAsyncExc finds a thread state with the unit's thread's ident"
        );
    }
}

fn this_thread() -> std::ffi::c_ulong {
    // SAFETY: This has no precondition and reads the OS thread's id.
    unsafe { ffi::PyThread_get_thread_ident() }
}

/// In every case nothing was set, and the unit does not read
/// [interrupted][InterruptTarget::interrupted].
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InterruptError {
    #[error(transparent)]
    NoThreadState(ThreadStateCreationFailure),
    #[error(
        "a unit of work of thread {thread} in interpreter {interp} has not ended, but that \
         thread has no thread state in it any more (the unit was leaked, not ended): nothing \
         was interrupted"
    )]
    UnitThreadGone {
        /// The value is `PyThread_get_thread_ident()`.
        thread: std::ffi::c_ulong,
        interp: i64,
    },
    #[error("the process is finalizing: no interpreter is entered to interrupt work any more")]
    Finalizing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("PyThreadState_New returned NULL (the runtime is out of memory)")]
pub struct ThreadStateCreationFailure;

/// Runs `f` with a token for a thread state PyO3 does not know is attached.
///
/// `Python::attach` would switch to the thread's gilstate thread state instead. A `Py<T>`
/// dropped in `f` is deferred rather than released, so `f` must use `drop_ref`.
///
/// # Safety
///
/// A thread state must be current on this thread.
unsafe fn with_assumed_attached<T>(f: impl for<'py> FnOnce(Python<'py>) -> T) -> T {
    f(Python::assume_attached())
}

#[cfg(test)]
mod test_gil;

#[cfg(test)]
#[cfg(sync_model)]
mod model_host;

#[cfg(test)]
#[cfg(all(sync_model, not(sync_shuttle)))]
mod loom_model;

#[cfg(test)]
#[cfg(all(sync_model, sync_shuttle))]
mod shuttle_model;

#[cfg(test)]
#[cfg(not(sync_model))]
mod unit_protocol_tests;

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    use super::*;

    /// A new phase fails to compile here until its row is written.
    #[test]
    fn every_phase_has_a_pinned_row_of_transitions() {
        for phase in UnitPhase::ALL {
            let (interrupt, take_back, end, has_ended, was_interrupted, pending) = match phase {
                UnitPhase::Running => (
                    UnitPhase::RunningInterruptOutstanding,
                    None,
                    UnitPhase::Ended,
                    false,
                    false,
                    false,
                ),
                UnitPhase::RunningInterruptOutstanding => (
                    UnitPhase::RunningInterruptOutstanding,
                    Some(UnitPhase::RunningInterruptTakenBack),
                    UnitPhase::EndedInterrupted,
                    false,
                    true,
                    true,
                ),
                UnitPhase::RunningInterruptTakenBack => (
                    UnitPhase::RunningInterruptOutstanding,
                    None,
                    UnitPhase::EndedInterrupted,
                    false,
                    true,
                    false,
                ),
                UnitPhase::Ended => (UnitPhase::EndedInterrupted, None, UnitPhase::Ended, true, false, false),
                UnitPhase::EndedInterrupted => (
                    UnitPhase::EndedInterrupted,
                    None,
                    UnitPhase::EndedInterrupted,
                    true,
                    true,
                    true,
                ),
            };
            assert_eq!(phase.interrupt(), interrupt, "interrupt on {phase:?}");
            assert_eq!(phase.take_back(), take_back, "take_back on {phase:?}");
            assert_eq!(phase.end(), end, "end on {phase:?}");
            assert_eq!(phase.has_ended(), has_ended, "has_ended on {phase:?}");
            assert_eq!(phase.was_interrupted(), was_interrupted, "was_interrupted on {phase:?}");
            assert_eq!(phase.exception_may_be_pending(), pending, "pending on {phase:?}");
        }
        assert_eq!(UnitPhase::ALL.len(), 5);
    }

    /// Runs on a real sub-interpreter, because the async-exception slot lives only in CPython.
    #[test]
    fn a_pending_interruption_is_taken_back_by_the_unit_end_or_drop() {
        Python::initialize();
        let serve = crate::python_thread("take-back-serve".into()).spawn(|| {
            let mut worker = SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                "take-back".into(),
                std::convert::identity,
                |_py| Ok(Nothing),
            )
            .expect("a sub-interpreter on its serving thread");
            worker.with_gil(|_held, _py| ()).expect("no panic");
            assert!(!worker.unit_slot_made(), "with_gil makes no unit slot");
            for dropped in [false, true] {
                let interrupted_then_ended = |_held: &mut Nothing, unit: WorkUnit<'_>, py: Python<'_>| {
                    let target = unit.target();
                    let interrupting = crate::python_thread("interrupting".into())
                        .spawn(move || {
                            target.interrupt().expect("set");
                            target
                        })
                        .expect("spawn the interrupting thread");
                    let target = py.detach(|| interrupting.join().expect("the interruption ends"));
                    if dropped {
                        drop(unit);
                    } else {
                        unit.end();
                    }
                    assert!(target.interrupted(), "the interruption was not recorded");
                    let two: i32 = py
                        .eval(c"1 + 1", None, None)
                        .expect("no exception left pending on the thread")
                        .extract()
                        .expect("an int");
                    assert_eq!(two, 2);
                };
                let ran = worker.with_unit(interrupted_then_ended);
                ran.expect("no panic in the unit");
                assert!(worker.unit_slot_made(), "the first unit makes the slot");
            }
            worker.end_on_own_thread();
        });
        serve
            .expect("spawn the worker thread")
            .join()
            .expect("the worker thread's assertions hold");
    }

    #[test]
    fn the_unit_phase_transitions_are_the_table() {
        use UnitPhase::*;
        let table = [
            (Running, RunningInterruptOutstanding, None, Ended, false, false, false),
            (
                RunningInterruptOutstanding,
                RunningInterruptOutstanding,
                Some(RunningInterruptTakenBack),
                EndedInterrupted,
                false,
                true,
                true,
            ),
            (
                RunningInterruptTakenBack,
                RunningInterruptOutstanding,
                None,
                EndedInterrupted,
                false,
                true,
                false,
            ),
            (Ended, EndedInterrupted, None, Ended, true, false, false),
            (
                EndedInterrupted,
                EndedInterrupted,
                None,
                EndedInterrupted,
                true,
                true,
                true,
            ),
        ];
        for (phase, interrupted, taken_back, ended, has_ended, was_interrupted, pending) in table {
            assert_eq!(phase.interrupt(), interrupted, "interrupt on {phase:?}");
            assert_eq!(phase.take_back(), taken_back, "take_back on {phase:?}");
            assert_eq!(phase.end(), ended, "end on {phase:?}");
            assert_eq!(phase.has_ended(), has_ended, "has_ended on {phase:?}");
            assert_eq!(phase.was_interrupted(), was_interrupted, "was_interrupted on {phase:?}");
            assert_eq!(
                phase.exception_may_be_pending(),
                pending,
                "exception_may_be_pending on {phase:?}"
            );
        }
    }

    /// The exit, on another thread, finds the unit running and leaves the word alone; once the
    /// unit is over it marks the word, and the next unit removes the mark as it enters.
    #[test]
    fn the_exit_marks_a_thread_only_between_units_and_entering_removes_the_mark() {
        use std::sync::mpsc;
        Python::initialize();
        let (entry_out, entry_in) = mpsc::channel::<ThreadEntry>();
        let (in_unit, is_in_unit) = mpsc::channel::<()>();
        let (looked, has_looked) = mpsc::channel::<ThreadAtExit>();
        let (unit_over, is_unit_over) = mpsc::channel::<()>();
        let (marked, is_marked) = mpsc::channel::<()>();
        let serve = crate::python_thread("watched".into())
            .spawn(move || {
                let mut worker = SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                    "watched".into(),
                    std::convert::identity,
                    |_py| Ok(Nothing),
                )
                .expect("a sub-interpreter on its serving thread");
                entry_out.send(ThreadEntry::this_thread()).unwrap();
                let seen = worker
                    .with_gil(|_held, _py| {
                        in_unit.send(()).unwrap();
                        has_looked.recv().unwrap()
                    })
                    .expect("no panic");
                unit_over.send(()).unwrap();
                is_marked.recv().unwrap();
                let marked_word = ENTRY.with(|e| e.load(Ordering::Relaxed)).addr();
                let in_unit_word = worker.with_gil(|_held, _py| ENTRY.with(|e| e.load(Ordering::Relaxed)).addr());
                worker.end_on_own_thread();
                (seen, marked_word, in_unit_word.expect("no panic"))
            })
            .expect("spawn the worker thread");
        let entry = entry_in.recv().unwrap();
        is_in_unit.recv().unwrap();
        // The worker thread waits on `has_looked`, so its word is alive.
        looked.send(entry.mark()).unwrap();
        is_unit_over.recv().unwrap();
        // The worker thread waits on `is_marked`, so its word is alive.
        let after = entry.mark();
        marked.send(()).unwrap();
        let (seen, marked_word, in_unit_word) = serve.join().expect("the worker thread ends");
        assert_eq!(seen, ThreadAtExit::RunningPython);
        assert_eq!(after, ThreadAtExit::OutsidePython);
        assert_eq!(marked_word & WATCHED, WATCHED);
        assert_eq!(in_unit_word, IN_PYTHON.addr());
    }

    struct Nothing;

    impl Releasable for Nothing {
        fn release(self, _py: Python<'_>) {}
    }

    #[test]
    fn a_panic_in_prepare_leaves_no_thread_state_current() {
        Python::initialize();
        let outcome = std::thread::spawn(|| {
            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                    "prepare-panics".into(),
                    std::convert::identity,
                    |_py| -> Result<Nothing, NewInterpreterError> { panic!("prepare panics") },
                )
            }))
            .is_err();
            // SAFETY: This is always safe to call.
            let current = unsafe { ffi::PyThreadState_GetUnchecked() };
            (panicked, current.is_null())
        })
        .join()
        .expect("the thread ends");
        assert_eq!(
            outcome,
            (true, true),
            "prepare panicked, and left a thread state current"
        );
    }

    /// Without the detach in `end`, the restore after `Py_EndInterpreter` would bring back the
    /// destroyed thread state.
    #[test]
    fn a_kept_worker_ended_without_detaching_leaves_no_thread_state() {
        Python::initialize();
        let serve = crate::python_thread("keep-end-serve".into())
            .spawn(move || {
                let mut worker = SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                    "keep-end".into(),
                    std::convert::identity,
                    |_py| Ok(Nothing),
                )
                .expect("a sub-interpreter on its serving thread");
                keep_attached_until_idle();
                worker.with_gil(|_held, _py| {}).expect("no panic");
                worker.end_on_own_thread();
                // SAFETY: This is always safe to call.
                let current = unsafe { ffi::PyThreadState_GetUnchecked() };
                assert!(
                    current.is_null(),
                    "the thread is left on a thread state after the end: {current:?}"
                );
            })
            .expect("spawn the worker thread");
        serve.join().expect("the worker thread ends");
    }

    #[test]
    fn after_a_panicking_prepare_a_new_sub_interpreter_can_be_built() {
        Python::initialize();
        std::thread::spawn(|| {
            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                    "prepare-panics-then".into(),
                    std::convert::identity,
                    |_py| -> Result<Nothing, NewInterpreterError> { panic!("prepare boom") },
                )
            }))
            .is_err();
            assert!(panicked);
            let again = SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                "after-panic".into(),
                std::convert::identity,
                |_py| Ok(Nothing),
            )
            .expect("a second sub-interpreter can be built on the same thread");
            again.end_on_own_thread();
            // SAFETY: This is always safe to call.
            assert!(unsafe { ffi::PyThreadState_GetUnchecked() }.is_null());
        })
        .join()
        .expect("the thread ends");
        Python::attach(|py| {
            let two: i32 = py
                .eval(c"1 + 1", None, None)
                .expect("main still runs Python")
                .extract()
                .expect("an int");
            assert_eq!(two, 2);
        });
    }

    /// An interruption entering the ended interpreter would be a use-after-free of its
    /// `PyInterpreterState`.
    #[test]
    fn a_leaked_unit_is_ended_by_its_workers_end() {
        Python::initialize();
        let target = crate::python_thread("leak-serve".into())
            .spawn(|| {
                let mut worker = SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                    "leak".into(),
                    std::convert::identity,
                    |_py| Ok(Nothing),
                )
                .expect("a sub-interpreter on its serving thread");
                let target = worker
                    .with_unit(|_held, unit, _py| {
                        let target = unit.target();
                        std::mem::forget(unit);
                        target
                    })
                    .expect("no panic in the unit");
                assert!(target.is_running(), "a leaked unit has not ended");
                worker.end_on_own_thread();
                target
            })
            .expect("spawn the worker thread")
            .join()
            .expect("the worker thread ends");
        assert!(!target.is_running(), "the worker's end ended its leaked unit");
        assert_eq!(target.interrupt(), Ok(()));
        assert!(!target.interrupted(), "nothing was interrupted");
    }

    #[derive(Debug)]
    enum PrepareError {
        Refused(NewInterpreterError),
        InInterpreter(i64),
    }

    #[test]
    fn a_prepare_that_fails_ends_its_interpreter() {
        Python::initialize();
        let failed_in = std::thread::spawn(|| {
            let failed = SubInterpreter::<Nothing>::create_on_this_thread::<PrepareError>(
                "prepare-fails".into(),
                PrepareError::Refused,
                |py| Err(PrepareError::InInterpreter(InterpreterHandle::current(py).id())),
            );
            let id = match failed {
                Err(PrepareError::InInterpreter(id)) => id,
                Err(PrepareError::Refused(e)) => panic!("the creation itself failed: {e}"),
                Ok(_) => panic!("a failing prepare built a worker"),
            };
            // SAFETY: This is always safe to call.
            assert!(unsafe { ffi::PyThreadState_GetUnchecked() }.is_null());
            let again = SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                "after-failed-prepare".into(),
                std::convert::identity,
                |_py| Ok(Nothing),
            )
            .expect("a second sub-interpreter can be built on the same thread");
            again.end_on_own_thread();
            id
        })
        .join()
        .expect("the thread ends");
        assert_ne!(failed_in, 0, "prepare ran in a sub-interpreter, not main");
        crate::attach::with_main(|py| {
            let live: Vec<(i64, i64)> = py
                .import("_interpreters")
                .and_then(|m| m.call_method0("list_all"))
                .and_then(|all| all.extract())
                .expect("the runtime lists its interpreters");
            assert!(
                live.iter().all(|&(id, _)| id != failed_in),
                "interpreter {failed_in} outlived its failed prepare: {live:?}"
            );
        });
    }

    #[test]
    fn a_refused_interpreter_reports_its_pystatus() {
        let status = ffi::PyStatus {
            _type: ffi::_PyStatus_TYPE::_PyStatus_TYPE_ERROR,
            func: c"init_interp_create_gil".as_ptr(),
            err_msg: c"failed to create a new GIL".as_ptr(),
            exitcode: 0,
        };
        // SAFETY: Both strings are NUL-terminated literals.
        let err = unsafe { NewInterpreterError::from_status(&status) };
        assert_eq!(
            err.to_string(),
            "Py_NewInterpreterFromConfig failed in init_interp_create_gil: failed to create \
             a new GIL"
        );
    }

    #[test]
    fn a_status_without_text_says_so() {
        let status = ffi::PyStatus {
            _type: ffi::_PyStatus_TYPE::_PyStatus_TYPE_OK,
            func: std::ptr::null(),
            err_msg: std::ptr::null(),
            exitcode: 0,
        };
        // SAFETY: NULL pointers are allowed.
        let err = unsafe { NewInterpreterError::from_status(&status) };
        assert_eq!(
            err.to_string(),
            "Py_NewInterpreterFromConfig failed: it returned no error message and no thread \
             state"
        );
    }
}
