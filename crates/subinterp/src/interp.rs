//! Creates, serves and ends one isolated sub-interpreter with its own GIL, all on one thread.

use std::cell::{Cell, RefCell};
use std::ffi::CStr;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PerInterpreterCell;

use crate::entry::{self, ThreadAtExit, ThreadStateSwap};
use crate::finalization::{Finalization, OwnRefusal, Ownership, RegisterHookError, FINALIZATION};
use crate::sync::AtomicPtr;
use crate::sync::Ordering;
use crate::NewThreadStateError;

pub(super) mod unit;

use self::unit::{UnitSlot, WorkUnit};

/// Why no sub-interpreter was created; in every case none is left behind.
#[derive(Debug, thiserror::Error)]
pub enum NewInterpreterError {
    #[error("no thread state could be made for the main interpreter to create from: {0}")]
    NoMainThreadState(NewThreadStateError),
    #[error(transparent)]
    HookRefused(RegisterHookError),
    #[error(transparent)]
    Refused(PyStatusError),
    /// The process's exit started deciding, and an interpreter created now could run Python
    /// after it decided that none would.
    #[error("the process is exiting: no sub-interpreter is created once its exit has started")]
    Exiting,
    #[error("the calling thread is ending: no sub-interpreter is created while its thread-locals are destroyed")]
    ThreadEnding,
    /// The main interpreter a creation starts from is going away.
    #[error("the process is finalizing: no sub-interpreter is created once Py_Finalize has begun")]
    Finalizing,
}

/// `Py_NewInterpreterFromConfig`'s error status: the CPython function that failed and its
/// message, each when CPython gave one.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "Py_NewInterpreterFromConfig failed{}: {}",
    .func.as_deref().map(|f| format!(" in {f}")).unwrap_or_default(),
    .message.as_deref().unwrap_or("it returned no error message and no thread state")
)]
pub struct PyStatusError {
    pub func: Option<String>,
    pub message: Option<String>,
}

impl PyStatusError {
    /// # Safety
    ///
    /// `status.func` and `status.err_msg` must be NULL or NUL-terminated, as CPython guarantees.
    unsafe fn from_status(status: &ffi::PyStatus) -> Self {
        let text = |p: *const std::ffi::c_char| {
            (!p.is_null()).then(|| {
                // SAFETY: `p` is non-null here and NUL-terminated by the caller's contract, and the
                // borrow ends once the text is copied, while `status` is still borrowed.
                unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
            })
        };
        PyStatusError {
            func: text(status.func),
            message: text(status.err_msg),
        }
    }
}

impl From<OwnRefusal> for NewInterpreterError {
    fn from(refusal: OwnRefusal) -> Self {
        match refusal {
            OwnRefusal::ExitStarted => NewInterpreterError::Exiting,
            OwnRefusal::ThreadEnding => NewInterpreterError::ThreadEnding,
        }
    }
}

/// The references a worker holds inside its interpreter, which must be released while that
/// interpreter is alive and its thread state is current.
pub trait Releasable: Sized {
    /// Releases every reference it holds; this interpreter's thread state must be current.
    fn release(self, py: Python<'_>);
}

/// A PEP 684 sub-interpreter with its own GIL, ended on its thread when dropped.
///
/// It is neither `Send` nor `Sync`, so every method runs on the thread that created it and the
/// thread state they restore is that thread's.
///
/// Dropping it releases what it holds and ends the interpreter, which runs Python
/// (`Py_EndInterpreter` joins the interpreter's non-daemon threads and runs its `atexit` and
/// `__del__`) for as long as that takes; meanwhile the thread counts as running Python, so the
/// process's exit does not wait for it. Once finalization has begun, dropping it leaves the
/// interpreter to `Py_Finalize` instead and leaks what it holds.
pub struct SubInterpreter<R: Releasable> {
    /// The thread state is saved while nothing runs on it.
    tstate: NonNull<ffi::PyThreadState>,
    attachment: ManuallyDrop<R>,
    /// The slot is kept here rather than by the caller so that a unit is only ever begun in the
    /// interpreter its slot names; it is made by the first [`Self::with_unit`].
    units: Option<UnitSlot>,
    /// Dropped after the interpreter ended, so the exit waits for the end.
    #[expect(
        dead_code,
        reason = "held for its drop, which tells the exit this thread owns no interpreter"
    )]
    ownership: Ownership,
}

impl<R: Releasable> Drop for SubInterpreter<R> {
    fn drop(&mut self) {
        let Some(_not_finalizing) = FINALIZATION.try_hold() else {
            return;
        };
        // Otherwise the thread state saved as `previous` below could be this worker's kept one,
        // and the restore after `Py_EndInterpreter` would bring it back destroyed.
        detach_for_idle();
        let span = PythonSpan::enter();
        // SAFETY: The worker's thread state was made on this thread and is saved (detached above
        // if kept), and the interpreter is alive: only this drop ends it, while finalization is
        // held off.
        let released = unsafe { self.end_here() };
        drop(span);
        // A thread already unwinding would abort on a second panic; the first is what it reports.
        if let Err(panic) = released {
            if !std::thread::panicking() {
                std::panic::resume_unwind(panic);
            }
        }
    }
}

impl<R: Releasable> SubInterpreter<R> {
    /// Ends a leaked unit, releases what the worker holds, and ends the interpreter; a panic in
    /// the release is caught and returned once the interpreter has ended.
    ///
    /// Another interpreter's thread state current on this thread is saved and restored around
    /// it.
    ///
    /// # Safety
    ///
    /// The worker's thread state must be saved, and finalization must not begin before this
    /// returns. It is called once, by the drop.
    unsafe fn end_here(&mut self) -> std::thread::Result<()> {
        // SAFETY: The contract; `attachment` is taken once, because this runs once.
        unsafe {
            let previous = ffi::PyThreadState_GetUnchecked();
            if !previous.is_null() {
                ffi::PyEval_SaveThread();
            }
            ffi::PyEval_RestoreThread(self.tstate.as_ptr());
            let attachment = ManuallyDrop::take(&mut self.attachment);
            let units = self.units.take();
            let released = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                with_assumed_attached(|py| {
                    // Otherwise an interruption still aimed at a leaked unit could land in the
                    // release below or enter the interpreter after it is gone.
                    if let Some(units) = units {
                        units.end_leaked_unit(py);
                    }
                    attachment.release(py);
                })
            }));
            ffi::Py_EndInterpreter(ffi::PyThreadState_Get());
            if !previous.is_null() {
                ffi::PyEval_RestoreThread(previous);
            }
            released
        }
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
        // SAFETY: `PyInterpreterState_Main` only reads the runtime's pointer to the main
        // interpreter, which lives for the process; `PyThreadState_New` does not need the GIL and
        // leaves the state it makes detached.
        let tstate = unsafe { ffi::PyThreadState_New(ffi::PyInterpreterState_Main()) };
        NonNull::new(tstate)
            .map(TemporaryMainThreadState)
            .ok_or(NewInterpreterError::NoMainThreadState(NewThreadStateError))
    }

    /// Attaches the thread state, taking the main GIL.
    ///
    /// # Safety
    ///
    /// No thread state may be current on the calling thread.
    unsafe fn attach(&self) {
        // SAFETY: No thread state is current on this thread (the contract), and `self.0` is a live
        // main-interpreter thread state, made by `make` and deleted only by consuming `self`.
        unsafe { ffi::PyEval_RestoreThread(self.0.as_ptr()) };
    }

    /// Clears and deletes the thread state, leaving none current.
    ///
    /// # Safety
    ///
    /// This thread state must be current on the calling thread.
    unsafe fn dispose_attached(self) {
        // SAFETY: `self.0` is current (the contract), so the main GIL is held as clearing needs,
        // and clearing does not detach it; `DeleteCurrent` deletes it and releases the GIL, and
        // `self` is consumed here, so nothing uses the deleted state again.
        unsafe {
            ffi::PyThreadState_Clear(self.0.as_ptr());
            ffi::PyThreadState_DeleteCurrent();
        }
    }

    /// # Safety
    ///
    /// No thread state may be current on the calling thread.
    unsafe fn dispose(self) {
        // SAFETY: No thread state is current (the contract), as `attach` needs, and the attach
        // makes `self.0` current, as `dispose_attached` needs.
        unsafe {
            self.attach();
            self.dispose_attached();
        }
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

/// Holds nothing; reaching it in a new interpreter makes PyO3's per-interpreter registry there.
///
/// Making that registry imports `atexit` and `gc`, which runs bytecode and clears whatever it
/// raises. Made inside a unit, it would run an interruption pending on the unit's thread and
/// swallow it, so a creation makes it before `prepare`.
static PYO3_REGISTRY: PerInterpreterCell<()> = PerInterpreterCell::new();

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
    /// Returns `refused`'s error when the runtime refuses, the process's exit has started or
    /// finalization has begun, and `prepare`'s error after ending the interpreter. A panic in
    /// `prepare` also ends it.
    ///
    /// # Aborts
    ///
    /// CPython aborts the process when another thread state is current on the calling thread,
    /// such as inside a `Python::attach`.
    pub fn create_on_this_thread<E>(
        refused: impl FnOnce(NewInterpreterError) -> E,
        prepare: impl for<'py> FnOnce(Python<'py>) -> Result<R, E>,
    ) -> Result<Self, E> {
        let Some(_not_finalizing) = FINALIZATION.try_hold() else {
            return Err(refused(NewInterpreterError::Finalizing));
        };
        detach_for_idle();
        // Owned from before the creation runs Python, so an exit that starts meanwhile watches
        // this thread.
        let ownership = match FINALIZATION.own() {
            Ok(ownership) => ownership,
            Err(refusal) => return Err(refused(refusal.into())),
        };
        let _span = PythonSpan::enter();
        // SAFETY: Finalization cannot begin while `_not_finalizing` is held, and no thread state
        // of ours is current after the detach; anyone else's makes CPython abort.
        unsafe { Self::create_here(refused, prepare, ownership) }
    }

    /// # Safety
    ///
    /// The main interpreter must not be finalized, nor begin to be, before this returns.
    unsafe fn create_here<E>(
        refused: impl FnOnce(NewInterpreterError) -> E,
        prepare: impl for<'py> FnOnce(Python<'py>) -> Result<R, E>,
        ownership: Ownership,
    ) -> Result<Self, E> {
        #[cfg(feature = "startup_trace")]
        trace("create_begin");
        // SAFETY: No thread state of this thread's is current: `create_on_this_thread` detached
        // the kept one before calling, and one current from elsewhere makes CPython abort on the
        // attach below.
        let main = match unsafe { TemporaryMainThreadState::make() } {
            Ok(main) => main,
            Err(e) => return Err(refused(e)),
        };
        // SAFETY: `make` left `main` detached and nothing else was current (above).
        unsafe { main.attach() };
        #[cfg(feature = "startup_trace")]
        trace("main_attached");
        // SAFETY: The main interpreter's thread state was attached above.
        let armed = unsafe { with_assumed_attached(|py| FINALIZATION.arm(py)) };
        if let Err(error) = armed {
            // SAFETY: `main` is still current: registering the hook does not detach it.
            unsafe { main.dispose_attached() };
            return Err(refused(NewInterpreterError::HookRefused(error)));
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
        // SAFETY: The main thread state is current, so the main GIL is held; the main interpreter
        // is not finalizing (the contract); both pointers are to live locals.
        let status = unsafe { ffi::Py_NewInterpreterFromConfig(&mut new_tstate, &config) };
        let new_tstate = match NonNull::new(new_tstate) {
            // SAFETY: `PyStatus_IsError` reads only the status it is passed by value.
            Some(tstate) if unsafe { ffi::PyStatus_IsError(status) } == 0 => tstate,
            _ => {
                // SAFETY: CPython sets a status's `func` and `err_msg` to NULL or to static
                // NUL-terminated strings.
                let error = unsafe { PyStatusError::from_status(&status) };
                // SAFETY: The creation failed, and a failed creation attaches the main thread
                // state again, so `main` is current.
                unsafe { main.dispose_attached() };
                return Err(refused(NewInterpreterError::Refused(error)));
            }
        };

        #[cfg(feature = "startup_trace")]
        trace("interp_created");
        let mut unwind_guard = EndOnUnwind {
            main: ManuallyDrop::new(main),
            armed: true,
        };
        // SAFETY: The new interpreter's thread state is current after a successful creation.
        let built = unsafe {
            with_assumed_attached(|py| {
                PYO3_REGISTRY.get_or_init(py, || ());
                prepare(py)
            })
        };
        unwind_guard.armed = false;
        // SAFETY: The guard is disarmed, so nothing else takes `main` out of it.
        let main = unsafe { ManuallyDrop::take(&mut unwind_guard.main) };
        #[cfg(feature = "startup_trace")]
        trace("prepared");
        let result = match built {
            Ok(attachment) => {
                // SAFETY: The new interpreter's thread state is still current: `prepare` got only a
                // token for it, and a detach inside `prepare` restores it before returning.
                unsafe { ffi::PyEval_SaveThread() };
                Ok(SubInterpreter {
                    tstate: new_tstate,
                    attachment: ManuallyDrop::new(attachment),
                    units: None,
                    ownership,
                })
            }
            Err(e) => {
                // SAFETY: `PyThreadState_Get` returns the current thread state, which is the new
                // interpreter's for the same reason as in the `Ok` arm, and that is the state
                // `Py_EndInterpreter` must be given; ending it leaves none current.
                unsafe { ffi::Py_EndInterpreter(ffi::PyThreadState_Get()) };
                Err(e)
            }
        };
        // SAFETY: No thread state is current: the save or the end above left none, and the
        // successful creation detached `main`.
        unsafe { main.dispose() };
        #[cfg(feature = "startup_trace")]
        trace("main_disposed");
        result
    }

    /// Returns what the worker holds without its GIL, for plain Rust fields only, never a `Py<T>`.
    pub fn attachment(&self) -> &R {
        &self.attachment
    }

    /// Returns whether [`Self::with_unit`] has run on this worker.
    pub fn unit_slot_made(&self) -> bool {
        self.units.is_some()
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
        let attachment = &mut *self.attachment;
        // SAFETY: `self.tstate` was made on this thread, which the worker never leaves, and lives
        // until the drop ends the worker.
        unsafe { enter(self.tstate, |py| f(attachment, py)) }
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
        f: impl for<'s, 'py> FnOnce(&mut R, WorkUnit<'s, 'py>, Python<'py>) -> T,
    ) -> std::thread::Result<T> {
        let attachment = &mut *self.attachment;
        let units = &mut self.units;
        // SAFETY: The same holds as in `with_gil`.
        unsafe {
            enter(self.tstate, |py| {
                let unit = units.get_or_insert_with(|| UnitSlot::new(py)).begin(py);
                f(attachment, unit, py)
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
    let before = ENTRY.with(|e| e.swap(in_python(), Ordering::Relaxed));
    let (switch, watched) = entry::entering(before.addr(), tstate.addr());
    if watched {
        python_entered_while_watched();
    }
    match switch {
        ThreadStateSwap::Stay => {}
        // SAFETY: `tstate` is this thread's saved worker state (the contract), and the word kept
        // none current; if this thread is already in a unit, CPython aborts here.
        ThreadStateSwap::Restore => unsafe { ffi::PyEval_RestoreThread(tstate) },
        // SAFETY: The word holds a thread state only while it is current on this thread, so the
        // other worker's is current and is the state saved here; then none is, and `tstate` is
        // this thread's saved worker state (the contract).
        ThreadStateSwap::SaveThenRestore => unsafe {
            ffi::PyEval_SaveThread();
            ffi::PyEval_RestoreThread(tstate);
        },
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: This worker's thread state is current and stays current for `f`.
        unsafe { Python::attach_current(f) }
    }));
    if KEEP.with(Cell::get) {
        ENTRY.with(|e| e.store(tstate, Ordering::Relaxed));
    } else {
        // SAFETY: `tstate` is current: `attach_current` leaves it so, also when `f` panicked,
        // which `catch_unwind` caught.
        unsafe { ffi::PyEval_SaveThread() };
        ENTRY.with(|e| e.store(std::ptr::null_mut(), Ordering::Relaxed));
    }
    result
}

fn in_python() -> *mut ffi::PyThreadState {
    std::ptr::without_provenance_mut(entry::IN_PYTHON)
}

/// Tells the exit watching this thread that Python runs on it again.
#[cold]
#[inline(never)]
fn python_entered_while_watched() {
    FINALIZATION.python_entered_while_watched();
}

/// Marks this thread as running Python for a creation or an end until dropped, as [`enter`] does
/// for a unit.
///
/// The thread keeps no worker thread state current here (the caller detached it), so the word it
/// leaves is null.
struct PythonSpan;

impl PythonSpan {
    fn enter() -> PythonSpan {
        let before = ENTRY.with(|e| e.swap(in_python(), Ordering::Relaxed));
        if entry::watched(before.addr()) {
            python_entered_while_watched();
        }
        PythonSpan
    }
}

impl Drop for PythonSpan {
    fn drop(&mut self) {
        ENTRY.with(|e| e.store(std::ptr::null_mut(), Ordering::Relaxed));
    }
}

/// A thread's entry word ([`crate::entry`]), for the exit to mark from another thread.
///
/// [`Finalization::own`] makes it on the owning thread and keeps it in the owners' list until the
/// ownership drops or the thread ends ([`ThreadOwner`]), before its thread-locals are freed: so
/// while one is in the list its word is alive.
pub(crate) struct ThreadEntry(*const AtomicPtr<ffi::PyThreadState>);

// SAFETY: The word is an atomic, alive while the entry is held (above).
unsafe impl Send for ThreadEntry {}

impl ThreadEntry {
    /// The calling thread's word.
    pub(crate) fn this_thread() -> Self {
        ThreadEntry(ENTRY.with(std::ptr::from_ref))
    }

    /// Returns whether both are the same thread's word.
    pub(crate) fn is(&self, other: &ThreadEntry) -> bool {
        std::ptr::eq(self.0, other.0)
    }

    /// Marks the word for the exit unless Python runs on the thread, and says which.
    pub(crate) fn mark(&self) -> ThreadAtExit {
        // SAFETY: The word is alive while the entry is held (the type's invariant).
        let word = unsafe { &*self.0 };
        let mut seen = word.load(Ordering::Relaxed);
        loop {
            let Some(marking) = entry::marked(seen.addr()) else {
                return ThreadAtExit::RunningPython;
            };
            match word.compare_exchange_weak(seen, seen.with_addr(marking), Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return ThreadAtExit::OutsidePython,
                Err(now) => seen = now,
            }
        }
    }
}

/// Takes this thread's entries out of every owners' list it joined when the thread ends, before
/// its word is freed: a worker leaked with `mem::forget` never gives its entry up itself.
struct ThreadOwner(RefCell<Vec<&'static Finalization>>);

impl Drop for ThreadOwner {
    fn drop(&mut self) {
        let entry = ThreadEntry::this_thread();
        for finalization in self.0.get_mut() {
            finalization.thread_ended(&entry);
        }
    }
}

/// Makes this thread take its entries out of `finalization`'s list when it ends.
///
/// # Errors
///
/// Fails while the thread's thread-locals are being destroyed.
pub(crate) fn forget_owners_at_thread_exit(
    finalization: &'static Finalization,
) -> Result<(), std::thread::AccessError> {
    THREAD_OWNER.try_with(|owner| {
        let mut joined = owner.0.borrow_mut();
        if !joined.iter().any(|f| std::ptr::eq(*f, finalization)) {
            joined.push(finalization);
        }
    })
}

thread_local! {
    static KEEP: Cell<bool> = const { Cell::new(false) };
    pub(crate) static ENTRY: AtomicPtr<ffi::PyThreadState> = const { AtomicPtr::new(std::ptr::null_mut()) };
    static THREAD_OWNER: ThreadOwner = const { ThreadOwner(RefCell::new(Vec::new())) };
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
    let _ = ENTRY.try_with(|word| {
        if entry::keeps(word.load(Ordering::Relaxed).addr()) {
            // SAFETY: `enter` keeps a thread state in the word only while it is current, and only
            // this takes it out; the exit only adds `WATCHED`, which the clear below keeps.
            unsafe { ffi::PyEval_SaveThread() };
            word.fetch_and(entry::WATCHED, Ordering::Relaxed);
        }
    });
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
    let () = <WorkUnit<'static, 'static> as NotSend<_>>::CHECK;
    let () = <WorkUnit<'static, 'static> as NotSync<_>>::CHECK;
    let () = <UnitSlot as NotSend<_>>::CHECK;
    let () = <UnitSlot as NotSync<_>>::CHECK;
    struct Nothing;
    impl Releasable for Nothing {
        fn release(self, _py: Python<'_>) {}
    }
    let () = <SubInterpreter<Nothing> as NotSend<_>>::CHECK;
    <SubInterpreter<Nothing> as NotSync<_>>::CHECK
};

/// Runs `f` with a token for a thread state PyO3 does not know is attached.
///
/// `Python::attach` would switch to the thread's gilstate thread state instead. A `Py<T>`
/// dropped in `f` is deferred rather than released, so `f` must use `drop_ref`.
///
/// # Safety
///
/// A thread state must be current on this thread.
unsafe fn with_assumed_attached<T>(f: impl for<'py> FnOnce(Python<'py>) -> T) -> T {
    // SAFETY: A thread state is current on this thread (the contract), which is all
    // `assume_attached` asks.
    f(unsafe { Python::assume_attached() })
}

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    use pyo3::sync::InterpreterHandle;

    use super::*;

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
                    std::convert::identity,
                    |_py| Ok(Nothing),
                )
                .expect("a sub-interpreter on its serving thread");
                entry_out.send(ThreadEntry::this_thread()).unwrap();
                let seen = worker
                    .with_gil(|_attachment, _py| {
                        in_unit.send(()).unwrap();
                        has_looked.recv().unwrap()
                    })
                    .expect("no panic");
                unit_over.send(()).unwrap();
                is_marked.recv().unwrap();
                let marked_word = ENTRY.with(|e| e.load(Ordering::Relaxed)).addr();
                let in_unit_word = worker.with_gil(|_attachment, _py| ENTRY.with(|e| e.load(Ordering::Relaxed)).addr());
                drop(worker);
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
        assert_eq!(marked_word & entry::WATCHED, entry::WATCHED);
        assert_eq!(in_unit_word, entry::IN_PYTHON);
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

    /// Without the detach in the drop, the restore after `Py_EndInterpreter` would bring back the
    /// destroyed thread state.
    #[test]
    fn a_kept_worker_ended_without_detaching_leaves_no_thread_state() {
        Python::initialize();
        let serve = crate::python_thread("keep-end-serve".into())
            .spawn(move || {
                let mut worker = SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                    std::convert::identity,
                    |_py| Ok(Nothing),
                )
                .expect("a sub-interpreter on its serving thread");
                keep_attached_until_idle();
                worker.with_gil(|_attachment, _py| {}).expect("no panic");
                drop(worker);
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
                    std::convert::identity,
                    |_py| -> Result<Nothing, NewInterpreterError> { panic!("prepare boom") },
                )
            }))
            .is_err();
            assert!(panicked);
            let again = SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                std::convert::identity,
                |_py| Ok(Nothing),
            )
            .expect("a second sub-interpreter can be built on the same thread");
            drop(again);
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

    #[derive(Debug)]
    enum PrepareError {
        Refused(NewInterpreterError),
        InInterpreter(i64),
    }

    #[test]
    fn a_prepare_that_fails_ends_its_interpreter() {
        Python::initialize();
        let failed_in = std::thread::spawn(|| {
            let failed =
                SubInterpreter::<Nothing>::create_on_this_thread::<PrepareError>(PrepareError::Refused, |py| {
                    Err(PrepareError::InInterpreter(InterpreterHandle::current(py).id()))
                });
            let id = match failed {
                Err(PrepareError::InInterpreter(id)) => id,
                Err(PrepareError::Refused(e)) => panic!("the creation itself failed: {e}"),
                Ok(_) => panic!("a failing prepare built a worker"),
            };
            // SAFETY: This is always safe to call.
            assert!(unsafe { ffi::PyThreadState_GetUnchecked() }.is_null());
            let again = SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                std::convert::identity,
                |_py| Ok(Nothing),
            )
            .expect("a second sub-interpreter can be built on the same thread");
            drop(again);
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
        let err = unsafe { PyStatusError::from_status(&status) };
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
        let err = unsafe { PyStatusError::from_status(&status) };
        assert_eq!(
            err.to_string(),
            "Py_NewInterpreterFromConfig failed: it returned no error message and no thread \
             state"
        );
    }
}
