//! Lets another thread interrupt one unit of work of a worker, and ends the unit so that no
//! interruption lands after it.

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::{Arc, PoisonError};

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::InterpreterHandle;

use self::word::{AdmitError, SlotWord, UnitId, UnitPhase, UnitState, ONE_INTERRUPTER};
use crate::finalization::FINALIZATION;
use crate::sync::{AtomicU64, Condvar, Mutex, Ordering};

mod word;

/// One unit of work of a worker, which another thread may interrupt through its
/// [`InterruptTarget`].
///
/// It cannot leave [`SubInterpreter::with_unit`](super::SubInterpreter::with_unit)'s closure and
/// is neither `Send` nor `Sync`, so wherever it is ended or dropped the thread is attached to the
/// unit's own interpreter. Ending or dropping it takes back an interruption that was set and not
/// yet raised, and none lands after. A unit leaked with `mem::forget` is ended the same way by the
/// next unit's begin, or by the worker's end.
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
///     let _ = worker.with_unit(|_attachment, unit, _py| kept = Some(unit));
/// }
/// ```
///
/// ```compile_fail
/// # use mipyre_subinterp::{Releasable, SubInterpreter};
/// # use pyo3::Python;
/// # struct Nothing;
/// # impl Releasable for Nothing { fn release(self, _py: Python<'_>) {} }
/// fn escape(worker: &mut SubInterpreter<Nothing>) {
///     let unit = worker.with_unit(|_attachment, unit, _py| unit);
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
///     let _ = worker.with_unit(|_attachment, unit, py| {
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
///     let ran = worker.with_unit(|_attachment, unit, py| {
///         py.detach(|| ());
///         unit.end().interrupted
///     });
///     ran.is_ok_and(|interrupted| !interrupted)
/// }
/// ```
#[must_use = "a unit of work ends when it is ended or dropped"]
pub struct WorkUnit<'s, 'py> {
    inner: &'s Arc<SlotInner>,
    unit: UnitId,
    py: Python<'py>,
    /// Whether this worker took an interruption back during the unit: its own fact, which only
    /// its end reads.
    taken_back: Cell<bool>,
}

/// How one unit of work ended: `interrupted` says whether an interruption was set during it,
/// even one the worker took back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnitEnd {
    pub interrupted: bool,
}

/// A handle another thread uses to interrupt one unit of work.
#[derive(Clone, Debug)]
pub struct InterruptTarget {
    inner: Arc<SlotInner>,
    unit: UnitId,
}

/// What an interruption did; in every case it returned once nothing of it is left in flight.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Interruption {
    /// `KeyboardInterrupt` was set on the unit's thread, and the unit's end reports it.
    Set,
    /// The unit had ended, or another one began: nothing was set.
    UnitEnded,
}

/// The state of one worker's slot, whose `interpreter` is fixed while the worker is served.
#[derive(Debug)]
struct SlotInner<H = SlotThread> {
    interpreter: H,
    /// Holds a [`SlotWord`], so every change of the unit, its phase and the interrupters counted
    /// in is one atomic update.
    ///
    /// Every write is a read-modify-write on this one word, so all threads agree on a single
    /// order without `SeqCst`. If an interrupter counts in first, the end waits for it; if the
    /// end or the next begin comes first, the interrupter sees that and leaves without entering
    /// the interpreter. `AcqRel` and `Acquire` carry the writes made under the GIL.
    word: mipyre_cpu::CacheLine<AtomicU64>,
    /// A waiter checks the count under this lock before it sleeps, and the last interrupter to
    /// leave takes it before notifying, so no wake-up is lost.
    idle_lock: Mutex<()>,
    idle: Condvar,
}

#[derive(Debug)]
struct SlotThread {
    interp: InterpreterHandle,
    /// The `PyThread_get_thread_ident()` value that `PyThreadState_SetAsyncExc` takes.
    thread: std::ffi::c_ulong,
}

/// The unit's thread has no thread state in its interpreter any more.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ThreadGone;

/// Why an interrupter left without setting anything, as the slot protocol sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InterruptFailure {
    TooManyInFlight,
    NoThreadState(NewThreadStateError),
    ThreadGone,
}

/// The interpreter operations the slot protocol needs, abstracted so the tests and the models run
/// the protocol code itself rather than a copy of it.
trait Interpreter {
    /// The value proves that the calling thread holds the unit's GIL.
    type Gil<'py>: Copy;
    type Exception: Copy;

    /// Runs `f` holding the unit's GIL from another thread.
    fn visit<R>(&self, f: impl for<'py> FnOnce(Self::Gil<'py>) -> R) -> Result<R, NewThreadStateError>;

    /// Sets `exc` pending on the unit's thread.
    fn raise(&self, gil: Self::Gil<'_>, exc: Self::Exception) -> Result<(), ThreadGone>;

    /// Clears what is pending on the unit's thread. Not finding the thread means its thread state,
    /// and whatever was pending on it, is gone, so there is nothing to report.
    fn clear(&self, gil: Self::Gil<'_>);

    /// Runs `f` with the GIL released and takes it back after.
    fn without_gil(&self, gil: Self::Gil<'_>, f: impl FnOnce());
}

impl Interpreter for SlotThread {
    type Gil<'py> = Python<'py>;
    type Exception = *mut ffi::PyObject;

    fn visit<R>(&self, f: impl for<'py> FnOnce(Python<'py>) -> R) -> Result<R, NewThreadStateError> {
        self.interp.try_attach(f).ok_or(NewThreadStateError)
    }

    fn raise(&self, _attached: Python<'_>, exc: *mut ffi::PyObject) -> Result<(), ThreadGone> {
        // SAFETY: The token proves a thread state is current, and the only exception set is the
        // static `KeyboardInterrupt` type.
        let reached = unsafe { ffi::PyThreadState_SetAsyncExc(self.thread as std::ffi::c_long, exc) };
        if reached == 0 {
            return Err(ThreadGone);
        }
        Ok(())
    }

    fn clear(&self, _attached: Python<'_>) {
        // SAFETY: The token proves a thread state is current, and NULL clears. The count of thread
        // states it changed is 0 only when the unit's thread state is gone, and its exception with
        // it.
        unsafe { ffi::PyThreadState_SetAsyncExc(self.thread as std::ffi::c_long, std::ptr::null_mut()) };
    }

    fn without_gil(&self, _attached: Python<'_>, f: impl FnOnce()) {
        // SAFETY: The token proves a thread state is current, and it is restored below.
        let saved = unsafe { ffi::PyEval_SaveThread() };
        f();
        // SAFETY: `saved` is this thread's, saved above.
        unsafe { ffi::PyEval_RestoreThread(saved) };
    }
}

/// Counts one interrupter in until dropped; the last drop wakes a waiting end.
struct InterrupterPermit<'a, H: Interpreter>(&'a SlotInner<H>);

impl<H: Interpreter> Drop for InterrupterPermit<'_, H> {
    fn drop(&mut self) {
        // `word::leave` is this subtraction (proved there): the pass holds one of the count.
        let before = SlotWord::from_bits(self.0.word.fetch_sub(ONE_INTERRUPTER, Ordering::AcqRel));
        if before.decode().in_flight == 1 {
            self.0.wake_waiters();
        }
    }
}

/// One worker's reusable unit, made once on the serving thread and begun for each request without
/// allocating.
#[derive(Debug)]
pub(super) struct UnitSlot(
    Arc<SlotInner>,
    // The take-back relies on every unit being begun on the thread the slot was made on.
    PhantomData<*const ()>,
);

impl UnitSlot {
    pub(super) fn new(py: Python<'_>) -> UnitSlot {
        UnitSlot(
            Arc::new(SlotInner::new(SlotThread {
                interp: InterpreterHandle::current(py),
                thread: this_thread(),
            })),
            PhantomData,
        )
    }

    /// Begins the next unit, ending first a unit that was leaked rather than ended.
    #[inline]
    pub(super) fn begin<'py>(&self, py: Python<'py>) -> WorkUnit<'_, 'py> {
        WorkUnit {
            inner: &self.0,
            unit: self.0.begin(py),
            py,
            taken_back: Cell::new(false),
        }
    }

    /// Ends the last unit as a drop would, if a leaked [`WorkUnit`] left it unended.
    pub(super) fn end_leaked_unit(self, py: Python<'_>) {
        self.0.end_attached(py, false);
    }
}

impl std::fmt::Debug for WorkUnit<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkUnit")
            .field("inner", &self.inner)
            .field("unit", &self.unit)
            .finish_non_exhaustive()
    }
}

impl WorkUnit<'_, '_> {
    pub fn target(&self) -> InterruptTarget {
        InterruptTarget {
            inner: Arc::clone(self.inner),
            unit: self.unit,
        }
    }

    /// Returns whether the unit was interrupted since it last asked, clearing the unraised
    /// exception.
    ///
    /// A worker waiting outside bytecode uses this to raise its own exception: the pending one
    /// would land only at the thread's next eval-breaker check, which may be inside C code that
    /// clears whatever it raises.
    pub fn take_interruption(&self) -> bool {
        let taken = self.inner.take_interruption(self.py);
        if taken {
            self.taken_back.set(true);
        }
        taken
    }

    /// Ends this unit and takes back an unraised interruption, so it cannot land in whatever the
    /// thread runs next.
    #[inline]
    pub fn end(self) -> UnitEnd {
        let unit = std::mem::ManuallyDrop::new(self);
        UnitEnd {
            interrupted: unit.inner.end_attached(unit.py, unit.taken_back.get()),
        }
    }
}

impl Drop for WorkUnit<'_, '_> {
    fn drop(&mut self) {
        self.inner.end_attached(self.py, self.taken_back.get());
    }
}

impl InterruptTarget {
    /// Raises `KeyboardInterrupt` at the unit's next bytecode boundary if it is still running,
    /// and records in the same GIL that the unit was interrupted, which its [`UnitEnd`] reports.
    ///
    /// `PyThreadState_SetAsyncExc` only finds threads of the current interpreter, so this enters
    /// the unit's interpreter and waits for its GIL; a worker inside one long C call keeps it
    /// waiting, as that call would ignore a signal. The interrupter is counted in before it
    /// enters, and both the unit's end and finalization wait for it, so the interpreter stays
    /// alive.
    ///
    /// A caller detached from an interpreter of its own briefly takes that interpreter's GIL on
    /// the way out, so it must not hold a lock that a thread of that interpreter may wait on
    /// with that GIL held.
    ///
    /// # Errors
    ///
    /// Returns an [`InterruptError`] when 255 interrupters of the unit are in flight already, when
    /// no thread state could be made, when the unit's thread state is gone, or once finalization
    /// has begun; nothing was set in any case.
    ///
    /// # Panics
    ///
    /// Panics if the calling thread is attached, not merely detached, to another interpreter.
    pub fn interrupt(&self) -> Result<Interruption, InterruptError> {
        // The hold is taken before the interrupter is counted, so a beginning finalization never
        // waits on an interrupter that waits on this hold.
        let Some(_not_finalizing) = FINALIZATION.try_hold() else {
            return Err(InterruptError::Finalizing);
        };
        // SAFETY: This reads the address of a built-in type object, static for the process.
        let exc = unsafe { ffi::PyExc_KeyboardInterrupt };
        self.inner
            .interrupt(self.unit, exc)
            .map_err(|unreached| match unreached {
                InterruptFailure::TooManyInFlight => InterruptError::TooManyInFlight,
                InterruptFailure::NoThreadState(failure) => InterruptError::NoThreadState(failure),
                InterruptFailure::ThreadGone => InterruptError::UnitThreadGone {
                    thread: self.inner.interpreter.thread,
                    interp: self.inner.interpreter.interp.id(),
                },
            })
    }
}

impl<H: Interpreter> SlotInner<H> {
    fn new(interpreter: H) -> Self {
        SlotInner {
            interpreter,
            word: mipyre_cpu::CacheLine(AtomicU64::new(SlotWord::NEW.bits())),
            idle_lock: Mutex::new(()),
            idle: Condvar::new(),
        }
    }

    fn load(&self) -> UnitState {
        SlotWord::from_bits(self.word.load(Ordering::Acquire)).decode()
    }

    /// Applies `step` to the slot atomically and returns the state before and after, or the state
    /// it saw, unchanged, when `step` gives none.
    #[inline]
    fn transition(&self, step: impl Fn(UnitState) -> Option<UnitState>) -> Result<(UnitState, UnitState), UnitState> {
        let mut seen = self.word.load(Ordering::Acquire);
        loop {
            let before = SlotWord::from_bits(seen).decode();
            let after = step(before).ok_or(before)?;
            match self.word.compare_exchange_weak(
                seen,
                SlotWord::encode(after).bits(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok((before, after)),
                Err(now) => seen = now,
            }
        }
    }

    /// Begins the next unit and returns it; a unit leaked rather than ended is ended first, as
    /// its drop would have, so its interruption and its interrupters are gone before this one.
    #[inline]
    fn begin(&self, gil: H::Gil<'_>) -> UnitId {
        loop {
            match self.transition(word::begin) {
                Ok((_, begun)) => return begun.unit,
                Err(_) => self.end_leaked(gil),
            }
        }
    }

    #[cold]
    fn end_leaked(&self, gil: H::Gil<'_>) {
        self.end_attached(gil, false);
    }

    /// Ends the unit, takes back an unraised interruption, waits for interrupters in flight to
    /// leave, and returns whether the unit was interrupted.
    ///
    /// An interrupter marks the unit only in this GIL after seeing it running, so none changes
    /// the phase this end leaves.
    #[inline]
    fn end_attached(&self, gil: H::Gil<'_>, taken_back: bool) -> bool {
        let (before, ended) = match self.transition(|state| word::end(state, taken_back)) {
            Ok(changed) => changed,
            Err(already) => return already.phase == (UnitPhase::Ended { interrupted: true }),
        };
        if before.phase == (UnitPhase::Running { outstanding: true }) {
            self.interpreter.clear(gil);
        }
        // Interrupters in flight need this GIL, and the interpreter may end right after this
        // returns.
        if before.in_flight > 0 {
            self.interpreter.without_gil(gil, || self.wait_until_idle());
        }
        ended.phase == (UnitPhase::Ended { interrupted: true })
    }

    /// Takes back an interruption set since the last call and returns whether there was one.
    fn take_interruption(&self, gil: H::Gil<'_>) -> bool {
        // An interrupter sets under the same GIL, so nothing is set between the step and the
        // clear.
        if self.transition(word::take_back).is_err() {
            return false;
        }
        self.interpreter.clear(gil);
        true
    }

    /// Interrupts `unit` with `exc` from another thread, as [`InterruptTarget::interrupt`] does.
    fn interrupt(&self, unit: UnitId, exc: H::Exception) -> Result<Interruption, InterruptFailure> {
        // Counting in before entering keeps the unit's end, and so the interpreter, waiting.
        let _pass = match self.admit(unit) {
            Ok(pass) => pass,
            Err(AdmitError::Full) => return Err(InterruptFailure::TooManyInFlight),
            Err(AdmitError::UnitOver) => return Ok(Interruption::UnitEnded),
        };
        self.interpreter
            .visit(|gil| {
                // A counted interrupter fixes the unit: only its end can have happened.
                if word::interrupt(self.load(), unit).is_none() {
                    return Ok(Interruption::UnitEnded);
                }
                self.interpreter
                    .raise(gil, exc)
                    .map_err(|ThreadGone| InterruptFailure::ThreadGone)?;
                match self.transition(|state| word::interrupt(state, unit)) {
                    Ok(_) => Ok(Interruption::Set),
                    Err(_) => {
                        self.interpreter.clear(gil);
                        Ok(Interruption::UnitEnded)
                    }
                }
            })
            .map_err(InterruptFailure::NoThreadState)?
    }

    /// Counts an interrupter of `unit` in; the pass keeps the interpreter alive until it drops.
    fn admit(&self, unit: UnitId) -> Result<InterrupterPermit<'_, H>, AdmitError> {
        let mut seen = self.word.load(Ordering::Acquire);
        loop {
            let counted = word::admit(SlotWord::from_bits(seen), unit)?;
            match self
                .word
                .compare_exchange_weak(seen, counted.bits(), Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(InterrupterPermit(self)),
                Err(now) => seen = now,
            }
        }
    }

    /// Waits until no interrupter is in flight; the caller must not hold the unit's GIL, which an
    /// interrupter needs to finish.
    #[cold]
    fn wait_until_idle(&self) {
        // The lock guards no data, so a poisoned one serves as well.
        let mut guard = self.idle_lock.lock().unwrap_or_else(PoisonError::into_inner);
        while self.load().in_flight > 0 {
            guard = self.idle.wait(guard).unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Notifies under the lock, so a waiter that checked the count and has not slept yet is woken.
    fn wake_waiters(&self) {
        let _guard = self.idle_lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.idle.notify_all();
    }
}

fn this_thread() -> std::ffi::c_ulong {
    // SAFETY: This has no precondition and reads the OS thread's id.
    unsafe { ffi::PyThread_get_thread_ident() }
}

/// In every case nothing was set, and the unit's [`UnitEnd`] does not report it interrupted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InterruptError {
    #[error(transparent)]
    NoThreadState(NewThreadStateError),
    #[error(
        "a unit of work of thread {thread} in interpreter {interp} has not ended, but that \
         thread has no thread state in it any more: nothing was interrupted"
    )]
    UnitThreadGone {
        /// The value is `PyThread_get_thread_ident()`.
        thread: std::ffi::c_ulong,
        interp: i64,
    },
    #[error("255 interruptions of this unit of work are in flight already: nothing more was set")]
    TooManyInFlight,
    #[error("the process is finalizing: no interpreter is entered to interrupt work any more")]
    Finalizing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("PyThreadState_New returned NULL (the runtime is out of memory)")]
pub struct NewThreadStateError;

#[cfg(test)]
mod test_gil;

#[cfg(test)]
#[cfg(any(not(sync_model), sync_shuttle))]
mod test_phase;

#[cfg(test)]
#[cfg(sync_model)]
mod model_interpreter;

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
    use crate::interp::{NewInterpreterError, Releasable, SubInterpreter};

    /// Runs on a real sub-interpreter, because the async-exception slot lives only in CPython.
    #[test]
    fn a_pending_interruption_is_taken_back_by_the_unit_end_or_drop() {
        Python::initialize();
        let serve = crate::python_thread("take-back-serve".into()).spawn(|| {
            let mut worker = SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                std::convert::identity,
                |_py| Ok(Nothing),
            )
            .expect("a sub-interpreter on its serving thread");
            worker.with_gil(|_attachment, _py| ()).expect("no panic");
            assert!(!worker.unit_slot_made(), "with_gil makes no unit slot");
            for dropped in [false, true] {
                let interrupted_then_ended = |_attachment: &mut Nothing, unit: WorkUnit<'_, '_>, py: Python<'_>| {
                    let target = unit.target();
                    let interrupting = crate::python_thread("interrupting".into())
                        .spawn(move || (target.interrupt(), target))
                        .expect("spawn the interrupting thread");
                    let (interrupted, target) = py.detach(|| interrupting.join().expect("the interruption ends"));
                    assert_eq!(interrupted, Ok(Interruption::Set));
                    if dropped {
                        drop(unit);
                    } else {
                        assert!(unit.end().interrupted, "the end did not report the interruption");
                    }
                    assert_eq!(
                        target.inner.load().phase,
                        UnitPhase::Ended { interrupted: true },
                        "the interruption was not recorded"
                    );
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
        });
        serve
            .expect("spawn the worker thread")
            .join()
            .expect("the worker thread's assertions hold");
    }

    /// An interruption entering the ended interpreter would be a use-after-free of its
    /// `PyInterpreterState`.
    #[test]
    fn a_leaked_unit_is_ended_by_its_workers_end() {
        Python::initialize();
        let target = crate::python_thread("leak-serve".into())
            .spawn(|| {
                let mut worker = SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                    std::convert::identity,
                    |_py| Ok(Nothing),
                )
                .expect("a sub-interpreter on its serving thread");
                let target = worker
                    .with_unit(|_attachment, unit, _py| {
                        let target = unit.target();
                        std::mem::forget(unit);
                        target
                    })
                    .expect("no panic in the unit");
                assert_eq!(target.inner.load().phase, UnitPhase::Running { outstanding: false });
                drop(worker);
                target
            })
            .expect("spawn the worker thread")
            .join()
            .expect("the worker thread ends");
        assert_eq!(target.inner.load().phase, UnitPhase::Ended { interrupted: false });
        assert_eq!(target.interrupt(), Ok(Interruption::UnitEnded));
    }

    /// The same in every build: the next unit ends the leaked one, and an interruption aimed at
    /// the leaked one neither lands in the next nor is reported by it.
    #[test]
    fn a_leaked_unit_is_ended_by_the_next_units_begin() {
        Python::initialize();
        crate::python_thread("leak-begin-serve".into())
            .spawn(|| {
                let mut worker = SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                    std::convert::identity,
                    |_py| Ok(Nothing),
                )
                .expect("a sub-interpreter on its serving thread");
                let leaked = worker
                    .with_unit(|_attachment, unit, py| {
                        let target = unit.target();
                        let interrupting = target.clone();
                        let set = py.detach(move || {
                            crate::python_thread("interrupting".into())
                                .spawn(move || interrupting.interrupt())
                                .expect("spawn the interrupting thread")
                                .join()
                                .expect("the interruption ends")
                        });
                        assert_eq!(set, Ok(Interruption::Set));
                        std::mem::forget(unit);
                        target
                    })
                    .expect("no panic in the unit");
                let next = worker
                    .with_unit(|_attachment, unit, py| {
                        let ran = py.eval(c"1 + 1", None, None).map(|two| two.to_string());
                        (ran.map_err(|e| e.to_string()), unit.end())
                    })
                    .expect("no panic in the unit");
                assert_eq!(next, (Ok("2".to_string()), UnitEnd { interrupted: false }));
                assert_eq!(leaked.interrupt(), Ok(Interruption::UnitEnded));
            })
            .expect("spawn the worker thread")
            .join()
            .expect("the worker thread's assertions hold");
    }

    struct Nothing;

    impl Releasable for Nothing {
        fn release(self, _py: Python<'_>) {}
    }
}
