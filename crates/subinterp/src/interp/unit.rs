//! Lets another thread interrupt one unit of work of a worker, and ends the unit so that no
//! interruption lands after it.

use std::marker::PhantomData;
use std::sync::Arc;

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::InterpreterHandle;

use crate::finalization::FINALIZATION;
use crate::sync::{AtomicU64, Condvar, Mutex, Ordering};

/// One unit of work of a worker, which another thread may interrupt through its
/// [`InterruptTarget`].
///
/// It cannot leave [`SubInterpreter::with_unit`](super::SubInterpreter::with_unit)'s closure and
/// is neither `Send` nor `Sync`, so wherever it is ended or dropped the thread is attached to the
/// unit's own interpreter. Ending or dropping it takes back an interruption that was set and not
/// yet raised, and none lands after.
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
///         py.detach(|| ());
///         unit.end().interrupted
///     });
///     ran.is_ok_and(|interrupted| !interrupted)
/// }
/// ```
#[must_use = "a unit of work ends when it is ended or dropped"]
pub struct WorkUnit<'s, 'py> {
    state: &'s Arc<UnitState>,
    run: RunId,
    py: Python<'py>,
}

/// How one run of a [`WorkUnit`] ended: `interrupted` says whether an interruption was set
/// during it, even one the worker took back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnitEnd {
    pub interrupted: bool,
}

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
pub(super) struct UnitSlot(
    Arc<UnitState>,
    // The take-back relies on every run being begun on the thread the slot was made on.
    PhantomData<*const ()>,
);

impl UnitSlot {
    pub(super) fn new(py: Python<'_>) -> UnitSlot {
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
    pub(super) fn begin<'py>(&self, py: Python<'py>) -> WorkUnit<'_, 'py> {
        let (run, before) = self.0.begin_run();
        debug_assert!(
            UnitPhase::from_word(before).has_ended(),
            "a slot began a run while its previous run had not ended"
        );
        WorkUnit {
            state: &self.0,
            run,
            py,
        }
    }

    /// Ends the last run as a drop would, if a leaked [`WorkUnit`] left it unended.
    pub(super) fn end_leaked_run(self, py: Python<'_>) {
        if !self.0.phase().has_ended() {
            self.0.end_attached(py);
        }
    }
}

impl std::fmt::Debug for WorkUnit<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkUnit")
            .field("state", &self.state)
            .field("run", &self.run)
            .finish_non_exhaustive()
    }
}

impl WorkUnit<'_, '_> {
    pub fn target(&self) -> InterruptTarget {
        InterruptTarget {
            state: Arc::clone(self.state),
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
    pub fn end(self) -> UnitEnd {
        let unit = std::mem::ManuallyDrop::new(self);
        UnitEnd {
            interrupted: unit.state.end_attached(unit.py).was_interrupted(),
        }
    }
}

impl Drop for WorkUnit<'_, '_> {
    fn drop(&mut self) {
        let unit = self.state;
        let py = self.py;
        // Only the worker ends its unit, so an unended phase stays unended while this runs.
        let before = unit.end_run();
        // Not finding this thread breaks an invariant that `end` panics on, but a drop may run
        // during an unwind, where a second panic aborts, so this logs it instead.
        if UnitPhase::from_word(before).exception_may_be_pending() && !unit.clear_raise(py) {
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
        unit.wait_for_visits(py, before);
    }
}

impl InterruptTarget {
    /// Raises `KeyboardInterrupt` at the unit's next bytecode boundary if it is still running,
    /// and records in the same GIL that the run was interrupted, which its [`UnitEnd`] reports.
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

    /// Ends the run, takes back an unraised interruption, waits for visits in flight to leave, and
    /// returns the phase the run ended in.
    ///
    /// `interrupt` steps the phase only in this GIL after seeing the run not ended, so no visit
    /// changes the phase this end leaves.
    #[inline]
    fn end_attached(&self, gil: H::Gil<'_>) -> UnitPhase {
        // `interrupt` sets under the same GIL, so nothing is set between the phase step and the
        // take-back.
        let before = self.end_run();
        let phase = UnitPhase::from_word(before);
        if phase.exception_may_be_pending() {
            self.take_back_raise(gil);
        }
        // Visits in flight need this GIL, and the interpreter may end right after this returns.
        self.wait_for_visits(gil, before);
        phase.end()
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

    /// Waits with the GIL released until no interruption is in flight, without locking if none was
    /// counted in by `ended_from`, the word the end replaced: a visit counted in after the end sees
    /// the run ended and leaves without entering.
    #[inline]
    fn wait_for_visits(&self, gil: H::Gil<'_>, ended_from: u64) {
        if in_flight_of(ended_from) == 0 {
            return;
        }
        self.host.without_gil(gil, || self.wait_until_idle());
    }

    fn phase(&self) -> UnitPhase {
        UnitPhase::from_word(self.state.load(Ordering::Acquire))
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

    /// Ends the run, keeping the run and the count, and returns the word before.
    fn end_run(&self) -> u64 {
        match self.state.fetch_update(Ordering::AcqRel, Ordering::Acquire, |word| {
            Some((word & !PHASE_MASK) | UnitPhase::from_word(word).end() as u64)
        }) {
            Ok(before) | Err(before) => before,
        }
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
    #[cold]
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
    #[cold]
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

/// In every case nothing was set, and the run's [`UnitEnd`] does not report it interrupted.
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

#[cfg(test)]
mod test_gil;

#[cfg(test)]
mod test_phase;

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
    use crate::interp::{NewInterpreterError, Releasable, SubInterpreter};

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
                let interrupted_then_ended = |_held: &mut Nothing, unit: WorkUnit<'_, '_>, py: Python<'_>| {
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
                        assert!(unit.end().interrupted, "the end did not report the interruption");
                    }
                    assert!(
                        target.state.phase().was_interrupted(),
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
                assert!(!target.state.phase().has_ended(), "a leaked unit has not ended");
                worker.end_on_own_thread();
                target
            })
            .expect("spawn the worker thread")
            .join()
            .expect("the worker thread ends");
        assert!(
            target.state.phase().has_ended(),
            "the worker's end ended its leaked unit"
        );
        assert_eq!(target.interrupt(), Ok(()));
        assert!(!target.state.phase().was_interrupted(), "nothing was interrupted");
    }

    struct Nothing;

    impl Releasable for Nothing {
        fn release(self, _py: Python<'_>) {}
    }
}
