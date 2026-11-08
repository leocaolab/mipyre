//! Makes sure each sub-interpreter is ended by its own thread or by `Py_Finalize`, never both.
//!
//! `Py_Finalize` (CPython 3.14) ends every sub-interpreter still alive right after the main
//! interpreter's `atexit` callbacks. A worker thread ending its interpreter at the same moment
//! marks the thread state finalization just made as shutting down, and the main thread then
//! hangs for good in `take_gil`. So a thread ends its worker only while holding a
//! [`FinalizationHold`], and an `atexit` callback waits for every hold before finalization goes
//! on; a thread that comes to its end later leaves its interpreter to `Py_Finalize`.
//!
//! A thread still serving its interpreter when `Py_Finalize` ends it would restore a thread state
//! finalization deleted, a use-after-free no hold can prevent: a handler in `time.sleep` restores
//! it inside CPython. So this also keeps the threads that own interpreters and, at exit, what
//! each is doing ([`crate::entry`]): a thread running Python then may never stop, and the process
//! must exit without finalizing; otherwise the exit waits for the last to end its interpreter,
//! unless one enters Python meanwhile.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::entry::ThreadAtExit;
use crate::interp::ThreadEntry;

use parking_lot::{Condvar, Mutex, RwLock, RwLockReadGuard};
use pyo3::prelude::*;
use pyo3::types::PyCFunction;

/// The phase only moves forward, from `NotBegun` to `Begun`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FinalizationPhase {
    NotBegun,
    Begun,
}

pub(crate) struct Finalization {
    phase: RwLock<FinalizationPhase>,
    armed: AtomicBool,
    /// The sub-interpreters whose threads may still enter them.
    owned: Mutex<Owners>,
    /// Wakes the exit when an owner gives its interpreter up, or one enters Python.
    owners_changed: Condvar,
}

/// Each owned sub-interpreter, by the number [`Finalization::own`] gave it, with its thread's
/// word.
struct Owners {
    next: u64,
    owners: Vec<(u64, ThreadEntry)>,
    exit: ExitPhase,
}

/// Whether the process's exit has started deciding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExitPhase {
    NotStarted,
    /// `entered` counts the threads that entered Python after the exit found them outside it.
    Watching {
        entered: usize,
    },
}

/// Stands for one entry of [`Finalization::own`]: dropping it takes the entry out, once.
#[must_use = "an owned interpreter is kept until this is dropped"]
pub(crate) struct Ownership {
    finalization: &'static Finalization,
    id: u64,
}

/// Why a thread may not take on an interpreter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnRefusal {
    /// The process's exit started deciding: an interpreter created now could run Python after
    /// the exit decided that none would.
    ExitStarted,
    /// The thread is ending, its thread-locals being destroyed, so its word could not be taken
    /// out of the list before it is freed.
    ThreadEnding,
}

/// What the process does at exit, given what its threads that own interpreters are doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExitDecision {
    /// No thread runs Python, and none will again: every interpreter has ended and no new one
    /// is created, so `Py_Finalize` may run.
    Finalize,
    /// Threads run Python, which may never return; finalizing would free their interpreters
    /// under them.
    ExitWithoutFinalizing { running_python: NonZeroUsize },
}

/// What the exit does next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExitStep {
    Decided(ExitDecision),
    /// Every thread that owns an interpreter is outside Python: wait for one to end it or to
    /// enter Python.
    Wait,
}

/// Decides the exit from how many threads run Python (found running it, or entering it since)
/// and how many still own an interpreter.
fn exit_step(running_python: usize, owned: usize) -> ExitStep {
    match NonZeroUsize::new(running_python) {
        Some(running_python) => ExitStep::Decided(ExitDecision::ExitWithoutFinalizing { running_python }),
        None if owned == 0 => ExitStep::Decided(ExitDecision::Finalize),
        None => ExitStep::Wait,
    }
}

/// Keeps finalization from beginning while it lives.
pub(crate) struct FinalizationHold<'a>(
    #[expect(dead_code, reason = "held for its drop, which lets the finalization begin")]
    RwLockReadGuard<'a, FinalizationPhase>,
);

/// The registered `atexit` callback raised, with its exception as CPython gave it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("registering the finalization hook with atexit raised {exception_type}: {message}")]
pub struct RegisterHookError {
    /// The exception's type, as `type(e)` prints.
    pub exception_type: String,
    /// The exception, as `str(e)` prints.
    pub message: String,
}

pub(crate) static FINALIZATION: Finalization = Finalization::new();

impl Finalization {
    const fn new() -> Self {
        Finalization {
            phase: parking_lot::const_rwlock(FinalizationPhase::NotBegun),
            armed: AtomicBool::new(false),
            owned: parking_lot::const_mutex(Owners {
                next: 0,
                owners: Vec::new(),
                exit: ExitPhase::NotStarted,
            }),
            owners_changed: Condvar::new(),
        }
    }

    /// Keeps a sub-interpreter the calling thread is about to own, with the thread's entry word,
    /// until the returned token drops.
    ///
    /// # Errors
    ///
    /// Refuses once the exit started deciding, or while the thread is ending.
    pub(crate) fn own(&'static self) -> Result<Ownership, OwnRefusal> {
        crate::interp::forget_owners_at_thread_exit(self).map_err(|_| OwnRefusal::ThreadEnding)?;
        let entry = ThreadEntry::this_thread();
        let mut owned = self.owned.lock();
        if owned.exit != ExitPhase::NotStarted {
            return Err(OwnRefusal::ExitStarted);
        }
        let id = owned.next;
        owned.next += 1;
        owned.owners.push((id, entry));
        Ok(Ownership { finalization: self, id })
    }

    /// Takes out every interpreter the thread whose word is `entry` owns: the thread is ending,
    /// and its word with it, whether or not it ended them.
    pub(crate) fn thread_ended(&self, entry: &ThreadEntry) {
        let mut owners = self.owned.lock();
        owners.owners.retain(|(_, owner)| !owner.is(entry));
        self.owners_changed.notify_all();
    }

    /// Tells the exit that a thread it found outside Python entered it.
    pub(crate) fn python_entered_while_watched(&self) {
        let mut owned = self.owned.lock();
        if let ExitPhase::Watching { entered } = &mut owned.exit {
            *entered += 1;
        }
        self.owners_changed.notify_all();
    }

    /// Finds what every thread that owns an interpreter is doing and decides the exit; when none
    /// runs Python it waits, without a deadline, until the last has ended its interpreter
    /// ([`ExitDecision::Finalize`]) or one enters Python. From the first call on, no interpreter
    /// is created.
    pub(crate) fn decide_exit(&self) -> ExitDecision {
        let mut owned = self.owned.lock();
        if owned.exit == ExitPhase::NotStarted {
            owned.exit = ExitPhase::Watching { entered: 0 };
        }
        let running = owned
            .owners
            .iter()
            .filter(|(_, entry)| entry.mark() == ThreadAtExit::RunningPython)
            .count();
        loop {
            let entered = match owned.exit {
                ExitPhase::Watching { entered } => entered,
                ExitPhase::NotStarted => 0,
            };
            match exit_step(running + entered, owned.owners.len()) {
                ExitStep::Decided(decision) => return decision,
                ExitStep::Wait => self.owners_changed.wait(&mut owned),
            }
        }
    }

    /// Returns a hold that keeps finalization from beginning, or `None` once it has begun.
    ///
    /// A hold is given even while a beginning is waiting, so a thread that already holds one
    /// can nest another without deadlocking against that beginning.
    pub(crate) fn try_hold(&self) -> Option<FinalizationHold<'_>> {
        let phase = self.phase.read_recursive();
        (*phase == FinalizationPhase::NotBegun).then_some(FinalizationHold(phase))
    }

    /// Waits for every hold to be dropped and refuses new ones from then on.
    pub(crate) fn begin(&self) {
        *self.phase.write() = FinalizationPhase::Begun;
    }

    /// Returns whether [`Self::arm`] has registered the `atexit` callback.
    pub(crate) fn armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }

    /// Registers [`Self::begin`] once as an `atexit` callback of the main interpreter, which
    /// `py` must be attached to.
    ///
    /// The callback waits with the main GIL released, because a Python thread of the main
    /// interpreter may need it meanwhile. The flag is not a lock: creations are serialized by
    /// the main GIL, and a second registration would only begin twice, which is harmless.
    ///
    /// # Errors
    ///
    /// Returns the exception when the callback cannot be built or `atexit.register` raises.
    pub(crate) fn arm(&'static self, py: Python<'_>) -> Result<(), RegisterHookError> {
        if self.armed() {
            return Ok(());
        }
        self.register(py).map_err(|e| RegisterHookError {
            exception_type: e.get_type(py).to_string(),
            message: e.value(py).to_string(),
        })?;
        self.armed.store(true, Ordering::Release);
        Ok(())
    }

    fn register(&'static self, py: Python<'_>) -> PyResult<()> {
        let begin = PyCFunction::new_closure(
            py,
            Some(c"mipyre_finalization_begins"),
            Some(c"Worker threads stop ending their interpreters: Py_Finalize ends what is left."),
            move |args, _kwargs| args.py().detach(|| self.begin()),
        )?;
        py.import("atexit")?.call_method1("register", (begin,))?;
        Ok(())
    }
}

impl Drop for Ownership {
    fn drop(&mut self) {
        let id = self.id;
        let mut owners = self.finalization.owned.lock();
        owners.owners.retain(|(owner, _)| *owner != id);
        self.finalization.owners_changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::{mpsc, Arc};

    #[test]
    fn one_thread_running_python_decides_at_once_and_none_waits_for_the_last_owner() {
        let exit = |n| {
            ExitStep::Decided(ExitDecision::ExitWithoutFinalizing {
                running_python: NonZeroUsize::new(n).unwrap(),
            })
        };
        assert_eq!(exit_step(0, 0), ExitStep::Decided(ExitDecision::Finalize));
        assert_eq!(exit_step(0, 1), ExitStep::Wait);
        assert_eq!(exit_step(0, 7), ExitStep::Wait);
        assert_eq!(exit_step(1, 1), exit(1));
        assert_eq!(exit_step(1, 0), exit(1));
        assert_eq!(exit_step(2, 5), exit(2));
    }

    /// Owns an interpreter on a thread of its own, whose word is `word`, until told to give it up;
    /// `told` is set before it gives it up.
    fn owner(
        finalization: &'static Finalization,
        word: usize,
        told: &'static AtomicBool,
    ) -> (mpsc::Sender<()>, std::thread::JoinHandle<()>) {
        let (owning, owns) = mpsc::channel();
        let (give_up, give_up_told) = mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            crate::interp::ENTRY.with(|e| e.store(std::ptr::without_provenance_mut(word), Ordering::Relaxed));
            let owned = finalization.own().expect("the exit has not started");
            owning.send(()).unwrap();
            give_up_told.recv().unwrap();
            told.store(true, Ordering::SeqCst);
            drop(owned);
        });
        owns.recv().unwrap();
        (give_up, thread)
    }

    /// Starts the exit on a thread of its own once it has begun deciding, and returns its decision
    /// with whether `before` was set when the decision returned.
    fn exit_started(
        finalization: &'static Finalization,
        before: &'static AtomicBool,
    ) -> std::thread::JoinHandle<(ExitDecision, bool)> {
        let exit = std::thread::spawn(move || {
            let decision = finalization.decide_exit();
            (decision, before.load(Ordering::SeqCst))
        });
        while finalization.owned.lock().exit == ExitPhase::NotStarted {
            std::thread::yield_now();
        }
        exit
    }

    /// The decision returns only after the owner was told to give up: an exit that did not wait
    /// would return with the flag unset.
    #[test]
    fn the_exit_waits_for_the_last_owner_when_none_runs_python() {
        static FINALIZATION: Finalization = Finalization::new();
        static GIVEN_UP: AtomicBool = AtomicBool::new(false);
        let (give_up, thread) = owner(&FINALIZATION, 0, &GIVEN_UP);
        let exit = exit_started(&FINALIZATION, &GIVEN_UP);
        give_up.send(()).unwrap();
        thread.join().unwrap();
        assert_eq!(exit.join().unwrap(), (ExitDecision::Finalize, true));
    }

    /// The decision returns only after the thread entered Python, and with the owner still there.
    #[test]
    fn an_owner_entering_python_while_the_exit_waits_decides_the_exit() {
        static FINALIZATION: Finalization = Finalization::new();
        static GIVEN_UP: AtomicBool = AtomicBool::new(false);
        static ENTERED: AtomicBool = AtomicBool::new(false);
        let (give_up, thread) = owner(&FINALIZATION, 0, &GIVEN_UP);
        let exit = exit_started(&FINALIZATION, &ENTERED);
        ENTERED.store(true, Ordering::SeqCst);
        FINALIZATION.python_entered_while_watched();
        let decided = exit.join().unwrap();
        assert!(!GIVEN_UP.load(Ordering::SeqCst));
        give_up.send(()).unwrap();
        thread.join().unwrap();
        assert_eq!(
            decided,
            (
                ExitDecision::ExitWithoutFinalizing {
                    running_python: NonZeroUsize::new(1).unwrap()
                },
                true
            )
        );
    }

    /// The owner is never told to give up before the decision returns, so it returns without
    /// waiting.
    #[test]
    fn the_exit_does_not_wait_while_a_thread_runs_python() {
        static FINALIZATION: Finalization = Finalization::new();
        static GIVEN_UP: AtomicBool = AtomicBool::new(false);
        let (give_up_idle, idle) = owner(&FINALIZATION, 0, &GIVEN_UP);
        let (give_up_running, running) = owner(&FINALIZATION, crate::entry::IN_PYTHON, &GIVEN_UP);
        assert_eq!(
            FINALIZATION.decide_exit(),
            ExitDecision::ExitWithoutFinalizing {
                running_python: NonZeroUsize::new(1).unwrap()
            }
        );
        for (give_up, thread) in [(give_up_idle, idle), (give_up_running, running)] {
            give_up.send(()).unwrap();
            thread.join().unwrap();
        }
    }

    #[test]
    fn no_interpreter_is_owned_once_the_exit_started() {
        static FINALIZATION: Finalization = Finalization::new();
        assert_eq!(FINALIZATION.decide_exit(), ExitDecision::Finalize);
        assert!(matches!(FINALIZATION.own(), Err(OwnRefusal::ExitStarted)));
    }

    /// A thread that ends without giving its interpreter up (its worker leaked with
    /// `mem::forget`) leaves no entry behind, so the exit never reads its freed word.
    #[test]
    fn a_thread_that_ends_owning_an_interpreter_leaves_no_entry() {
        static FINALIZATION: Finalization = Finalization::new();
        std::thread::spawn(|| std::mem::forget(FINALIZATION.own().expect("the exit has not started")))
            .join()
            .unwrap();
        assert!(FINALIZATION.owned.lock().owners.is_empty());
        assert_eq!(FINALIZATION.decide_exit(), ExitDecision::Finalize);
    }

    #[test]
    fn an_end_is_let_in_before_finalization_begins_and_not_after() {
        let finalization = Finalization::new();
        assert!(finalization.try_hold().is_some());
        finalization.begin();
        assert!(finalization.try_hold().is_none());
    }

    #[test]
    fn finalization_begins_only_after_the_end_in_progress_is_done() {
        let finalization = Arc::new(Finalization::new());
        let ended = Arc::new(AtomicBool::new(false));
        let (entered, has_entered) = mpsc::channel();
        let (release, released) = mpsc::channel::<()>();
        let ender = {
            let finalization = Arc::clone(&finalization);
            let ended = Arc::clone(&ended);
            std::thread::spawn(move || {
                let hold = finalization.try_hold().expect("not begun yet");
                entered.send(()).unwrap();
                released.recv().unwrap();
                ended.store(true, Ordering::SeqCst);
                drop(hold);
            })
        };
        has_entered.recv().unwrap();
        let beginner = {
            let finalization = Arc::clone(&finalization);
            let ended = Arc::clone(&ended);
            std::thread::spawn(move || {
                finalization.begin();
                ended.load(Ordering::SeqCst)
            })
        };
        release.send(()).unwrap();
        ender.join().unwrap();
        assert!(
            beginner.join().unwrap(),
            "the beginning returned before the end in progress was done"
        );
        assert!(finalization.try_hold().is_none());
    }
}
