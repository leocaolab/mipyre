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
enum Phase {
    NotBegun,
    Begun,
}

pub(crate) struct Finalization {
    phase: RwLock<Phase>,
    armed: AtomicBool,
    /// The sub-interpreters whose threads may still enter them.
    owned: Mutex<Owners>,
    /// Wakes the exit when the last owner gives its interpreter up, or one enters Python.
    exit_news: Condvar,
}

/// Each owned sub-interpreter, by the number [`Finalization::own`] gave it, with its thread's
/// word.
struct Owners {
    next: u64,
    owners: Vec<(u64, ThreadEntry)>,
    /// Counts the units begun on threads the exit had found outside Python.
    entered_while_watched: usize,
}

/// Stands for one entry of [`Finalization::own`]; only `own` makes one, and giving it up consumes
/// it, so an entry is never taken out twice.
#[must_use = "an owned interpreter is kept until it is given up"]
pub(crate) struct Ownership(u64);

/// What the process does at exit, given what its threads that own interpreters are doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExitDecision {
    /// No thread runs Python, and none will again: once the last ends its interpreter,
    /// `Py_Finalize` may run.
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

/// Decides the exit from how many threads run Python (found in a unit, or entering one since)
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
    #[expect(dead_code, reason = "held for its drop, which lets the finalization begin")] RwLockReadGuard<'a, Phase>,
);

pub(crate) static FINALIZATION: Finalization = Finalization::new();

impl Finalization {
    const fn new() -> Self {
        Finalization {
            phase: parking_lot::const_rwlock(Phase::NotBegun),
            armed: AtomicBool::new(false),
            owned: parking_lot::const_mutex(Owners {
                next: 0,
                owners: Vec::new(),
                entered_while_watched: 0,
            }),
            exit_news: Condvar::new(),
        }
    }

    /// Keeps a sub-interpreter the calling thread now owns, with the thread's entry word, until
    /// the returned token is given up.
    pub(crate) fn own(&self) -> Ownership {
        let entry = ThreadEntry::this_thread();
        let mut owned = self.owned.lock();
        let id = owned.next;
        owned.next += 1;
        owned.owners.push((id, entry));
        Ownership(id)
    }

    /// Takes the sub-interpreter out: its thread ended it or gave it up, and never enters it
    /// again.
    pub(crate) fn give_up(&self, owned: Ownership) {
        let Ownership(id) = owned;
        let mut owners = self.owned.lock();
        owners.owners.retain(|(owner, _)| *owner != id);
        if owners.owners.is_empty() {
            self.exit_news.notify_all();
        }
    }

    /// Tells the exit that a thread it found outside Python entered it.
    pub(crate) fn python_entered_while_watched(&self) {
        self.owned.lock().entered_while_watched += 1;
        self.exit_news.notify_all();
    }

    /// Finds what every thread that owns an interpreter is doing and decides the exit; when none
    /// runs Python it waits, without a deadline, until the last has ended its interpreter
    /// ([`ExitDecision::Finalize`]) or one enters Python.
    pub(crate) fn decide_exit(&self) -> ExitDecision {
        let mut owned = self.owned.lock();
        let running = owned
            .owners
            .iter()
            .filter(|(_, entry)| entry.mark() == ThreadAtExit::RunningPython)
            .count();
        loop {
            match exit_step(running + owned.entered_while_watched, owned.owners.len()) {
                ExitStep::Decided(decision) => return decision,
                ExitStep::Wait => self.exit_news.wait(&mut owned),
            }
        }
    }

    /// Returns a hold that keeps finalization from beginning, or `None` once it has begun.
    ///
    /// A hold is given even while a beginning is waiting, so a thread that already holds one
    /// can nest another without deadlocking against that beginning.
    pub(crate) fn not_begun(&self) -> Option<FinalizationHold<'_>> {
        let phase = self.phase.read_recursive();
        (*phase == Phase::NotBegun).then_some(FinalizationHold(phase))
    }

    /// Waits for every hold to be dropped and refuses new ones from then on.
    pub(crate) fn begin(&self) {
        *self.phase.write() = Phase::Begun;
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
    /// Returns the Python error when the callback cannot be built or `atexit.register` fails.
    pub(crate) fn arm(&'static self, py: Python<'_>) -> PyResult<()> {
        if self.armed() {
            return Ok(());
        }
        let begin = PyCFunction::new_closure(
            py,
            Some(c"mipyre_finalization_begins"),
            Some(c"Worker threads stop ending their interpreters: Py_Finalize ends what is left."),
            move |args, _kwargs| args.py().detach(|| self.begin()),
        )?;
        py.import("atexit")?.call_method1("register", (begin,))?;
        self.armed.store(true, Ordering::Release);
        Ok(())
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

    /// Owns an interpreter on a thread of its own, whose word is `word`, until told to give it up.
    fn owner(finalization: &Arc<Finalization>, word: usize) -> (mpsc::Sender<()>, std::thread::JoinHandle<()>) {
        let (owning, owns) = mpsc::channel();
        let (give_up, told) = mpsc::channel::<()>();
        let finalization = Arc::clone(finalization);
        let thread = std::thread::spawn(move || {
            crate::interp::ENTRY.with(|e| e.store(std::ptr::without_provenance_mut(word), Ordering::Relaxed));
            let owned = finalization.own();
            owning.send(()).unwrap();
            told.recv().unwrap();
            finalization.give_up(owned);
        });
        owns.recv().unwrap();
        (give_up, thread)
    }

    /// Nothing but the last give-up ends the wait: there is no deadline.
    #[test]
    fn the_exit_waits_for_the_last_owner_when_none_runs_python() {
        let finalization = Arc::new(Finalization::new());
        let (give_up, thread) = owner(&finalization, 0);
        let (waiting, is_waiting) = mpsc::channel();
        let exit = {
            let finalization = Arc::clone(&finalization);
            std::thread::spawn(move || {
                waiting.send(()).unwrap();
                finalization.decide_exit()
            })
        };
        is_waiting.recv().unwrap();
        give_up.send(()).unwrap();
        thread.join().unwrap();
        assert_eq!(exit.join().unwrap(), ExitDecision::Finalize);
    }

    /// The owner enters Python while the exit waits for it: the exit decides at once.
    #[test]
    fn an_owner_entering_python_while_the_exit_waits_decides_the_exit() {
        let finalization = Arc::new(Finalization::new());
        let (give_up, thread) = owner(&finalization, 0);
        let (waiting, is_waiting) = mpsc::channel();
        let exit = {
            let finalization = Arc::clone(&finalization);
            std::thread::spawn(move || {
                waiting.send(()).unwrap();
                finalization.decide_exit()
            })
        };
        is_waiting.recv().unwrap();
        finalization.python_entered_while_watched();
        assert_eq!(
            exit.join().unwrap(),
            ExitDecision::ExitWithoutFinalizing {
                running_python: NonZeroUsize::new(1).unwrap()
            }
        );
        give_up.send(()).unwrap();
        thread.join().unwrap();
    }

    /// The owner is never told to give up before the decision returns, so it returns without
    /// waiting.
    #[test]
    fn the_exit_does_not_wait_while_a_thread_runs_python() {
        let finalization = Arc::new(Finalization::new());
        let (give_up_idle, idle) = owner(&finalization, 0);
        let (give_up_running, running) = owner(&finalization, crate::entry::IN_PYTHON.addr());
        assert_eq!(
            finalization.decide_exit(),
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
    fn an_end_is_let_in_before_finalization_begins_and_not_after() {
        let finalization = Finalization::new();
        assert!(finalization.not_begun().is_some());
        finalization.begin();
        assert!(finalization.not_begun().is_none());
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
                let hold = finalization.not_begun().expect("not begun yet");
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
        assert!(finalization.not_begun().is_none());
    }
}
