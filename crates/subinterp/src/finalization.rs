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
//! it inside CPython. So this also counts the interpreters threads still own, and a process whose
//! threads still own some when it exits must exit without finalizing.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

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
    /// Counts the sub-interpreters whose threads may still enter them.
    owned: Mutex<usize>,
    none_owned: Condvar,
}

/// Stands for one count of [`Finalization::own`]; only `own` makes one, and giving it up consumes
/// it, so a count is never taken back twice.
#[must_use = "an owned interpreter is counted until it is given up"]
pub(crate) struct Ownership(());

/// Keeps finalization from beginning while it lives.
pub(crate) struct FinalizationHold<'a>(
    #[expect(dead_code, reason = "held for its drop, which lets the finalization begin")]
    RwLockReadGuard<'a, Phase>,
);

pub(crate) static FINALIZATION: Finalization = Finalization::new();

impl Finalization {
    const fn new() -> Self {
        Finalization {
            phase: parking_lot::const_rwlock(Phase::NotBegun),
            armed: AtomicBool::new(false),
            owned: parking_lot::const_mutex(0),
            none_owned: Condvar::new(),
        }
    }

    /// Counts a sub-interpreter its thread now owns, until the returned token is given up.
    pub(crate) fn own(&self) -> Ownership {
        *self.owned.lock() += 1;
        Ownership(())
    }

    /// Counts one fewer owned sub-interpreter: its thread ended it or gave it up, and never
    /// enters it again.
    pub(crate) fn give_up(&self, owned: Ownership) {
        let Ownership(()) = owned;
        let mut count = self.owned.lock();
        // `owned` was counted by `own` and is consumed here, so the count is at least 1.
        *count -= 1;
        if *count == 0 {
            self.none_owned.notify_all();
        }
    }

    /// Waits until no thread owns a sub-interpreter, or until `deadline`, and returns how many
    /// are owned then.
    pub(crate) fn owned_by(&self, deadline: Instant) -> usize {
        let mut owned = self.owned.lock();
        while *owned > 0 {
            if self.none_owned.wait_until(&mut owned, deadline).timed_out() {
                break;
            }
        }
        *owned
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
        if self.armed.load(Ordering::Acquire) {
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
    fn owned_interpreters_are_counted_at_a_deadline_already_past() {
        let finalization = Finalization::new();
        let past = Instant::now();
        assert_eq!(finalization.owned_by(past), 0);
        let first = finalization.own();
        let _second = finalization.own();
        finalization.give_up(first);
        assert_eq!(finalization.owned_by(past), 1);
    }

    /// The deadline is an hour away, so the wait returns only because the last one was given up.
    #[test]
    fn the_wait_ends_when_the_last_owned_interpreter_is_given_up() {
        let finalization = Arc::new(Finalization::new());
        let owned = finalization.own();
        let (waiting, is_waiting) = mpsc::channel();
        let waiter = {
            let finalization = Arc::clone(&finalization);
            std::thread::spawn(move || {
                waiting.send(()).unwrap();
                finalization.owned_by(Instant::now() + std::time::Duration::from_secs(3600))
            })
        };
        is_waiting.recv().unwrap();
        finalization.give_up(owned);
        assert_eq!(waiter.join().unwrap(), 0);
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
