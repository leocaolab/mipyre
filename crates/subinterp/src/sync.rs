//! Selects the real atomic and lock in every build, Loom's under `--cfg sync_model` and Shuttle's
//! under `--cfg sync_model --cfg sync_shuttle`, so the model checker runs the same slot code; only
//! the names switch, nothing wraps the real types. The cfg swaps them in every package of the
//! build, so it is set only to run the models.
//!
//! Process-wide state in a `static` (`finalization.rs`, `isolation.rs`) names parking_lot's and
//! std's types itself: Loom has no `const` constructor, and a static outlives the model's
//! executions.

#[cfg(not(sync_model))]
pub(crate) use parking_lot::{Condvar, Mutex};
#[cfg(not(sync_model))]
pub(crate) use std::sync::atomic::AtomicU64;

/// The model checker whose names a model build takes: Loom's, or Shuttle's under `sync_shuttle`.
/// The two have std's calls, so one wrapper below serves both.
#[cfg(all(sync_model, not(sync_shuttle)))]
use loom as checker;
#[cfg(all(sync_model, sync_shuttle))]
use shuttle as checker;

#[cfg(sync_model)]
pub(crate) use self::model::{Condvar, Mutex};
#[cfg(sync_model)]
pub(crate) use checker::sync::atomic::AtomicU64;

pub(crate) use std::sync::atomic::Ordering;

/// The thread's entry word (`ENTRY` in `interp.rs`) is a `const`-initialized thread-local, which Loom's atomic
/// cannot be, so it is std's in every build.
pub(crate) use std::sync::atomic::AtomicPtr;

/// Wraps the checker's lock and condition variable in parking_lot's signatures: `lock` ignores
/// poisoning and `wait` takes the guard by mutable borrow.
#[cfg(sync_model)]
mod model {
    use std::sync::PoisonError;

    use super::checker;

    #[derive(Debug)]
    pub(crate) struct Mutex<T>(checker::sync::Mutex<T>);

    /// The guard is empty only inside [`Condvar::wait`], between taking it out and putting the
    /// reacquired one back.
    pub(crate) struct MutexGuard<'a, T>(Option<checker::sync::MutexGuard<'a, T>>);

    impl<T> Mutex<T> {
        pub(crate) fn new(value: T) -> Self {
            Mutex(checker::sync::Mutex::new(value))
        }

        pub(crate) fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(Some(self.0.lock().unwrap_or_else(PoisonError::into_inner)))
        }
    }

    #[expect(
        clippy::expect_used,
        reason = "a model, built only under `--cfg sync_model`: the guard is empty only inside `Condvar::wait`"
    )]
    impl<T> std::ops::Deref for MutexGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            self.0.as_ref().expect("a guard holds its lock outside `wait`")
        }
    }

    #[expect(
        clippy::expect_used,
        reason = "a model, built only under `--cfg sync_model`: the guard is empty only inside `Condvar::wait`"
    )]
    impl<T> std::ops::DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            self.0.as_mut().expect("a guard holds its lock outside `wait`")
        }
    }

    #[derive(Debug)]
    pub(crate) struct Condvar(checker::sync::Condvar);

    impl Condvar {
        pub(crate) fn new() -> Self {
            Condvar(checker::sync::Condvar::new())
        }

        pub(crate) fn notify_all(&self) {
            self.0.notify_all();
        }

        #[expect(
            clippy::expect_used,
            reason = "a model, built only under `--cfg sync_model`: the guard is empty only between the take and the put back below"
        )]
        pub(crate) fn wait<T>(&self, guard: &mut MutexGuard<'_, T>) {
            let held = guard.0.take().expect("a guard holds its lock outside `wait`");
            guard.0 = Some(self.0.wait(held).unwrap_or_else(PoisonError::into_inner));
        }
    }
}
