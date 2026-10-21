//! Selects the real atomic and lock in every build and Loom's under `--cfg sync_model`, so the
//! model checker runs the same slot code; only the names switch, nothing wraps the real types.
//! The cfg swaps them in every package of the build, so it is set only to run the models.
//!
//! Process-wide state in a `static` (`finalization.rs`, `isolation.rs`) names parking_lot's and
//! std's types itself: Loom has no `const` constructor, and a static outlives the model's
//! executions.

#[cfg(not(sync_model))]
pub(crate) use parking_lot::{Condvar, Mutex};
#[cfg(not(sync_model))]
pub(crate) use std::sync::atomic::AtomicU64;

#[cfg(sync_model)]
pub(crate) use self::model::{Condvar, Mutex};
#[cfg(sync_model)]
pub(crate) use loom::sync::atomic::AtomicU64;

pub(crate) use std::sync::atomic::Ordering;

/// Wraps Loom's lock and condition variable in parking_lot's signatures: `lock` ignores
/// poisoning and `wait` takes the guard by mutable borrow.
#[cfg(sync_model)]
mod model {
    use std::sync::PoisonError;

    #[derive(Debug)]
    pub(crate) struct Mutex<T>(loom::sync::Mutex<T>);

    /// The guard is empty only inside [`Condvar::wait`], between taking it out and putting the
    /// reacquired one back.
    pub(crate) struct MutexGuard<'a, T>(Option<loom::sync::MutexGuard<'a, T>>);

    impl<T> Mutex<T> {
        pub(crate) fn new(value: T) -> Self {
            Mutex(loom::sync::Mutex::new(value))
        }

        pub(crate) fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(Some(self.0.lock().unwrap_or_else(PoisonError::into_inner)))
        }
    }

    #[expect(clippy::expect_used, reason = "a Loom model, built only under `--cfg sync_model`: the guard is empty only inside `Condvar::wait`")]
    impl<T> std::ops::Deref for MutexGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            self.0.as_ref().expect("a guard holds its lock outside `wait`")
        }
    }

    #[expect(clippy::expect_used, reason = "a Loom model, built only under `--cfg sync_model`: the guard is empty only inside `Condvar::wait`")]
    impl<T> std::ops::DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            self.0.as_mut().expect("a guard holds its lock outside `wait`")
        }
    }

    #[derive(Debug)]
    pub(crate) struct Condvar(loom::sync::Condvar);

    impl Condvar {
        pub(crate) fn new() -> Self {
            Condvar(loom::sync::Condvar::new())
        }

        pub(crate) fn notify_all(&self) {
            self.0.notify_all();
        }

        #[expect(
            clippy::expect_used,
            reason = "a Loom model, built only under `--cfg sync_model`: the guard is empty only between the take and the put back below"
        )]
        pub(crate) fn wait<T>(&self, guard: &mut MutexGuard<'_, T>) {
            let held = guard.0.take().expect("a guard holds its lock outside `wait`");
            guard.0 = Some(self.0.wait(held).unwrap_or_else(PoisonError::into_inner));
        }
    }
}
