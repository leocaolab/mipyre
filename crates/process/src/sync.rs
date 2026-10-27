//! The atomics and locks the lanes' cross-thread state is made of: std's and parking_lot's in a
//! normal build, Loom's under `--cfg sync_model`, where the model (`inbox/loom_model.rs`) runs the
//! same code over every interleaving, and Shuttle's under `--cfg sync_model --cfg sync_shuttle`,
//! which the engine's Shuttle models build every package with. Only the names switch; nothing
//! wraps the real ones, so a build pays nothing for them. Shipping code names an atomic or a lock
//! only through this module.
//!
//! What a `static` holds (`role.rs`) names std's itself: Loom has no `const` constructor, and a
//! static outlives the model's executions.

#[cfg(not(sync_model))]
pub(crate) use parking_lot::{Mutex, RwLock};
#[cfg(not(sync_model))]
pub(crate) use std::sync::atomic::{fence, AtomicBool, AtomicU32, AtomicU64, AtomicUsize};

/// The model checker whose names a model build takes: Loom's, or Shuttle's under `sync_shuttle`.
/// The two have std's calls, so one wrapper below serves both.
#[cfg(all(sync_model, not(sync_shuttle)))]
use loom as checker;
#[cfg(all(sync_model, sync_shuttle))]
use shuttle as checker;

#[cfg(sync_model)]
pub(crate) use self::model::{Mutex, RwLock};
#[cfg(sync_model)]
pub(crate) use checker::sync::atomic::{fence, AtomicBool, AtomicU32, AtomicU64, AtomicUsize};

pub(crate) use std::sync::atomic::Ordering;

/// The checker's lock and read-write lock with the parking_lot calls this crate makes (`lock`
/// and `read` / `write` without poisoning, `try_lock`, `default`).
#[cfg(sync_model)]
mod model {
    use std::sync::PoisonError;

    use super::checker;

    #[derive(Debug)]
    pub(crate) struct Mutex<T>(checker::sync::Mutex<T>);

    impl<T> Mutex<T> {
        pub(crate) fn new(value: T) -> Self {
            Mutex(checker::sync::Mutex::new(value))
        }

        pub(crate) fn lock(&self) -> checker::sync::MutexGuard<'_, T> {
            self.0.lock().unwrap_or_else(PoisonError::into_inner)
        }

        pub(crate) fn try_lock(&self) -> Option<checker::sync::MutexGuard<'_, T>> {
            self.0.try_lock().ok()
        }
    }

    impl<T: Default> Default for Mutex<T> {
        fn default() -> Self {
            Mutex::new(T::default())
        }
    }

    #[derive(Debug)]
    pub(crate) struct RwLock<T>(checker::sync::RwLock<T>);

    impl<T> RwLock<T> {
        pub(crate) fn new(value: T) -> Self {
            RwLock(checker::sync::RwLock::new(value))
        }

        pub(crate) fn read(&self) -> checker::sync::RwLockReadGuard<'_, T> {
            self.0.read().unwrap_or_else(PoisonError::into_inner)
        }

        pub(crate) fn write(&self) -> checker::sync::RwLockWriteGuard<'_, T> {
            self.0.write().unwrap_or_else(PoisonError::into_inner)
        }
    }
}
