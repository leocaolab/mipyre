//! The atomics and locks this crate's cross-thread state is made of: std's and parking_lot's in
//! a normal build, Loom's under `--cfg sync_model`, where the models
//! (`io/static_files/cache/loom_model.rs`) run the same code over every interleaving. Only the
//! names switch; nothing wraps the real ones, so a build pays nothing for them. Shipping code names
//! an atomic or a lock only through this module (dev/archcheck.toml E-L1).
//!
//! State Loom cannot take — what a `static` holds (no `const` constructor in Loom, and a static
//! outlives the model's executions) — names std's and parking_lot's itself, in the files E-L1
//! excludes, each with why.

#[cfg(not(sync_model))]
pub(crate) use parking_lot::{Mutex, MutexGuard};
#[cfg(not(sync_model))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicU64};

#[cfg(sync_model)]
pub(crate) use self::model::{Mutex, MutexGuard};
#[cfg(sync_model)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU64};

pub(crate) use std::sync::atomic::Ordering;

/// Loom's lock with the parking_lot calls this crate makes (`lock` without poisoning,
/// `into_inner`, `default`).
#[cfg(sync_model)]
mod model {
    use std::ops::{Deref, DerefMut};
    use std::sync::PoisonError;

    // `pub`: `coalesce::SendShare` names them in a public (hidden) associated type; the module is private.
    #[derive(Debug)]
    pub struct Mutex<T>(loom::sync::Mutex<T>);

    pub struct MutexGuard<'a, T>(loom::sync::MutexGuard<'a, T>);

    impl<T> Mutex<T> {
        pub(crate) fn new(value: T) -> Self {
            Mutex(loom::sync::Mutex::new(value))
        }

        pub(crate) fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(self.0.lock().unwrap_or_else(PoisonError::into_inner))
        }

        pub(crate) fn into_inner(self) -> T {
            self.0.into_inner().unwrap_or_else(PoisonError::into_inner)
        }
    }

    impl<T: Default> Default for Mutex<T> {
        fn default() -> Self {
            Mutex::new(T::default())
        }
    }

    impl<T> Deref for MutexGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    impl<T> DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            &mut self.0
        }
    }
}
