//! The atomics and locks the engine's cross-thread state is made of: std's and parking_lot's in
//! a normal build, Loom's under `--cfg sync_model`, where the models
//! (`runtime/live_workers/loom_model.rs`) run the same code over every interleaving, and
//! Shuttle's under `--cfg sync_model --cfg sync_shuttle`, where the Shuttle models
//! (`runtime/job_queue/shuttle_model.rs`) run it under seeded random schedules at a scale Loom
//! cannot enumerate. Only the names switch; nothing wraps the real ones, so a build pays nothing
//! for them. Shipping code names an atomic or a lock only through this module (dev/archcheck.toml
//! E-L1).
//!
//! State Loom cannot take names std's and parking_lot's itself, in the files E-L1 excludes, each
//! with why: what a `static` holds (no `const` constructor in Loom, and a static outlives the
//! model's executions) and the lane headers in memory another process maps (`lane/shm.rs`).

#[cfg(not(sync_model))]
pub(crate) use parking_lot::{Condvar, Mutex, RwLock};
#[cfg(not(sync_model))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize};
#[cfg(not(sync_model))]
pub(crate) use std::sync::OnceLock;

/// The model checker whose names a model build takes: Loom's, or Shuttle's under `sync_shuttle`.
/// The two have std's calls, so one wrapper below serves both.
#[cfg(all(sync_model, not(sync_shuttle)))]
use loom as checker;
#[cfg(all(sync_model, sync_shuttle))]
use shuttle as checker;

#[cfg(sync_model)]
pub(crate) use self::model::{Condvar, Mutex, OnceLock, RwLock};
#[cfg(sync_model)]
pub(crate) use checker::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize};

pub(crate) use std::sync::atomic::Ordering;

/// std's mutex and condition variable, which poison on a panic, where the code reads the poison
/// (the body stream, the streamed response and the job queue take the state back out of it).
/// Loom's and Shuttle's have the same calls.
pub(crate) mod poisoning {
    #[cfg(not(sync_model))]
    pub(crate) use std::sync::{Condvar, Mutex, MutexGuard};

    #[cfg(sync_model)]
    pub(crate) use super::checker::sync::{Condvar, Mutex, MutexGuard};
}

/// The checker's lock, read-write lock, condition variable and once-cell with the calls this
/// crate makes of parking_lot's and std's (`lock` without poisoning, `wait` on a guard borrowed
/// mutably, `get_or_init`).
#[cfg(sync_model)]
mod model {
    use std::ops::{Deref, DerefMut};
    use std::sync::PoisonError;

    use super::checker;
    use checker::sync::atomic::{AtomicU8, Ordering};

    #[derive(Debug)]
    pub(crate) struct Mutex<T>(checker::sync::Mutex<T>);

    /// Empty only inside [`Condvar::wait`], which takes the guard out and puts the reacquired one
    /// back.
    pub(crate) struct MutexGuard<'a, T>(Option<checker::sync::MutexGuard<'a, T>>);

    impl<T> Mutex<T> {
        pub(crate) fn new(value: T) -> Self {
            Mutex(checker::sync::Mutex::new(value))
        }

        pub(crate) fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(Some(self.0.lock().unwrap_or_else(PoisonError::into_inner)))
        }

        pub(crate) fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
            self.0.try_lock().ok().map(|held| MutexGuard(Some(held)))
        }

        pub(crate) fn get_mut(&mut self) -> &mut T {
            self.0.get_mut().unwrap_or_else(PoisonError::into_inner)
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

    #[expect(
        clippy::expect_used,
        reason = "a model, built only under `--cfg sync_model`: the guard is empty only inside `Condvar::wait`"
    )]
    impl<T> Deref for MutexGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            self.0.as_ref().expect("a guard holds its lock outside `wait`")
        }
    }

    #[expect(
        clippy::expect_used,
        reason = "a model, built only under `--cfg sync_model`: the guard is empty only inside `Condvar::wait`"
    )]
    impl<T> DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            self.0.as_mut().expect("a guard holds its lock outside `wait`")
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

        pub(crate) fn try_read(&self) -> Option<checker::sync::RwLockReadGuard<'_, T>> {
            self.0.try_read().ok()
        }

        pub(crate) fn try_write(&self) -> Option<checker::sync::RwLockWriteGuard<'_, T>> {
            self.0.try_write().ok()
        }
    }

    #[derive(Debug, Default)]
    pub(crate) struct Condvar(checker::sync::Condvar);

    impl Condvar {
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

    /// std's `OnceLock` behind a claim on the checker's atomic, so a model sees the race for the first
    /// value: one caller of `get_or_init` claims the cell and sets it, the others wait for it. The
    /// value sits in a std `OnceLock` that only the claimer initializes and that is read only after
    /// the claim reads `SET`, so no caller ever blocks on std's lock, which the checker cannot see.
    #[derive(Debug)]
    pub(crate) struct OnceLock<T> {
        state: AtomicU8,
        value: std::sync::OnceLock<T>,
    }

    const EMPTY: u8 = 0;
    const WRITING: u8 = 1;
    const SET: u8 = 2;

    impl<T> OnceLock<T> {
        pub(crate) fn new() -> Self {
            OnceLock {
                state: AtomicU8::new(EMPTY),
                value: std::sync::OnceLock::new(),
            }
        }

        pub(crate) fn get(&self) -> Option<&T> {
            if self.state.load(Ordering::Acquire) != SET {
                return None;
            }
            self.value.get()
        }

        pub(crate) fn get_or_init(&self, init: impl FnOnce() -> T) -> &T {
            let mut init = Some(init);
            loop {
                match self
                    .state
                    .compare_exchange(EMPTY, WRITING, Ordering::Acquire, Ordering::Acquire)
                {
                    Ok(_) => {
                        if let Some(init) = init.take() {
                            self.value.get_or_init(init);
                        }
                        self.state.store(SET, Ordering::Release);
                    }
                    Err(WRITING) => checker::thread::yield_now(),
                    Err(_) => {}
                }
                if let Some(value) = self.get() {
                    return value;
                }
            }
        }
    }
}
