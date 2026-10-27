//! The atomics and locks the runtime's cross-thread state is made of: std's and parking_lot's in
//! a normal build, Loom's under `--cfg sync_model`, where the models
//! (`live_workers/loom_model.rs`) run the same code over every interleaving, and
//! Shuttle's under `--cfg sync_model --cfg sync_shuttle`, where the Shuttle models
//! (`job_queue/shuttle_model.rs`) run it under seeded random schedules at a scale Loom
//! cannot enumerate. Only the names switch; nothing wraps the real ones, so a build pays nothing
//! for them. Shipping code names an atomic or a lock only through this module.
//!
//! State Loom cannot take names std's and parking_lot's itself, each with why: what a `static`
//! holds (no `const` constructor in Loom, and a static outlives the model's executions) and the
//! lane headers in memory another process maps (`lane/shm.rs`).
//!
//! It also has the doorbell an asyncio loop waits on (`os.rs`), the one wake that crosses from
//! Rust's threads to Python's loop without the GIL.

#[cfg(not(sync_model))]
pub(crate) use parking_lot::Mutex;
#[cfg(not(sync_model))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicUsize};
#[cfg(not(sync_model))]
pub(crate) use std::sync::OnceLock;

/// The model checker whose names a model build takes: Loom's, or Shuttle's under `sync_shuttle`.
/// The two have std's calls, so one wrapper below serves both.
#[cfg(all(sync_model, not(sync_shuttle)))]
use loom as checker;
#[cfg(all(sync_model, sync_shuttle))]
use shuttle as checker;

#[cfg(sync_model)]
pub(crate) use self::model::{Mutex, OnceLock};
#[cfg(sync_model)]
pub(crate) use checker::sync::atomic::{AtomicBool, AtomicUsize};

pub(crate) use std::sync::atomic::Ordering;

mod os;
pub(crate) use os::Doorbell;

/// std's mutex and condition variable, which poison on a panic, where the code reads the poison
/// (the body stream, the streamed response and the job queue take the state back out of it).
/// Loom's and Shuttle's have the same calls.
pub(crate) mod poisoning {
    #[cfg(not(sync_model))]
    pub(crate) use std::sync::{Condvar, Mutex, MutexGuard};

    #[cfg(sync_model)]
    pub(crate) use super::checker::sync::{Condvar, Mutex, MutexGuard};
}

/// The checker's lock and once-cell with the calls this crate makes of parking_lot's and std's
/// (`lock` without poisoning, `get_or_init`).
#[cfg(sync_model)]
mod model {
    use std::sync::PoisonError;

    use super::checker;
    use checker::sync::atomic::{AtomicU8, Ordering};

    #[derive(Debug)]
    pub struct Mutex<T>(checker::sync::Mutex<T>);

    impl<T> Mutex<T> {
        pub(crate) fn new(value: T) -> Self {
            Mutex(checker::sync::Mutex::new(value))
        }

        pub(crate) fn lock(&self) -> checker::sync::MutexGuard<'_, T> {
            self.0.lock().unwrap_or_else(PoisonError::into_inner)
        }

        pub(crate) fn get_mut(&mut self) -> &mut T {
            self.0.get_mut().unwrap_or_else(PoisonError::into_inner)
        }
    }

    impl<T: Default> Default for Mutex<T> {
        fn default() -> Self {
            Mutex::new(T::default())
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
