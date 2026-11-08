//! Selects the real atomic and lock in every build, Loom's under `--cfg sync_model` and Shuttle's
//! under `--cfg sync_model --cfg sync_shuttle`, so the model checker runs the same slot code; only
//! the names switch, nothing wraps the real types. The cfg swaps them in every package of the
//! build, so it is set only to run the models.
//!
//! Process-wide state in a `static` (`finalization.rs`, `isolation.rs`) names parking_lot's and
//! std's types itself: Loom has no `const` constructor, and a static outlives the model's
//! executions.

#[cfg(not(sync_model))]
pub(crate) use std::sync::atomic::AtomicU64;
/// std's lock and condition variable rather than parking_lot's, because the checkers have std's
/// calls; their one waiter is cold (an end waiting out an interrupter).
#[cfg(not(sync_model))]
pub(crate) use std::sync::{Condvar, Mutex};

/// The model checker whose names a model build takes: Loom's, or Shuttle's under `sync_shuttle`.
#[cfg(all(sync_model, not(sync_shuttle)))]
use loom as checker;
#[cfg(all(sync_model, sync_shuttle))]
use shuttle as checker;

#[cfg(sync_model)]
pub(crate) use checker::sync::atomic::AtomicU64;
#[cfg(sync_model)]
pub(crate) use checker::sync::{Condvar, Mutex};

pub(crate) use std::sync::atomic::Ordering;

/// The thread's entry word (`ENTRY` in `interp.rs`) is a `const`-initialized thread-local, which Loom's atomic
/// cannot be, so it is std's in every build.
pub(crate) use std::sync::atomic::AtomicPtr;
