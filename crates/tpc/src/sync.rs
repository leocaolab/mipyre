//! The atomics and the lock this crate's cross-thread state is made of (a run's cause and its
//! threads still running, [`RunStop`]): std's in a normal build, Loom's under `--cfg sync_model`,
//! where the models (`threads/loom_model.rs`) run the same code over every interleaving. Only the
//! names switch; nothing wraps the real ones, so a build pays nothing for them. Shipping code names
//! an atomic or a lock only through this module.
//!
//! [`RunStop`]: crate::RunStop

#[cfg(not(sync_model))]
pub(crate) use std::sync::{atomic::AtomicUsize, Mutex};

#[cfg(sync_model)]
pub(crate) use loom::sync::{atomic::AtomicUsize, Mutex};

pub(crate) use std::sync::atomic::Ordering;
