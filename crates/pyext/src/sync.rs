//! The atomics and locks the engine's own cross-thread state is made of. Shipping code names an
//! atomic or a lock only through this module. No model runs this
//! package's code, so the names stay std's and parking_lot's under `--cfg sync_model` too.

pub(crate) use parking_lot::{Condvar, Mutex, RwLock};
pub(crate) use std::sync::atomic::{AtomicU64, Ordering};
