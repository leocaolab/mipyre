//! Names the atomics this crate's cross-thread state is made of; shipping code names one only
//! through this module.

pub(crate) use std::sync::atomic::{AtomicUsize, Ordering};
