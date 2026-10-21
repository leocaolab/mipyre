//! Exposes the engine's allocator, mimalloc, whose options pyre leaves at their defaults.
//!
//! mimalloc returns freed pages to the OS only after `purge_delay` milliseconds (1000 by
//! default) and only when the freeing thread allocates again, so a thread that goes idle after
//! a burst keeps closed connections' buffers resident (about 150 MiB after 4096 connections).
//! `MIMALLOC_PURGE_DELAY=0` returns them at once but puts an `madvise` on the request path,
//! which cost 28% more kernel instructions per GET and 15% of one thread's throughput
//! (docs/memory-per-worker.md 5.1). The engine therefore keeps the default, and a service that
//! wants memory back after bursts sets the variable itself.

use libmimalloc_sys::{mi_option_get, mi_option_t};
use pyo3::prelude::*;

/// Mirrors `mi_option_purge_delay` from mimalloc v3's `mi_option_e`, which the binding does not
/// export; `tests/test_mimalloc_purge.py` fails if the index is wrong.
const MI_OPTION_PURGE_DELAY: mi_option_t = 15;

/// Return mimalloc's current `purge_delay` in milliseconds.
#[pyfunction]
pub(crate) fn _mimalloc_purge_delay() -> std::ffi::c_long {
    // SAFETY: Reading a mimalloc option has no preconditions.
    unsafe { mi_option_get(MI_OPTION_PURGE_DELAY) }
}
