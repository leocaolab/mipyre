//! Creates, enters, collects and ends sub-interpreters that own their GIL, behind a safe API.
//!
//! What a unit of work is belongs to the caller's [`Releasable`]; the threads, and giving up on
//! one that does not stop, belong to `mipyre_tpc`. The one rule no type here can express, that
//! no other thread state is current on a thread entering a worker, is checked by CPython, which
//! aborts.

pub mod attach;
pub mod context;
mod finalization;
pub mod gc;
pub mod isolation;
mod interp;
mod sync;

#[cfg(feature = "startup_trace")]
pub use interp::trace;
pub use interp::{
    detach_for_idle, keep_attached_until_idle, InterruptError, InterruptTarget,
    NewInterpreterError, Releasable, SubInterpreter, ThreadStateCreationFailure, WorkUnit,
};

/// Waits until no thread owns a sub-interpreter, or until `deadline`, and returns how many are
/// owned then. A thread owns its interpreter from its creation until it ends or gives it up, and
/// may enter it all that time: a process must not finalize while any is owned.
pub fn wait_for_owned_interpreters(deadline: std::time::Instant) -> usize {
    finalization::FINALIZATION.owned_by(deadline)
}

/// Stack size in bytes for every thread that runs Python code.
///
/// C extensions are written against the 8 MiB pthread default, not Rust's 2 MiB: OpenBLAS's
/// `dgetrf_parallel` overflows 2 MiB under `np.linalg.inv` (numpy 2.5.1, OpenBLAS 0.3.33).
/// It reserves address space, not resident memory.
pub const PYTHON_THREAD_STACK: usize = 8 << 20;

#[cfg(test)]
#[cfg(not(sync_model))]
fn python_thread(name: String) -> std::thread::Builder {
    std::thread::Builder::new()
        .name(name)
        .stack_size(PYTHON_THREAD_STACK)
}
