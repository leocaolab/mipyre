//! Creates, enters, collects and ends sub-interpreters that own their GIL, behind a safe API.
//!
//! What a unit of work is belongs to the caller's [`Releasable`]; the threads, and giving up on
//! one that does not stop, belong to `mipyre_tpc`. The one rule no type here can express, that
//! no other thread state is current on a thread entering a worker, is checked by CPython, which
//! aborts.

#![deny(unsafe_op_in_unsafe_fn)]

pub mod attach;
pub mod context;
mod entry;
mod finalization;
pub mod gc;
mod interp;
pub mod isolation;
mod sync;

#[cfg(feature = "startup_trace")]
pub use interp::trace;
pub use interp::unit::{InterruptError, InterruptTarget, ThreadStateCreationFailure, UnitEnd, WorkUnit};
pub use interp::{detach_for_idle, keep_attached_until_idle, NewInterpreterError, Releasable, SubInterpreter};

pub use finalization::ExitDecision;

/// Decides whether the process may finalize, from what every thread that owns a sub-interpreter
/// is doing; on [`ExitDecision::Finalize`] it returns once the last has ended its interpreter.
///
/// A thread owns its interpreter from its creation until it ends or gives it up. One that runs
/// Python (a handler, possibly one that never returns) makes the decision
/// [`ExitDecision::ExitWithoutFinalizing`] at once. When none does, every one is only on its way
/// to ending its interpreter, which is waited for without a deadline; a unit that begins on one
/// meanwhile decides [`ExitDecision::ExitWithoutFinalizing`] as it enters.
pub fn decide_exit() -> ExitDecision {
    finalization::FINALIZATION.decide_exit()
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
    std::thread::Builder::new().name(name).stack_size(PYTHON_THREAD_STACK)
}
