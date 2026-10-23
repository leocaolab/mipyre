//! Whether Python runs on a worker thread now, in the one word per thread that entering a worker
//! already writes, so the process's exit can read it.
//!
//! The word is a thread-local the thread writes on every entry and exit of a unit of work. Its
//! values: null, no worker thread state current here; a worker's thread state, kept current
//! between units ([`crate::keep_attached_until_idle`]); [`IN_PYTHON`], a unit runs and Python
//! code may be on the stack. The exit marks any but the last with [`WATCHED`]: a unit that then
//! begins on the thread tells the exit as it enters, so the exit, waiting for the thread to end
//! its interpreter, learns that Python runs there again.
//!
//! The thread swaps [`IN_PYTHON`] in and the exit compares-and-swaps [`WATCHED`] in, both
//! read-modify-writes of the one word, so they are ordered: either the exit sees the unit running,
//! or the entering thread sees the mark and tells the exit. No fence is needed for that, so every
//! access is `Relaxed`; the telling synchronizes through the owners' lock.
//!
//! The word (`ENTRY`) and `ThreadEntry`, which reads it from another thread, live in `interp.rs`
//! beside `enter`: reading another thread's thread-local is unsafe code, which has its homes.

use pyo3::ffi::PyThreadState;

/// The bit the exit sets on a thread's word. CPython allocates a thread state as a struct of
/// pointers, so its address has neither this bit nor [`IN_PYTHON`]'s (`aligned`).
pub(crate) const WATCHED: usize = 1;

/// The word while a unit runs.
pub(crate) const IN_PYTHON: *mut PyThreadState = std::ptr::without_provenance_mut(2);

/// Whether `tstate`'s address leaves the word's two low bits free.
pub(crate) fn aligned(tstate: *mut PyThreadState) -> bool {
    tstate.addr() & (WATCHED | IN_PYTHON.addr()) == 0
}

/// What the exit found a thread that owns an interpreter doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ThreadAtExit {
    /// A unit runs: it may be in a handler that never returns.
    RunningPython,
    /// No unit runs; one that begins now tells the exit as it enters.
    OutsidePython,
}

/// The word marked for the exit, or `None` while a unit runs on the thread.
pub(crate) fn marked(word: usize) -> Option<usize> {
    (word != IN_PYTHON.addr()).then_some(word | WATCHED)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_running_unit_is_not_marked_and_every_other_word_is() {
        let kept = 0x7f00_0000_1000;
        assert_eq!(marked(IN_PYTHON.addr()), None);
        assert_eq!(marked(0), Some(WATCHED));
        assert_eq!(marked(kept), Some(kept | WATCHED));
        assert_eq!(marked(kept | WATCHED), Some(kept | WATCHED));
        assert_eq!(marked(WATCHED), Some(WATCHED));
    }
}
