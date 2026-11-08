//! Whether Python runs on a worker thread now, in the one word per thread that entering a worker
//! already writes, so the process's exit can read it.
//!
//! The word is a thread-local the thread writes whenever it starts or stops running a
//! sub-interpreter's Python: a unit of work, a creation with its `prepare`, an end. Its values:
//! null, no worker thread state current here; a worker's thread state, kept current between units
//! ([`crate::keep_attached_until_idle`]); [`IN_PYTHON`], Python code may be on the stack. The exit
//! marks any but the last with [`WATCHED`]: a thread that then enters Python tells the exit as it
//! enters, so the exit, waiting for the thread to end its interpreter, learns that Python runs
//! there again.
//!
//! The thread swaps [`IN_PYTHON`] in and the exit compares-and-swaps [`WATCHED`] in, both
//! read-modify-writes of the one word, so they are ordered: either the exit sees Python running,
//! or the entering thread sees the mark and tells the exit. No fence is needed for that, so every
//! access is `Relaxed`; the telling synchronizes through the owners' lock.
//!
//! This module decides from the word's value; the word (`ENTRY`) and `ThreadEntry`, which reads it
//! from another thread, live in `interp.rs` beside `enter`: reading another thread's thread-local
//! is unsafe code, which has its homes.

/// The bit the exit sets on a thread's word. CPython allocates a thread state with
/// `PyMem_RawCalloc`, aligned for any type, so its address has neither this bit nor
/// [`IN_PYTHON`]'s.
pub(crate) const WATCHED: usize = 1;

/// The word while this thread runs Python.
pub(crate) const IN_PYTHON: usize = 2;

/// What the exit found a thread that owns an interpreter doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ThreadAtExit {
    /// Python runs: it may be in a handler that never returns.
    RunningPython,
    /// No Python runs; Python that starts now tells the exit as it enters.
    OutsidePython,
}

/// What entering a worker's thread state does to the thread states of this thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ThreadStateSwap {
    /// The worker's thread state is kept current already.
    Stay,
    /// None is current: restore the worker's.
    Restore,
    /// Another worker's is kept current: save it, then restore this one.
    SaveThenRestore,
}

/// Returns whether the exit marked the word.
#[inline]
pub(crate) fn watched(word: usize) -> bool {
    word & WATCHED != 0
}

/// Decides, from the word before the thread swapped [`IN_PYTHON`] in, how it enters the thread
/// state at `tstate` and whether the exit was watching it.
///
/// A word that already says [`IN_PYTHON`] is a unit nested in another on this thread: it restores,
/// and CPython aborts there, since a thread state is current.
#[inline(always)]
pub(crate) fn entering(before: usize, tstate: usize) -> (ThreadStateSwap, bool) {
    // The word a thread kept between units holds exactly its thread state, unwatched: one compare
    // decides the case every request of a kept worker takes.
    if before == tstate {
        return (ThreadStateSwap::Stay, false);
    }
    let watched = watched(before);
    let switch = match kept(before) {
        Some(kept) if kept == tstate => ThreadStateSwap::Stay,
        Some(_) => ThreadStateSwap::SaveThenRestore,
        None => ThreadStateSwap::Restore,
    };
    (switch, watched)
}

/// Returns the thread state the word keeps current, if any.
#[inline]
fn kept(word: usize) -> Option<usize> {
    let addr = word & !WATCHED;
    (addr & IN_PYTHON == 0 && addr != 0).then_some(addr)
}

/// Returns whether the word keeps a thread state current, which the thread saves before it idles;
/// clearing the word then keeps only [`WATCHED`].
pub(crate) fn keeps(word: usize) -> bool {
    kept(word).is_some()
}

/// The word marked for the exit, or `None` while Python runs on the thread.
pub(crate) fn marked(word: usize) -> Option<usize> {
    (word & !WATCHED != IN_PYTHON).then_some(word | WATCHED)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEPT: usize = 0x7f00_0000_1000;
    const OTHER: usize = 0x7f00_0000_2000;

    #[test]
    fn python_running_is_not_marked_and_every_other_word_is() {
        assert_eq!(marked(IN_PYTHON), None);
        assert_eq!(marked(IN_PYTHON | WATCHED), None);
        assert_eq!(marked(0), Some(WATCHED));
        assert_eq!(marked(KEPT), Some(KEPT | WATCHED));
        assert_eq!(marked(KEPT | WATCHED), Some(KEPT | WATCHED));
        assert_eq!(marked(WATCHED), Some(WATCHED));
    }

    #[test]
    fn entering_restores_keeps_or_switches_and_reports_the_watch() {
        assert_eq!(entering(0, KEPT), (ThreadStateSwap::Restore, false));
        assert_eq!(entering(WATCHED, KEPT), (ThreadStateSwap::Restore, true));
        assert_eq!(entering(KEPT, KEPT), (ThreadStateSwap::Stay, false));
        assert_eq!(entering(KEPT | WATCHED, KEPT), (ThreadStateSwap::Stay, true));
        assert_eq!(entering(OTHER, KEPT), (ThreadStateSwap::SaveThenRestore, false));
        assert_eq!(
            entering(OTHER | WATCHED, KEPT),
            (ThreadStateSwap::SaveThenRestore, true)
        );
        assert_eq!(entering(IN_PYTHON, KEPT), (ThreadStateSwap::Restore, false));
    }

    #[test]
    fn only_a_kept_thread_state_is_saved_before_idling() {
        assert!(keeps(KEPT));
        assert!(keeps(KEPT | WATCHED));
        assert!(!keeps(0));
        assert!(!keeps(WATCHED));
        assert!(!keeps(IN_PYTHON));
    }
}

#[cfg(kani)]
mod proofs {
    use super::*;

    /// Any address CPython could give a thread state: non-null, with the two low bits clear.
    fn any_thread_state() -> usize {
        let tstate: usize = kani::any();
        kani::assume(tstate != 0 && tstate & (WATCHED | IN_PYTHON) == 0);
        tstate
    }

    /// Any word one of the thread's spans or the exit leaves: vacant, kept or in Python, watched
    /// or not.
    fn any_word(kept: usize) -> usize {
        let occupant = match kani::any::<u8>() % 3 {
            0 => 0,
            1 => kept,
            _ => IN_PYTHON,
        };
        occupant | if kani::any() { WATCHED } else { 0 }
    }

    #[kani::proof]
    fn entering_stays_only_on_the_kept_thread_state_and_always_reports_the_watch() {
        let tstate = any_thread_state();
        let other = any_thread_state();
        let word = any_word(other);
        let (switch, watched) = entering(word, tstate);
        assert_eq!(watched, word & WATCHED != 0);
        assert_eq!(switch == ThreadStateSwap::Stay, kept(word) == Some(tstate));
        assert_eq!(switch == ThreadStateSwap::Restore, kept(word).is_none());
    }

    #[kani::proof]
    fn the_exit_marks_every_word_but_python_running_and_marking_changes_only_the_mark() {
        let word = any_word(any_thread_state());
        match marked(word) {
            Some(marked) => {
                assert_ne!(word & !WATCHED, IN_PYTHON);
                assert_eq!(marked & !WATCHED, word & !WATCHED);
                assert_eq!(marked & WATCHED, WATCHED);
            }
            None => assert_eq!(word & !WATCHED, IN_PYTHON),
        }
    }

    #[kani::proof]
    fn only_a_kept_thread_state_is_saved_before_idling() {
        let tstate = any_thread_state();
        let word = any_word(tstate);
        assert_eq!(keeps(word), word & !WATCHED == tstate);
    }
}
