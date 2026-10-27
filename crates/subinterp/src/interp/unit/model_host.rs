//! The interpreter the slot protocol's models run against (`loom_model.rs`, `shuttle_model.rs`):
//! the GIL is a lock and the unit thread's state a cell under it, and the host asserts, as it is
//! called, that no interruption is set after the end of the run it was aimed at returned and that
//! nothing reaches the thread state after the unit's last end.

use std::sync::Arc;

use super::test_gil::{Gil, Held};
use super::{InterruptError, RunId, UnitHost, UnitState};

#[cfg(sync_shuttle)]
use self::plain::ThreadCell;
/// Loom's cell, which fails the model when two accesses overlap; Shuttle checks no cell, so under
/// it the cell is a plain one with the same calls.
#[cfg(not(sync_shuttle))]
#[expect(
    clippy::unsafe_removed_from_name,
    reason = "the cell is named for what it holds; its calls are safe, only the pointers they hand out are not"
)]
use loom::cell::UnsafeCell as ThreadCell;

#[cfg(sync_shuttle)]
mod plain {
    pub(in crate::interp) struct ThreadCell<T>(std::cell::UnsafeCell<T>);

    impl<T> ThreadCell<T> {
        pub(in crate::interp) fn new(value: T) -> Self {
            ThreadCell(std::cell::UnsafeCell::new(value))
        }

        pub(in crate::interp) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }
    }

    // SAFETY: the model reaches the cell only through `ModelHost::with_thread`, under the model's
    // GIL, as Loom's cell (which is `Sync` too) is reached; Shuttle runs one thread at a time.
    unsafe impl<T: Send> Sync for ThreadCell<T> {}
}

pub(super) struct ModelHost {
    pub(super) gil: Gil,
    thread: ThreadCell<ThreadState>,
}

pub(super) struct ThreadState {
    alive: bool,
    /// The run the exception set on the thread and not taken back was aimed at.
    pub(super) pending: Option<RunId>,
    last_ended: Option<RunId>,
}

impl ModelHost {
    fn new() -> Self {
        ModelHost {
            gil: Gil::new(),
            thread: ThreadCell::new(ThreadState {
                alive: true,
                pending: None,
                last_ended: None,
            }),
        }
    }

    pub(super) fn with_thread<R>(&self, _gil: Held<'_>, f: impl FnOnce(&mut ThreadState) -> R) -> R {
        // SAFETY: The caller holds the model's GIL, so no access is concurrent, and Loom fails
        // the model if one is (Shuttle checks no cell; its model leans on Loom's for this).
        self.thread.with_mut(|state| f(unsafe { &mut *state }))
    }
}

impl UnitHost for ModelHost {
    type Gil<'py> = Held<'py>;
    type Exception = RunId;

    fn visit<R>(&self, f: impl for<'py> FnOnce(Held<'py>) -> R) -> Option<R> {
        let gil = self.gil.take();
        self.with_thread(gil, |state| {
            assert!(
                state.alive,
                "an interruption entered the interpreter after the unit's end returned"
            );
        });
        let out = f(gil);
        self.gil.give();
        Some(out)
    }

    fn set_raise(&self, gil: Held<'_>, exc: Option<RunId>) -> bool {
        self.with_thread(gil, |state| {
            assert!(
                state.alive,
                "SetAsyncExc reached the unit's thread state after its end returned"
            );
            if let Some(aimed) = exc {
                assert_ne!(
                    state.last_ended,
                    Some(aimed),
                    "an interruption was set after the end of the run it was aimed at returned"
                );
            }
            state.pending = exc;
        });
        true
    }

    fn without_gil(&self, _gil: Held<'_>, f: impl FnOnce()) {
        self.gil.give();
        f();
        self.gil.take();
    }

    /// The model's unit thread is never gone, because `set_raise` always finds it.
    fn thread_gone(&self, _gil: Held<'_>) -> InterruptError {
        InterruptError::UnitThreadGone { thread: 0, interp: 0 }
    }
}

#[cfg_attr(
    not(sync_shuttle),
    expect(
        clippy::arc_with_non_send_sync,
        reason = "Loom's cell is not `Sync`, and Loom's threads do not ask for it"
    )
)]
pub(super) fn new_slot() -> Arc<UnitState<ModelHost>> {
    Arc::new(UnitState::new(ModelHost::new()))
}

pub(super) fn pending(slot: &UnitState<ModelHost>) -> Option<RunId> {
    let gil = slot.host.gil.take();
    let pending = slot.host.with_thread(gil, |state| state.pending);
    slot.host.gil.give();
    pending
}

/// Ends `run` in the GIL, as `WorkUnit::end` does, checks that the end returns the phase it
/// left, and records that its end returned.
pub(super) fn end(slot: &UnitState<ModelHost>, run: RunId) {
    let gil = slot.host.gil.take();
    let ended = slot.end_attached(gil);
    assert_eq!(
        ended,
        slot.phase(),
        "the end returned a phase other than the one it left"
    );
    slot.host.with_thread(gil, |state| state.last_ended = Some(run));
    slot.host.gil.give();
}

/// Marks the worker's thread state gone after its last end.
pub(super) fn thread_state_gone(slot: &UnitState<ModelHost>) {
    let gil = slot.host.gil.take();
    slot.host.with_thread(gil, |state| state.alive = false);
    slot.host.gil.give();
}
