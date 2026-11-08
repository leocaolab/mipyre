//! The interpreter the slot protocol's models run against (`loom_model.rs`, `shuttle_model.rs`):
//! the GIL is a lock and the unit thread's state a cell under it, and the interpreter asserts, as it is
//! called, that no interruption is set after the end of the unit it was aimed at returned and that
//! nothing reaches the thread state after the unit's last end.

use std::sync::Arc;

use super::test_gil::{Gil, GilToken};
use super::word::UnitId;
use super::{Interpreter, NewThreadStateError, SlotInner, ThreadGone};

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

    // SAFETY: the model reaches the cell only through `ModelInterpreter::with_thread`, under the model's
    // GIL, as Loom's cell (which is `Sync` too) is reached; Shuttle runs one thread at a time.
    unsafe impl<T: Send> Sync for ThreadCell<T> {}
}

pub(super) struct ModelInterpreter {
    pub(super) gil: Gil,
    thread: ThreadCell<ThreadState>,
}

pub(super) struct ThreadState {
    alive: bool,
    /// The unit the exception set on the thread and not taken back was aimed at.
    pub(super) pending: Option<UnitId>,
    last_ended: Option<UnitId>,
}

impl ModelInterpreter {
    fn new() -> Self {
        ModelInterpreter {
            gil: Gil::new(),
            thread: ThreadCell::new(ThreadState {
                alive: true,
                pending: None,
                last_ended: None,
            }),
        }
    }

    fn reach(&self, gil: GilToken<'_>, exc: Option<UnitId>) {
        self.with_thread(gil, |state| {
            assert!(
                state.alive,
                "SetAsyncExc reached the unit's thread state after its end returned"
            );
            if let Some(aimed) = exc {
                assert_ne!(
                    state.last_ended,
                    Some(aimed),
                    "an interruption was set after the end of the unit it was aimed at returned"
                );
            }
            state.pending = exc;
        });
    }

    pub(super) fn with_thread<R>(&self, _gil: GilToken<'_>, f: impl FnOnce(&mut ThreadState) -> R) -> R {
        // SAFETY: The caller holds the model's GIL, so no access is concurrent, and Loom fails
        // the model if one is (Shuttle checks no cell; its model leans on Loom's for this).
        self.thread.with_mut(|state| f(unsafe { &mut *state }))
    }
}

impl Interpreter for ModelInterpreter {
    type Gil<'py> = GilToken<'py>;
    type Exception = UnitId;

    fn visit<R>(&self, f: impl for<'py> FnOnce(GilToken<'py>) -> R) -> Result<R, NewThreadStateError> {
        let gil = self.gil.take();
        self.with_thread(gil, |state| {
            assert!(
                state.alive,
                "an interruption entered the interpreter after the unit's end returned"
            );
        });
        let out = f(gil);
        self.gil.give();
        Ok(out)
    }

    /// The model's unit thread is never gone: a set after its end is a failure of the protocol,
    /// which the model reports.
    fn raise(&self, gil: GilToken<'_>, exc: UnitId) -> Result<(), ThreadGone> {
        self.reach(gil, Some(exc));
        Ok(())
    }

    fn clear(&self, gil: GilToken<'_>) {
        self.reach(gil, None);
    }

    fn without_gil(&self, _gil: GilToken<'_>, f: impl FnOnce()) {
        self.gil.give();
        f();
        self.gil.take();
    }
}

#[cfg_attr(
    not(sync_shuttle),
    expect(
        clippy::arc_with_non_send_sync,
        reason = "Loom's cell is not `Sync`, and Loom's threads do not ask for it"
    )
)]
pub(super) fn new_slot() -> Arc<SlotInner<ModelInterpreter>> {
    Arc::new(SlotInner::new(ModelInterpreter::new()))
}

pub(super) fn pending(slot: &SlotInner<ModelInterpreter>) -> Option<UnitId> {
    let gil = slot.interpreter.gil.take();
    let pending = slot.interpreter.with_thread(gil, |state| state.pending);
    slot.interpreter.gil.give();
    pending
}

/// Ends `unit` in the GIL, as `WorkUnit::end` does, checks that the end reports what it left,
/// records that its end returned, and returns whether it reported the unit interrupted.
pub(super) fn end(slot: &SlotInner<ModelInterpreter>, unit: UnitId, taken_back: bool) -> bool {
    let gil = slot.interpreter.gil.take();
    let interrupted = slot.end_attached(gil, taken_back);
    assert_eq!(
        slot.load().phase,
        super::word::UnitPhase::Ended { interrupted },
        "the end reported other than the phase it left"
    );
    slot.interpreter.with_thread(gil, |state| state.last_ended = Some(unit));
    slot.interpreter.gil.give();
    interrupted
}

/// Begins the next unit in the GIL, as `UnitSlot::begin` does.
pub(super) fn begin(slot: &SlotInner<ModelInterpreter>) -> UnitId {
    let gil = slot.interpreter.gil.take();
    let unit = slot.begin(gil);
    slot.interpreter.gil.give();
    unit
}

/// Marks the worker's thread state gone after its last end.
pub(super) fn thread_state_gone(slot: &SlotInner<ModelInterpreter>) {
    let gil = slot.interpreter.gil.take();
    slot.interpreter.with_thread(gil, |state| state.alive = false);
    slot.interpreter.gil.give();
}
