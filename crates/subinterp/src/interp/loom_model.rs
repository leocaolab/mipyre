//! Checks a slot's end and begin against interrupting threads over every Loom interleaving.
//!
//! Run with
//! `RUSTFLAGS="--cfg sync_model" cargo test -p mipyre_subinterp --lib --release --target-dir target/loom loom_model`.
//! The thread state is a `loom::cell::UnsafeCell`, so Loom also fails any two host calls that
//! are in "the GIL" at once. Every interleaving explored shows that an interruption aimed at a
//! run never stays pending past that run's end or lands in the next run, that nothing touches
//! the unit's thread state once its last end returned, and that an interruption which set its
//! exception reports the run interrupted unless the slot moved on.

use std::sync::Arc;

use loom::cell::UnsafeCell;
use loom::thread;

use super::test_gil::{Gil, Held};
use super::{InterruptError, RunId, UnitHost, UnitPhase, UnitState};

struct ModelHost {
    gil: Gil,
    thread: UnsafeCell<ThreadState>,
}


struct ThreadState {
    alive: bool,
    /// The run the exception set on the thread and not taken back was aimed at.
    pending: Option<RunId>,
    last_ended: Option<RunId>,
}


impl ModelHost {
    fn new() -> Self {
        ModelHost {
            gil: Gil::new(),
            thread: UnsafeCell::new(ThreadState {
                alive: true,
                pending: None,
                last_ended: None,
            }),
        }
    }

    fn with_thread<R>(&self, _gil: Held<'_>, f: impl FnOnce(&mut ThreadState) -> R) -> R {
        // SAFETY: The caller holds the model's GIL, so no access is concurrent, and Loom fails
        // the model if one is.
        self.thread.with_mut(|state| f(unsafe { &mut *state }))
    }
}

impl UnitHost for ModelHost {
    type Gil<'py> = Held<'py>;
    type Exception = RunId;

    fn visit<R>(&self, f: impl for<'py> FnOnce(Held<'py>) -> R) -> Option<R> {
        let gil = self.gil.take();
        self.with_thread(gil, |state| {
            assert!(state.alive, "an interruption entered the interpreter after the unit's end returned");
        });
        let out = f(gil);
        self.gil.give();
        Some(out)
    }

    fn set_raise(&self, gil: Held<'_>, exc: Option<RunId>) -> bool {
        self.with_thread(gil, |state| {
            assert!(state.alive, "SetAsyncExc reached the unit's thread state after its end returned");
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

fn new_slot() -> Arc<UnitState<ModelHost>> {
    Arc::new(UnitState::new(ModelHost::new()))
}

fn pending(slot: &UnitState<ModelHost>) -> Option<RunId> {
    let gil = slot.host.gil.take();
    let pending = slot.host.with_thread(gil, |state| state.pending);
    slot.host.gil.give();
    pending
}

/// Ends `run` in the GIL, as `WorkUnit::end` does, and records that its end returned.
fn end(slot: &UnitState<ModelHost>, run: RunId) {
    let gil = slot.host.gil.take();
    slot.end_attached(gil);
    slot.host.with_thread(gil, |state| state.last_ended = Some(run));
    slot.host.gil.give();
}

/// Marks the worker's thread state gone after its last end.
fn thread_state_gone(slot: &UnitState<ModelHost>) {
    let gil = slot.host.gil.take();
    slot.host.with_thread(gil, |state| state.alive = false);
    slot.host.gil.give();
}

/// Spawns a thread that interrupts `run` and checks what it reports.
fn interrupter(slot: &Arc<UnitState<ModelHost>>, run: RunId) -> thread::JoinHandle<()> {
    let slot = Arc::clone(slot);
    thread::spawn(move || {
        slot.interrupt(run, run).unwrap();
        // An interruption of an ended run sets nothing, so only another run's state reported
        // for this one is wrong.
        if let Some(phase) = slot.phase_of_run(run) {
            assert!(
                phase.was_interrupted() || phase.has_ended(),
                "a run still running after its interruption returned reads not interrupted"
            );
        }
    })
}

#[test]
fn a_stale_interruption_never_reaches_the_next_run() {
    loom::model(|| {
        let slot = new_slot();
        let (first, _) = slot.begin_run();
        let interrupting = interrupter(&slot, first);

        end(&slot, first);
        assert_eq!(pending(&slot), None, "an interruption outlived the end of its run");
        let (second, before) = slot.begin_run();
        assert_ne!(second, first);
        assert!(UnitPhase::from_word(before).has_ended());
        assert_eq!(pending(&slot), None, "an interruption of run 1 is pending in run 2");
        end(&slot, second);
        thread_state_gone(&slot);

        interrupting.join().unwrap();
    });
}

/// The worker takes the interruption back mid-run, as a handler waiting outside bytecode does.
#[test]
fn a_take_back_and_an_end_leave_nothing_pending() {
    loom::model(|| {
        let slot = new_slot();
        let (run, _) = slot.begin_run();
        let interrupting = interrupter(&slot, run);

        let gil = slot.host.gil.take();
        let was_pending = slot.host.with_thread(gil, |state| state.pending.is_some());
        let taken = slot.take_interruption(gil);
        assert_eq!(taken, was_pending, "the take-back disagrees with what was pending");
        assert_eq!(slot.host.with_thread(gil, |state| state.pending), None);
        slot.host.gil.give();

        end(&slot, run);
        assert_eq!(pending(&slot), None, "an interruption outlived the end of its run");
        thread_state_gone(&slot);

        interrupting.join().unwrap();
    });
}

/// Bounds three threads at 4 preemptions, which takes 7 s on an M5; each step up multiplies the
/// time by about ten.
#[test]
fn an_end_waits_for_every_interruption_in_flight() {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(4);
    model.check(|| {
        let slot = new_slot();
        let (run, _) = slot.begin_run();
        let one = interrupter(&slot, run);
        let two = interrupter(&slot, run);

        end(&slot, run);
        assert_eq!(pending(&slot), None);
        thread_state_gone(&slot);

        one.join().unwrap();
        two.join().unwrap();
    });
}
