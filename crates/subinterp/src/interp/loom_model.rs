//! Checks a slot's end and begin against interrupting threads over every Loom interleaving.
//!
//! Run with
//! `RUSTFLAGS="--cfg sync_model" cargo test -p mipyre_subinterp --lib --profile model --target-dir target/loom loom_model`.
//! The thread state is a `loom::cell::UnsafeCell`, so Loom also fails any two host calls that
//! are in "the GIL" at once. Every interleaving explored shows that an interruption aimed at a
//! run never stays pending past that run's end or lands in the next run, that nothing touches
//! the unit's thread state once its last end returned, and that an interruption which set its
//! exception reports the run interrupted unless the slot moved on.

use std::sync::Arc;

use loom::thread;

use super::model_host::{end, new_slot, pending, thread_state_gone, ModelHost};
use super::{RunId, UnitPhase, UnitState};

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
