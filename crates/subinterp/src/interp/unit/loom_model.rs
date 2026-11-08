//! Checks a slot's end and begin against interrupting threads over every Loom interleaving.
//!
//! Run with
//! `RUSTFLAGS="--cfg sync_model" cargo test -p mipyre_subinterp --lib --profile model --target-dir target/loom loom_model`.
//! The thread state is a `loom::cell::UnsafeCell`, so Loom also fails any two interpreter calls that
//! are in "the GIL" at once. Every interleaving explored shows that an interruption aimed at a
//! unit never stays pending past that unit's end or lands in the next unit, that nothing touches
//! the unit's thread state once its last end returned, and that an interruption reported set is
//! reported by its unit's end.

use std::sync::Arc;

use loom::thread;

use super::model_interpreter::{begin, end, new_slot, pending, thread_state_gone, ModelInterpreter};
use super::word::UnitId;
use super::{Interruption, SlotInner};

/// Spawns a thread that interrupts `unit` and returns what it reports.
fn interrupter(slot: &Arc<SlotInner<ModelInterpreter>>, unit: UnitId) -> thread::JoinHandle<Interruption> {
    let slot = Arc::clone(slot);
    thread::spawn(move || slot.interrupt(unit, unit).unwrap())
}

#[test]
fn a_stale_interruption_never_reaches_the_next_unit() {
    loom::model(|| {
        let slot = new_slot();
        let first = begin(&slot);
        let interrupting = interrupter(&slot, first);

        let first_interrupted = end(&slot, first, false);
        assert_eq!(pending(&slot), None, "an interruption outlived the end of its unit");
        let second = begin(&slot);
        assert_ne!(second, first);
        assert_eq!(pending(&slot), None, "an interruption of unit 1 is pending in unit 2");
        assert!(!end(&slot, second, false), "unit 2 reports unit 1's interruption");
        thread_state_gone(&slot);

        let set = interrupting.join().unwrap() == Interruption::Set;
        assert_eq!(set, first_interrupted, "a set interruption and the end disagree");
    });
}

/// The worker takes the interruption back mid-unit, as a handler waiting outside bytecode does.
#[test]
fn a_take_back_and_an_end_leave_nothing_pending() {
    loom::model(|| {
        let slot = new_slot();
        let unit = begin(&slot);
        let interrupting = interrupter(&slot, unit);

        let gil = slot.interpreter.gil.take();
        let was_pending = slot.interpreter.with_thread(gil, |state| state.pending.is_some());
        let taken = slot.take_interruption(gil);
        assert_eq!(taken, was_pending, "the take-back disagrees with what was pending");
        assert_eq!(slot.interpreter.with_thread(gil, |state| state.pending), None);
        slot.interpreter.gil.give();

        let interrupted = end(&slot, unit, taken);
        assert_eq!(pending(&slot), None, "an interruption outlived the end of its unit");
        thread_state_gone(&slot);

        let set = interrupting.join().unwrap() == Interruption::Set;
        assert_eq!(set, interrupted, "a set interruption and the end disagree");
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
        let unit = begin(&slot);
        let one = interrupter(&slot, unit);
        let two = interrupter(&slot, unit);

        let interrupted = end(&slot, unit, false);
        assert_eq!(pending(&slot), None);
        thread_state_gone(&slot);

        let set = [one.join().unwrap(), two.join().unwrap()].contains(&Interruption::Set);
        assert_eq!(set, interrupted, "a set interruption and the end disagree");
    });
}
