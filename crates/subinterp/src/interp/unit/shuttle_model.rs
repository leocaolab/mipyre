//! The slot protocol's Shuttle model: the shipping `SlotInner` against the model's
//! interpreter (`model_interpreter.rs`) under seeded PCT schedules, at a scale the Loom model
//! (`loom_model.rs`, one or two interrupters on one unit) cannot enumerate: a worker runs
//! [`UNITS`] units, taking an interruption back in every other one, then ends its interpreter,
//! while [`INTERRUPTERS`] threads interrupt whichever unit they last saw begin, the ended ones and
//! the last one after the interpreter's end included. The model's interpreter asserts as it is called
//! that no interruption is set after the end of the unit it was aimed at returned and that nothing
//! reaches the thread state once the unit's last end returned; the worker checks that nothing is
//! pending after each end; every interruption reported set is reported by its unit's end. An
//! interrupter the end does not wait for enters after the unit's end returned, which the interpreter's
//! assertion reports. Shuttle runs every atomic as `SeqCst` and checks no cell, so memory ordering
//! and overlapping GIL holders stay the Loom model's.
//!
//! Every run takes [`SCHEDULES`] schedules of [`SEED`]:
//! `RUSTFLAGS="--cfg sync_model --cfg sync_shuttle" cargo test -p mipyre_subinterp --lib --profile model --target-dir target/shuttle shuttle_model`.
//! The nightly run adds `--cfg sync_shuttle_random` for random schedules; a schedule that fails
//! prints itself, and goes into [`REGRESSIONS`], replayed on every run.

use std::sync::Arc;

use shuttle::scheduler::PctScheduler;
use shuttle::{thread, Config, Runner};

use super::model_interpreter::{begin, end, new_slot, pending, thread_state_gone, ModelInterpreter};
use super::test_phase::phase_of_unit;
use super::word::{UnitId, UnitPhase};
use super::{Interpreter, Interruption, SlotInner};
use crate::sync::Mutex;

const INTERRUPTERS: usize = 4;
const UNITS: usize = 30;
const INTERRUPTS_EACH: usize = 20;
/// The bug depth a PCT schedule targets: bugs that need at most this many ordering constraints to
/// show are found with a known probability per run.
const PCT_DEPTH: usize = 2;

/// The fixed schedules: [`SCHEDULES`] PCT schedules drawn from this seed, the same on every
/// run. A schedule of the model takes about 80 µs on an M5 (release), so a run takes a thousand;
/// in 32 an interruption landed in 154 of 2,560 tries and was taken back once.
const SEED: u64 = 0;
const SCHEDULES: usize = 1024;

/// Schedules a random run found failing, as Shuttle printed them; each is replayed on every run.
const REGRESSIONS: &[&str] = &[];

/// Interrupts the unit it last saw begin, [`INTERRUPTS_EACH`] times, and returns the units it
/// reports an interruption set on. `current` holds the unit the worker began last, `None` before
/// the first.
fn interrupter(
    slot: &Arc<SlotInner<ModelInterpreter>>,
    current: &Arc<Mutex<Option<UnitId>>>,
) -> thread::JoinHandle<Vec<UnitId>> {
    let (slot, current) = (Arc::clone(slot), Arc::clone(current));
    thread::spawn(move || {
        let mut set = Vec::new();
        for _ in 0..INTERRUPTS_EACH {
            let Some(unit) = *current.lock().unwrap() else {
                thread::yield_now();
                continue;
            };
            let interrupted = slot
                .interrupt(unit, unit)
                .expect("the model's unit thread is always found");
            if interrupted == Interruption::Set {
                set.push(unit);
            }
        }
        set
    })
}

/// One run of the worker against the interrupters: it begins each unit, tells them, takes an
/// interruption back mid-unit in every other one, ends it, and finally ends its interpreter.
fn model() {
    let slot = new_slot();
    let current = Arc::new(Mutex::new(None));
    let interrupters: Vec<_> = (0..INTERRUPTERS).map(|_| interrupter(&slot, &current)).collect();

    let mut ended = Vec::new();
    for n in 0..UNITS {
        let unit = begin(&slot);
        *current.lock().unwrap() = Some(unit);
        let mut taken_back = false;
        if n % 2 == 1 {
            let gil = slot.interpreter.gil.take();
            let was_pending = slot.interpreter.with_thread(gil, |state| state.pending.is_some());
            taken_back = slot.take_interruption(gil);
            assert_eq!(taken_back, was_pending, "the take-back disagrees with what was pending");
            slot.interpreter.gil.give();
        }
        ended.push((unit, end(&slot, unit, taken_back)));
        assert_eq!(pending(&slot), None, "an interruption outlived the end of its unit");
    }
    thread_state_gone(&slot);

    for interrupter in interrupters {
        for unit in interrupter.join().unwrap() {
            assert!(
                ended.contains(&(unit, true)),
                "an interruption reported set on unit {unit:?} was not reported by its end"
            );
        }
    }
}

fn config() -> Config {
    let mut config = Config::new();
    // Shuttle warns, once per process, that it runs weaker orderings as `SeqCst`: this module's
    // doc says so.
    config.silence_warnings = true;
    config
}

/// Runs the fixed schedules ([`SEED`], [`SCHEDULES`]), the same on every run; each runs the
/// model to its end.
#[test]
fn no_interruption_outlives_its_unit_or_its_interpreter_under_the_fixed_schedules() {
    // Counts the runs that got to the model's end, outside Shuttle's view: the schedules ran the
    // model whole.
    let ended = Arc::new(std::sync::Mutex::new(0usize));
    let counted = Arc::clone(&ended);
    Runner::new(PctScheduler::new_from_seed(SEED, PCT_DEPTH, SCHEDULES), config()).run(move || {
        model();
        *counted.lock().unwrap() += 1;
    });
    assert_eq!(*ended.lock().unwrap(), SCHEDULES);
}

/// The check itself: an interrupter that reads the unit as running and then sets its exception,
/// without counting itself in so the end waits for it, sets it after the unit's end returned in
/// one of the fixed schedules, and the model's interpreter reports it. It drives the interpreter
/// directly, not the shipping protocol.
#[test]
#[should_panic(expected = "an interruption was set after the end of the unit it was aimed at returned")]
fn an_interruption_that_does_not_count_itself_in_is_set_after_its_units_end() {
    let unguarded = || {
        let slot = new_slot();
        let unit = begin(&slot);
        let interrupting = thread::spawn({
            let slot = Arc::clone(&slot);
            move || {
                if phase_of_unit(&slot, unit).is_some_and(|phase| matches!(phase, UnitPhase::Running { .. })) {
                    let raised = slot.interpreter.visit(|gil| slot.interpreter.raise(gil, unit));
                    assert_eq!(raised, Ok(Ok(())));
                }
            }
        });
        end(&slot, unit, false);
        interrupting.join().unwrap();
    };
    Runner::new(PctScheduler::new_from_seed(SEED, PCT_DEPTH, SCHEDULES), config()).run(unguarded);
}

/// The schedules a random run found failing, replayed.
#[test]
fn the_schedules_found_failing_pass() {
    for schedule in REGRESSIONS {
        shuttle::replay(model, schedule);
    }
}

/// The nightly run: random PCT schedules, many of them, built only under
/// `--cfg sync_shuttle_random` so the default run stays the fixed [`SCHEDULES`]; a failing
/// one prints its schedule for [`REGRESSIONS`].
#[cfg(sync_shuttle_random)]
#[test]
fn no_interruption_outlives_its_unit_or_its_interpreter_under_random_schedules() {
    Runner::new(PctScheduler::new(PCT_DEPTH, 100_000), config()).run(model);
}
