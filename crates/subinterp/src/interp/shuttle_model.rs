//! The slot protocol's Shuttle model (b/744): the shipping `UnitState` against the model's
//! interpreter (`model_host.rs`) under seeded PCT schedules, at a scale the Loom model
//! (`loom_model.rs`, one or two interrupters on one run) cannot enumerate: a worker runs
//! [`RUNS`] runs, taking an interruption back in every other one, then ends its interpreter,
//! while [`INTERRUPTERS`] threads interrupt whichever run they last saw begin, the ended ones and
//! the last one after the interpreter's end included. The model's host asserts as it is called
//! that no interruption is set after the end of the run it was aimed at returned and that nothing
//! reaches the thread state once the unit's last end returned; the worker checks that nothing is
//! pending after each end; a visit the end does not wait for leaves a thread blocked on the GIL
//! for good, which Shuttle reports as a deadlock. Shuttle runs every atomic as `SeqCst` and checks
//! no cell, so memory ordering and overlapping GIL holders stay the Loom model's.
//!
//! The merge gate runs [`SCHEDULES`] schedules of [`SEED`] (dev/proofs.toml):
//! `RUSTFLAGS="--cfg sync_model --cfg sync_shuttle" cargo test -p mipyre_subinterp --lib --profile model --target-dir target/shuttle shuttle_model`.
//! The nightly run adds `--cfg sync_shuttle_random` for random schedules; a schedule that fails
//! prints itself, and goes into [`REGRESSIONS`], replayed on every run.

use std::sync::Arc;

use shuttle::scheduler::PctScheduler;
use shuttle::{thread, Config, Runner};

use super::model_host::{end, new_slot, pending, thread_state_gone, ModelHost};
use super::{RunId, UnitHost, UnitState};
use crate::sync::Mutex;

const INTERRUPTERS: usize = 4;
const RUNS: usize = 30;
const INTERRUPTS_EACH: usize = 20;
/// The bug depth a PCT schedule targets: bugs that need at most this many ordering constraints to
/// show are found with a known probability per run.
const PCT_DEPTH: usize = 2;

/// The merge gate's schedules: [`SCHEDULES`] PCT schedules drawn from this seed, the same on every
/// run. A schedule of the model takes about 80 µs on an M5 (release), so the gate runs a thousand;
/// in 32 an interruption landed in 154 of 2,560 tries and was taken back once.
const SEED: u64 = 0;
const SCHEDULES: usize = 1024;

/// Schedules a random run found failing, as Shuttle printed them; each is replayed on every run.
const REGRESSIONS: &[&str] = &[];

/// Interrupts the run it last saw begin, [`INTERRUPTS_EACH`] times, and checks what each reports:
/// a run still current after its interruption returned reads interrupted, or ended.
/// `current` holds the run the worker began last, `None` before the first.
fn interrupter(slot: &Arc<UnitState<ModelHost>>, current: &Arc<Mutex<Option<RunId>>>) -> thread::JoinHandle<()> {
    let (slot, current) = (Arc::clone(slot), Arc::clone(current));
    thread::spawn(move || {
        for _ in 0..INTERRUPTS_EACH {
            let Some(run) = *current.lock() else {
                thread::yield_now();
                continue;
            };
            slot.interrupt(run, run)
                .expect("the model's unit thread is always found");
            if let Some(phase) = slot.phase_of_run(run) {
                assert!(
                    phase.was_interrupted() || phase.has_ended(),
                    "a run still running after its interruption returned reads not interrupted"
                );
            }
        }
    })
}

/// One run of the worker against the interrupters: it begins each run, tells them, takes an
/// interruption back mid-run in every other one, ends it, and finally ends its interpreter.
fn model() {
    let slot = new_slot();
    let current = Arc::new(Mutex::new(None));
    let interrupters: Vec<_> = (0..INTERRUPTERS).map(|_| interrupter(&slot, &current)).collect();

    for n in 0..RUNS {
        let (run, _) = slot.begin_run();
        *current.lock() = Some(run);
        if n % 2 == 1 {
            let gil = slot.host.gil.take();
            let was_pending = slot.host.with_thread(gil, |state| state.pending.is_some());
            assert_eq!(
                slot.take_interruption(gil),
                was_pending,
                "the take-back disagrees with what was pending"
            );
            slot.host.gil.give();
        }
        end(&slot, run);
        assert_eq!(pending(&slot), None, "an interruption outlived the end of its run");
    }
    thread_state_gone(&slot);

    for interrupter in interrupters {
        interrupter.join().unwrap();
    }
}

fn config() -> Config {
    let mut config = Config::new();
    // Shuttle warns, once per process, that it runs weaker orderings as `SeqCst`: this module's
    // doc says so.
    config.silence_warnings = true;
    config
}

/// The merge gate's schedules ([`SEED`], [`SCHEDULES`]), the same on every run; each runs the
/// model to its end.
#[test]
fn no_interruption_outlives_its_run_or_its_interpreter_under_the_fixed_schedules() {
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

/// The check itself: an interrupter that reads the run as running and then sets its exception,
/// without counting itself in so the end waits for it, sets it after the run's end returned in
/// some schedule the merge gate runs, and the model's host reports it. It drives the host
/// directly, not the shipping protocol.
#[test]
#[should_panic(expected = "an interruption was set after the end of the run it was aimed at returned")]
fn an_interruption_that_does_not_count_itself_in_is_set_after_its_runs_end() {
    let unguarded = || {
        let slot = new_slot();
        let (run, _) = slot.begin_run();
        let interrupting = thread::spawn({
            let slot = Arc::clone(&slot);
            move || {
                if slot.phase_of_run(run).is_some_and(|phase| !phase.has_ended()) {
                    slot.host.visit(|gil| slot.host.set_raise(gil, Some(run)));
                }
            }
        });
        end(&slot, run);
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

/// The nightly run (b/744): random PCT schedules, many of them, built only under
/// `--cfg sync_shuttle_random` so the merge gate's run stays the fixed [`SCHEDULES`]; a failing
/// one prints its schedule for [`REGRESSIONS`].
#[cfg(sync_shuttle_random)]
#[test]
fn no_interruption_outlives_its_run_or_its_interpreter_under_random_schedules() {
    Runner::new(PctScheduler::new(PCT_DEPTH, 100_000), config()).run(model);
}
