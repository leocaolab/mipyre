//! Checks, under Shuttle's seeded PCT schedules, that no wake is lost between a loop's driver
//! and the threads that schedule on it: a lost wake leaves the driver parked with callbacks queued
//! and nobody left to ring, which Shuttle reports as a deadlock. The decisions are the shipping
//! [`wakes`] and [`next_wait`]; the bell is tokio's `Notify` with its one stored permit.
//!
//! `RUSTFLAGS="--cfg sync_model --cfg sync_shuttle" cargo test -p mipyre_runtime --lib --profile model --target-dir target/shuttle event_loop::shuttle_model`.

use std::sync::Arc;

use shuttle::scheduler::PctScheduler;
use shuttle::{thread, Config, Runner};

use super::{next_wait, wakes, LoopPhase, NextWait, ScanEnd};
use crate::sync::poisoning::{Condvar, Mutex};

const SCHEDULERS: usize = 3;
const CALLS_PER_SCHEDULER: usize = 4;
/// Every callback another thread schedules schedules one follow-up on the loop's own thread.
const CALLS: usize = 2 * SCHEDULERS * CALLS_PER_SCHEDULER;
const PCT_DEPTH: usize = 2;
const SEED: u64 = 0;
const SCHEDULES: usize = 1024;

struct TurnState {
    phase: LoopPhase,
    rung: bool,
    /// Callbacks queued by other threads, each of which schedules a follow-up when it runs.
    from_outside: usize,
    /// Follow-ups queued by the loop's own callbacks.
    follow_ups: usize,
}

impl TurnState {
    fn ready(&self) -> usize {
        self.from_outside + self.follow_ups
    }
}

/// Models tokio's `Notify` as the bell uses it: a ring while nobody waits is kept for the next
/// wait.
#[derive(Default)]
struct NotifyPermit {
    permit: Mutex<bool>,
    rung: Condvar,
}

impl NotifyPermit {
    fn ring(&self) {
        *self.permit.lock().unwrap() = true;
        self.rung.notify_one();
    }

    fn wait(&self) {
        let mut permit = self.permit.lock().unwrap();
        while !*permit {
            permit = self.rung.wait(permit).unwrap();
        }
        *permit = false;
    }
}

struct ModelLoop {
    core: Mutex<TurnState>,
    bell: NotifyPermit,
}

impl ModelLoop {
    fn new() -> Self {
        ModelLoop {
            core: Mutex::new(TurnState {
                phase: LoopPhase::Idle,
                rung: false,
                from_outside: 0,
                follow_ups: 0,
            }),
            bell: NotifyPermit::default(),
        }
    }

    /// Schedules from another thread, as `call_soon_threadsafe`: it always rings.
    fn schedule_threadsafe(&self) {
        {
            let mut core = self.core.lock().unwrap();
            core.from_outside += 1;
            core.rung = true;
        }
        self.bell.ring();
    }

    /// Schedules from the loop's own thread, as `call_soon`: it rings only as `wakes` says.
    fn schedule_here(&self) {
        let ring = {
            let mut core = self.core.lock().unwrap();
            core.follow_ups += 1;
            let ring = wakes(core.phase, core.rung);
            core.rung |= ring;
            ring
        };
        if ring {
            self.bell.ring();
        }
    }

    /// Runs one turn: the callbacks ready at its start run, and it returns how many ran and how
    /// the driver waits next, read as `wait_from` says.
    fn turn(&self, wait_from: fn(&TurnState) -> NextWait) -> (usize, NextWait) {
        let (outside, follow_ups) = {
            let mut core = self.core.lock().unwrap();
            core.phase = LoopPhase::InTurn;
            core.rung = false;
            (
                std::mem::take(&mut core.from_outside),
                std::mem::take(&mut core.follow_ups),
            )
        };
        for _ in 0..outside {
            self.schedule_here();
        }
        let wait = {
            let mut core = self.core.lock().unwrap();
            core.phase = LoopPhase::Idle;
            wait_from(&core)
        };
        (outside + follow_ups, wait)
    }
}

/// The shipping rule: how the driver waits is read from the queue when the turn ends. The model
/// watches no fd, so no turn acts on one and every scan sees all there is.
fn after_the_turn(core: &TurnState) -> NextWait {
    let acted_on_fd = false;
    next_wait(core.ready(), acted_on_fd, ScanEnd::Drained)
}

fn model(wait_from: fn(&TurnState) -> NextWait) {
    let event_loop = Arc::new(ModelLoop::new());
    let schedulers: Vec<_> = (0..SCHEDULERS)
        .map(|_| {
            let event_loop = Arc::clone(&event_loop);
            thread::spawn(move || {
                for _ in 0..CALLS_PER_SCHEDULER {
                    event_loop.schedule_threadsafe();
                }
            })
        })
        .collect();
    let mut ran = 0;
    while ran < CALLS {
        let (turned, wait) = event_loop.turn(wait_from);
        ran += turned;
        if ran < CALLS && wait == NextWait::Parked {
            event_loop.bell.wait();
        }
    }
    for scheduler in schedulers {
        scheduler.join().unwrap();
    }
    assert_eq!(ran, CALLS, "every callback scheduled ran once");
}

fn config() -> Config {
    let mut config = Config::new();
    config.silence_warnings = true;
    config
}

#[test]
fn a_threadsafe_schedule_during_a_turn_is_never_lost() {
    let ended = Arc::new(std::sync::Mutex::new(0usize));
    let counted = Arc::clone(&ended);
    Runner::new(PctScheduler::new_from_seed(SEED, PCT_DEPTH, SCHEDULES), config()).run(move || {
        model(after_the_turn);
        *counted.lock().unwrap() += 1;
    });
    assert_eq!(*ended.lock().unwrap(), SCHEDULES);
}

/// The check itself: a driver that parks after every turn, without looking at the queue again,
/// sleeps on the follow-ups its own turn queued (they did not ring: the turn was on), and Shuttle
/// reports the deadlock.
#[test]
#[should_panic(expected = "deadlock")]
fn a_driver_that_parks_without_looking_at_the_queue_again_loses_a_wake() {
    fn always_parks(_core: &TurnState) -> NextWait {
        NextWait::Parked
    }
    Runner::new(PctScheduler::new_from_seed(SEED, PCT_DEPTH, SCHEDULES), config()).run(|| model(always_parks));
}
