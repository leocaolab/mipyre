//! Pins a slot's interrupt and end protocol scenario by scenario over a recording host.
//!
//! Orders that need two threads are fixed by channels the host's own calls send on, never by a
//! clock.

use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;

use parking_lot::Mutex;

use super::test_gil::{Gil, Held};
use super::{InterruptError, RunId, UnitHost, UnitPhase, UnitState, COUNT_MASK};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Call {
    Visit {
        alive: bool,
    },
    /// `None` means the exception was cleared.
    SetRaise(Option<RunId>),
    GaveUpGil,
}

struct RecordingHost {
    gil: Gil,
    alive: Mutex<bool>,
    pending: Mutex<Option<RunId>>,
    calls: Mutex<Vec<Call>>,
    /// An interruption runs this once it is counted in, before it asks for the interpreter.
    before_visit: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    gave_up_gil: Mutex<Option<Sender<()>>>,
}

impl RecordingHost {
    fn new() -> Self {
        RecordingHost {
            gil: Gil::new(),
            alive: Mutex::new(true),
            pending: Mutex::new(None),
            calls: Mutex::new(Vec::new()),
            before_visit: Mutex::new(None),
            gave_up_gil: Mutex::new(None),
        }
    }

    fn record(&self, call: Call) {
        self.calls.lock().push(call);
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().clone()
    }
}

impl UnitHost for RecordingHost {
    type Gil<'py> = Held<'py>;
    type Exception = RunId;

    fn visit<R>(&self, f: impl for<'py> FnOnce(Held<'py>) -> R) -> Option<R> {
        let pause = self.before_visit.lock().take();
        if let Some(pause) = pause {
            pause();
        }
        let gil = self.gil.take();
        self.record(Call::Visit {
            alive: *self.alive.lock(),
        });
        let out = f(gil);
        self.gil.give();
        Some(out)
    }

    /// Sets or clears only while the thread state is there, as CPython does.
    fn set_raise(&self, _gil: Held<'_>, exc: Option<RunId>) -> bool {
        self.record(Call::SetRaise(exc));
        let found = *self.alive.lock();
        if found {
            *self.pending.lock() = exc;
        }
        found
    }

    fn thread_gone(&self, _gil: Held<'_>) -> InterruptError {
        InterruptError::UnitThreadGone { thread: 0, interp: 0 }
    }

    fn without_gil(&self, _gil: Held<'_>, f: impl FnOnce()) {
        self.record(Call::GaveUpGil);
        self.gil.give();
        if let Some(told) = self.gave_up_gil.lock().take() {
            told.send(()).expect("the test listens");
        }
        f();
        self.gil.take();
    }
}

fn new_slot() -> Arc<UnitState<RecordingHost>> {
    Arc::new(UnitState::new(RecordingHost::new()))
}

/// Ends `slot`'s run in its GIL, as `WorkUnit::end` does.
fn end(slot: &UnitState<RecordingHost>) {
    let gil = slot.host.gil.take();
    slot.end_attached(gil);
    slot.host.gil.give();
}

fn thread_state_gone(slot: &UnitState<RecordingHost>) {
    *slot.host.alive.lock() = false;
}

/// Pauses the next interruption before it asks for the interpreter; it reports the pause on
/// the receiver and resumes when the sender is sent to.
fn pause_before_visit(slot: &UnitState<RecordingHost>) -> (Receiver<()>, Sender<()>) {
    let (paused_tx, paused_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    *slot.host.before_visit.lock() = Some(Box::new(move || {
        paused_tx.send(()).expect("the test listens");
        go_rx.recv().expect("the test lets it go");
    }));
    (paused_rx, go_tx)
}

#[test]
fn an_end_waits_for_an_interruption_in_flight_which_then_sets_nothing() {
    let slot = new_slot();
    let (run, _) = slot.begin_run();
    let (paused, go) = pause_before_visit(&slot);
    let interrupting = {
        let slot = Arc::clone(&slot);
        std::thread::spawn(move || slot.interrupt(run, run))
    };
    paused.recv().expect("the interruption is counted in");
    assert_eq!(slot.in_flight(), 1);

    let (gave_up_tx, gave_up) = mpsc::channel::<()>();
    *slot.host.gave_up_gil.lock() = Some(gave_up_tx);
    let (ended_tx, ended) = mpsc::channel::<()>();
    let worker = {
        let slot = Arc::clone(&slot);
        std::thread::spawn(move || {
            end(&slot);
            thread_state_gone(&slot);
            ended_tx.send(()).expect("the test listens");
        })
    };
    gave_up.recv().expect("the end gives the GIL up to wait");
    // The interruption holds its pass, so the end cannot have returned.
    assert!(
        ended.try_recv().is_err(),
        "the end returned with an interruption in flight"
    );

    go.send(()).expect("the interruption is paused");
    interrupting
        .join()
        .expect("the interruption ends")
        .expect("the host always enters");
    ended.recv().expect("the end returns once the interruption has left");
    worker.join().expect("the worker ends");

    assert_eq!(
        slot.host.calls(),
        [Call::GaveUpGil, Call::Visit { alive: true }],
        "the interruption entered after the end, while the interpreter was alive, and set nothing"
    );
    assert_eq!(*slot.host.pending.lock(), None);
    assert_eq!(slot.in_flight(), 0);
    let phase = slot.phase_of_run(run).expect("no run began since");
    assert_eq!(
        phase,
        UnitPhase::Ended,
        "an interruption that set nothing marked the run"
    );
}

#[test]
fn a_late_interruption_of_an_ended_run_does_not_enter_the_interpreter() {
    let slot = new_slot();
    let (run, _) = slot.begin_run();
    end(&slot);
    thread_state_gone(&slot);

    slot.interrupt(run, run).expect("an ended run is not an error");

    assert_eq!(slot.host.calls(), []);
    assert_eq!(slot.phase_of_run(run), Some(UnitPhase::Ended));
}

#[test]
fn an_interruption_aimed_at_an_earlier_run_does_not_reach_the_next_one() {
    let slot = new_slot();
    let (first, _) = slot.begin_run();
    end(&slot);
    let (next, before) = slot.begin_run();
    assert_ne!(next, first);
    assert!(UnitPhase::from_word(before).has_ended());

    slot.interrupt(first, first).expect("a stale target is not an error");

    assert_eq!(slot.host.calls(), []);
    assert_eq!(slot.phase_of_run(first), None, "an earlier run's state is not kept");
    assert_eq!(slot.phase_of_run(next), Some(UnitPhase::Running));
}

#[test]
fn an_interruption_is_set_marked_and_taken_back_once() {
    let slot = new_slot();
    let (run, _) = slot.begin_run();

    slot.interrupt(run, run).expect("the host always enters");
    assert_eq!(*slot.host.pending.lock(), Some(run));
    assert_eq!(slot.phase_of_run(run), Some(UnitPhase::RunningInterruptOutstanding));

    let gil = slot.host.gil.take();
    assert!(
        slot.take_interruption(gil),
        "the outstanding interruption is taken back"
    );
    assert!(
        !slot.take_interruption(gil),
        "nothing is outstanding after the take-back"
    );
    slot.host.gil.give();
    end(&slot);

    assert_eq!(
        slot.host.calls(),
        [
            Call::Visit { alive: true },
            Call::SetRaise(Some(run)),
            Call::SetRaise(None),
        ]
    );
    assert_eq!(*slot.host.pending.lock(), None);
    assert_eq!(slot.phase_of_run(run), Some(UnitPhase::EndedInterrupted));
}

#[test]
fn the_end_clears_an_interruption_left_outstanding() {
    let slot = new_slot();
    let (run, _) = slot.begin_run();
    slot.interrupt(run, run).expect("the host always enters");

    end(&slot);

    assert_eq!(
        slot.host.calls(),
        [
            Call::Visit { alive: true },
            Call::SetRaise(Some(run)),
            Call::SetRaise(None),
        ]
    );
    assert_eq!(*slot.host.pending.lock(), None);
    assert_eq!(slot.phase_of_run(run), Some(UnitPhase::EndedInterrupted));
}

/// Covers a unit leaked while running, never ended or dropped, whose thread state then goes.
#[test]
fn an_interruption_of_a_run_that_outlived_its_thread_state_reports_it() {
    let slot = new_slot();
    let (run, _) = slot.begin_run();
    thread_state_gone(&slot);

    assert!(matches!(
        slot.interrupt(run, run),
        Err(InterruptError::UnitThreadGone { .. })
    ));

    assert_eq!(
        slot.host.calls(),
        [Call::Visit { alive: false }, Call::SetRaise(Some(run))]
    );
    assert_eq!(*slot.host.pending.lock(), None);
    assert_eq!(slot.phase_of_run(run), Some(UnitPhase::Running));
    assert_eq!(slot.in_flight(), 0);
}

#[test]
fn a_full_in_flight_count_refuses_one_more_visit() {
    let slot = new_slot();
    let (run, _) = slot.begin_run();
    let mut passes: Vec<_> = (0..COUNT_MASK - 1)
        .map(|_| slot.enter_visit(run).expect("room left"))
        .collect();
    passes.push(slot.enter_visit(run).expect("the last place is taken"));
    assert_eq!(slot.in_flight(), COUNT_MASK);
    assert!(slot.enter_visit(run).is_none(), "a full count took one more");
    assert_eq!(slot.in_flight(), COUNT_MASK, "a refused visit changed the count");

    passes.pop();
    assert!(slot.enter_visit(run).is_some(), "a freed place is taken again");
    assert_eq!(slot.phase_of_run(run), Some(UnitPhase::Running));
}
