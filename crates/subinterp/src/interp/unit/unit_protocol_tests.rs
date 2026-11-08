//! Pins a slot's interrupt and end protocol scenario by scenario over a recording host.
//!
//! Orders that need two threads are fixed by channels the host's own calls send on, never by a
//! clock.

use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;

use parking_lot::Mutex;

use super::test_gil::{Gil, GilToken};
use super::test_phase::phase_of_unit;
use super::word::{UnitId, UnitPhase};
use super::{Interpreter, InterruptFailure, Interruption, NewThreadStateError, SlotInner, ThreadGone};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Call {
    Visit { alive: bool },
    Raise(UnitId),
    Clear,
    GaveUpGil,
}

struct FakeInterpreter {
    gil: Gil,
    alive: Mutex<bool>,
    /// Whether a visit can make a thread state, as `PyThreadState_New` may not.
    thread_states: Mutex<bool>,
    pending: Mutex<Option<UnitId>>,
    calls: Mutex<Vec<Call>>,
    /// An interrupter runs this once it is counted in, before it asks for the interpreter.
    before_visit: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    gave_up_gil: Mutex<Option<Sender<()>>>,
}

impl FakeInterpreter {
    fn new() -> Self {
        FakeInterpreter {
            gil: Gil::new(),
            alive: Mutex::new(true),
            thread_states: Mutex::new(true),
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

impl Interpreter for FakeInterpreter {
    type Gil<'py> = GilToken<'py>;
    type Exception = UnitId;

    fn visit<R>(&self, f: impl for<'py> FnOnce(GilToken<'py>) -> R) -> Result<R, NewThreadStateError> {
        let pause = self.before_visit.lock().take();
        if let Some(pause) = pause {
            pause();
        }
        if !*self.thread_states.lock() {
            return Err(NewThreadStateError);
        }
        let gil = self.gil.take();
        self.record(Call::Visit {
            alive: *self.alive.lock(),
        });
        let out = f(gil);
        self.gil.give();
        Ok(out)
    }

    /// Sets only while the thread state is there, as CPython does.
    fn raise(&self, _gil: GilToken<'_>, exc: UnitId) -> Result<(), ThreadGone> {
        self.record(Call::Raise(exc));
        if !*self.alive.lock() {
            return Err(ThreadGone);
        }
        *self.pending.lock() = Some(exc);
        Ok(())
    }

    fn clear(&self, _gil: GilToken<'_>) {
        self.record(Call::Clear);
        *self.pending.lock() = None;
    }

    fn without_gil(&self, _gil: GilToken<'_>, f: impl FnOnce()) {
        self.record(Call::GaveUpGil);
        self.gil.give();
        if let Some(told) = self.gave_up_gil.lock().take() {
            told.send(()).expect("the test listens");
        }
        f();
        self.gil.take();
    }
}

fn new_slot() -> Arc<SlotInner<FakeInterpreter>> {
    Arc::new(SlotInner::new(FakeInterpreter::new()))
}

fn begin(slot: &SlotInner<FakeInterpreter>) -> UnitId {
    let gil = slot.interpreter.gil.take();
    let unit = slot.begin(gil);
    slot.interpreter.gil.give();
    unit
}

/// Ends `slot`'s unit in its GIL, as `WorkUnit::end` does, and returns whether it reported the
/// unit interrupted.
fn end(slot: &SlotInner<FakeInterpreter>, taken_back: bool) -> bool {
    let gil = slot.interpreter.gil.take();
    let interrupted = slot.end_attached(gil, taken_back);
    slot.interpreter.gil.give();
    interrupted
}

fn thread_state_gone(slot: &SlotInner<FakeInterpreter>) {
    *slot.interpreter.alive.lock() = false;
}

/// Pauses the next interrupter before it asks for the interpreter; it reports the pause on the
/// receiver and resumes when the sender is sent to.
fn pause_before_visit(slot: &SlotInner<FakeInterpreter>) -> (Receiver<()>, Sender<()>) {
    let (paused_tx, paused_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    *slot.interpreter.before_visit.lock() = Some(Box::new(move || {
        paused_tx.send(()).expect("the test listens");
        go_rx.recv().expect("the test lets it go");
    }));
    (paused_rx, go_tx)
}

#[test]
fn an_end_waits_for_an_interruption_in_flight_which_then_sets_nothing() {
    let slot = new_slot();
    let unit = begin(&slot);
    let (paused, go) = pause_before_visit(&slot);
    let interrupting = {
        let slot = Arc::clone(&slot);
        std::thread::spawn(move || slot.interrupt(unit, unit))
    };
    paused.recv().expect("the interrupter is counted in");
    assert_eq!(slot.load().in_flight, 1);

    let (gave_up_tx, gave_up) = mpsc::channel::<()>();
    *slot.interpreter.gave_up_gil.lock() = Some(gave_up_tx);
    let (ended_tx, ended) = mpsc::channel::<bool>();
    let worker = {
        let slot = Arc::clone(&slot);
        std::thread::spawn(move || {
            let interrupted = end(&slot, false);
            thread_state_gone(&slot);
            ended_tx.send(interrupted).expect("the test listens");
        })
    };
    gave_up.recv().expect("the end gives the GIL up to wait");
    // The interrupter holds its pass, so the end cannot have returned.
    assert!(
        ended.try_recv().is_err(),
        "the end returned with an interruption in flight"
    );

    go.send(()).expect("the interrupter is paused");
    assert_eq!(
        interrupting.join().expect("the interrupter ends"),
        Ok(Interruption::UnitEnded)
    );
    assert!(!ended.recv().expect("the end returns once the interrupter has left"));
    worker.join().expect("the worker ends");

    assert_eq!(
        slot.interpreter.calls(),
        [Call::GaveUpGil, Call::Visit { alive: true }],
        "the interrupter entered after the end, while the interpreter was alive, and set nothing"
    );
    assert_eq!(*slot.interpreter.pending.lock(), None);
    assert_eq!(slot.load().in_flight, 0);
    assert_eq!(
        phase_of_unit(&slot, unit),
        Some(UnitPhase::Ended { interrupted: false })
    );
}

#[test]
fn a_late_interruption_of_an_ended_unit_does_not_enter_the_interpreter() {
    let slot = new_slot();
    let unit = begin(&slot);
    end(&slot, false);
    thread_state_gone(&slot);

    assert_eq!(slot.interrupt(unit, unit), Ok(Interruption::UnitEnded));

    assert_eq!(slot.interpreter.calls(), []);
    assert_eq!(
        phase_of_unit(&slot, unit),
        Some(UnitPhase::Ended { interrupted: false })
    );
}

#[test]
fn an_interruption_aimed_at_an_earlier_unit_does_not_reach_the_next_one() {
    let slot = new_slot();
    let first = begin(&slot);
    end(&slot, false);
    let next = begin(&slot);
    assert_ne!(next, first);

    assert_eq!(slot.interrupt(first, first), Ok(Interruption::UnitEnded));

    assert_eq!(slot.interpreter.calls(), []);
    assert_eq!(phase_of_unit(&slot, first), None, "an earlier unit's state is not kept");
    assert_eq!(
        phase_of_unit(&slot, next),
        Some(UnitPhase::Running { outstanding: false })
    );
}

#[test]
fn an_interruption_is_set_marked_and_taken_back_once() {
    let slot = new_slot();
    let unit = begin(&slot);

    assert_eq!(slot.interrupt(unit, unit), Ok(Interruption::Set));
    assert_eq!(*slot.interpreter.pending.lock(), Some(unit));
    assert_eq!(
        phase_of_unit(&slot, unit),
        Some(UnitPhase::Running { outstanding: true })
    );

    let gil = slot.interpreter.gil.take();
    assert!(
        slot.take_interruption(gil),
        "the outstanding interruption is taken back"
    );
    assert!(
        !slot.take_interruption(gil),
        "nothing is outstanding after the take-back"
    );
    slot.interpreter.gil.give();
    assert!(
        end(&slot, true),
        "a unit whose interruption was taken back was interrupted"
    );

    assert_eq!(
        slot.interpreter.calls(),
        [Call::Visit { alive: true }, Call::Raise(unit), Call::Clear]
    );
    assert_eq!(*slot.interpreter.pending.lock(), None);
}

#[test]
fn the_end_clears_an_interruption_left_outstanding() {
    let slot = new_slot();
    let unit = begin(&slot);
    assert_eq!(slot.interrupt(unit, unit), Ok(Interruption::Set));

    assert!(end(&slot, false));

    assert_eq!(
        slot.interpreter.calls(),
        [Call::Visit { alive: true }, Call::Raise(unit), Call::Clear]
    );
    assert_eq!(*slot.interpreter.pending.lock(), None);
}

/// A unit leaked rather than ended: the next begin ends it, taking its interruption back and
/// waiting for its interrupters, before the next unit runs.
#[test]
fn a_begin_after_a_leaked_unit_ends_it_first() {
    let slot = new_slot();
    let leaked = begin(&slot);
    assert_eq!(slot.interrupt(leaked, leaked), Ok(Interruption::Set));

    let next = begin(&slot);

    assert_ne!(next, leaked);
    assert_eq!(
        *slot.interpreter.pending.lock(),
        None,
        "the leaked unit's interruption is pending"
    );
    assert_eq!(
        slot.interpreter.calls(),
        [Call::Visit { alive: true }, Call::Raise(leaked), Call::Clear]
    );
    assert_eq!(slot.interrupt(leaked, leaked), Ok(Interruption::UnitEnded));
    assert_eq!(
        phase_of_unit(&slot, next),
        Some(UnitPhase::Running { outstanding: false })
    );
}

#[test]
fn an_interruption_of_a_unit_that_outlived_its_thread_state_reports_it() {
    let slot = new_slot();
    let unit = begin(&slot);
    thread_state_gone(&slot);

    assert_eq!(slot.interrupt(unit, unit), Err(InterruptFailure::ThreadGone));

    assert_eq!(
        slot.interpreter.calls(),
        [Call::Visit { alive: false }, Call::Raise(unit)]
    );
    assert_eq!(*slot.interpreter.pending.lock(), None);
    assert_eq!(
        phase_of_unit(&slot, unit),
        Some(UnitPhase::Running { outstanding: false })
    );
    assert_eq!(slot.load().in_flight, 0);
}

#[test]
fn an_interruption_that_gets_no_thread_state_reports_it_and_sets_nothing() {
    let slot = new_slot();
    let unit = begin(&slot);
    *slot.interpreter.thread_states.lock() = false;

    assert_eq!(
        slot.interrupt(unit, unit),
        Err(InterruptFailure::NoThreadState(NewThreadStateError))
    );

    assert_eq!(slot.interpreter.calls(), []);
    assert_eq!(
        phase_of_unit(&slot, unit),
        Some(UnitPhase::Running { outstanding: false })
    );
    assert_eq!(slot.load().in_flight, 0);
    assert!(!end(&slot, false));
}

/// 255 interrupters are counted in; the 256th is refused, sets nothing, and says so.
#[test]
fn a_full_in_flight_count_refuses_one_more_interruption() {
    let slot = new_slot();
    let unit = begin(&slot);
    let mut passes: Vec<_> = (0..u8::MAX).map(|_| slot.admit(unit).expect("room left")).collect();
    assert_eq!(slot.load().in_flight, u8::MAX);

    assert_eq!(slot.interrupt(unit, unit), Err(InterruptFailure::TooManyInFlight));
    assert_eq!(
        slot.load().in_flight,
        u8::MAX,
        "a refused interrupter changed the count"
    );
    assert_eq!(slot.interpreter.calls(), []);

    passes.pop();
    assert_eq!(
        slot.interrupt(unit, unit),
        Ok(Interruption::Set),
        "a freed place is taken again"
    );
    drop(passes);
    assert_eq!(slot.load().in_flight, 0);
}
