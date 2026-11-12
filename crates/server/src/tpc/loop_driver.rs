//! Drives a TPC thread's event loop from the thread's tokio runtime: after each turn the driver
//! turns again, or waits for the loop's bell, for its poller or for a time of its stop, without
//! holding the thread. At the stop, or once the worker retires, it ends the loop: it cancels the
//! request Tasks still live, starts the loop's finalizers once none is, and closes it.
//!
//! The driver turns the loop, which runs the callbacks, but decides nothing about a request: the
//! request side (`on_loop.rs`) owns the seat, the budget, the 503 and the 504. It does not know
//! when the thread's connections are gone, which is the thread's drain to say by bringing the
//! stop's cancel time forward; whether the worker retires is the runtime's to say, through a turn.

use std::rc::Rc;
use std::time::Duration;

use tokio::sync::{oneshot, watch};
use tokio::time::Instant;

use mipyre_runtime::NextWait;

use super::conn::{LoopWakeups, LoopWorker, TpcThread};
use super::ServeError;
use crate::counters::LoopTasksGauge;
use crate::ServerCounters;

/// Sets how long a worker that retired lets its loop's Tasks end, cancelled, and its finalizers
/// run.
const RETIRE_GRACE: Duration = Duration::from_secs(1);

/// Sets the most of a stop's drain that is left for the Tasks to end once cancelled.
const MAX_CANCEL_MARGIN: Duration = Duration::from_secs(1);

/// Returns how long before the end of a stop's drain the Tasks still live are cancelled: a tenth
/// of the drain, at most a second.
fn cancel_margin(drain: Duration) -> Duration {
    (drain / 10).min(MAX_CANCEL_MARGIN)
}

/// When a stopped loop cancels its live Tasks, and when it gives up on them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct DriverStop {
    cancel_at: Instant,
    until: Instant,
}

impl DriverStop {
    /// Returns the stop of a drain of `drain` from `now`.
    pub(super) fn draining(now: Instant, drain: Duration) -> Self {
        let until = now + drain;
        DriverStop {
            cancel_at: until - cancel_margin(drain),
            until,
        }
    }

    /// Returns the stop of a worker that retired at `now`: its Tasks are cancelled at once.
    fn retiring(now: Instant) -> Self {
        DriverStop {
            cancel_at: now,
            until: now + RETIRE_GRACE,
        }
    }

    /// Returns this stop with the Tasks cancelled by `now` at the latest: once the thread's
    /// connections are gone, no reply has anyone to take it.
    pub(super) fn cancelled_by(self, now: Instant) -> Self {
        DriverStop {
            cancel_at: self.cancel_at.min(now),
            until: self.until,
        }
    }

    /// Returns the stop that keeps the earlier of each of the two stops' times.
    fn earliest(self, other: DriverStop) -> Self {
        DriverStop {
            cancel_at: self.cancel_at.min(other.cancel_at),
            until: self.until.min(other.until),
        }
    }
}

/// How far a loop's life has gone: serving requests, or ending by a stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DriverPhase {
    Serving,
    Ending { stop: DriverStop, end: EndStep },
}

/// Where the end of a loop's life has got to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EndStep {
    /// The live Tasks run on, and requests still come, until the stop's cancel time.
    Draining,
    /// The live Tasks were cancelled; the finalizers start once none is left.
    Cancelled,
    /// The finalizers run; the loop closes once they end, or at the stop's end.
    Finalizing,
}

/// What the driver knows after a turn.
#[derive(Clone, Copy, Debug)]
struct DriverFacts {
    now: Instant,
    /// It counts the request Tasks live on the loop.
    live: usize,
    /// It is the stop the thread gave, if it gave one.
    given: Option<DriverStop>,
    retired: bool,
    /// It says the thread can no longer stop the driver: it dropped the driver without a stop, or
    /// the runtime's reactor the driver waits on shut down.
    cut_off: bool,
    /// It says the finalizers ended, however they ended.
    finalized: bool,
    /// It says the turn left callbacks ready, or fds possibly still ready.
    turn_again: bool,
}

/// What the driver does after a turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DriverAction {
    /// It cancels the live request Tasks and turns again.
    Cancel,
    /// It starts the loop's finalizers, which wait this long at most, and turns again.
    StartFinalizers(Duration),
    /// It closes the loop, for the reason given.
    Close(CloseCause),
    /// It turns again once the thread's other tasks had their go.
    Turn,
    /// It waits for the bell, the poller, the worker's retirement, a new stop or the time given.
    Wait(Option<Instant>),
}

/// Why the driver closes the loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CloseCause {
    FinalizersEnded,
    /// The stop's deadline came while request Tasks were still live, so the finalizers never
    /// started.
    DeadlineBeforeFinalizers,
    /// The stop's deadline came while the finalizers ran: a Task ignored its cancel, or the
    /// default executor's threads were still busy.
    DeadlineDuringFinalizers,
}

/// Returns the loop's next phase and what the driver does, from its phase and what a turn left.
/// A retirement, or a driver cut off from its thread, ends the loop as a worker that retired now.
/// The finalizers start only once the live Tasks were cancelled and none is left: before the
/// cancel time the thread's connections may still start Tasks.
fn step(phase: DriverPhase, facts: DriverFacts) -> (DriverPhase, DriverAction) {
    let at_once = (facts.retired || facts.cut_off).then(|| DriverStop::retiring(facts.now));
    let told = [facts.given, at_once]
        .into_iter()
        .flatten()
        .reduce(DriverStop::earliest);
    let (stop, end) = match (phase, told) {
        (DriverPhase::Serving, None) => return (DriverPhase::Serving, wait(facts, None)),
        (DriverPhase::Serving, Some(stop)) => (stop, EndStep::Draining),
        (DriverPhase::Ending { stop, end }, told) => (told.map_or(stop, |told| stop.earliest(told)), end),
    };
    let (end, action) = match end {
        EndStep::Draining if facts.now >= stop.cancel_at => (EndStep::Cancelled, DriverAction::Cancel),
        EndStep::Cancelled if facts.live == 0 => (
            EndStep::Finalizing,
            DriverAction::StartFinalizers(stop.until.saturating_duration_since(facts.now)),
        ),
        EndStep::Finalizing if facts.finalized => (end, DriverAction::Close(CloseCause::FinalizersEnded)),
        EndStep::Finalizing if facts.now >= stop.until => {
            (end, DriverAction::Close(CloseCause::DeadlineDuringFinalizers))
        }
        _ if facts.now >= stop.until => (end, DriverAction::Close(CloseCause::DeadlineBeforeFinalizers)),
        EndStep::Draining => (end, wait(facts, Some(stop.cancel_at))),
        EndStep::Cancelled | EndStep::Finalizing => (end, wait(facts, Some(stop.until))),
    };
    (DriverPhase::Ending { stop, end }, action)
}

/// Returns whether the driver turns again at once or waits, until `until` at the latest.
fn wait(facts: DriverFacts, until: Option<Instant>) -> DriverAction {
    if facts.turn_again {
        DriverAction::Turn
    } else {
        DriverAction::Wait(until)
    }
}

/// The driver of a TPC thread's loop: a task on the thread's `LocalSet`, the sweeper of its
/// request budgets, and how to stop them.
pub(super) struct LoopDriver {
    /// It holds the stop the driver goes by, `None` while it serves; a later stop can only bring
    /// the times forward.
    stop: watch::Sender<Option<DriverStop>>,
    task: tokio::task::JoinHandle<()>,
    sweeper: tokio::task::JoinHandle<()>,
}

impl LoopDriver {
    /// Opens the loop of `tpc_thread`'s worker and starts driving it; a thread with no `async def`
    /// route has none.
    ///
    /// # Errors
    ///
    /// The loop could not be opened, or its poller not registered with this thread's runtime.
    pub(super) fn start(tpc_thread: &Rc<TpcThread>) -> Result<Option<LoopDriver>, ServeError> {
        let Some(on) = tpc_thread.on_loop() else {
            return Ok(None);
        };
        let wakeups = on.open()?;
        let (stop, stopped) = watch::channel(None);
        let task = tokio::task::spawn_local(drive(Rc::clone(tpc_thread), wakeups, stopped));
        let swept = Rc::clone(tpc_thread);
        let sweeper = tokio::task::spawn_local(async move {
            if let Some(on) = swept.on_loop() {
                on.event_loop.deadlines.sweep().await;
            }
        });
        Ok(Some(LoopDriver { stop, task, sweeper }))
    }

    /// Stops the loop as `stop` says; a driver whose worker retired has stopped already and keeps
    /// the earlier times.
    pub(super) fn stop(&self, stop: DriverStop) {
        self.stop.send_replace(Some(stop));
    }

    /// Waits until the stopped loop is closed.
    pub(super) async fn finish(self) {
        let LoopDriver {
            stop: stopping,
            task,
            sweeper,
        } = self;
        if let Err(ended) = task.await {
            tracing::error!(
                target: "mipyre::server",
                error = %ended,
                "the driver of a TPC thread's event loop ended abnormally; the loop is closed with the worker"
            );
        }
        sweeper.abort();
        drop(stopping);
    }
}

/// Turns `tpc_thread`'s loop until it ends, waiting on `wakeups` when it parks; then closes it.
async fn drive(tpc_thread: Rc<TpcThread>, wakeups: LoopWakeups, mut stopped: watch::Receiver<Option<DriverStop>>) {
    let Some(on) = tpc_thread.on_loop() else {
        return;
    };
    let LoopWakeups { bell, poller } = wakeups;
    let thread = tpc_thread.index.get();
    let counters = &tpc_thread.site.counters.run;
    let gauge = counters.loop_tasks_gauge(thread);
    let mut phase = DriverPhase::Serving;
    let mut finalizers: Option<oneshot::Receiver<()>> = None;
    let mut unstoppable = false;
    let mut reactor_gone = false;
    let cause = loop {
        let wait = on.turn();
        // Only this driver closes the loop, so it is open here unless the thread ended it.
        let Some(live) = on.live() else { break None };
        gauge.set(live.count);
        let retired = on.worker.has_retired();
        let facts = DriverFacts {
            now: Instant::now(),
            live: live.count,
            given: *stopped.borrow_and_update(),
            retired,
            cut_off: unstoppable || reactor_gone,
            finalized: finalizers.as_mut().is_some_and(finalized),
            turn_again: wait == NextWait::Now,
        };
        let action;
        (phase, action) = step(phase, facts);
        match action {
            DriverAction::Cancel => cancel(on, thread),
            DriverAction::StartFinalizers(timeout) => match on.start_finalizers(timeout) {
                Ok(Some(running)) => finalizers = Some(running),
                Ok(None) => break None,
                Err(failed) => {
                    tracing::warn!(
                        target: "mipyre::server",
                        worker = thread,
                        error = %failed,
                        "the TPC thread's event loop's finalizers could not start; it closes without them"
                    );
                    break None;
                }
            },
            DriverAction::Close(cause) => break Some(cause),
            DriverAction::Turn => tokio::task::yield_now().await,
            DriverAction::Wait(until) => tokio::select! {
                () = bell.rung() => {}
                waited = poller.wait(), if !reactor_gone => {
                    if let Err(error) = waited {
                        tracing::error!(
                            target: "mipyre::server",
                            worker = thread,
                            %error,
                            "the runtime failed waiting on the TPC thread's event loop poller; the loop ends"
                        );
                        reactor_gone = true;
                    }
                }
                () = on.worker.retired(), if !retired => {}
                changed = stopped.changed(), if !unstoppable => unstoppable = changed.is_err(),
                () = sleep_until(until) => {}
                () = finalizers_end(&mut finalizers) => {}
            },
        }
    };
    close(on, thread, counters, &gauge);
    if let Some(cause) = cause {
        report_close(cause, thread, counters);
    }
}

/// Cancels the loop's live Tasks as its life ends; a failed cancel is logged, and the loop ends
/// by its deadline all the same.
fn cancel(on: LoopWorker<'_>, thread: usize) {
    if let Err(failed) = on.cancel_live_tasks() {
        tracing::warn!(
            target: "mipyre::server",
            worker = thread,
            error = %failed,
            "cancelling the request Tasks on the TPC thread's event loop failed; it closes by its deadline"
        );
    }
}

/// Closes the loop, and counts and logs the request Tasks it dropped.
fn close(on: LoopWorker<'_>, thread: usize, counters: &ServerCounters, gauge: &LoopTasksGauge) {
    let left = on.close();
    gauge.set(0);
    let Some(left) = left.filter(|left| left.count > 0) else {
        return;
    };
    counters.loop_tasks_left(left.count);
    tracing::warn!(
        target: "mipyre::server",
        worker = thread,
        tasks = left.count,
        "the TPC thread's event loop closed with request Tasks still live; they are dropped"
    );
}

/// Logs and counts a loop closed at the stop's deadline before its finalizers ended; a loop whose
/// finalizers ended says nothing.
fn report_close(cause: CloseCause, thread: usize, counters: &ServerCounters) {
    let why = match cause {
        CloseCause::FinalizersEnded => return,
        CloseCause::DeadlineBeforeFinalizers => {
            "the TPC thread's event loop closed at the stop's deadline before its finalizers started, as request \
             Tasks were still live; the Tasks the handlers spawned were not cancelled, and its async generators and \
             default executor were not shut down"
        }
        CloseCause::DeadlineDuringFinalizers => {
            "the TPC thread's event loop closed at the stop's deadline before its finalizers ended: a Task ignored \
             its cancel or the default executor's threads were busy; its async generators or default executor may \
             not be shut down"
        }
    };
    counters.loop_finalizers_cut_off();
    tracing::warn!(target: "mipyre::server", worker = thread, "{why}");
}

/// Completes at `until`, and never without it.
async fn sleep_until(until: Option<Instant>) {
    match until {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Returns whether running finalizers have ended, however they ended.
fn finalized(running: &mut oneshot::Receiver<()>) -> bool {
    !matches!(running.try_recv(), Err(oneshot::error::TryRecvError::Empty))
}

/// Completes when running finalizers end, and never before they start; the next round sees it.
async fn finalizers_end(finalizers: &mut Option<oneshot::Receiver<()>>) {
    match finalizers {
        Some(running) => {
            // Either way they ended: a sender dropped without a word went with its Task.
            let _ended = running.await;
        }
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(now: Instant) -> DriverFacts {
        DriverFacts {
            now,
            live: 0,
            given: None,
            retired: false,
            cut_off: false,
            finalized: false,
            turn_again: false,
        }
    }

    fn ending(stop: DriverStop, end: EndStep) -> DriverPhase {
        DriverPhase::Ending { stop, end }
    }

    #[test]
    fn a_stop_brought_forward_keeps_the_earlier_times() {
        let now = Instant::now();
        let draining = DriverStop::draining(now, Duration::from_secs(10));
        let drained = draining.cancelled_by(now + Duration::from_secs(2));
        assert_eq!(drained.cancel_at, now + Duration::from_secs(2));
        assert_eq!(drained.until, draining.until);
        assert_eq!(draining.cancelled_by(now + Duration::from_secs(20)), draining);
        let retiring = DriverStop::retiring(now);
        assert_eq!(retiring.earliest(draining), retiring);
        assert_eq!(draining.earliest(retiring), retiring);
    }

    #[test]
    fn the_cancel_margin_is_a_tenth_of_the_drain_up_to_a_second() {
        assert_eq!(cancel_margin(Duration::from_secs(5)), Duration::from_millis(500));
        assert_eq!(cancel_margin(Duration::from_secs(10)), Duration::from_secs(1));
        assert_eq!(cancel_margin(Duration::from_secs(20)), Duration::from_secs(1));
    }

    #[test]
    fn a_serving_loop_waits_without_a_time_and_turns_again_when_the_turn_left_work() {
        let now = Instant::now();
        assert_eq!(
            step(DriverPhase::Serving, facts(now)),
            (DriverPhase::Serving, DriverAction::Wait(None))
        );
        let busy = DriverFacts {
            turn_again: true,
            ..facts(now)
        };
        assert_eq!(
            step(DriverPhase::Serving, busy),
            (DriverPhase::Serving, DriverAction::Turn)
        );
    }

    #[test]
    fn a_stop_drains_until_its_cancel_time_and_cancels_at_it() {
        let now = Instant::now();
        let stop = DriverStop::draining(now, Duration::from_secs(10));
        let given = DriverFacts {
            given: Some(stop),
            live: 1,
            ..facts(now)
        };
        assert_eq!(
            step(DriverPhase::Serving, given),
            (
                ending(stop, EndStep::Draining),
                DriverAction::Wait(Some(stop.cancel_at))
            )
        );
        let before = DriverFacts {
            now: stop.cancel_at - Duration::from_nanos(1),
            ..given
        };
        assert_eq!(
            step(ending(stop, EndStep::Draining), before),
            (
                ending(stop, EndStep::Draining),
                DriverAction::Wait(Some(stop.cancel_at))
            )
        );
        let at = DriverFacts {
            now: stop.cancel_at,
            ..given
        };
        assert_eq!(
            step(ending(stop, EndStep::Draining), at),
            (ending(stop, EndStep::Cancelled), DriverAction::Cancel)
        );
    }

    #[test]
    fn a_draining_loop_with_no_live_task_waits_for_the_cancel_time_and_does_not_finalize() {
        let now = Instant::now();
        let stop = DriverStop::draining(now, Duration::from_secs(10));
        let idle = DriverFacts {
            given: Some(stop),
            ..facts(now)
        };
        assert_eq!(
            step(ending(stop, EndStep::Draining), idle),
            (
                ending(stop, EndStep::Draining),
                DriverAction::Wait(Some(stop.cancel_at))
            )
        );
    }

    #[test]
    fn the_connections_drained_bring_the_cancel_forward() {
        let now = Instant::now();
        let stop = DriverStop::draining(now, Duration::from_secs(10));
        let later = now + Duration::from_secs(2);
        let drained = DriverFacts {
            given: Some(stop.cancelled_by(later)),
            ..facts(later)
        };
        assert_eq!(
            step(ending(stop, EndStep::Draining), drained),
            (
                ending(stop.cancelled_by(later), EndStep::Cancelled),
                DriverAction::Cancel
            )
        );
    }

    #[test]
    fn the_finalizers_start_once_no_cancelled_task_is_left_and_not_before() {
        let now = Instant::now();
        let stop = DriverStop::draining(now, Duration::from_secs(10));
        let none_left = DriverFacts {
            given: Some(stop),
            ..facts(stop.cancel_at)
        };
        let one_left = DriverFacts { live: 1, ..none_left };
        assert_eq!(
            step(ending(stop, EndStep::Cancelled), one_left),
            (ending(stop, EndStep::Cancelled), DriverAction::Wait(Some(stop.until)))
        );
        assert_eq!(
            step(ending(stop, EndStep::Cancelled), none_left),
            (
                ending(stop, EndStep::Finalizing),
                DriverAction::StartFinalizers(stop.until - stop.cancel_at)
            )
        );
    }

    #[test]
    fn the_loop_closes_once_its_finalizers_end_or_at_the_stops_end() {
        let now = Instant::now();
        let stop = DriverStop::draining(now, Duration::from_secs(10));
        let running = DriverFacts {
            given: Some(stop),
            ..facts(stop.until - Duration::from_nanos(1))
        };
        assert_eq!(
            step(ending(stop, EndStep::Finalizing), running),
            (ending(stop, EndStep::Finalizing), DriverAction::Wait(Some(stop.until)))
        );
        let ended = DriverFacts {
            finalized: true,
            ..running
        };
        assert_eq!(
            step(ending(stop, EndStep::Finalizing), ended),
            (
                ending(stop, EndStep::Finalizing),
                DriverAction::Close(CloseCause::FinalizersEnded)
            )
        );
        let over = DriverFacts {
            now: stop.until,
            ..running
        };
        assert_eq!(
            step(ending(stop, EndStep::Finalizing), over),
            (
                ending(stop, EndStep::Finalizing),
                DriverAction::Close(CloseCause::DeadlineDuringFinalizers)
            )
        );
    }

    #[test]
    fn cancelled_tasks_still_live_at_the_stops_end_are_left_to_the_close() {
        let now = Instant::now();
        let stop = DriverStop::draining(now, Duration::from_secs(10));
        let stuck = DriverFacts {
            given: Some(stop),
            live: 1,
            ..facts(stop.until)
        };
        assert_eq!(
            step(ending(stop, EndStep::Cancelled), stuck),
            (
                ending(stop, EndStep::Cancelled),
                DriverAction::Close(CloseCause::DeadlineBeforeFinalizers)
            )
        );
    }

    #[test]
    fn a_worker_that_retires_or_a_driver_cut_off_while_serving_cancels_its_tasks_at_once() {
        let now = Instant::now();
        let retired = DriverFacts {
            retired: true,
            live: 1,
            ..facts(now)
        };
        assert_eq!(
            step(DriverPhase::Serving, retired),
            (
                ending(DriverStop::retiring(now), EndStep::Cancelled),
                DriverAction::Cancel
            )
        );
        let cut_off = DriverFacts {
            cut_off: true,
            ..facts(now)
        };
        assert_eq!(
            step(DriverPhase::Serving, cut_off),
            (
                ending(DriverStop::retiring(now), EndStep::Cancelled),
                DriverAction::Cancel
            )
        );
    }

    #[test]
    fn a_worker_that_retires_while_draining_brings_the_stop_forward() {
        let now = Instant::now();
        let stop = DriverStop::draining(now, Duration::from_secs(10));
        let later = now + Duration::from_secs(1);
        let retired = DriverFacts {
            given: Some(stop),
            retired: true,
            live: 1,
            ..facts(later)
        };
        assert_eq!(
            step(ending(stop, EndStep::Draining), retired),
            (
                ending(DriverStop::retiring(later), EndStep::Cancelled),
                DriverAction::Cancel
            )
        );
    }
    #[test]
    fn a_loop_closed_at_the_deadline_before_its_finalizers_ended_is_logged_once_and_counted() {
        let counters = ServerCounters::new(false);
        let log = mipyre_runtime::testkit::captured_log(|| {
            report_close(CloseCause::DeadlineDuringFinalizers, 3, &counters);
            report_close(CloseCause::DeadlineBeforeFinalizers, 4, &counters);
        });
        assert_eq!(counters.read().loop_finalizers_cut_off, 2);
        let lines: Vec<&str> = log
            .lines()
            .filter(|l| l.contains("closed at the stop's deadline"))
            .collect();
        assert_eq!(lines.len(), 2, "{log}");
        assert!(
            lines[0].contains("before its finalizers ended") && lines[0].contains("\"worker\":3"),
            "{log}"
        );
        assert!(
            lines[1].contains("before its finalizers started") && lines[1].contains("\"worker\":4"),
            "{log}"
        );
        assert!(lines.iter().all(|l| l.contains("\"level\":\"WARN\"")), "{log}");
    }

    #[test]
    fn a_loop_whose_finalizers_ended_says_nothing_and_counts_nothing() {
        let counters = ServerCounters::new(false);
        let log = mipyre_runtime::testkit::captured_log(|| report_close(CloseCause::FinalizersEnded, 3, &counters));
        assert_eq!(counters.read().loop_finalizers_cut_off, 0);
        assert!(!log.contains("closed at the stop's deadline"), "{log}");
    }
}
