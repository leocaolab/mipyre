//! Watches the sync workers: interrupts a request past its budget and abandons and replaces a
//! worker an interruption did not end.

use std::num::NonZeroUsize;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use mipyre_subinterp::InterruptTarget;
use mipyre_tpc::{spawn, OsThread};
use mipyre_util::clock;
use mipyre_util::throttle::{ThrottlePeriod, ThrottleState};

use super::serve::serve_sync;
use super::threads::{python_thread, AbandonedWorker, WorkerThread};
use super::{
    answer_queue_if_last, pool_left_abandon_limit, pool_reached_abandon_limit, shift_workers_abandoned, PoolJob,
};
use crate::job_queue::JobReceiver;
use crate::live_workers::WorkerLife;
use crate::sync::{AtomicBool, Mutex, Ordering};
use crate::worker::start::WorkerRecipe;
use crate::worker::sync_runner::SyncRunner;
use crate::worker::{SubInterpreterWorker, WorkerIndex};
use mipyre_http::plan::route::RouteKey;

pub(crate) struct Supervision {
    lanes: Mutex<Lanes>,
    abandoned: Mutex<Vec<Abandonment>>,
    /// It is indexed by route number.
    routes: Vec<RouteKey>,
}

/// A worker the watchdog abandoned: reported when the pool shuts down, and still running while
/// its thread holds its lane, which the thread does until it exits.
struct Abandonment {
    worker: AbandonedWorker,
    lane: Weak<SyncLane>,
}

struct Lanes {
    /// An abandoned lane leaves this list.
    live: Vec<Arc<SyncLane>>,
    /// The shutdown sets it, and no replacement starts afterwards.
    closed: bool,
}

impl Supervision {
    pub(super) fn new(routes: Vec<RouteKey>) -> Arc<Self> {
        Arc::new(Supervision {
            lanes: Mutex::new(Lanes {
                live: Vec::new(),
                closed: false,
            }),
            abandoned: Mutex::new(Vec::new()),
            routes,
        })
    }

    /// Watches `lanes` from now on.
    pub(super) fn watch_lanes(&self, lanes: Vec<Arc<SyncLane>>) {
        self.lanes.lock().live.extend(lanes);
    }

    /// Stops replacing workers and returns the live lanes' threads, for the shutdown to join.
    pub(super) fn close(&self) -> Vec<WorkerThread> {
        let mut lanes = self.lanes.lock();
        lanes.closed = true;
        lanes.live.drain(..).filter_map(|lane| lane.take_thread()).collect()
    }

    /// Returns the workers the watchdog abandoned so far, leaving none.
    pub(super) fn take_abandoned(&self) -> Vec<AbandonedWorker> {
        std::mem::take(&mut *self.abandoned.lock())
            .into_iter()
            .map(|a| a.worker)
            .collect()
    }

    /// Returns how many of the threads the watchdog abandoned still run.
    fn abandoned_running(&self) -> usize {
        self.abandoned
            .lock()
            .iter()
            .filter(|a| a.lane.strong_count() > 0)
            .count()
    }

    /// Returns thread `thread` as abandoned while it runs route number `route`, or none.
    pub(super) fn abandoned_worker(&self, thread: String, route: Option<usize>) -> AbandonedWorker {
        AbandonedWorker {
            thread,
            route: route.and_then(|route| self.routes.get(route)).cloned(),
        }
    }
}

const WATCH_INTERVAL: Duration = Duration::from_millis(100);

const LOG_PERIOD: ThrottlePeriod = ThrottlePeriod::new(Duration::from_secs(60));

/// Returns whether a pool is at its limit of abandoned workers, with `running` it abandoned still
/// running. At the limit a stuck worker keeps its lane, so a handler that never ends cannot grow
/// the process's threads without bound.
fn at_limit(running: usize, limit: NonZeroUsize) -> bool {
    running >= limit.get()
}

#[derive(Debug, PartialEq, Eq)]
enum LimitChange {
    Reached,
    Left,
}

/// Returns how a pool's standing against its limit changed from one look (`was`) to the next
/// (`is`), or `None` when it did not.
fn limit_change(was: bool, is: bool) -> Option<LimitChange> {
    match (was, is) {
        (false, true) => Some(LimitChange::Reached),
        (true, false) => Some(LimitChange::Left),
        _ => None,
    }
}

/// What one watchdog last added to the process's abandon gauges.
#[derive(Clone, Copy, Default)]
struct AbandonGauge {
    running: usize,
    at_limit: bool,
}

impl AbandonGauge {
    /// Moves the process's sums from this watchdog's last share to `next`, and returns `next`.
    fn moved_to(self, next: AbandonGauge) -> AbandonGauge {
        shift_workers_abandoned(self.running as u64, next.running as u64);
        match limit_change(self.at_limit, next.at_limit) {
            Some(LimitChange::Reached) => pool_reached_abandon_limit(),
            Some(LimitChange::Left) => pool_left_abandon_limit(),
            None => {}
        }
        next
    }
}

enum LaneWork<U> {
    Idle,
    Serving {
        /// It is the number of the route the request is for.
        route: usize,
        since: Instant,
        unit: Option<U>,
        interrupted: Option<Instant>,
    },
}

/// What the watchdog does about a lane when it looks: nothing yet, interrupt its request, or
/// abandon its worker.
#[derive(Debug, PartialEq, Eq)]
enum LaneAction<U> {
    Wait,
    Interrupt(U),
    Abandon,
}

/// Decides what the watchdog does about a lane at `now`: a request past `zombie_after` is
/// interrupted once, and a worker still in its request `grace` after that is abandoned. It changes
/// `work` only to record the interruption.
fn next_action<U: Clone>(
    work: &mut LaneWork<U>,
    now: Instant,
    zombie_after: Duration,
    grace: Duration,
) -> LaneAction<U> {
    let LaneWork::Serving {
        since,
        unit,
        interrupted,
        ..
    } = work
    else {
        return LaneAction::Wait;
    };
    match *interrupted {
        None if now.saturating_duration_since(*since) >= zombie_after => match unit {
            Some(unit) => {
                *interrupted = Some(now);
                LaneAction::Interrupt(unit.clone())
            }
            None => LaneAction::Wait,
        },
        Some(at) if now.saturating_duration_since(at) >= grace => LaneAction::Abandon,
        _ => LaneAction::Wait,
    }
}

/// One sync worker's slot. An abandoned worker's replacement gets a new lane and takes the
/// abandoned one's life, and the abandoned thread ends once its request does.
pub(crate) struct SyncLane {
    index: usize,
    /// The worker's thread takes it when it stops serving; the watchdog takes it for the
    /// replacement when it abandons the worker, whose thread then ends counting nothing.
    life: Mutex<Option<WorkerLife>>,
    /// The worker writes it as a request begins and ends; the watchdog and the shutdown read it.
    work: Mutex<LaneWork<InterruptTarget>>,
    /// The watchdog sets it on abandoning the worker; the thread ends after its current request.
    retired: AtomicBool,
    /// Whoever stops the thread takes it: the shutdown or the watchdog.
    handle: Mutex<Option<OsThread<()>>>,
}

impl SyncLane {
    pub(super) fn new(index: usize, life: Option<WorkerLife>) -> Arc<Self> {
        Arc::new(SyncLane {
            index,
            life: Mutex::new(life),
            work: Mutex::new(LaneWork::Idle),
            retired: AtomicBool::new(false),
            handle: Mutex::new(None),
        })
    }

    pub(super) fn begin(&self, route: usize) {
        *self.work.lock() = LaneWork::Serving {
            route,
            since: Instant::now(),
            unit: None,
            interrupted: None,
        };
    }

    pub(super) fn publish_unit(&self, unit: InterruptTarget) {
        if let LaneWork::Serving { unit: slot, .. } = &mut *self.work.lock() {
            *slot = Some(unit);
        }
    }

    pub(super) fn end(&self) {
        *self.work.lock() = LaneWork::Idle;
    }

    /// Returns the number of the route the lane's worker is running, or `None` while it is idle.
    pub(super) fn running_route(&self) -> Option<usize> {
        match *self.work.lock() {
            LaneWork::Serving { route, .. } => Some(route),
            LaneWork::Idle => None,
        }
    }

    pub(super) fn retired(&self) -> bool {
        self.retired.load(Ordering::SeqCst)
    }

    /// Keeps the thread serving this lane, for whoever stops it.
    pub(super) fn hold_thread(&self, handle: OsThread<()>) {
        *self.handle.lock() = Some(handle);
    }

    /// Takes the lane's thread, with the lane that says what it is running, to join it.
    pub(super) fn take_thread(self: &Arc<Self>) -> Option<WorkerThread> {
        let handle = self.handle.lock().take()?;
        Some(WorkerThread {
            handle,
            lane: Some(Arc::clone(self)),
        })
    }

    /// The worker's life, once: for its thread as it stops serving, or for its replacement.
    pub(super) fn take_life(&self) -> Option<WorkerLife> {
        self.life.lock().take()
    }
}

/// Holds owned copies of what a replacement for an abandoned sync worker is built from and serves.
/// Its life comes in its lane.
pub(super) struct Replacer {
    pub(super) recipe: Arc<WorkerRecipe>,
    pub(super) queue: JobReceiver<PoolJob>,
}

impl Replacer {
    fn start(self: &Arc<Self>, lane: Arc<SyncLane>) -> std::io::Result<()> {
        let index = lane.index;
        let for_thread = Arc::clone(&lane);
        let replacer = Arc::clone(self);
        let handle = spawn(python_thread(format!("mipyre-worker-{index}")), move || {
            replace_sync_worker(&replacer, &for_thread)
        })?;
        lane.hold_thread(handle);
        Ok(())
    }
}

fn replace_sync_worker(replacer: &Replacer, lane: &SyncLane) {
    let index = WorkerIndex::new(lane.index);
    let created = SubInterpreterWorker::<SyncRunner>::create(index, &replacer.recipe.spec());
    match created {
        Ok(worker) => serve_sync(
            worker,
            replacer.queue.clone(),
            lane,
            replacer.recipe.spec().limits.request_budget.get(),
        ),
        Err(failure) => {
            tracing::error!(
                target: "mipyre::server",
                worker = lane.index,
                error_type = failure.kind(),
                error = %failure,
                "could not build the replacement for an abandoned worker; the pool serves with one fewer"
            );
            lane.retired.store(true, Ordering::SeqCst);
            if let Some(life) = lane.take_life() {
                answer_queue_if_last(
                    life.died(format!(
                        "the replacement for worker {index} could not be built: {failure}"
                    )),
                    &replacer.queue,
                );
            }
        }
    }
}

/// Runs the watchdog until the stop channel closes: every [`WATCH_INTERVAL`] it interrupts sync
/// requests past the request budget, whose callers already got a 504, and abandons the workers an
/// interruption did not end while fewer than `abandon_limit` it abandoned still run.
pub(super) fn watch(
    sup: Arc<Supervision>,
    replacer: Arc<Replacer>,
    stop: crossbeam_channel::Receiver<()>,
    abandon_limit: NonZeroUsize,
) {
    let zombie_after = replacer.recipe.spec().limits.request_budget.get();
    let grace = replacer.recipe.spec().limits.interrupt_grace.get();
    let mut interrupt_lines = ThrottleState::new();
    let mut gauge = AbandonGauge::default();
    while let Err(crossbeam_channel::RecvTimeoutError::Timeout) = stop.recv_timeout(WATCH_INTERVAL) {
        let now = Instant::now();
        let mut running = sup.abandoned_running();
        let live: Vec<Arc<SyncLane>> = sup.lanes.lock().live.clone();
        for lane in live {
            let action = next_action(&mut lane.work.lock(), now, zombie_after, grace);
            match action {
                LaneAction::Wait => {}
                LaneAction::Interrupt(unit) => {
                    interrupt(&lane, unit, interrupt_lines.admit(clock::now(), LOG_PERIOD));
                }
                LaneAction::Abandon if !at_limit(running, abandon_limit) => {
                    abandon(&sup, &replacer, &lane);
                    running += 1;
                }
                LaneAction::Abandon => {}
            }
        }
        let next = AbandonGauge {
            running,
            at_limit: at_limit(running, abandon_limit),
        };
        say_limit_change(limit_change(gauge.at_limit, next.at_limit), running, abandon_limit);
        gauge = gauge.moved_to(next);
    }
    gauge.moved_to(AbandonGauge::default());
}

fn say_limit_change(change: Option<LimitChange>, running: usize, limit: NonZeroUsize) {
    match change {
        Some(LimitChange::Reached) => tracing::error!(
            target: "mipyre::server",
            running,
            "{running} abandoned sync workers still run, the pool's limit of {limit}: a worker stuck \
             in its request now keeps its lane, and /readyz answers not ready until one ends"
        ),
        Some(LimitChange::Left) => tracing::info!(
            target: "mipyre::server",
            running,
            "{running} abandoned sync workers still run, below the pool's limit of {limit}: stuck \
             workers are abandoned and replaced again"
        ),
        None => {}
    }
}

/// Interrupts a request past its budget on a thread of its own, because
/// `InterruptTarget::interrupt` waits for the worker's GIL, which a C call can hold indefinitely.
/// It logs when the throttle let the line out (`logged`, with the lines held back before it).
fn interrupt(lane: &SyncLane, unit: InterruptTarget, logged: Option<u64>) {
    let index = lane.index;
    if let Some(held) = logged {
        tracing::warn!(
            target: "mipyre::server",
            worker = index,
            held,
            "a request ran past the request budget; interrupting its worker"
        );
    }
    let started = spawn(python_thread(format!("mipyre-interrupt-{index}")), move || {
        if let Err(e) = unit.interrupt() {
            tracing::error!(target: "mipyre::server", worker = index, error = %e,
                "could not interrupt a request past its budget");
        }
    });
    if let Err(e) = started {
        tracing::error!(target: "mipyre::server", worker = index, error = %e,
            "could not start the thread that interrupts a request past its budget");
    }
}

fn abandon(sup: &Supervision, replacer: &Arc<Replacer>, lane: &Arc<SyncLane>) {
    lane.retired.store(true, Ordering::SeqCst);
    let abandoned = sup.abandoned_worker(format!("mipyre-worker-{}", lane.index), lane.running_route());
    tracing::error!(
        target: "mipyre::server",
        "worker thread {abandoned} did not end its request within {:?} of an \
         interruption; abandoning it and starting a replacement",
        replacer.recipe.spec().limits.interrupt_grace.get()
    );
    // Dropping the handle detaches the thread, which is still in its request.
    drop(lane.handle.lock().take());
    let mut lanes = sup.lanes.lock();
    lanes.live.retain(|other| !Arc::ptr_eq(other, lane));
    sup.abandoned.lock().push(Abandonment {
        worker: abandoned,
        lane: Arc::downgrade(lane),
    });
    if lanes.closed {
        return;
    }
    let replacement = SyncLane::new(lane.index, lane.take_life());
    lanes.live.push(Arc::clone(&replacement));
    if let Err(source) = replacer.start(Arc::clone(&replacement)) {
        lanes.live.retain(|other| !Arc::ptr_eq(other, &replacement));
        tracing::error!(
            target: "mipyre::server",
            worker = lane.index,
            error = %source,
            "could not spawn the thread of the replacement for an abandoned worker; the pool serves with one fewer"
        );
        if let Some(life) = replacement.take_life() {
            answer_queue_if_last(
                life.died(format!(
                    "the thread of the replacement for worker {} could not be spawned: {source}",
                    lane.index
                )),
                &replacer.queue,
            );
        }
    }
}

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    use super::*;

    #[test]
    fn a_request_is_interrupted_once_then_its_worker_abandoned_after_the_grace() {
        let budget = Duration::from_secs(30);
        let grace = Duration::from_secs(2);
        let start = Instant::now();
        let mut work = LaneWork::Serving {
            route: 0,
            since: start,
            unit: Some("unit"),
            interrupted: None,
        };
        let at = |secs: u64| start + Duration::from_secs(secs);
        assert_eq!(next_action(&mut work, at(29), budget, grace), LaneAction::Wait);
        assert_eq!(
            next_action(&mut work, at(31), budget, grace),
            LaneAction::Interrupt("unit")
        );
        assert_eq!(next_action(&mut work, at(31), budget, grace), LaneAction::Wait);
        assert_eq!(next_action(&mut work, at(32), budget, grace), LaneAction::Wait);
        assert_eq!(next_action(&mut work, at(34), budget, grace), LaneAction::Abandon);
    }

    #[test]
    fn a_pool_is_at_its_limit_once_as_many_abandoned_workers_run_as_it_has_workers() {
        let limit = NonZeroUsize::new(4).unwrap();
        assert!(!at_limit(3, limit));
        assert!(at_limit(4, limit));
    }

    #[test]
    fn only_a_change_of_standing_against_the_limit_is_a_limit_change() {
        assert_eq!(limit_change(false, true), Some(LimitChange::Reached));
        assert_eq!(limit_change(true, false), Some(LimitChange::Left));
        assert_eq!(limit_change(true, true), None);
        assert_eq!(limit_change(false, false), None);
    }

    #[test]
    fn an_abandoned_worker_runs_while_its_thread_holds_its_lane() {
        let sup = Supervision::new(Vec::new());
        let lane = SyncLane::new(0, None);
        sup.abandoned.lock().push(Abandonment {
            worker: sup.abandoned_worker("mipyre-worker-0".into(), None),
            lane: Arc::downgrade(&lane),
        });
        assert_eq!(sup.abandoned_running(), 1);
        drop(lane);
        assert_eq!(sup.abandoned_running(), 0);
        assert_eq!(
            sup.take_abandoned().len(),
            1,
            "an ended one is still reported at shutdown"
        );
    }

    #[test]
    fn a_lane_says_the_route_its_worker_runs_and_none_once_it_ends() {
        let lane = SyncLane::new(0, None);
        assert_eq!(lane.running_route(), None);
        lane.begin(usize::MAX);
        assert_eq!(lane.running_route(), Some(usize::MAX));
        lane.begin(0);
        assert_eq!(lane.running_route(), Some(0));
        lane.end();
        assert_eq!(lane.running_route(), None);
    }

    #[test]
    fn a_request_not_yet_begun_is_not_interrupted_and_an_idle_lane_waits() {
        let budget = Duration::from_secs(30);
        let grace = Duration::from_secs(2);
        let start = Instant::now();
        let later = start + Duration::from_secs(60);
        let mut not_begun: LaneWork<&str> = LaneWork::Serving {
            route: 0,
            since: start,
            unit: None,
            interrupted: None,
        };
        assert_eq!(next_action(&mut not_begun, later, budget, grace), LaneAction::Wait);
        let mut idle: LaneWork<&str> = LaneWork::Idle;
        assert_eq!(next_action(&mut idle, later, budget, grace), LaneAction::Wait);
    }
}
