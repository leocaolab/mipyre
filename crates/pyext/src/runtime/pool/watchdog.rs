//! Watches the sync workers: interrupts a request past its budget and abandons and replaces a
//! worker an interruption did not end.

use std::sync::Arc;
use std::time::{Duration, Instant};

use mipyre_subinterp::InterruptTarget;
use mipyre_tpc::{spawn, OsThread};

use super::serve::serve_sync;
use super::threads::{python_thread, AbandonedWorker, WorkerThread};
use super::{answer_queue_if_last, PoolJob};
use crate::runtime::job_queue::JobReceiver;
use crate::runtime::live_workers::WorkerLife;
use crate::runtime::worker::start::WorkerRecipe;
use crate::runtime::worker::sync_runner::SyncRunner;
use crate::runtime::worker::{SubInterpreterWorker, WorkerIndex};
use crate::sync::{AtomicBool, Mutex, Ordering};
use mipyre_http::plan::route::RouteKey;

pub(crate) struct Supervision {
    lanes: Mutex<Lanes>,
    /// The workers the watchdog abandoned are reported when the pool shuts down.
    abandoned: Mutex<Vec<AbandonedWorker>>,
    /// It is indexed by route number.
    routes: Vec<RouteKey>,
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
/// interruption did not end.
pub(super) fn watch(sup: Arc<Supervision>, replacer: Arc<Replacer>, stop: crossbeam_channel::Receiver<()>) {
    let zombie_after = replacer.recipe.spec().limits.request_budget.get();
    let grace = replacer.recipe.spec().limits.interrupt_grace.get();
    loop {
        match stop.recv_timeout(WATCH_INTERVAL) {
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Ok(()) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
        }
        let now = Instant::now();
        let live: Vec<Arc<SyncLane>> = sup.lanes.lock().live.clone();
        for lane in live {
            let action = next_action(&mut lane.work.lock(), now, zombie_after, grace);
            match action {
                LaneAction::Wait => {}
                LaneAction::Interrupt(unit) => interrupt(&lane, unit),
                LaneAction::Abandon => abandon(&sup, &replacer, &lane),
            }
        }
    }
}

/// Interrupts a request past its budget on a thread of its own, because
/// `InterruptTarget::interrupt` waits for the worker's GIL, which a C call can hold indefinitely.
fn interrupt(lane: &SyncLane, unit: InterruptTarget) {
    let index = lane.index;
    tracing::warn!(
        target: "mipyre::server",
        worker = index,
        "a request ran past the request budget; interrupting its worker"
    );
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
    sup.abandoned.lock().push(abandoned);
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
