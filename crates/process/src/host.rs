//! Runs the server side of the process lanes: the hosts that put requests into worker
//! processes' lanes and read their answers.
//!
//! Each lane belongs to one *host*: an event-loop thread that is the only producer of the
//! lane's requests and the only consumer of its answers (single producer, single consumer,
//! no lock on the lane). On the default TPC server the hosts are the TPC threads
//! themselves (lane `j` belongs to TPC thread `j % H`, `H = min(lanes, TPC threads)`); a
//! request on a TPC thread that hosts lanes goes into one of its own lanes and its answer
//! is read and completed on that same thread, no cross-thread hand-off. Threads that host
//! no lane (fewer workers than TPC threads), and the non-TPC servers, forward the request
//! to a host through its inbox; the non-TPC servers run dedicated host threads.
//!
//! A host runs one local task, the lane poller ([`LaneHost::run`]). Tokio's current-thread
//! scheduler has no per-I/O-round hook for user code, so the poller is a task like the
//! connections: while requests are in flight and answers came recently it reads the answer
//! counters (atomic loads, no syscall) and yields, so it runs once per scheduler round,
//! between the connections' I/O; after the spin window it flags "sleeping" in each lane's
//! channel, re-checks, and waits in the thread's own kqueue/epoll (the runtime's reactor) on
//! its lanes' wake pipes, one per lane, all registered once in one epoll/kqueue that the reactor waits on as one fd.
//! A worker writes its pipe one byte only when it sees the flag, so a lane whose pipe brings
//! more bytes than its answers allow is that lane's worker's fault ([`HostEnd::hear_wakes`](mipyre_shm::channel::HostEnd::hear_wakes)). Worker deaths come through the same reactor: a kqueue with `EVFILT_PROC`
//! (macOS) or an epoll of pidfds (Linux), registered as one more readable fd.
//!
//! When a worker dies the host answers the request it was running (502, or 504 when the
//! host killed it for overrunning the timeout), leaves the requests it had not taken in
//! the lane, and has the template fork a replacement (on the blocking pool, so the event
//! loop never waits for a fork); the new worker continues from the first request not taken.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::core::{
    peer_corrupted, runs_queued, Completions, CoreConfig, DeathCause, HostCore, HostEffects, LaneNotice, LaneWorker,
    LocalLane, Rejection, WorkerTag, START_REFUSED,
};
use super::health::{self, CrashLoopWait, FailureResponse, HeadOfLine};
use super::lane_failure::{LaneIndex, WorkerExit};
use super::monitor::{worker_exit, ProcessMonitor};
use super::os::monotonic_ns;
use super::pid::Pid;
use super::poller::{
    bell_drained, drain_leftover, poller_failure, woken_pipe, HostBellWatch, PollerError, BELL_EVENTS,
};
use super::route::{answer_lane, LaneJob, LaneReplySender, LaneRequestError};
use super::run::{LaneEnd, RunLane, RunPhase, RunState, ServerLanes};
use super::stats::{HostCounts, RESPAWN_SAMPLES};
use super::template_host::{ForkError, FORK_ANSWER_TIMEOUT};
use super::worker_process::WorkerProcess;
use crate::sync::Ordering::*;
use bytes::BytesMut;
use futures_util::FutureExt as _;
use mipyre_http::plan::response::HandlerResponse;
use mipyre_shm::adapter::doorbell::drain_once;
use mipyre_shm::channel::{sleep_all, wake_all, Attempt, PeerFault, SleepVerdict};
use mipyre_shm::plan::ring::RingDepth;
use mipyre_shm::plan::spin_window::SpinWindow;
use mipyre_shm::plan::start::StartReportError;
use mipyre_shm::protocol::doorbell::{spin, SpinOutcome};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Why a lane has no worker for good ([`LaneWorker::Broken`]).
#[derive(Debug, thiserror::Error)]
pub enum LaneLossError {
    #[error("respawn failed: {0}")]
    NotRespawned(#[source] ForkError),
    /// Its replacement was forked, but its exit cannot be watched.
    #[error("watching worker {pid}: {source}")]
    ExitNotWatched { pid: Pid, source: std::io::Error },
    /// Its replacement could not import the app; `failure` is the worker's own report, a
    /// traceback.
    #[error("its replacement worker could not start:\n{failure}")]
    ReplacementNotStarted { failure: String },
    /// Its replacement left a start report no worker writes.
    #[error("its replacement worker {pid} left a start report the server cannot read: {source}")]
    ReplacementReportUnreadable { pid: Pid, source: StartReportError },
    /// Its worker refused to start (exit status `START_REFUSED`): it would run another build of
    /// mipyre, or could not map its lane. Another worker would refuse the same way.
    #[error("its worker {pid} refused to start (exit status {START_REFUSED}): it runs another build of mipyre, or could not map its lane; its stderr says which")]
    StartRefused { pid: Pid },
    /// Its host stopped reading it.
    #[error("{0}")]
    HostFailed(Arc<HostError>),
}

/// A host that stopped reading its lanes, and why.
#[derive(Debug)]
pub struct HostError {
    pub(crate) host: usize,
    pub(crate) cause: PollerError,
}

impl std::fmt::Display for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let host = self.host;
        match &self.cause {
            PollerError::Panicked(panic) => write!(f, "process lane host {host} panicked: {panic}"),
            PollerError::BellUnreadable { .. }
            | PollerError::BellsUnwatchable { .. }
            | PollerError::ExitWatchFailed(_) => {
                write!(f, "process lane host {host} failed: {}", self.cause)
            }
        }
    }
}

impl std::error::Error for HostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

/// Why a server run's lanes failed: its first lost lane or failed host.
#[derive(Debug, Clone, thiserror::Error)]
pub enum LaneRunError {
    /// A lane lost its worker for good because its respawn failed.
    #[error("{why}")]
    LaneLost { lane: LaneIndex, why: Arc<LaneLossError> },
    /// A host stopped reading its lanes; they all broke with it.
    #[error("{0}")]
    HostFailed(Arc<HostError>),
}

/// What the lanes tell the server run they serve, as it happens.
#[derive(Debug)]
pub enum LaneRunEvent {
    /// A worker died while the run served; its lane forks a replacement.
    WorkerDied,
    /// The lanes failed in a way they cannot serve past, so the server must stop.
    Failed(LaneRunError),
}

thread_local! {
    /// The host this thread is, while it serves.
    pub(crate) static HOST: RefCell<Option<Rc<LaneHost>>> = const { RefCell::new(None) };

}

/// Logs one death of lane `i`'s worker `pid`, which served for `lived`: every death, so a worker
/// replaced again and again (OOM-killed, segfaulting) is seen before the lane crash-loops. The
/// crash backoff bounds how often a lane's workers can die.
fn say_worker_died(i: LaneIndex, pid: Pid, cause: DeathCause, exit: &WorkerExit, lived: Duration) {
    tracing::warn!(
        target: "mipyre::server", lane = i.0, pid = pid.as_raw(), exit = %exit, lived_ms = u64::try_from(lived.as_millis()).unwrap_or(u64::MAX),
        "process lane {i}: worker {} {cause} ({exit}) after serving {lived:.1?}; the lane forks a \
         replacement",
        pid.as_raw()
    );
}

/// A worker a replacement's fork made, until its host claims it as its lane's worker. One never
/// claimed (its lane broke, or its host closed, while it forked) is killed when dropped: no lane
/// runs it, and nothing else learns its pid. The drop needs no task of the host's, so a fork that
/// answers after its host is gone still ends its worker.
struct WorkerCandidate(Option<WorkerProcess>);

impl WorkerCandidate {
    fn new(worker: WorkerProcess) -> Self {
        WorkerCandidate(Some(worker))
    }

    #[expect(
        clippy::expect_used,
        reason = "the worker is taken out only here, and a claim consumes it: the drop needs it in an `Option` to move it out"
    )]
    fn claim(mut self) -> WorkerProcess {
        self.0
            .take()
            .expect("only a claim takes the worker, and a claim consumes it")
    }
}

impl Drop for WorkerCandidate {
    fn drop(&mut self) {
        if let Some(worker) = self.0.take() {
            worker.kill();
        }
    }
}

/// A replacement's fork, on the blocking pool.
type ForkTask = tokio::task::JoinHandle<Result<WorkerCandidate, ForkError>>;

/// A thread that hosts lanes. `!Send`: it lives on its thread, in its thread-local.
pub struct LaneHost {
    pub(crate) lanes: Arc<ServerLanes>,
    pub(crate) run: Arc<RunState>,
    pub(crate) index: usize,
    pub(crate) owned: RefCell<HostCore>,
    /// Where each answer is copied out of its slot: it is split off whole, and its body and an
    /// uncommon content type are slices of it, so an answer is copied once.
    /// What is left of its room takes the next answers.
    answer_bytes: RefCell<BytesMut>,
    /// What a collect completes, kept between collects so a collect that reads answers does not
    /// allocate for them. Empty between collects.
    completions: RefCell<Completions>,
    /// The forks of replacement workers still running, by lane ([`LaneWorker::Forking`]).
    forks: RefCell<std::collections::HashMap<LaneIndex, ForkTask>>,
    /// Whether the crash-loop watch ([`LaneHost::watch_crash_loops`]) runs: from a lane's
    /// first wait until none of the host's lanes loops.
    crash_watch: Cell<bool>,
    /// Wakes the poller: a request was put in a lane, or a fork finished.
    notify: Arc<Notify>,
    /// The lanes' wake pipes, as this thread's reactor waits on them.
    bells: HostBellWatch,
    /// The places of the lanes whose pipes the host stopped listening to, because their workers
    /// are at fault: until the lane runs its next worker ([`LaneHost::listen_again`]).
    muted: RefCell<Vec<usize>>,
    pub(crate) stop: CancellationToken,
    done: Notify,
}

/// How many bytes the host's answer buffer starts with, and grows by when it is all handed out.
const ANSWER_BUFFER_BYTES: usize = 16 << 10;

impl LaneHost {
    /// Starts hosting on this thread (a current-thread runtime with a `LocalSet`) if the
    /// current run gives it lanes: host `index` of the run. The poller runs as a local task
    /// until the run stops or [`LaneHost::shutdown`]. What the poller needs (the wake pipe in
    /// this thread's reactor, the worker-exit watch) is set up here: a host that cannot watch
    /// its lanes is this start's error, not a host that answers every request 503.
    pub(crate) fn start(lanes: &Arc<ServerLanes>, index: usize) -> std::io::Result<Option<Rc<LaneHost>>> {
        let run = match &*lanes.run.read() {
            RunPhase::Serving(run) => Arc::clone(run),
            RunPhase::Idle | RunPhase::Starting => return Ok(None),
        };
        let Some(run_lanes) = run.hosts.get(index).and_then(|h| h.start.lock().take()) else {
            return Ok(None);
        };
        let depth = lanes.geometry.depth().get() as usize;
        let mut owned = HostCore {
            lanes: Vec::with_capacity(run_lanes.len()),
            ends: Vec::with_capacity(run_lanes.len()),
            crash_runs: Vec::new(),
        };
        let mut bells = Vec::with_capacity(run_lanes.len());
        for RunLane { index: j, end, bell } in run_lanes {
            owned.lanes.push(LocalLane {
                index: j,
                waiting: (0..depth).map(|_| None).collect(),
                queued: VecDeque::new(),
                worker: match lanes.processes[j.0].lock().as_ref() {
                    None => LaneWorker::Gone,
                    Some(worker) => LaneWorker::Running {
                        worker: worker.tag(),
                        starting: None,
                        head: None,
                    },
                },
            });
            owned.ends.push(end);
            bells.push(bell);
        }
        let bells = HostBellWatch::new(bells)?;
        let host = Rc::new(LaneHost {
            lanes: Arc::clone(lanes),
            run: Arc::clone(&run),
            index,
            owned: RefCell::new(owned),
            answer_bytes: RefCell::new(BytesMut::with_capacity(ANSWER_BUFFER_BYTES)),
            completions: RefCell::new(Completions::with_capacity(depth)),
            forks: RefCell::new(std::collections::HashMap::new()),
            crash_watch: Cell::new(false),
            notify: Arc::new(Notify::new()),
            bells,
            muted: RefCell::new(Vec::new()),
            stop: run.stop.child_token(),
            done: Notify::new(),
        });
        let watch = host.watch()?;
        HOST.with(|h| *h.borrow_mut() = Some(Rc::clone(&host)));
        let poller = Rc::clone(&host);
        // `poller_failure` turns how the poller ended, a panic included, into the host's failure.
        tokio::task::spawn_local(async move {
            let failure = poller_failure(Rc::clone(&poller).run(watch)).await;
            poller.run.hosts[poller.index].inbox.close();
            if let Some(failure) = failure {
                poller.fail(failure);
            }
            poller.close();
            poller.done.notify_one();
        });
        Ok(Some(host))
    }

    /// Stops this host and waits for its poller to answer what is left.
    pub async fn shutdown(self: Rc<Self>) {
        self.stop.cancel();
        self.done.notified().await;
        HOST.with(|h| *h.borrow_mut() = None);
    }

    pub(crate) async fn stopped(self: Rc<Self>) {
        self.done.notified().await;
        HOST.with(|h| *h.borrow_mut() = None);
    }

    fn depth(&self) -> RingDepth {
        self.lanes.geometry.depth()
    }

    /// What the core needs of this host's settings.
    fn core_config(&self) -> CoreConfig {
        CoreConfig {
            depth: self.depth(),
            timeout: self.lanes.host.timeout(),
            crash_backoff: *self.lanes.host.crash_backoff(),
        }
    }

    /// The process of `worker`, while it is its lane's current worker.
    fn process_of(&self, worker: WorkerTag) -> Option<Arc<WorkerProcess>> {
        self.lanes.processes[worker.lane.0]
            .lock()
            .as_ref()
            .filter(|p| p.tag() == worker)
            .cloned()
    }

    /// Kills `worker`, its lane's current worker. One the run's stop already took is killed by
    /// the stop (`ServerLanes::stop_run`).
    fn kill(&self, worker: WorkerTag) {
        if let Some(process) = self.process_of(worker) {
            process.kill();
        }
    }

    /// Puts a request of this thread into one of this host's lanes. Never waits. A refusal
    /// hands the request back with why, so the caller can send it elsewhere or answer it.
    #[expect(
        clippy::result_large_err,
        reason = "a refused job comes back by value, as it went in, to be forwarded or answered"
    )]
    pub(crate) fn submit(&self, job: LaneJob) -> Result<(), (LaneRequestError, LaneJob)> {
        let placed =
            self.owned
                .borrow_mut()
                .submit(job, self.lanes.host.queue_per_worker().get(), &mut ShellFx::new(self));
        match placed {
            Ok(()) => {}
            Err(Rejection::Refused(why, job)) => return Err((why, job)),
            Err(Rejection::NoLane(job)) => {
                let refusal = self.owned.borrow().no_lane_refusal(
                    self.lanes.host.crash_backoff(),
                    self.run.stop.is_cancelled(),
                    monotonic_ns(),
                    self.index,
                );
                return Err((LaneRequestError::Rejected(refusal), job));
            }
        }
        // The busy path: a thread that is submitting is awake; it reads its lanes' answers
        // on the way (atomic loads when there are none).
        let read = self.collect();
        if read > 0 {
            self.counts().read_by_submit.add(read);
        }
        self.publish_load();
        Ok(())
    }

    /// Whether every lane of this host is running a handler the host has seen running for at
    /// least [`health::SPREAD_AFTER_NS`]: its worker took the request at the head of the lane,
    /// and the host has seen that request there that long (it looks when it times the lane, and
    /// here).
    pub(crate) fn running_long(&self) -> bool {
        let now = monotonic_ns();
        let mut owned = self.owned.borrow_mut();
        let HostCore { lanes, ends, .. } = &mut *owned;
        lanes.iter_mut().zip(ends.iter_mut()).all(|(lane, end)| {
            let LaneWorker::Running { head, .. } = &mut lane.worker else {
                return false;
            };
            let read = end.window().read();
            // A fault found here is the end's, kept for the next collect, which acts on it.
            match end.running() {
                Ok(Some(running)) if running == read => {
                    let seen = HeadOfLine::look(head, read.get(), now);
                    health::ran_at_least(seen.since_ns, now, health::SPREAD_AFTER_NS)
                }
                Ok(_) | Err(_) => false,
            }
        })
    }

    fn publish_load(&self) {
        let load = self.owned.borrow().load();
        self.run.hosts[self.index].load.store(load, Relaxed);
    }

    /// Reads every published answer of this host's lanes and completes its request on this
    /// thread, returning how many it read; a lane with nothing new costs one atomic load.
    fn collect(&self) -> u64 {
        let mut read = 0u64;
        let depth = self.depth();
        // Taken out, not borrowed, for the answering below: a collect that runs inside it finds
        // an empty buffer instead of a borrowed one.
        let mut done = self.completions.take();
        read += self.owned.borrow_mut().collect(
            &mut self.answer_bytes.borrow_mut(),
            depth,
            &mut done,
            &mut ShellFx::new(self),
        );
        if read > 0 {
            self.publish_load();
        }
        for (reply, outcome) in done.drain(..) {
            answer_lane(reply, outcome);
        }
        self.completions.replace(done);
        read
    }

    fn in_flight(&self) -> bool {
        self.owned.borrow().ends.iter().any(|e| e.in_flight() != 0)
    }

    /// Kills the worker of any lane whose running handler is past the timeout.
    fn check_timeouts(&self) {
        let config = self.core_config();
        self.owned
            .borrow_mut()
            .check_timeouts(monotonic_ns(), &config, &mut ShellFx::new(self));
    }

    /// Logs that lane `i`'s worker `pid` wrote what no correct worker writes, with the values
    /// the host held and the one it read.
    fn say_peer_fault(&self, i: LaneIndex, pid: Pid, fault: PeerFault) {
        self.lanes.stats.peer_faults.fetch_add(1, Relaxed);
        tracing::error!(
            target: "mipyre::server", lane = i.0, pid = pid.as_raw(), detail = ?fault,
            "process lane {i}: {}; the worker is killed and its requests in flight are answered 502, none run again",
            peer_corrupted(i, pid, fault)
        );
    }

    /// Runs `f` on this host's lane `i` and its channel end (the lane by its index, its
    /// identity, not by its place in `owned`); `None` when this host does not own lane `i`.
    fn with_lane<R>(&self, i: LaneIndex, f: impl FnOnce(&mut LocalLane, &mut LaneEnd) -> R) -> Option<R> {
        let mut owned = self.owned.borrow_mut();
        let HostCore { lanes, ends, .. } = &mut *owned;
        let k = lanes.iter().position(|l| l.index == i)?;
        Some(f(&mut lanes[k], &mut ends[k]))
    }

    /// Handles the death of `dead`, a lane's worker: takes over its channel (reading what it
    /// answered), answers 502 what it was running and what it never took, and starts forking a
    /// replacement. A lane this host
    /// does not own, or a worker the lane no longer runs, is an earlier event's echo and changes
    /// nothing.
    async fn worker_died(self: &Rc<Self>, dead: Arc<WorkerProcess>) {
        let (i, pid) = (dead.lane(), dead.pid());
        if self
            .with_lane(i, |lane, _| lane.death_cause(dead.tag(), monotonic_ns()))
            .flatten()
            .is_none()
        {
            return;
        }
        self.lanes.stats.deaths.fetch_add(1, Relaxed);
        self.collect_in_poller();
        let status = self.exit_status(i, pid).await;
        // Looked up again after the wait: the lane is taken by its index, and whether `pid` is
        // still its worker is decided on the state this step changes, not on the one seen
        // before the wait.
        let config = self.core_config();
        let mut fx = ShellFx::new(self);
        fx.dead_lived = Some(dead.opened_at().elapsed());
        self.owned.borrow_mut().worker_exited(
            dead.tag(),
            &status,
            monotonic_ns(),
            self.run.stop.is_cancelled(),
            &config,
            &mut fx,
        );
        if fx.start_crash_watch && !self.crash_watch.replace(true) {
            tokio::task::spawn_local(Rc::clone(self).watch_crash_loops());
        }
    }

    /// Runs the crash-loop watch, a local task that lives only while one of the host's lanes
    /// crash-loops, so the poller does nothing for crash runs while every lane serves. It ends
    /// with the run, or once no lane loops.
    async fn watch_crash_loops(self: Rc<Self>) {
        while let Some(next) = self.tend_crash_runs() {
            tokio::select! {
                biased;
                () = self.stop.cancelled() => break,
                () = tokio::time::sleep(next) => {}
            }
        }
        self.crash_watch.set(false);
    }

    /// Looks once at this host's crash runs: a lane that answered ends its run (logging it if it
    /// was alerted), a waiting lane whose wait is over forks, and a looping lane's alert is
    /// logged again when due. Returns how long until the next look, or `None` once no lane loops.
    fn tend_crash_runs(&self) -> Option<Duration> {
        let config = self.core_config();
        let now = monotonic_ns();
        let next_ns = self.owned.borrow_mut().tend(now, &config, &mut ShellFx::new(self));
        next_ns.map(|at| Duration::from_nanos(at.saturating_sub(now)))
    }

    /// Returns how the dead worker ended, as the template reaped it, waiting up to the host's
    /// `exit_status_wait` for the template's report.
    async fn exit_status(&self, i: LaneIndex, pid: Pid) -> WorkerExit {
        let wait = self.lanes.host.exit_status_wait();
        let deadline = Instant::now() + wait;
        loop {
            self.lanes.read_template_reports();
            if let Some(status) = self.lanes.exits.status_of(i, pid) {
                return worker_exit(status);
            }
            if Instant::now() > deadline {
                return WorkerExit::Unreported(wait);
            }
            tokio::time::sleep(Duration::from_micros(500)).await;
        }
    }

    /// Settles finished forks: watches each new worker, breaks the lane of a failed fork or of a
    /// replacement that cannot import the app, and records the respawn latency once a
    /// replacement is ready.
    fn finish_respawns(&self, watch: &ProcessMonitor) {
        let config = self.core_config();
        let forking: Vec<LaneIndex> = self
            .owned
            .borrow()
            .lanes
            .iter()
            .filter(|l| matches!(l.worker, LaneWorker::Forking { .. }))
            .map(|l| l.index)
            .collect();
        for i in forking {
            let joined = {
                let mut forks = self.forks.borrow_mut();
                let Some(task) = forks.get_mut(&i) else {
                    tracing::error!(
                        target: "mipyre::server",
                        lane = i.0,
                        "process lane forks a replacement with no fork task: it waits for none"
                    );
                    continue;
                };
                // A fork still running is looked at again next round; its task keeps the answer.
                let Some(joined) = finished_now(task) else {
                    continue;
                };
                forks.remove(&i);
                joined
            };
            let answer = match joined.map_err(ForkError::Task).and_then(|forked| forked) {
                Ok(forked) => {
                    let worker = Arc::new(forked.claim());
                    *self.lanes.processes[i.0].lock() = Some(Arc::clone(&worker));
                    self.lanes.stats.respawns.fetch_add(1, Relaxed);
                    let (tag, pid) = (worker.tag(), worker.pid());
                    match watch.add(worker) {
                        Ok(()) => Ok(tag),
                        Err(source) => Err(LaneLossError::ExitNotWatched { pid, source }),
                    }
                }
                Err(e) => Err(LaneLossError::NotRespawned(e)),
            };
            self.owned
                .borrow_mut()
                .forked(i, answer, &config, &mut ShellFx::new(self));
        }
        self.owned
            .borrow_mut()
            .look_at_starts(monotonic_ns, &config, &mut ShellFx::new(self));
    }

    /// Records a failure that left `newly_broken` lanes without a worker for good, with `cause`
    /// kept when it is the run's first, and returns what the run does about it. Under
    /// [`FailureResponse::Stop`] the run is told the lanes failed, with the run's first cause, and
    /// its owner ends the server, so the process exits non-zero (systemd restarts it). Nothing is
    /// restarted here: the failures seen (the template, the reactor, the app's import in a
    /// replacement) would repeat in a new host or lane, and no evidence yet says a host ever fails
    /// by chance in production.
    fn fail_run(&self, newly_broken: usize, cause: LaneRunError) -> FailureResponse {
        let (verdict, first) = {
            let mut kept = self.lanes.first_failure.lock();
            let first = kept.get_or_insert(cause).clone();
            (self.lanes.health.lock().failed(newly_broken), first)
        };
        if verdict == FailureResponse::Stop {
            tracing::error!(target: "mipyre::server", error = %first, "process lanes failed: the server stops");
            (self.run.tell_run)(LaneRunEvent::Failed(first));
        }
        verdict
    }

    /// Handles a poller that failed while the run serves, after which nothing reads these lanes.
    /// This thread stops putting requests in them (it is no longer their host), the other threads
    /// skip this host when they send (`HostLink::serving`); every lane is broken, its requests
    /// answered with the failure, and the run decides whether the server ends
    /// ([`RunHealth::failed`](super::health::RunHealth::failed)).
    fn fail(&self, failure: PollerError) {
        HOST.with(|h| {
            let mut current = h.borrow_mut();
            if current.as_ref().is_some_and(|host| std::ptr::eq(&**host, self)) {
                *current = None;
            }
        });
        self.lanes.stats.host_failures.fetch_add(1, Relaxed);
        let down = Arc::new(HostError {
            host: self.index,
            cause: failure,
        });
        // A host's poller runs once, so this is the only cause.
        self.run.hosts[self.index].failure.get_or_init(|| Arc::clone(&down));
        let lanes: Vec<LaneIndex> = self.owned.borrow().lanes.iter().map(|l| l.index).collect();
        let why = Arc::new(LaneLossError::HostFailed(Arc::clone(&down)));
        let config = self.core_config();
        let newly = lanes
            .into_iter()
            .filter(|&i| {
                self.owned
                    .borrow_mut()
                    .mark_broken(i, &why, &config, &mut ShellFx::new(self))
            })
            .count();
        match self.fail_run(newly, LaneRunError::HostFailed(Arc::clone(&down))) {
            FailureResponse::Isolate => {
                tracing::error!(target: "mipyre::server", host = self.index, error = %down, "process lane host failed: isolated, the other hosts keep serving")
            }
            FailureResponse::Stop | FailureResponse::AlreadyStopped => {
                tracing::error!(target: "mipyre::server", host = self.index, error = %down, "process lane host failed: the server stops")
            }
        }
    }

    /// Answers everything still waiting with 503, once the run has stopped.
    fn close(&self) {
        self.owned.borrow_mut().close(&mut ShellFx::new(self));
    }

    /// Says to every lane's worker that this host is awake.
    pub(crate) fn wake(&self) {
        wake_all(&mut self.owned.borrow_mut().ends);
    }

    /// Starts the watch on this host's lanes' workers' exits.
    fn watch(&self) -> std::io::Result<ProcessMonitor> {
        let watch =
            ProcessMonitor::new().map_err(|e| std::io::Error::new(e.kind(), format!("the worker-exit watch: {e}")))?;
        for lane in self.owned.borrow().lanes.iter() {
            if let LaneWorker::Running { worker, .. } | LaneWorker::Overrun { worker, .. } = &lane.worker {
                if let Some(process) = self.process_of(*worker) {
                    watch.add(process)?;
                }
            }
        }
        Ok(watch)
    }

    /// Runs the poller described in the module doc until the run stops.
    async fn run(self: Rc<Self>, watch: ProcessMonitor) -> Result<(), PollerError> {
        let inbox = &self.run.hosts[self.index].inbox;
        loop {
            if self.stop.is_cancelled() {
                return Ok(());
            }
            self.count_answers();
            while let Some(job) = inbox.take() {
                self.take_forwarded(job);
            }
            self.collect_in_poller();
            for dead in watch.exited().map_err(PollerError::ExitWatchFailed)? {
                self.worker_died(dead).await;
            }
            self.finish_respawns(&watch);
            self.listen_again()?;
            let in_flight = self.in_flight();
            // Wait in this thread's reactor. The poller does not touch the sleeping flag: the
            // thread does, as it parks and unparks (`enter_idle` / `resume_from_idle`). While
            // the thread is awake, answers are read by its submits and by the spin before
            // it parks; while it is parked, a worker's answer writes the wake pipe, which
            // wakes this task.
            if self.collect_in_poller() > 0 {
                continue;
            }
            self.check_timeouts();
            self.counts().sleeps.add(1);
            let tick = if in_flight
                || self
                    .owned
                    .borrow()
                    .lanes
                    .iter()
                    .any(|l| matches!(l.worker, LaneWorker::Forking { .. }))
            {
                Duration::from_millis(10)
            } else {
                Duration::from_millis(500)
            };
            tokio::select! {
                biased;
                _ = self.stop.cancelled() => {}
                woken = self.bells.fd.readable() => {
                    let mut guard = woken_pipe(woken)?;
                    // Left ready while a pipe holds bytes, so the next wait returns at once; a
                    // byte written after a look that found none makes the queue ready again.
                    if self.hear_bells()? == 0 {
                        guard.clear_ready();
                    }
                    self.counts().pipe_wakes.add(1);
                }
                _ = self.notify.notified() => {}
                job = inbox.next() => self.take_forwarded(job),
                ready = watch.readable() => { ready.map_err(PollerError::ExitWatchFailed)?; }
                () = tokio::time::sleep(tick) => {}
            }
        }
    }

    /// Adds the answers each lane's end read since the last round to the room its worker has for
    /// wake bytes; once a round, so fewer than 2^32 answers pass between two counts.
    fn count_answers(&self) {
        self.owned.borrow_mut().ends.iter_mut().for_each(LaneEnd::count_answers);
    }

    /// Reads once each lane's wake pipe the queue says holds bytes, and counts the bytes against
    /// the room its answers leave ([`HostEnd::hear_wakes`](mipyre_shm::channel::HostEnd::hear_wakes)). A lane whose worker is at fault is
    /// muted: its fault stays on its end, and the next collect kills the worker as for any
    /// other fault. Returns how many pipes held bytes.
    fn hear_bells(&self) -> Result<usize, PollerError> {
        let queue = self.bells.fd.get_ref();
        let mut rung = [0usize; BELL_EVENTS];
        let n = queue
            .rung(&mut rung)
            .map_err(|source| PollerError::BellsUnwatchable { source })?;
        for &k in &rung[..n] {
            let lane = self.owned.borrow().lanes[k].index;
            let drained = bell_drained(lane, drain_once(queue.bell(k)))?;
            if self.owned.borrow_mut().ends[k].hear_wakes(drained.bytes).is_err() {
                queue
                    .mute(k)
                    .map_err(|source| PollerError::BellsUnwatchable { source })?;
                self.muted.borrow_mut().push(k);
            }
        }
        Ok(n)
    }

    /// Listens again to the pipe of each muted lane that runs its next worker, after reading what
    /// the pipe still holds: those bytes were written before the worker started, and are not its.
    fn listen_again(&self) -> Result<(), PollerError> {
        let mut muted = self.muted.borrow_mut();
        if muted.is_empty() {
            return Ok(());
        }
        let owned = self.owned.borrow();
        let queue = self.bells.fd.get_ref();
        let mut i = 0;
        while i < muted.len() {
            let k = muted[i];
            let serving = matches!(
                owned.lanes[k].worker,
                LaneWorker::Running { .. } | LaneWorker::Overrun { .. }
            );
            if !serving || owned.ends[k].fault().is_some() {
                i += 1;
                continue;
            }
            drain_leftover(owned.lanes[k].index, queue.bell(k))?;
            queue
                .listen(k)
                .map_err(|source| PollerError::BellsUnwatchable { source })?;
            muted.swap_remove(i);
        }
        Ok(())
    }

    /// Puts a request another thread forwarded into this host's lanes, or answers it with why
    /// not; it is never forwarded again. One nobody waits for any more is dropped unrun.
    fn take_forwarded(&self, job: LaneJob) {
        if !runs_queued(job.reply.is_closed()) {
            self.counts().unawaited_skips.add(1);
            return;
        }
        if let Err((failure, job)) = self.submit(job) {
            answer_lane(job.reply, Err(failure));
        }
    }

    /// Runs as the thread is about to park with no task ready. With requests in flight it spins
    /// on the lanes for up to the spin window, then says to every lane's worker that it sleeps
    /// and looks once more ([`sleep_all`]): an answer published after that writes the wake pipe,
    /// and one published before it is read now. An answer completes its request on this
    /// thread, and the runtime then runs it instead of parking.
    ///
    /// The spin only checks readiness ([`LaneHost::spin_on_lanes`]); [`LaneHost::collect`]
    /// reads the answers once, after it.
    pub(crate) fn spin_for_next_request(&self) {
        self.counts().park_hooks.add(1);
        if !self.in_flight() {
            return;
        }
        self.wake();
        let window = SpinWindow::until(monotonic_ns().saturating_add(self.lanes.host.spin_ns()));
        let spun = self.spin_on_lanes(window);
        let waits = &self.counts().waits;
        waits.waits.add(1);
        waits.spin_rounds.add(spun.rounds.into());
        waits.parks.add(u64::from(!spun.ready));
        // Found or not, the thread may park after this hook: every end says the host sleeps
        // before it is read once more (the Dekker handshake with each worker's publish), so
        // either this read sees an answer or its worker sees the flag and writes the pipe.
        let verdict = sleep_all(&mut self.owned.borrow_mut().ends);
        if verdict == SleepVerdict::Collect {
            let read = self.collect();
            self.counts().read_by_spin.add(read);
        }
    }

    fn collect_in_poller(&self) -> u64 {
        let read = self.collect();
        self.counts().read_by_poller.add(read);
        read
    }

    /// Spins until a worker of this host's lanes has published an answer this host has not
    /// read, or `window` is over. Not inlined, so its rounds cost the same instructions
    /// wherever they are counted (`host::tests::spin_rounds_bench`).
    #[inline(never)]
    fn spin_on_lanes(&self, window: SpinWindow) -> SpinOutcome {
        let owned = self.owned.borrow();
        spin(window, monotonic_ns, || owned.ends.iter().any(LaneEnd::has_answers))
    }

    /// Returns this host's counts, of which this host is the only writer.
    #[inline]
    pub(crate) fn counts(&self) -> &HostCounts {
        &self.lanes.host_counts[self.index].0
    }
}

/// What a host does for its core ([`HostEffects`]): kills and forks through the server's
/// processes and the template, logs and counts, answers over the requests' oneshots, and publishes
/// what other threads read. It never borrows the host's lanes (`owned`): the core holds them while
/// it calls in.
struct ShellFx<'h> {
    host: &'h LaneHost,
    /// How long the worker whose death is being handled served, for its death's log line.
    dead_lived: Option<Duration>,
    /// A lane started waiting out a crash loop: the host's caller starts the crash-loop watch.
    start_crash_watch: bool,
}

impl<'h> ShellFx<'h> {
    #[inline]
    fn new(host: &'h LaneHost) -> Self {
        ShellFx {
            host,
            dead_lived: None,
            start_crash_watch: false,
        }
    }
}

impl HostEffects for ShellFx<'_> {
    fn take_attempt(&mut self) -> Attempt {
        self.host.lanes.take_attempt()
    }

    fn kill(&mut self, worker: WorkerTag) {
        self.host.kill(worker);
    }

    fn fork(&mut self, lane: LaneIndex, attempt: Attempt) {
        let lanes = Arc::clone(&self.host.lanes);
        let notify = Arc::clone(&self.host.notify);
        let task = tokio::task::spawn_blocking(move || {
            let forked = lanes
                .spawn_worker(lane, attempt, FORK_ANSWER_TIMEOUT)
                .map(WorkerCandidate::new);
            notify.notify_one();
            forked
        });
        self.host.forks.borrow_mut().insert(lane, task);
    }

    fn drop_fork(&mut self, lane: LaneIndex) {
        // The task's answer, once it comes, is dropped unread, and its worker with it.
        drop(self.host.forks.borrow_mut().remove(&lane));
    }

    fn forget(&mut self, worker: WorkerTag) {
        *self.host.lanes.processes[worker.lane.0].lock() = None;
    }

    #[inline]
    fn woke_worker(&mut self) {
        self.host.counts().waits.wakes.add(1);
    }

    fn skipped_unawaited(&mut self) {
        self.host.counts().unawaited_skips.add(1);
    }

    #[inline]
    fn reply(&mut self, reply: LaneReplySender, outcome: Result<HandlerResponse, LaneRequestError>) {
        answer_lane(reply, outcome);
    }

    fn say(&mut self, notice: LaneNotice) {
        let host = self.host;
        match notice {
            LaneNotice::WorkerDied { worker, cause, exit } => {
                (host.run.tell_run)(LaneRunEvent::WorkerDied);
                say_worker_died(
                    worker.lane,
                    worker.pid,
                    cause,
                    &exit,
                    self.dead_lived.unwrap_or_default(),
                );
            }
            LaneNotice::PeerFault { worker, fault } => host.say_peer_fault(worker.lane, worker.pid, fault),
            LaneNotice::FaultWithoutWorker { lane: i, fault } => {
                host.lanes.stats.peer_faults.fetch_add(1, Relaxed);
                tracing::error!(
                    target: "mipyre::server", lane = i.0, detail = ?fault,
                    "process lane {i}: its channel holds what no worker writes, while it has no worker; the \
                     host gives up the requests in it and puts its counters back"
                );
            }
            LaneNotice::Recovered { lane: i, crashes } => {
                tracing::warn!(
                    target: "mipyre::server",
                    lane = i.0,
                    crashes,
                    "process lane {i} recovered: it answered a request after {crashes} consecutive \
                     crashes; its forks no longer wait"
                );
                host.lanes.set_crash_loop(i.0, None);
            }
            LaneNotice::CrashLoop {
                lane: i,
                crashes,
                exit,
                wait,
                waiting,
            } => {
                let reason = format!(
                    "crash-looping: its last {crashes} workers died without answering a request, the \
                     last one {exit}; each fork waits (now {wait:?}), its requests are answered 503 \
                     while it waits"
                );
                tracing::error!(
                    target: "mipyre::server", lane = i.0, crashes, exit = %exit, wait = ?wait, waiting,
                    "process lane {i} {reason}"
                );
                host.lanes.set_crash_loop(i.0, Some(reason));
            }
            LaneNotice::LoopEnded { lane } => {
                host.lanes.set_crash_loop(lane.0, None);
            }
            LaneNotice::LaneLost { lane, why } => {
                host.lanes.stats.respawn_failures.fetch_add(1, Relaxed);
                tracing::error!(target: "mipyre::server", lane = lane.0, error = %why, detail = ?why, "process lane lost its worker for good");
            }
            LaneNotice::TimedOut => {
                host.lanes.stats.timeouts_killed.fetch_add(1, Relaxed);
            }
            LaneNotice::Respawned { latency_us } => {
                let mut kept = host.lanes.stats.respawn_us.lock();
                if kept.len() >= RESPAWN_SAMPLES {
                    kept.remove(0);
                }
                kept.push(latency_us);
            }
        }
    }

    fn lane_lost(&mut self, lane: LaneIndex, why: Arc<LaneLossError>, newly: bool) {
        self.host
            .fail_run(usize::from(newly), LaneRunError::LaneLost { lane, why });
    }

    fn usable_lanes(&mut self, delta: isize) {
        let usable = &self.host.run.hosts[self.host.index].usable_lanes;
        if delta >= 0 {
            usable.fetch_add(delta.unsigned_abs(), Relaxed);
        } else {
            usable.fetch_sub(delta.unsigned_abs(), Relaxed);
        }
    }

    fn load(&mut self, load: usize) {
        self.host.run.hosts[self.host.index].load.store(load, Relaxed);
    }

    fn crash_wait(&mut self, soonest: Option<CrashLoopWait>, watch: bool) {
        *self.host.run.hosts[self.host.index].crash_wait.lock() = soonest;
        self.start_crash_watch |= watch;
    }
}

/// Returns a task's outcome if it has finished, from one poll that does not wait, or `None`
/// while it runs. The poll is unconstrained so the runtime's cooperative budget never makes a
/// finished task read as still running.
fn finished_now<T>(task: &mut tokio::task::JoinHandle<T>) -> Option<Result<T, tokio::task::JoinError>> {
    tokio::task::unconstrained(task).now_or_never()
}

/// Starts this TPC thread's host for `lanes`, if the thread's routes use a lane. Run the returned
/// host's [`LaneHost::shutdown`] once the thread's connections are drained. A host that can't
/// watch its lanes is an error: the thread, and so the server, does not serve without it.
pub fn host_tpc_thread(
    lanes: Option<&Arc<ServerLanes>>,
    uses_lane: bool,
    thread: usize,
) -> std::io::Result<Option<Rc<LaneHost>>> {
    match lanes {
        Some(lanes) if uses_lane => LaneHost::start(lanes, thread),
        _ => Ok(None),
    }
}

#[cfg(test)]
#[cfg(not(sync_model))]
#[path = "tests.rs"]
mod tests;
