//! A server run of the lanes: making each lane's channel, forking and waiting for the workers, handing the lanes to
//! their hosts, and stopping the run.
//!
//! Each run gives every lane a channel of its own (`mipyre_shm`), new shared memory and a new wake
//! pipe, handed to the template once per run with each lane's channel, so no worker of an
//! earlier run can write into this run's lanes or wake its hosts.

use std::num::NonZeroUsize;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use super::health::{self, CrashLoopWait, RunHealth};
use super::host::{HostError, LaneHost, LaneRunError, LaneRunEvent};
use super::host_config::HostConfig;
use super::inbox::{Inbox, InboxRefusal};
use super::lane_failure::{fault_said, LaneIndex, WorkerExit};
use super::monitor::worker_exit;
use super::pid::Pid;
use super::poller::{enter_idle, resume_from_idle};
use super::route::{answer_lane, LaneJob, LaneRequestError};
use super::stats::{sum_waits, HostCounts, Stats};
use super::template::ExitReports;
use super::template_host::{fork_failed, ForkError, Template, FORK_ANSWER_TIMEOUT};
use super::worker_process::WorkerProcess;
use crate::sync::{AtomicU32, AtomicUsize, Mutex, Ordering::*, RwLock};
use mipyre_cpu::CacheLine;
use mipyre_shm::adapter::doorbell::{drain_once, open_host_doorbell, FutexDoorbell, HostBell};
use mipyre_shm::adapter::mapped::MappedMemory;
use mipyre_shm::channel::{Attempt, Channel, HostEnd, PeerFault, StartOutcome, WaitTotals};
use mipyre_shm::plan::geometry::Geometry;
use mipyre_shm::plan::start::StartReportError;
use mipyre_shm::SyscallError;
use mipyre_tpc::{OsThread, ShutdownDeadline, ThreadJoinError};
use tokio_util::sync::CancellationToken;

/// The host's end of one lane's channel: the shared mapping, and a futex to wake the worker with.
pub(super) type LaneEnd = HostEnd<MappedMemory, FutexDoorbell>;

/// Why a server run's lanes could not start.
#[derive(Debug, thiserror::Error)]
pub enum LaneStartError {
    #[error(
        "the process lanes are already serving another server of this app; a second server \
         with runtime='process' routes needs its own app"
    )]
    AlreadyServing,
    /// The template could not prepare (with start="fork" the app failed to import, or it is
    /// not single-threaded) and said why before it ended; the fork request for `lane` met it.
    #[error("the process-lane template could not start workers (met by the fork request for lane {lane}):\n{failure}")]
    TemplateFailed { lane: LaneIndex, failure: String },
    /// The run's channel for `lane` (its shared memory) could not be made.
    #[error("making the shared memory of lane {lane}: {source}")]
    Channel { lane: LaneIndex, source: SyscallError },
    /// The run's wake pipe of lane `lane` could not be made.
    #[error("making the wake pipe of process lane {lane}: {source}")]
    Bell { lane: LaneIndex, source: SyscallError },
    #[error("forking the worker of lane {lane}: {source}")]
    Fork { lane: LaneIndex, source: ForkError },
    /// The template got `got` of the `want` fds of lane `lane`'s frame (it may be out of fds:
    /// it holds two per lane), and ended.
    #[error("the process-lane template got {got} of the {want} fds of lane {lane}")]
    LaneFds { lane: LaneIndex, got: u32, want: u32 },
    #[error("the process worker of lane {lane} (pid {pid}) could not start:\n{failure}")]
    WorkerStart { lane: LaneIndex, pid: Pid, failure: String },
    #[error("the process worker of lane {lane} (pid {pid}) left a start report the server cannot read: {source}")]
    WorkerReportUnreadable {
        lane: LaneIndex,
        pid: Pid,
        source: StartReportError,
    },
    /// The worker ended before it was ready without leaving a start failure; `exit` is how it
    /// ended, as the template reaped it.
    #[error("the process worker of lane {lane} (pid {pid}) ended before it was ready: {exit}")]
    DiedBeforeReady {
        lane: LaneIndex,
        pid: Pid,
        exit: WorkerExit,
    },
    #[error("worker {pid} of lane {lane} not ready within 30 s")]
    NotReady { lane: LaneIndex, pid: Pid },
    /// The start could not watch its workers' exits, or sleep until one of them is due.
    #[error("waiting for the process workers to start: {0}")]
    StartWatch(std::io::Error),
    #[error("reading the wake pipe of process lane {lane} while its worker starts: {source}")]
    BellUnreadable { lane: LaneIndex, source: SyscallError },
    #[error(
        "the process worker of lane {lane} (pid {pid}) corrupted the lane as it started: {}",
        fault_said(fault)
    )]
    WorkerFault {
        lane: LaneIndex,
        pid: Pid,
        fault: PeerFault,
    },
    #[error("could not build a process lane host's runtime: {0}")]
    Runtime(std::io::Error),
    #[error("could not spawn process lane host thread {host}: {source}")]
    HostThread { host: usize, source: std::io::Error },
    #[error("process lane host {host} could not start: {source}")]
    HostStart { host: usize, source: std::io::Error },
}

/// Where a server run hosts the lanes.
#[derive(Clone, Copy, Debug)]
pub enum HostKind {
    Tpc {
        threads: usize,
    },
    /// The non-TPC servers run one dedicated host thread per lane.
    Dedicated,
}

/// Where the lanes are in a server run.
pub(crate) enum RunPhase {
    Idle,
    /// A server's `start_run` is forking the workers; nothing can be submitted yet.
    Starting,
    Serving(Arc<RunState>),
}

/// What one server run shares between its hosts.
pub(crate) struct RunState {
    pub(crate) hosts: Vec<HostLink>,
    /// The run's channel of each lane, read only for how its worker waited.
    pub(crate) channels: Vec<Arc<Channel<MappedMemory>>>,
    pub(crate) stop: CancellationToken,
    /// It is called on the thread where the event happens, and never on a request's path.
    pub(crate) tell_run: Box<dyn Fn(LaneRunEvent) + Send + Sync>,
}

impl RunState {
    /// Hands `job` to the first host from `start` on (round-robin order) that still serves,
    /// other than `skip`. A host whose inbox is gone since it was checked is passed over, and
    /// the next one is tried; the job comes back when no host takes it.
    #[expect(
        clippy::result_large_err,
        reason = "a job nobody takes comes back by value, to be answered"
    )]
    pub(crate) fn hand_to(&self, start: usize, skip: Option<usize>, job: LaneJob) -> Result<(), LaneJob> {
        let n = self.hosts.len();
        let mut job = job;
        for i in 0..n {
            let k = (start + i) % n;
            if Some(k) == skip || !self.hosts[k].serving() {
                continue;
            }
            match self.hosts[k].send(job) {
                Ok(()) => return Ok(()),
                Err(back) => job = back,
            }
        }
        Err(job)
    }
}

/// One lane's part of a run, as its host takes it: the host's end of the lane's channel, and the
/// pipe the lane's workers wake the host with.
pub(crate) struct RunLane {
    pub(crate) index: LaneIndex,
    pub(crate) end: LaneEnd,
    pub(crate) bell: RunBell,
}

/// The pipe a lane's workers wake its host with, one per lane in a run:
/// its read end, and the write end, which the template hands each worker of the lane and the host
/// keeps for the run as well. A pipe with no write end left reads as end of file, which a reactor
/// reports readable for good: once the template and the workers are gone, the host's poller would
/// wake for it on every round.
pub(crate) struct RunBell {
    pub(crate) bell: HostBell,
    pub(crate) write: OwnedFd,
}

/// The server's lanes: how each lane's channel is laid out and how the hosts run them, the
/// workers, the template that forks them, and the current server run.
pub struct ServerLanes {
    lanes: usize,
    pub(crate) geometry: Geometry,
    pub(crate) host: HostConfig,
    /// Holds each lane's current worker; written by whoever forks it, kills it or sees it die.
    pub(crate) processes: Vec<Mutex<Option<Arc<WorkerProcess>>>>,
    pub(crate) template: OnceLock<Template>,
    /// The wait statuses the template reported, filled by whoever reads its socket.
    pub(super) exits: ExitReports,
    pub(crate) run: RwLock<RunPhase>,
    pub(crate) dedicated: Mutex<Vec<OsThread<()>>>,
    /// Counts the start attempts of every lane over every run, so no two share a number: the
    /// template's late answer to one is never taken for another's.
    attempts: AtomicU32,
    /// Holds what the current run's failures decided, or the last run's once it ended; a new run
    /// replaces it.
    pub(crate) health: Mutex<RunHealth>,
    /// Holds why the current run, or the last one, first failed. It is set before
    /// [`RunHealth::failed`] decides, so a run that stopped always has it.
    pub(crate) first_failure: Mutex<Option<LaneRunError>>,
    pub(crate) stats: Stats,
    /// Has one entry per lane, since no run has more hosts than lanes, and lives as long as the
    /// server. Host `i` of a run is the only writer of entry `i`, which is read for stats only.
    pub(crate) host_counts: Vec<CacheLine<HostCounts>>,
    /// How the workers of the runs that ended waited for requests; each run's channels count
    /// only that run.
    pub(crate) ended_peer_waits: Mutex<WaitTotals>,
    /// Holds, per lane, why it crash-loops while it does; its host writes it and the server's
    /// metrics read it.
    pub(crate) crash_loops: Vec<Mutex<Option<String>>>,
}

/// Hands a run's `lanes` to `hosts` hosts, lane `j` to host [`host_of`]`(j)`: the run they and the
/// other threads share. A host that would get no lane is not made.
fn run_of(
    lanes: Vec<RunLane>,
    hosts: usize,
    config: &HostConfig,
    tell_run: Box<dyn Fn(LaneRunEvent) + Send + Sync>,
) -> RunState {
    let channels = lanes.iter().map(|lane| Arc::clone(lane.end.channel())).collect();
    let mut by_host: Vec<Vec<RunLane>> = (0..hosts).map(|_| Vec::new()).collect();
    for lane in lanes {
        by_host[host_of(lane.index, hosts)].push(lane);
    }
    let links = by_host
        .into_iter()
        .filter_map(|lanes| {
            let capacity = config.inbox_capacity(NonZeroUsize::new(lanes.len())?);
            Some(HostLink {
                inbox: Inbox::new(capacity),
                usable_lanes: AtomicUsize::new(lanes.len()),
                start: Mutex::new(Some(lanes)),
                load: AtomicUsize::new(0),
                failure: OnceLock::new(),
                crash_wait: Mutex::new(None),
            })
        })
        .collect();
    RunState {
        hosts: links,
        channels,
        stop: CancellationToken::new(),
        tell_run,
    }
}

/// Which of a run's `hosts` hosts lane `lane` belongs to.
fn host_of(lane: LaneIndex, hosts: usize) -> usize {
    lane.0 % hosts
}

/// What other threads hold of one host: where to send it a request, and how busy it is.
pub(crate) struct HostLink {
    pub(crate) inbox: Inbox<LaneJob>,
    /// The host's lanes, which it takes when it starts.
    pub(crate) start: Mutex<Option<Vec<RunLane>>>,
    /// Requests in its lanes and queued behind them, as the host last published it (read by
    /// other threads to decide whether to spread a request).
    pub(crate) load: AtomicUsize,
    /// Counts this host's lanes that take requests ([`LaneWorker::takes_requests`](super::core::LaneWorker::takes_requests)); the host
    /// takes requests while one is left. Only the host's poller writes it, as its lanes break
    /// and enter and leave a crash-loop wait.
    pub(crate) usable_lanes: AtomicUsize,
    /// Why the host stopped reading its lanes while the run serves (its poller failed);
    /// unset while it reads them.
    pub(crate) failure: OnceLock<Arc<HostError>>,
    /// Holds the wait of this host's crash-looping lane ([`LaneWorker::CrashWait`](super::core::LaneWorker::CrashWait)) that forks
    /// first, `None` while none waits. The host's poller writes it as its lanes enter and leave
    /// the wait; it is read only to refuse a request no host takes.
    pub(crate) crash_wait: Mutex<Option<CrashLoopWait>>,
}

impl HostLink {
    pub(crate) fn serving(&self) -> bool {
        self.failure.get().is_none() && self.usable_lanes.load(Relaxed) > 0 && !self.inbox.is_closed()
    }

    /// Hands `job` to the host. A full inbox counts as handed: the job is refused in its reply as
    /// the host refuses one past its lanes' room. A closed one gives the job back, to go elsewhere.
    #[expect(
        clippy::result_large_err,
        reason = "a job the inbox no longer takes comes back by value, to go elsewhere"
    )]
    pub(crate) fn send(&self, job: LaneJob) -> Result<(), LaneJob> {
        match self.inbox.send(job) {
            Ok(()) => Ok(()),
            Err(InboxRefusal::Full(job)) => {
                answer_lane(job.reply, Err(LaneRequestError::Rejected(health::lanes_full())));
                Ok(())
            }
            Err(InboxRefusal::Closed(job)) => Err(job),
        }
    }
}

/// Whether the hosts of a run started, from every host's report: all are heard before any
/// failure is returned (so no host is still starting when the caller stops the run), and the
/// first host that could not start fails the start with its own cause. The caller then stops
/// the run, which stops the hosts that did start.
fn hosts_started(reports: impl IntoIterator<Item = Result<(), LaneStartError>>) -> Result<(), LaneStartError> {
    let reports: Vec<_> = reports.into_iter().collect();
    reports.into_iter().collect()
}

/// A lane's starting worker as one look saw it, while the start waits for it to be ready.
struct WorkerStartSnapshot {
    /// What the worker reported on its start attempt, if it has.
    report: Result<Option<StartOutcome>, StartReportError>,
    /// The wait status the template recorded for this worker, once it reaped it.
    reaped: Option<i32>,
    /// When the wait first found the worker gone, if it has.
    gone_since: Option<Instant>,
    now: Instant,
}

/// Where a starting worker is, after one look.
#[derive(Debug)]
enum StartWait {
    Ready,
    Waiting,
    Failed(LaneStartError),
}

/// One look at the worker `pid` of lane `lane` while the start waits for it (until
/// `deadline`). A worker that reported a failed start fails the start with its traceback; one
/// that ended without reporting fails it with how it ended (its wait status, or, when the
/// template has not reported it within `exit_status_wait` of the worker being gone, that).
fn starting_worker(
    lane: LaneIndex,
    pid: Pid,
    look: WorkerStartSnapshot,
    deadline: Instant,
    exit_status_wait: Duration,
) -> StartWait {
    match look.report {
        Ok(Some(StartOutcome::Ready)) => return StartWait::Ready,
        Ok(Some(StartOutcome::Failed(note))) => {
            let failure = String::from_utf8_lossy(&note).into_owned();
            return StartWait::Failed(LaneStartError::WorkerStart { lane, pid, failure });
        }
        Err(source) => return StartWait::Failed(LaneStartError::WorkerReportUnreadable { lane, pid, source }),
        Ok(None) => {}
    }
    let exit = match (look.reaped, look.gone_since) {
        (Some(status), _) => Some(worker_exit(status)),
        (None, Some(since)) if look.now.duration_since(since) > exit_status_wait => {
            Some(WorkerExit::Unreported(exit_status_wait))
        }
        (None, _) => None,
    };
    if let Some(exit) = exit {
        return StartWait::Failed(LaneStartError::DiedBeforeReady { lane, pid, exit });
    }
    if look.now > deadline {
        return StartWait::Failed(LaneStartError::NotReady { lane, pid });
    }
    StartWait::Waiting
}

/// Returns when a worker the start still waits for must be looked at again though nothing woke
/// the start: when the template's report of its exit is due, once it was seen gone, and at the
/// latest at the deadline.
fn look_again_by(gone_since: Option<Instant>, deadline: Instant, exit_status_wait: Duration) -> Instant {
    gone_since
        .and_then(|since| since.checked_add(exit_status_wait))
        .map_or(deadline, |due| due.min(deadline))
}

/// Marks in `gone`, at `now`, each starting worker (by its place in `workers`) the exit watch
/// saw exit, unless an earlier look did, and stops watching it: a pidfd that exited stays
/// readable, and would wake every later sleep.
fn see_exits(
    watch: &super::os::ExitWatch,
    workers: &[(Arc<WorkerProcess>, Attempt)],
    gone: &mut [Option<Instant>],
    now: Instant,
) -> std::io::Result<()> {
    const ROOM: usize = 16;
    loop {
        let seen = watch.exited::<ROOM>(Some(Duration::ZERO), |token| {
            let place = usize::try_from(token).ok();
            let (Some((worker, _)), Some(since)) =
                (place.and_then(|k| workers.get(k)), place.and_then(|k| gone.get_mut(k)))
            else {
                return Err(std::io::Error::other(format!(
                    "the exit watch reported token {token}, which no starting worker has"
                )));
            };
            watch.forget(worker.handle())?;
            worker.record_exit();
            since.get_or_insert(now);
            Ok(())
        })?;
        if seen < ROOM {
            return Ok(());
        }
    }
}

/// Sleeps until one of `fds` is readable or `by` passes. A sleep a signal interrupted ends
/// early, which only means one more look.
fn sleep_until_readable<const N: usize>(fds: [BorrowedFd<'_>; N], by: Instant) -> Result<(), rustix::io::Errno> {
    use rustix::event::{PollFd, PollFlags, Timespec};
    let left = by.saturating_duration_since(Instant::now());
    let timeout = Timespec {
        tv_sec: i64::try_from(left.as_secs()).unwrap_or(i64::MAX),
        tv_nsec: left.subsec_nanos().into(),
    };
    let mut polls = fds.each_ref().map(|fd| PollFd::new(fd, PollFlags::IN));
    match rustix::event::poll(&mut polls, Some(&timeout)) {
        Ok(_) | Err(rustix::io::Errno::INTR) => Ok(()),
        Err(errno) => Err(errno),
    }
}

/// Waits until `fd` is readable, however long that is; a wait a signal interrupted is made again.
fn wait_readable(fd: BorrowedFd<'_>) -> Result<(), rustix::io::Errno> {
    use rustix::event::{PollFd, PollFlags};
    let mut polls = [PollFd::new(&fd, PollFlags::IN)];
    loop {
        match rustix::event::poll(&mut polls, None) {
            Ok(_) => return Ok(()),
            Err(rustix::io::Errno::INTR) => {}
            Err(errno) => return Err(errno),
        }
    }
}

/// Waits for the dedicated host threads to end by `deadline`, as a pool's workers
/// are joined. A host still running then is reported and left detached: waiting for it could
/// hang the shutdown (a host blocked on a handler), so its thread is leaked instead. Returns
/// the ones left.
fn join_hosts(threads: Vec<OsThread<()>>, deadline: &ShutdownDeadline) -> Vec<String> {
    let mut left = Vec::new();
    let deadline_at = deadline.fix(Instant::now());
    for handle in threads {
        let thread = handle.name();
        match handle.join_by(deadline_at) {
            Ok(()) => {}
            Err(ThreadJoinError::Panicked(message)) => tracing::error!(
                target: "mipyre::server",
                thread = %thread,
                message = %message,
                "a process lane host thread panicked",
            ),
            Err(ThreadJoinError::StillRunning) => {
                tracing::error!(
                    target: "mipyre::server",
                    thread = %thread,
                    "a process lane host thread did not stop within {:?}; abandoning it",
                    deadline.grace(),
                );
                left.push(thread);
            }
        }
    }
    left
}

impl ServerLanes {
    /// Creates the server's `lanes` lanes, each run's channels laid out by `geometry`.
    pub(crate) fn create(lanes: NonZeroUsize, geometry: Geometry, host: HostConfig) -> Self {
        let n = lanes.get();
        ServerLanes {
            lanes: n,
            geometry,
            host,
            processes: (0..n).map(|_| Mutex::new(None)).collect(),
            template: OnceLock::new(),
            exits: ExitReports::new(n),
            run: RwLock::new(RunPhase::Idle),
            dedicated: Mutex::new(Vec::new()),
            attempts: AtomicU32::new(0),
            health: Mutex::new(RunHealth::new(n, host.on_failure())),
            first_failure: Mutex::new(None),
            stats: Stats::default(),
            host_counts: (0..n).map(|_| CacheLine(HostCounts::default())).collect(),
            ended_peer_waits: Mutex::new(WaitTotals::default()),
            crash_loops: (0..n).map(|_| Mutex::new(None)).collect(),
        }
    }

    pub fn workers(&self) -> usize {
        self.lanes
    }

    /// Takes the number of a new start attempt.
    pub(crate) fn take_attempt(&self) -> Attempt {
        Attempt::new(self.attempts.fetch_add(1, Relaxed).wrapping_add(1))
    }

    /// Makes the run's channel for each lane and hands it to the template, forks one worker per
    /// lane, waits until they are ready, and sets up the hosts.
    pub fn start_run(
        self: &Arc<Self>,
        hosting: HostKind,
        tell_run: Box<dyn Fn(LaneRunEvent) + Send + Sync>,
    ) -> Result<(), LaneStartError> {
        {
            let mut phase = self.run.write();
            if !matches!(*phase, RunPhase::Idle) {
                return Err(LaneStartError::AlreadyServing);
            }
            *phase = RunPhase::Starting;
        }
        let n = self.workers();
        *self.health.lock() = RunHealth::new(n, self.host.on_failure());
        *self.first_failure.lock() = None;
        let template = self.template.get().ok_or(LaneStartError::Fork {
            lane: LaneIndex(0),
            source: ForkError::NoTemplate,
        })?;
        let hosts = match hosting {
            HostKind::Tpc { threads } => n.min(threads.max(1)),
            HostKind::Dedicated => n,
        };
        let (mut lanes, shms) = self.open_lanes()?;
        for (lane, shm) in lanes.iter().zip(shms) {
            // The template keeps the pair until the next run's; the server needs the shared memory
            // object no more once it went.
            let i = lane.index;
            template
                .send_lane(i, [shm.as_fd(), lane.bell.write.as_fd()], &self.exits)
                .map_err(|fork| fork_failed(i, fork))?;
        }
        let mut spawned = Vec::with_capacity(n);
        for lane in &mut lanes {
            let i = lane.index;
            let attempt = self.take_attempt();
            lane.end.begin_attempt(attempt);
            let worker = Arc::new(
                self.spawn_worker(i, attempt, FORK_ANSWER_TIMEOUT)
                    .map_err(|fork| fork_failed(i, fork))?,
            );
            *self.processes[i.0].lock() = Some(Arc::clone(&worker));
            spawned.push((worker, attempt));
        }
        self.wait_until_ready(template, &mut lanes, &spawned)?;
        // Built here, before any host thread exists: a runtime that cannot be built fails
        // this start, rather than leaving a host thread that logs it and serves nothing.
        let runtimes = match hosting {
            HostKind::Dedicated => (0..hosts)
                .map(|_| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .on_thread_park(enter_idle)
                        .on_thread_unpark(resume_from_idle)
                        .build()
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(LaneStartError::Runtime)?,
            HostKind::Tpc { .. } => Vec::new(),
        };
        self.serve(lanes, hosts, tell_run);
        if let HostKind::Dedicated = hosting {
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            {
                let mut threads = self.dedicated.lock();
                for (h, rt) in runtimes.into_iter().enumerate() {
                    let lanes = Arc::clone(self);
                    let started = started_tx.clone();
                    let spawned = mipyre_tpc::spawn(
                        std::thread::Builder::new().name(format!("mipyre-lane-host-{h}")),
                        move || {
                            let local = tokio::task::LocalSet::new();
                            local.block_on(&rt, async move {
                                let (report, host) = match LaneHost::start(&lanes, h) {
                                    Ok(host) => (Ok(()), host),
                                    Err(source) => (Err(LaneStartError::HostStart { host: h, source }), None),
                                };
                                // Unheard only when start_run gave up first (a later host
                                // thread could not be spawned); its caller stops the run.
                                if started.send(report).is_err() {
                                    tracing::debug!(
                                        target: "mipyre::server",
                                        host = h,
                                        "the process lanes' start stopped waiting before this host reported"
                                    );
                                }
                                // The sender goes now: start_run waits until every host's has.
                                drop(started);
                                if let Some(host) = host {
                                    host.stopped().await;
                                }
                            });
                        },
                    )
                    .map_err(|source| LaneStartError::HostThread { host: h, source })?;
                    threads.push(spawned);
                }
            }
            drop(started_tx);
            hosts_started(started_rx.iter())?;
        }
        Ok(())
    }

    /// Waits until the worker of each lane reported ready, and fails the start with the first that
    /// cannot be. Between looks it sleeps until the lane's worker rings its pipe, a worker exits,
    /// the template reports, or the look's decision could change with time.
    fn wait_until_ready(
        &self,
        template: &Template,
        lanes: &mut [RunLane],
        spawned: &[(Arc<WorkerProcess>, Attempt)],
    ) -> Result<(), LaneStartError> {
        let deadline = Instant::now() + self.host.worker_start_timeout();
        let watch = super::os::ExitWatch::new().map_err(LaneStartError::StartWatch)?;
        let mut gone = vec![None; spawned.len()];
        for (k, (worker, _)) in spawned.iter().enumerate() {
            if watch
                .add(worker.handle(), worker.pid().into(), k as u64)
                .map_err(LaneStartError::StartWatch)?
                == super::os::Registration::Gone
            {
                worker.record_exit();
                gone[k] = Some(Instant::now());
            }
        }
        // Nothing else talks to the template while a run starts: the hosts, which fork
        // replacements and read its reports, are not up yet.
        let mut link = template.link.lock();
        for (k, (lane, (worker, attempt))) in lanes.iter_mut().zip(spawned).enumerate() {
            let (i, pid) = (lane.index, worker.pid());
            loop {
                let now = Instant::now();
                // Read in the order the worker and the template write: a worker writes its start
                // report before it ends, and is reaped after. So what ended it is looked at
                // first, and a report it left is never missed.
                see_exits(&watch, spawned, &mut gone, now).map_err(LaneStartError::StartWatch)?;
                template.take_ready(&mut link, &self.exits);
                let look = WorkerStartSnapshot {
                    report: lane.end.start_report(*attempt),
                    reaped: self.exits.status_of(i, pid),
                    gone_since: gone[k],
                    now,
                };
                match starting_worker(i, pid, look, deadline, self.host.exit_status_wait()) {
                    StartWait::Ready => break,
                    StartWait::Waiting => {}
                    StartWait::Failed(e) => return Err(e),
                }
                let by = look_again_by(gone[k], deadline, self.host.exit_status_wait());
                let bell = lane.bell.bell.as_fd();
                let slept = match link.ended() {
                    None => sleep_until_readable([bell, watch.as_fd(), link.socket()], by),
                    Some(_) => sleep_until_readable([bell, watch.as_fd()], by),
                };
                slept.map_err(|errno| LaneStartError::StartWatch(errno.into()))?;
                let drained =
                    drain_once(&lane.bell.bell).map_err(|source| LaneStartError::BellUnreadable { lane: i, source })?;
                lane.end
                    .hear_wakes(drained.bytes)
                    .map_err(|fault| LaneStartError::WorkerFault { lane: i, pid, fault })?;
            }
        }
        Ok(())
    }

    /// Hands `lanes` to `hosts` hosts and makes theirs the run the lanes serve.
    fn serve(
        &self,
        lanes: Vec<RunLane>,
        hosts: usize,
        tell_run: Box<dyn Fn(LaneRunEvent) + Send + Sync>,
    ) -> Arc<RunState> {
        let run = Arc::new(run_of(lanes, hosts, &self.host, tell_run));
        *self.run.write() = RunPhase::Serving(Arc::clone(&run));
        run
    }

    /// Makes this run's channel and wake pipe for every lane: the host's part of each, and each
    /// one's shared memory object, which goes to the lane's workers.
    fn open_lanes(&self) -> Result<(Vec<RunLane>, Vec<OwnedFd>), LaneStartError> {
        let n = self.workers();
        let (mut lanes, mut fds) = (Vec::with_capacity(n), Vec::with_capacity(n));
        for i in 0..n {
            let lane = LaneIndex(i);
            let (memory, shm) =
                MappedMemory::create(&self.geometry).map_err(|source| LaneStartError::Channel { lane, source })?;
            let (bell, write) = open_host_doorbell().map_err(|source| LaneStartError::Bell { lane, source })?;
            lanes.push(RunLane {
                index: lane,
                end: HostEnd::new(Arc::new(Channel::new(memory)), FutexDoorbell),
                bell: RunBell { bell, write },
            });
            fds.push(shm);
        }
        Ok((lanes, fds))
    }

    /// Ends the run: stops the hosts (they answer what is still waiting with 503), kills the
    /// workers. The template stays for the next run. The dedicated host threads get until
    /// `deadline` to end: the server's stop deadline, the one its pool workers and TPC threads
    /// are joined by. Returns the names of the host threads still running then, each already
    /// logged and left detached.
    pub fn stop_run(&self, deadline: &ShutdownDeadline) -> Vec<String> {
        let ended = std::mem::replace(&mut *self.run.write(), RunPhase::Idle);
        match ended {
            RunPhase::Idle => return Vec::new(),
            RunPhase::Starting => {}
            RunPhase::Serving(run) => {
                run.stop.cancel();
                let mut ended = self.ended_peer_waits.lock();
                *ended = sum_waits(std::iter::once(*ended).chain(run.channels.iter().map(|c| c.peer_waits())));
            }
        }
        let abandoned = join_hosts(std::mem::take(&mut *self.dedicated.lock()), deadline);
        for p in &self.processes {
            if let Some(worker) = p.lock().take() {
                worker.kill();
            }
        }
        abandoned
    }

    /// Asks the template for a new worker on lane `i`, as start attempt `attempt`, and returns
    /// it, blocking until the template answers or `within` passed. A request that ran out of
    /// time is not waited for again: a thread waits on the socket for its late answer, which
    /// carries its own attempt, and ends the worker it names as soon as it comes.
    pub(crate) fn spawn_worker(
        self: &Arc<Self>,
        i: LaneIndex,
        attempt: Attempt,
        within: Duration,
    ) -> Result<WorkerProcess, ForkError> {
        let template = self.template.get().ok_or(ForkError::NoTemplate)?;
        let forked = template.fork(i, attempt, within, &self.exits);
        if let Err(ForkError::TimedOut { .. }) = forked {
            self.watch_late_answers(template);
        }
        forked
    }

    /// Starts the thread that reads the template's late answers, unless one already waits for
    /// them. Without it a late worker would wait on its lane until somebody next read the socket
    /// (the next fork, the next run's start, the next exit status).
    fn watch_late_answers(self: &Arc<Self>, template: &Template) {
        let mut link = template.link.lock();
        if !link.start_watch() {
            return;
        }
        let started = link.socket().try_clone_to_owned().and_then(|sock| {
            let lanes = Arc::clone(self);
            std::thread::Builder::new()
                .name("mipyre-late-forks".into())
                .spawn(move || lanes.read_late_answers(&sock))
        });
        if let Err(error) = started {
            link.drop_watch();
            tracing::warn!(
                target: "mipyre::server",
                %error,
                "no thread could wait for the template's late fork answers: each of their workers \
                 is ended when the template's socket is next read"
            );
        }
    }

    /// Reads the template's socket each time it is readable until no late answer is owed or the
    /// connection ended. It reads only when no fork exchange holds the socket, and an exchange
    /// holds it until its own answer came or it gave up, so every answer it reads is a late one.
    fn read_late_answers(&self, sock: &OwnedFd) {
        let Some(template) = self.template.get() else {
            tracing::error!(
                target: "mipyre::server",
                "the template's late fork answers are watched with no template attached"
            );
            return;
        };
        loop {
            {
                let mut link = template.link.lock();
                template.take_ready(&mut link, &self.exits);
                if link.end_watch() {
                    return;
                }
            }
            if let Err(errno) = wait_readable(sock.as_fd()) {
                template.link.lock().drop_watch();
                tracing::warn!(
                    target: "mipyre::server",
                    %errno,
                    "waiting for the template's late fork answers failed: each of their workers is \
                     ended when the template's socket is next read"
                );
                return;
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Serves `lanes` with `hosts` hosts as [`ServerLanes::start_run`] does once its workers are
    /// ready, with no worker forked, and returns the run and each lane's shared memory object.
    #[cfg(not(sync_model))]
    pub(crate) fn serve_unforked(
        lanes: &ServerLanes,
        hosts: usize,
        tell_run: Box<dyn Fn(LaneRunEvent) + Send + Sync>,
    ) -> Result<(Arc<RunState>, Vec<OwnedFd>), LaneStartError> {
        let (run_lanes, fds) = lanes.open_lanes()?;
        Ok((lanes.serve(run_lanes, hosts, tell_run), fds))
    }

    const STATUS_WAIT: Duration = Duration::from_millis(100);

    /// Builds a look, at `now`, at a starting worker that is alive and has not reported.
    fn look_at(now: Instant) -> WorkerStartSnapshot {
        WorkerStartSnapshot {
            report: Ok(None),
            reaped: None,
            gone_since: None,
            now,
        }
    }

    /// A starting worker is ready once it reported so; one that reported a failed start fails the
    /// start with its traceback, even after it ended; one whose report no worker writes fails it
    /// with what was read.
    #[test]
    fn a_starting_worker_is_ready_by_its_report_and_fails_by_its_traceback() {
        let pid = Pid::new(4242).unwrap();
        let t = Instant::now();
        let deadline = t + Duration::from_secs(30);
        let ready = WorkerStartSnapshot {
            report: Ok(Some(StartOutcome::Ready)),
            ..look_at(t)
        };
        assert!(matches!(
            starting_worker(LaneIndex(0), pid, ready, deadline, STATUS_WAIT),
            StartWait::Ready
        ));
        assert!(matches!(
            starting_worker(LaneIndex(0), pid, look_at(t), deadline, STATUS_WAIT),
            StartWait::Waiting
        ));
        let failed = WorkerStartSnapshot {
            report: Ok(Some(StartOutcome::Failed(b"Traceback: ImportError".to_vec()))),
            reaped: Some(70 << 8),
            gone_since: Some(t),
            ..look_at(t)
        };
        match starting_worker(LaneIndex(1), pid, failed, deadline, STATUS_WAIT) {
            StartWait::Failed(LaneStartError::WorkerStart {
                lane: LaneIndex(1),
                pid: p,
                failure,
            }) => {
                assert_eq!(p, pid);
                assert_eq!(failure, "Traceback: ImportError");
            }
            other => panic!("expected the worker's start failure, got {other:?}"),
        }
        let unreadable = WorkerStartSnapshot {
            report: Err(StartReportError::UnknownState(3)),
            ..look_at(t)
        };
        assert!(matches!(
            starting_worker(LaneIndex(1), pid, unreadable, deadline, STATUS_WAIT),
            StartWait::Failed(LaneStartError::WorkerReportUnreadable {
                source: StartReportError::UnknownState(3),
                ..
            })
        ));
    }

    /// A starting worker that ended without a start failure fails the start with how it ended,
    /// as the template reaped it: its exit status, or its signal.
    #[test]
    fn a_starting_worker_that_ended_before_ready_fails_the_start_with_its_exit() {
        let pid = Pid::new(4242).unwrap();
        let t = Instant::now();
        let deadline = t + Duration::from_secs(30);
        let killed =
            |exit: &WorkerExit| matches!(exit, WorkerExit::Signaled { signal, .. } if *signal == libc::SIGKILL);
        let exited_3 = |exit: &WorkerExit| *exit == WorkerExit::Exited(3);
        for (status, want) in [
            (3 << 8, &exited_3 as &dyn Fn(&WorkerExit) -> bool),
            (libc::SIGKILL, &killed),
        ] {
            let ended = WorkerStartSnapshot {
                reaped: Some(status),
                ..look_at(t)
            };
            match starting_worker(LaneIndex(2), pid, ended, deadline, STATUS_WAIT) {
                StartWait::Failed(LaneStartError::DiedBeforeReady {
                    lane: LaneIndex(2),
                    pid: p,
                    exit,
                }) => {
                    assert_eq!(p, pid);
                    assert!(want(&exit), "status {status:#x} read as {exit:?}");
                }
                other => panic!("expected the worker's exit, got {other:?}"),
            }
        }
    }

    /// A worker found gone whose wait status the template has not reported is waited for up to
    /// the exit-status wait, and past it fails the start saying the status went unreported.
    #[test]
    fn a_starting_worker_gone_unreported_fails_the_start_past_the_exit_status_wait() {
        let pid = Pid::new(4242).unwrap();
        let since = Instant::now();
        let deadline = since + Duration::from_secs(30);
        let at = |now| WorkerStartSnapshot {
            gone_since: Some(since),
            ..look_at(now)
        };
        assert!(matches!(
            starting_worker(LaneIndex(0), pid, at(since + STATUS_WAIT), deadline, STATUS_WAIT),
            StartWait::Waiting
        ));
        match starting_worker(
            LaneIndex(0),
            pid,
            at(since + STATUS_WAIT + Duration::from_nanos(1)),
            deadline,
            STATUS_WAIT,
        ) {
            StartWait::Failed(LaneStartError::DiedBeforeReady {
                exit: WorkerExit::Unreported(waited),
                ..
            }) => assert_eq!(waited, STATUS_WAIT),
            other => panic!("expected an unreported exit, got {other:?}"),
        }
    }

    /// A live worker that is not ready is waited for until the deadline, and past it fails the
    /// start as not ready.
    #[test]
    fn a_live_starting_worker_is_waited_for_until_the_deadline() {
        let pid = Pid::new(4242).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        assert!(matches!(
            starting_worker(LaneIndex(0), pid, look_at(deadline), deadline, STATUS_WAIT),
            StartWait::Waiting
        ));
        assert!(matches!(
            starting_worker(LaneIndex(0), pid, look_at(deadline + Duration::from_nanos(1)), deadline, STATUS_WAIT),
            StartWait::Failed(LaneStartError::NotReady { lane: LaneIndex(0), pid: p }) if p == pid
        ));
    }

    /// A live worker is looked at again by the deadline; one seen gone by when its exit report is
    /// due, unless the deadline comes first, the two meeting at the instant both are due.
    #[test]
    fn a_waited_for_worker_is_looked_at_again_by_its_exit_report_or_the_deadline_whichever_is_first() {
        let since = Instant::now();
        let deadline = since + STATUS_WAIT;
        let nano = Duration::from_nanos(1);
        assert_eq!(look_again_by(None, deadline, STATUS_WAIT), deadline);
        assert_eq!(
            look_again_by(Some(since - nano), deadline, STATUS_WAIT),
            deadline - nano
        );
        assert_eq!(look_again_by(Some(since), deadline, STATUS_WAIT), deadline);
        assert_eq!(look_again_by(Some(since + nano), deadline, STATUS_WAIT), deadline);
    }

    /// A start in which one host could not start fails with that host's cause, once every host
    /// has reported.
    #[test]
    fn a_run_start_hears_every_host_and_fails_with_the_one_that_could_not_start() {
        let reports = [
            Ok(()),
            Err(LaneStartError::HostStart {
                host: 1,
                source: std::io::Error::other("the worker-exit watch: EMFILE"),
            }),
            Ok(()),
        ];
        let mut heard = 0;
        let result = hosts_started(reports.into_iter().inspect(|_| heard += 1));
        assert_eq!(heard, 3, "every host is heard before the start fails");
        match result {
            Err(LaneStartError::HostStart { host: 1, source }) => {
                assert_eq!(source.to_string(), "the worker-exit watch: EMFILE");
            }
            other => panic!("expected host 1's start failure, got {other:?}"),
        }
        assert!(hosts_started([Ok(()), Ok(())]).is_ok());
    }

    #[test]
    fn a_host_thread_that_does_not_stop_is_left_at_the_deadline() {
        // The host stays blocked until the test releases it, after the join has returned: a
        // join that waited for it would never return, so the join returning is the proof and
        // how long it took is not asked. The deadline is short. The finished host
        // beside it shows the list names the one left, not every host.
        let (release, released) = std::sync::mpsc::channel::<()>();
        let stuck = mipyre_tpc::spawn(std::thread::Builder::new().name("stuck-host".into()), move || {
            let _released = released.recv();
        })
        .unwrap();
        let ended = mipyre_tpc::spawn(std::thread::Builder::new().name("ended-host".into()), || {}).unwrap();
        while !ended.is_finished() {
            std::thread::yield_now();
        }
        let left = join_hosts(vec![ended, stuck], &ShutdownDeadline::new(Duration::from_millis(40)));
        assert_eq!(left, ["stuck-host"]);
        drop(release);
    }
}
