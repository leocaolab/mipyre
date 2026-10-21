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
//! between the connections' I/O; after the spin window it flags "sleeping" in the lane's
//! shared memory, re-checks, and waits in the thread's own kqueue/epoll (the runtime's
//! reactor) on the lane's wake pipe, which a worker writes one byte to only when it sees
//! the flag. Worker deaths come through the same reactor: a kqueue with `EVFILT_PROC`
//! (macOS) or an epoll of pidfds (Linux), registered as one more readable fd.
//!
//! When a worker dies the host answers the request it was running (502, or 504 when the
//! host killed it for overrunning the timeout), leaves the requests it had not taken in
//! the lane, and has the template fork a replacement (on the blocking pool, so the event
//! loop never waits for a fork); the new worker continues from the first request not taken.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::rc::Rc;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use futures_util::FutureExt as _;
use crate::sync::{AtomicU64, AtomicUsize, Mutex, Ordering, Ordering::*, RwLock};
use tokio::io::unix::{AsyncFd, AsyncFdReadyGuard};
use tokio::io::Interest;
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::CancellationToken;

use super::health::{
    self, CrashBackoff, CrashLoopWait, CrashStreak, FailurePolicy, FailureResponse, HeadOfLine,
    HostLoad, RespawnStep, RunHealth,
};
use super::pid::{AtomicPid, Pid};
use super::shm::{self, CacheLine, LaneShm, OwnCounter, WaitCounts, WaitTotals, NO_WORKER};
use super::spin::{spin, SpinEnd};
use super::template::{ExitReports, LinkEnd, TemplateCommand, TemplateLink, TemplateReport, TemplateSendError};
use super::wire::{self, LaneRequest, TooLarge};
use crate::exchange::{
    refuse, HandlerError, LaneIndex, LaneDecodeError, UnansweredError, WorkerExit, WorkerFailure,
    WorkerGroup,
};
use crate::runtime::{answer_if_awaited, ServerRunStop, ServerStop};
use mipyre_http::plan::response::HandlerResponse;
use mipyre_shm::plan::ring::{self, ExitSplit, PeerCounterError, Seq, Window};
use mipyre_shm::plan::spin_window::SpinWindow;
use mipyre_tpc::{ThreadJoinError, OsThread, ShutdownDeadline};

/// Why a lane request got no response. The request's side turns it into its error and
/// counts a refusal there, once ([`LaneRequestError::into_handler_error`]).
#[derive(Debug)]
pub(crate) enum LaneRequestError {
    /// The lanes gave up on it: they did not take it (full, no run serving them, no usable
    /// lane), its worker died running it, it ran past the lane timeout, or its lane lost its
    /// worker for good.
    Rejected(UnansweredError),
    TooLarge(TooLarge),
    WorkerFailed(WorkerFailure),
    /// The worker's answer could not be decoded, which only a bug causes.
    Malformed(LaneDecodeError),
}

impl LaneRequestError {
    /// Converts into the request's error, counting a refusal here on the request's side so a
    /// request that already timed out is not counted twice.
    pub(crate) fn into_handler_error(self) -> HandlerError {
        match self {
            LaneRequestError::Rejected(refusal) => refuse(refusal),
            LaneRequestError::TooLarge(too_large) => {
                tracing::debug!(
                    target: "mipyre::server",
                    needed = too_large.needed,
                    room = too_large.room,
                    "a request does not fit a process-lane slot"
                );
                HandlerError::BodyRejected(mipyre_http::io::body::BodyReadError::TooLarge)
            }
            LaneRequestError::WorkerFailed(failure) => HandlerError::LaneWorkerFailed(failure),
            LaneRequestError::Malformed(malformed) => HandlerError::LaneAnswerMalformed(malformed),
        }
    }
}

/// Why the template gave no new worker for a lane.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ForkError {
    #[error("no template process (the app was not prepared with runtime='process' routes)")]
    NoTemplate,
    #[error(transparent)]
    Send(TemplateSendError),
    #[error("reading the forked pid from template pid {template}: {source}")]
    Receive {
        template: Pid,
        source: std::io::Error,
    },
    #[error(
        "reading the forked pid from template pid {template}: the template closed the connection"
    )]
    TemplateClosed { template: Pid },
    #[error("template pid {template} could not fork: {source}")]
    Fork {
        template: Pid,
        source: std::io::Error,
    },
    /// The template did not answer within `waited`; its late answer is read by a later
    /// exchange, which ends the worker it names.
    #[error("template pid {template} did not answer a fork request within {waited:?}")]
    TimedOut { template: Pid, waited: Duration },
    /// With start="fork" the template could not import the app; `traceback` is its own report.
    #[error("template pid {template} could not import the app:\n{traceback}")]
    ImportFailed { template: Pid, traceback: String },
    /// The template got `got` fds on lane `lane`'s frame, not two (it may be out of fds: it holds
    /// two per lane).
    #[error("template pid {template} got {got} of the {want} fds of lane {lane}", want = LANE_FDS)]
    LaneFdsMissing { template: Pid, lane: LaneIndex, got: u32 },
    /// The template broke the socket's protocol; the connection is no use any more.
    #[error("template pid {template} broke the socket protocol: {end}")]
    ProtocolViolated { template: Pid, end: LinkEnd },
    /// The template named a process it did not start; the server never takes it as a worker,
    /// so it is never killed on the template's word.
    #[error("template pid {template} answered pid {pid} to a fork request, but its parent is pid {parent}")]
    NotTemplateChild { template: Pid, pid: Pid, parent: i32 },
    #[error("reading the parent of pid {pid}, which template pid {template} answered to a fork request: {source}")]
    ParentUnread {
        template: Pid,
        pid: Pid,
        source: std::io::Error,
    },
    /// The blocking task that asked for the fork ended without its answer (it panicked, or
    /// the runtime was shutting down).
    #[error("the fork task ended without an answer: {0}")]
    Task(#[from] tokio::task::JoinError),
}

/// Why a lane has no worker for good ([`LaneWorker::Broken`]).
#[derive(Debug, thiserror::Error)]
pub(crate) enum LaneLossError {
    #[error("respawn failed: {0}")]
    NotRespawned(#[source] ForkError),
    /// Its replacement was forked, but its exit cannot be watched.
    #[error("watching worker {pid}: {source}")]
    ExitNotWatched { pid: Pid, source: std::io::Error },
    /// Its replacement could not import the app; `failure` is the worker's own report, a
    /// traceback.
    #[error("its replacement worker could not start:\n{failure}")]
    ReplacementNotStarted { failure: String },
    /// Its host stopped reading it.
    #[error("{0}")]
    HostFailed(Arc<HostError>),
}

/// Why a host's poller stopped reading its lanes while the run serves.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PollerError {
    #[error("the wake pipe of lane {lane} cannot be read: {source}")]
    HostBellUnreadable { lane: LaneIndex, source: std::io::Error },
    #[error("the wake pipe of lane {lane} cannot be waited on: {source}")]
    HostBellUnwatchable { lane: LaneIndex, source: std::io::Error },
    #[error("the worker-exit watch failed: {0}")]
    ExitWatchFailed(std::io::Error),
    /// The poller panicked; this holds the panic's text.
    #[error("{0}")]
    Panicked(String),
}

/// A host that stopped reading its lanes, and why.
#[derive(Debug)]
pub(crate) struct HostError {
    pub(crate) host: usize,
    pub(crate) cause: PollerError,
}

impl std::fmt::Display for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let host = self.host;
        match &self.cause {
            PollerError::Panicked(panic) => write!(f, "process lane host {host} panicked: {panic}"),
            PollerError::HostBellUnreadable { .. }
            | PollerError::HostBellUnwatchable { .. }
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
pub(crate) enum LaneRunError {
    /// A lane lost its worker for good because its respawn failed.
    #[error("{why}")]
    LaneLost { lane: LaneIndex, why: Arc<LaneLossError> },
    /// A host stopped reading its lanes; they all broke with it.
    #[error("{0}")]
    HostFailed(Arc<HostError>),
}

/// Why a server run's lanes could not start.
#[derive(Debug, thiserror::Error)]
pub(crate) enum LaneStartError {
    #[error(
        "the process lanes are already serving another server of this app; a second server \
         with runtime='process' routes needs its own app"
    )]
    AlreadyServing,
    /// The template could not prepare (with start="fork" the app failed to import, or it is
    /// not single-threaded) and said why before it ended; the fork request for `lane` met it.
    #[error("the process-lane template could not start workers (met by the fork request for lane {lane}):\n{failure}")]
    TemplateFailed { lane: LaneIndex, failure: String },
    #[error("forking the worker of lane {lane}: {source}")]
    Fork { lane: LaneIndex, source: ForkError },
    /// The template got `got` of the `want` fds of lane `lane`'s frame (it may be out of fds:
    /// it holds two per lane), and ended.
    #[error("the process-lane template got {got} of the {want} fds of lane {lane}")]
    LaneFds { lane: LaneIndex, got: u32, want: u32 },
    #[error("the process worker of lane {lane} (pid {pid}) could not start:\n{failure}")]
    WorkerStart {
        lane: LaneIndex,
        pid: Pid,
        failure: String,
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
    #[error("could not build a process lane host's runtime: {0}")]
    Runtime(std::io::Error),
    #[error("could not spawn process lane host thread {host}: {source}")]
    HostThread { host: usize, source: std::io::Error },
    #[error("process lane host {host} could not start: {source}")]
    HostStart { host: usize, source: std::io::Error },
}

type LaneReplySender = tokio::sync::oneshot::Sender<Result<HandlerResponse, LaneRequestError>>;

pub(crate) struct LaneJob {
    pub(crate) request: LaneRequest,
    pub(crate) reply: LaneReplySender,
}

/// How the server's hosts run their lanes. The fields are private because
/// [`HostConfig::new`] checks that the queue holds at least one lane's depth.
#[derive(Clone, Copy, Debug)]
pub(crate) struct HostConfig {
    /// A handler running longer than this is killed with its worker (504).
    timeout: Duration,
    /// Requests per lane, in the lane and queued behind it, before 503; at least the lane
    /// depth.
    queue_per_worker: usize,
    /// How long a host spins on its lanes, in nanoseconds, before its thread parks.
    spin_ns: u64,
    on_failure: FailurePolicy,
    crash_backoff: CrashBackoff,
}

/// Why [`HostConfig::new`] refused.
#[derive(Debug, thiserror::Error)]
pub(crate) enum HostConfigError {
    #[error("queue_per_worker ({queue_per_worker}) must be at least the lane depth ({depth})")]
    QueueShorterThanLane { queue_per_worker: usize, depth: u32 },
}

impl HostConfig {
    pub(crate) fn new(
        geometry: super::shm::Geometry,
        timeout: Duration,
        queue_per_worker: usize,
        server_spin: Duration,
        on_failure: FailurePolicy,
    ) -> Result<Self, HostConfigError> {
        let depth = geometry.depth();
        if queue_per_worker < depth as usize {
            return Err(HostConfigError::QueueShorterThanLane {
                queue_per_worker,
                depth,
            });
        }
        Ok(HostConfig {
            timeout,
            queue_per_worker,
            // A spin longer than u64 nanoseconds hold (584 years) saturates and never ends.
            spin_ns: u64::try_from(server_spin.as_nanos()).unwrap_or(u64::MAX),
            on_failure,
            crash_backoff: CrashBackoff::DEFAULT,
        })
    }
}

/// Caps the respawn latencies kept for `stats()` to the most recent ones, so a server that
/// loses workers for months does not grow by them.
const RESPAWN_SAMPLES: usize = 10_000;

/// Counters a test or a bench reads (`ProcessLanes.stats()`).
#[derive(Default)]
pub(crate) struct Stats {
    pub(crate) deaths: AtomicU64,
    pub(crate) timeouts_killed: AtomicU64,
    /// Counts the workers killed for writing a lane counter the protocol cannot produce.
    pub(crate) peer_faults: AtomicU64,
    pub(crate) respawns: AtomicU64,
    pub(crate) respawn_failures: AtomicU64,
    /// Replacement fork asked → new worker ready, per respawn, in microseconds (the fork is
    /// asked when the death is seen, or once a crash-looping lane's wait is over).
    pub(crate) respawn_us: Mutex<Vec<u64>>,
    /// Counts the times a host went to sleep on its wake pipe, and the times a worker's byte
    /// woke it.
    pub(crate) host_sleeps: AtomicU64,
    pub(crate) pipe_wakes: AtomicU64,
    /// Requests a TPC thread forwarded to another host because its own lanes were full.
    pub(crate) overflow_forwards: AtomicU64,
    /// Requests a busy host thread handed to a less loaded host.
    pub(crate) spread_forwards: AtomicU64,
    pub(crate) host_failures: AtomicU64,
}

/// Where a server run hosts the lanes.
#[derive(Clone, Copy, Debug)]
pub(crate) enum HostKind {
    Tpc { threads: usize },
    /// The non-TPC servers run one dedicated host thread per lane.
    Dedicated,
}

/// The process that forks the lane workers, and the server's side of the socket to it.
pub(super) struct Template {
    pid: Pid,
    /// Held by whoever reads or writes the socket: a fork exchange for as long as it waits for
    /// its answer, a look for exit reports only when nobody holds it (it never waits for it).
    link: Mutex<TemplateLink>,
}

/// How many fds a [`TemplateCommand::Lane`] frame carries: the shared memory and the lane's
/// wake-pipe write end.
const LANE_FDS: u32 = 2;

/// Where the lanes are in a server run.
enum RunPhase {
    Idle,
    /// A server's `start_run` is forking the workers; nothing can be submitted yet.
    Starting,
    Serving(Arc<RunState>),
}

/// What one server run shares between its hosts.
struct RunState {
    hosts: Vec<HostLink>,
    stop: CancellationToken,
    /// The server's stop, which the lanes stop with their cause when they end the server (see
    /// [`LaneHost::carry_out`]).
    server_stop: ServerRunStop,
}

impl RunState {
    /// Hands `job` to the first host from `start` on (round-robin order) that still serves,
    /// other than `skip`. A host whose inbox is gone since it was checked is passed over, and
    /// the next one is tried; the job comes back when no host takes it.
    #[expect(
        clippy::result_large_err,
        reason = "a job nobody takes comes back by value, to be answered"
    )]
    fn hand_to(&self, start: usize, skip: Option<usize>, job: LaneJob) -> Result<(), LaneJob> {
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

/// The lanes' memory, as both sides map it: the shared memory, and the write ends of the
/// pipes a worker wakes a sleeping host with. The server makes them; the template maps the
/// same ones (received with its setup) and its workers write to them.
pub(crate) struct LaneMemory {
    pub(crate) region: LaneShm,
    wake_writes: Vec<OwnedFd>,
    /// The server keeps the shared memory object to hand to the template; the template keeps
    /// the one it mapped.
    shm: OwnedFd,
}

/// The server's lanes: their memory, and what only the server has — the wake pipes' read
/// ends its hosts sleep on, how the hosts run, the workers, the template that forks them,
/// and the current server run.
pub(crate) struct ServerLanes {
    pub(crate) memory: LaneMemory,
    wake_reads: Vec<OwnedFd>,
    host: HostConfig,
    pids: Vec<AtomicPid>,
    template: OnceLock<Template>,
    /// The wait statuses the template reported, filled by whoever reads its socket.
    pub(super) exits: ExitReports,
    run: RwLock<RunPhase>,
    dedicated: Mutex<Vec<OsThread<()>>>,
    rr: AtomicUsize,
    /// Holds the current run's failures, or the last run's once it ended; a new run replaces it.
    health: Mutex<RunHealth<LaneRunError>>,
    pub(crate) stats: Stats,
    /// Has one entry per lane, since no run has more hosts than lanes, and lives as long as the
    /// server. Host `i` of a run is the only writer of entry `i`, which is read for stats only.
    pub(crate) host_counts: Vec<CacheLine<HostCounts>>,
}

/// What one host counted: how it waited for its lanes' answers, where it read them, and how
/// often its thread's park hook ran. The host's thread is the only writer ([`OwnCounter`]).
#[derive(Default)]
pub(crate) struct HostCounts {
    pub(crate) waits: WaitCounts,
    /// Counts answers read by a submit on this thread, the busy path.
    read_by_submit: OwnCounter,
    /// Counts answers read by the spin before the thread parks
    /// ([`LaneHost::spin_for_next_request`]).
    read_by_spin: OwnCounter,
    /// Counts answers read by the poller task ([`LaneHost::run`]).
    read_by_poller: OwnCounter,
    park_hooks: OwnCounter,
}

/// A snapshot of [`HostCounts`]' answer counts as plain numbers.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct HostReads {
    pub(crate) by_submit: u64,
    pub(crate) by_spin: u64,
    pub(crate) by_poller: u64,
    pub(crate) park_hooks: u64,
}

impl HostCounts {
    fn reads(&self) -> HostReads {
        HostReads {
            by_submit: self.read_by_submit.read(),
            by_spin: self.read_by_spin.read(),
            by_poller: self.read_by_poller.read(),
            park_hooks: self.park_hooks.read(),
        }
    }
}

fn sum_waits(counts: impl Iterator<Item = WaitTotals>) -> WaitTotals {
    counts.fold(WaitTotals::default(), |a, b| WaitTotals {
        waits: a.waits.wrapping_add(b.waits),
        spin_rounds: a.spin_rounds.wrapping_add(b.spin_rounds),
        parks: a.parks.wrapping_add(b.parks),
        wakes: a.wakes.wrapping_add(b.wakes),
    })
}

/// Makes a lane's wake pipe, both ends nonblocking. Neither end is close-on-exec: the write end
/// goes to the template with its setup (`SCM_RIGHTS`), as before.
fn make_wake_channel() -> std::io::Result<(OwnedFd, OwnedFd)> {
    let (read, write) = rustix::pipe::pipe()?;
    for end in [&read, &write] {
        let flags = rustix::fs::fcntl_getfl(end)?;
        rustix::fs::fcntl_setfl(end, flags | rustix::fs::OFlags::NONBLOCK)?;
    }
    Ok((read, write))
}

/// What other threads hold of one host: where to send it a request, and how busy it is.
struct HostLink {
    inbox: mpsc::UnboundedSender<LaneJob>,
    /// The other end of `inbox`, until the host takes it when it starts.
    receiver: Mutex<Option<mpsc::UnboundedReceiver<LaneJob>>>,
    /// Requests in its lanes and queued behind them, as the host last published it (read by
    /// other threads to decide whether to spread a request).
    load: AtomicUsize,
    /// Counts this host's lanes that take requests ([`LaneWorker::takes_requests`]); the host
    /// takes requests while one is left. Only the host's poller writes it, as its lanes break
    /// and enter and leave a crash-loop wait.
    usable_lanes: AtomicUsize,
    /// Why the host stopped reading its lanes while the run serves (its poller failed);
    /// unset while it reads them.
    failure: OnceLock<Arc<HostError>>,
    /// Holds the wait of this host's crash-looping lane ([`LaneWorker::CrashWait`]) that forks
    /// first, `None` while none waits. The host's poller writes it as its lanes enter and leave
    /// the wait; it is read only to refuse a request no host takes.
    crash_wait: Mutex<Option<CrashLoopWait>>,
}

impl HostLink {
    fn serving(&self) -> bool {
        self.failure.get().is_none()
            && self.usable_lanes.load(Relaxed) > 0
            && !self.inbox.is_closed()
    }

    #[expect(
        clippy::result_large_err,
        reason = "a job the inbox no longer takes comes back by value, to go elsewhere"
    )]
    fn send(&self, job: LaneJob) -> Result<(), LaneJob> {
        self.inbox
            .send(job)
            .map_err(|mpsc::error::SendError(job)| job)
    }
}

impl LaneMemory {
    /// Maps, in the template, the server's shared memory and wake-pipe write ends that came
    /// over the template socket.
    pub(crate) fn attach(
        geometry: super::shm::Geometry,
        shm: OwnedFd,
        wake_writes: Vec<OwnedFd>,
    ) -> std::io::Result<Self> {
        if wake_writes.len() != geometry.lanes() {
            return Err(std::io::Error::other(format!(
                "{} wake pipes for {} lanes",
                wake_writes.len(),
                geometry.lanes()
            )));
        }
        let region = LaneShm::attach(geometry, shm.as_fd())?;
        Ok(LaneMemory {
            region,
            wake_writes,
            shm,
        })
    }

    /// Returns the fds lane `lane`'s frame to the template carries: the shared memory, then the
    /// lane's wake-pipe write end.
    pub(super) fn lane_fds(&self, lane: usize) -> [BorrowedFd<'_>; LANE_FDS as usize] {
        [self.shm.as_fd(), self.wake_writes[lane].as_fd()]
    }

    pub(crate) fn workers(&self) -> usize {
        self.region.geometry().lanes()
    }

    pub(crate) fn wake_channel_fd(&self, lane: usize) -> BorrowedFd<'_> {
        self.wake_writes[lane].as_fd()
    }
}

impl ServerLanes {
    /// Creates the server's lanes with new shared memory and wake pipes.
    pub(crate) fn create(
        geometry: super::shm::Geometry,
        host: HostConfig,
    ) -> std::io::Result<Self> {
        let (region, shm) = LaneShm::create(geometry)?;
        let (wake_reads, wake_writes) = (0..geometry.lanes())
            .map(|_| make_wake_channel())
            .collect::<std::io::Result<(Vec<_>, Vec<_>)>>()?;
        let n = geometry.lanes();
        Ok(ServerLanes {
            memory: LaneMemory {
                region,
                wake_writes,
                shm,
            },
            wake_reads,
            host,
            pids: (0..n).map(|_| AtomicPid::default()).collect(),
            template: OnceLock::new(),
            exits: ExitReports::new(n),
            run: RwLock::new(RunPhase::Idle),
            dedicated: Mutex::new(Vec::new()),
            rr: AtomicUsize::new(0),
            health: Mutex::new(RunHealth::new(n, host.on_failure)),
            stats: Stats::default(),
            host_counts: (0..n).map(|_| CacheLine(HostCounts::default())).collect(),
        })
    }

    pub(crate) fn workers(&self) -> usize {
        self.memory.workers()
    }

    pub(crate) fn host_waits_total(&self) -> WaitTotals {
        sum_waits(self.host_counts.iter().map(|c| c.0.waits.read()))
    }

    pub(crate) fn host_reads_total(&self) -> HostReads {
        self.host_counts
            .iter()
            .map(|c| c.0.reads())
            .fold(HostReads::default(), |a, b| HostReads {
                by_submit: a.by_submit.wrapping_add(b.by_submit),
                by_spin: a.by_spin.wrapping_add(b.by_spin),
                by_poller: a.by_poller.wrapping_add(b.by_poller),
                park_hooks: a.park_hooks.wrapping_add(b.park_hooks),
            })
    }

    pub(crate) fn worker_waits_total(&self) -> WaitTotals {
        sum_waits((0..self.workers()).map(|i| self.memory.region.header(i).worker_waits.0.read()))
    }

    /// Returns why the lanes ended the server, if they did.
    pub(crate) fn fatal(&self) -> Option<LaneRunError> {
        self.health.lock().fatal().cloned()
    }

    /// Counts the current run's lanes that have no worker for good.
    pub(crate) fn broken_lanes(&self) -> usize {
        self.health.lock().broken()
    }

    /// Builds the refusal for a request no host can take, from whether the run ended, the
    /// crash-looping lane that is back first, and the run's first failure (else a host's).
    fn no_host_left(&self, run: &RunState) -> UnansweredError {
        let waiting = run
            .hosts
            .iter()
            .filter_map(|h| h.crash_wait.lock().clone())
            .reduce(CrashLoopWait::sooner)
            .map(|wait| wait.refusal(shm::monotonic_ns()));
        let first = self.health.lock().first_cause().map(ToString::to_string);
        let cause = first
            .or_else(|| {
                run.hosts
                    .iter()
                    .find_map(|h| h.failure.get().map(ToString::to_string))
            })
            .map(Arc::from);
        health::no_host_left(run.stop.is_cancelled(), waiting, cause)
    }

    /// Takes `pid` as the template, `sock` as the socket to it. The lanes have one template: a
    /// second is refused, its pid given back (its socket is closed).
    pub(crate) fn attach_template(&self, pid: Pid, sock: OwnedFd) -> Result<(), Pid> {
        self.template
            .set(Template { pid, link: Mutex::new(TemplateLink::new(pid, sock)) })
            .map_err(|refused| refused.pid)
    }

    pub(crate) fn template_pid(&self) -> Option<Pid> {
        self.template.get().map(|t| t.pid)
    }

    pub(super) fn template(&self) -> Option<&Template> {
        self.template.get()
    }

    /// Takes the template's reports already waiting on its socket, unless a fork exchange is
    /// reading it (and takes them itself).
    fn read_template_reports(&self) {
        if let Some(template) = self.template.get() {
            template.read_ready(&self.exits);
        }
    }

    pub(crate) fn worker_pids(&self) -> Vec<Option<Pid>> {
        self.pids.iter().map(AtomicPid::load).collect()
    }

    /// Queues a request: on this thread's own lanes when it hosts some (no hand-off), else
    /// through a host's inbox. Never waits.
    pub(crate) fn submit(
        self: &Arc<Self>,
        job: LaneJob,
    ) -> Result<(), (LaneRequestError, LaneReplySender)> {
        if let Err(e) = self.check_fits(&job.request.head, &job.request.body) {
            return Err((LaneRequestError::TooLarge(e), job.reply));
        }
        let local = HOST.with(|h| {
            h.borrow()
                .as_ref()
                .filter(|host| Arc::ptr_eq(&host.lanes, self))
                .cloned()
        });
        // A thread hosting lanes of the current run reads the run from its host: no shared
        // lock on this path. Its run stopped once its stop token is cancelled.
        if let Some(host) = local {
            if host.stop.is_cancelled() {
                return Err((
                    LaneRequestError::Rejected(UnansweredError::WorkersGone(WorkerGroup::Lanes)),
                    job.reply,
                ));
            }
            let run = &host.run;
            // This thread's own lanes, unless they are busy and another host's are clearly
            // less so: then that host's inbox (a cross-thread hop, taken only when it buys
            // parallelism; e.g. macOS hands every connection of a SO_REUSEPORT group to one
            // thread). When its own lanes are full, another host too, before 503.
            let job = match self.spread_target(&host) {
                Some(k) => {
                    self.stats.spread_forwards.fetch_add(1, Relaxed);
                    match run.hosts[k].send(job) {
                        Ok(()) => return Ok(()),
                        // Its inbox went since it was checked: this host's own lanes take it.
                        Err(job) => job,
                    }
                }
                None => job,
            };
            return match host.submit(job) {
                Ok(()) => Ok(()),
                // Its lanes are full, or none is usable: another host that still serves takes it.
                Err((refused @ LaneRequestError::Rejected(_), job)) => {
                    let start = self.rr.fetch_add(1, Relaxed) % run.hosts.len();
                    match run.hand_to(start, Some(host.index), job) {
                        Ok(()) => {
                            self.stats.overflow_forwards.fetch_add(1, Relaxed);
                            Ok(())
                        }
                        Err(job) => Err((refused, job.reply)),
                    }
                }
                Err((failed, job)) => Err((failed, job.reply)),
            };
        }
        let run = match &*self.run.read() {
            RunPhase::Serving(run) => Arc::clone(run),
            RunPhase::Idle | RunPhase::Starting => {
                return Err((
                    LaneRequestError::Rejected(UnansweredError::WorkersGone(WorkerGroup::Lanes)),
                    job.reply,
                ))
            }
        };
        let start = self.rr.fetch_add(1, Relaxed) % run.hosts.len();
        match run.hand_to(start, None, job) {
            Ok(()) => Ok(()),
            Err(job) => Err((LaneRequestError::Rejected(self.no_host_left(&run)), job.reply)),
        }
    }

    /// Another host to give a request to instead of `host` ([`health::loaded_to_spread`],
    /// [`health::spread_to`]), while `host`'s handlers run long.
    fn spread_target(&self, host: &LaneHost) -> Option<usize> {
        let run = &host.run;
        if run.hosts.len() < 2 {
            return None;
        }
        let own = run.hosts[host.index].load.load(Relaxed);
        if !health::loaded_to_spread(own, host.owned.borrow().len()) || !host.running_long() {
            return None;
        }
        let start = self.rr.fetch_add(1, Relaxed) % run.hosts.len();
        let others = (0..run.hosts.len())
            .map(|i| (start + i) % run.hosts.len())
            .filter(|&i| i != host.index && run.hosts[i].serving())
            .map(|i| HostLoad {
                index: i,
                load: run.hosts[i].load.load(Relaxed),
            });
        health::spread_to(own, others)
    }

    /// Checks that a request fits a slot, so one that does not gets 413 before it is queued.
    fn check_fits(&self, head: &crate::exchange::RequestHead, body: &[u8]) -> Result<(), TooLarge> {
        fits(
            approx_message_len(head, body),
            self.memory.region.geometry().max_message(),
        )
    }

    fn write_request(&self, lane: usize, seq: u32, job: &LaneJob) -> Result<(), TooLarge> {
        let slot = self.memory.region.req_slot(lane, seq);
        // SAFETY: slot `seq` is free (its host checked `head - cursor < depth`), and only the
        // lane's host writes requests. A request that does not fit is not published.
        unsafe {
            self.memory
                .region
                .write_slot(slot, |w| wire::encode_request(w, &job.request))
        }
    }

    /// Forks one worker per lane, waits until they are ready, and sets up the hosts.
    pub(crate) fn start_run(
        self: &Arc<Self>,
        hosting: HostKind,
        server_stop: ServerRunStop,
    ) -> Result<(), LaneStartError> {
        {
            let mut phase = self.run.write();
            if !matches!(*phase, RunPhase::Idle) {
                return Err(LaneStartError::AlreadyServing);
            }
            *phase = RunPhase::Starting;
        }
        let n = self.workers();
        *self.health.lock() = RunHealth::new(n, self.host.on_failure);
        self.memory.region.reset();
        let mut spawned = Vec::with_capacity(n);
        for i in 0..n {
            let pid = self.spawn_worker(i, FORK_ANSWER_TIMEOUT).map_err(|fork| match fork {
                ForkError::ImportFailed { traceback, .. } => {
                    LaneStartError::TemplateFailed { lane: LaneIndex(i), failure: traceback }
                }
                ForkError::LaneFdsMissing { lane, got, .. } => LaneStartError::LaneFds { lane, got, want: LANE_FDS },
                source => LaneStartError::Fork { lane: LaneIndex(i), source },
            })?;
            self.pids[i].store(Some(pid));
            spawned.push(pid);
        }
        let deadline = Instant::now() + WORKER_START_TIMEOUT;
        for (i, pid) in spawned.into_iter().enumerate() {
            let h = self.memory.region.header(i);
            let mut gone_since = None;
            loop {
                let now = Instant::now();
                // Read in the order the worker and the template write: a worker writes its
                // start failure (or its ready pid) before it ends, and is reaped after. So
                // what ended it is looked at first, and a failure it left is never missed.
                if gone_since.is_none() && !process_exists(pid) {
                    gone_since = Some(now);
                }
                self.read_template_reports();
                let reaped = self.exits.status_of(i, pid);
                let look = WorkerStartSnapshot {
                    ready_pid: h.ready_pid.0.load(Acquire),
                    start_failure: self.memory.region.start_failure(i),
                    reaped,
                    gone_since,
                    now,
                };
                match starting_worker(i, pid, look, deadline) {
                    StartWait::Ready => break,
                    StartWait::Waiting => std::thread::sleep(Duration::from_millis(1)),
                    StartWait::Failed(e) => return Err(e),
                }
            }
        }
        let hosts = match hosting {
            HostKind::Tpc { threads } => n.min(threads.max(1)),
            HostKind::Dedicated => n,
        };
        let links: Vec<_> = (0..hosts)
            .map(|h| {
                let (inbox, rx) = mpsc::unbounded_channel();
                HostLink {
                    inbox,
                    receiver: Mutex::new(Some(rx)),
                    load: AtomicUsize::new(0),
                    // Lane `j` is hosted by host `j % hosts` (see `LaneHost::start`).
                    usable_lanes: AtomicUsize::new((0..n).filter(|j| j % hosts == h).count()),
                    failure: OnceLock::new(),
                    crash_wait: Mutex::new(None),
                }
            })
            .collect();
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
        let run = Arc::new(RunState {
            hosts: links,
            stop: CancellationToken::new(),
            server_stop,
        });
        *self.run.write() = RunPhase::Serving(Arc::clone(&run));
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
                                    Err(source) => {
                                        (Err(LaneStartError::HostStart { host: h, source }), None)
                                    }
                                };
                                // Unheard only when start_run gave up first (a later host
                                // thread could not be spawned); its caller stops the run.
                                if started.send(report).is_err() {
                                    tracing::debug!(target: "mipyre::server", host = h,
                                        "the process lanes' start stopped waiting before this host reported");
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

    /// Ends the run: stops the hosts (they answer what is still waiting with 503), kills the
    /// workers. The template stays for the next run. The dedicated host threads get until
    /// `deadline` to end: the server's stop deadline, the one its pool workers and TPC threads
    /// are joined by. Returns the names of the host threads still running then, each already
    /// logged and left detached.
    pub(crate) fn stop_run(&self, deadline: &ShutdownDeadline) -> Vec<String> {
        let ended = std::mem::replace(&mut *self.run.write(), RunPhase::Idle);
        match ended {
            RunPhase::Idle => return Vec::new(),
            RunPhase::Starting => {}
            RunPhase::Serving(run) => run.stop.cancel(),
        }
        let abandoned = join_hosts(std::mem::take(&mut *self.dedicated.lock()), deadline);
        for p in &self.pids {
            if let Some(pid) = p.swap(None) {
                kill_worker(pid);
            }
        }
        abandoned
    }

    /// Asks the template for a new worker on lane `i` and returns its pid, blocking until the
    /// template answers or `within` passed. A request that ran out of time is not waited for
    /// again: a later read of the socket finds its late answer, which carries another attempt,
    /// and ends the worker it names.
    fn spawn_worker(&self, i: usize, within: Duration) -> Result<Pid, ForkError> {
        let template = self.template.get().ok_or(ForkError::NoTemplate)?;
        template.fork(LaneIndex(i), within, &self.exits)
    }
}

impl Template {
    /// Sends the template its setup: `message`, then each lane's shared memory and wake-pipe
    /// write end, one frame per lane.
    pub(super) fn send_setup(&self, message: &[u8], memory: &LaneMemory) -> Result<(), TemplateSendError> {
        let link = self.link.lock();
        link.send(&TemplateCommand::Setup(message))?;
        for lane in 0..memory.workers() {
            link.send(&TemplateCommand::Lane {
                lane: LaneIndex(lane),
                fds: memory.lane_fds(lane),
            })?;
        }
        Ok(())
    }

    /// Asks for a worker on `lane` and returns its pid once the template's answer to this
    /// request comes, taking the other reports that come before it (`exits` gets the exit
    /// reports). Fails when `within` passes first or when the connection ends.
    fn fork(&self, lane: LaneIndex, within: Duration, exits: &ExitReports) -> Result<Pid, ForkError> {
        let template = self.pid;
        let mut link = self.link.lock();
        if let Some(end) = link.ended() {
            return Err(fork_error(template, end.clone()));
        }
        let attempt = link.next_attempt;
        link.next_attempt = attempt.next();
        if let Err(cut) = link.send(&TemplateCommand::Start { lane, attempt }) {
            // A template that ended because it cannot start workers said why first: that, not
            // the send it made fail, is what this request met.
            self.take_ready(&mut link, exits);
            return Err(match link.ended() {
                Some(said @ (LinkEnd::ImportFailed(_) | LinkEnd::LaneFdsMissing { .. })) => {
                    fork_error(template, said.clone())
                }
                _ => ForkError::Send(cut),
            });
        }
        let deadline = Instant::now() + within;
        loop {
            match link.next_report(exits.lanes(), Some(deadline)) {
                Ok(Some(TemplateReport::Forked { attempt: answered, result, .. })) if answered == attempt => {
                    // What else is already there goes now, so nobody reads it later.
                    self.take_ready(&mut link, exits);
                    drop(link);
                    let pid = result.map_err(|errno| ForkError::Fork { template, source: errno.into() })?;
                    return self.child(pid);
                }
                Ok(Some(report)) => self.take(&mut link, report, exits),
                Ok(None) => return Err(ForkError::TimedOut { template, waited: within }),
                Err(end) => return Err(fork_error(template, end)),
            }
        }
    }

    /// Takes the reports already waiting on the socket, unless someone else is reading it. A
    /// connection that ended stays ended: the next fork request fails with how.
    fn read_ready(&self, exits: &ExitReports) {
        if let Some(mut link) = self.link.try_lock() {
            self.take_ready(&mut link, exits);
        }
    }

    fn take_ready(&self, link: &mut TemplateLink, exits: &ExitReports) {
        while let Ok(Some(report)) = link.next_report(exits.lanes(), None) {
            self.take(link, report, exits);
        }
    }

    /// Takes one report that is not the answer a fork request waits for. A template that cannot
    /// start workers says why as its last frame: that is how the connection ended, and every
    /// fork request from then on fails with it.
    fn take(&self, link: &mut TemplateLink, report: TemplateReport, exits: &ExitReports) {
        match report {
            TemplateReport::Exited { lane, pid, status } => exits.record(lane, pid, status),
            TemplateReport::Forked { lane, attempt, result } => self.end_late(lane, attempt.0, result),
            TemplateReport::ImportFailed(traceback) => link.end(LinkEnd::ImportFailed(traceback)),
            TemplateReport::LaneFdsMissing { lane, got } => link.end(LinkEnd::LaneFdsMissing { lane, got }),
        }
    }

    /// Takes `pid`, which the template answered to a fork request, once the kernel says the
    /// template is its parent.
    fn child(&self, pid: Pid) -> Result<Pid, ForkError> {
        let template = self.pid;
        let parent = super::os::parent_of(pid.as_raw())
            .map_err(|source| ForkError::ParentUnread { template, pid, source })?;
        template_child(template, pid, parent)
    }

    /// Ends the worker a late answer names (a request whose spawn already gave up), when the
    /// template is its parent; a late fork failure only gets logged.
    fn end_late(&self, lane: LaneIndex, attempt: u32, result: Result<Pid, rustix::io::Errno>) {
        let template = self.pid;
        let worker = result
            .map_err(|errno| ForkError::Fork { template, source: errno.into() })
            .and_then(|pid| self.child(pid));
        match worker {
            Ok(pid) => {
                tracing::warn!(
                    target: "mipyre::server",
                    lane = lane.0,
                    attempt,
                    pid = pid.as_raw(),
                    template = template.as_raw(),
                    "the template answered a fork request after its deadline: its worker is killed"
                );
                kill_worker(pid);
            }
            Err(error) => tracing::warn!(
                target: "mipyre::server",
                lane = lane.0,
                attempt,
                template = template.as_raw(),
                %error,
                "the template answered a fork request after its deadline with no worker of its own"
            ),
        }
    }
}

/// What a fork request says when the connection to the template ended.
fn fork_error(template: Pid, end: LinkEnd) -> ForkError {
    match end {
        LinkEnd::Closed => ForkError::TemplateClosed { template },
        LinkEnd::RecvFailed(errno) => ForkError::Receive { template, source: errno.into() },
        end @ LinkEnd::BadFrame { .. } => ForkError::ProtocolViolated { template, end },
        LinkEnd::ImportFailed(traceback) => ForkError::ImportFailed { template, traceback },
        LinkEnd::LaneFdsMissing { lane, got } => ForkError::LaneFdsMissing { template, lane, got },
    }
}

/// Whether the hosts of a run started, from every host's report: all are heard before any
/// failure is returned (so no host is still starting when the caller stops the run), and the
/// first host that could not start fails the start with its own cause. The caller then stops
/// the run, which stops the hosts that did start.
fn hosts_started(
    reports: impl IntoIterator<Item = Result<(), LaneStartError>>,
) -> Result<(), LaneStartError> {
    let reports: Vec<_> = reports.into_iter().collect();
    reports.into_iter().collect()
}

/// A lane's starting worker as one look saw it, while the start waits for it to be ready.
struct WorkerStartSnapshot {
    ready_pid: u32,
    /// The traceback the worker left when it could not import the app.
    start_failure: Option<String>,
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
/// `deadline`). A worker that left a start failure fails the start with it; one that ended
/// without leaving one fails it with how it ended (its wait status, or, when the template has
/// not reported it within [`EXIT_STATUS_WAIT`] of the worker being gone, that).
fn starting_worker(lane: usize, pid: Pid, look: WorkerStartSnapshot, deadline: Instant) -> StartWait {
    match start_report(pid, look.ready_pid, look.start_failure) {
        StartReport::Ready => return StartWait::Ready,
        StartReport::Failed { failure } => {
            return StartWait::Failed(LaneStartError::WorkerStart { lane: LaneIndex(lane), pid, failure })
        }
        StartReport::Pending => {}
    }
    let exit = match (look.reaped, look.gone_since) {
        (Some(status), _) => Some(worker_exit(status)),
        (None, Some(since)) if look.now.duration_since(since) > EXIT_STATUS_WAIT => {
            Some(WorkerExit::Unreported(EXIT_STATUS_WAIT))
        }
        (None, _) => None,
    };
    if let Some(exit) = exit {
        return StartWait::Failed(LaneStartError::DiedBeforeReady { lane: LaneIndex(lane), pid, exit });
    }
    if look.now > deadline {
        return StartWait::Failed(LaneStartError::NotReady { lane: LaneIndex(lane), pid });
    }
    StartWait::Waiting
}

/// What a lane's header says about the start of its worker `pid`.
#[derive(Debug, PartialEq, Eq)]
enum StartReport {
    Ready,
    /// The worker could not import the app; `failure` is its traceback.
    Failed { failure: String },
    Pending,
}

/// Reads the start of worker `pid` from its lane's `ready_pid` and start failure. A worker
/// that wrote its pid as ready started, whatever failure note the header also holds: a note is
/// about a start that never became ready.
fn start_report(pid: Pid, ready_pid: u32, start_failure: Option<String>) -> StartReport {
    if ready_pid == pid.as_raw() as u32 {
        return StartReport::Ready;
    }
    match start_failure {
        Some(failure) => StartReport::Failed { failure },
        None => StartReport::Pending,
    }
}

/// Whether `pid` is still a process (a zombie not yet reaped is one): `kill(pid, 0)` finds
/// it, or fails for a reason other than its absence.
fn process_exists(pid: Pid) -> bool {
    rustix::process::test_kill_process(pid.into()) != Err(rustix::io::Errno::SRCH)
}

/// Sends a worker SIGKILL. A worker already gone (ESRCH) is what the kill wanted; any other
/// refusal leaves it running, which only the log can tell.
fn kill_worker(pid: Pid) {
    match rustix::process::kill_process(pid.into(), rustix::process::Signal::KILL) {
        Ok(()) | Err(rustix::io::Errno::SRCH) => {}
        Err(e) => tracing::error!(
            target: "mipyre::server",
            pid = pid.as_raw(),
            errno = e.raw_os_error(),
            error = %e,
            "SIGKILL to a lane worker was refused: it may still run"
        ),
    }
}

/// Takes `pid`, which the template answered to a fork request, as the lane's worker only when
/// the kernel says the template is its `parent`. A pid with no process (`None`) names nothing a
/// kill could hit: it is a worker that already ended and was reaped, and is taken, its death
/// then seen as any worker's is.
fn template_child(template: Pid, pid: Pid, parent: Option<i32>) -> Result<Pid, ForkError> {
    match parent {
        Some(parent) if parent != template.as_raw() => Err(ForkError::NotTemplateChild { template, pid, parent }),
        Some(_) | None => Ok(pid),
    }
}

fn worker_exit(status: i32) -> WorkerExit {
    if libc::WIFSIGNALED(status) {
        let signal = libc::WTERMSIG(status);
        WorkerExit::Signaled {
            signal,
            name: super::os::signal_name(signal),
        }
    } else if libc::WIFEXITED(status) {
        WorkerExit::Exited(libc::WEXITSTATUS(status))
    } else {
        WorkerExit::Other(status)
    }
}

/// How long a host waits for the template to report a dead worker's wait status.
const EXIT_STATUS_WAIT: Duration = Duration::from_millis(100);

/// How long a run's start waits for its workers to report ready (they import the app).
const WORKER_START_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a fork request waits for the template's answer, which comes once one `fork` or
/// `posix_spawn` returns. It only guards against a template that is alive but stuck (a dead one
/// closes its socket), while every other lane's fork request waits behind it. A fork's time
/// grows with the template's heap: the slowest of 500 forks of an 8 GiB heap took 279 ms on
/// Linux, about 35 ms per GiB, so 3 s covers a heap as large as a 64 GiB machine's memory
/// (`benchmarks/micro/fork_answer`).
const FORK_ANSWER_TIMEOUT: Duration = Duration::from_secs(3);

thread_local! {
    /// The host this thread is, while it serves.
    static HOST: RefCell<Option<Rc<LaneHost>>> = const { RefCell::new(None) };
}

/// One lane as its host keeps it.
struct LocalLane {
    index: usize,
    /// Holds the next sequence to publish, and is the source of the lane's shared `req_head`
    /// (`LaneHost::publish`). The two never diverge: this host is the only writer while a run
    /// serves the lane, and `LaneShm::reset` runs only between runs and leaves 0, where a new
    /// host starts.
    head: u32,
    /// Counts the answers read; the slots below it are free.
    cursor: u32,
    /// Holds who waits for sequence `k`, at index `k % depth`.
    waiting: Vec<Option<LaneReplySender>>,
    queued: VecDeque<LaneJob>,
    worker: LaneWorker,
}

/// Where a lane's worker process is.
enum LaneWorker {
    /// A worker runs the lane's requests.
    Running {
        pid: Pid,
        /// A replacement not yet reported ready: when its predecessor's death was seen (the
        /// respawn latency), and whether its start fails is still being watched.
        starting_since: Option<Instant>,
        /// What the host last saw at the head of the lane while this worker runs it; `None`
        /// until the host first looks at a request in flight. It is kept while the lane is
        /// idle, since a request once answered never comes back to the head, and it goes with
        /// the worker: a replacement's time on a request starts when the host sees it running.
        head: Option<HeadOfLine>,
    },
    /// Killed for running a handler past the timeout (once): its death answers 504.
    /// `head_since_ns` is when the host first saw that handler's request at the head.
    Overrun {
        pid: Pid,
        starting_since: Option<Instant>,
        head_since_ns: u64,
    },
    /// The worker died and a replacement is being forked; `since` is when the death was seen.
    Forking {
        task: tokio::task::JoinHandle<Result<Pid, ForkError>>,
        since: Instant,
    },
    /// The worker died while the server was stopping: no replacement comes.
    Gone,
    /// The lane could not get a worker and takes no requests (`break_lane`). It stays broken for
    /// the run, and the run's `broken_lanes` is counted from this state.
    Broken { why: Arc<LaneLossError> },
    /// The lane's workers crash-loop: it waits before forking the next one, as long as its crash
    /// streak says, and refuses its requests with 503 meanwhile.
    CrashWait,
    /// Killed for writing a lane counter the protocol cannot produce. Its requests were answered
    /// 502 when that was seen, and none of them runs again; until it is dead the lane takes none.
    /// It is declared beside the other states that take no requests, so that test is one range.
    Faulted { pid: Pid, fault: PeerCounterError },
}

impl LaneWorker {
    #[inline]
    fn takes_requests(&self) -> bool {
        !matches!(
            self,
            LaneWorker::Broken { .. } | LaneWorker::CrashWait | LaneWorker::Faulted { .. }
        )
    }
}

impl LocalLane {
    fn load(&self) -> usize {
        self.head.wrapping_sub(self.cursor) as usize + self.queued.len()
    }

    fn broken_why(&self) -> Option<&LaneLossError> {
        match &self.worker {
            LaneWorker::Broken { why } => Some(why),
            LaneWorker::CrashWait
            | LaneWorker::Running { .. }
            | LaneWorker::Overrun { .. }
            | LaneWorker::Faulted { .. }
            | LaneWorker::Forking { .. }
            | LaneWorker::Gone => None,
        }
    }

    fn waiting(&self) -> bool {
        matches!(self.worker, LaneWorker::CrashWait)
    }

    /// Returns the host's own place on the lane's ring.
    #[inline]
    fn window(&self) -> Window {
        Window::new(Seq::new(self.cursor), Seq::new(self.head))
    }

    /// Skips past a dead worker as `split` says it left the lane, once every answer it published
    /// is read. The request it died running leaves the lane, and its reply comes out to be
    /// answered with the death; the requests it never took stay in their slots and queue for the
    /// next worker.
    ///
    /// Returns `Some` when it died running a request, holding who still waits for its answer if
    /// anyone does, and `None` when it had answered every request it took.
    fn pass_dead_run(&mut self, split: &ExitSplit, depth: u32) -> Option<Option<LaneReplySender>> {
        let ran = split.running()?;
        let reply = self.waiting[(ran.get() % depth) as usize].take();
        self.cursor = ran.after(1).get();
        Some(reply)
    }

    /// Takes who waits for each request in the lane's slots, leaving the slots held.
    fn take_replies(&mut self, depth: u32) -> Vec<LaneReplySender> {
        (0..self.head.wrapping_sub(self.cursor))
            .filter_map(|k| self.waiting[(self.cursor.wrapping_add(k) % depth) as usize].take())
            .collect()
    }

    /// Takes who waits for each request in the lane's slots and frees the slots, so no worker
    /// runs those requests.
    fn drain_in_flight(&mut self, depth: u32) -> Vec<LaneReplySender> {
        let replies = self.take_replies(depth);
        self.cursor = self.head;
        replies
    }
}

/// The bytes a request takes in a slot, over-counted: its body, URI, headers and path
/// parameters, with a margin for the fields' framing.
fn approx_message_len(head: &crate::exchange::RequestHead, body: &[u8]) -> usize {
    body.len()
        + head.parts.uri.to_string().len()
        + head
            .parts
            .headers
            .iter()
            .map(|(k, v)| k.as_str().len() + v.len() + 8)
            .sum::<usize>()
        + head
            .params
            .iter()
            .map(|(k, v)| k.len() + v.len() + 8)
            .sum::<usize>()
        + 256
}

fn fits(len: usize, room: usize) -> Result<(), TooLarge> {
    if len > room {
        return Err(TooLarge { needed: len, room });
    }
    Ok(())
}

/// Why a worker the host watches dies, as far as the host caused it.
#[derive(Clone, Copy)]
enum DeathCause {
    Crashed,
    Overran { ran: Duration },
    Faulted(PeerCounterError),
}

/// Kills the worker that a fork still running for broken lane `i` makes, once the template
/// answers: a broken lane runs no worker, and nothing else learns that worker's pid.
fn kill_when_forked(i: usize, task: tokio::task::JoinHandle<Result<Pid, ForkError>>) {
    tokio::spawn(async move {
        match task.await {
            Ok(Ok(pid)) => kill_worker(pid),
            // The fork failed, so no worker was made; the lane is broken already.
            Ok(Err(_)) => {}
            Err(e) => tracing::error!(
                target: "mipyre::server", lane = i, error = %e,
                "process lane {i} broke while forking a worker, and the fork task did not finish \
                 ({e}); a worker it made is left running"
            ),
        }
    });
}

fn peer_corrupted(i: usize, pid: Pid, fault: PeerCounterError) -> UnansweredError {
    UnansweredError::PeerCorrupted {
        pid: pid.as_raw(),
        lane: LaneIndex(i),
        fault,
    }
}

/// A lane of the host in a crash streak ([`CrashStreak`]), kept by the host beside its lanes
/// while the streak lasts: a lane that serves carries nothing, so the lanes the host scans per
/// request are as they were.
struct LaneCrashStreak {
    lane: usize,
    run: CrashStreak,
    /// The lane's `cursor` as its last death left it: an answer read since moves `cursor` past
    /// it, which ends the run. Compared at the next death (and by the crash-loop watch), not
    /// counted per answer, so reading answers costs nothing more.
    read_at_death: u32,
}

/// Of the runs in `runs` whose lane (in `owned`) waits out a crash loop, the one that forks
/// first.
fn soonest_wait(
    owned: &[LocalLane],
    runs: &[LaneCrashStreak],
    backoff: &CrashBackoff,
) -> Option<CrashLoopWait> {
    runs.iter()
        .filter(|r| owned.iter().any(|l| l.index == r.lane && l.waiting()))
        .map(|r| r.run.wait(LaneIndex(r.lane), backoff))
        .reduce(CrashLoopWait::sooner)
}

/// A thread that hosts lanes. `!Send`: it lives on its thread, in its thread-local.
pub(crate) struct LaneHost {
    lanes: Arc<ServerLanes>,
    run: Arc<RunState>,
    index: usize,
    owned: RefCell<Vec<LocalLane>>,
    /// Holds only the lanes in a crash run, so it is empty while they all serve.
    crash_runs: RefCell<Vec<LaneCrashStreak>>,
    /// Whether the crash-loop watch ([`LaneHost::watch_crash_loops`]) runs: from a lane's
    /// first wait until none of the host's lanes loops.
    crash_watch: Cell<bool>,
    /// Wakes the poller: a request was put in a lane, or a fork finished.
    notify: Arc<Notify>,
    stop: CancellationToken,
    done: Notify,
}

/// A raw fd for `AsyncFd` (the fd is owned elsewhere and outlives it).
struct Fd(RawFd);

impl AsRawFd for Fd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

impl LaneHost {
    /// Starts hosting on this thread (a current-thread runtime with a `LocalSet`) if the
    /// current run gives it lanes: host `index` of the run. The poller runs as a local task
    /// until the run stops or [`LaneHost::shutdown`]. What the poller needs (the wake pipes in
    /// this thread's reactor, the worker-exit watch) is set up here: a host that cannot watch
    /// its lanes is this start's error, not a host that answers every request 503.
    pub(crate) fn start(
        lanes: &Arc<ServerLanes>,
        index: usize,
    ) -> std::io::Result<Option<Rc<LaneHost>>> {
        let run = match &*lanes.run.read() {
            RunPhase::Serving(run) => Arc::clone(run),
            RunPhase::Idle | RunPhase::Starting => return Ok(None),
        };
        let Some(inbox) = run.hosts.get(index).and_then(|h| h.receiver.lock().take()) else {
            return Ok(None);
        };
        let depth = lanes.memory.region.geometry().depth() as usize;
        let owned = (0..lanes.workers())
            .filter(|j| j % run.hosts.len() == index)
            .map(|j| LocalLane {
                index: j,
                head: 0,
                cursor: 0,
                waiting: (0..depth).map(|_| None).collect(),
                queued: VecDeque::new(),
                worker: match lanes.pids[j].load() {
                    None => LaneWorker::Gone,
                    Some(pid) => LaneWorker::Running {
                        pid,
                        starting_since: None,
                        head: None,
                    },
                },
            })
            .collect();
        let host = Rc::new(LaneHost {
            lanes: Arc::clone(lanes),
            run: Arc::clone(&run),
            index,
            owned: RefCell::new(owned),
            crash_runs: RefCell::new(Vec::new()),
            crash_watch: Cell::new(false),
            notify: Arc::new(Notify::new()),
            stop: run.stop.child_token(),
            done: Notify::new(),
        });
        let (pipes, watch) = host.watch()?;
        HOST.with(|h| *h.borrow_mut() = Some(Rc::clone(&host)));
        let poller = Rc::clone(&host);
        // `poller_failure` turns how the poller ended, a panic included, into the host's failure.
        tokio::task::spawn_local(async move {
            if let Some(failure) = poller_failure(Rc::clone(&poller).run(inbox, pipes, watch)).await
            {
                poller.fail(failure);
            }
            poller.close();
            poller.done.notify_one();
        });
        Ok(Some(host))
    }

    /// Stops this host and waits for its poller to answer what is left.
    pub(crate) async fn shutdown(self: Rc<Self>) {
        self.stop.cancel();
        self.done.notified().await;
        HOST.with(|h| *h.borrow_mut() = None);
    }

    async fn stopped(self: Rc<Self>) {
        self.done.notified().await;
        HOST.with(|h| *h.borrow_mut() = None);
    }

    /// Puts a request of this thread into one of this host's lanes. Never waits. A refusal
    /// hands the request back with why, so the caller can send it elsewhere or answer it.
    #[expect(
        clippy::result_large_err,
        reason = "a refused job comes back by value, as it went in, to be forwarded or answered"
    )]
    fn submit(&self, job: LaneJob) -> Result<(), (LaneRequestError, LaneJob)> {
        let mut owned = self.owned.borrow_mut();
        let loads = owned.iter().map(|l| l.worker.takes_requests().then(|| l.load()));
        let picked = health::pick_lane(loads, self.lanes.host.queue_per_worker, || {
            // A lane that only waits out a crash loop comes back: the request is told when.
            let backoff = &self.lanes.host.crash_backoff;
            let wait = soonest_wait(&owned, &self.crash_runs.borrow(), backoff)
                .map(|wait| (wait, shm::monotonic_ns()));
            // Else the refusal is said to the request: the first broken lane's cause.
            let cause = || {
                let cause = match owned.iter().find_map(LocalLane::broken_why) {
                    Some(why) => why.to_string(),
                    None => format!("process lane host {} has no lane", self.index),
                };
                Arc::from(cause)
            };
            health::no_usable_lane(self.run.stop.is_cancelled(), wait, cause)
        });
        let k = match picked {
            Ok(k) => k,
            Err(refusal) => return Err((LaneRequestError::Rejected(refusal), job)),
        };
        let lane = &mut owned[k];
        if lane.queued.is_empty() && self.room(lane) {
            self.publish(lane, job)
                .map_err(|(e, job)| (LaneRequestError::TooLarge(e), job))?;
        } else {
            lane.queued.push_back(job);
        }
        drop(owned);
        // The busy path: a thread that is submitting is awake; it reads its lanes' answers
        // on the way (atomic loads when there are none).
        let read = self.collect();
        if read > 0 {
            self.counts().read_by_submit.add(read);
        }
        self.publish_load();
        Ok(())
    }

    /// Whether every lane of this host is running a handler that started at least
    /// [`health::SPREAD_AFTER_NS`] ago.
    fn running_long(&self) -> bool {
        let now = shm::monotonic_ns();
        self.owned.borrow().iter().all(|lane| {
            let h = self.lanes.memory.region.header(lane.index);
            // Running = taken but not answered by the worker (an answer this host has not
            // read yet is not a running handler).
            let running = h.taken.0.load(Acquire) != h.resp_head.0.load(Acquire);
            health::ran_at_least(
                running,
                h.started_ns.0.load(Acquire),
                now,
                health::SPREAD_AFTER_NS,
            )
        })
    }

    fn publish_load(&self) {
        let load = self.owned.borrow().iter().map(LocalLane::load).sum();
        self.run.hosts[self.index].load.store(load, Relaxed);
    }

    fn room(&self, lane: &LocalLane) -> bool {
        lane.window().has_room(self.lanes.memory.region.geometry().ring_depth())
    }

    #[expect(
        clippy::result_large_err,
        reason = "a refused job comes back by value, as it went in, to be forwarded or answered"
    )]
    fn publish(&self, lane: &mut LocalLane, job: LaneJob) -> Result<(), (TooLarge, LaneJob)> {
        let seq = lane.head;
        if let Err(e) = self.lanes.write_request(lane.index, seq, &job) {
            return Err((e, job));
        }
        let depth = self.lanes.memory.region.geometry().depth();
        lane.waiting[(seq % depth) as usize] = Some(job.reply);
        lane.head = seq.wrapping_add(1);
        let h = self.lanes.memory.region.header(lane.index);
        if shm::publish(&h.req_head.0, &h.worker_sleeping.0, lane.head) {
            self.counts().waits.wakes.add(1);
        }
        Ok(())
    }

    /// Moves queued requests into the lane while it has free slots, answering one that does not
    /// fit with 413.
    fn refill(&self, lane: &mut LocalLane) {
        while self.room(lane) {
            let Some(job) = lane.queued.pop_front() else {
                break;
            };
            if let Err((e, job)) = self.publish(lane, job) {
                answer_if_awaited(job.reply, Err(LaneRequestError::TooLarge(e)));
            }
        }
    }

    /// Reads every published answer of this host's lanes and completes its request on this
    /// thread, returning how many it read; with nothing published it costs only atomic loads.
    fn collect(&self) -> u64 {
        self.collect_with(Acquire)
    }

    /// Does what [`LaneHost::collect`] does, reading each lane's `resp_head` with `head_order`.
    /// Inlined so the ordering is a constant at each call and costs no branch.
    #[inline(always)]
    fn collect_with(&self, head_order: Ordering) -> u64 {
        let mut read = 0u64;
        let depth = self.lanes.memory.region.geometry().depth();
        let mut done = Vec::new();
        {
            let mut owned = self.owned.borrow_mut();
            for lane in owned.iter_mut() {
                let h = self.lanes.memory.region.header(lane.index);
                let published = h.resp_head.0.load(head_order);
                if published == lane.cursor {
                    continue;
                }
                if let Err(fault) = ring::count_ready_answers(&lane.window(), Seq::new(published)) {
                    self.peer_faulted(lane, fault, &mut done);
                    continue;
                }
                while lane.cursor != published {
                    let slot = self.lanes.memory.region.resp_slot(lane.index, lane.cursor);
                    // SAFETY: the worker published `cursor`: the slot holds its answer, and the
                    // worker won't touch it until the request slot is reused.
                    let outcome = unsafe { self.lanes.memory.region.read_slot(slot) }
                        .and_then(wire::decode_answer)
                        .map_err(LaneRequestError::Malformed)
                        .and_then(|answer| answer.map_err(LaneRequestError::WorkerFailed));
                    if let Some(reply) = lane.waiting[(lane.cursor % depth) as usize].take() {
                        done.push((reply, outcome));
                    }
                    lane.cursor = lane.cursor.wrapping_add(1);
                    read += 1;
                }
                self.refill(lane);
            }
        }
        if read > 0 {
            self.publish_load();
        }
        for (reply, outcome) in done {
            answer_if_awaited(reply, outcome);
        }
        read
    }

    fn in_flight(&self) -> bool {
        self.owned.borrow().iter().any(|l| l.head != l.cursor)
    }

    /// Kills the worker of any lane whose running handler is past the timeout.
    fn check_timeouts(&self) {
        let timeout = self.lanes.host.timeout.as_nanos() as u64;
        let now = shm::monotonic_ns();
        for lane in self.owned.borrow_mut().iter_mut() {
            let LaneWorker::Running {
                pid,
                starting_since,
                ref mut head,
            } = lane.worker
            else {
                continue;
            };
            if lane.cursor == lane.head {
                continue;
            }
            let seen = HeadOfLine::look(head, lane.cursor, now);
            if !seen.overran(now, timeout) {
                continue;
            }
            lane.worker = LaneWorker::Overrun {
                pid,
                starting_since,
                head_since_ns: seen.since_ns,
            };
            self.lanes.stats.timeouts_killed.fetch_add(1, Relaxed);
            kill_worker(pid);
        }
    }

    /// Handles `lane`'s worker writing an answer counter the protocol cannot produce: the worker
    /// is killed and every request in the lane's slots is answered 502 (into `done`), none run
    /// again, and the death that follows frees the slots. While a replacement is being forked
    /// nothing is done yet: the replacement is killed once it runs if the counter still does not
    /// fit. With no worker of the lane's own, the host owns the counters until the next fork and
    /// puts them back. The other lanes are not touched. It is kept out of line, so the reading
    /// loop pays only for the comparison.
    #[cold]
    #[inline(never)]
    fn peer_faulted(
        &self,
        lane: &mut LocalLane,
        fault: PeerCounterError,
        done: &mut Vec<(LaneReplySender, Result<HandlerResponse, LaneRequestError>)>,
    ) {
        let i = lane.index;
        match lane.worker {
            LaneWorker::Running { pid, .. } | LaneWorker::Overrun { pid, .. } => {
                lane.worker = LaneWorker::Faulted { pid, fault };
                self.say_peer_fault(i, pid, fault);
                kill_worker(pid);
                let depth = self.lanes.memory.region.geometry().depth();
                let refusal = || Err(LaneRequestError::Rejected(peer_corrupted(i, pid, fault)));
                done.extend(lane.take_replies(depth).into_iter().map(|r| (r, refusal())));
            }
            LaneWorker::Faulted { .. } | LaneWorker::Broken { .. } | LaneWorker::Forking { .. } => {}
            LaneWorker::Gone | LaneWorker::CrashWait => {
                self.lanes.stats.peer_faults.fetch_add(1, Relaxed);
                tracing::error!(
                    target: "mipyre::server", lane = i, error = %fault, detail = ?fault,
                    "process lane {i}: {fault}, while it has no worker; the host puts its counters back"
                );
                let h = self.lanes.memory.region.header(i);
                h.taken.0.store(lane.cursor, SeqCst);
                h.resp_head.0.store(lane.cursor, SeqCst);
            }
        }
    }

    /// Logs that lane `i`'s worker `pid` wrote a counter the protocol cannot produce, with the
    /// values the host held and the one it read.
    fn say_peer_fault(&self, i: usize, pid: Pid, fault: PeerCounterError) {
        self.lanes.stats.peer_faults.fetch_add(1, Relaxed);
        tracing::error!(
            target: "mipyre::server", lane = i, pid = pid.as_raw(), error = %fault, detail = ?fault,
            "process lane {i}: {fault}; the worker is killed and its requests in flight are \
             answered 502, none run again"
        );
    }

    /// Runs `f` on this host's lane `i` (the lane by its index, its identity, not by its place
    /// in `owned`); `None` when this host does not own lane `i`.
    fn with_lane<R>(&self, i: usize, f: impl FnOnce(&mut LocalLane) -> R) -> Option<R> {
        self.owned
            .borrow_mut()
            .iter_mut()
            .find(|l| l.index == i)
            .map(f)
    }

    /// Returns why `pid`, the worker lane `lane` runs now, dies, as far as the host caused it;
    /// `None` for a worker the lane no longer runs (an earlier worker's late event).
    fn dying_worker(lane: &LocalLane, pid: Pid) -> Option<DeathCause> {
        match lane.worker {
            LaneWorker::Running { pid: running, .. } if running == pid => Some(DeathCause::Crashed),
            LaneWorker::Overrun { pid: running, head_since_ns, .. } if running == pid => {
                let ran = Duration::from_nanos(shm::monotonic_ns().saturating_sub(head_since_ns));
                Some(DeathCause::Overran { ran })
            }
            LaneWorker::Faulted { pid: running, fault } if running == pid => {
                Some(DeathCause::Faulted(fault))
            }
            _ => None,
        }
    }

    /// Handles the death of lane `i`'s worker `pid`: reads what it answered, answers what it was
    /// running, and starts forking a replacement. A lane this host does not own, or a worker the
    /// lane no longer runs, is an earlier event's echo and changes nothing.
    async fn worker_died(self: &Rc<Self>, i: usize, pid: Pid) {
        if self
            .with_lane(i, |lane| Self::dying_worker(lane, pid))
            .flatten()
            .is_none()
        {
            return;
        }
        self.lanes.stats.deaths.fetch_add(1, Relaxed);
        self.collect_in_poller();
        let h = self.lanes.memory.region.header(i);
        let taken = h.taken.0.load(Acquire);
        let status = self.exit_status(i, pid).await;
        // Looked up again after the wait: the lane is taken by its index, and whether `pid` is
        // still its worker is decided on the state this step changes, not on the one seen
        // before the wait.
        let died = self.with_lane(i, |lane| {
            let cause = Self::dying_worker(lane, pid)?;
            // Read before the dead worker's slot is skipped: an answer since the last death
            // ends the crash streak, a skip is not one.
            let cursor_at_death = lane.cursor;
            let depth = self.lanes.memory.region.geometry().depth();
            // Every answer the worker published is read by now, so `published` is the cursor.
            let split = || ring::exit_split(&lane.window(), Seq::new(lane.cursor), Seq::new(taken));
            let left = match cause {
                DeathCause::Crashed => split().map(|s| (s, None)),
                DeathCause::Overran { ran } => split().map(|s| (s, Some(ran))),
                DeathCause::Faulted(fault) => Err(fault),
            };
            let mut answered = Vec::new();
            match left {
                Ok((split, overran)) => {
                    if let Some(reply) = lane.pass_dead_run(&split, depth) {
                        let (pid, exit) = (pid.as_raw(), status.clone());
                        let refusal = match overran {
                            Some(ran) => UnansweredError::LaneTimedOut {
                                pid,
                                lane: LaneIndex(i),
                                ran,
                                timeout: self.lanes.host.timeout,
                                exit,
                            },
                            None => {
                                let started = h.started_ns.0.load(Acquire);
                                UnansweredError::WorkerDied {
                                    pid,
                                    lane: LaneIndex(i),
                                    ran: Duration::from_nanos(
                                        shm::monotonic_ns().saturating_sub(started),
                                    ),
                                    exit,
                                }
                            }
                        };
                        answered.extend(reply.map(|r| (r, refusal)));
                        // The dead worker publishes no more: skip its slot.
                        h.resp_head.0.store(lane.cursor, SeqCst);
                    }
                }
                Err(fault) => {
                    if !matches!(cause, DeathCause::Faulted(_)) {
                        self.say_peer_fault(i, pid, fault);
                    }
                    // Whether it ran them is unknown: they are answered, never run again.
                    answered.extend(
                        lane.drain_in_flight(depth)
                            .into_iter()
                            .map(|r| (r, peer_corrupted(i, pid, fault))),
                    );
                    // The next worker starts at `taken`, after the slots just freed.
                    h.taken.0.store(lane.head, SeqCst);
                    h.resp_head.0.store(lane.head, SeqCst);
                }
            }
            h.ready_pid.0.store(NO_WORKER as u32, SeqCst);
            // A start failure in the header was left by this worker (or written over it): the
            // next worker's start is a new attempt and must not read it as its own.
            h.start_failed.0.store(0, SeqCst);
            lane.worker = LaneWorker::Gone;
            self.lanes.pids[i].store(None);
            self.refill(lane);
            Some((cursor_at_death, answered))
        });
        let Some((cursor_at_death, answered)) = died.flatten() else {
            return;
        };
        if !self.run.stop.is_cancelled() {
            self.crashed(i, cursor_at_death, &status);
        }
        self.publish_load();
        for (reply, refusal) in answered {
            answer_if_awaited(reply, Err(LaneRequestError::Rejected(refusal)));
        }
    }

    /// Records that lane `i`'s worker died with `exit`, its answers read up to
    /// `cursor_at_death`. The lane's crash streak grows, or a new one starts (logging the
    /// recovery) if it answered since the last death; the crash-loop alert is logged when due;
    /// and the next worker is forked now, or after the lane waits out the crash loop with its
    /// requests refused.
    fn crashed(self: &Rc<Self>, i: usize, cursor_at_death: u32, exit: &WorkerExit) {
        let backoff = self.lanes.host.crash_backoff;
        let now = shm::monotonic_ns();
        let (mut streak, recovered) = {
            let mut runs = self.crash_runs.borrow_mut();
            match runs.iter().position(|r| r.lane == i) {
                Some(k) if runs[k].read_at_death == cursor_at_death => {
                    let mut streak = runs.swap_remove(k).run;
                    streak.died(now, exit.clone());
                    (streak, None)
                }
                // A streak whose lane answered since its last death is over.
                found => {
                    let recovered = found.and_then(|k| runs.swap_remove(k).run.answered());
                    (CrashStreak::first(now, exit.clone()), recovered)
                }
            }
        };
        if let Some(crashes) = recovered {
            self.say_recovered(i, crashes);
        }
        let alert = streak.alert_due(&backoff, now);
        let respawned = match streak.respawn(&backoff, now) {
            RespawnStep::Fork => self.with_lane(i, |lane| {
                self.fork_replacement(lane);
                (lane.cursor, None)
            }),
            RespawnStep::Delay(_) => {
                let wait = streak.wait(LaneIndex(i), &backoff);
                self.with_lane(i, |lane| {
                    let refused = self.close_for_crash_loop(lane, &wait, now);
                    (lane.cursor, Some(refused))
                })
            }
        };
        let Some((read_at_death, refused)) = respawned else {
            return;
        };
        self.crash_runs.borrow_mut().push(LaneCrashStreak {
            lane: i,
            run: streak,
            read_at_death,
        });
        if refused.is_some() {
            self.run.hosts[self.index]
                .usable_lanes
                .fetch_sub(1, Relaxed);
            self.publish_crash_wait();
            if !self.crash_watch.replace(true) {
                tokio::task::spawn_local(Rc::clone(self).watch_crash_loops());
            }
        }
        if alert {
            self.say_crash_loop(i);
        }
        for (reply, refusal) in refused.into_iter().flatten() {
            answer_if_awaited(reply, Err(LaneRequestError::Rejected(refusal)));
        }
    }

    /// Asks the template for lane `lane`'s replacement worker, on the blocking pool: the lane
    /// is [`LaneWorker::Forking`] until [`LaneHost::finish_respawns`] sees the answer.
    fn fork_replacement(&self, lane: &mut LocalLane) {
        let lanes = Arc::clone(&self.lanes);
        let notify = Arc::clone(&self.notify);
        let i = lane.index;
        let task = tokio::task::spawn_blocking(move || {
            let r = lanes.spawn_worker(i, FORK_ANSWER_TIMEOUT);
            notify.notify_one();
            r
        });
        lane.worker = LaneWorker::Forking {
            task,
            since: Instant::now(),
        };
    }

    /// Lane `lane`, whose worker is dead, waits out its crash loop: it takes no requests, and
    /// those already in it (none taken: the worker is dead) are taken back out of its ring and
    /// handed back with their refusal, so the next worker never runs them. `now` is in
    /// monotonic nanoseconds.
    fn close_for_crash_loop(
        &self,
        lane: &mut LocalLane,
        wait: &CrashLoopWait,
        now: u64,
    ) -> Vec<(LaneReplySender, UnansweredError)> {
        lane.worker = LaneWorker::CrashWait;
        let depth = self.lanes.memory.region.geometry().depth();
        let mut replies = lane.drain_in_flight(depth);
        replies.extend(lane.queued.drain(..).map(|j| j.reply));
        // No worker runs the lane until the next fork, which starts at `taken`: the published
        // requests are marked taken and answered, as a dead worker's slot is skipped.
        let h = self.lanes.memory.region.header(lane.index);
        h.taken.0.store(lane.head, SeqCst);
        h.resp_head.0.store(lane.head, SeqCst);
        replies
            .into_iter()
            .map(|reply| (reply, wait.clone().refusal(now)))
            .collect()
    }

    /// Publishes which of this host's lanes waiting out a crash loop forks first, for a
    /// request no host takes to say when to retry.
    fn publish_crash_wait(&self) {
        let wait = soonest_wait(
            &self.owned.borrow(),
            &self.crash_runs.borrow(),
            &self.lanes.host.crash_backoff,
        );
        *self.run.hosts[self.index].crash_wait.lock() = wait;
    }

    /// Logs lane `i`'s crash-loop alert and lists the lane degraded with the same facts. It is
    /// logged when the lane starts looping and again every `alert_every` while it does.
    fn say_crash_loop(&self, i: usize) {
        let backoff = self.lanes.host.crash_backoff;
        let waiting = self.with_lane(i, |lane| lane.waiting()).unwrap_or(false);
        let facts = self
            .crash_runs
            .borrow()
            .iter()
            .find(|r| r.lane == i)
            .map(|r| (r.run.crashes(), r.run.last_exit().to_string()));
        let Some((crashes, exit)) = facts else {
            return;
        };
        let wait = backoff.wait(crashes);
        let reason = format!(
            "crash-looping: its last {crashes} workers died without answering a request, the \
             last one {exit}; each fork waits (now {wait:?}), its requests are answered 503 \
             while it waits"
        );
        tracing::error!(
            target: "mipyre::server", lane = i, crashes, exit = %exit, wait = ?wait, waiting,
            "process lane {i} {reason}"
        );
        crate::telemetry::set_degraded(crate::telemetry::Component::ProcessLane(i), reason);
    }

    /// Logs that lane `i` answered a request after its crash-loop alert, and clears its degraded
    /// listing.
    fn say_recovered(&self, i: usize, crashes: u32) {
        tracing::warn!(
            target: "mipyre::server", lane = i, crashes,
            "process lane {i} recovered: it answered a request after {crashes} consecutive \
             crashes; its forks no longer wait"
        );
        crate::telemetry::clear_degraded(&crate::telemetry::Component::ProcessLane(i));
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
        let backoff = self.lanes.host.crash_backoff;
        let now = shm::monotonic_ns();
        let (mut recovered, mut alerts, mut forked) = (Vec::new(), Vec::new(), 0usize);
        let mut next_ns: Option<u64> = None;
        {
            let mut owned = self.owned.borrow_mut();
            let mut runs = self.crash_runs.borrow_mut();
            let mut k = 0;
            while k < runs.len() {
                let entry = &mut runs[k];
                let Some(lane) = owned.iter_mut().find(|l| l.index == entry.lane) else {
                    k += 1;
                    continue;
                };
                if lane.cursor != entry.read_at_death {
                    let ended = runs.swap_remove(k);
                    if let Some(crashes) = ended.run.answered() {
                        recovered.push((ended.lane, crashes));
                    }
                    continue;
                }
                if lane.waiting() && entry.run.respawn(&backoff, now) == RespawnStep::Fork {
                    self.fork_replacement(lane);
                    forked += 1;
                }
                if entry.run.alert_due(&backoff, now) {
                    alerts.push(entry.lane);
                }
                if let Some(alert_at) = entry.run.alert_at_ns(&backoff) {
                    // A looping lane is looked at again when it forks (waiting), else after the
                    // first wait (has it answered yet?), and at its next alert at the latest.
                    let look_at = if lane.waiting() {
                        entry.run.wait(LaneIndex(entry.lane), &backoff).fork_at_ns
                    } else {
                        now.saturating_add(backoff.first.as_nanos().min(u128::from(u64::MAX)) as u64)
                    };
                    let at = look_at.min(alert_at);
                    next_ns = Some(next_ns.map_or(at, |n| n.min(at)));
                }
                k += 1;
            }
        }
        if forked > 0 {
            self.run.hosts[self.index]
                .usable_lanes
                .fetch_add(forked, Relaxed);
            self.publish_crash_wait();
        }
        for (i, crashes) in recovered {
            self.say_recovered(i, crashes);
        }
        for i in alerts {
            self.say_crash_loop(i);
        }
        next_ns.map(|at| Duration::from_nanos(at.saturating_sub(now)))
    }

    /// Returns how the dead worker ended, as the template reaped it, waiting up to
    /// [`EXIT_STATUS_WAIT`] for the template's report.
    async fn exit_status(&self, i: usize, pid: Pid) -> WorkerExit {
        let deadline = Instant::now() + EXIT_STATUS_WAIT;
        loop {
            self.lanes.read_template_reports();
            if let Some(status) = self.lanes.exits.status_of(i, pid) {
                return worker_exit(status);
            }
            if Instant::now() > deadline {
                return WorkerExit::Unreported(EXIT_STATUS_WAIT);
            }
            tokio::time::sleep(Duration::from_micros(500)).await;
        }
    }

    /// Settles finished forks: watches each new worker, breaks the lane of a failed fork or of a
    /// replacement that cannot import the app, and records the respawn latency once a
    /// replacement is ready.
    fn finish_respawns(&self, watch: &ProcessMonitor) {
        let mut broken = Vec::new();
        for lane in self.owned.borrow_mut().iter_mut() {
            let LaneWorker::Forking { task, since } = &mut lane.worker else {
                continue;
            };
            // A fork still running is looked at again next round; its task keeps the answer.
            let Some(joined) = finished_now(task) else {
                continue;
            };
            let since = *since;
            lane.worker = LaneWorker::Gone;
            match joined.map_err(ForkError::Task).and_then(|forked| forked) {
                Ok(pid) => {
                    lane.worker = LaneWorker::Running {
                        pid,
                        starting_since: Some(since),
                        head: None,
                    };
                    self.lanes.pids[lane.index].store(Some(pid));
                    self.lanes.stats.respawns.fetch_add(1, Relaxed);
                    let worker = LaneProcess {
                        lane: lane.index,
                        pid,
                    };
                    if let Err(e) = watch.add(worker) {
                        if e.raw_os_error() == Some(libc::ESRCH) {
                            watch.dead_now(worker);
                        } else {
                            broken.push((lane.index, LaneLossError::ExitNotWatched { pid, source: e }));
                        }
                    }
                }
                Err(e) => broken.push((lane.index, LaneLossError::NotRespawned(e))),
            }
        }
        for (i, why) in broken {
            self.break_lane(i, why);
        }
        let region = &self.lanes.memory.region;
        let mut failed = Vec::new();
        for lane in self.owned.borrow_mut().iter_mut() {
            let (LaneWorker::Running {
                pid, starting_since, ..
            }
            | LaneWorker::Overrun {
                pid, starting_since, ..
            }) = &mut lane.worker
            else {
                continue;
            };
            let Some(since) = *starting_since else {
                continue;
            };
            let ready_pid = region.header(lane.index).ready_pid.0.load(Acquire);
            match start_report(*pid, ready_pid, region.start_failure(lane.index)) {
                StartReport::Ready => {
                    let mut kept = self.lanes.stats.respawn_us.lock();
                    if kept.len() >= RESPAWN_SAMPLES {
                        kept.remove(0);
                    }
                    kept.push(since.elapsed().as_micros() as u64);
                    drop(kept);
                    *starting_since = None;
                }
                StartReport::Failed { failure } => failed.push((lane.index, failure)),
                StartReport::Pending => {}
            }
        }
        for (i, failure) in failed {
            // A replacement that can't import the app would fail again: no more forks.
            self.break_lane(i, LaneLossError::ReplacementNotStarted { failure });
        }
    }

    /// Breaks lane `i`, which could not get a worker again: its requests are answered with why,
    /// it takes no more, and the run is told ([`LaneHost::carry_out`]).
    fn break_lane(&self, i: usize, why: LaneLossError) {
        let why = Arc::new(why);
        let newly = self.mark_broken(i, &why);
        let verdict = self
            .lanes
            .health
            .lock()
            .failed(usize::from(newly), LaneRunError::LaneLost { lane: LaneIndex(i), why });
        self.carry_out(verdict);
    }

    /// Latches lane `i` broken with `why`, answers its requests, and returns whether it was not
    /// broken before, so a lane is counted broken once. A lane this host does not own changes
    /// nothing.
    fn mark_broken(&self, i: usize, why: &Arc<LaneLossError>) -> bool {
        self.lanes.stats.respawn_failures.fetch_add(1, Relaxed);
        tracing::error!(target: "mipyre::server", lane = i, error = %why, detail = ?why, "process lane lost its worker for good");
        let cause: Arc<str> = Arc::from(why.to_string());
        let marked = self.with_lane(i, |lane| {
            let newly = lane.broken_why().is_none();
            // A lane waiting out a crash loop was taken out of `usable_lanes` already.
            let was_waiting = lane.waiting();
            let counted = lane.worker.takes_requests();
            let before = std::mem::replace(
                &mut lane.worker,
                LaneWorker::Broken {
                    why: Arc::clone(why),
                },
            );
            let forking = match before {
                LaneWorker::Forking { task, .. } => Some(task),
                _ => None,
            };
            let mut replies: Vec<_> = lane.waiting.iter_mut().filter_map(Option::take).collect();
            replies.extend(lane.queued.drain(..).map(|j| j.reply));
            (newly, counted, was_waiting, replies, forking)
        });
        let Some((newly, counted, was_waiting, replies, forking)) = marked else {
            return false;
        };
        if let Some(task) = forking {
            kill_when_forked(i, task);
        }
        if counted {
            self.run.hosts[self.index]
                .usable_lanes
                .fetch_sub(1, Relaxed);
        }
        // A broken lane's crash run is over: it forks no more.
        let ended = {
            let mut runs = self.crash_runs.borrow_mut();
            runs.iter().position(|r| r.lane == i).map(|k| runs.swap_remove(k))
        };
        if ended.is_some_and(|r| r.run.alerted()) {
            crate::telemetry::clear_degraded(&crate::telemetry::Component::ProcessLane(i));
        }
        if was_waiting {
            self.publish_crash_wait();
        }
        self.publish_load();
        for reply in replies {
            answer_if_awaited(
                reply,
                Err(LaneRequestError::Rejected(UnansweredError::LaneLost {
                    lane: LaneIndex(i),
                    cause: Arc::clone(&cause),
                })),
            );
        }
        newly
    }

    /// Carries out what a failure does to the run: under [`FailureResponse::Stop`] the server ends
    /// with the run's first cause, and `serve` returns it, so the process exits non-zero
    /// (systemd restarts it). Nothing is restarted here: the failures seen (the template, the
    /// reactor, the app's import in a replacement) would repeat in a new host or lane, and no
    /// evidence yet says a host ever fails by chance in production.
    fn carry_out(&self, verdict: FailureResponse<LaneRunError>) {
        match verdict {
            FailureResponse::Isolate | FailureResponse::AlreadyStopped => {}
            FailureResponse::Stop(cause) => {
                tracing::error!(target: "mipyre::server", error = %cause, "process lanes failed: the server stops");
                self.run.server_stop.stop(ServerStop::LanesFailed(Arc::new(cause)));
            }
        }
    }

    /// Handles a poller that failed while the run serves, after which nothing reads these lanes.
    /// This thread stops putting requests in them (it is no longer their host), the other threads
    /// skip this host when they send (`HostLink::serving`); every lane is broken, its requests
    /// answered with the failure, and the run decides whether the server ends
    /// ([`RunHealth::failed`]).
    fn fail(&self, failure: PollerError) {
        HOST.with(|h| {
            let mut current = h.borrow_mut();
            if current
                .as_ref()
                .is_some_and(|host| std::ptr::eq(&**host, self))
            {
                *current = None;
            }
        });
        self.lanes.stats.host_failures.fetch_add(1, Relaxed);
        let down = Arc::new(HostError {
            host: self.index,
            cause: failure,
        });
        // A host's poller runs once, so this is the only cause.
        self.run.hosts[self.index]
            .failure
            .get_or_init(|| Arc::clone(&down));
        let lanes: Vec<usize> = self.owned.borrow().iter().map(|l| l.index).collect();
        let why = Arc::new(LaneLossError::HostFailed(Arc::clone(&down)));
        let newly = lanes
            .into_iter()
            .filter(|&i| self.mark_broken(i, &why))
            .count();
        let verdict = self
            .lanes
            .health
            .lock()
            .failed(newly, LaneRunError::HostFailed(Arc::clone(&down)));
        match &verdict {
            FailureResponse::Isolate => {
                tracing::error!(target: "mipyre::server", host = self.index, error = %down, "process lane host failed: isolated, the other hosts keep serving")
            }
            FailureResponse::Stop(_) | FailureResponse::AlreadyStopped => {
                tracing::error!(target: "mipyre::server", host = self.index, error = %down, "process lane host failed: the server stops")
            }
        }
        self.carry_out(verdict);
    }

    /// Answers everything still waiting with 503, once the run has stopped.
    fn close(&self) {
        let mut replies = Vec::new();
        // A crash-loop alert ends with the run that raised it.
        for ended in self.crash_runs.borrow_mut().drain(..) {
            if ended.run.alerted() {
                crate::telemetry::clear_degraded(&crate::telemetry::Component::ProcessLane(ended.lane));
            }
        }
        for lane in self.owned.borrow_mut().iter_mut() {
            replies.extend(lane.waiting.iter_mut().filter_map(Option::take));
            replies.extend(lane.queued.drain(..).map(|j| j.reply));
        }
        for reply in replies {
            answer_if_awaited(
                reply,
                Err(LaneRequestError::Rejected(UnansweredError::WorkersGone(
                    WorkerGroup::Lanes,
                ))),
            );
        }
    }

    fn set_asleep(&self) {
        self.store_server_sleeping(1);
    }

    fn set_awake(&self) {
        self.store_server_sleeping(0);
    }

    fn store_server_sleeping(&self, value: u32) {
        for lane in self.owned.borrow().iter() {
            self.lanes
                .memory
                .region
                .header(lane.index)
                .server_sleeping
                .0
                .store(value, SeqCst);
        }
    }

    /// Registers the wake pipes of this host's lanes in this thread's reactor and starts the
    /// watch on their workers' exits.
    fn watch(&self) -> std::io::Result<(Vec<WakePipe>, ProcessMonitor)> {
        let pipes: Vec<WakePipe> = self
            .owned
            .borrow()
            .iter()
            .map(|l| {
                AsyncFd::with_interest(
                    Fd(self.lanes.wake_reads[l.index].as_raw_fd()),
                    Interest::READABLE,
                )
                .map(|fd| WakePipe { lane: l.index, fd })
                .map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!("registering the wake pipe of lane {}: {e}", l.index),
                    )
                })
            })
            .collect::<std::io::Result<_>>()?;
        let watch = ProcessMonitor::new()
            .map_err(|e| std::io::Error::new(e.kind(), format!("the worker-exit watch: {e}")))?;
        for lane in self.owned.borrow().iter() {
            if let LaneWorker::Running { pid, .. } | LaneWorker::Overrun { pid, .. } = lane.worker {
                let worker = LaneProcess {
                    lane: lane.index,
                    pid,
                };
                if let Err(e) = watch.add(worker) {
                    if e.raw_os_error() == Some(libc::ESRCH) {
                        watch.dead_now(worker);
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        Ok((pipes, watch))
    }

    /// Runs the poller described in the module doc until the run stops.
    async fn run(
        self: Rc<Self>,
        mut inbox: mpsc::UnboundedReceiver<LaneJob>,
        pipes: Vec<WakePipe>,
        watch: ProcessMonitor,
    ) -> Result<(), PollerError> {
        loop {
            if self.stop.is_cancelled() {
                return Ok(());
            }
            while let Ok(job) = inbox.try_recv() {
                self.take_forwarded(job);
            }
            self.collect_in_poller();
            for LaneProcess { lane, pid } in watch.exited().map_err(PollerError::ExitWatchFailed)? {
                self.worker_died(lane, pid).await;
            }
            self.finish_respawns(&watch);
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
            self.lanes.stats.host_sleeps.fetch_add(1, Relaxed);
            let tick = if in_flight
                || self
                    .owned
                    .borrow()
                    .iter()
                    .any(|l| matches!(l.worker, LaneWorker::Forking { .. }))
            {
                Duration::from_millis(10)
            } else {
                Duration::from_millis(500)
            };
            let readable =
                futures_util::future::select_all(pipes.iter().map(|p| Box::pin(wake_readable(p))));
            tokio::select! {
                biased;
                _ = self.stop.cancelled() => {}
                woken = readable => {
                    let ((lane, guard), _, _) = woken;
                    let mut guard = woken_pipe(lane, guard)?;
                    // Left ready while bytes remain, so the next wait returns at once; a byte
                    // written after an `Empty` read makes the pipe ready again.
                    let drained = drain_once(self.lanes.wake_reads[lane].as_fd()).map_err(|errno| {
                        PollerError::HostBellUnreadable { lane: LaneIndex(lane), source: errno.into() }
                    })?;
                    if drained == DrainOutcome::Empty {
                        guard.clear_ready();
                    }
                    self.lanes.stats.pipe_wakes.fetch_add(1, Relaxed);
                }
                _ = self.notify.notified() => {}
                job = inbox.recv() => {
                    if let Some(job) = job {
                        self.take_forwarded(job);
                    }
                }
                ready = watch.readable() => { ready.map_err(PollerError::ExitWatchFailed)?; }
                () = tokio::time::sleep(tick) => {}
            }
        }
    }

    /// Puts a request another thread forwarded into this host's lanes, or answers it with why
    /// not; it is never forwarded again.
    fn take_forwarded(&self, job: LaneJob) {
        if let Err((failure, job)) = self.submit(job) {
            answer_if_awaited(job.reply, Err(failure));
        }
    }

    /// Runs as the thread is about to park with no task ready. With requests in flight it spins
    /// on the lanes for up to the spin window, then flags sleeping, so a worker's later answer
    /// writes the wake pipe, and reads the answers; an answer completes its request on this
    /// thread, and the runtime then runs it instead of parking.
    ///
    /// The spin only checks readiness ([`LaneHost::spin_on_lanes`]); [`LaneHost::collect`]
    /// reads the answers once, after it.
    fn spin_for_next_request(&self) {
        self.counts().park_hooks.add(1);
        if !self.in_flight() {
            return;
        }
        self.set_awake();
        let window = SpinWindow::until(shm::monotonic_ns().saturating_add(self.lanes.host.spin_ns));
        let spun = self.spin_on_lanes(window);
        self.counts().waits.add(WaitTotals {
            waits: 1,
            spin_rounds: spun.rounds.into(),
            parks: u64::from(!spun.ready),
            wakes: 0,
        });
        // Found or not, the thread may park after this hook: flag sleeping first, then read
        // (an answer published after the flag writes the wake pipe). The flag store and this
        // read are one side of a Dekker handshake whose other side is the worker's `resp_head`
        // store and `server_sleeping` load (`worker.rs`), all four `SeqCst`: their single total
        // order lets at most one side miss the other's write, so either this read sees the
        // answer or the worker sees the flag and writes the pipe. An `Acquire` read here may
        // see the old head while the worker sees the old flag (store buffering; on aarch64 an
        // `ldapr` may pass the earlier `stlr`), and the answer would wait for the poller's tick.
        // On x86 a `SeqCst` load is the same `mov` as an `Acquire` one.
        self.set_asleep();
        let read = self.collect_with(SeqCst);
        self.counts().read_by_spin.add(read);
    }

    fn collect_in_poller(&self) -> u64 {
        let read = self.collect();
        self.counts().read_by_poller.add(read);
        read
    }

    /// Spins until a worker of this host's lanes has published an answer this host has not
    /// read, or `window` is over. Not inlined, so its rounds cost the same instructions
    /// wherever they are counted (`lane::server::tests::spin_rounds_bench`).
    #[inline(never)]
    fn spin_on_lanes(&self, window: SpinWindow) -> SpinEnd {
        let owned = self.owned.borrow();
        let region = &self.lanes.memory.region;
        spin(window, shm::monotonic_ns, || {
            owned
                .iter()
                .any(|lane| region.header(lane.index).resp_head.0.load(Acquire) != lane.cursor)
        })
    }

    /// Returns this host's counts, of which this host is the only writer.
    #[inline]
    fn counts(&self) -> &HostCounts {
        &self.lanes.host_counts[self.index].0
    }
}

/// Clears the sleeping flag as tokio's `on_thread_unpark` hook: while the thread is awake its
/// own submits and its next `enter_idle` read the answers, so workers need not write the wake
/// pipe.
pub(crate) fn resume_from_idle() {
    let host = HOST.with(|h| h.borrow().clone());
    if let Some(host) = host {
        host.set_awake();
    }
}

/// Runs [`LaneHost::spin_for_next_request`] as tokio's `on_thread_park` hook on a thread that
/// may host lanes. Tokio calls it only when the thread has nothing ready, so the spin delays
/// no task.
pub(crate) fn enter_idle() {
    let host = HOST.with(|h| h.borrow().clone());
    if let Some(host) = host {
        host.spin_for_next_request();
    }
}

/// A lane's wake pipe, as the host's reactor watches it.
struct WakePipe {
    lane: usize,
    fd: AsyncFd<Fd>,
}

/// Returns a task's outcome if it has finished, from one poll that does not wait, or `None`
/// while it runs. The poll is unconstrained so the runtime's cooperative budget never makes a
/// finished task read as still running.
fn finished_now<T>(
    task: &mut tokio::task::JoinHandle<T>,
) -> Option<Result<T, tokio::task::JoinError>> {
    tokio::task::unconstrained(task).now_or_never()
}

/// Returns how a host's poller ended: `None` when it stopped because the run did, else the
/// failure that ended it. Its unwind is caught here because a `spawn_local` task that panics
/// only drops its output, and the host's lanes would stay usable with no poller behind them.
/// A panic fails the host like an error does, with its text as the cause.
async fn poller_failure(
    poller: impl std::future::Future<Output = Result<(), PollerError>>,
) -> Option<PollerError> {
    match std::panic::AssertUnwindSafe(poller).catch_unwind().await {
        Ok(Ok(())) => None,
        Ok(Err(failure)) => Some(failure),
        Err(payload) => Some(PollerError::Panicked(mipyre_tpc::panic_message(&*payload))),
    }
}

/// Turns waiting on lane `lane`'s wake pipe into its ready guard or the host's failure. A pipe
/// that cannot be waited on ends the poller, because going round again would find it failing
/// at once and spin a core with nobody reading the lanes.
#[inline]
fn woken_pipe<G>(lane: usize, waited: std::io::Result<G>) -> Result<G, PollerError> {
    waited.map_err(|source| PollerError::HostBellUnwatchable { lane: LaneIndex(lane), source })
}

/// Waits until a lane's wake pipe is readable and returns which lane it was.
async fn wake_readable(pipe: &WakePipe) -> (usize, std::io::Result<AsyncFdReadyGuard<'_, Fd>>) {
    (pipe.lane, pipe.fd.readable().await)
}

/// What one read of a wake pipe left in it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DrainOutcome {
    Empty,
    MoreLeft,
}

/// Makes one read of a nonblocking wake pipe, so a peer that keeps writing cannot hold this
/// thread in a loop: whatever it leaves is read on a later round of the poller.
///
/// # Errors
///
/// The read's errno when the pipe cannot be read; reading it again would fail the same way.
fn drain_once(fd: BorrowedFd<'_>) -> Result<DrainOutcome, rustix::io::Errno> {
    let mut buf = [0u8; WAKE_READ_BYTES];
    drain_outcome(super::os::read_bell(fd, &mut buf), WAKE_READ_BYTES)
}

const WAKE_READ_BYTES: usize = 256;

/// Says whether a wake pipe may hold more after a read into a buffer of `cap` bytes returned
/// `read`. A full read or an interrupted one may have left bytes; a short read, end of file and
/// EAGAIN leave none. Any other error is the pipe's and goes back to the caller, which alone
/// can decide what a host whose wake pipe cannot be read does.
fn drain_outcome(
    read: Result<usize, rustix::io::Errno>,
    cap: usize,
) -> Result<DrainOutcome, rustix::io::Errno> {
    match read {
        Ok(n) if n == cap => Ok(DrainOutcome::MoreLeft),
        Err(rustix::io::Errno::INTR) => Ok(DrainOutcome::MoreLeft),
        Ok(_) | Err(rustix::io::Errno::AGAIN) => Ok(DrainOutcome::Empty),
        Err(errno) => Err(errno),
    }
}

/// Sees a host's workers exit through the host's reactor: a kqueue with `EVFILT_PROC`
/// (macOS) or an epoll of pidfds (Linux), itself one readable fd in the runtime. Workers are
/// the template's children, not the server's: the server only needs to know they are gone
/// (the template reaps them and reports each wait status over its socket).
struct ProcessMonitor {
    #[cfg(target_os = "macos")]
    fd: AsyncFd<super::os::ExitQueue>,
    #[cfg(target_os = "linux")]
    fd: AsyncFd<OwnedFd>,
    /// Deaths seen while registering (the pid was already gone).
    early: RefCell<Vec<LaneProcess>>,
    #[cfg(target_os = "linux")]
    pidfds: RefCell<std::collections::HashMap<RawFd, (OwnedFd, LaneProcess)>>,
}

/// The worker process serving a lane.
#[derive(Clone, Copy, Debug)]
struct LaneProcess {
    lane: usize,
    pid: Pid,
}

impl ProcessMonitor {
    /// Records that `worker` was gone before it could be watched, for
    /// [`ProcessMonitor::exited`] to report.
    fn dead_now(&self, worker: LaneProcess) {
        self.early.borrow_mut().push(worker);
    }

    async fn readable(&self) -> std::io::Result<()> {
        let mut guard = self.fd.readable().await?;
        guard.clear_ready();
        Ok(())
    }

    #[cfg(target_os = "macos")]
    fn new() -> std::io::Result<Self> {
        Ok(ProcessMonitor {
            fd: AsyncFd::with_interest(super::os::ExitQueue::new()?, Interest::READABLE)?,
            early: RefCell::new(Vec::new()),
        })
    }

    #[cfg(target_os = "macos")]
    fn add(&self, worker: LaneProcess) -> std::io::Result<()> {
        self.fd.get_ref().add(worker.pid.into(), worker.lane)
    }

    /// Returns every worker seen exiting since the last call, without blocking.
    #[cfg(target_os = "macos")]
    fn exited(&self) -> std::io::Result<Vec<LaneProcess>> {
        const ROOM: usize = 16;
        let mut out = std::mem::take(&mut *self.early.borrow_mut());
        loop {
            let seen = self.fd.get_ref().exited::<ROOM>(Some(Duration::ZERO), |pid, lane| {
                let pid = Pid::new(pid.as_raw_nonzero().get()).ok_or_else(|| {
                    std::io::Error::other(format!("a worker exit was reported for pid {pid:?}"))
                })?;
                out.push(LaneProcess { lane, pid });
                Ok(())
            })?;
            if seen < ROOM {
                return Ok(out);
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn new() -> std::io::Result<Self> {
        let ep = rustix::event::epoll::create(rustix::event::epoll::CreateFlags::CLOEXEC)?;
        Ok(ProcessMonitor {
            fd: AsyncFd::with_interest(ep, Interest::READABLE)?,
            early: RefCell::new(Vec::new()),
            pidfds: RefCell::new(std::collections::HashMap::new()),
        })
    }

    #[cfg(target_os = "linux")]
    fn add(&self, worker: LaneProcess) -> std::io::Result<()> {
        use rustix::event::epoll;
        let owned = rustix::process::pidfd_open(worker.pid.into(), rustix::process::PidfdFlags::empty())?;
        let pfd = owned.as_raw_fd();
        // The pidfd's number is its key: it stays open, so no other fd takes the number, until
        // `exited` removes it.
        epoll::add(
            self.fd.get_ref(),
            &owned,
            epoll::EventData::new_u64(pfd as u64),
            epoll::EventFlags::IN,
        )?;
        self.pidfds.borrow_mut().insert(pfd, (owned, worker));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn exited(&self) -> std::io::Result<Vec<LaneProcess>> {
        use rustix::event::epoll;
        const ROOM: usize = 16;
        let zero = rustix::time::Timespec { tv_sec: 0, tv_nsec: 0 };
        let mut out = std::mem::take(&mut *self.early.borrow_mut());
        // On the stack: the poller asks every round, and a heap buffer costs an allocation each.
        let mut room = [std::mem::MaybeUninit::<epoll::Event>::uninit(); ROOM];
        loop {
            let events: &[epoll::Event] = match epoll::wait(self.fd.get_ref(), &mut room, Some(&zero)) {
                Ok((events, _)) => events,
                Err(rustix::io::Errno::INTR) => continue,
                Err(e) => return Err(e.into()),
            };
            for ev in events {
                let pfd = ev.data.u64() as RawFd;
                if let Some((owned, worker)) = self.pidfds.borrow_mut().remove(&pfd) {
                    epoll::delete(self.fd.get_ref(), &owned)?;
                    drop(owned);
                    out.push(worker);
                }
            }
            if events.len() < ROOM {
                return Ok(out);
            }
        }
    }
}

/// Starts this TPC thread's host for `lanes`, if `rules` route to any. Run the returned
/// host's [`LaneHost::shutdown`] once the thread's connections are drained. A host that can't
/// watch its lanes is an error: the thread, and so the server, does not serve without it.
pub(crate) fn host_tpc_thread<H, M>(
    lanes: Option<&Arc<ServerLanes>>,
    rules: &mipyre_http::plan::site::SiteRules<H, M>,
    thread: usize,
) -> std::io::Result<Option<Rc<LaneHost>>> {
    match lanes {
        Some(lanes) if rules.routes.uses_lane() => LaneHost::start(lanes, thread),
        _ => Ok(None),
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

#[cfg(test)]
mod join_hosts_tests {
    use super::*;

    #[test]
    fn a_host_thread_that_does_not_stop_is_left_at_the_deadline() {
        // The host stays blocked until the test releases it, after the join has returned: a
        // join that waited for it would never return, so the join returning is the proof and
        // how long it took is not asked. The deadline is short. The finished host
        // beside it shows the list names the one left, not every host.
        let (release, released) = std::sync::mpsc::channel::<()>();
        let stuck = mipyre_tpc::spawn(
            std::thread::Builder::new().name("stuck-host".into()),
            move || {
                let _released = released.recv();
            },
        )
        .unwrap();
        let ended = mipyre_tpc::spawn(std::thread::Builder::new().name("ended-host".into()), || {})
            .unwrap();
        while !ended.is_finished() {
            std::thread::yield_now();
        }
        let left = join_hosts(vec![ended, stuck], &ShutdownDeadline::new(Duration::from_millis(40)));
        assert_eq!(left, ["stuck-host"]);
        drop(release);
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
