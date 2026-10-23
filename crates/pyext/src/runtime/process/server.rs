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
//! its lanes' wake pipes, one per lane, all registered once in one epoll/kqueue that the reactor waits on as one fd
//! (shm-channel.md §7.3 H3). A worker writes its pipe one byte only when it sees the flag, so a lane whose pipe brings
//! more bytes than its answers allow is that lane's worker's fault ([`HostEnd::hear_wakes`]). Worker deaths come through the same reactor: a kqueue with `EVFILT_PROC`
//! (macOS) or an epoll of pidfds (Linux), registered as one more readable fd.
//!
//! When a worker dies the host answers the request it was running (502, or 504 when the
//! host killed it for overrunning the timeout), leaves the requests it had not taken in
//! the lane, and has the template fork a replacement (on the blocking pool, so the event
//! loop never waits for a fork); the new worker continues from the first request not taken.
//!
//! Each run gives every lane a channel of its own (`mipyre_shm`), new shared memory and a new wake
//! pipe, handed to the template once per run with each lane's channel, so no worker of an
//! earlier run can write into this run's lanes or wake its hosts (docs/design/shm-channel.md §6.1,
//! root 11).

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::rc::Rc;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crate::sync::{AtomicU32, AtomicU64, AtomicUsize, Mutex, Ordering::*, RwLock};
use bytes::BytesMut;
use futures_util::FutureExt as _;
use tokio::io::unix::AsyncFd;
use tokio::io::Interest;
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::CancellationToken;

use super::core::{
    peer_corrupted, Completions, CoreConfig, DeathCause, HostCore, HostEffects, LaneNotice, LaneWorker, LocalLane,
    Rejection, WorkerTag,
};
use super::health::{
    self, CrashBackoff, CrashLoopWait, FailurePolicy, FailureResponse, HeadOfLine, HostLoad, RunHealth,
};
use super::os::{monotonic_ns, BellQueue};
use super::pid::Pid;
use super::template::{ExitReports, LinkEnd, TemplateCommand, TemplateLink, TemplateReport, TemplateSendError};
use super::wire::LaneRequest;
use super::worker::START_REFUSED;
use super::worker_process::WorkerProcess;
use crate::exchange::{
    refuse, HandlerError, LaneDecodeError, LaneIndex, UnansweredError, WorkerExit, WorkerFailure, WorkerGroup,
};
use crate::runtime::{answer_if_awaited, ServerRunStop, ServerStop};
use crate::telemetry::RunCounters;
use mipyre_cpu::CacheLine;
use mipyre_http::plan::response::HandlerResponse;
use mipyre_shm::adapter::doorbell::{drain_once, open_host_doorbell, Drain, DrainOutcome, FutexDoorbell, HostBell};
use mipyre_shm::adapter::mapped::MappedMemory;
use mipyre_shm::channel::{
    sleep_all, wake_all, Attempt, Channel, HostEnd, PeerFault, SleepVerdict, SlotRoomError, StartOutcome, WaitTotals,
};
use mipyre_shm::plan::geometry::Geometry;
use mipyre_shm::plan::ring::RingDepth;
use mipyre_shm::plan::spin_window::SpinWindow;
use mipyre_shm::plan::start::StartReportError;
use mipyre_shm::protocol::doorbell::{spin, SpinOutcome};
use mipyre_shm::SyscallError;
use mipyre_tpc::{OsThread, ShutdownDeadline, ThreadJoinError};

/// The host's end of one lane's channel: the shared mapping, and a futex to wake the worker with.
pub(super) type LaneEnd = HostEnd<MappedMemory, FutexDoorbell>;

/// Why a lane request got no response. The request's side turns it into its error and
/// counts a refusal there, once ([`LaneRequestError::into_handler_error`]).
#[derive(Debug)]
pub(crate) enum LaneRequestError {
    /// The lanes gave up on it: they did not take it (full, no run serving them, no usable
    /// lane), its worker died running it, it ran past the lane timeout, or its lane lost its
    /// worker for good.
    Rejected(UnansweredError),
    TooLarge(SlotRoomError),
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
    Receive { template: Pid, source: std::io::Error },
    #[error("reading the forked pid from template pid {template}: the template closed the connection")]
    TemplateClosed { template: Pid },
    #[error("template pid {template} could not fork: {source}")]
    Fork { template: Pid, source: std::io::Error },
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
    /// No pidfd could be opened for the template's child `pid` (Linux; macOS opens none), so it
    /// was killed: a worker the server cannot signal safely is not taken.
    #[error("opening a pidfd for pid {pid}, which template pid {template} forked, failed, so it was killed: {source}")]
    PidfdUnopened {
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

/// Why a host's poller stopped reading its lanes while the run serves.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PollerError {
    #[error("the wake pipe of lane {lane} cannot be read: {source}")]
    BellUnreadable { lane: LaneIndex, source: SyscallError },
    #[error("its lanes' wake pipes cannot be waited on: {source}")]
    BellsUnwatchable { source: std::io::Error },
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
    #[error("could not build a process lane host's runtime: {0}")]
    Runtime(std::io::Error),
    #[error("could not spawn process lane host thread {host}: {source}")]
    HostThread { host: usize, source: std::io::Error },
    #[error("process lane host {host} could not start: {source}")]
    HostStart { host: usize, source: std::io::Error },
}

pub(super) type LaneReplySender = tokio::sync::oneshot::Sender<Result<HandlerResponse, LaneRequestError>>;

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
    /// How long a run's start waits for its workers to report ready (they import the app).
    worker_start_timeout: Duration,
    /// How long a host waits for the template to report a dead worker's wait status.
    exit_status_wait: Duration,
}

/// Why [`HostConfig::new`] refused.
#[derive(Debug, thiserror::Error)]
pub(crate) enum HostConfigError {
    #[error("queue_per_worker ({queue_per_worker}) must be at least the lane depth ({depth})")]
    QueueShorterThanLane { queue_per_worker: usize, depth: u32 },
}

impl HostConfig {
    pub(crate) fn new(
        geometry: &Geometry,
        timeout: Duration,
        queue_per_worker: usize,
        server_spin: Duration,
        on_failure: FailurePolicy,
    ) -> Result<Self, HostConfigError> {
        let depth = geometry.depth().get();
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
            worker_start_timeout: WORKER_START_TIMEOUT,
            exit_status_wait: EXIT_STATUS_WAIT,
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
    pub(crate) host_failures: AtomicU64,
}

/// Where a server run hosts the lanes.
#[derive(Clone, Copy, Debug)]
pub(crate) enum HostKind {
    Tpc {
        threads: usize,
    },
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
    /// The run's channel of each lane, read only for how its worker waited.
    channels: Vec<Arc<Channel<MappedMemory>>>,
    stop: CancellationToken,
    /// The server's stop, which the lanes stop with their cause when they end the server (see
    /// [`LaneHost::carry_out`]).
    server_stop: ServerRunStop,
    /// The server run's counts, where each worker death is counted.
    counters: Arc<RunCounters>,
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

/// One lane's part of a run, as its host takes it: the host's end of the lane's channel, and the
/// pipe the lane's workers wake the host with.
struct RunLane {
    index: LaneIndex,
    end: LaneEnd,
    bell: RunBell,
}

/// The pipe a lane's workers wake its host with, one per lane in a run (shm-channel.md §7.3 H3):
/// its read end, and the write end, which the template hands each worker of the lane and the host
/// keeps for the run as well. A pipe with no write end left reads as end of file, which a reactor
/// reports readable for good: once the template and the workers are gone, the host's poller would
/// wake for it on every round.
struct RunBell {
    bell: HostBell,
    write: OwnedFd,
}

/// The server's lanes: how each lane's channel is laid out and how the hosts run them, the
/// workers, the template that forks them, and the current server run.
pub(crate) struct ServerLanes {
    lanes: usize,
    geometry: Geometry,
    host: HostConfig,
    /// Holds each lane's current worker; written by whoever forks it, kills it or sees it die.
    processes: Vec<Mutex<Option<Arc<WorkerProcess>>>>,
    template: OnceLock<Template>,
    /// The wait statuses the template reported, filled by whoever reads its socket.
    pub(super) exits: ExitReports,
    run: RwLock<RunPhase>,
    dedicated: Mutex<Vec<OsThread<()>>>,
    /// Counts the start attempts of every lane over every run, so no two share a number: the
    /// template's late answer to one is never taken for another's.
    attempts: AtomicU32,
    /// Holds the current run's failures, or the last run's once it ended; a new run replaces it.
    health: Mutex<RunHealth<LaneRunError>>,
    pub(crate) stats: Stats,
    /// Has one entry per lane, since no run has more hosts than lanes, and lives as long as the
    /// server. Host `i` of a run is the only writer of entry `i`, which is read for stats only.
    pub(crate) host_counts: Vec<CacheLine<HostCounts>>,
    /// How the workers of the runs that ended waited for requests; each run's channels count
    /// only that run.
    ended_peer_waits: Mutex<WaitTotals>,
    /// Holds, per lane, why it crash-loops while it does; its host writes it and the server's
    /// metrics read it.
    crash_loops: Vec<Mutex<Option<String>>>,
}

/// Holds a counter whose one writer adds with a plain load and store instead of a
/// read-modify-write; others only read it.
#[derive(Default)]
struct OwnCounter(AtomicU64);

impl OwnCounter {
    /// Adds `n`; only the counter's one writer may call it.
    #[inline]
    fn add(&self, n: u64) {
        self.0.store(self.0.load(Relaxed).wrapping_add(n), Relaxed);
    }

    fn read(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

/// Counts how a host waited for its lanes' answers: each spin before its thread parks.
#[derive(Default)]
pub(crate) struct HostWaitCounts {
    waits: OwnCounter,
    spin_rounds: OwnCounter,
    parks: OwnCounter,
    /// Counts the times a publish woke the lane's sleeping worker.
    wakes: OwnCounter,
}

impl HostWaitCounts {
    fn read(&self) -> WaitTotals {
        WaitTotals {
            waits: self.waits.read(),
            spin_rounds: self.spin_rounds.read(),
            parks: self.parks.read(),
            wakes: self.wakes.read(),
        }
    }
}

/// What one host counted: how it waited for its lanes' answers, where it read them, and how
/// often its thread's park hook ran. The host's thread is the only writer ([`OwnCounter`]).
#[derive(Default)]
pub(crate) struct HostCounts {
    waits: HostWaitCounts,
    /// Counts answers read by a submit on this thread, the busy path.
    read_by_submit: OwnCounter,
    /// Counts answers read after the spin before the thread parks
    /// ([`LaneHost::spin_for_next_request`]).
    read_by_spin: OwnCounter,
    /// Counts answers read by the poller task ([`LaneHost::run`]).
    read_by_poller: OwnCounter,
    park_hooks: OwnCounter,
    /// Counts the times the poller went to sleep on the wake pipe, and the times a worker's byte
    /// woke it.
    sleeps: OwnCounter,
    pipe_wakes: OwnCounter,
    /// Counts requests this host's thread handed to a less loaded host while its handlers ran long.
    spread_forwards: OwnCounter,
    /// Counts requests this host's thread forwarded to another host because its own lanes were full.
    overflow_forwards: OwnCounter,
}

/// How often each thing [`HostCounts`] counts, besides its waits, happened on a host, or on all of
/// them summed.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct HostEvents {
    pub(crate) by_submit: u64,
    pub(crate) by_spin: u64,
    pub(crate) by_poller: u64,
    pub(crate) park_hooks: u64,
    pub(crate) sleeps: u64,
    pub(crate) pipe_wakes: u64,
    pub(crate) spread_forwards: u64,
    pub(crate) overflow_forwards: u64,
}

impl HostCounts {
    fn events(&self) -> HostEvents {
        HostEvents {
            by_submit: self.read_by_submit.read(),
            by_spin: self.read_by_spin.read(),
            by_poller: self.read_by_poller.read(),
            park_hooks: self.park_hooks.read(),
            sleeps: self.sleeps.read(),
            pipe_wakes: self.pipe_wakes.read(),
            spread_forwards: self.spread_forwards.read(),
            overflow_forwards: self.overflow_forwards.read(),
        }
    }
}

impl HostEvents {
    fn plus(self, b: HostEvents) -> HostEvents {
        HostEvents {
            by_submit: self.by_submit.wrapping_add(b.by_submit),
            by_spin: self.by_spin.wrapping_add(b.by_spin),
            by_poller: self.by_poller.wrapping_add(b.by_poller),
            park_hooks: self.park_hooks.wrapping_add(b.park_hooks),
            sleeps: self.sleeps.wrapping_add(b.sleeps),
            pipe_wakes: self.pipe_wakes.wrapping_add(b.pipe_wakes),
            spread_forwards: self.spread_forwards.wrapping_add(b.spread_forwards),
            overflow_forwards: self.overflow_forwards.wrapping_add(b.overflow_forwards),
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

/// Hands a run's `lanes` to `hosts` hosts, lane `j` to host [`host_of`]`(j)`: the run they and the
/// other threads share.
fn run_of(lanes: Vec<RunLane>, hosts: usize, server_stop: ServerRunStop, counters: Arc<RunCounters>) -> RunState {
    let channels = lanes.iter().map(|lane| Arc::clone(lane.end.channel())).collect();
    let mut by_host: Vec<Vec<RunLane>> = (0..hosts).map(|_| Vec::new()).collect();
    for lane in lanes {
        by_host[host_of(lane.index, hosts)].push(lane);
    }
    let links = by_host
        .into_iter()
        .map(|lanes| {
            let (inbox, rx) = mpsc::unbounded_channel();
            HostLink {
                inbox,
                usable_lanes: AtomicUsize::new(lanes.len()),
                start: Mutex::new(Some(HostShare { inbox: rx, lanes })),
                load: AtomicUsize::new(0),
                failure: OnceLock::new(),
                crash_wait: Mutex::new(None),
            }
        })
        .collect();
    RunState {
        hosts: links,
        channels,
        stop: CancellationToken::new(),
        server_stop,
        counters,
    }
}

/// Which of a run's `hosts` hosts lane `lane` belongs to.
fn host_of(lane: LaneIndex, hosts: usize) -> usize {
    lane.0 % hosts
}

/// What a host takes when it starts serving a run: the receiving end of its inbox, and its lanes'
/// part of the run.
struct HostShare {
    inbox: mpsc::UnboundedReceiver<LaneJob>,
    lanes: Vec<RunLane>,
}

/// What other threads hold of one host: where to send it a request, and how busy it is.
struct HostLink {
    inbox: mpsc::UnboundedSender<LaneJob>,
    /// What the host takes when it starts.
    start: Mutex<Option<HostShare>>,
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
        self.failure.get().is_none() && self.usable_lanes.load(Relaxed) > 0 && !self.inbox.is_closed()
    }

    #[expect(
        clippy::result_large_err,
        reason = "a job the inbox no longer takes comes back by value, to go elsewhere"
    )]
    fn send(&self, job: LaneJob) -> Result<(), LaneJob> {
        self.inbox.send(job).map_err(|mpsc::error::SendError(job)| job)
    }
}

impl ServerLanes {
    /// Creates the server's `lanes` lanes, each run's channels laid out by `geometry`.
    pub(crate) fn create(lanes: std::num::NonZeroUsize, geometry: Geometry, host: HostConfig) -> Self {
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
            health: Mutex::new(RunHealth::new(n, host.on_failure)),
            stats: Stats::default(),
            host_counts: (0..n).map(|_| CacheLine(HostCounts::default())).collect(),
            ended_peer_waits: Mutex::new(WaitTotals::default()),
            crash_loops: (0..n).map(|_| Mutex::new(None)).collect(),
        }
    }

    /// Returns the lanes that crash-loop now, as `(lane, why)`.
    pub(crate) fn crash_looping(&self) -> Vec<(usize, String)> {
        self.crash_loops
            .iter()
            .enumerate()
            .filter_map(|(i, why)| why.lock().clone().map(|why| (i, why)))
            .collect()
    }

    /// Records why lane `i` crash-loops, or with `None` that it no longer does.
    pub(super) fn set_crash_loop(&self, i: usize, why: Option<String>) {
        if let Some(slot) = self.crash_loops.get(i) {
            *slot.lock() = why;
        }
    }

    pub(crate) fn workers(&self) -> usize {
        self.lanes
    }

    /// Takes the number of a new start attempt.
    fn take_attempt(&self) -> Attempt {
        Attempt::new(self.attempts.fetch_add(1, Relaxed).wrapping_add(1))
    }

    pub(crate) fn host_waits_total(&self) -> WaitTotals {
        sum_waits(self.host_counts.iter().map(|c| c.0.waits.read()))
    }

    pub(crate) fn host_events_total(&self) -> HostEvents {
        self.host_counts
            .iter()
            .map(|c| c.0.events())
            .fold(HostEvents::default(), HostEvents::plus)
    }

    pub(crate) fn worker_waits_total(&self) -> WaitTotals {
        let ended = *self.ended_peer_waits.lock();
        match &*self.run.read() {
            RunPhase::Serving(run) => {
                sum_waits(std::iter::once(ended).chain(run.channels.iter().map(|c| c.peer_waits())))
            }
            RunPhase::Idle | RunPhase::Starting => ended,
        }
    }

    /// Counts the requests in the current run's lanes and queued behind them, as each host last
    /// published it after a submit or a read; none while no run serves.
    pub(crate) fn requests_in_lanes(&self) -> usize {
        match &*self.run.read() {
            RunPhase::Serving(run) => run.hosts.iter().map(|h| h.load.load(Relaxed)).sum(),
            RunPhase::Idle | RunPhase::Starting => 0,
        }
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
            .map(|wait| wait.refusal(monotonic_ns()));
        let first = self.health.lock().first_cause().map(ToString::to_string);
        let cause = first
            .or_else(|| run.hosts.iter().find_map(|h| h.failure.get().map(ToString::to_string)))
            .map(Arc::from);
        health::no_host_left(run.stop.is_cancelled(), waiting, cause)
    }

    /// Takes `pid` as the template, `sock` as the socket to it. The lanes have one template: a
    /// second is refused, its pid given back (its socket is closed).
    pub(crate) fn attach_template(&self, pid: Pid, sock: OwnedFd) -> Result<(), Pid> {
        self.template
            .set(Template {
                pid,
                link: Mutex::new(TemplateLink::new(pid, sock)),
            })
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
        self.processes
            .iter()
            .map(|p| p.lock().as_ref().map(|w| w.pid()))
            .collect()
    }

    /// Queues a request: on this thread's own lanes when it hosts some (no hand-off), else
    /// through a host's inbox. Never waits.
    pub(crate) fn submit(self: &Arc<Self>, job: LaneJob) -> Result<(), (LaneRequestError, LaneReplySender)> {
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
                    host.counts().spread_forwards.add(1);
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
                    let start = next_turn() % run.hosts.len();
                    match run.hand_to(start, Some(host.index), job) {
                        Ok(()) => {
                            host.counts().overflow_forwards.add(1);
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
        let start = next_turn() % run.hosts.len();
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
        if !health::loaded_to_spread(own, host.owned.borrow().lanes.len()) || !host.running_long() {
            return None;
        }
        let start = next_turn() % run.hosts.len();
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
    fn check_fits(&self, head: &crate::exchange::RequestHead, body: &[u8]) -> Result<(), SlotRoomError> {
        fits(approx_message_len(head, body), self.geometry.max_message())
    }

    /// Makes the run's channel for each lane and hands it to the template, forks one worker per
    /// lane, waits until they are ready, and sets up the hosts.
    pub(crate) fn start_run(
        self: &Arc<Self>,
        hosting: HostKind,
        server_stop: ServerRunStop,
        counters: Arc<RunCounters>,
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
            let worker = template
                .fork(i, attempt, FORK_ANSWER_TIMEOUT, &self.exits)
                .map_err(|fork| fork_failed(i, fork))?;
            let pid = worker.pid();
            *self.processes[i.0].lock() = Some(Arc::new(worker));
            spawned.push((pid, attempt));
        }
        let deadline = Instant::now() + self.host.worker_start_timeout;
        for (lane, (pid, attempt)) in lanes.iter().zip(spawned) {
            let i = lane.index;
            let mut gone_since = None;
            loop {
                let now = Instant::now();
                // Read in the order the worker and the template write: a worker writes its start
                // report before it ends, and is reaped after. So what ended it is looked at
                // first, and a report it left is never missed.
                if gone_since.is_none() && !process_exists(pid) {
                    gone_since = Some(now);
                }
                self.read_template_reports();
                let reaped = self.exits.status_of(i, pid);
                let look = WorkerStartSnapshot {
                    report: lane.end.start_report(attempt),
                    reaped,
                    gone_since,
                    now,
                };
                match starting_worker(i, pid, look, deadline, self.host.exit_status_wait) {
                    StartWait::Ready => break,
                    StartWait::Waiting => std::thread::sleep(Duration::from_millis(1)),
                    StartWait::Failed(e) => return Err(e),
                }
            }
        }
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
        let run = Arc::new(run_of(lanes, hosts, server_stop, counters));
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
                                    Err(source) => (Err(LaneStartError::HostStart { host: h, source }), None),
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
    pub(crate) fn stop_run(&self, deadline: &ShutdownDeadline) -> Vec<String> {
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
    /// time is not waited for again: a later read of the socket finds its late answer, which
    /// carries its own attempt, and ends the worker it names.
    fn spawn_worker(&self, i: LaneIndex, attempt: Attempt, within: Duration) -> Result<WorkerProcess, ForkError> {
        let template = self.template.get().ok_or(ForkError::NoTemplate)?;
        template.fork(i, attempt, within, &self.exits)
    }
}

impl Template {
    /// Sends the template its setup: what a worker imports and how a lane's channel is laid out.
    pub(super) fn send_setup(&self, message: &[u8]) -> Result<(), TemplateSendError> {
        self.link.lock().send(&TemplateCommand::Setup(message))
    }

    /// Sends the template lane `lane`'s channel for this run: its shared memory and the write
    /// end of its wake pipe, which the template hands to each worker it starts for the
    /// lane, in place of the last run's.
    fn send_lane(
        &self,
        lane: LaneIndex,
        fds: [BorrowedFd<'_>; LANE_FDS as usize],
        exits: &ExitReports,
    ) -> Result<(), ForkError> {
        let mut link = self.link.lock();
        if let Some(end) = link.ended() {
            return Err(fork_error(self.pid, end.clone()));
        }
        link.send(&TemplateCommand::Lane { lane, fds })
            .map_err(|cut| self.cut_short(&mut link, cut, exits))
    }

    /// Says what a command that did not reach the template met: a template that ended because it
    /// cannot start workers said why first, and that, not the send it made fail, is the cause.
    fn cut_short(&self, link: &mut TemplateLink, cut: TemplateSendError, exits: &ExitReports) -> ForkError {
        self.take_ready(link, exits);
        match link.ended() {
            Some(said @ (LinkEnd::ImportFailed(_) | LinkEnd::LaneFdsMissing { .. })) => {
                fork_error(self.pid, said.clone())
            }
            _ => ForkError::Send(cut),
        }
    }

    /// Tells the template the command line of the run whose workers it starts next: the workers
    /// of one app's runs come from one template, and each run's flag values are its own.
    pub(super) fn send_run(&self, command_line: &[u8]) -> Result<(), TemplateSendError> {
        self.link.lock().send(&TemplateCommand::Run(command_line))
    }

    /// Asks for a worker on `lane` and returns it once the template's answer to this
    /// request comes, taking the other reports that come before it (`exits` gets the exit
    /// reports). Fails when `within` passes first or when the connection ends.
    fn fork(
        &self,
        lane: LaneIndex,
        attempt: Attempt,
        within: Duration,
        exits: &ExitReports,
    ) -> Result<WorkerProcess, ForkError> {
        let template = self.pid;
        let mut link = self.link.lock();
        if let Some(end) = link.ended() {
            return Err(fork_error(template, end.clone()));
        }
        if let Err(cut) = link.send(&TemplateCommand::Start { lane, attempt }) {
            return Err(self.cut_short(&mut link, cut, exits));
        }
        let deadline = Instant::now() + within;
        loop {
            match link.next_report(exits.lanes(), Some(deadline)) {
                Ok(Some(TemplateReport::Forked {
                    attempt: answered,
                    result,
                    ..
                })) if answered == attempt => {
                    // What else is already there goes now, so nobody reads it later.
                    self.take_ready(&mut link, exits);
                    drop(link);
                    let pid = result.map_err(|errno| ForkError::Fork {
                        template,
                        source: errno.into(),
                    })?;
                    return self.child(WorkerTag { lane, attempt, pid });
                }
                Ok(Some(report)) => self.take(&mut link, report, exits),
                Ok(None) => {
                    return Err(ForkError::TimedOut {
                        template,
                        waited: within,
                    })
                }
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
            TemplateReport::Forked { lane, attempt, result } => self.end_late(lane, attempt.get(), result),
            TemplateReport::ImportFailed(traceback) => link.end(LinkEnd::ImportFailed(traceback)),
            TemplateReport::LaneFdsMissing { lane, got } => link.end(LinkEnd::LaneFdsMissing { lane, got }),
        }
    }

    /// Takes `pid`, which the template answered to the fork request `tag`, as a worker once
    /// the kernel says the template is its parent.
    fn child(&self, tag: WorkerTag) -> Result<WorkerProcess, ForkError> {
        WorkerProcess::open(self.pid, tag)
    }

    /// Ends the worker a late answer names (a request whose spawn already gave up), when the
    /// template is its parent; a late fork failure only gets logged.
    fn end_late(&self, lane: LaneIndex, attempt: u32, result: Result<Pid, rustix::io::Errno>) {
        let template = self.pid;
        let worker = result
            .map_err(|errno| ForkError::Fork {
                template,
                source: errno.into(),
            })
            .and_then(|pid| {
                self.child(WorkerTag {
                    lane,
                    attempt: Attempt::new(attempt),
                    pid,
                })
            });
        match worker {
            Ok(worker) => {
                tracing::warn!(
                    target: "mipyre::server",
                    lane = lane.0,
                    attempt,
                    pid = worker.pid().as_raw(),
                    template = template.as_raw(),
                    "the template answered a fork request after its deadline: its worker is killed"
                );
                worker.kill();
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

/// Says why a run's start failed at lane `lane`'s fork (or its lane frame): the template's own
/// report when it could not start workers, else the fork's error.
fn fork_failed(lane: LaneIndex, fork: ForkError) -> LaneStartError {
    match fork {
        ForkError::ImportFailed { traceback, .. } => LaneStartError::TemplateFailed {
            lane,
            failure: traceback,
        },
        ForkError::LaneFdsMissing { lane, got, .. } => LaneStartError::LaneFds {
            lane,
            got,
            want: LANE_FDS,
        },
        source => LaneStartError::Fork { lane, source },
    }
}

/// What a fork request says when the connection to the template ended.
fn fork_error(template: Pid, end: LinkEnd) -> ForkError {
    match end {
        LinkEnd::Closed => ForkError::TemplateClosed { template },
        LinkEnd::RecvFailed(errno) => ForkError::Receive {
            template,
            source: errno.into(),
        },
        end @ LinkEnd::BadFrame { .. } => ForkError::ProtocolViolated { template, end },
        LinkEnd::ImportFailed(traceback) => ForkError::ImportFailed { template, traceback },
        LinkEnd::LaneFdsMissing { lane, got } => ForkError::LaneFdsMissing { template, lane, got },
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

/// Whether `pid` is still a process (a zombie not yet reaped is one): `kill(pid, 0)` finds
/// it, or fails for a reason other than its absence.
fn process_exists(pid: Pid) -> bool {
    rustix::process::test_kill_process(pid.into()) != Err(rustix::io::Errno::SRCH)
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

/// How long a host waits for the template to report a dead worker's wait status, unless its
/// [`HostConfig`] says otherwise.
const EXIT_STATUS_WAIT: Duration = Duration::from_millis(100);

/// How long a run's start waits for its workers to report ready (they import the app), unless
/// its [`HostConfig`] says otherwise.
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
    /// Where this thread's next round-robin pick over a run's hosts starts. Per thread, so the
    /// threads that submit do not all write one shared line (shm-channel.md §7.3 H2); the picks
    /// of each thread still go round every host.
    static TURN: Cell<usize> = const { Cell::new(0) };
}

/// Takes this thread's next round-robin turn ([`TURN`]).
fn next_turn() -> usize {
    TURN.with(|t| {
        let turn = t.get();
        t.set(turn.wrapping_add(1));
        turn
    })
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
        + head.params.iter().map(|(k, v)| k.len() + v.len() + 8).sum::<usize>()
        + 256
}

fn fits(len: usize, room: u32) -> Result<(), SlotRoomError> {
    if len as u64 > u64::from(room) {
        return Err(SlotRoomError {
            room,
            needed: len as u64,
        });
    }
    Ok(())
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
pub(crate) struct LaneHost {
    lanes: Arc<ServerLanes>,
    run: Arc<RunState>,
    index: usize,
    owned: RefCell<HostCore>,
    /// Where each answer is copied out of its slot: it is split off whole, and its body and an
    /// uncommon content type are slices of it, so an answer is copied once (shm-channel.md §2.5,
    /// E). What is left of its room takes the next answers.
    answer_bytes: RefCell<BytesMut>,
    /// What a collect completes, kept between collects so a collect that reads answers does not
    /// allocate for them (shm-channel.md §7.3 H1). Empty between collects.
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
    stop: CancellationToken,
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
        let Some(HostShare {
            inbox,
            lanes: run_lanes,
        }) = run.hosts.get(index).and_then(|h| h.start.lock().take())
        else {
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
            if let Some(failure) = poller_failure(Rc::clone(&poller).run(inbox, watch)).await {
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

    fn depth(&self) -> RingDepth {
        self.lanes.geometry.depth()
    }

    /// What the core needs of this host's settings.
    fn core_config(&self) -> CoreConfig {
        CoreConfig {
            depth: self.depth(),
            timeout: self.lanes.host.timeout,
            crash_backoff: self.lanes.host.crash_backoff,
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
    fn submit(&self, job: LaneJob) -> Result<(), (LaneRequestError, LaneJob)> {
        let placed = self
            .owned
            .borrow_mut()
            .submit(job, self.lanes.host.queue_per_worker, &mut ShellFx::new(self));
        match placed {
            Ok(()) => {}
            Err(Rejection::Refused(why, job)) => return Err((why, job)),
            Err(Rejection::NoLane(job)) => {
                let refusal = self.owned.borrow().no_lane_refusal(
                    &self.lanes.host.crash_backoff,
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
    fn running_long(&self) -> bool {
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
                    health::ran_at_least(true, seen.since_ns, now, health::SPREAD_AFTER_NS)
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
            answer_if_awaited(reply, outcome);
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
        let wait = self.lanes.host.exit_status_wait;
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
                    tracing::error!(target: "mipyre::server", lane = i.0, "process lane forks a replacement with no fork task: it waits for none");
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
        self.owned.borrow_mut().close(&mut ShellFx::new(self));
    }

    /// Says to every lane's worker that this host is awake.
    fn wake(&self) {
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
    async fn run(
        self: Rc<Self>,
        mut inbox: mpsc::UnboundedReceiver<LaneJob>,
        watch: ProcessMonitor,
    ) -> Result<(), PollerError> {
        loop {
            if self.stop.is_cancelled() {
                return Ok(());
            }
            self.count_answers();
            while let Ok(job) = inbox.try_recv() {
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

    /// Adds the answers each lane's end read since the last round to the room its worker has for
    /// wake bytes; once a round, so fewer than 2^32 answers pass between two counts.
    fn count_answers(&self) {
        self.owned.borrow_mut().ends.iter_mut().for_each(LaneEnd::count_answers);
    }

    /// Reads once each lane's wake pipe the queue says holds bytes, and counts the bytes against
    /// the room its answers leave ([`HostEnd::hear_wakes`]). A lane whose worker is at fault is
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
    /// not; it is never forwarded again.
    fn take_forwarded(&self, job: LaneJob) {
        if let Err((failure, job)) = self.submit(job) {
            answer_if_awaited(job.reply, Err(failure));
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
    fn spin_for_next_request(&self) {
        self.counts().park_hooks.add(1);
        if !self.in_flight() {
            return;
        }
        self.wake();
        let window = SpinWindow::until(monotonic_ns().saturating_add(self.lanes.host.spin_ns));
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
    /// wherever they are counted (`lane::server::tests::spin_rounds_bench`).
    #[inline(never)]
    fn spin_on_lanes(&self, window: SpinWindow) -> SpinOutcome {
        let owned = self.owned.borrow();
        spin(window, monotonic_ns, || owned.ends.iter().any(LaneEnd::has_answers))
    }

    /// Returns this host's counts, of which this host is the only writer.
    #[inline]
    fn counts(&self) -> &HostCounts {
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

    #[inline]
    fn reply(&mut self, reply: LaneReplySender, outcome: Result<HandlerResponse, LaneRequestError>) {
        answer_if_awaited(reply, outcome);
    }

    fn say(&mut self, notice: LaneNotice) {
        let host = self.host;
        match notice {
            LaneNotice::WorkerDied { worker, cause, exit } => {
                host.run.counters.process_worker_died();
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
                    target: "mipyre::server", lane = i.0, crashes,
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
        let verdict = self
            .host
            .lanes
            .health
            .lock()
            .failed(usize::from(newly), LaneRunError::LaneLost { lane, why });
        self.host.carry_out(verdict);
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

/// Clears the sleeping flag as tokio's `on_thread_unpark` hook: while the thread is awake its
/// own submits and its next `enter_idle` read the answers, so workers need not write the wake
/// pipe.
pub(crate) fn resume_from_idle() {
    let host = HOST.with(|h| h.borrow().clone());
    if let Some(host) = host {
        host.wake();
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

/// A host's lanes' wake pipes, in one queue this thread's reactor watches, and the write ends the
/// host keeps so no pipe reads as end of file ([`RunBell`]).
struct HostBellWatch {
    fd: AsyncFd<BellQueue>,
    _writes: Vec<OwnedFd>,
}

impl HostBellWatch {
    /// Registers each of `bells` in one queue, under its lane's place among the host's lanes, and
    /// the queue in this thread's reactor.
    fn new(bells: Vec<RunBell>) -> std::io::Result<Self> {
        let (reads, writes): (Vec<HostBell>, Vec<OwnedFd>) =
            bells.into_iter().map(|RunBell { bell, write }| (bell, write)).unzip();
        let queue =
            BellQueue::new(reads).map_err(|e| std::io::Error::new(e.kind(), format!("the wake pipes' queue: {e}")))?;
        let fd = AsyncFd::with_interest(queue, Interest::READABLE)
            .map_err(|e| std::io::Error::new(e.kind(), format!("registering the wake pipes: {e}")))?;
        Ok(HostBellWatch { fd, _writes: writes })
    }
}

/// How many lanes' pipes one look at a host's queue reports; the rest are reported by the next.
const BELL_EVENTS: usize = 16;

/// How many reads of a pipe the host makes, at most, to clear what a dead worker left in it:
/// 1 MiB, the most an unprivileged process can grow a pipe to on Linux.
const LEFTOVER_READS: usize = 4096;

/// Reads what lane `lane`'s pipe holds, up to [`LEFTOVER_READS`] reads, so its next worker is not
/// held to bytes its predecessor (or anything else holding the write end) wrote. What is written
/// past that counts against the next worker.
fn drain_leftover(lane: LaneIndex, bell: &HostBell) -> Result<(), PollerError> {
    for _ in 0..LEFTOVER_READS {
        if bell_drained(lane, drain_once(bell))?.outcome == DrainOutcome::Empty {
            break;
        }
    }
    Ok(())
}

/// Returns a task's outcome if it has finished, from one poll that does not wait, or `None`
/// while it runs. The poll is unconstrained so the runtime's cooperative budget never makes a
/// finished task read as still running.
fn finished_now<T>(task: &mut tokio::task::JoinHandle<T>) -> Option<Result<T, tokio::task::JoinError>> {
    tokio::task::unconstrained(task).now_or_never()
}

/// Returns how a host's poller ended: `None` when it stopped because the run did, else the
/// failure that ended it. Its unwind is caught here because a `spawn_local` task that panics
/// only drops its output, and the host's lanes would stay usable with no poller behind them.
/// A panic fails the host like an error does, with its text as the cause.
async fn poller_failure(poller: impl std::future::Future<Output = Result<(), PollerError>>) -> Option<PollerError> {
    match std::panic::AssertUnwindSafe(poller).catch_unwind().await {
        Ok(Ok(())) => None,
        Ok(Err(failure)) => Some(failure),
        Err(payload) => Some(PollerError::Panicked(mipyre_tpc::panic_message(&*payload))),
    }
}

/// Turns waiting on the host's wake pipe into its ready guard or the host's failure. A pipe
/// that cannot be waited on ends the poller, because going round again would find it failing
/// at once and spin a core with nobody reading the lanes.
#[inline]
fn woken_pipe<G>(waited: std::io::Result<G>) -> Result<G, PollerError> {
    waited.map_err(|source| PollerError::BellsUnwatchable { source })
}

/// Turns one read of lane `lane`'s wake pipe into what it took, or the host's failure: a pipe
/// that cannot be read would fail the same way at once, with nobody reading the lanes.
#[inline]
fn bell_drained(lane: LaneIndex, drained: Result<Drain, SyscallError>) -> Result<Drain, PollerError> {
    drained.map_err(|source| PollerError::BellUnreadable { lane, source })
}

/// Sees a host's workers exit through the host's reactor: the exit watch ([`super::os::ExitWatch`])
/// is itself one readable fd in the runtime. Workers are the template's children, not the server's:
/// the server only needs to know they are gone (the template reaps them and reports each wait
/// status over its socket).
struct ProcessMonitor {
    fd: AsyncFd<super::os::ExitWatch>,
    /// Holds the workers that were gone before they could be watched.
    early: RefCell<Vec<Arc<WorkerProcess>>>,
    /// Holds the workers watched, by the token their exit is reported under, each until it is.
    watched: RefCell<std::collections::HashMap<u64, Arc<WorkerProcess>>>,
    /// The token the next worker watched is given.
    next_token: Cell<u64>,
}

impl ProcessMonitor {
    fn new() -> std::io::Result<Self> {
        Ok(ProcessMonitor {
            fd: AsyncFd::with_interest(super::os::ExitWatch::new()?, Interest::READABLE)?,
            early: RefCell::new(Vec::new()),
            watched: RefCell::new(std::collections::HashMap::new()),
            next_token: Cell::new(0),
        })
    }

    async fn readable(&self) -> std::io::Result<()> {
        let mut guard = self.fd.readable().await?;
        guard.clear_ready();
        Ok(())
    }

    /// Watches `worker` for its exit; one already gone is reported by the next
    /// [`ProcessMonitor::exited`].
    fn add(&self, worker: Arc<WorkerProcess>) -> std::io::Result<()> {
        let token = self.next_token.get();
        match self.fd.get_ref().add(worker.handle(), worker.pid().into(), token)? {
            super::os::Registration::Watching => {
                self.next_token.set(token.wrapping_add(1));
                self.watched.borrow_mut().insert(token, worker);
            }
            super::os::Registration::Gone => {
                worker.record_exit();
                self.early.borrow_mut().push(worker);
            }
        }
        Ok(())
    }

    /// Returns every worker seen exiting since the last call, without blocking.
    fn exited(&self) -> std::io::Result<Vec<Arc<WorkerProcess>>> {
        const ROOM: usize = 16;
        let mut out = std::mem::take(&mut *self.early.borrow_mut());
        loop {
            let seen = self.fd.get_ref().exited::<ROOM>(Some(Duration::ZERO), |token| {
                let dead = self.watched.borrow_mut().remove(&token).ok_or_else(|| {
                    std::io::Error::other(format!(
                        "the exit watch reported token {token}, which no watched worker has"
                    ))
                })?;
                self.fd.get_ref().forget(dead.handle())?;
                dead.record_exit();
                out.push(dead);
                Ok(())
            })?;
            if seen < ROOM {
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

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
