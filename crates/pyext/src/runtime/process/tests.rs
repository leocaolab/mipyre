//! Tests of the process-lane host, on lanes with no workers behind them: the fixture makes the
//! lanes, starts a run that hosts them, and runs the host on a thread of its own, so a host
//! that spins shows as a test that times out instead of hanging the test binary. Where a test
//! plays a lane's worker it does so through the channel's own peer end, as a worker does; where
//! it plays a worker that writes what no worker writes, it writes the channel's header words
//! through a mapping of its own.

use std::convert::Infallible;
use std::os::fd::{AsRawFd, RawFd};

// Test code, as this whole file is; said again on the declaration, so tools that judge a file by its own
// declaration (archcheck's shipping rules) see it as a test.
#[cfg(test)]
#[path = "tests/core_dst.rs"]
mod core_dst;
#[cfg(test)]
#[path = "tests/wire_requests.rs"]
mod wire_requests;

use super::super::core::{publish, refused_start, Replacement};
use super::super::wire;
use super::*;
use crate::exchange::RequestHead;
use crate::runtime::process::lane_set::{lane_geometry, MIN_SLOT_BYTES};
use crate::runtime::process::template::tests::report_frame;
use crate::runtime::process::template::{send_attached, send_with_fds, transfer_all, TransferError, MAX_FRAME_BYTES};
use mipyre_http::io::request_id::{ClientRequestId, RequestId};
use mipyre_http::plan::route::RouteKey;
use mipyre_shm::adapter::doorbell::{HostBellOutcome, PipeDoorbell};
use mipyre_shm::channel::{PeerEnd, SlotWriter, Wait};
use mipyre_shm::plan::ring::{PeerCounterError, Seq};
use mipyre_shm::plan::wakes::WakeFloodError;
use mipyre_shm::protocol::doorbell::{AtomicU32 as ShmWord, Doorbell, Parker};
use mipyre_tpc::ShutdownDeadline;

const DEPTH: u32 = 2;

impl LaneHost {
    /// Breaks lane `i`, which could not get a worker again: its requests are answered with why,
    /// it takes no more, and the run is told ([`LaneHost::carry_out`]).
    fn break_lane(&self, i: LaneIndex, why: LaneLossError) {
        let config = self.core_config();
        self.owned
            .borrow_mut()
            .break_lane(i, why, &config, &mut ShellFx::new(self));
    }
}

/// Past any pid either kernel hands out (Linux at most 2^22, macOS 99,999): no process has
/// `NO_PROCESS + n`.
const NO_PROCESS: i32 = 1 << 30;

fn this_process() -> Pid {
    Pid::new(std::process::id() as i32).unwrap()
}

/// Names the worker of lane `i` at `pid`, as a start attempt of its own.
fn tag(i: usize, pid: Pid) -> WorkerTag {
    WorkerTag {
        lane: LaneIndex(i),
        attempt: Attempt::new(pid.as_raw() as u32),
        pid,
    }
}

/// Takes `pid`, a process this test started, as a worker of lane `i`: the test process plays
/// the template.
fn started_worker(i: usize, pid: Pid) -> WorkerProcess {
    WorkerProcess::open(this_process(), tag(i, pid)).expect("the test process started it")
}

/// Takes `pid`, a process this test started, as lane `i`'s current worker, as a fork leaves it.
fn run_as_worker(lanes: &ServerLanes, i: usize, pid: Pid) -> Arc<WorkerProcess> {
    let worker = Arc::new(started_worker(i, pid));
    *lanes.processes[i].lock() = Some(Arc::clone(&worker));
    worker
}

/// Makes a worker of lane `i` whose process ended and was reaped, at pid `NO_PROCESS + n`.
fn ended_worker(i: usize, n: i32) -> Arc<WorkerProcess> {
    let pid = Pid::new(NO_PROCESS + n).unwrap();
    Arc::new(WorkerProcess::open(this_process(), tag(i, pid)).expect("a pid with no process is taken"))
}

/// Creates lanes for `workers` workers with none forked, as `ServerLanes::create` leaves them
/// before a run.
fn lanes(workers: usize) -> Arc<ServerLanes> {
    lanes_with(workers, FailurePolicy::Isolate)
}

/// Creates [`lanes`] with the failure policy `on_failure`.
fn lanes_with(workers: usize, on_failure: FailurePolicy) -> Arc<ServerLanes> {
    lanes_queueing(workers, DEPTH as usize, on_failure)
}

/// Creates [`lanes`] that hold `queue_per_worker` requests per lane, in its slots and queued
/// behind them.
fn lanes_queueing(workers: usize, queue_per_worker: usize, on_failure: FailurePolicy) -> Arc<ServerLanes> {
    lanes_configured(workers, |host| HostConfig {
        queue_per_worker,
        on_failure,
        ..host
    })
}

/// Creates [`lanes`] whose hosts run as `configure` makes of the default.
fn lanes_configured(workers: usize, configure: impl FnOnce(HostConfig) -> HostConfig) -> Arc<ServerLanes> {
    let geometry = lane_geometry(DEPTH, MIN_SLOT_BYTES).unwrap();
    let host = HostConfig::new(
        &geometry,
        Duration::from_secs(5),
        DEPTH as usize,
        Duration::ZERO,
        FailurePolicy::Isolate,
    )
    .unwrap();
    let lanes = std::num::NonZeroUsize::new(workers).expect("a test asks for at least one lane");
    Arc::new(ServerLanes::create(lanes, geometry, configure(host)))
}

/// Puts the lanes in a run that serves them with `hosts` hosts, as `start_run` does once its
/// workers are ready (here none is forked), and returns the run and each lane's shared memory object, as its
/// workers would get them.
fn serve(lanes: &ServerLanes, hosts: usize) -> (Arc<RunState>, Vec<OwnedFd>) {
    let (run_lanes, fds) = lanes.open_lanes().unwrap();
    let run = Arc::new(run_of(
        run_lanes,
        hosts,
        ServerRunStop::default(),
        Arc::new(RunCounters::new(false)),
    ));
    *lanes.run.write() = RunPhase::Serving(Arc::clone(&run));
    (run, fds)
}

/// Rings nothing: the test drives the host by hand, so a host that said it sleeps is read anyway.
struct NoBell;

impl Doorbell for NoBell {
    type Outcome = ();
    type Error = Infallible;
    fn ring(&self, _: &ShmWord) -> Result<(), Infallible> {
        Ok(())
    }
}

/// Sleeps not at all, so a wait for a request no host sent ends at once.
struct NoSleep;

impl Parker for NoSleep {
    type Error = Infallible;
    fn park(&self, _: &ShmWord, _: u32, _: u64) -> Result<(), Infallible> {
        Ok(())
    }
}

/// A lane's worker as the test plays it.
type TestPeer = PeerEnd<MappedMemory, NoBell, NoSleep>;

/// Returns lane `i`'s worker in `run`, starting where the last one left the lane.
fn peer(run: &RunState, i: usize) -> TestPeer {
    PeerEnd::new(Arc::clone(&run.channels[i]), NoBell, NoSleep)
}

/// Maps lane `i`'s channel on its own from its fds, as a worker's C extension could reach it, to
/// write what no worker writes.
fn raw(fds: &[OwnedFd], i: usize, geometry: Geometry) -> RawChannel {
    RawChannel::map(fds[i].as_fd(), geometry)
}

/// Takes the next request `peer` was sent and answers it with what `fill` writes.
fn answer_with(peer: &mut TestPeer, fill: impl FnOnce(&mut SlotWriter<'_>)) {
    let request = match peer.wait_for_request(SpinWindow::until(0), || 0, 1) {
        Ok(Wait::Request(request)) => request,
        Ok(Wait::SliceOver) => panic!("the host sent the worker no request"),
        Err(never) => match never {},
    };
    request.answer(fill).unwrap_or_else(|never| match never {});
}

/// A lane's channel mapped by the test on its own: the words a worker writes, at the places
/// `mipyre_shm`'s header lays them out (each on a cache line of its own, in order), and its
/// slots, where `mipyre_shm`'s geometry puts them.
struct RawChannel {
    base: std::ptr::NonNull<u8>,
    geometry: Geometry,
}

/// The header words a test writes, by their place.
#[derive(Clone, Copy)]
enum HeaderWord {
    ReqHead = 0,
    Taken = 1,
    RespHead = 2,
}

impl RawChannel {
    fn map(fd: BorrowedFd<'_>, geometry: Geometry) -> Self {
        // SAFETY: a NULL hint lets the kernel pick an address no mapping of this process uses.
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                geometry.total_bytes(),
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        assert_ne!(p, libc::MAP_FAILED, "{}", std::io::Error::last_os_error());
        RawChannel {
            base: std::ptr::NonNull::new(p.cast()).expect("mmap gave a mapping"),
            geometry,
        }
    }

    fn word(&self, word: HeaderWord) -> &ShmWord {
        let at = word as usize * std::mem::size_of::<CacheLine<ShmWord>>();
        // SAFETY: the word lies in the mapped header page, 4-aligned, and atomics are valid over any bytes.
        unsafe { self.base.add(at).cast::<ShmWord>().as_ref() }
    }

    fn store(&self, word: HeaderWord, value: u32) {
        self.word(word).store(value, SeqCst);
    }

    fn load(&self, word: HeaderWord) -> u32 {
        self.word(word).load(SeqCst)
    }
}

impl Drop for RawChannel {
    fn drop(&mut self) {
        // SAFETY: mapped in `map` with this length, and no reference into it outlives `self`.
        unsafe { libc::munmap(self.base.as_ptr().cast(), self.geometry.total_bytes()) };
    }
}

/// Returns host `host`'s load on lane `k` of its own: requests in its slots and queued.
fn load_of(host: &LaneHost, k: usize) -> usize {
    let owned = host.owned.borrow();
    owned.lanes[k].load(&owned.ends[k])
}

/// Returns where host `host`'s lane `k` of its own stands on its ring: (read, sent).
fn window_of(host: &LaneHost, k: usize) -> (u32, u32) {
    let w = host.owned.borrow().ends[k].window();
    (w.read().get(), w.sent().get())
}

/// Builds a lane's cause for a respawn the template refused, its `fork` failing with `text`.
fn fork_failed(text: &str) -> LaneLossError {
    LaneLossError::NotRespawned(ForkError::Fork {
        template: Pid::new(1).unwrap(),
        source: std::io::Error::other(text.to_string()),
    })
}

/// Builds a request for a lane with no worker behind it, so it is only ever queued or refused.
fn job() -> (
    LaneJob,
    tokio::sync::oneshot::Receiver<Result<HandlerResponse, LaneRequestError>>,
) {
    job_with_body(bytes::Bytes::new())
}

/// Builds a [`job`] with `body`.
fn job_with_body(
    body: bytes::Bytes,
) -> (
    LaneJob,
    tokio::sync::oneshot::Receiver<Result<HandlerResponse, LaneRequestError>>,
) {
    let (parts, ()) = hyper::Request::builder()
        .method("GET")
        .uri("/x")
        .body(())
        .unwrap()
        .into_parts();
    let (reply, answer) = tokio::sync::oneshot::channel();
    let request = LaneRequest {
        route_key: RouteKey {
            method: hyper::Method::GET,
            path: "/x".into(),
        },
        head: RequestHead {
            parts,
            params: Default::default(),
            client: mipyre_transport::driver::Peer::Ip("10.0.0.1".parse().unwrap()),
            request_id: RequestId::Client(ClientRequestId::parse("b98").unwrap()),
        },
        body,
    };
    (LaneJob { request, reply }, answer)
}

/// Runs `f` with host `index` of the served run started on this thread (its poller is a local
/// task that never gets to run here, so the test drives the host's lanes by hand).
fn on_host<R>(lanes: &Arc<ServerLanes>, index: usize, f: impl FnOnce(&Rc<LaneHost>) -> R) -> R {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(lanes, index).unwrap().expect("a host");
        f(&host)
    })
}

/// A broken lane takes no request while another lane of its host is usable: the requests go
/// to the usable lane, and the broken one is never chosen (round-robin or the hot-lane pick).
#[test]
fn a_broken_lane_is_skipped_while_another_lane_of_its_host_serves() {
    let lanes = lanes(2);
    let (run, _fds) = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        host.break_lane(LaneIndex(0), fork_failed("injected"));
        let mut answers = Vec::new();
        for n in 0..2 {
            let (job, answer) = job();
            if let Err((why, _)) = lanes.submit(job) {
                panic!("request {n} was refused with lane 1 usable: {why:?}");
            }
            answers.push(answer);
        }
        let owned = host.owned.borrow();
        assert_eq!(owned.lanes[0].load(&owned.ends[0]), 0, "the broken lane took a request");
        assert_eq!(owned.lanes[1].load(&owned.ends[1]), 2);
    });
    assert_eq!(lanes.broken_lanes(), 1);
    assert!(!run.server_stop.is_stopped(), "one usable lane is left");
}

/// A host whose lanes are all broken takes no request from its own thread: the request goes to
/// the other host, which still has a usable lane, and it is counted as an overflow.
#[test]
fn a_host_with_only_broken_lanes_is_passed_over_for_the_other_host() {
    let lanes = lanes(2);
    let (run, _fds) = serve(&lanes, 2);
    on_host(&lanes, 0, |host| {
        host.break_lane(LaneIndex(0), fork_failed("injected"));
        assert!(!run.hosts[0].serving(), "a host with no usable lane still serves");
        assert!(run.hosts[1].serving());
        let (job, _answer) = job();
        if let Err((why, _)) = lanes.submit(job) {
            panic!("refused with host 1 serving: {why:?}");
        }
        assert_eq!(lanes.host_events_total().overflow_forwards, 1);
    });
}

/// A host with no usable lane refuses with its own variant, carrying why its lane broke; with no
/// other host to take the request, that refusal is what the request gets, not WorkersDied.
#[test]
fn no_usable_lane_is_its_own_rejection_with_the_lane_cause() {
    let lanes = lanes(1);
    let (_run, _fds) = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        host.break_lane(LaneIndex(0), fork_failed("boom"));
        let (first, _answer) = job();
        match host.submit(first) {
            Err((LaneRequestError::Rejected(UnansweredError::NoUsableLane { cause: why, .. }), _)) => {
                assert!(why.contains("boom"), "{why}");
            }
            other => panic!("expected NoUsableLane, got {:?}", other.map_err(|(e, _)| e)),
        }
        let (job, _answer) = job();
        match lanes.submit(job) {
            Err((LaneRequestError::Rejected(UnansweredError::NoUsableLane { cause, .. }), _)) => {
                assert!(cause.contains("boom"), "{cause}");
            }
            other => panic!(
                "expected NoUsableLane naming the cause, got {:?}",
                other.map_err(|(e, _)| e)
            ),
        }
    });
}

/// Every lane broken ends the server with the first cause, and the stats count the broken lanes.
#[test]
fn all_lanes_broken_ends_the_server_with_the_first_cause() {
    let lanes = lanes(2);
    let (run, _fds) = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        host.break_lane(LaneIndex(0), fork_failed("boom0"));
        assert!(!run.server_stop.is_stopped(), "one lane of two is left");
        host.break_lane(LaneIndex(1), fork_failed("boom1"));
        assert!(run.server_stop.is_stopped(), "no lane is left");
    });
    let fatal = lanes
        .fatal()
        .expect("the lanes recorded why the server stops")
        .to_string();
    assert!(fatal.contains("boom0"), "{fatal}");
    assert_eq!(lanes.broken_lanes(), 2);
}

/// Builds the host failure the poller reports when a lane's wake pipe cannot be read.
fn wake_pipe_failed() -> PollerError {
    PollerError::BellUnreadable {
        lane: LaneIndex(0),
        source: SyscallError {
            call: mipyre_shm::Syscall::Read,
            errno: rustix::io::Errno::BADF,
        },
    }
}

/// A host whose poller failed records why, with the OS's error, and is no longer one requests
/// are sent to.
#[test]
fn a_failed_host_records_its_failure_and_stops_serving() {
    let lanes = lanes(2);
    let (run, _fds) = serve(&lanes, 2);
    on_host(&lanes, 0, |host| host.fail(wake_pipe_failed()));
    let why = run.hosts[0].failure.get().expect("the host's failure").to_string();
    assert_eq!(
        why,
        format!(
            "process lane host 0 failed: the wake pipe of lane 0 cannot be read: read: {}",
            rustix::io::Errno::BADF
        )
    );
    assert!(!run.hosts[0].serving());
    assert!(run.hosts[1].serving());
    assert_eq!(lanes.stats.host_failures.load(Relaxed), 1);
}

/// A panicking poller is a failure of its host like an error, with the panic's text.
#[test]
fn a_panicking_host_is_failed_with_its_panic_text() {
    let down = HostError {
        host: 3,
        cause: PollerError::Panicked("boom".into()),
    };
    assert_eq!(down.to_string(), "process lane host 3 panicked: boom");
}

/// With host 0 failed, requests from its thread go to host 1, the host still serving; none is
/// refused while it serves.
#[test]
fn requests_go_to_the_host_still_serving_when_another_host_failed() {
    let lanes = lanes(2);
    let (run, _fds) = serve(&lanes, 2);
    on_host(&lanes, 0, |host| {
        host.fail(wake_pipe_failed());
        let mut answers = Vec::new();
        for n in 0..8 {
            let (job, answer) = job();
            if let Err((why, _)) = lanes.submit(job) {
                panic!("request {n} was refused with host 1 still serving: {why:?}");
            }
            answers.push(answer);
        }
    });
    assert!(!run.server_stop.is_stopped());
}

/// By default one failed host is isolated: the server keeps running while another host still
/// serves, and only the failed host's lane is counted as broken.
#[test]
fn a_failed_host_is_isolated_while_another_host_serves() {
    let lanes = lanes(2);
    let (run, _fds) = serve(&lanes, 2);
    on_host(&lanes, 0, |host| host.fail(wake_pipe_failed()));
    assert_eq!(lanes.broken_lanes(), 1);
    assert!(
        !run.server_stop.is_stopped(),
        "the server stopped with one of two hosts still serving"
    );
    assert!(lanes.fatal().is_none());
}

/// The only host fails: the server ends with the host's failure as its cause, and a request
/// gets an explicit refusal that names it, not a send into a dead host.
#[test]
fn with_no_host_left_the_server_ends_and_a_request_is_refused_with_the_cause() {
    let lanes = lanes(1);
    let (run, _fds) = serve(&lanes, 1);
    on_host(&lanes, 0, |host| host.fail(wake_pipe_failed()));
    assert!(run.server_stop.is_stopped());
    let fatal = lanes.fatal().expect("the lanes recorded why the server stops");
    assert!(
        matches!(fatal, LaneRunError::HostFailed(ref down) if down.host == 0),
        "{fatal:?}"
    );
    let (job, _answer) = job();
    match lanes.submit(job) {
        Err((LaneRequestError::Rejected(UnansweredError::WorkersDied { cause, .. }), _)) => {
            assert_eq!(&*cause, fatal.to_string());
        }
        other => panic!(
            "expected a refusal naming the cause, got {:?}",
            other.map_err(|(e, _)| e)
        ),
    }
}

/// Under the exit policy the first failed host ends the server, though another host serves.
#[test]
fn the_exit_policy_ends_the_server_at_the_first_failed_host() {
    let lanes = lanes_with(2, FailurePolicy::Exit);
    let (run, _fds) = serve(&lanes, 2);
    on_host(&lanes, 0, |host| host.fail(wake_pipe_failed()));
    assert!(run.server_stop.is_stopped());
    assert!(lanes.fatal().is_some());
}

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
        starting_worker(LaneIndex(0), pid, ready, deadline, EXIT_STATUS_WAIT),
        StartWait::Ready
    ));
    assert!(matches!(
        starting_worker(LaneIndex(0), pid, look_at(t), deadline, EXIT_STATUS_WAIT),
        StartWait::Waiting
    ));
    let failed = WorkerStartSnapshot {
        report: Ok(Some(StartOutcome::Failed(b"Traceback: ImportError".to_vec()))),
        reaped: Some(70 << 8),
        gone_since: Some(t),
        ..look_at(t)
    };
    match starting_worker(LaneIndex(1), pid, failed, deadline, EXIT_STATUS_WAIT) {
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
        starting_worker(LaneIndex(1), pid, unreadable, deadline, EXIT_STATUS_WAIT),
        StartWait::Failed(LaneStartError::WorkerReportUnreadable {
            source: StartReportError::UnknownState(3),
            ..
        })
    ));
}

/// A worker that exits `START_REFUSED` refused its start, which no replacement would do
/// otherwise; any other exit, 1 among them, is a crash another worker may not repeat.
#[test]
fn only_the_start_refused_status_is_a_refused_start() {
    assert!(refused_start(&WorkerExit::Exited(START_REFUSED)));
    assert!(!refused_start(&WorkerExit::Exited(1)));
    assert!(!refused_start(&WorkerExit::Signaled {
        signal: libc::SIGKILL,
        name: "Killed".into()
    }));
}

/// A starting worker that ended without a start failure fails the start with how it ended,
/// as the template reaped it: its exit status, or its signal.
#[test]
fn a_starting_worker_that_ended_before_ready_fails_the_start_with_its_exit() {
    let pid = Pid::new(4242).unwrap();
    let t = Instant::now();
    let deadline = t + Duration::from_secs(30);
    let killed = |exit: &WorkerExit| matches!(exit, WorkerExit::Signaled { signal, .. } if *signal == libc::SIGKILL);
    let exited_3 = |exit: &WorkerExit| *exit == WorkerExit::Exited(3);
    for (status, want) in [
        (3 << 8, &exited_3 as &dyn Fn(&WorkerExit) -> bool),
        (libc::SIGKILL, &killed),
    ] {
        let ended = WorkerStartSnapshot {
            reaped: Some(status),
            ..look_at(t)
        };
        match starting_worker(LaneIndex(2), pid, ended, deadline, EXIT_STATUS_WAIT) {
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
/// `EXIT_STATUS_WAIT`, and past it fails the start saying the status went unreported.
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
        starting_worker(
            LaneIndex(0),
            pid,
            at(since + EXIT_STATUS_WAIT),
            deadline,
            EXIT_STATUS_WAIT
        ),
        StartWait::Waiting
    ));
    match starting_worker(
        LaneIndex(0),
        pid,
        at(since + EXIT_STATUS_WAIT + Duration::from_nanos(1)),
        deadline,
        EXIT_STATUS_WAIT,
    ) {
        StartWait::Failed(LaneStartError::DiedBeforeReady {
            exit: WorkerExit::Unreported(waited),
            ..
        }) => assert_eq!(waited, EXIT_STATUS_WAIT),
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
        starting_worker(LaneIndex(0), pid, look_at(deadline), deadline, EXIT_STATUS_WAIT),
        StartWait::Waiting
    ));
    assert!(matches!(
        starting_worker(LaneIndex(0), pid, look_at(deadline + Duration::from_nanos(1)), deadline, EXIT_STATUS_WAIT),
        StartWait::Failed(LaneStartError::NotReady { lane: LaneIndex(0), pid: p }) if p == pid
    ));
}

/// A run that cannot fork its workers says so with the typed fork error, by lane.
#[test]
fn a_run_with_no_template_fails_its_start_with_the_typed_fork_error() {
    let lanes = lanes(1);
    match lanes.start_run(
        HostKind::Dedicated,
        ServerRunStop::default(),
        Arc::new(RunCounters::new(false)),
    ) {
        Err(LaneStartError::Fork {
            lane: LaneIndex(0),
            source: ForkError::NoTemplate,
        }) => {}
        other => panic!("expected the lane's fork error, got {other:?}"),
    }
    lanes.stop_run(&ShutdownDeadline::new(Duration::from_secs(5)));
}

/// A stop gives the dedicated host threads the join timeout it is handed (the server's
/// `shutdown_timeout`), not a fixed one: a host thread that does not end is left once that
/// deadline has passed, and the stop returns it. The thread stays blocked until the test
/// releases it, after the stop has returned, so a stop that waited for it would not return.
#[test]
fn a_stop_leaves_a_host_thread_that_does_not_end_at_the_given_timeout_and_returns_it() {
    let lanes = lanes(1);
    let (_run, _fds) = serve(&lanes, 1);
    let (release, released) = std::sync::mpsc::channel::<()>();
    let stuck = mipyre_tpc::spawn(
        std::thread::Builder::new().name("lane-host-stuck".to_owned()),
        move || while released.recv().is_ok() {},
    )
    .unwrap();
    lanes.dedicated.lock().push(stuck);
    let timeout = Duration::from_millis(20);
    let started = Instant::now();
    let abandoned = lanes.stop_run(&ShutdownDeadline::new(timeout));
    let waited = started.elapsed();
    drop(release);
    assert_eq!(abandoned, ["lane-host-stuck"]);
    assert!(
        waited >= timeout,
        "left the host after {waited:?}, before its {timeout:?}"
    );
}

/// A worker death reported for a lane this host does not own, or for a worker its lane no
/// longer runs, changes nothing: the lane is found by its index, not by a position.
#[test]
fn a_death_for_a_lane_not_owned_or_a_stale_worker_changes_nothing() {
    let lanes = lanes(2);
    let mut child = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    let pid = Pid::new(child.id() as i32).unwrap();
    let worker = run_as_worker(&lanes, 0, pid);
    let (_run, _fds) = serve(&lanes, 2);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        // Lane 1 is host 1's.
        host.worker_died(Arc::new(started_worker(1, pid))).await;
        // Lane 0 runs `worker`, not this one.
        host.worker_died(ended_worker(0, 0)).await;
        assert_eq!(lanes.stats.deaths.load(Relaxed), 0);
        assert!(matches!(
            &host.owned.borrow().lanes[0].worker,
            LaneWorker::Running { worker: running, .. } if *running == worker.tag()
        ));
        host.shutdown().await;
    });
    child.kill().unwrap();
    child.wait().unwrap();
}

/// A worker that dies with no template to fork its replacement breaks its lane with the fork's
/// typed error (not its text), and with no lane left the server ends with that cause.
#[test]
fn a_failed_respawn_breaks_the_lane_with_the_typed_fork_error() {
    let lanes = lanes(1);
    let mut child = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    run_as_worker(&lanes, 0, Pid::new(child.id() as i32).unwrap());
    let (run, _fds) = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        child.kill().unwrap();
        // The broken lane is the only lane, so the server is told to stop: wait for that.
        run.server_stop.token().cancelled().await;
        assert!(matches!(
            host.owned.borrow().lanes[0].broken_why(),
            Some(LaneLossError::NotRespawned(ForkError::NoTemplate))
        ));
        run.stop.cancel();
        host.stopped().await;
    });
    child.wait().unwrap();
    assert_eq!(lanes.stats.deaths.load(Relaxed), 1);
    let fatal = lanes
        .fatal()
        .expect("the lanes recorded why the server stops")
        .to_string();
    assert!(fatal.contains("no template process"), "{fatal}");
}

/// A backoff whose waits outlast any test: the lane stays in its wait while the test looks.
const LONG_WAITS: CrashBackoff = CrashBackoff {
    after: 3,
    first: Duration::from_secs(600),
    cap: Duration::from_secs(600),
    alert_every: Duration::from_secs(600),
};

/// Creates [`lanes`] whose crash-looping lanes wait `backoff` before they fork.
fn lanes_backing_off(workers: usize, backoff: CrashBackoff) -> Arc<ServerLanes> {
    lanes_configured(workers, |host| HostConfig {
        crash_backoff: backoff,
        ..host
    })
}

/// Reports lane `i`'s worker `n` dead with exit status 3, as the template would. No worker
/// ever ran, so the lane is first set to run it, as a respawn leaves it.
async fn crash(host: &Rc<LaneHost>, i: usize, n: i32) {
    let worker = ended_worker(i, n);
    host.with_lane(LaneIndex(i), |lane, _| {
        lane.worker = LaneWorker::Running {
            worker: worker.tag(),
            starting: None,
            head: None,
        }
    })
    .expect("the host owns the lane");
    host.lanes.exits.record(LaneIndex(i), worker.pid(), 3 << 8);
    host.worker_died(worker).await;
}

/// Each worker death while the run serves is counted in the run's counts, the metrics page's
/// `mipyre_workers_died_total{runtime="process"}`; a death once the run is stopping is not.
#[test]
fn a_worker_death_is_counted_in_the_run_until_the_run_stops() {
    let lanes = lanes_backing_off(2, LONG_WAITS);
    let (run, _fds) = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        crash(&host, 0, 4101).await;
        crash(&host, 1, 4102).await;
        assert_eq!(run.counters.read().process_workers_died, 2);
        run.stop.cancel();
        crash(&host, 0, 4103).await;
        assert_eq!(run.counters.read().process_workers_died, 2);
    });
}

/// The third consecutive crash of a lane makes it wait before its next fork: a request handed
/// to the dead worker is answered 502 like at any death (not run by the next worker), new ones
/// go to the host's other lane, and with every lane waiting a request is refused 503 with when
/// to retry, from the host's thread and from a thread hosting none.
#[test]
fn a_lane_crashing_three_times_in_a_row_waits_and_the_other_lane_serves() {
    let lanes = lanes_backing_off(2, LONG_WAITS);
    let (run, fds) = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        crash(&host, 0, 4001).await;
        crash(&host, 0, 4002).await;
        assert!(
            matches!(host.owned.borrow().lanes[0].worker, LaneWorker::Forking { .. }),
            "the second crash forks at once"
        );
        assert_eq!(run.hosts[0].usable_lanes.load(Relaxed), 2);
        // A request waiting in lane 0 when its third worker dies.
        let (held, mut held_answer) = job();
        if let Err((why, _)) = host.submit(held) {
            panic!("refused before the lane waits: {why:?}");
        }
        assert_eq!(load_of(&host, 0), 1);
        crash(&host, 0, 4003).await;
        assert!(host.owned.borrow().lanes[0].waiting());
        assert_eq!(run.hosts[0].usable_lanes.load(Relaxed), 1);
        match held_answer.try_recv() {
            Ok(Err(LaneRequestError::Rejected(UnansweredError::WorkerDiedBeforeTaking {
                lane: LaneIndex(0),
                exit,
                ..
            }))) => {
                assert_eq!(exit.to_string(), "exited with status 3");
            }
            other => panic!("expected the dead worker's refusal, got {other:?}"),
        }
        let h = raw(&fds, 0, lanes.geometry);
        assert_eq!(
            h.load(HeaderWord::Taken),
            h.load(HeaderWord::ReqHead),
            "the refused request is taken out of the ring: the next worker never runs it"
        );
        // The other lane takes new requests.
        let (next, _next_answer) = job();
        if let Err((why, _)) = host.submit(next) {
            panic!("refused with lane 1 serving: {why:?}");
        }
        assert_eq!(load_of(&host, 1), 1);
        assert_eq!(load_of(&host, 0), 0);
        // Lane 1 crash-loops too (its request is refused with it): nothing takes requests.
        for pid in [4011, 4012, 4013] {
            crash(&host, 1, pid).await;
        }
        assert!(!run.hosts[0].serving());
        let (job0, _a) = job();
        match lanes.submit(job0) {
            Err((
                LaneRequestError::Rejected(UnansweredError::LaneCrashLooping {
                    lane: LaneIndex(0),
                    crashes: 3,
                    ..
                }),
                _,
            )) => {}
            other => panic!(
                "expected lane 0's wait, back first, got {:?}",
                other.map_err(|(e, _)| e)
            ),
        }
        let off_host = Arc::clone(&lanes);
        let refused = std::thread::spawn(move || {
            let (job, _a) = job();
            off_host.submit(job).map_err(|(e, _)| e)
        })
        .join()
        .unwrap();
        assert!(
            matches!(
                refused,
                Err(LaneRequestError::Rejected(UnansweredError::LaneCrashLooping { .. }))
            ),
            "{refused:?}"
        );
        assert_eq!(lanes.stats.deaths.load(Relaxed), 6);
        assert!(!run.server_stop.is_stopped(), "a crash loop does not end the server");
        assert_eq!(lanes.broken_lanes(), 0);
        run.stop.cancel();
        host.stopped().await;
    });
}

/// A lane waiting out a crash loop that then breaks (its host failed) is counted out of its
/// host's usable lanes once, not twice.
#[test]
fn a_waiting_lane_that_breaks_leaves_the_usable_count_once() {
    let lanes = lanes_backing_off(2, LONG_WAITS);
    let (run, _fds) = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        for pid in [4021, 4022, 4023] {
            crash(&host, 0, pid).await;
        }
        assert_eq!(run.hosts[0].usable_lanes.load(Relaxed), 1);
        host.break_lane(LaneIndex(0), fork_failed("injected"));
        assert_eq!(run.hosts[0].usable_lanes.load(Relaxed), 1);
        assert!(
            run.hosts[0].crash_wait.lock().is_none(),
            "a broken lane waits for nothing"
        );
        run.stop.cancel();
        host.stopped().await;
    });
}

// ─────────────────────────── per-request instruction counts ───────────────────────────
//
// The lane's per-request logic on its own, with no worker process and no waiting, so a request
// costs a fixed number of instructions: `benchmarks/micro/lane_submit/count.sh` runs each pair
// under `perf stat -e instructions:u` and divides the difference of the `_x2` and `_x1` runs by
// `BENCH_REQUESTS`. In the normal test run they are short runs that check every request is taken.

/// Requests per `_x1` run (the `_x2` run makes twice as many).
const BENCH_REQUESTS: u64 = 10_000;

/// The answer the stand-in worker publishes for every request.
fn bench_answer() -> HandlerResponse {
    HandlerResponse::new(
        hyper::StatusCode::OK,
        bytes::Bytes::from_static(b"{\"message\":\"Hello, World!\"}"),
        mipyre_http::plan::response::JSON,
        mipyre_http::plan::response::HeaderLines::new(),
    )
}

/// Runs `requests` requests through a host's own lane as a TPC thread hosting it runs them,
/// with the worker's side done in place through the lane's peer end.
fn bench_requests_on_host(requests: u64) {
    let lanes = lanes(1);
    let (run, _fds) = serve(&lanes, 1);
    let answer = bench_answer();
    on_host(&lanes, 0, |_host| {
        let host = HOST.with(|h| h.borrow().clone()).expect("this thread hosts the lanes");
        let mut worker = peer(&run, 0);
        for _ in 0..requests {
            let (job, reply) = job();
            if let Err((why, _)) = lanes.submit(job) {
                panic!("refused: {why:?}");
            }
            stand_in_answers(&mut worker, &answer);
            host.collect();
            std::hint::black_box(reply);
        }
    });
}

/// The stand-in worker's part of a bench request: takes the request the host published and
/// encodes `answer` into its answer slot, as a worker does.
#[inline(never)]
fn stand_in_answers(peer: &mut TestPeer, answer: &HandlerResponse) {
    let Ok(Wait::Request(request)) = peer.wait_for_request(SpinWindow::until(0), || 0, 1) else {
        panic!("the host published a request");
    };
    let answered = request.answer(|w| wire::encode_response(w, answer).expect("the answer fits"));
    std::hint::black_box(answered.is_ok());
}

/// Runs `requests` requests from a thread that hosts no lane: `submit` hands each to the
/// host's inbox, and the host only takes it off without running it.
fn bench_requests_off_host(requests: u64) {
    let lanes = lanes(1);
    let (run, _fds) = serve(&lanes, 1);
    let mut inbox = run.hosts[0].start.lock().take().expect("no host took the inbox").inbox;
    for _ in 0..requests {
        let (job, reply) = job();
        if let Err((why, _)) = lanes.submit(job) {
            panic!("refused: {why:?}");
        }
        std::hint::black_box(inbox.try_recv().expect("the job is in the inbox"));
        std::hint::black_box(reply);
    }
}

#[test]
fn bench_on_host_x1() {
    bench_requests_on_host(BENCH_REQUESTS);
}

#[test]
fn bench_on_host_x2() {
    bench_requests_on_host(2 * BENCH_REQUESTS);
}

#[test]
fn bench_off_host_x1() {
    bench_requests_off_host(BENCH_REQUESTS);
}

#[test]
fn bench_off_host_x2() {
    bench_requests_off_host(2 * BENCH_REQUESTS);
}

/// Runs [`bench_requests_on_host`] with a poller round per request: the host's poller is woken
/// after each answer is published and runs before the next request.
fn bench_requests_with_poller(requests: u64) {
    let lanes = lanes(1);
    let (run, _fds) = serve(&lanes, 1);
    let answer = bench_answer();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        let mut worker = peer(&run, 0);
        for _ in 0..requests {
            let (job, reply) = job();
            if let Err((why, _)) = lanes.submit(job) {
                panic!("refused: {why:?}");
            }
            stand_in_answers(&mut worker, &answer);
            host.notify.notify_one();
            tokio::task::yield_now().await;
            std::hint::black_box(reply);
        }
        run.stop.cancel();
        host.stopped().await;
    });
}

#[test]
fn bench_with_poller_x1() {
    bench_requests_with_poller(BENCH_REQUESTS);
}

#[test]
fn bench_with_poller_x2() {
    bench_requests_with_poller(2 * BENCH_REQUESTS);
}

/// Returns what a request got once its answer was sent, and panics while none was.
fn answered(
    answer: &mut tokio::sync::oneshot::Receiver<Result<HandlerResponse, LaneRequestError>>,
) -> Result<HandlerResponse, LaneRequestError> {
    answer.try_recv().expect("the request was answered")
}

/// Writes a worker's answer `200 ok`.
fn ok_answer(w: &mut SlotWriter<'_>) {
    let ok = Ok(HandlerResponse::new(
        hyper::StatusCode::OK,
        bytes::Bytes::from_static(b"ok"),
        hyper::header::HeaderValue::from_static("text/plain"),
        mipyre_http::plan::response::HeaderLines::new(),
    ));
    wire::encode_answer(w, &ok).expect("a short answer fits");
}

/// Returns the status and body a lane failure is answered with, through the request's error.
fn response_of(failure: LaneRequestError) -> (u16, String) {
    use http_body_util::BodyExt;
    let id = mipyre_http::io::request_id::RequestId::mint();
    let resp = crate::exchange::captured_log_value(|| {
        failure
            .into_handler_error()
            .log(&crate::exchange::label(&id))
            .into_response(&crate::telemetry::RunCounters::new(false))
    });
    let status = resp.status().as_u16();
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let body = rt.block_on(resp.into_body().collect()).unwrap().to_bytes();
    (status, String::from_utf8_lossy(&body).into_owned())
}

fn status_of(failure: LaneRequestError) -> u16 {
    response_of(failure).0
}

/// A message exactly a slot's room fits it; one byte more is refused before it is queued, with
/// both sizes.
#[test]
fn a_message_fits_a_slot_up_to_its_room_and_one_byte_more_is_too_large() {
    let room = (MIN_SLOT_BYTES - 8) as u32;
    assert!(fits(room as usize, room).is_ok());
    assert!(fits(0, room).is_ok());
    match fits(room as usize + 1, room) {
        Err(SlotRoomError { needed, room: r }) => assert_eq!((needed, r), (u64::from(room) + 1, room)),
        Ok(()) => panic!("a message one byte past the slot fits"),
    }
}

/// A request bigger than a slot is answered 413 without taking a slot or a place in the queue,
/// and the next request goes into the lane as if it had not come.
#[test]
fn a_too_large_request_is_413_and_takes_nothing_from_the_lane() {
    let lanes = lanes(1);
    let (_run, _fds) = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        let room = lanes.geometry.max_message() as usize;
        let (big, _answer) = job_with_body(bytes::Bytes::from(vec![b'x'; room]));
        match lanes.submit(big) {
            Err((LaneRequestError::TooLarge(SlotRoomError { needed, room: r }), _)) => {
                assert!(needed > room as u64, "{needed} <= {room}");
                assert_eq!(r as usize, room);
            }
            other => panic!("expected TooLarge, got {:?}", other.map_err(|(e, _)| e)),
        }
        assert_eq!(load_of(host, 0), 0, "the refused request holds a place");
        let (next, _answer) = job();
        if let Err((why, _)) = lanes.submit(next) {
            panic!("the request after a too-large one was refused: {why:?}");
        }
        assert_eq!((window_of(host, 0).1, load_of(host, 0)), (1, 1));
    });
}

/// Each way a lane request fails is its request's error and status: the lanes' refusal as it
/// is (a dead worker 502), a request too large for a slot 413, the worker's failure and an
/// answer that cannot be read 500, whose body says neither (the log line does).
#[test]
fn every_lane_failure_is_its_request_error_and_status() {
    let died = || {
        LaneRequestError::Rejected(UnansweredError::WorkerDied {
            pid: 42,
            lane: LaneIndex(0),
            ran: Some(Duration::from_millis(3)),
            exit: WorkerExit::Exited(3),
        })
    };
    assert!(matches!(
        died().into_handler_error(),
        HandlerError::Refused(UnansweredError::WorkerDied {
            pid: 42,
            lane: LaneIndex(0),
            ..
        })
    ));
    assert_eq!(status_of(died()), 502);

    let too_large = || LaneRequestError::TooLarge(SlotRoomError { needed: 9, room: 8 });
    assert!(matches!(
        too_large().into_handler_error(),
        HandlerError::BodyRejected(mipyre_http::io::body::BodyReadError::TooLarge)
    ));
    assert_eq!(status_of(too_large()), 413);

    let failed = || {
        LaneRequestError::WorkerFailed(WorkerFailure {
            error: "ValueError: 7f3a".into(),
            traceback: Some("Traceback ...".into()),
        })
    };
    match failed().into_handler_error() {
        HandlerError::LaneWorkerFailed(f) => {
            assert_eq!(f.error, "ValueError: 7f3a");
            assert_eq!(f.traceback.as_deref(), Some("Traceback ..."));
        }
        other => panic!("expected the worker's failure, got {other:?}"),
    }
    let (status, body) = response_of(failed());
    assert_eq!(status, 500);
    assert!(!body.contains("7f3a") && !body.contains("Traceback"), "{body}");

    let malformed = || {
        LaneRequestError::Malformed(LaneDecodeError::UnknownTag {
            field: crate::exchange::TaggedField::AnswerKind,
            tag: 9,
        })
    };
    assert!(matches!(
        malformed().into_handler_error(),
        HandlerError::LaneAnswerMalformed(LaneDecodeError::UnknownTag { tag: 9, .. })
    ));
    assert_eq!(status_of(malformed()), 500);
}

/// An answer the host cannot read fails only its own request (Malformed); its slot is free
/// again, so the lane takes a full depth of new requests into its slots and answers them.
#[test]
fn a_malformed_answer_fails_its_request_and_frees_its_slot() {
    let lanes = lanes(1);
    let (run, _fds) = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        let mut worker = peer(&run, 0);
        let (first, mut first_answer) = job();
        if let Err((why, _)) = lanes.submit(first) {
            panic!("refused: {why:?}");
        }
        answer_with(&mut worker, |w| w.put(&[9]).unwrap());
        assert!(host.collect() > 0);
        match answered(&mut first_answer) {
            Err(LaneRequestError::Malformed(LaneDecodeError::UnknownTag { tag: 9, .. })) => {}
            other => panic!("expected the answer to be malformed, got {other:?}"),
        }
        assert_eq!(window_of(host, 0).0, 1);
        assert_eq!(load_of(host, 0), 0);

        let mut answers = Vec::new();
        for n in 0..DEPTH {
            let (next, answer) = job();
            if let Err((why, _)) = lanes.submit(next) {
                panic!("request {n} after the malformed answer was refused: {why:?}");
            }
            answers.push(answer);
        }
        assert!(
            host.owned.borrow().lanes[0].queued.is_empty(),
            "a slot is still held by the bad answer"
        );
        assert_eq!(window_of(host, 0).1, 1 + DEPTH);
        for answer in answers.iter_mut() {
            answer_with(&mut worker, ok_answer);
            host.collect();
            let resp = answered(answer).expect("a good answer after the bad one");
            assert_eq!(&resp.body[..], b"ok");
        }
    });
}

/// A worker that dies running a request gives up that request and every one handed to it that it never took: the
/// one it ran is answered 502 `WorkerDied`, the untaken one 502 `WorkerDiedBeforeTaking` (the dead worker could have
/// rewritten its bytes), and the next worker finds none of them, only what is sent after. (How the window splits,
/// also across the wrap, is `mipyre_shm`'s `exit_split`.)
#[test]
fn a_dead_worker_gives_up_the_request_it_ran_and_those_it_never_took() {
    let lanes = lanes_backing_off(1, LONG_WAITS);
    let (mut gone, worker) = stand_in_worker(&lanes, 0);
    let (run, _fds) = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        let mut ran = publish_into(&host, 0);
        let mut untaken = publish_into(&host, 0);
        // The worker takes the first request, and dies running it.
        let mut taker = peer(&run, 0);
        let Ok(Wait::Request(request)) = taker.wait_for_request(SpinWindow::until(0), || 0, 1) else {
            panic!("the host published a request");
        };
        drop(request);
        gone.kill().unwrap();
        gone.wait().unwrap();
        report_death(&host, worker).await;
        assert!(matches!(
            answered(&mut ran),
            Err(LaneRequestError::Rejected(UnansweredError::WorkerDied {
                lane: LaneIndex(0),
                ..
            }))
        ));
        assert!(matches!(
            answered(&mut untaken),
            Err(LaneRequestError::Rejected(UnansweredError::WorkerDiedBeforeTaking {
                lane: LaneIndex(0),
                ..
            }))
        ));
        assert_eq!(window_of(&host, 0), (2, 2));
        let mut next = peer(&run, 0);
        assert!(
            matches!(
                next.wait_for_request(SpinWindow::until(0), || 0, 1),
                Ok(Wait::SliceOver)
            ),
            "the next worker was handed a request its predecessor could have rewritten"
        );
        host.shutdown().await;
    });
}

/// Breaking a lane answers every request it holds — in its slots and queued behind them — with
/// LaneLost naming the lane and its cause; none is left waiting, and the lane holds none.
#[test]
fn breaking_a_lane_answers_every_request_it_holds_lane_lost() {
    let lanes = lanes_queueing(1, DEPTH as usize + 1, FailurePolicy::Isolate);
    let (_run, _fds) = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        let mut answers = Vec::new();
        for n in 0..=DEPTH {
            let (job, answer) = job();
            if let Err((why, _)) = lanes.submit(job) {
                panic!("request {n} was refused: {why:?}");
            }
            answers.push(answer);
        }
        assert_eq!(
            host.owned.borrow().lanes[0].queued.len(),
            1,
            "one request queued behind the slots"
        );
        host.break_lane(LaneIndex(0), fork_failed("boom"));
        for (n, answer) in answers.iter_mut().enumerate() {
            match answered(answer) {
                Err(LaneRequestError::Rejected(UnansweredError::LaneLost {
                    lane: LaneIndex(0),
                    cause,
                })) => {
                    assert!(cause.contains("boom"), "request {n}: {cause}");
                }
                other => panic!("request {n}: expected LaneLost, got {other:?}"),
            }
        }
        let owned = host.owned.borrow();
        assert!(owned.lanes[0].waiting.iter().all(Option::is_none));
        assert!(owned.lanes[0].queued.is_empty());
    });
}

/// Measures the instructions one empty spin round costs on each side, so the spin can be taken
/// out of a full-path count (`bench_paths.py`'s `lane` reports the rounds per request). It
/// spins for `BENCH_US` microseconds on lanes no worker answers (host) and on a counter nobody
/// moves (worker), and prints the rounds; a round costs (instructions:u at `BENCH_US`=N minus
/// at 0) divided by the rounds printed:
///
///   cargo test --release -p mipyre_pyext --lib --no-run
///   BENCH_SPIN=host BENCH_US=200000 perf stat -x, -e instructions:u -- \
///     target/release/deps/mipyre_engine-<hash> --exact lane::server::tests::spin_rounds_bench
///
/// It spins the server's own non-inlined function ([`LaneHost::spin_on_lanes`]) and, for the
/// worker, the channel's spin on one counter as `PeerEnd::wait_for_request` makes it
/// ([`worker_spin`]).
///
/// Run as a test without `BENCH_SPIN` (both sides, 50 µs each), it checks that a spin on lanes
/// nobody answers ends with nothing found, whatever the time.
#[test]
fn spin_rounds_bench() {
    let us: u64 = std::env::var("BENCH_US").map_or(50, |v| v.parse().expect("BENCH_US: microseconds"));
    let sides = match std::env::var("BENCH_SPIN") {
        Ok(side) => vec![side],
        Err(_) => vec!["host".to_string(), "worker".to_string()],
    };
    for side in sides {
        let window = SpinWindow::until(monotonic_ns() + us * 1_000);
        let spun = match side.as_str() {
            "host" => {
                let lanes = lanes(1);
                let (_run, _fds) = serve(&lanes, 1);
                on_host(&lanes, 0, |host| host.spin_on_lanes(window))
            }
            "worker" => worker_spin(&ShmWord::new(0), window),
            other => panic!("BENCH_SPIN={other}: host or worker"),
        };
        assert!(!spun.ready, "{side}: nothing was published, nothing is found");
        // With no time there is no round; the setup is the same either way.
        let rounds = if us == 0 { 0 } else { spun.rounds };
        println!("ROUNDS={rounds}");
    }
}

/// Spins on `counter` until it moves from 0 or `window` is over, as a worker waits for a request.
#[inline(never)]
fn worker_spin(counter: &ShmWord, window: SpinWindow) -> SpinOutcome {
    spin(window, monotonic_ns, || counter.load(Acquire) != 0)
}

/// Measures the instructions of the lane's logic for one request on fixed input, without any
/// waiting and without the handler. It runs `BENCH_N` requests; a request costs
/// (instructions:u at `BENCH_N`=N minus at 0) divided by N:
///
///   cargo test --release -p mipyre_pyext --lib --no-run
///   perf stat -x, -e instructions:u -- env BENCH_N=200000 \
///     target/release/deps/mipyre_engine-<hash> --exact lane::server::tests::lane_logic_bench
///
/// This is the lane's per-PR count, because the full path (`bench_paths.py`'s `lane`) also
/// counts the waiting, which follows timing. Run as a test without `BENCH_N` (16 requests), it
/// checks that each request is taken, decodes, and is answered.
#[test]
fn lane_logic_bench() {
    let n: u32 = std::env::var("BENCH_N").map_or(16, |v| v.parse().expect("BENCH_N: requests"));
    let lanes = lanes(1);
    let (run, _fds) = serve(&lanes, 1);
    let response = Ok(HandlerResponse::new(
        hyper::StatusCode::OK,
        bytes::Bytes::from_static(br#"{"id":1,"name":"mipyre","tags":["a","b","c"],"price":12.5,"ok":true}"#),
        hyper::header::HeaderValue::from_static("application/json"),
        mipyre_http::plan::response::HeaderLines::new(),
    ));
    on_host(&lanes, 0, |host| {
        let mut worker = peer(&run, 0);
        for seq in 0..n {
            let (job, mut answer) = job();
            assert!(host.submit(job).is_ok(), "the lane takes request {seq}");
            let Ok(Wait::Request(request)) = worker.wait_for_request(SpinWindow::until(0), || 0, 1) else {
                panic!("request {seq} was published");
            };
            let view = request.bytes();
            let req = view
                .as_deref()
                .map_err(|e| LaneDecodeError::PastSlot(*e))
                .and_then(wire::decode_request);
            assert!(req.is_ok(), "request {seq} decodes");
            drop(req);
            drop(view);
            let fits = request.answer(|w| assert!(wire::encode_answer(w, &response).is_ok(), "answer {seq} fits"));
            assert!(fits.is_ok());
            host.collect();
            assert!(matches!(answer.try_recv(), Ok(Ok(_))), "request {seq} answered");
        }
    });
}

/// Runs a poller that panics with `text`.
async fn poller_panicking(text: &'static str) -> Result<(), PollerError> {
    panic!("{text}")
}

/// How a poller ended is the host's failure: its error as it was, its panic with the panic's
/// own text; one that stopped with the run is no failure.
#[test]
fn a_poller_that_fails_or_panics_is_the_hosts_failure_with_its_cause() {
    let ended = |poller| {
        poller_failure(poller)
            .now_or_never()
            .expect("the poller ended at its first poll")
    };
    assert!(ended(std::future::ready(Ok(())).boxed_local()).is_none());
    let exit_watch = std::io::Error::other("kqueue closed");
    match ended(std::future::ready(Err(PollerError::ExitWatchFailed(exit_watch))).boxed_local()) {
        Some(PollerError::ExitWatchFailed(e)) => assert_eq!(e.to_string(), "kqueue closed"),
        other => panic!("expected the exit-watch failure, got {other:?}"),
    }
    match ended(poller_panicking("the poller fell over").boxed_local()) {
        Some(PollerError::Panicked(text)) => assert_eq!(text, "the poller fell over"),
        other => panic!("expected the panic, got {other:?}"),
    }
}

/// A wake pipe that cannot be waited on is the host's failure, with the OS error; a ready pipe
/// is its guard.
#[test]
fn a_wake_pipe_that_cannot_be_waited_on_fails_the_host_with_the_os_error() {
    assert!(matches!(woken_pipe(Ok("guard")), Ok("guard")));
    let ebadf = std::io::Error::from_raw_os_error(libc::EBADF);
    match woken_pipe::<()>(Err(ebadf)) {
        Err(PollerError::BellsUnwatchable { source }) => {
            assert_eq!(source.raw_os_error(), Some(libc::EBADF));
        }
        other => panic!("expected the wake pipe's failure, got {other:?}"),
    }
}

/// Returns the write end of lane `k`'s wake pipe, of the host that takes `host`'s share of the
/// run, as every worker of the lane is handed it.
fn bell_write(run: &RunState, host: usize, k: usize) -> OwnedFd {
    run.hosts[host]
        .start
        .lock()
        .as_ref()
        .expect("no host took its share")
        .lanes[k]
        .bell
        .write
        .try_clone()
        .unwrap()
}

/// Each lane has a wake pipe of its own (shm-channel.md §7.3 H3), all in the one queue the host's
/// reactor waits on: a byte into either lane's write end wakes the poller while it waits.
#[test]
fn a_host_of_two_lanes_wakes_on_either_lanes_pipe() {
    let lanes = lanes(2);
    let (run, _fds) = serve(&lanes, 1);
    let writes = [bell_write(&run, 0, 0), bell_write(&run, 0, 1)];
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        assert_eq!(host.owned.borrow().lanes.len(), 2);
        for (k, write) in writes.into_iter().enumerate() {
            // The poller runs to its wait, with nothing in flight.
            tokio::task::yield_now().await;
            let before = host.counts().events().pipe_wakes;
            let rung = PipeDoorbell::new(write).ring(&ShmWord::new(0));
            assert_eq!(rung.ok(), Some(HostBellOutcome::Rung));
            tokio::time::timeout(Duration::from_secs(10), async {
                while host.counts().events().pipe_wakes == before {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("the byte into lane {k}'s pipe did not wake the poller"));
        }
        run.stop.cancel();
        host.stopped().await;
    });
}

/// A wake pipe whose read fails ends the poller: the host fails with the read's error; a read
/// that left the pipe empty, or more in it, is what it left. (What a read of the pipe returns is
/// `mipyre_shm`'s `drain_once`, tested there on real pipes.)
#[test]
fn a_wake_pipe_whose_read_fails_fails_the_host_with_the_error() {
    for drain in [
        Drain {
            bytes: 3,
            outcome: DrainOutcome::Empty,
        },
        Drain {
            bytes: 256,
            outcome: DrainOutcome::MoreLeft,
        },
    ] {
        assert_eq!(bell_drained(LaneIndex(1), Ok(drain)).ok(), Some(drain));
    }
    let reset = SyscallError {
        call: mipyre_shm::Syscall::Read,
        errno: rustix::io::Errno::CONNRESET,
    };
    match bell_drained(LaneIndex(1), Err(reset)) {
        Err(PollerError::BellUnreadable {
            lane: LaneIndex(1),
            source,
        }) => assert_eq!(source, reset),
        other => panic!("expected the wake pipe's read failure, got {other:?}"),
    }
}

/// A start in which one host could not start fails with that host's cause, once every host
/// has reported; stopping the run then stops the host that did start (its poller ends).
#[test]
fn a_host_that_cannot_start_fails_the_run_start_and_the_stop_ends_the_started_host() {
    let lanes = lanes(2);
    let (run, _fds) = serve(&lanes, 2);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let started = LaneHost::start(&lanes, 0).unwrap().expect("host 0");
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
        assert!(!run.stop.is_cancelled());
        assert!(lanes
            .stop_run(&ShutdownDeadline::new(Duration::from_secs(5)))
            .is_empty());
        assert!(started.stop.is_cancelled(), "the started host was told to stop");
        Rc::clone(&started).stopped().await;
    });
    assert!(hosts_started([Ok(()), Ok(())]).is_ok());
}

/// Builds a step of a fork exchange that plays `turns` (a count moved, or the error the call
/// set) and records the offset each call started at.
fn scripted(
    turns: Vec<std::io::Result<usize>>,
) -> (impl FnMut(usize) -> std::io::Result<usize>, Rc<RefCell<Vec<usize>>>) {
    let offsets = Rc::new(RefCell::new(Vec::new()));
    let seen = Rc::clone(&offsets);
    let mut turns = turns.into_iter();
    let step = move |at| {
        seen.borrow_mut().push(at);
        turns
            .next()
            .expect("the exchange asked for no more turns than scripted")
    };
    (step, offsets)
}

/// A fork exchange carries on past an interrupt and a short transfer, from where it stopped,
/// until the whole message moved; any other error, or a call that moves nothing, ends it.
#[test]
fn a_fork_exchange_retries_an_interrupt_and_continues_a_short_transfer() {
    let eintr = || Err(std::io::Error::from_raw_os_error(libc::EINTR));
    let (step, offsets) = scripted(vec![eintr(), Ok(1), eintr(), Ok(3)]);
    assert!(transfer_all(4, step).is_ok());
    assert_eq!(*offsets.borrow(), vec![0, 0, 1, 1]);

    let (step, offsets) = scripted(vec![Ok(2), Err(std::io::Error::from_raw_os_error(libc::EPIPE))]);
    match transfer_all(4, step) {
        Err(TransferError::CallFailed(e)) => assert_eq!(e.raw_os_error(), Some(libc::EPIPE)),
        other => panic!("expected the EPIPE, got {other:?}"),
    }
    assert_eq!(*offsets.borrow(), vec![0, 2]);

    let (step, _) = scripted(vec![Ok(3), Ok(0)]);
    assert!(matches!(transfer_all(4, step), Err(TransferError::Closed)));
}

/// A finished fork task reads as finished even when the runtime's cooperative budget is spent
/// (it is, after a poller round of channel reads): the lane is not left waiting on it.
#[test]
fn a_finished_fork_task_reads_finished_with_the_coop_budget_spent() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        let mut task = tokio::spawn(async { 7 });
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        let (tx, mut rx) = mpsc::unbounded_channel();
        for n in 0..10_000 {
            tx.send(n).unwrap();
        }
        let mut read = 0;
        while rx.recv().now_or_never().is_some() {
            read += 1;
        }
        assert!(read < 10_000, "the budget was spent: a ready channel read as pending");
        assert!(
            (&mut task).now_or_never().is_none(),
            "with the budget spent, a constrained poll reads the finished task as pending"
        );
        assert_eq!(finished_now(&mut task).expect("finished").unwrap(), 7);
    });
}

/// The template setup's fds ride on its first byte: the fds call is made again after an
/// interrupt (it moved nothing, so carried none), the rest goes plain from where the first
/// call stopped, retried on an interrupt; a call that moves nothing of what is left is
/// WriteZero, any other error is itself.
#[test]
fn the_setup_sends_its_fds_with_its_first_byte_and_retries_an_interrupt() {
    let eintr = || Err(std::io::Error::from_raw_os_error(libc::EINTR));
    let (with_fds, fds_calls) = scripted(vec![eintr(), Ok(2)]);
    let mut with_fds = with_fds;
    let (plain, offsets) = scripted(vec![eintr(), Ok(1), Ok(3)]);
    assert!(send_attached(6, || with_fds(0), plain).is_ok());
    assert_eq!(fds_calls.borrow().len(), 2, "the fds call again after its interrupt");
    assert_eq!(*offsets.borrow(), vec![2, 2, 3]);

    let (mut with_fds, _) = scripted(vec![Ok(4)]);
    let (plain, _) = scripted(vec![Ok(0)]);
    let cut = send_attached(6, || with_fds(0), plain).unwrap_err();
    assert_eq!(cut.kind(), std::io::ErrorKind::WriteZero);

    let (mut with_fds, _) = scripted(vec![Err(std::io::Error::from_raw_os_error(libc::EPIPE))]);
    let (plain, offsets) = scripted(vec![]);
    let failed = send_attached(6, || with_fds(0), plain).unwrap_err();
    assert_eq!(failed.raw_os_error(), Some(libc::EPIPE));
    assert!(offsets.borrow().is_empty(), "nothing goes plain before the fds went");
}

/// Over a real socket pair the setup arrives whole, with its fds.
#[test]
fn the_setup_arrives_whole_with_its_fds_over_a_socket_pair() {
    use std::os::fd::FromRawFd;
    let (ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    let (pipe_read, pipe_write) = rustix::pipe::pipe().unwrap();
    let data = b"setup: the lanes".to_vec();
    send_with_fds(ours.as_fd(), &data, &[pipe_write.as_fd()]).unwrap();

    let mut buf = vec![0u8; data.len()];
    // SAFETY: `CMSG_SPACE` only computes a length from its argument.
    let space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) } as usize;
    let mut control = vec![0u8; space];
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };
    // SAFETY: zeroed msghdr, filled with buffers that outlive the call.
    let (got, passed) = unsafe {
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = space as _;
        let got = libc::recvmsg(theirs.as_raw_fd(), &mut msg, libc::MSG_WAITALL);
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        assert!(!cmsg.is_null(), "the fds came with the message");
        let fd = std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<RawFd>());
        (got, OwnedFd::from_raw_fd(fd))
    };
    assert_eq!(got, data.len() as isize);
    assert_eq!(buf, data);
    // The passed fd is the pipe's write end: a byte written through it reaches the read end.
    // SAFETY: valid fds and a one-byte buffer.
    assert_eq!(unsafe { libc::write(passed.as_raw_fd(), b"x".as_ptr().cast(), 1) }, 1);
    let mut byte = [0u8; 1];
    // SAFETY: `pipe_read` is open and `byte` is a writable one-byte buffer.
    assert_eq!(
        unsafe { libc::read(pipe_read.as_raw_fd(), byte.as_mut_ptr().cast(), 1) },
        1
    );
    assert_eq!(&byte, b"x");
}

/// Runs `n` calls of one part of a request's lane work on fixed input: building the request
/// (`job`), the host's encode of it into a slot (`encode_request`), the worker's decode of it from
/// its slot (`decode_request`), the worker's encode of [`bench_answer`] into a slot
/// (`encode_answer`; a request slot's writer, the same `SlotWriter` an answer's is) and the host's
/// decode of the answer it copied out (`decode_answer`). `benchmarks/micro/lane_submit/parts.sh`
/// divides the difference of the `LANE_PART_N`=2N and =N runs by N, so the parts of
/// `bench_on_host` can be told apart (`docs/design/shm-channel.md` §7). Run as a test without
/// `LANE_PART`, it runs each part 16 times.
#[test]
fn lane_parts_bench() {
    let n: u64 = std::env::var("LANE_PART_N").map_or(16, |v| v.parse().expect("LANE_PART_N: calls"));
    let parts = match std::env::var("LANE_PART") {
        Ok(part) => vec![part],
        Err(_) => [
            "job",
            "encode_request",
            "decode_request",
            "encode_answer",
            "decode_answer",
        ]
        .map(String::from)
        .to_vec(),
    };
    let geometry = lane_geometry(DEPTH, MIN_SLOT_BYTES).unwrap();
    let channel = Arc::new(Channel::new(MappedMemory::create(&geometry).unwrap().0));
    let mut host = HostEnd::new(Arc::clone(&channel), NoBell);
    let mut peer = PeerEnd::new(channel, NoBell, NoSleep);
    let answer = bench_answer();
    let (request, _reply) = job();
    // Request 0 goes round once: the worker holds it, and the host keeps a copy of its answer.
    let mut slot = host.request_slot().expect("an empty ring has room");
    slot.write(|w| wire::encode_request(w, &request.request)).unwrap();
    slot.publish().unwrap_or_else(|never| match never {});
    let Ok(Wait::Request(taken)) = peer.wait_for_request(SpinWindow::until(0), || 0, 1) else {
        panic!("request 0 was published");
    };
    let mut copied = BytesMut::new();
    {
        let mut ready = host.answers().unwrap();
        assert_eq!(ready.take_next(&mut copied), Ok(None));
    }
    let answer_bytes = wire::tests::written(MIN_SLOT_BYTES - 8, |w| wire::encode_response(w, &answer).unwrap()).1;
    // The slot request 1 will take: written into, never published.
    let mut slot = host.request_slot().expect("a ring of two has room for a second");
    for part in parts {
        for _ in 0..n {
            match part.as_str() {
                "job" => {
                    std::hint::black_box(job());
                }
                "encode_request" => {
                    slot.write(|w| wire::encode_request(w, std::hint::black_box(&request.request)))
                        .unwrap();
                }
                "decode_request" => {
                    let bytes = std::hint::black_box(&taken).bytes().unwrap();
                    std::hint::black_box(wire::decode_request(&bytes).unwrap());
                }
                "encode_answer" => {
                    slot.write(|w| wire::encode_response(w, std::hint::black_box(&answer)))
                        .unwrap();
                }
                "decode_answer" => {
                    std::hint::black_box(
                        wire::decode_answer(std::hint::black_box(&answer_bytes).clone())
                            .unwrap()
                            .unwrap(),
                    );
                }
                other => {
                    panic!("LANE_PART={other}: job, encode_request, decode_request, encode_answer or decode_answer")
                }
            }
        }
    }
}

// ───────────────────────── breaker (b/761 shm design, 7-layer check) ─────────────────────────

/// A worker that left a failed start report (an earlier attempt's, or one a C extension wrote
/// over the header) is followed by a replacement that starts and reports ready under its own
/// attempt. The fact a lane should keep: a ready replacement serves.
#[test]
fn breaker_a_start_failure_left_by_an_earlier_worker_breaks_a_ready_replacement() {
    let lanes = lanes(1);
    let (run, _fds) = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        // The earlier attempt's worker could not start, and said so.
        let earlier = lanes.take_attempt();
        host.with_lane(LaneIndex(0), |_, end| end.begin_attempt(earlier))
            .unwrap();
        peer(&run, 0).report_failed(earlier, b"forged traceback");
        // Its replacement forked and reported ready, as finish_respawns finds a successful fork.
        let attempt = lanes.take_attempt();
        host.with_lane(LaneIndex(0), |_, end| end.begin_attempt(attempt))
            .unwrap();
        peer(&run, 0).report_ready(attempt);
        let replacement = ended_worker(0, 4242);
        host.with_lane(LaneIndex(0), |lane, _| {
            let starting = Some(Replacement {
                since_ns: monotonic_ns(),
                attempt,
            });
            lane.worker = LaneWorker::Running {
                worker: replacement.tag(),
                starting,
                head: None,
            }
        })
        .unwrap();
        let watch = ProcessMonitor::new().unwrap();
        host.finish_respawns(&watch);
        let owned = host.owned.borrow();
        let state = owned.lanes[0].broken_why().map(ToString::to_string);
        assert!(
            state.is_none(),
            "a ready replacement's lane was broken by its predecessor's report: {state:?}"
        );
        assert!(
            matches!(owned.lanes[0].worker, LaneWorker::Running { starting: None, .. }),
            "it was taken as started"
        );
    });
}

/// A replacement that reports a failed start under its own attempt breaks its lane with its
/// traceback: another would fail the same way.
#[test]
fn a_replacement_that_reports_a_failed_start_breaks_its_lane_with_its_traceback() {
    let lanes = lanes(1);
    let (run, _fds) = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        let attempt = lanes.take_attempt();
        host.with_lane(LaneIndex(0), |lane, end| {
            end.begin_attempt(attempt);
            let starting = Some(Replacement {
                since_ns: monotonic_ns(),
                attempt,
            });
            lane.worker = LaneWorker::Running {
                worker: ended_worker(0, 4243).tag(),
                starting,
                head: None,
            };
        })
        .unwrap();
        peer(&run, 0).report_failed(attempt, b"Traceback: ImportError");
        host.finish_respawns(&ProcessMonitor::new().unwrap());
        assert!(matches!(
            host.owned.borrow().lanes[0].broken_why(),
            Some(LaneLossError::ReplacementNotStarted { failure }) if failure == "Traceback: ImportError"
        ));
    });
}

/// A worker that exits refusing its start breaks its lane for good: no replacement is forked
/// (here none could be: there is no template), and the lane says why.
#[test]
fn a_worker_that_refused_its_start_breaks_its_lane() {
    let lanes = lanes(1);
    let (_run, _fds) = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        let worker = ended_worker(0, 4244);
        host.with_lane(LaneIndex(0), |lane, _| {
            lane.worker = LaneWorker::Running {
                worker: worker.tag(),
                starting: None,
                head: None,
            }
        })
        .unwrap();
        host.lanes.exits.record(LaneIndex(0), worker.pid(), START_REFUSED << 8);
        host.worker_died(worker).await;
        assert!(matches!(
            host.owned.borrow().lanes[0].broken_why(),
            Some(LaneLossError::StartRefused { .. })
        ));
        assert_eq!(lanes.broken_lanes(), 1);
    });
}

/// The exit watch reports a worker as the worker it was handed, the same one the lane runs,
/// both one whose process ends while watched and one already gone when it was added.
#[test]
fn the_exit_watch_reports_each_worker_it_was_handed() {
    let mut child = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    let running = Arc::new(started_worker(0, Pid::new(child.id() as i32).unwrap()));
    let gone = ended_worker(1, 0);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let reported = rt.block_on(async {
        let watch = ProcessMonitor::new().unwrap();
        watch.add(Arc::clone(&running)).unwrap();
        watch.add(Arc::clone(&gone)).unwrap();
        let first = watch.exited().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        let mut later = Vec::new();
        while later.is_empty() {
            watch.readable().await.unwrap();
            later = watch.exited().unwrap();
        }
        (first, later)
    });
    let (first, later) = reported;
    assert!(matches!(&first[..], [w] if Arc::ptr_eq(w, &gone)), "{first:?}");
    assert!(matches!(&later[..], [w] if Arc::ptr_eq(w, &running)), "{later:?}");
}

/// The template answers a fork request with a pid; the server keeps it as the lane's worker
/// and later SIGKILLs it. The fact to keep: the server kills only processes it (through the
/// template) started. Here the "template" names an unrelated process.
#[test]
fn breaker_the_server_kills_whatever_pid_the_template_names() {
    use std::io::Write;
    let lanes = lanes(1);
    let (_run, _fds) = serve(&lanes, 1);
    let mut victim = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    let victim_pid = victim.id() as i32;
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    lanes
        .attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours))
        .unwrap();
    let answering = std::thread::spawn(move || {
        let (lane, attempt) = read_fork_request(&mut theirs);
        theirs.write_all(&fork_answer_bytes(lane, attempt, victim_pid)).unwrap();
        theirs
    });
    let answer = lanes.spawn_worker(LaneIndex(0), lanes.take_attempt(), FORK_ANSWER_TIMEOUT);
    let _theirs = answering.join().unwrap();
    let refused = matches!(
        &answer,
        Err(ForkError::NotTemplateChild { pid, parent, .. })
            if pid.as_raw() == victim_pid && *parent == std::process::id() as i32
    );
    if let Ok(worker) = answer {
        *lanes.processes[0].lock() = Some(Arc::new(worker));
    }
    lanes.stop_run(&ShutdownDeadline::new(Duration::from_secs(1)));
    // Ended by the test's own SIGTERM: a victim the server SIGKILLed was dead by then.
    rustix::process::kill_process(Pid::new(victim_pid).unwrap().into(), rustix::process::Signal::TERM).unwrap();
    let status = victim.wait().unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(
        status.signal(),
        Some(libc::SIGTERM),
        "pid {victim_pid}, never a child of the template, was SIGKILLed on the template's word"
    );
    assert!(
        refused,
        "the template's answer naming pid {victim_pid} was not refused as no child of it"
    );
}

/// A template that answers the pid of its own child has it taken as the lane's worker.
#[test]
fn the_server_takes_the_pid_of_a_child_of_the_template() {
    use std::io::Write;
    let lanes = lanes(1);
    let mut child = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    let child_pid = child.id() as i32;
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    // The test process plays the template: it started `child`.
    lanes
        .attach_template(Pid::new(std::process::id() as i32).unwrap(), OwnedFd::from(ours))
        .unwrap();
    let answering = std::thread::spawn(move || {
        let (lane, attempt) = read_fork_request(&mut theirs);
        theirs.write_all(&fork_answer_bytes(lane, attempt, child_pid)).unwrap();
        theirs
    });
    let answer = lanes
        .spawn_worker(LaneIndex(0), lanes.take_attempt(), FORK_ANSWER_TIMEOUT)
        .map(|w| w.pid().as_raw())
        .map_err(|e| e.to_string());
    let _theirs = answering.join().unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(answer, Ok(child_pid));
}

/// Reads a start frame as the template does (`decode_command` in template.py): its lane and its
/// attempt.
fn read_fork_request(theirs: &mut std::os::unix::net::UnixStream) -> (u32, u32) {
    use std::io::Read;
    let mut msg = [0u8; 13];
    theirs.read_exact(&mut msg).unwrap();
    let [n0, n1, n2, n3, tag, l0, l1, l2, l3, a0, a1, a2, a3] = msg;
    assert_eq!((u32::from_le_bytes([n0, n1, n2, n3]), tag), (9, 3), "a start frame");
    (
        u32::from_le_bytes([l0, l1, l2, l3]),
        u32::from_le_bytes([a0, a1, a2, a3]),
    )
}

/// A fork answer as the template writes it (`report_frame` in template.py): the request's lane
/// and attempt, then the pid it started.
fn fork_answer_bytes(lane: u32, attempt: u32, pid: i32) -> Vec<u8> {
    report_frame(&TemplateReport::Forked {
        lane: LaneIndex(lane as usize),
        attempt: Attempt::new(attempt),
        result: Ok(Pid::new(pid).unwrap()),
    })
}

/// The fork exchange has a deadline: a template that reads the request and never answers fails
/// it with a timeout once the deadline passes, while it still holds its socket open.
#[test]
fn breaker_a_template_that_never_answers_holds_the_fork_exchange_until_it_closes() {
    let lanes = lanes(1);
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    lanes
        .attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours))
        .unwrap();
    let (read_tx, read_rx) = std::sync::mpsc::channel();
    let (close_tx, close_rx) = std::sync::mpsc::channel::<()>();
    let template = std::thread::spawn(move || {
        read_fork_request(&mut theirs);
        read_tx.send(()).unwrap();
        // Holds the socket open, answering nothing, until the test lets go.
        close_rx
            .recv()
            .expect_err("the test lets go by dropping its end, sending nothing");
        drop(theirs);
    });
    let within = Duration::from_millis(1);
    let forking = Arc::clone(&lanes);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let fork = std::thread::spawn(move || {
        done_tx
            .send(forking.spawn_worker(LaneIndex(0), forking.take_attempt(), within))
            .unwrap()
    });
    read_rx.recv().unwrap();
    // A guard against a hang only: the exchange ends at its own deadline, long before this.
    let ended = done_rx.recv_timeout(Duration::from_secs(10));
    drop(close_tx);
    template.join().unwrap();
    fork.join().unwrap();
    assert!(
        matches!(ended, Ok(Err(ForkError::TimedOut { waited, .. })) if waited == within),
        "spawn_worker had no deadline: it ended only when the template closed ({ended:?})"
    );
}

/// An answer that comes after its request's deadline is told apart from the next request's by
/// the attempt it carries: the next fork request still gets its own worker, and the late worker,
/// whose spawn gave up, is killed rather than left running unknown. `late_lane` and `next_lane`
/// are the two requests' lanes.
fn a_late_fork_answer_is_read_before_the_next_request_gets_its_own(late_lane: u32, next_lane: u32) {
    use std::io::Write;
    use std::os::unix::process::ExitStatusExt;
    let lanes = lanes(2);
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    // The test process plays the template: it starts both children.
    lanes
        .attach_template(Pid::new(std::process::id() as i32).unwrap(), OwnedFd::from(ours))
        .unwrap();
    let within = Duration::from_millis(1);
    let mut late = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    let mut own = std::process::Command::new("sleep").arg("30").spawn().unwrap();

    let silent = lanes.spawn_worker(LaneIndex(late_lane as usize), lanes.take_attempt(), within);
    let (lane, late_attempt) = read_fork_request(&mut theirs);
    assert_eq!(lane, late_lane);
    assert!(matches!(silent, Err(ForkError::TimedOut { .. })), "{silent:?}");
    // The template answers the first request only now. It answers the next request once it has
    // read it, as the template does.
    theirs
        .write_all(&fork_answer_bytes(late_lane, late_attempt, late.id() as i32))
        .unwrap();
    let own_pid = own.id() as i32;
    let answering = std::thread::spawn(move || {
        let (lane, attempt) = read_fork_request(&mut theirs);
        theirs.write_all(&fork_answer_bytes(lane, attempt, own_pid)).unwrap();
        (lane, attempt, theirs)
    });
    let next = lanes
        .spawn_worker(LaneIndex(next_lane as usize), lanes.take_attempt(), FORK_ANSWER_TIMEOUT)
        .map(|w| w.pid().as_raw());
    let (lane, next_attempt, _theirs) = answering.join().unwrap();
    assert_eq!(lane, next_lane);
    assert_ne!(next_attempt, late_attempt);

    let late_status = late.wait().unwrap();
    own.kill().unwrap();
    own.wait().unwrap();
    assert_eq!(next.ok(), Some(own_pid));
    assert_eq!(late_status.signal(), Some(libc::SIGKILL));
}

#[test]
fn a_late_fork_answer_kills_its_worker_and_the_next_lane_gets_its_own() {
    a_late_fork_answer_is_read_before_the_next_request_gets_its_own(0, 1);
}

/// The lane alone cannot tell the two answers apart here: the attempt does.
#[test]
fn a_late_fork_answer_kills_its_worker_and_the_same_lane_asking_again_gets_its_own() {
    a_late_fork_answer_is_read_before_the_next_request_gets_its_own(0, 0);
}

/// Bytes of a golden case's `hex`.
fn golden_bytes(case: &serde_json::Value) -> Vec<u8> {
    let hex = case["hex"].as_str().unwrap();
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
        .collect()
}

/// The template socket's golden frames, shared with the Python side.
fn golden_frames() -> serde_json::Value {
    serde_json::from_str(include_str!("../../../../../tests/fixtures/template_frames.json")).unwrap()
}

fn golden_u32(case: &serde_json::Value, field: &str) -> u32 {
    u32::try_from(case[field].as_u64().unwrap()).unwrap()
}

fn golden_i32(case: &serde_json::Value, field: &str) -> i32 {
    i32::try_from(case[field].as_i64().unwrap()).unwrap()
}

/// The server reads the reports the template writes: the same file is checked against the
/// Python encoder (tests/test_process_lane_start_failures.py). A change here is a protocol
/// change: read the diff, then change both sides and the file together.
#[test]
fn the_server_reads_the_reports_the_template_writes() {
    let golden = golden_frames();
    assert_eq!(golden["max_frame_bytes"].as_u64(), Some(u64::from(MAX_FRAME_BYTES)));
    for case in golden["reports"].as_array().unwrap() {
        let lane = || LaneIndex(golden_u32(case, "lane") as usize);
        let pid = |field| Pid::new(golden_i32(case, field)).unwrap();
        let expected = match case["kind"].as_str().unwrap() {
            "forked" => TemplateReport::Forked {
                lane: lane(),
                attempt: Attempt::new(golden_u32(case, "attempt")),
                result: match case.get("errno") {
                    Some(_) => Err(rustix::io::Errno::from_raw_os_error(golden_i32(case, "errno"))),
                    None => Ok(pid("pid")),
                },
            },
            "exited" => TemplateReport::Exited {
                lane: lane(),
                pid: pid("pid"),
                status: golden_i32(case, "status"),
            },
            "import_failed" => TemplateReport::ImportFailed(case["traceback"].as_str().unwrap().into()),
            "lane_fds_missing" => TemplateReport::LaneFdsMissing {
                lane: lane(),
                got: golden_u32(case, "got"),
            },
            other => panic!("a report kind the server has no variant for: {other}"),
        };
        let bytes = golden_bytes(case);
        assert_eq!(
            crate::runtime::process::template::decode_template_report(&bytes, 8),
            Ok(Some((expected, bytes.len()))),
            "{}",
            case["what"]
        );
    }
}

/// The server writes the commands the template reads (the Python side checks the same file).
#[test]
fn the_server_writes_the_commands_the_template_reads() {
    let (shm, wake) = rustix::pipe::pipe().unwrap();
    for case in golden_frames()["commands"].as_array().unwrap() {
        let lane = || LaneIndex(golden_u32(case, "lane") as usize);
        let body;
        let command = match case["kind"].as_str().unwrap() {
            "setup" => {
                body = case["json"].as_str().unwrap().as_bytes().to_vec();
                TemplateCommand::Setup(&body)
            }
            "lane" => TemplateCommand::Lane {
                lane: lane(),
                fds: [shm.as_fd(), wake.as_fd()],
            },
            "start" => TemplateCommand::Start {
                lane: lane(),
                attempt: Attempt::new(golden_u32(case, "attempt")),
            },
            "run" => {
                body = case["json"].as_str().unwrap().as_bytes().to_vec();
                TemplateCommand::Run(&body)
            }
            other => panic!("a command kind the server has no variant for: {other}"),
        };
        assert_eq!(command.frame(), golden_bytes(case), "{}", case["what"]);
    }
}

/// A connection that ends inside a frame fails the fork waiting on it as closed, and every
/// later fork request fails the same way without being sent.
#[test]
fn a_template_that_closes_inside_a_frame_fails_this_fork_and_the_next() {
    use std::io::Write;
    let lanes = lanes(1);
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    lanes
        .attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours))
        .unwrap();
    let template = std::thread::spawn(move || {
        let (lane, attempt) = read_fork_request(&mut theirs);
        let answer = fork_answer_bytes(lane, attempt, 4242);
        theirs.write_all(&answer[..answer.len() / 2]).unwrap();
    });
    let first = lanes.spawn_worker(LaneIndex(0), lanes.take_attempt(), FORK_ANSWER_TIMEOUT);
    template.join().unwrap();
    assert!(matches!(first, Err(ForkError::TemplateClosed { .. })), "{first:?}");
    let second = lanes.spawn_worker(LaneIndex(0), lanes.take_attempt(), FORK_ANSWER_TIMEOUT);
    assert!(matches!(second, Err(ForkError::TemplateClosed { .. })), "{second:?}");
}

/// Bytes that are no frame end the connection: the fork fails with the frame error and the
/// first bytes the template sent.
#[test]
fn a_frame_with_an_unknown_tag_fails_the_fork_with_its_first_bytes() {
    use std::io::Write;
    let lanes = lanes(1);
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    lanes
        .attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours))
        .unwrap();
    let bad = [1u8, 0, 0, 0, 99];
    let template = std::thread::spawn(move || {
        read_fork_request(&mut theirs);
        theirs.write_all(&bad).unwrap();
        theirs
    });
    let fork = lanes.spawn_worker(LaneIndex(0), lanes.take_attempt(), FORK_ANSWER_TIMEOUT);
    let _theirs = template.join().unwrap();
    match fork {
        Err(ForkError::ProtocolViolated {
            end:
                LinkEnd::BadFrame {
                    error: crate::runtime::process::template::TemplateFrameError::UnknownTag(99),
                    prefix,
                },
            ..
        }) => assert_eq!(prefix, bad),
        other => panic!("expected the bad frame, got {other:?}"),
    }
}

/// A frame whose header says more than the limit ends the connection from the header alone:
/// the fork does not wait for a body that long.
#[test]
fn a_frame_past_the_length_limit_fails_the_fork_from_its_header() {
    use std::io::Write;
    let lanes = lanes(1);
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    lanes
        .attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours))
        .unwrap();
    let template = std::thread::spawn(move || {
        read_fork_request(&mut theirs);
        theirs.write_all(&(MAX_FRAME_BYTES + 1).to_le_bytes()).unwrap();
        theirs
    });
    let fork = lanes.spawn_worker(LaneIndex(0), lanes.take_attempt(), FORK_ANSWER_TIMEOUT);
    let _theirs = template.join().unwrap();
    assert!(
        matches!(
            fork,
            Err(ForkError::ProtocolViolated {
                end: LinkEnd::BadFrame {
                    error: crate::runtime::process::template::TemplateFrameError::TooLong { .. },
                    ..
                },
                ..
            })
        ),
        "{fork:?}"
    );
}

/// An exit report that comes while a fork waits for its answer is kept, and the fork still
/// gets its own worker.
#[test]
fn an_exit_report_before_the_fork_answer_is_kept_and_the_fork_gets_its_worker() {
    use std::io::Write;
    let lanes = lanes(2);
    let mut child = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    let child_pid = child.id() as i32;
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    lanes
        .attach_template(Pid::new(std::process::id() as i32).unwrap(), OwnedFd::from(ours))
        .unwrap();
    let dead = Pid::new(777).unwrap();
    let template = std::thread::spawn(move || {
        let (lane, attempt) = read_fork_request(&mut theirs);
        let exited = report_frame(&TemplateReport::Exited {
            lane: LaneIndex(1),
            pid: dead,
            status: 9,
        });
        theirs.write_all(&exited).unwrap();
        theirs.write_all(&fork_answer_bytes(lane, attempt, child_pid)).unwrap();
        theirs
    });
    let fork = lanes
        .spawn_worker(LaneIndex(0), lanes.take_attempt(), FORK_ANSWER_TIMEOUT)
        .map(|w| w.pid().as_raw())
        .map_err(|e| e.to_string());
    let _theirs = template.join().unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(fork, Ok(child_pid));
    assert_eq!(lanes.exits.status_of(LaneIndex(1), dead), Some(9));
    assert_eq!(
        lanes.exits.status_of(LaneIndex(1), Pid::new(778).unwrap()),
        None,
        "a report is read by pid"
    );
    assert_eq!(lanes.exits.status_of(LaneIndex(0), dead), None, "and by lane");
}

/// The fork exchange holds the socket while it waits; a host looking for a dead worker's exit
/// status never waits for it: it reads what the exchange took off the socket, within its own
/// wait, while the exchange is still waiting.
#[test]
fn a_host_reads_an_exit_status_while_a_fork_holds_the_socket() {
    use std::io::Write;
    let lanes = lanes(1);
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    lanes
        .attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours))
        .unwrap();
    let dead = Pid::new(777).unwrap();
    let (close_tx, close_rx) = std::sync::mpsc::channel::<()>();
    let template = std::thread::spawn(move || {
        read_fork_request(&mut theirs);
        let exited = report_frame(&TemplateReport::Exited {
            lane: LaneIndex(0),
            pid: dead,
            status: 9 << 8,
        });
        theirs.write_all(&exited).unwrap();
        // Answers nothing more, holding the socket open, until the test lets go.
        close_rx
            .recv()
            .expect_err("the test lets go by dropping its end, sending nothing");
    });
    let forking = Arc::clone(&lanes);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let fork = std::thread::spawn(move || {
        done_tx
            .send(forking.spawn_worker(LaneIndex(0), forking.take_attempt(), FORK_ANSWER_TIMEOUT))
            .unwrap()
    });
    let (_run, _fds) = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let (exit, still_waiting) = local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        // Until the exchange took the report: it is the socket's only reader meanwhile.
        while lanes.exits.status_of(LaneIndex(0), dead).is_none() {
            tokio::task::yield_now().await;
        }
        let exit = host.exit_status(LaneIndex(0), dead).await;
        (exit, done_rx.try_recv().is_err())
    });
    drop(close_tx);
    template.join().unwrap();
    fork.join().unwrap();
    assert!(matches!(exit, WorkerExit::Exited(9)), "{exit:?}");
    assert!(still_waiting, "the host's read waited for the fork exchange to end");
}

/// A template that did not get both fds of a lane says so and ends: the start fails with
/// which lane and how many came.
#[test]
fn a_lane_frame_short_of_its_fds_fails_the_start() {
    use std::io::Write;
    let lanes = lanes(1);
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    lanes
        .attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours))
        .unwrap();
    let missing = report_frame(&TemplateReport::LaneFdsMissing {
        lane: LaneIndex(0),
        got: 1,
    });
    theirs.write_all(&missing).unwrap();
    drop(theirs);
    match lanes.start_run(
        HostKind::Dedicated,
        ServerRunStop::default(),
        Arc::new(RunCounters::new(false)),
    ) {
        Err(LaneStartError::LaneFds {
            lane: LaneIndex(0),
            got: 1,
            want: 2,
        }) => {}
        other => panic!("expected the missing fds, got {other:?}"),
    }
    lanes.stop_run(&ShutdownDeadline::new(Duration::from_secs(5)));
}

/// A template whose app import failed (start="fork") says so with the traceback and ends: the
/// start fails with that traceback.
#[test]
fn a_template_import_failure_fails_the_start_with_its_traceback() {
    use std::io::Write;
    let lanes = lanes(1);
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    lanes
        .attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours))
        .unwrap();
    let failed = report_frame(&TemplateReport::ImportFailed("Traceback: import failed".into()));
    theirs.write_all(&failed).unwrap();
    drop(theirs);
    match lanes.start_run(
        HostKind::Dedicated,
        ServerRunStop::default(),
        Arc::new(RunCounters::new(false)),
    ) {
        Err(LaneStartError::TemplateFailed {
            lane: LaneIndex(0),
            failure,
            ..
        }) => {
            assert_eq!(failure, "Traceback: import failed");
        }
        other => panic!("expected the template's traceback, got {other:?}"),
    }
    lanes.stop_run(&ShutdownDeadline::new(Duration::from_secs(5)));
}

// ─────────────────────────── a worker that writes its counters wrong ───────────────────────────

/// Starts a process that stands in for lane `i`'s worker, so the host has a real process to kill.
fn stand_in_worker(lanes: &ServerLanes, i: usize) -> (std::process::Child, Arc<WorkerProcess>) {
    let child = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    let worker = run_as_worker(lanes, i, Pid::new(child.id() as i32).unwrap());
    (child, worker)
}

/// Publishes a request into lane `i` of `host` and returns its receiver.
fn publish_into(
    host: &LaneHost,
    i: usize,
) -> tokio::sync::oneshot::Receiver<Result<HandlerResponse, LaneRequestError>> {
    let (job, answer) = job();
    host.with_lane(LaneIndex(i), |lane, end| {
        let slot = end.request_slot().expect("the lane has room");
        if let Err((e, _)) = publish(&mut lane.waiting, slot, job, &mut ShellFx::new(host)) {
            panic!("request refused: {e:?}");
        }
    })
    .expect("the host owns the lane");
    answer
}

fn assert_peer_corrupted(
    answer: &mut tokio::sync::oneshot::Receiver<Result<HandlerResponse, LaneRequestError>>,
    want: PeerCounterError,
) {
    match answered(answer) {
        Err(LaneRequestError::Rejected(UnansweredError::PeerCorrupted {
            lane: LaneIndex(0),
            fault,
            ..
        })) => {
            assert_eq!(fault, PeerFault::Counter(want));
        }
        other => panic!("expected PeerCorrupted, got {other:?}"),
    }
}

/// A worker that steps its answer counter back one (to `read - 1`) would, unchecked, have the
/// host read 2^32 - 1 slots. Instead the host reads nothing of that lane, kills its worker,
/// answers its requests 502 (none run again) and counts the fault once; the other lane of the
/// same host is read as usual.
#[test]
fn an_answer_counter_stepped_back_kills_the_worker_and_fails_only_its_lane() {
    let lanes = lanes(2);
    let (mut faulty, faulty_worker) = stand_in_worker(&lanes, 0);
    let (mut fine, _) = stand_in_worker(&lanes, 1);
    let (run, fds) = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        let mut in_faulty = [publish_into(host, 0), publish_into(host, 0)];
        let mut in_fine = publish_into(host, 1);
        raw(&fds, 0, lanes.geometry).store(HeaderWord::RespHead, u32::MAX);
        answer_with(&mut peer(&run, 1), ok_answer);

        assert_eq!(host.collect(), 1, "only the other lane's answer is read");

        let fault = PeerCounterError::AnswersPastSent {
            read: Seq::new(0),
            sent: Seq::new(2),
            published: Seq::new(u32::MAX),
        };
        for answer in &mut in_faulty {
            assert_peer_corrupted(answer, fault);
        }
        assert_eq!(&answered(&mut in_fine).expect("the other lane answers").body[..], b"ok");
        {
            let owned = host.owned.borrow();
            assert!(matches!(&owned.lanes[0].worker, LaneWorker::Faulted { worker } if *worker == faulty_worker.tag()));
            assert!(matches!(owned.lanes[1].worker, LaneWorker::Running { .. }));
        }
        assert_eq!(window_of(host, 0), (0, 2), "the slots stay the worker's until it dies");
        host.collect();
        assert_eq!(lanes.stats.peer_faults.load(Relaxed), 1, "the fault is handled once");
        let (job, _answer) = job();
        assert!(host.submit(job).is_ok());
        assert_eq!(window_of(host, 0).1, 2, "no new request went into the faulted lane");
    });
    let status = faulty.wait().unwrap();
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(libc::SIGKILL)
    );
    fine.kill().unwrap();
    fine.wait().unwrap();
}

/// A 502 is what a request of a corrupted lane is answered, and its body never names the
/// worker.
#[test]
fn a_request_of_a_corrupted_lane_is_502() {
    let corrupted = LaneRequestError::Rejected(UnansweredError::PeerCorrupted {
        pid: 4242,
        lane: LaneIndex(0),
        fault: PeerFault::Counter(PeerCounterError::AnswersPastSent {
            read: Seq::new(0),
            sent: Seq::new(1),
            published: Seq::new(7),
        }),
    });
    let (status, body) = response_of(corrupted);
    assert_eq!(status, 502);
    assert!(!body.contains("4242"), "{body}");
}

/// Reports `worker` dead with exit status 9, as the template would.
async fn report_death(host: &Rc<LaneHost>, worker: Arc<WorkerProcess>) {
    host.lanes.exits.record(worker.lane(), worker.pid(), 9);
    host.worker_died(worker).await;
}

/// The death of a worker killed for a corrupt counter frees every slot of its lane, and the
/// shared counters say the next worker starts after them: none of its requests runs again.
#[test]
fn a_faulted_workers_death_frees_its_slots_and_the_next_worker_starts_after_them() {
    let lanes = lanes_backing_off(1, LONG_WAITS);
    let (mut faulty, worker) = stand_in_worker(&lanes, 0);
    let (_run, fds) = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        let mut answers = [publish_into(&host, 0), publish_into(&host, 0)];
        let h = raw(&fds, 0, lanes.geometry);
        h.store(HeaderWord::RespHead, 5);
        host.collect();
        faulty.wait().unwrap();
        report_death(&host, worker).await;

        let fault = PeerCounterError::AnswersPastSent {
            read: Seq::new(0),
            sent: Seq::new(2),
            published: Seq::new(5),
        };
        for answer in &mut answers {
            assert_peer_corrupted(answer, fault);
        }
        assert_eq!(window_of(&host, 0), (2, 2));
        assert!(host.owned.borrow().lanes[0].waiting.iter().all(Option::is_none));
        assert_eq!((h.load(HeaderWord::Taken), h.load(HeaderWord::RespHead)), (2, 2));
        host.shutdown().await;
    });
}

/// A worker that writes its lane's wake pipe past what its answers allow is at fault in that lane,
/// as one that writes a counter wrong is (b/947): one read of the pipe finds it, the host stops
/// listening to the pipe, kills the worker and answers its requests 502. Once the lane runs its
/// next worker the host listens again, after reading what the pipe kept, and holds the new worker
/// to its own bytes. The other lane of the host is heard in the same look and read as before.
#[test]
fn a_worker_flooding_its_wake_pipe_is_faulted_and_only_its_lane_fails() {
    let lanes = lanes_backing_off(2, LONG_WAITS);
    let (mut flooding, flooding_worker) = stand_in_worker(&lanes, 0);
    let (mut fine, _) = stand_in_worker(&lanes, 1);
    let (run, _fds) = serve(&lanes, 1);
    let (flood, honest) = (bell_write(&run, 0, 0), bell_write(&run, 0, 1));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        let mut in_flooded = [publish_into(&host, 0), publish_into(&host, 0)];
        let mut in_fine = publish_into(&host, 1);
        let mut filled = 0;
        while let Ok(n) = rustix::io::write(&flood, &[1u8; 65536]) {
            filled += n;
        }
        assert!(filled > 256, "the pipe took {filled} bytes");
        answer_with(&mut peer(&run, 1), ok_answer);
        assert_eq!(PipeDoorbell::new(honest).ring(&ShmWord::new(0)).ok(), Some(HostBellOutcome::Rung));

        assert_eq!(host.hear_bells().ok(), Some(2), "both pipes hold bytes");
        assert_eq!(*host.muted.borrow(), [0]);
        assert_eq!(host.hear_bells().ok(), Some(0), "the flooded pipe is muted, the other one was read empty");
        assert_eq!(host.collect(), 1, "only the other lane's answer is read");
        let fault = PeerFault::Wakes(WakeFloodError { bytes: 256, room: u64::from(DEPTH) });
        for answer in &mut in_flooded {
            match answered(answer) {
                Err(LaneRequestError::Rejected(UnansweredError::PeerCorrupted { lane: LaneIndex(0), fault: f, .. })) => assert_eq!(f, fault),
                other => panic!("expected PeerCorrupted, got {other:?}"),
            }
        }
        assert_eq!(&answered(&mut in_fine).expect("the other lane answers").body[..], b"ok");
        assert!(matches!(&host.owned.borrow().lanes[0].worker, LaneWorker::Faulted { worker } if *worker == flooding_worker.tag()));
        assert_eq!(lanes.stats.peer_faults.load(Relaxed), 1);
        assert_eq!(host.owned.borrow().ends[1].fault(), None);

        flooding.wait().unwrap();
        report_death(&host, flooding_worker).await;
        assert!(matches!(host.owned.borrow().lanes[0].worker, LaneWorker::Forking { .. }), "the lane forks its next worker");
        assert!(host.listen_again().is_ok());
        assert_eq!(*host.muted.borrow(), [0], "no pipe is listened to before the next worker runs");

        let next = ended_worker(0, 1);
        host.with_lane(LaneIndex(0), |lane, _| {
            lane.worker = LaneWorker::Running { worker: next.tag(), starting: None, head: None };
        });
        assert!(host.listen_again().is_ok());
        assert!(host.muted.borrow().is_empty());
        assert_eq!(host.hear_bells().ok(), Some(0), "what the last worker left was read before listening");
        assert_eq!(rustix::io::write(&flood, &[1u8; DEPTH as usize]), Ok(DEPTH as usize));
        assert_eq!(host.hear_bells().ok(), Some(1));
        assert!(host.muted.borrow().is_empty(), "a byte per slot is the next worker's room");
        assert_eq!(rustix::io::write(&flood, &[1u8]), Ok(1));
        assert_eq!(host.hear_bells().ok(), Some(1));
        assert_eq!(*host.muted.borrow(), [0], "one byte past it is the next worker's fault");
        host.shutdown().await;
    });
    fine.kill().unwrap();
    fine.wait().unwrap();
}

/// A worker that died leaving `taken` where no worker can leave it might have run any of its
/// requests: every one in its slots is answered 502 and none is handed to the next worker.
#[test]
fn a_dead_worker_with_taken_out_of_range_fails_every_request_it_held() {
    let lanes = lanes_backing_off(1, LONG_WAITS);
    let (mut gone, worker) = stand_in_worker(&lanes, 0);
    gone.kill().unwrap();
    gone.wait().unwrap();
    let (_run, fds) = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        let mut answers = [publish_into(&host, 0), publish_into(&host, 0)];
        let h = raw(&fds, 0, lanes.geometry);
        h.store(HeaderWord::Taken, 2);
        report_death(&host, worker).await;

        let fault = PeerCounterError::TakenOutOfRange {
            read: Seq::new(0),
            sent: Seq::new(2),
            published: Seq::new(0),
            taken: Seq::new(2),
        };
        for answer in &mut answers {
            assert_peer_corrupted(answer, fault);
        }
        assert_eq!(lanes.stats.peer_faults.load(Relaxed), 1);
        assert_eq!(window_of(&host, 0).0, 2);
        assert_eq!((h.load(HeaderWord::Taken), h.load(HeaderWord::RespHead)), (2, 2));
        host.shutdown().await;
    });
}

/// The lane timeout is the host's own measure: a worker that says it took nothing (`taken` at
/// the host's reading) is still killed once the host has seen the request at the head of its
/// lane past the timeout.
#[test]
fn a_handler_is_timed_by_the_host_whatever_its_worker_writes() {
    let lanes = lanes_configured(1, |host| HostConfig {
        timeout: Duration::from_nanos(1),
        ..host
    });
    let (mut stuck, worker) = stand_in_worker(&lanes, 0);
    let (_run, fds) = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        let _answer = publish_into(host, 0);
        raw(&fds, 0, lanes.geometry).store(HeaderWord::Taken, 0);
        host.check_timeouts();
        let seen = match host.owned.borrow().lanes[0].worker {
            LaneWorker::Running { head, .. } => head,
            _ => None,
        };
        let Some(HeadOfLine { seq: 0, since_ns }) = seen else {
            panic!("the host did not see the request at the head: {seen:?}");
        };
        while monotonic_ns() <= since_ns + 1 {
            std::hint::spin_loop();
        }
        host.check_timeouts();
        assert!(
            matches!(&host.owned.borrow().lanes[0].worker, LaneWorker::Overrun { worker: w, .. } if *w == worker.tag())
        );
        assert_eq!(lanes.stats.timeouts_killed.load(Relaxed), 1);
    });
    let status = stuck.wait().unwrap();
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(libc::SIGKILL)
    );
}

/// Breaking a lane while its replacement is still being forked kills the worker that fork
/// makes, which would otherwise run on with no lane and no host knowing its pid.
#[test]
fn breaking_a_lane_mid_fork_kills_the_worker_the_fork_makes() {
    let lanes = lanes(1);
    let (_run, _fds) = serve(&lanes, 1);
    let mut forked = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    let pid = Pid::new(forked.id() as i32).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        let (answer_fork, fork_answered) = std::sync::mpsc::channel::<()>();
        let task = tokio::task::spawn_blocking(move || {
            fork_answered.recv().expect("the test answers the fork");
            Ok(WorkerCandidate::new(started_worker(0, pid)))
        });
        let attempt = lanes.take_attempt();
        host.forks.borrow_mut().insert(LaneIndex(0), task);
        host.with_lane(LaneIndex(0), |lane, _| {
            lane.worker = LaneWorker::Forking {
                since_ns: monotonic_ns(),
                attempt,
            }
        })
        .expect("the host owns the lane");
        host.break_lane(LaneIndex(0), fork_failed("gone"));
        answer_fork.send(()).unwrap();
        while forked.try_wait().unwrap().is_none() {
            tokio::task::yield_now().await;
        }
        host.shutdown().await;
    });
    let status = forked.wait().unwrap();
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(libc::SIGKILL)
    );
}

/// Closing a host while its lane's replacement is being forked kills the worker that fork makes
/// once it answers, after the host is gone (b/940): its poller no longer runs, so the worker would
/// never reach the lane's process, which the run's stop kills.
#[test]
fn closing_a_host_mid_fork_kills_the_worker_the_fork_makes_after_the_close() {
    let lanes = lanes(1);
    let (_run, _fds) = serve(&lanes, 1);
    let mut forked = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    let pid = Pid::new(forked.id() as i32).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        let (answer_fork, fork_answered) = std::sync::mpsc::channel::<()>();
        let task = tokio::task::spawn_blocking(move || {
            fork_answered.recv().expect("the test answers the fork");
            Ok(WorkerCandidate::new(started_worker(0, pid)))
        });
        let attempt = lanes.take_attempt();
        host.forks.borrow_mut().insert(LaneIndex(0), task);
        host.with_lane(LaneIndex(0), |lane, _| {
            lane.worker = LaneWorker::Forking {
                since_ns: monotonic_ns(),
                attempt,
            }
        })
        .expect("the host owns the lane");
        Rc::clone(&host).shutdown().await;
        assert!(host.forks.borrow().is_empty(), "the closed host still waits for a fork");
        assert!(matches!(host.owned.borrow().lanes[0].worker, LaneWorker::Gone));
        answer_fork.send(()).unwrap();
    });
    let status = forked.wait().unwrap();
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(libc::SIGKILL)
    );
    assert!(
        lanes.processes[0].lock().is_none(),
        "the dropped fork's worker became the lane's"
    );
}

/// A corrupt answer counter seen while the lane's replacement is being forked waits for the
/// replacement to run, and is then that worker's fault: killed, its requests answered 502.
#[test]
fn a_corrupt_counter_seen_mid_fork_is_the_replacements_fault_once_it_runs() {
    let lanes = lanes(1);
    let (mut replacement, worker) = stand_in_worker(&lanes, 0);
    let (_run, fds) = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        let mut answer = publish_into(host, 0);
        let pid = worker.pid();
        let task = tokio::task::spawn_blocking(move || Ok(WorkerCandidate::new(started_worker(0, pid))));
        let attempt = lanes.take_attempt();
        host.forks.borrow_mut().insert(LaneIndex(0), task);
        host.with_lane(LaneIndex(0), |lane, _| {
            lane.worker = LaneWorker::Forking {
                since_ns: monotonic_ns(),
                attempt,
            }
        })
        .expect("the host owns the lane");
        raw(&fds, 0, lanes.geometry).store(HeaderWord::RespHead, 7);
        assert_eq!(host.collect(), 0);
        assert!(
            answer.try_recv().is_err(),
            "a request was answered before the worker ran"
        );
        assert_eq!(lanes.stats.peer_faults.load(Relaxed), 0);

        host.with_lane(LaneIndex(0), |lane, _| {
            lane.worker = LaneWorker::Running {
                worker: worker.tag(),
                starting: None,
                head: None,
            }
        })
        .expect("the host owns the lane");
        host.collect();
        assert_peer_corrupted(
            &mut answer,
            PeerCounterError::AnswersPastSent {
                read: Seq::new(0),
                sent: Seq::new(1),
                published: Seq::new(7),
            },
        );
    });
    let status = replacement.wait().unwrap();
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(libc::SIGKILL)
    );
}

/// With no worker of its own (here waiting out a crash loop) a lane's counters are the host's:
/// a write that does not fit is put back, counted once, and answers nothing.
#[test]
fn a_counter_written_while_the_lane_has_no_worker_is_put_back() {
    let lanes = lanes(1);
    let (_run, fds) = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        host.with_lane(LaneIndex(0), |lane, _| lane.worker = LaneWorker::CrashWait)
            .expect("the host owns the lane");
        let h = raw(&fds, 0, lanes.geometry);
        h.store(HeaderWord::RespHead, 9);
        assert_eq!(host.collect(), 0);
        assert_eq!((h.load(HeaderWord::Taken), h.load(HeaderWord::RespHead)), (0, 0));
        assert_eq!(host.collect(), 0);
        assert_eq!(lanes.stats.peer_faults.load(Relaxed), 1);
    });
}

/// A crash-looping lane is listed by its own lanes, with why, until it recovers; another set of
/// lanes in the process lists nothing.
#[test]
fn a_crash_loop_is_listed_by_its_own_lanes_until_it_recovers() {
    let first = lanes(2);
    let second = lanes(2);
    first.set_crash_loop(1, Some("crash-looping: its last 3 workers died".to_string()));
    assert_eq!(
        first.crash_looping(),
        [(1, "crash-looping: its last 3 workers died".to_string())]
    );
    assert!(second.crash_looping().is_empty());
    first.set_crash_loop(1, None);
    assert!(first.crash_looping().is_empty());
}
