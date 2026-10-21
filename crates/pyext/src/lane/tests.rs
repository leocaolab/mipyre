//! Tests of the process-lane host, on lanes with no workers behind them: the fixture makes the
//! lanes, starts a run that hosts them, and runs the host on a thread of its own, so a host
//! that spins shows as a test that times out instead of hanging the test binary.

use super::*;
use crate::lane::template::tests::report_frame;
use crate::lane::template::{send_attached, send_with_fds, transfer_all, Attempt, TransferError, MAX_FRAME_BYTES};
use mipyre_tpc::ShutdownDeadline;
use crate::exchange::RequestHead;
use mipyre_http::io::request_id::{ClientRequestId, RequestId};
use mipyre_http::plan::route::RouteKey;

const DEPTH: u32 = 2;

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
fn lanes_queueing(
    workers: usize,
    queue_per_worker: usize,
    on_failure: FailurePolicy,
) -> Arc<ServerLanes> {
    let geometry = shm::Geometry::new(
        std::num::NonZeroUsize::new(workers).expect("a test asks for at least one lane"),
        mipyre_shm::plan::ring::RingDepth::new(DEPTH).expect("DEPTH is a power of two"),
        shm::SlotBytes::new(shm::MIN_SLOT_BYTES).expect("MIN_SLOT_BYTES is the minimum"),
    )
    .unwrap();
    let host = HostConfig::new(
        geometry,
        Duration::from_secs(5),
        queue_per_worker,
        Duration::ZERO,
        on_failure,
    )
    .unwrap();
    Arc::new(ServerLanes::create(geometry, host).unwrap())
}

/// Puts the lanes in a run that serves them with `hosts` hosts (what `start_run` does, minus
/// the forking). Host `h` hosts the lanes `j % hosts == h`, as `LaneHost::start` gives them.
fn serve(lanes: &ServerLanes, hosts: usize) -> Arc<RunState> {
    let n = lanes.workers();
    let links = (0..hosts)
        .map(|h| {
            let (inbox, receiver) = mpsc::unbounded_channel();
            HostLink {
                inbox,
                receiver: Mutex::new(Some(receiver)),
                load: AtomicUsize::new(0),
                usable_lanes: AtomicUsize::new((0..n).filter(|j| j % hosts == h).count()),
                failure: OnceLock::new(),
                crash_wait: Mutex::new(None),
            }
        })
        .collect();
    let run = Arc::new(RunState {
        hosts: links,
        stop: CancellationToken::new(),
        server_stop: ServerRunStop::default(),
    });
    *lanes.run.write() = RunPhase::Serving(Arc::clone(&run));
    run
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
    let run = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        host.break_lane(0, fork_failed("injected"));
        let mut answers = Vec::new();
        for n in 0..2 {
            let (job, answer) = job();
            if let Err((why, _)) = lanes.submit(job) {
                panic!("request {n} was refused with lane 1 usable: {why:?}");
            }
            answers.push(answer);
        }
        let owned = host.owned.borrow();
        assert_eq!(owned[0].load(), 0, "the broken lane took a request");
        assert_eq!(owned[1].load(), 2);
    });
    assert_eq!(lanes.broken_lanes(), 1);
    assert!(!run.server_stop.is_stopped(), "one usable lane is left");
}

/// A host whose lanes are all broken takes no request from its own thread: the request goes to
/// the other host, which still has a usable lane, and it is counted as an overflow.
#[test]
fn a_host_with_only_broken_lanes_is_passed_over_for_the_other_host() {
    let lanes = lanes(2);
    let run = serve(&lanes, 2);
    on_host(&lanes, 0, |host| {
        host.break_lane(0, fork_failed("injected"));
        assert!(
            !run.hosts[0].serving(),
            "a host with no usable lane still serves"
        );
        assert!(run.hosts[1].serving());
        let (job, _answer) = job();
        if let Err((why, _)) = lanes.submit(job) {
            panic!("refused with host 1 serving: {why:?}");
        }
        assert_eq!(lanes.stats.overflow_forwards.load(Relaxed), 1);
    });
}

/// A host with no usable lane refuses with its own variant, carrying why its lane broke; with no
/// other host to take the request, that refusal is what the request gets, not WorkersDied.
#[test]
fn no_usable_lane_is_its_own_rejection_with_the_lane_cause() {
    let lanes = lanes(1);
    let _run = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        host.break_lane(0, fork_failed("boom"));
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
    let run = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        host.break_lane(0, fork_failed("boom0"));
        assert!(!run.server_stop.is_stopped(), "one lane of two is left");
        host.break_lane(1, fork_failed("boom1"));
        assert!(run.server_stop.is_stopped(), "no lane is left");
    });
    let fatal = lanes
        .fatal()
        .expect("the lanes recorded why the server stops")
        .to_string();
    assert!(fatal.contains("boom0"), "{fatal}");
    assert_eq!(lanes.broken_lanes(), 2);
}

/// Builds the host failure the poller reports when lane 0's wake pipe cannot be read.
fn wake_pipe_failed() -> PollerError {
    PollerError::HostBellUnreadable {
        lane: LaneIndex(0),
        source: std::io::Error::other("bad fd"),
    }
}

/// A host whose poller failed records why, with the lane and the OS's error, and is no longer
/// one requests are sent to.
#[test]
fn a_failed_host_records_its_failure_and_stops_serving() {
    let lanes = lanes(2);
    let run = serve(&lanes, 2);
    on_host(&lanes, 0, |host| host.fail(wake_pipe_failed()));
    let why = run.hosts[0]
        .failure
        .get()
        .expect("the host's failure")
        .to_string();
    assert_eq!(
        why,
        "process lane host 0 failed: the wake pipe of lane 0 cannot be read: bad fd"
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
    let run = serve(&lanes, 2);
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
    let run = serve(&lanes, 2);
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
    let run = serve(&lanes, 1);
    on_host(&lanes, 0, |host| host.fail(wake_pipe_failed()));
    assert!(run.server_stop.is_stopped());
    let fatal = lanes
        .fatal()
        .expect("the lanes recorded why the server stops");
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
    let run = serve(&lanes, 2);
    on_host(&lanes, 0, |host| host.fail(wake_pipe_failed()));
    assert!(run.server_stop.is_stopped());
    assert!(lanes.fatal().is_some());
}


/// Builds a look, at `now`, at a starting worker that is alive, not ready and left nothing.
fn look_at(now: Instant) -> WorkerStartSnapshot {
    WorkerStartSnapshot {
        ready_pid: NO_WORKER as u32,
        start_failure: None,
        reaped: None,
        gone_since: None,
        now,
    }
}

/// A starting worker is ready once it wrote its own pid; one that left a start failure fails
/// the start with it, even after it ended.
#[test]
fn a_starting_worker_is_ready_by_its_pid_and_fails_by_its_start_failure() {
    let pid = Pid::new(4242).unwrap();
    let t = Instant::now();
    let deadline = t + Duration::from_secs(30);
    let ready = WorkerStartSnapshot {
        ready_pid: 4242,
        ..look_at(t)
    };
    assert!(matches!(
        starting_worker(0, pid, ready, deadline),
        StartWait::Ready
    ));
    let other = WorkerStartSnapshot {
        ready_pid: 4243,
        ..look_at(t)
    };
    assert!(matches!(
        starting_worker(0, pid, other, deadline),
        StartWait::Waiting
    ));
    let failed = WorkerStartSnapshot {
        start_failure: Some("Traceback: ImportError".into()),
        reaped: Some(70 << 8),
        gone_since: Some(t),
        ..look_at(t)
    };
    match starting_worker(1, pid, failed, deadline) {
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
        match starting_worker(2, pid, ended, deadline) {
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
        starting_worker(0, pid, at(since + EXIT_STATUS_WAIT), deadline),
        StartWait::Waiting
    ));
    match starting_worker(
        0,
        pid,
        at(since + EXIT_STATUS_WAIT + Duration::from_nanos(1)),
        deadline,
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
        starting_worker(0, pid, look_at(deadline), deadline),
        StartWait::Waiting
    ));
    assert!(matches!(
        starting_worker(0, pid, look_at(deadline + Duration::from_nanos(1)), deadline),
        StartWait::Failed(LaneStartError::NotReady { lane: LaneIndex(0), pid: p }) if p == pid
    ));
}

/// A run that cannot fork its workers says so with the typed fork error, by lane.
#[test]
fn a_run_with_no_template_fails_its_start_with_the_typed_fork_error() {
    let lanes = lanes(1);
    match lanes.start_run(HostKind::Dedicated, ServerRunStop::default()) {
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
    let _run = serve(&lanes, 1);
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
    let mut child = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    let pid = Pid::new(child.id() as i32).unwrap();
    lanes.pids[0].store(Some(pid));
    let _run = serve(&lanes, 2);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        // Lane 1 is host 1's.
        host.worker_died(1, pid).await;
        // Lane 0 runs `pid`, not this one.
        host.worker_died(0, Pid::new(i32::MAX).unwrap()).await;
        assert_eq!(lanes.stats.deaths.load(Relaxed), 0);
        assert!(matches!(
            host.owned.borrow()[0].worker,
            LaneWorker::Running { pid: running, .. } if running == pid
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
    let mut child = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    let pid = Pid::new(child.id() as i32).unwrap();
    lanes.pids[0].store(Some(pid));
    let run = serve(&lanes, 1);
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
            host.owned.borrow()[0].broken_why(),
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
    let plain = lanes(workers);
    let geometry = plain.memory.region.geometry();
    let host = HostConfig {
        crash_backoff: backoff,
        ..plain.host
    };
    Arc::new(ServerLanes::create(geometry, host).unwrap())
}

/// Reports lane `i`'s worker `pid` dead with exit status 3, as the template would. No worker
/// ever ran, so the lane is first set to run `pid`, as a respawn leaves it.
async fn crash(host: &Rc<LaneHost>, i: usize, pid: i32) {
    let pid = Pid::new(pid).unwrap();
    host.with_lane(i, |lane| {
        lane.worker = LaneWorker::Running {
            pid,
            starting_since: None,
            head: None,
        }
    })
    .expect("the host owns the lane");
    host.lanes.exits.record(LaneIndex(i), pid, 3 << 8);
    host.worker_died(i, pid).await;
}

/// The third consecutive crash of a lane makes it wait before its next fork: the requests it
/// held are refused 503 with the facts (not run by the next worker), new ones go to the host's
/// other lane, and with every lane waiting a request is refused with when to retry, from the
/// host's thread and from a thread hosting none.
#[test]
fn a_lane_crashing_three_times_in_a_row_waits_and_the_other_lane_serves() {
    let lanes = lanes_backing_off(2, LONG_WAITS);
    let run = serve(&lanes, 1);
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
            matches!(host.owned.borrow()[0].worker, LaneWorker::Forking { .. }),
            "the second crash forks at once"
        );
        assert_eq!(run.hosts[0].usable_lanes.load(Relaxed), 2);
        // A request waiting in lane 0 when its third worker dies.
        let (held, mut held_answer) = job();
        if let Err((why, _)) = host.submit(held) {
            panic!("refused before the lane waits: {why:?}");
        }
        assert_eq!(host.owned.borrow()[0].load(), 1);
        crash(&host, 0, 4003).await;
        assert!(host.owned.borrow()[0].waiting());
        assert_eq!(run.hosts[0].usable_lanes.load(Relaxed), 1);
        match held_answer.try_recv() {
            Ok(Err(LaneRequestError::Rejected(UnansweredError::LaneCrashLooping {
                lane: LaneIndex(0),
                crashes: 3,
                exit,
                retry_in,
            }))) => {
                assert_eq!(exit.to_string(), "exited with status 3");
                assert!(retry_in > Duration::ZERO && retry_in <= LONG_WAITS.first, "{retry_in:?}");
            }
            other => panic!("expected the crash-loop refusal, got {other:?}"),
        }
        let h = lanes.memory.region.header(0);
        assert_eq!(
            h.taken.0.load(SeqCst),
            h.req_head.0.load(SeqCst),
            "the refused request is taken out of the ring: the next worker never runs it"
        );
        // The other lane takes new requests.
        let (next, _next_answer) = job();
        if let Err((why, _)) = host.submit(next) {
            panic!("refused with lane 1 serving: {why:?}");
        }
        assert_eq!(host.owned.borrow()[1].load(), 1);
        assert_eq!(host.owned.borrow()[0].load(), 0);
        // Lane 1 crash-loops too (its request is refused with it): nothing takes requests.
        for pid in [4011, 4012, 4013] {
            crash(&host, 1, pid).await;
        }
        assert!(!run.hosts[0].serving());
        let (job0, _a) = job();
        match lanes.submit(job0) {
            Err((LaneRequestError::Rejected(UnansweredError::LaneCrashLooping { lane: LaneIndex(0), crashes: 3, .. }), _)) => {}
            other => panic!("expected lane 0's wait, back first, got {:?}", other.map_err(|(e, _)| e)),
        }
        let off_host = Arc::clone(&lanes);
        let refused = std::thread::spawn(move || {
            let (job, _a) = job();
            off_host.submit(job).map_err(|(e, _)| e)
        })
        .join()
        .unwrap();
        assert!(
            matches!(refused, Err(LaneRequestError::Rejected(UnansweredError::LaneCrashLooping { .. }))),
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
    let run = serve(&lanes, 1);
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
        host.break_lane(0, fork_failed("injected"));
        assert_eq!(run.hosts[0].usable_lanes.load(Relaxed), 1);
        assert!(run.hosts[0].crash_wait.lock().is_none(), "a broken lane waits for nothing");
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
/// with the worker's side done in place.
fn bench_requests_on_host(requests: u64) {
    let lanes = lanes(1);
    let _run = serve(&lanes, 1);
    let answer = bench_answer();
    on_host(&lanes, 0, |_host| {
        let host = HOST.with(|h| h.borrow().clone()).expect("this thread hosts the lanes");
        let h = lanes.memory.region.header(0);
        for seq in 0..requests {
            let seq = seq as u32;
            let (job, reply) = job();
            if let Err((why, _)) = lanes.submit(job) {
                panic!("refused: {why:?}");
            }
            h.taken.0.store(seq.wrapping_add(1), SeqCst);
            let slot = lanes.memory.region.resp_slot(0, seq);
            // SAFETY: request `seq` is taken and unanswered: its answer slot is the worker's,
            // and this loop is the worker.
            unsafe { lanes.memory.region.write_slot(slot, |w| wire::encode_response(w, &answer)) }
                .expect("the answer fits");
            h.resp_head.0.store(seq.wrapping_add(1), SeqCst);
            host.collect();
            std::hint::black_box(reply);
        }
    });
}

/// Runs `requests` requests from a thread that hosts no lane: `submit` hands each to the
/// host's inbox, and the host only takes it off without running it.
fn bench_requests_off_host(requests: u64) {
    let lanes = lanes(1);
    let run = serve(&lanes, 1);
    let mut inbox = run.hosts[0].receiver.lock().take().expect("no host took the inbox");
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
    let run = serve(&lanes, 1);
    let answer = bench_answer();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        let h = lanes.memory.region.header(0);
        for seq in 0..requests {
            let seq = seq as u32;
            let (job, reply) = job();
            if let Err((why, _)) = lanes.submit(job) {
                panic!("refused: {why:?}");
            }
            h.taken.0.store(seq.wrapping_add(1), SeqCst);
            let slot = lanes.memory.region.resp_slot(0, seq);
            // SAFETY: as in `bench_requests_on_host`.
            unsafe { lanes.memory.region.write_slot(slot, |w| wire::encode_response(w, &answer)) }
                .expect("the answer fits");
            h.resp_head.0.store(seq.wrapping_add(1), SeqCst);
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

/// Plays lane `lane`'s worker for the request at `seq`: takes it and publishes the encoded
/// message `answer` as its answer.
fn worker_answers(lanes: &ServerLanes, lane: usize, seq: u32, answer: &[u8]) {
    let region = lanes.memory.region;
    let h = region.header(lane);
    h.taken.0.store(seq.wrapping_add(1), SeqCst);
    // SAFETY: the test is the lane's worker; the host reads the slot only after `resp_head`.
    unsafe {
        region.write_slot(region.resp_slot(lane, seq), |w| w.put(answer))
    }
    .expect("the answer fits a slot");
    h.resp_head.0.store(seq.wrapping_add(1), SeqCst);
}

/// Encodes a worker's answer `200 ok`.
fn ok_answer() -> Vec<u8> {
    let mut buf = vec![0; 256];
    let mut w = wire::MessageWriter::new(&mut buf);
    wire::encode_answer(
        &mut w,
        &Ok(HandlerResponse::new(
            hyper::StatusCode::OK,
            bytes::Bytes::from_static(b"ok"),
            hyper::header::HeaderValue::from_static("text/plain"),
            mipyre_http::plan::response::HeaderLines::new(),
        )),
    )
    .expect("a short answer fits");
    let n = w.len() as usize;
    buf.truncate(n);
    buf
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
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
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
    let room = shm::MIN_SLOT_BYTES - 4;
    assert!(fits(room, room).is_ok());
    assert!(fits(0, room).is_ok());
    match fits(room + 1, room) {
        Err(TooLarge { needed, room: r }) => assert_eq!((needed, r), (room + 1, room)),
        Ok(()) => panic!("a message one byte past the slot fits"),
    }
}

/// A request bigger than a slot is answered 413 without taking a slot or a place in the queue,
/// and the next request goes into the lane as if it had not come.
#[test]
fn a_too_large_request_is_413_and_takes_nothing_from_the_lane() {
    let lanes = lanes(1);
    let _run = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        let room = lanes.memory.region.geometry().max_message();
        let (big, _answer) = job_with_body(bytes::Bytes::from(vec![b'x'; room]));
        match lanes.submit(big) {
            Err((LaneRequestError::TooLarge(TooLarge { needed, room: r }), _)) => {
                assert!(needed > room, "{needed} <= {room}");
                assert_eq!(r, room);
            }
            other => panic!("expected TooLarge, got {:?}", other.map_err(|(e, _)| e)),
        }
        assert_eq!(host.owned.borrow()[0].load(), 0, "the refused request holds a place");
        let (next, _answer) = job();
        if let Err((why, _)) = lanes.submit(next) {
            panic!("the request after a too-large one was refused: {why:?}");
        }
        let owned = host.owned.borrow();
        assert_eq!((owned[0].head, owned[0].load()), (1, 1));
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
            ran: Duration::from_millis(3),
            exit: WorkerExit::Exited(3),
        })
    };
    assert!(matches!(
        died().into_handler_error(),
        HandlerError::Refused(UnansweredError::WorkerDied { pid: 42, lane: LaneIndex(0), .. })
    ));
    assert_eq!(status_of(died()), 502);

    let too_large = || LaneRequestError::TooLarge(TooLarge { needed: 9, room: 8 });
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
    let _run = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        let (first, mut first_answer) = job();
        if let Err((why, _)) = lanes.submit(first) {
            panic!("refused: {why:?}");
        }
        worker_answers(&lanes, 0, 0, &[9]);
        assert!(host.collect() > 0);
        match answered(&mut first_answer) {
            Err(LaneRequestError::Malformed(LaneDecodeError::UnknownTag { tag: 9, .. })) => {}
            other => panic!("expected the answer to be malformed, got {other:?}"),
        }
        assert_eq!(host.owned.borrow()[0].cursor, 1);
        assert_eq!(host.owned.borrow()[0].load(), 0);

        let mut answers = Vec::new();
        for n in 0..DEPTH {
            let (next, answer) = job();
            if let Err((why, _)) = lanes.submit(next) {
                panic!("request {n} after the malformed answer was refused: {why:?}");
            }
            answers.push(answer);
        }
        {
            let owned = host.owned.borrow();
            assert!(owned[0].queued.is_empty(), "a slot is still held by the bad answer");
            assert_eq!(owned[0].head, 1 + DEPTH);
        }
        for (seq, answer) in (1..).zip(answers.iter_mut()) {
            worker_answers(&lanes, 0, seq, &ok_answer());
            host.collect();
            let resp = answered(answer).expect("a good answer after the bad one");
            assert_eq!(&resp.body[..], b"ok");
        }
    });
}

/// Builds a lane for [`LocalLane::pass_dead_run`] with `seqs` awaited requests published from
/// `cursor` on and `queued` more behind them, and returns their receivers in order.
fn busy_lane(
    cursor: u32,
    seqs: u32,
    queued: usize,
) -> (
    LocalLane,
    Vec<tokio::sync::oneshot::Receiver<Result<HandlerResponse, LaneRequestError>>>,
) {
    let mut lane = LocalLane {
        index: 0,
        head: cursor.wrapping_add(seqs),
        cursor,
        waiting: (0..DEPTH).map(|_| None).collect(),
        queued: VecDeque::new(),
        worker: LaneWorker::Gone,
    };
    let mut answers = Vec::new();
    for k in 0..seqs {
        let (job, answer) = job();
        lane.waiting[(cursor.wrapping_add(k) % DEPTH) as usize] = Some(job.reply);
        answers.push(answer);
    }
    for _ in 0..queued {
        let (job, answer) = job();
        lane.queued.push_back(job);
        answers.push(answer);
    }
    (lane, answers)
}

/// A worker that dies having answered all it took leaves the lane as it was. One that dies
/// running a request gives up only that request's reply; the requests it never took (in their
/// slots and queued) stay awaited, and the cursor moves past the one it ran, where the next worker
/// starts. Also across the sequence's wrap.
#[test]
fn a_dead_worker_gives_up_only_the_request_it_ran_and_the_rest_wait_for_the_next() {
    let (mut lane, _answers) = busy_lane(0, DEPTH, 1);
    let answered_all = ring::exit_split(&lane.window(), Seq::new(0), Seq::new(0)).unwrap();
    assert!(lane.pass_dead_run(&answered_all, DEPTH).is_none());
    assert_eq!(lane.cursor, 0);
    assert!(lane.waiting.iter().all(Option::is_some));
    assert_eq!(lane.load(), DEPTH as usize + 1);

    for cursor in [0, u32::MAX] {
        let (mut lane, mut answers) = busy_lane(cursor, DEPTH, 1);
        let taken = cursor.wrapping_add(1);
        let died_running = ring::exit_split(&lane.window(), Seq::new(cursor), Seq::new(taken)).unwrap();
        let Some(Some(reply)) = lane.pass_dead_run(&died_running, DEPTH) else {
            panic!("the request the worker ran was not given up");
        };
        // The reply given up is the first request's, the one the worker took.
        answer_if_awaited(reply, Err(LaneRequestError::TooLarge(TooLarge { needed: 1, room: 0 })));
        assert!(answered(&mut answers[0]).is_err());
        assert_eq!(lane.cursor, taken);
        assert!(lane.waiting[(taken % DEPTH) as usize].is_some(), "an untaken request was dropped");
        assert_eq!(lane.queued.len(), 1);
        assert_eq!(lane.load(), DEPTH as usize);
        for answer in &mut answers[1..] {
            assert!(answer.try_recv().is_err(), "a request not taken was answered");
        }
    }
}

/// Breaking a lane answers every request it holds — in its slots and queued behind them — with
/// LaneLost naming the lane and its cause; none is left waiting, and the lane holds none.
#[test]
fn breaking_a_lane_answers_every_request_it_holds_lane_lost() {
    let lanes = lanes_queueing(1, DEPTH as usize + 1, FailurePolicy::Isolate);
    let _run = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        let mut answers = Vec::new();
        for n in 0..=DEPTH {
            let (job, answer) = job();
            if let Err((why, _)) = lanes.submit(job) {
                panic!("request {n} was refused: {why:?}");
            }
            answers.push(answer);
        }
        assert_eq!(host.owned.borrow()[0].queued.len(), 1, "one request queued behind the slots");
        host.break_lane(0, fork_failed("boom"));
        for (n, answer) in answers.iter_mut().enumerate() {
            match answered(answer) {
                Err(LaneRequestError::Rejected(UnansweredError::LaneLost { lane: LaneIndex(0), cause })) => {
                    assert!(cause.contains("boom"), "request {n}: {cause}");
                }
                other => panic!("request {n}: expected LaneLost, got {other:?}"),
            }
        }
        let owned = host.owned.borrow();
        assert!(owned[0].waiting.iter().all(Option::is_none));
        assert!(owned[0].queued.is_empty());
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
/// It spins the server's and the worker's own non-inlined functions
/// ([`LaneHost::spin_on_lanes`], [`shm::spin_for_change`]), so the rounds are the same machine
/// code.
///
/// Run as a test without `BENCH_SPIN` (both sides, 50 µs each), it checks that a spin on lanes
/// nobody answers ends with nothing found, whatever the time.
#[test]
fn spin_rounds_bench() {
    let us: u64 =
        std::env::var("BENCH_US").map_or(50, |v| v.parse().expect("BENCH_US: microseconds"));
    let sides = match std::env::var("BENCH_SPIN") {
        Ok(side) => vec![side],
        Err(_) => vec!["host".to_string(), "worker".to_string()],
    };
    for side in sides {
        let window = SpinWindow::until(shm::monotonic_ns() + us * 1_000);
        let spun = match side.as_str() {
            "host" => {
                let lanes = lanes(1);
                let _run = serve(&lanes, 1);
                on_host(&lanes, 0, |host| host.spin_on_lanes(window))
            }
            "worker" => {
                let counter = std::sync::atomic::AtomicU32::new(0);
                shm::spin_for_change(&counter, 0, window)
            }
            other => panic!("BENCH_SPIN={other}: host or worker"),
        };
        assert!(
            !spun.ready,
            "{side}: nothing was published, nothing is found"
        );
        // With no time there is no round; the setup is the same either way.
        let rounds = if us == 0 { 0 } else { spun.rounds };
        println!("ROUNDS={rounds}");
    }
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
    let _run = serve(&lanes, 1);
    let response = Ok(HandlerResponse::new(
        hyper::StatusCode::OK,
        bytes::Bytes::from_static(
            br#"{"id":1,"name":"mipyre","tags":["a","b","c"],"price":12.5,"ok":true}"#,
        ),
        hyper::header::HeaderValue::from_static("application/json"),
        mipyre_http::plan::response::HeaderLines::new(),
    ));
    on_host(&lanes, 0, |host| {
        let region = &lanes.memory.region;
        let h = region.header(0);
        for seq in 0..n {
            let (job, mut answer) = job();
            assert!(host.submit(job).is_ok(), "the lane takes request {seq}");
            // SAFETY: the host published `seq` and waits for its answer; nothing else touches
            // the two slots.
            unsafe {
                let req = region
                    .read_slot(region.req_slot(0, seq))
                    .and_then(wire::decode_request);
                assert!(req.is_ok(), "request {seq} decodes");
                let refused = region.write_slot(region.resp_slot(0, seq), |w| {
                    wire::encode_answer(w, &response)
                });
                assert!(refused.is_ok(), "answer {seq} fits");
            }
            h.taken.0.store(seq.wrapping_add(1), SeqCst);
            h.resp_head.0.store(seq.wrapping_add(1), SeqCst);
            host.collect();
            assert!(
                matches!(answer.try_recv(), Ok(Ok(_))),
                "request {seq} answered"
            );
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

/// A wake pipe that cannot be waited on is the host's failure, naming the lane and the OS
/// error; a ready pipe is its guard.
#[test]
fn a_wake_pipe_that_cannot_be_waited_on_fails_the_host_naming_its_lane() {
    assert!(matches!(woken_pipe(3, Ok("guard")), Ok("guard")));
    let ebadf = std::io::Error::from_raw_os_error(libc::EBADF);
    match woken_pipe::<()>(3, Err(ebadf)) {
        Err(PollerError::HostBellUnwatchable { lane: LaneIndex(3), source }) => {
            assert_eq!(source.raw_os_error(), Some(libc::EBADF));
        }
        other => panic!("expected the wake pipe's failure, got {other:?}"),
    }
}

/// A read that filled the buffer, or was interrupted, may have left bytes; one byte short of
/// full, EAGAIN and end of file leave none; any other error is the pipe's and comes back.
#[test]
fn a_wake_read_says_more_is_left_only_when_it_filled_the_buffer_or_was_interrupted() {
    use rustix::io::Errno;
    let cap = WAKE_READ_BYTES;
    assert_eq!(drain_outcome(Ok(cap), cap), Ok(DrainOutcome::MoreLeft));
    assert_eq!(drain_outcome(Ok(cap - 1), cap), Ok(DrainOutcome::Empty));
    assert_eq!(drain_outcome(Ok(1), cap), Ok(DrainOutcome::Empty));
    assert_eq!(drain_outcome(Ok(0), cap), Ok(DrainOutcome::Empty));
    assert_eq!(drain_outcome(Ok(1), 1), Ok(DrainOutcome::MoreLeft));
    assert_eq!(drain_outcome(Err(Errno::INTR), cap), Ok(DrainOutcome::MoreLeft));
    assert_eq!(drain_outcome(Err(Errno::AGAIN), cap), Ok(DrainOutcome::Empty));
    assert_eq!(drain_outcome(Err(Errno::BADF), cap), Err(Errno::BADF));
    assert_eq!(drain_outcome(Err(Errno::IO), cap), Err(Errno::IO));
}

/// A wake pipe's end that cannot be read gives its errno back, not an empty pipe.
#[test]
fn a_drain_of_an_end_that_cannot_be_read_returns_its_errno() {
    let (_read, write) = make_wake_channel().unwrap();
    assert_eq!(drain_once(write.as_fd()), Err(rustix::io::Errno::BADF));
}

/// A wake pipe that turns readable and then fails its read ends the poller: the host is failed
/// with the lane and the OS's error, and stops serving. The lane's pipe is a TCP stream reset by
/// its peer, which the reactor reports readable and whose read fails with ECONNRESET.
#[test]
fn a_wake_pipe_whose_read_fails_fails_the_host_naming_its_lane_and_the_error() {
    let mut lanes = lanes(1);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let stream = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    stream.set_nonblocking(true).unwrap();
    let (peer, _) = listener.accept().unwrap();
    rustix::net::sockopt::set_socket_linger(&peer, Some(Duration::ZERO)).unwrap();
    drop(peer);
    Arc::get_mut(&mut lanes).expect("no other owner yet").wake_reads[0] = stream.into();
    let run = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        host.stopped().await;
    });
    let failure = run.hosts[0].failure.get().expect("the host's failure");
    match &failure.cause {
        PollerError::HostBellUnreadable { lane: LaneIndex(0), source } => {
            assert_eq!(source.raw_os_error(), Some(libc::ECONNRESET));
        }
        other => panic!("expected lane 0's wake pipe read failure, got {other:?}"),
    }
    assert!(!run.hosts[0].serving());
    assert_eq!(lanes.stats.host_failures.load(Relaxed), 1);
}

/// One drain of a wake pipe holding more than one read's worth takes exactly one read's worth
/// and says more is left, however much the peer wrote.
#[test]
fn a_drain_of_a_wake_pipe_reads_once_however_much_was_written() {
    let (read, write) = make_wake_channel().unwrap();
    let written = rustix::io::write(&write, &[1u8; 3 * WAKE_READ_BYTES]).unwrap();
    assert_eq!(written, 3 * WAKE_READ_BYTES);
    assert_eq!(drain_once(read.as_fd()), Ok(DrainOutcome::MoreLeft));
    assert_eq!(drain_once(read.as_fd()), Ok(DrainOutcome::MoreLeft));
    assert_eq!(drain_once(read.as_fd()), Ok(DrainOutcome::MoreLeft));
    assert_eq!(drain_once(read.as_fd()), Ok(DrainOutcome::Empty));
    rustix::io::write(&write, &[1u8]).unwrap();
    assert_eq!(drain_once(read.as_fd()), Ok(DrainOutcome::Empty));
    drop(write);
    assert_eq!(drain_once(read.as_fd()), Ok(DrainOutcome::Empty));
}

/// A start in which one host could not start fails with that host's cause, once every host
/// has reported; stopping the run then stops the host that did start (its poller ends).
#[test]
fn a_host_that_cannot_start_fails_the_run_start_and_the_stop_ends_the_started_host() {
    let lanes = lanes(2);
    let run = serve(&lanes, 2);
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
        assert!(lanes.stop_run(&ShutdownDeadline::new(Duration::from_secs(5))).is_empty());
        assert!(started.stop.is_cancelled(), "the started host was told to stop");
        Rc::clone(&started).stopped().await;
    });
    assert!(hosts_started([Ok(()), Ok(())]).is_ok());
}

/// Builds a step of a fork exchange that plays `turns` (a count moved, or the error the call
/// set) and records the offset each call started at.
fn scripted(
    turns: Vec<std::io::Result<usize>>,
) -> (
    impl FnMut(usize) -> std::io::Result<usize>,
    Rc<RefCell<Vec<usize>>>,
) {
    let offsets = Rc::new(RefCell::new(Vec::new()));
    let seen = Rc::clone(&offsets);
    let mut turns = turns.into_iter();
    let step = move |at| {
        seen.borrow_mut().push(at);
        turns.next().expect("the exchange asked for no more turns than scripted")
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
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
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
    assert_eq!(unsafe { libc::read(pipe_read.as_raw_fd(), byte.as_mut_ptr().cast(), 1) }, 1);
    assert_eq!(&byte, b"x");
}

/// Runs `n` calls of one part of a request's lane work on fixed input: building the request
/// (`job`), the host's encode of it (`encode_request`), the worker's decode (`decode_request`),
/// the worker's encode of [`bench_answer`] (`encode_answer`) and the host's read of it
/// (`decode_answer`). `benchmarks/micro/lane_submit/parts.sh` divides the difference of the
/// `LANE_PART_N`=2N and =N runs by N, so the parts of `bench_on_host` can be told apart
/// (`docs/design/shm-channel.md` §7). Run as a test without `LANE_PART`, it runs each part 16
/// times.
#[test]
fn lane_parts_bench() {
    let n: u64 = std::env::var("LANE_PART_N").map_or(16, |v| v.parse().expect("LANE_PART_N: calls"));
    let parts = match std::env::var("LANE_PART") {
        Ok(part) => vec![part],
        Err(_) => ["job", "encode_request", "decode_request", "encode_answer", "decode_answer"]
            .map(String::from)
            .to_vec(),
    };
    let lanes = lanes(1);
    let _run = serve(&lanes, 1);
    let region = &lanes.memory.region;
    let answer = bench_answer();
    let (request, _reply) = job();
    let (req_slot, resp_slot) = (region.req_slot(0, 0), region.resp_slot(0, 0));
    // SAFETY: no host or worker runs on these lanes; the test owns both slots of request 0.
    unsafe {
        region.write_slot(req_slot, |w| wire::encode_request(w, &request.request)).unwrap();
        region.write_slot(resp_slot, |w| wire::encode_response(w, &answer)).unwrap();
    }
    for part in parts {
        for _ in 0..n {
            match part.as_str() {
                "job" => {
                    std::hint::black_box(job());
                }
                // SAFETY (this arm and the next three): as above.
                "encode_request" => unsafe {
                    region
                        .write_slot(req_slot, |w| wire::encode_request(w, std::hint::black_box(&request.request)))
                        .unwrap();
                },
                "decode_request" => {
                    let bytes = unsafe { region.read_slot(std::hint::black_box(req_slot)) }.unwrap();
                    std::hint::black_box(wire::decode_request(bytes).unwrap());
                }
                "encode_answer" => unsafe {
                    region
                        .write_slot(resp_slot, |w| wire::encode_response(w, std::hint::black_box(&answer)))
                        .unwrap();
                },
                "decode_answer" => {
                    let bytes = unsafe { region.read_slot(std::hint::black_box(resp_slot)) }.unwrap();
                    std::hint::black_box(wire::decode_answer(bytes).unwrap().unwrap());
                }
                other => panic!("LANE_PART={other}: job, encode_request, decode_request, encode_answer or decode_answer"),
            }
        }
    }
}

// ───────────────────────── breaker (b/761 shm design, 7-layer check) ─────────────────────────

/// A worker that served the lane (the ring is in use) and then scribbled a start failure into
/// its mapping, as a C extension writing over the header can, dies. Its replacement forks fine
/// and has already reported ready. The fact a lane should keep: a ready replacement serves.
#[test]
fn breaker_a_start_failure_left_by_an_earlier_worker_breaks_a_ready_replacement() {
    let lanes = lanes(1);
    let _run = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        // The ring is in use: one request went round.
        let (first, mut first_answer) = job();
        if let Err((why, _)) = lanes.submit(first) {
            panic!("refused: {why:?}");
        }
        worker_answers(&lanes, 0, 0, &ok_answer());
        host.collect();
        assert!(answered(&mut first_answer).is_ok());
        // The serving worker writes bytes of its choosing into response slot 0 and the flag.
        let region = lanes.memory.region;
        let h = region.header(0);
        // SAFETY: the test plays the lane's worker; no answer is pending in slot 0.
        unsafe { region.write_slot(region.resp_slot(0, 0), |w| w.put(b"forged traceback")) }
            .expect("fits");
        h.start_failed.0.store(1, SeqCst);
        // It died (worker_died clears ready_pid, not start_failed); its replacement forked and
        // is ready, as finish_respawns leaves a successful fork.
        let replacement = Pid::new(4242).unwrap();
        host.with_lane(0, |lane| {
            lane.worker = LaneWorker::Running { pid: replacement, starting_since: Some(Instant::now()), head: None }
        })
        .unwrap();
        h.ready_pid.0.store(replacement.as_raw() as u32, SeqCst);
        let watch = ProcessMonitor::new().unwrap();
        host.finish_respawns(&watch);
        let state = host.owned.borrow()[0].broken_why().map(ToString::to_string);
        assert!(
            state.is_none(),
            "a ready replacement's lane was broken by its predecessor's flag: {state:?}"
        );
    });
}

/// A worker that wrote its pid as ready started, even with a failure note in its header; one
/// not yet ready fails by the note, and with neither it is still starting.
#[test]
fn a_ready_pid_beats_a_start_failure_note() {
    let pid = Pid::new(4242).unwrap();
    let note = || Some("Traceback: ImportError".to_owned());
    assert_eq!(start_report(pid, 4242, note()), StartReport::Ready);
    assert_eq!(
        start_report(pid, 4243, note()),
        StartReport::Failed { failure: "Traceback: ImportError".to_owned() }
    );
    assert_eq!(start_report(pid, 4243, None), StartReport::Pending);
    assert_eq!(start_report(pid, NO_WORKER as u32, None), StartReport::Pending);
}

/// A worker's death ends its start attempt: a failure note it left (or wrote over) is gone
/// before its replacement is forked, so the replacement's start reads only its own.
#[test]
fn a_worker_death_clears_the_start_failure_note_it_left() {
    let lanes = lanes(1);
    let mut child = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    let pid = Pid::new(child.id() as i32).unwrap();
    lanes.pids[0].store(Some(pid));
    let _run = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        let region = lanes.memory.region;
        let h = region.header(0);
        // SAFETY: the test plays the lane's worker; no answer is pending in slot 0.
        unsafe { region.write_slot(region.resp_slot(0, 0), |w| w.put(b"forged traceback")) }
            .expect("fits");
        h.start_failed.0.store(1, SeqCst);
        child.kill().unwrap();
        child.wait().unwrap();
        // The template's report of the exit, so the death is not waited on.
        lanes.exits.record(LaneIndex(0), pid, 9);
        host.worker_died(0, pid).await;
        assert_eq!(region.start_failure(0), None);
    });
}

/// The template answers a fork request with a pid; the server keeps it as the lane's worker
/// and later SIGKILLs it. The fact to keep: the server kills only processes it (through the
/// template) started. Here the "template" names an unrelated process.
#[test]
fn breaker_the_server_kills_whatever_pid_the_template_names() {
    use std::io::Write;
    let lanes = lanes(1);
    let _run = serve(&lanes, 1);
    let mut victim = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    let victim_pid = victim.id() as i32;
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    lanes.attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours)).unwrap();
    let answering = std::thread::spawn(move || {
        let (lane, attempt) = read_fork_request(&mut theirs);
        theirs.write_all(&fork_answer_bytes(lane, attempt, victim_pid)).unwrap();
        theirs
    });
    let answer = lanes.spawn_worker(0, FORK_ANSWER_TIMEOUT);
    let _theirs = answering.join().unwrap();
    let refused = matches!(
        &answer,
        Err(ForkError::NotTemplateChild { pid, parent, .. })
            if pid.as_raw() == victim_pid && *parent == std::process::id() as i32
    );
    if let Ok(pid) = answer {
        lanes.pids[0].store(Some(pid));
    }
    lanes.stop_run(&ShutdownDeadline::new(Duration::from_secs(1)));
    // Ended by the test's own SIGTERM: a victim the server SIGKILLed was dead by then.
    rustix::process::kill_process(Pid::new(victim_pid).unwrap().into(), rustix::process::Signal::TERM)
        .unwrap();
    let status = victim.wait().unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(
        status.signal(),
        Some(libc::SIGTERM),
        "pid {victim_pid}, never a child of the template, was SIGKILLed on the template's word"
    );
    assert!(refused, "the template's answer naming pid {victim_pid} was not refused as no child of it");
}

/// A pid the template answered is its worker only when the template is its parent; one with no
/// process names nothing a kill could hit, and is taken.
#[test]
fn a_forked_pid_is_taken_only_as_a_child_of_the_template() {
    let template = Pid::new(100).unwrap();
    let pid = Pid::new(4242).unwrap();
    assert_eq!(template_child(template, pid, Some(100)).ok(), Some(pid));
    assert_eq!(template_child(template, pid, None).ok(), Some(pid));
    assert!(matches!(
        template_child(template, pid, Some(101)),
        Err(ForkError::NotTemplateChild { parent: 101, pid: p, template: t }) if p == pid && t == template
    ));
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
    lanes.attach_template(Pid::new(std::process::id() as i32).unwrap(), OwnedFd::from(ours)).unwrap();
    let answering = std::thread::spawn(move || {
        let (lane, attempt) = read_fork_request(&mut theirs);
        theirs.write_all(&fork_answer_bytes(lane, attempt, child_pid)).unwrap();
        theirs
    });
    let answer = lanes.spawn_worker(0, FORK_ANSWER_TIMEOUT).map(Pid::as_raw).map_err(|e| e.to_string());
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
    (u32::from_le_bytes([l0, l1, l2, l3]), u32::from_le_bytes([a0, a1, a2, a3]))
}

/// A fork answer as the template writes it (`report_frame` in template.py): the request's lane
/// and attempt, then the pid it started.
fn fork_answer_bytes(lane: u32, attempt: u32, pid: i32) -> Vec<u8> {
    report_frame(&TemplateReport::Forked {
        lane: LaneIndex(lane as usize),
        attempt: Attempt(attempt),
        result: Ok(Pid::new(pid).unwrap()),
    })
}

/// The fork exchange has a deadline: a template that reads the request and never answers fails
/// it with a timeout once the deadline passes, while it still holds its socket open.
#[test]
fn breaker_a_template_that_never_answers_holds_the_fork_exchange_until_it_closes() {
    let lanes = lanes(1);
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    lanes.attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours)).unwrap();
    let (read_tx, read_rx) = std::sync::mpsc::channel();
    let (close_tx, close_rx) = std::sync::mpsc::channel::<()>();
    let template = std::thread::spawn(move || {
        read_fork_request(&mut theirs);
        read_tx.send(()).unwrap();
        // Holds the socket open, answering nothing, until the test lets go.
        close_rx.recv().expect_err("the test lets go by dropping its end, sending nothing");
        drop(theirs);
    });
    let within = Duration::from_millis(1);
    let forking = Arc::clone(&lanes);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let fork = std::thread::spawn(move || done_tx.send(forking.spawn_worker(0, within)).unwrap());
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
    lanes.attach_template(Pid::new(std::process::id() as i32).unwrap(), OwnedFd::from(ours)).unwrap();
    let within = Duration::from_millis(1);
    let mut late = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    let mut own = std::process::Command::new("sleep").arg("30").spawn().unwrap();

    let silent = lanes.spawn_worker(late_lane as usize, within);
    let (lane, late_attempt) = read_fork_request(&mut theirs);
    assert_eq!(lane, late_lane);
    assert!(matches!(silent, Err(ForkError::TimedOut { .. })), "{silent:?}");
    // The template answers the first request only now. It answers the next request once it has
    // read it, as the template does.
    theirs.write_all(&fork_answer_bytes(late_lane, late_attempt, late.id() as i32)).unwrap();
    let own_pid = own.id() as i32;
    let answering = std::thread::spawn(move || {
        let (lane, attempt) = read_fork_request(&mut theirs);
        theirs.write_all(&fork_answer_bytes(lane, attempt, own_pid)).unwrap();
        (lane, attempt, theirs)
    });
    let next = lanes.spawn_worker(next_lane as usize, FORK_ANSWER_TIMEOUT).map(Pid::as_raw);
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
    (0..hex.len()).step_by(2).map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap()).collect()
}

/// The template socket's golden frames, shared with the Python side.
fn golden_frames() -> serde_json::Value {
    serde_json::from_str(include_str!("../../../../tests/fixtures/template_frames.json")).unwrap()
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
                attempt: Attempt(golden_u32(case, "attempt")),
                result: match case.get("errno") {
                    Some(_) => Err(rustix::io::Errno::from_raw_os_error(golden_i32(case, "errno"))),
                    None => Ok(pid("pid")),
                },
            },
            "exited" => TemplateReport::Exited { lane: lane(), pid: pid("pid"), status: golden_i32(case, "status") },
            "import_failed" => TemplateReport::ImportFailed(case["traceback"].as_str().unwrap().into()),
            "lane_fds_missing" => TemplateReport::LaneFdsMissing { lane: lane(), got: golden_u32(case, "got") },
            other => panic!("a report kind the server has no variant for: {other}"),
        };
        let bytes = golden_bytes(case);
        assert_eq!(
            crate::lane::template::decode_template_report(&bytes, 8),
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
        let setup;
        let command = match case["kind"].as_str().unwrap() {
            "setup" => {
                setup = case["json"].as_str().unwrap().as_bytes().to_vec();
                TemplateCommand::Setup(&setup)
            }
            "lane" => TemplateCommand::Lane { lane: lane(), fds: [shm.as_fd(), wake.as_fd()] },
            "start" => TemplateCommand::Start { lane: lane(), attempt: Attempt(golden_u32(case, "attempt")) },
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
    lanes.attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours)).unwrap();
    let template = std::thread::spawn(move || {
        let (lane, attempt) = read_fork_request(&mut theirs);
        let answer = fork_answer_bytes(lane, attempt, 4242);
        theirs.write_all(&answer[..answer.len() / 2]).unwrap();
    });
    let first = lanes.spawn_worker(0, FORK_ANSWER_TIMEOUT);
    template.join().unwrap();
    assert!(matches!(first, Err(ForkError::TemplateClosed { .. })), "{first:?}");
    let second = lanes.spawn_worker(0, FORK_ANSWER_TIMEOUT);
    assert!(matches!(second, Err(ForkError::TemplateClosed { .. })), "{second:?}");
}

/// Bytes that are no frame end the connection: the fork fails with the frame error and the
/// first bytes the template sent.
#[test]
fn a_frame_with_an_unknown_tag_fails_the_fork_with_its_first_bytes() {
    use std::io::Write;
    let lanes = lanes(1);
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    lanes.attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours)).unwrap();
    let bad = [1u8, 0, 0, 0, 99];
    let template = std::thread::spawn(move || {
        read_fork_request(&mut theirs);
        theirs.write_all(&bad).unwrap();
        theirs
    });
    let fork = lanes.spawn_worker(0, FORK_ANSWER_TIMEOUT);
    let _theirs = template.join().unwrap();
    match fork {
        Err(ForkError::ProtocolViolated {
            end: LinkEnd::BadFrame { error: crate::lane::template::TemplateFrameError::UnknownTag(99), prefix },
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
    lanes.attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours)).unwrap();
    let template = std::thread::spawn(move || {
        read_fork_request(&mut theirs);
        theirs.write_all(&(MAX_FRAME_BYTES + 1).to_le_bytes()).unwrap();
        theirs
    });
    let fork = lanes.spawn_worker(0, FORK_ANSWER_TIMEOUT);
    let _theirs = template.join().unwrap();
    assert!(
        matches!(
            fork,
            Err(ForkError::ProtocolViolated {
                end: LinkEnd::BadFrame { error: crate::lane::template::TemplateFrameError::TooLong { .. }, .. },
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
    lanes.attach_template(Pid::new(std::process::id() as i32).unwrap(), OwnedFd::from(ours)).unwrap();
    let dead = Pid::new(777).unwrap();
    let template = std::thread::spawn(move || {
        let (lane, attempt) = read_fork_request(&mut theirs);
        let exited = report_frame(&TemplateReport::Exited { lane: LaneIndex(1), pid: dead, status: 9 });
        theirs.write_all(&exited).unwrap();
        theirs.write_all(&fork_answer_bytes(lane, attempt, child_pid)).unwrap();
        theirs
    });
    let fork = lanes.spawn_worker(0, FORK_ANSWER_TIMEOUT).map(Pid::as_raw).map_err(|e| e.to_string());
    let _theirs = template.join().unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(fork, Ok(child_pid));
    assert_eq!(lanes.exits.status_of(1, dead), Some(9));
    assert_eq!(lanes.exits.status_of(1, Pid::new(778).unwrap()), None, "a report is read by pid");
    assert_eq!(lanes.exits.status_of(0, dead), None, "and by lane");
}

/// The fork exchange holds the socket while it waits; a host looking for a dead worker's exit
/// status never waits for it: it reads what the exchange took off the socket, within its own
/// wait, while the exchange is still waiting.
#[test]
fn a_host_reads_an_exit_status_while_a_fork_holds_the_socket() {
    use std::io::Write;
    let lanes = lanes(1);
    let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    lanes.attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours)).unwrap();
    let dead = Pid::new(777).unwrap();
    let (close_tx, close_rx) = std::sync::mpsc::channel::<()>();
    let template = std::thread::spawn(move || {
        read_fork_request(&mut theirs);
        let exited = report_frame(&TemplateReport::Exited { lane: LaneIndex(0), pid: dead, status: 9 << 8 });
        theirs.write_all(&exited).unwrap();
        // Answers nothing more, holding the socket open, until the test lets go.
        close_rx.recv().expect_err("the test lets go by dropping its end, sending nothing");
    });
    let forking = Arc::clone(&lanes);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let fork = std::thread::spawn(move || done_tx.send(forking.spawn_worker(0, FORK_ANSWER_TIMEOUT)).unwrap());
    let _run = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let local = tokio::task::LocalSet::new();
    let (exit, still_waiting) = local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        // Until the exchange took the report: it is the socket's only reader meanwhile.
        while lanes.exits.status_of(0, dead).is_none() {
            tokio::task::yield_now().await;
        }
        let exit = host.exit_status(0, dead).await;
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
    lanes.attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours)).unwrap();
    let missing = report_frame(&TemplateReport::LaneFdsMissing { lane: LaneIndex(0), got: 1 });
    theirs.write_all(&missing).unwrap();
    drop(theirs);
    match lanes.start_run(HostKind::Dedicated, ServerRunStop::default()) {
        Err(LaneStartError::LaneFds { lane: LaneIndex(0), got: 1, want: 2 }) => {}
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
    lanes.attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours)).unwrap();
    let failed = report_frame(&TemplateReport::ImportFailed("Traceback: import failed".into()));
    theirs.write_all(&failed).unwrap();
    drop(theirs);
    match lanes.start_run(HostKind::Dedicated, ServerRunStop::default()) {
        Err(LaneStartError::TemplateFailed { lane: LaneIndex(0), failure, .. }) => {
            assert_eq!(failure, "Traceback: import failed");
        }
        other => panic!("expected the template's traceback, got {other:?}"),
    }
    lanes.stop_run(&ShutdownDeadline::new(Duration::from_secs(5)));
}

// ─────────────────────────── a worker that writes its counters wrong ───────────────────────────

/// Starts a process that stands in for lane `i`'s worker, so the host has a real pid to kill.
fn stand_in_worker(lanes: &ServerLanes, i: usize) -> (std::process::Child, Pid) {
    let child = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    let pid = Pid::new(child.id() as i32).unwrap();
    lanes.pids[i].store(Some(pid));
    (child, pid)
}

/// Publishes a request into lane `i` of `host` and returns its receiver.
fn publish_into(
    host: &LaneHost,
    i: usize,
) -> tokio::sync::oneshot::Receiver<Result<HandlerResponse, LaneRequestError>> {
    let (job, answer) = job();
    host.with_lane(i, |lane| {
        if let Err((e, _)) = host.publish(lane, job) {
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
        Err(LaneRequestError::Rejected(UnansweredError::PeerCorrupted { lane: LaneIndex(0), fault, .. })) => {
            assert_eq!(fault, want);
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
    let (mut faulty, faulty_pid) = stand_in_worker(&lanes, 0);
    let (mut fine, _) = stand_in_worker(&lanes, 1);
    let _run = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        let mut in_faulty = [publish_into(host, 0), publish_into(host, 0)];
        let mut in_fine = publish_into(host, 1);
        lanes.memory.region.header(0).resp_head.0.store(u32::MAX, SeqCst);
        worker_answers(&lanes, 1, 0, &ok_answer());

        assert_eq!(host.collect(), 1, "only the other lane's answer is read");

        let fault = PeerCounterError::AnswersPastSent { read: Seq::new(0), sent: Seq::new(2), published: Seq::new(u32::MAX) };
        for answer in &mut in_faulty {
            assert_peer_corrupted(answer, fault);
        }
        assert_eq!(&answered(&mut in_fine).expect("the other lane answers").body[..], b"ok");
        {
            let owned = host.owned.borrow();
            assert!(matches!(owned[0].worker, LaneWorker::Faulted { pid, .. } if pid == faulty_pid));
            assert_eq!((owned[0].cursor, owned[0].head), (0, 2), "the slots stay the worker's until it dies");
            assert!(matches!(owned[1].worker, LaneWorker::Running { .. }));
        }
        host.collect();
        assert_eq!(lanes.stats.peer_faults.load(Relaxed), 1, "the fault is handled once");
        let (job, _answer) = job();
        assert!(host.submit(job).is_ok());
        assert_eq!(host.owned.borrow()[0].head, 2, "a new request went into the faulted lane");
    });
    let status = faulty.wait().unwrap();
    assert_eq!(std::os::unix::process::ExitStatusExt::signal(&status), Some(libc::SIGKILL));
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
        fault: PeerCounterError::AnswersPastSent { read: Seq::new(0), sent: Seq::new(1), published: Seq::new(7) },
    });
    let (status, body) = response_of(corrupted);
    assert_eq!(status, 502);
    assert!(!body.contains("4242"), "{body}");
}

/// Reports lane `i`'s worker `pid` dead with exit status 9, as the template would.
async fn report_death(host: &Rc<LaneHost>, i: usize, pid: Pid) {
    host.lanes.exits.record(LaneIndex(i), pid, 9);
    host.worker_died(i, pid).await;
}

/// The death of a worker killed for a corrupt counter frees every slot of its lane, and the
/// shared counters say the next worker starts after them: none of its requests runs again.
#[test]
fn a_faulted_workers_death_frees_its_slots_and_the_next_worker_starts_after_them() {
    let lanes = lanes_backing_off(1, LONG_WAITS);
    let (mut faulty, pid) = stand_in_worker(&lanes, 0);
    let _run = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        let mut answers = [publish_into(&host, 0), publish_into(&host, 0)];
        let h = lanes.memory.region.header(0);
        h.resp_head.0.store(5, SeqCst);
        host.collect();
        faulty.wait().unwrap();
        report_death(&host, 0, pid).await;

        let fault = PeerCounterError::AnswersPastSent { read: Seq::new(0), sent: Seq::new(2), published: Seq::new(5) };
        for answer in &mut answers {
            assert_peer_corrupted(answer, fault);
        }
        {
            let owned = host.owned.borrow();
            assert_eq!((owned[0].cursor, owned[0].head), (2, 2));
            assert!(owned[0].waiting.iter().all(Option::is_none));
        }
        assert_eq!((h.taken.0.load(SeqCst), h.resp_head.0.load(SeqCst)), (2, 2));
        host.shutdown().await;
    });
}

/// A worker that died leaving `taken` where no worker can leave it might have run any of its
/// requests: every one in its slots is answered 502 and none is handed to the next worker.
#[test]
fn a_dead_worker_with_taken_out_of_range_fails_every_request_it_held() {
    let lanes = lanes_backing_off(1, LONG_WAITS);
    let (mut gone, pid) = stand_in_worker(&lanes, 0);
    gone.kill().unwrap();
    gone.wait().unwrap();
    let _run = serve(&lanes, 1);
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        let mut answers = [publish_into(&host, 0), publish_into(&host, 0)];
        let h = lanes.memory.region.header(0);
        h.taken.0.store(2, SeqCst);
        report_death(&host, 0, pid).await;

        let fault = PeerCounterError::TakenOutOfRange { read: Seq::new(0), sent: Seq::new(2), published: Seq::new(0), taken: Seq::new(2) };
        for answer in &mut answers {
            assert_peer_corrupted(answer, fault);
        }
        assert_eq!(lanes.stats.peer_faults.load(Relaxed), 1);
        assert_eq!(host.owned.borrow()[0].cursor, 2);
        assert_eq!((h.taken.0.load(SeqCst), h.resp_head.0.load(SeqCst)), (2, 2));
        host.shutdown().await;
    });
}

/// The lane timeout is the host's own measure: a worker that says it took nothing and never
/// stamped a start (`taken` at the host's cursor, `started_ns` 0) is still killed once the
/// host has seen the request at the head of its lane past the timeout.
#[test]
fn a_handler_is_timed_by_the_host_whatever_its_worker_writes() {
    let plain = lanes(1);
    let geometry = plain.memory.region.geometry();
    let host_config = HostConfig { timeout: Duration::from_nanos(1), ..plain.host };
    let lanes = Arc::new(ServerLanes::create(geometry, host_config).unwrap());
    let (mut stuck, pid) = stand_in_worker(&lanes, 0);
    let _run = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        let _answer = publish_into(host, 0);
        let h = lanes.memory.region.header(0);
        h.taken.0.store(0, SeqCst);
        h.started_ns.0.store(0, SeqCst);
        host.check_timeouts();
        let seen = match host.owned.borrow()[0].worker {
            LaneWorker::Running { head, .. } => head,
            _ => None,
        };
        let Some(HeadOfLine { seq: 0, since_ns }) = seen else {
            panic!("the host did not see the request at the head: {seen:?}");
        };
        while shm::monotonic_ns() <= since_ns + 1 {
            std::hint::spin_loop();
        }
        host.check_timeouts();
        assert!(matches!(host.owned.borrow()[0].worker, LaneWorker::Overrun { pid: p, .. } if p == pid));
        assert_eq!(lanes.stats.timeouts_killed.load(Relaxed), 1);
    });
    let status = stuck.wait().unwrap();
    assert_eq!(std::os::unix::process::ExitStatusExt::signal(&status), Some(libc::SIGKILL));
}

/// Breaking a lane while its replacement is still being forked kills the worker that fork
/// makes, which would otherwise run on with no lane and no host knowing its pid.
#[test]
fn breaking_a_lane_mid_fork_kills_the_worker_the_fork_makes() {
    let lanes = lanes(1);
    let _run = serve(&lanes, 1);
    let mut forked = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    let pid = Pid::new(forked.id() as i32).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let host = LaneHost::start(&lanes, 0).unwrap().expect("a host");
        let (answer_fork, fork_answered) = std::sync::mpsc::channel::<()>();
        let task = tokio::task::spawn_blocking(move || {
            fork_answered.recv().expect("the test answers the fork");
            Ok(pid)
        });
        host.with_lane(0, |lane| lane.worker = LaneWorker::Forking { task, since: Instant::now() })
            .expect("the host owns the lane");
        host.break_lane(0, fork_failed("gone"));
        answer_fork.send(()).unwrap();
        while forked.try_wait().unwrap().is_none() {
            tokio::task::yield_now().await;
        }
        host.shutdown().await;
    });
    let status = forked.wait().unwrap();
    assert_eq!(std::os::unix::process::ExitStatusExt::signal(&status), Some(libc::SIGKILL));
}

/// A corrupt answer counter seen while the lane's replacement is being forked waits for the
/// replacement to run, and is then that worker's fault: killed, its requests answered 502.
#[test]
fn a_corrupt_counter_seen_mid_fork_is_the_replacements_fault_once_it_runs() {
    let lanes = lanes(1);
    let (mut replacement, pid) = stand_in_worker(&lanes, 0);
    let _run = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        let mut answer = publish_into(host, 0);
        let task = tokio::task::spawn_blocking(move || Ok(pid));
        host.with_lane(0, |lane| lane.worker = LaneWorker::Forking { task, since: Instant::now() })
            .expect("the host owns the lane");
        lanes.memory.region.header(0).resp_head.0.store(7, SeqCst);
        assert_eq!(host.collect(), 0);
        assert!(answer.try_recv().is_err(), "a request was answered before the worker ran");
        assert_eq!(lanes.stats.peer_faults.load(Relaxed), 0);

        host.with_lane(0, |lane| lane.worker = LaneWorker::Running { pid, starting_since: None, head: None })
            .expect("the host owns the lane");
        host.collect();
        assert_peer_corrupted(
            &mut answer,
            PeerCounterError::AnswersPastSent { read: Seq::new(0), sent: Seq::new(1), published: Seq::new(7) },
        );
    });
    let status = replacement.wait().unwrap();
    assert_eq!(std::os::unix::process::ExitStatusExt::signal(&status), Some(libc::SIGKILL));
}

/// With no worker of its own (here waiting out a crash loop) a lane's counters are the host's:
/// a write that does not fit is put back, counted once, and answers nothing.
#[test]
fn a_counter_written_while_the_lane_has_no_worker_is_put_back() {
    let lanes = lanes(1);
    let _run = serve(&lanes, 1);
    on_host(&lanes, 0, |host| {
        host.with_lane(0, |lane| lane.worker = LaneWorker::CrashWait).expect("the host owns the lane");
        let h = lanes.memory.region.header(0);
        h.resp_head.0.store(9, SeqCst);
        assert_eq!(host.collect(), 0);
        assert_eq!((h.taken.0.load(SeqCst), h.resp_head.0.load(SeqCst)), (0, 0));
        assert_eq!(host.collect(), 0);
        assert_eq!(lanes.stats.peer_faults.load(Relaxed), 1);
    });
}
