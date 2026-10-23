//! Runs the loop a process-lane worker never returns from: it serves each request on this
//! process's main interpreter, as a `runtime="main"` route runs in the server.

use std::collections::HashMap;
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;

use super::os::monotonic_ns;
use super::pid::Pid;
use super::wire;
use crate::exchange::{catch_panic, LaneDecodeError, LaneIndex, Request};
use crate::runtime::{in_request_context, MainReply};
use mipyre_http::plan::route::{HandlerTarget, RouteKey};
use mipyre_http::plan::site::SiteRules;
use mipyre_shm::adapter::doorbell::{HostBellOutcome, PipeDoorbell};
use mipyre_shm::adapter::mapped::MappedMemory;
use mipyre_shm::adapter::parker::FutexParker;
use mipyre_shm::channel::{Attempt, Channel, PeerEnd, SlotWriter, Wait};
use mipyre_shm::plan::spin_window::SpinWindow;
use mipyre_shm::protocol::doorbell::{AtomicU32, Parker};
use mipyre_shm::SyscallError;

/// Says why a worker stops serving its lane.
#[derive(Debug, PartialEq, Eq)]
enum ServeEnd {
    /// The template is gone, and the server with it.
    TemplateGone,
    /// The server closed its end of the wake pipe, so it is gone.
    HostGone,
    /// The OS refused the wait for requests: waiting again would return at once and spin.
    WaitRefused(SyscallError),
    /// The host's wake pipe cannot be written: an answer would wait for something else to wake a sleeping host.
    BellBroken(SyscallError),
}

/// Is sysexits.h's EX_OSERR, the status of a worker the OS would not let wait; the server reports it as the worker's
/// exit and starts another under its crash-loop limit.
const WAIT_REFUSED_STATUS: i32 = 71;

/// Is sysexits.h's EX_IOERR, the status of a worker that cannot ring its host; the server starts another, as for any
/// crash.
const BELL_BROKEN_STATUS: i32 = 74;

/// Is sysexits.h's EX_CONFIG, the status of a worker that refuses to start because it cannot trust what it would
/// start with: another build's `mipyre`, or a lane it cannot map. It writes nothing into the lane, and the server
/// breaks the lane for good instead of starting another. `START_REFUSED` in `mipyre._process_lane.server` is the same.
pub(crate) const START_REFUSED: i32 = 78;

/// Returns the exit status a worker ends with for `end`.
fn serve_end_status(end: &ServeEnd) -> i32 {
    match end {
        ServeEnd::TemplateGone | ServeEnd::HostGone => 0,
        ServeEnd::WaitRefused(_) => WAIT_REFUSED_STATUS,
        ServeEnd::BellBroken(_) => BELL_BROKEN_STATUS,
    }
}

/// Ends this worker for `end`, without finalizing Python.
fn end_worker(lane: LaneIndex, end: ServeEnd) -> ! {
    match &end {
        ServeEnd::TemplateGone | ServeEnd::HostGone => {}
        ServeEnd::WaitRefused(e) => tracing::error!(
            target: "mipyre::server",
            lane = lane.0,
            error = %e,
            "a lane worker cannot wait for requests: it exits so the server starts another"
        ),
        ServeEnd::BellBroken(e) => tracing::error!(
            target: "mipyre::server",
            lane = lane.0,
            error = %e,
            "a lane worker cannot wake its host: it exits so the server starts another"
        ),
    }
    super::os::exit_now(serve_end_status(&end))
}

/// How long a worker sleeps at a time while it waits for a request: between two sleeps it looks whether its template
/// is still there.
const PARK_SLICE_NS: u64 = 1_000_000_000;

/// Sleeps for the worker with the GIL released, through the OS's address wait.
struct GilFreeParker<'py> {
    py: Python<'py>,
}

impl Parker for GilFreeParker<'_> {
    type Error = SyscallError;

    fn park(&self, word: &AtomicU32, expect: u32, slice_ns: u64) -> Result<(), SyscallError> {
        self.py.detach(|| FutexParker.park(word, expect, slice_ns))
    }
}

/// Starts a thread that ends this process the moment the template exits, unless the kernel does
/// that ([`super::os::KERNEL_ENDS_ORPHANED_WORKERS`]).
///
/// macOS has no parent-death signal like Linux's `PR_SET_PDEATHSIG`, so without it a worker stuck
/// in a handler would outlive a template killed with SIGKILL. The worker never forks, so the extra
/// thread is safe.
fn die_with_template(template_pid: Pid) -> PyResult<()> {
    use super::os::{ExitWatch, ProcessHandle, Registration};
    if super::os::KERNEL_ENDS_ORPHANED_WORKERS {
        return Ok(());
    }
    let os_err = |what: &str, e: std::io::Error| pyo3::exceptions::PyOSError::new_err(format!("{what}: {e}"));
    let watch = ExitWatch::new().map_err(|e| os_err("the exit watch for the template", e))?;
    let template = ProcessHandle::open(template_pid.into()).map_err(|e| os_err("the template's handle", e.into()))?;
    match watch.add(&template, template_pid.into(), 0) {
        Ok(Registration::Watching) => {}
        Ok(Registration::Gone) => super::os::exit_now(0),
        Err(e) => return Err(os_err("watching the template", e)),
    }
    std::thread::Builder::new()
        .name("mipyre-template-watch".into())
        .spawn(move || {
            // Held for as long as the watch: on Linux the pidfd is what is watched.
            let _template = template;
            loop {
                match watch.exited::<1>(None, |_| Ok(())) {
                    Ok(0) => {}
                    Ok(_) => super::os::exit_now(0),
                    // The watch cannot go on; the worker's 1 s park still checks its parent
                    // (`getppid` in `serve`), so it ends at most that much later.
                    Err(e) => {
                        tracing::error!(
                            target: "mipyre::server",
                            errno = e.raw_os_error(),
                            error = %e,
                            "the template watch failed: a dead template is seen at the next park instead"
                        );
                        return;
                    }
                }
            }
        })
        .map_err(|e| os_err("the template watch thread", e))?;
    Ok(())
}

/// What a worker serves: its lane's channel, the write end of its host's wake pipe and the start attempt it is.
pub(crate) struct PeerLane {
    pub(crate) lane: LaneIndex,
    pub(crate) attempt: Attempt,
    pub(crate) channel: Arc<Channel<MappedMemory>>,
    pub(crate) worker_spin: Duration,
}

impl PeerLane {
    /// Reports that this worker could not start, with `text` (its traceback) cut to the note's room.
    pub(crate) fn report_failed(&self, bell: OwnedFd, text: &str) {
        let note = wire::cut_to_fit(text, mipyre_shm::channel::NOTE_BYTES as usize);
        PeerEnd::new(Arc::clone(&self.channel), PipeDoorbell::new(bell), FutexParker)
            .report_failed(self.attempt, note.as_bytes());
    }
}

/// Serves `peer`'s lane until the process is killed, and exits the process when the template or the host is gone.
/// `bell` is the write end of the host's wake pipe.
///
/// # Errors
///
/// Returns an error only when the template watch cannot be set up.
pub(crate) fn serve<M>(
    py: Python<'_>,
    peer: &PeerLane,
    bell: OwnedFd,
    rules: SiteRules<Py<PyAny>, M>,
    template_pid: Pid,
) -> PyResult<()> {
    super::role::mark_lane_worker(super::role::LaneWorkerProcess {
        lane: peer.lane,
        template: template_pid,
    })
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    die_with_template(template_pid)?;
    let lane = peer.lane;
    let routes = routes_by_key(&rules);
    let mut end = PeerEnd::new(Arc::clone(&peer.channel), PipeDoorbell::new(bell), GilFreeParker { py });
    end.report_ready(peer.attempt);
    // A spin longer than u64 nanoseconds hold (584 years) never ends anyway, so it saturates.
    let spin_ns = u64::try_from(peer.worker_spin.as_nanos()).unwrap_or(u64::MAX);
    let mut last_done = monotonic_ns();
    loop {
        // The worker spins for `worker_spin` after its last answer, then sleeps with the GIL released in slices, so it
        // notices a template that is gone.
        let window = SpinWindow::until(last_done.saturating_add(spin_ns));
        let request = match end.wait_for_request(window, monotonic_ns, PARK_SLICE_NS) {
            Ok(Wait::Request(request)) => request,
            Ok(Wait::SliceOver) => {
                if rustix::process::getppid() != Some(template_pid.into()) {
                    end_worker(lane, ServeEnd::TemplateGone);
                }
                continue;
            }
            Err(refused) => end_worker(lane, ServeEnd::WaitRefused(refused)),
        };
        let view = request.bytes();
        let result = match view
            .as_deref()
            .map_err(|e| LaneDecodeError::PastSlot(*e))
            .and_then(wire::decode_request)
        {
            Ok(req) => run(py, &rules, &routes, req),
            Err(e) => Err(crate::exchange::WorkerFailure {
                error: format!("the process-lane worker could not read its request: {e}"),
                traceback: None,
            }),
        };
        drop(view);
        // The answer is published before the wake byte, and the byte is written only when the host said it sleeps
        // in its reactor.
        match request.answer(|w| write_answer(lane, w, &result)) {
            Ok(None | Some(HostBellOutcome::Rung)) => {}
            Ok(Some(HostBellOutcome::HostGone)) => end_worker(lane, ServeEnd::HostGone),
            Err(broken) => end_worker(lane, ServeEnd::BellBroken(broken)),
        }
        last_done = monotonic_ns();
    }
}

/// Writes `result` into the answer slot; one that does not fit leaves the slot empty.
fn write_answer(
    lane: LaneIndex,
    w: &mut SlotWriter<'_>,
    result: &Result<mipyre_http::plan::response::HandlerResponse, crate::exchange::WorkerFailure>,
) {
    if let Err(e) = wire::encode_answer(w, result) {
        w.restart();
        // The lane's slots are at least `MIN_SLOT_BYTES`, which a failure always fits, so this is a slot this worker
        // was not given; the server reads the empty answer as malformed.
        tracing::error!(target: "mipyre::server", lane = lane.0, error = %e, "process lane answer refused");
    }
}

fn run<M>(
    py: Python<'_>,
    rules: &SiteRules<Py<PyAny>, M>,
    routes: &HashMap<RouteKey, HandlerTarget>,
    req: wire::SlotRequest<'_>,
) -> Result<mipyre_http::plan::response::HandlerResponse, crate::exchange::WorkerFailure> {
    let wire::SlotRequest { route_key, head, body } = req;
    let body = pyo3::types::PyBytes::new(py, body).unbind();
    let request = Request::new(head, crate::exchange::RequestBody::Received(body));
    let Some(&target) = routes.get(&route_key) else {
        return Err(crate::exchange::WorkerFailure {
            error: format!(
                "route {route_key} is not registered in this worker: the app it imported has {}",
                routes.keys().map(RouteKey::to_string).collect::<Vec<_>>().join(", ")
            ),
            traceback: None,
        });
    };
    answer_of(|| in_request_context(py, |rc| crate::runtime::run_with_hooks(py, rc, rules, target, request)))
}

/// Runs `call` and turns a `Stream` reply, an error or a panic into a failure; it catches the
/// panic so the worker answers it and serves the next request.
fn answer_of(
    call: impl FnOnce() -> Result<MainReply, crate::exchange::HandlerError>,
) -> Result<mipyre_http::plan::response::HandlerResponse, crate::exchange::WorkerFailure> {
    match catch_panic(call) {
        Ok(MainReply::Response(data)) => Ok(data),
        Ok(MainReply::Stream(_)) => Err(crate::exchange::WorkerFailure {
            error: "a runtime='process' handler returned a Stream; streaming responses are not served \
                    from process lanes: use runtime='main'"
                .into(),
            traceback: None,
        }),
        // Its raiser chose the answer: the lane carries it to the host as any other response.
        Err(crate::exchange::HandlerError::Http { answer, .. }) => Ok(*answer),
        Err(e) => Err(crate::exchange::WorkerFailure {
            error: e.to_string(),
            traceback: e.traceback().map(str::to_owned),
        }),
    }
}

fn routes_by_key<M>(rules: &SiteRules<Py<PyAny>, M>) -> HashMap<RouteKey, HandlerTarget> {
    rules
        .routes
        .routes()
        .iter()
        .map(|r| (r.key.clone(), r.handler))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::HandlerError;
    use crate::runtime::StreamReply;
    use mipyre_http::plan::response::HandlerResponse;

    /// The status a worker ends with says why: a wait the OS refused, a host bell it cannot ring, or nothing wrong
    /// (its template or host went first).
    #[test]
    fn a_worker_ends_with_the_status_of_why_it_stopped() {
        let refused = SyscallError {
            call: mipyre_shm::Syscall::WaitOnAddress,
            errno: rustix::io::Errno::INVAL,
        };
        let broken = SyscallError {
            call: mipyre_shm::Syscall::Write,
            errno: rustix::io::Errno::BADF,
        };
        assert_eq!(serve_end_status(&ServeEnd::WaitRefused(refused)), 71);
        assert_eq!(serve_end_status(&ServeEnd::BellBroken(broken)), 74);
        assert_eq!(serve_end_status(&ServeEnd::HostGone), 0);
        assert_eq!(serve_end_status(&ServeEnd::TemplateGone), 0);
    }

    /// The worker that refuses its start exits with the status the Python side refuses with.
    #[test]
    fn the_start_refused_status_is_the_python_sides() {
        let python = include_str!("../../../../../python/mipyre/_process_lane/server.py");
        let said = python
            .lines()
            .find_map(|line| line.strip_prefix("START_REFUSED = "))
            .expect("mipyre._process_lane.server names START_REFUSED");
        assert_eq!(said.trim().parse::<i32>(), Ok(START_REFUSED));
    }

    #[test]
    fn a_response_is_answered_and_a_stream_or_a_panic_is_a_failure_in_its_own_words() {
        let answered = answer_of(|| {
            Ok(MainReply::Response(HandlerResponse::new(
                hyper::StatusCode::CREATED,
                bytes::Bytes::from_static(b"made"),
                hyper::header::HeaderValue::from_static("text/plain"),
                mipyre_http::plan::response::HeaderLines::new(),
            )))
        })
        .expect("a response is answered as it came");
        assert_eq!(answered.status, hyper::StatusCode::CREATED);
        assert_eq!(&answered.body[..], b"made");

        let refused_by_its_raiser = answer_of(|| {
            Err(HandlerError::Http {
                stage: crate::exchange::RequestStage::Handler,
                answer: Box::new(HandlerResponse::new(
                    hyper::StatusCode::NOT_FOUND,
                    bytes::Bytes::from_static(b"{\"detail\":\"gone\"}"),
                    mipyre_http::plan::response::JSON,
                    mipyre_http::plan::response::HeaderLines::new(),
                )),
            })
        })
        .expect("an HTTPException is answered as the response it asks for");
        assert_eq!(refused_by_its_raiser.status, hyper::StatusCode::NOT_FOUND);
        assert_eq!(&refused_by_its_raiser.body[..], b"{\"detail\":\"gone\"}");

        let (_chunks, rx) = tokio::sync::mpsc::channel(1);
        let stream = StreamReply {
            rx,
            head: crate::exchange::stream::StreamHead {
                content_type: hyper::header::HeaderValue::from_static("text/event-stream"),
                status: hyper::StatusCode::OK,
                headers: hyper::HeaderMap::new(),
            },
        };
        let refused = answer_of(|| Ok(MainReply::Stream(stream))).expect_err("a Stream is refused");
        assert!(
            refused.error.contains("returned a Stream") && refused.error.contains("runtime='main'"),
            "{}",
            refused.error
        );
        assert!(refused.traceback.is_none());

        let panicked = answer_of(|| -> Result<MainReply, HandlerError> { panic!("lane panic 5c1e") })
            .expect_err("a panic is a failure");
        assert_eq!(
            panicked.error,
            "a Rust panic while running the request: lane panic 5c1e"
        );
        assert!(panicked.traceback.is_none());
    }
}
