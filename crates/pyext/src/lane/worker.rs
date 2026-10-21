//! Runs the loop a process-lane worker never returns from: it serves each request on this
//! process's main interpreter, as a `runtime="main"` route runs in the server.

use std::collections::HashMap;
use crate::sync::Ordering::*;
use std::time::Duration;

use pyo3::prelude::*;

use super::os::WaitError;
use super::pid::Pid;
use super::shm::{self, LaneShm, WaitTotals};
use mipyre_shm::plan::spin_window::SpinWindow;
use super::wire;
use crate::exchange::{catch_panic, Request};
use crate::runtime::{in_request_context, MainReply};
use mipyre_http::plan::route::{HandlerTarget, RouteKey};
use mipyre_http::plan::site::SiteRules;

/// Says why a worker stops serving its lane.
#[derive(Debug, PartialEq, Eq)]
enum ServeEnd {
    /// The template is gone, and the server with it.
    TemplateGone,
    /// The server closed its end of the wake pipe, so it is gone.
    HostGone,
    /// The OS refused the wait for requests: waiting again would return at once and spin.
    WaitRefused(WaitError),
}

/// Is sysexits.h's EX_OSERR, the status of a worker the OS would not let wait; the server
/// reports it as the worker's exit and starts another under its crash-loop limit.
const WAIT_REFUSED_STATUS: i32 = 71;

/// Ends this worker for `end`, without finalizing Python.
fn end_worker(lane: usize, end: ServeEnd) -> ! {
    match end {
        ServeEnd::TemplateGone | ServeEnd::HostGone => super::os::exit_now(0),
        ServeEnd::WaitRefused(e) => {
            tracing::error!(
                target: "mipyre::server",
                lane,
                error = %e,
                "a lane worker cannot wait for requests: it exits so the server starts another"
            );
            super::os::exit_now(WAIT_REFUSED_STATUS)
        }
    }
}

/// Says what writing the wake byte found.
#[derive(Debug, PartialEq, Eq)]
enum Doorbell {
    /// The byte is in the pipe, or the pipe is full and wakes the host already.
    Rung,
    HostGone,
    Failed(rustix::io::Errno),
}

fn doorbell(written: Result<usize, rustix::io::Errno>) -> Doorbell {
    use rustix::io::Errno;
    match written {
        Ok(_) | Err(Errno::AGAIN) => Doorbell::Rung,
        Err(Errno::PIPE) => Doorbell::HostGone,
        Err(e) => Doorbell::Failed(e),
    }
}

/// Writes one wake byte for the host; SIGPIPE is ignored (Python ignores it at start-up), so a
/// host that is gone shows as EPIPE.
fn ring(wake_channel_fd: std::os::fd::BorrowedFd<'_>) -> Doorbell {
    doorbell(rustix::io::write(wake_channel_fd, &[1u8]))
}

/// Starts a thread that ends this process the moment the template exits.
///
/// macOS has no parent-death signal like Linux's `PR_SET_PDEATHSIG`, so without it a worker stuck
/// in a handler would outlive a template killed with SIGKILL. The worker never forks, so the extra
/// thread is safe.
#[cfg(target_os = "macos")]
fn die_with_template(template_pid: Pid) -> PyResult<()> {
    let os_err = |what: &str, e: std::io::Error| {
        pyo3::exceptions::PyOSError::new_err(format!("{what}: {e}"))
    };
    let queue =
        super::os::ExitQueue::new().map_err(|e| os_err("kqueue for the template watch", e))?;
    if let Err(e) = queue.add(template_pid.into(), 0) {
        if e.raw_os_error() == Some(libc::ESRCH) {
            super::os::exit_now(0); // ESRCH means the template is already gone.
        }
        return Err(os_err("watching the template", e));
    }
    std::thread::Builder::new()
        .name("mipyre-template-watch".into())
        .spawn(move || loop {
            match queue.exited::<1>(None, |_, _| Ok(())) {
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
        })
        .map_err(|e| os_err("the template watch thread", e))?;
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn die_with_template(_template_pid: Pid) -> PyResult<()> {
    Ok(())
}

/// Serves lane `lane` until the process is killed, and exits the process when the template is
/// gone.
///
/// # Errors
///
/// Returns an error only when the template watch cannot be set up.
pub(crate) fn serve<M>(
    py: Python<'_>,
    region: LaneShm,
    lane: usize,
    rules: SiteRules<Py<PyAny>, M>,
    worker_spin: Duration,
    template_pid: Pid,
    wake_channel_fd: std::os::fd::BorrowedFd<'_>,
) -> PyResult<()> {
    super::role::mark_lane_worker();
    die_with_template(template_pid)?;
    let h = region.header(lane);
    let routes = routes_by_key(&rules);
    let mut cursor = h.taken.0.load(Acquire);
    let pid = rustix::process::getpid().as_raw_nonzero().get();
    h.ready_pid.0.store(pid as u32, SeqCst);
    // A spin longer than u64 nanoseconds hold (584 years) never ends anyway, so it saturates.
    let spin_ns = u64::try_from(worker_spin.as_nanos()).unwrap_or(u64::MAX);
    let mut last_done = shm::monotonic_ns();
    loop {
        // The worker spins for `worker_spin` after its last answer, then parks with the GIL
        // released in 1 s slices so it notices a template that is gone.
        let mut waited = WaitTotals::default();
        if h.req_head.0.load(Acquire) == cursor {
            waited.waits = 1;
            let window = SpinWindow::until(last_done.saturating_add(spin_ns));
            let spun = shm::spin_for_change(&h.req_head.0, cursor, window);
            waited.spin_rounds = spun.rounds.into();
            if !spun.ready {
                let parked = shm::park_for_change(
                    &h.req_head.0,
                    &h.worker_sleeping.0,
                    cursor,
                    1_000_000_000,
                    |a, v, t| py.detach(|| shm::park(a, v, t)).map_err(ServeEnd::WaitRefused),
                    || {
                        if rustix::process::getppid() == Some(template_pid.into()) {
                            Ok(())
                        } else {
                            Err(ServeEnd::TemplateGone)
                        }
                    },
                );
                match parked {
                    Ok(parked) => waited.parks = parked.parks,
                    Err(end) => end_worker(lane, end),
                }
            }
        }
        let seq = cursor;
        h.started_ns.0.store(shm::monotonic_ns(), SeqCst);
        h.taken.0.store(seq.wrapping_add(1), SeqCst);

        // SAFETY: The server published `seq` and does not touch its slot until it has read the
        // answer, and this worker owns the response slot until it publishes it.
        let input = unsafe { region.read_slot(region.req_slot(lane, seq)) };
        let answer_slot = region.resp_slot(lane, seq);
        let result = match input.and_then(wire::decode_request) {
            Ok(req) => run(py, &rules, &routes, req),
            Err(e) => Err(crate::exchange::WorkerFailure {
                error: format!("the process-lane worker could not read its request: {e}"),
                traceback: None,
            }),
        };
        // SAFETY: as above.
        if let Some(e) = unsafe { write_answer(&region, answer_slot, &result) } {
            // `Geometry::new` refuses a slot too small for a failure, so this is a slot this
            // worker was not given; the server reads the empty answer as malformed.
            tracing::error!(target: "mipyre::server", error = %e, "process lane answer refused");
        }
        cursor = seq.wrapping_add(1);
        // The answer is published before the wake byte, and the byte is written only when the
        // host flagged that it sleeps in its reactor.
        h.resp_head.0.store(cursor, SeqCst);
        let host_asleep = h.server_sleeping.0.load(SeqCst) == 1;
        if host_asleep {
            match ring(wake_channel_fd) {
                Doorbell::Rung => {}
                Doorbell::HostGone => end_worker(lane, ServeEnd::HostGone),
                Doorbell::Failed(e) => tracing::error!(
                    target: "mipyre::server",
                    lane,
                    errno = e.raw_os_error(),
                    error = %e,
                    "a lane worker could not wake its sleeping host: the answer waits until something else wakes it"
                ),
            }
        }
        waited.wakes = host_asleep.into();
        h.worker_waits.0.add(waited);
        last_done = shm::monotonic_ns();
    }
}

/// Writes `result` into `slot` and returns the refusal when it does not fit, leaving the slot
/// empty.
///
/// # Safety
///
/// The caller owns the slot.
unsafe fn write_answer(
    region: &LaneShm,
    slot: *mut u8,
    result: &Result<mipyre_http::plan::response::HandlerResponse, crate::exchange::WorkerFailure>,
) -> Option<wire::TooLarge> {
    region.write_slot(slot, |w| {
        let refused = wire::encode_answer(w, result).err();
        if refused.is_some() {
            w.restart();
        }
        refused
    })
}

fn run<M>(
    py: Python<'_>,
    rules: &SiteRules<Py<PyAny>, M>,
    routes: &HashMap<RouteKey, HandlerTarget>,
    req: wire::SlotRequest<'_>,
) -> Result<mipyre_http::plan::response::HandlerResponse, crate::exchange::WorkerFailure> {
    let wire::SlotRequest {
        route_key,
        head,
        body,
    } = req;
    let body = pyo3::types::PyBytes::new(py, body).unbind();
    let request = Request::new(head, crate::exchange::RequestBody::Received(body));
    let Some(&target) = routes.get(&route_key) else {
        return Err(crate::exchange::WorkerFailure {
            error: format!(
                "route {route_key} is not registered in this worker: the app it imported has {}",
                routes
                    .keys()
                    .map(RouteKey::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            traceback: None,
        });
    };
    answer_of(|| {
        in_request_context(py, |rc| {
            crate::runtime::run_with_hooks(py, rc, rules, target, request)
        })
    })
}

/// Runs `call` and turns a `Stream` reply, an error or a panic into a failure; it catches the
/// panic so the worker answers it and serves the next request.
fn answer_of(
    call: impl FnOnce() -> Result<MainReply, crate::exchange::HandlerError>,
) -> Result<mipyre_http::plan::response::HandlerResponse, crate::exchange::WorkerFailure> {
    match catch_panic(call) {
        Ok(MainReply::Response(data)) => Ok(data),
        Ok(MainReply::Stream(_)) => Err(crate::exchange::WorkerFailure {
            error:
                "a runtime='process' handler returned a Stream; streaming responses are not served \
                    from process lanes: use runtime='main'"
                    .into(),
            traceback: None,
        }),
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

    #[test]
    fn a_wake_byte_written_or_a_full_pipe_rings_and_a_closed_pipe_is_a_host_gone() {
        use rustix::io::Errno;
        assert_eq!(doorbell(Ok(1)), Doorbell::Rung);
        assert_eq!(doorbell(Err(Errno::AGAIN)), Doorbell::Rung);
        assert_eq!(doorbell(Err(Errno::PIPE)), Doorbell::HostGone);
        assert_eq!(doorbell(Err(Errno::BADF)), Doorbell::Failed(Errno::BADF));
    }

    #[test]
    fn a_wake_into_a_pipe_whose_reader_closed_finds_the_host_gone() {
        use std::os::fd::AsFd;
        let (read, write) = rustix::pipe::pipe().unwrap();
        assert_eq!(ring(write.as_fd()), Doorbell::Rung);
        drop(read);
        assert_eq!(ring(write.as_fd()), Doorbell::HostGone);
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

        let panicked = answer_of(|| -> Result<MainReply, HandlerError> {
            panic!("lane panic 5c1e")
        })
        .expect_err("a panic is a failure");
        assert_eq!(
            panicked.error,
            "a Rust panic while running the request: lane panic 5c1e"
        );
        assert!(panicked.traceback.is_none());
    }
}
