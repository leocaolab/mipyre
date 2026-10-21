//! Serves the multi-thread server (`MIPYRE_TPC=0`): Tokio accept loops on a work-stealing runtime,
//! each connection a `Send` task any IO thread may poll.

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioExecutor;
use pyo3::PyResult;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio_util::sync::CancellationToken;

use mipyre_http::io::body::ResponseBody;
use mipyre_tpc::ShutdownDeadline;
use mipyre_transport::driver::{AcceptError, Peer, TransportDriver};

use crate::serve::conn::{drive_connection, ConnectionTimeouts};
use crate::serve::websocket::{upgrade_or, Upgrades};
use crate::serve::{until_stopped, StopSignals};
use crate::telemetry::RunCounters;

/// Fixes how many tasks a tokio worker polls from its own queue between looks at the shared one.
/// Left unset, tokio tunes it from measured poll times, which makes a request's instruction count
/// follow how fast the machine ran; 61 is where tokio's tuning starts.
const GLOBAL_QUEUE_INTERVAL: u32 = 61;

/// Builds the multi-thread server's runtime. Its blocking pool runs main-interpreter handlers,
/// so every thread gets `PYTHON_THREAD_STACK`: tokio's default is 2 MiB, and C extensions are
/// written against 8.
fn io_runtime(io_workers: usize) -> std::io::Result<tokio::runtime::Runtime> {
    RuntimeBuilder::new_multi_thread()
        .worker_threads(io_workers)
        .global_queue_interval(GLOBAL_QUEUE_INTERVAL)
        .thread_stack_size(mipyre_subinterp::PYTHON_THREAD_STACK)
        .enable_all()
        .build()
}

/// Runs the multi-thread (`MIPYRE_TPC=0`) server until SIGINT, SIGTERM or `shutdown()`, then
/// drains in-flight connections for the drain part of the stop's grace, because dropping the
/// runtime would abort them mid-request. The runtime's blocking threads get until `deadline` to
/// end; one still running a handler then is left running.
///
/// # Errors
///
/// Returns a `RuntimeError` when the runtime or the signal handlers cannot be set up, and the
/// accept error that stopped the server.
pub(crate) fn serve_multi_thread<D, H, Fut>(
    transport: D,
    io_workers: usize,
    stop: CancellationToken,
    deadline: &ShutdownDeadline,
    counters: Arc<RunCounters>,
    upgrades: Upgrades,
    handle: H,
) -> PyResult<()>
where
    D: TransportDriver,
    H: Fn(Request<Incoming>, Peer) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<Response<ResponseBody>, hyper::Error>> + Send + 'static,
{
    let rt = io_runtime(io_workers).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("tokio runtime error: {e}"))
    })?;
    let served = rt.block_on(async move {
        // The handlers are registered before any accept loop, so a server that cannot watch for
        // its stop signals does not start.
        let signals = StopSignals::install().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("stop signal handlers: {e}"))
        })?;
        let conn_tracker = tokio_util::task::TaskTracker::new();
        // The first accept loop that can take no more connections stops the server.
        let accept_stopped: Arc<std::sync::OnceLock<AcceptError>> = Arc::default();
        for share in transport.into_loops() {
            let mut source = share.register().map_err(crate::serve::listener_error)?;
            let (token, tracker, upgrades, handle, accept_stopped) = (
                stop.clone(),
                conn_tracker.clone(),
                upgrades.clone(),
                handle.clone(),
                Arc::clone(&accept_stopped),
            );
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        accepted = D::accept(&mut source) => match accepted {
                            Ok(accepted) => {
                                tracker.spawn(serve_accepted::<D, _, _>(
                                    accepted,
                                    upgrades.clone(),
                                    handle.clone(),
                                    token.clone(),
                                ));
                            }
                            Err(stopped_accepting) => {
                                // A later loop's error is kept only in its own log line.
                                if let Err(later) = accept_stopped.set(stopped_accepting) {
                                    tracing::error!(target: "mipyre::server", error = %later, "accept loop stopped");
                                }
                                token.cancel();
                                break;
                            }
                        },
                        _ = token.cancelled() => break,
                    }
                }
            });
        }

        until_stopped(stop, signals).await;
        let stopped = Instant::now();
        conn_tracker.close();
        drain_connections(&conn_tracker, crate::serve::drain_timeout(deadline, stopped), &counters).await;
        match accept_stopped.get() {
            Some(stopped_accepting) => Err(crate::serve::accept_stopped_error(stopped_accepting)),
            None => Ok(()),
        }
    });
    // Dropping the runtime would wait for a blocking handler with no limit.
    rt.shutdown_timeout(deadline.fix(Instant::now()).saturating_duration_since(Instant::now()));
    served
}

/// Sets up an accepted connection, registered and TLS-handshaken when the listener has an
/// acceptor, and serves its requests with `handle` until it ends or `stop` is cancelled.
async fn serve_accepted<D, H, Fut>(accepted: D::Connection, upgrades: Upgrades, handle: H, stop: CancellationToken)
where
    D: TransportDriver,
    H: Fn(Request<Incoming>, Peer) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Response<ResponseBody>, hyper::Error>> + Send + 'static,
{
    let peer = D::peer(&accepted);
    let Some(stream) = D::into_stream(accepted).await else {
        return;
    };
    let stop = stop.cancelled_owned();
    match upgrades {
        Upgrades::WebSocket(site) => {
            let svc = service_fn(move |req: Request<Incoming>| upgrade_or(req, &site, peer, &handle));
            drive_connection(stream, svc, TokioExecutor::new(), ConnectionTimeouts::DEFAULT, stop).await;
        }
        Upgrades::Off => {
            let svc = service_fn(move |req: Request<Incoming>| handle(req, peer));
            drive_connection(stream, svc, TokioExecutor::new(), ConnectionTimeouts::DEFAULT, stop).await;
        }
    }
}

/// Lets the multi-thread server's open connections, on the closed `tracker`, finish for at most
/// `timeout`; the ones still open then are counted and logged, and closed as their tasks drop.
async fn drain_connections(tracker: &tokio_util::task::TaskTracker, timeout: std::time::Duration, counters: &RunCounters) {
    if tokio::time::timeout(timeout, tracker.wait()).await.is_err() {
        crate::serve::report_drain_timeout(tracker.len(), timeout, counters);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn the_io_runtime_starts_its_threads_with_python_stacks() {
        crate::thread_stack::with_exact_stacks(
            concat!(module_path!(), "::the_io_runtime_starts_its_threads_with_python_stacks"),
            || {
                let python = crate::thread_stack::of_a_thread_with(mipyre_subinterp::PYTHON_THREAD_STACK);
                let rt = io_runtime(1).expect("the IO runtime builds");
                let (worker, blocking) = rt.block_on(async {
                    let worker = tokio::spawn(async { crate::thread_stack::current() });
                    let blocking = tokio::task::spawn_blocking(crate::thread_stack::current);
                    (worker.await.expect("on a worker"), blocking.await.expect("on the blocking pool"))
                });
                assert_eq!((worker, blocking), (python, python));
            },
        );
    }

    /// Stands in for a stop's drain budget; the drain waits exactly as long as it is given.
    const MULTI_THREAD_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

    /// Runs `body` on a paused-clock runtime, where a timer fires at its exact instant once the
    /// runtime has nothing else to do, with no real wait.
    fn on_paused_clock(body: impl std::future::Future<Output = ()>) {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap()
            .block_on(body);
    }

    /// Opens one connection per entry of `lasting`, each open for that long, on a tracker closed
    /// as the server closes it at the stop.
    fn open_connections(lasting: &[std::time::Duration]) -> tokio_util::task::TaskTracker {
        let tracker = tokio_util::task::TaskTracker::new();
        for &d in lasting {
            tracker.spawn(tokio::time::sleep(d));
        }
        tracker.close();
        tracker
    }

    /// Returns the drain timeouts and the connections they closed that a drain of `tracker`
    /// counted.
    async fn drained(tracker: &tokio_util::task::TaskTracker) -> (u64, u64) {
        let counters = RunCounters::new(false);
        drain_connections(tracker, MULTI_THREAD_DRAIN_TIMEOUT, &counters).await;
        let counts = counters.read();
        (counts.drain_timeouts, counts.drain_closed_connections)
    }

    #[test]
    fn multi_thread_drain_waits_for_a_connection_that_ends_just_before_the_timeout() {
        on_paused_clock(async {
            let tracker = open_connections(&[Duration::from_millis(29_999)]);
            assert_eq!(drained(&tracker).await, (0, 0));
        });
    }

    #[test]
    fn a_multi_thread_drain_that_times_out_counts_the_connections_still_open() {
        on_paused_clock(async {
            let tracker = open_connections(&[
                Duration::from_millis(29_999),
                Duration::from_millis(30_001),
                Duration::MAX,
            ]);
            assert_eq!(drained(&tracker).await, (1, 2));
        });
    }

    /// Proves the drain is woken by the last connection ending, not by the timeout.
    #[test]
    fn multi_thread_drain_completes_when_the_last_connection_ends() {
        on_paused_clock(async {
            let tracker = open_connections(&[Duration::ZERO]);
            let started = tokio::time::Instant::now();
            assert_eq!(drained(&tracker).await, (0, 0));
            assert_eq!(started.elapsed(), Duration::ZERO, "no timer fired");
        });
    }
}
