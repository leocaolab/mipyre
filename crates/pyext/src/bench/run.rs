//! Runs the in-process benchmarks behind `MipyreApp.bench_inmem` and `bench_loopback`.
//!
//! The in-memory bench drives the full request pipeline over `tokio::io::duplex` with no
//! kernel socket, and the loopback bench uses real TCP on 127.0.0.1, so the gap between them is
//! the kernel's share. Both serve on the production TPC worker threads
//! (`serve::serve_on_workers`: pinning, park hooks, start gate, join) through the production
//! connection driver, so a bench number is the production per-request cost.

use std::io;
use std::net::Ipv4Addr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::client::{client_failures, drive_client, run_loopback_clients, ClientError, LOOPBACK_CLIENT};
use super::measure::{measure, measure_profiled, Measurement, ProfileDepth, ProfileReport};
use crate::boot::worker_loss_listener;
use crate::sync::{AtomicU64, Mutex};
use mipyre_cpu::CoreId;
use mipyre_runtime::{LiveWorkers, WorkerCounters, WorkerKind, WorkerRecipe};
use mipyre_server::ServerCounters;
use mipyre_server::{
    serve_on_workers, tpc_accept_loop_inline, ServeError, ServerRunStop, Site, TpcServer, TpcThread, TpcThreads,
};
use mipyre_tpc::ShutdownDeadline;
use mipyre_transport::driver::TransportDriver;
use mipyre_transport::endpoint::{Endpoints, ListenerSet, ListenerSpec};
use mipyre_util::panic_message;

/// Sizes the in-memory connection buffer in each direction.
const DUPLEX_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub(crate) enum BenchError {
    #[error("spawn thread {thread}: {source}")]
    ThreadNotSpawned {
        thread: String,
        #[source]
        source: io::Error,
    },
    #[error("bind a loopback port: {0}")]
    BindFailed(#[source] mipyre_transport::tcp::listener::ListenerError),
    /// The server's threads did not start, or one of them failed.
    #[error(transparent)]
    Serve(ServeError),
    #[error("the bench failed: {}", join_failures(first, rest))]
    ThreadsFailed {
        first: Box<BenchPartError>,
        rest: Vec<BenchPartError>,
    },
}

/// Says why a client, or a worker the bench serves on, failed. Any such failure fails the whole
/// bench, because a number measured with a dead worker or client is not the number asked for.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BenchPartError {
    #[error("thread {thread} panicked: {payload}")]
    Panicked { thread: String, payload: String },
    #[error("thread {thread} could not build its runtime: {source}")]
    RuntimeNotBuilt {
        thread: String,
        #[source]
        source: io::Error,
    },
    #[error("client connection {conn}: {source}")]
    ClientFailed {
        conn: usize,
        #[source]
        source: ClientError,
    },
    #[error("client connection {conn} panicked: {payload}")]
    ClientPanicked { conn: usize, payload: String },
    #[error("worker {worker}: {why}")]
    WorkerRetired { worker: usize, why: Arc<str> },
}

fn join_failures(first: &BenchPartError, rest: &[BenchPartError]) -> String {
    std::iter::once(first)
        .chain(rest)
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

/// Holds what a bench's worker threads are made of. Each thread gets its own copy of the site so
/// no two cores share a refcount cacheline; every copy counts in `counters`, and the workers in
/// `workers`.
pub(crate) struct BenchWorkers {
    pub(crate) recipe: Arc<WorkerRecipe>,
    pub(crate) sites: Vec<Arc<Site>>,
    pub(crate) counters: Arc<ServerCounters>,
    pub(crate) workers: Arc<WorkerCounters>,
}

impl BenchWorkers {
    /// Returns the TPC worker server these make, one thread per site pinned to `cpus` in order,
    /// stopped by `run` once no worker is left serving, as production's is.
    fn into_server(self, cpus: &[CoreId], run: &ServerRunStop) -> TpcServer {
        let BenchWorkers {
            recipe,
            sites,
            counters,
            workers,
        } = self;
        let (_, lives) = LiveWorkers::new(
            WorkerKind::TpcThread,
            sites.len(),
            worker_loss_listener(run.clone()),
            &workers,
        );
        TpcServer {
            gc: recipe.spec().gc,
            threads: TpcThreads::WithWorkers {
                threads: sites.into_iter().zip(lives).collect(),
                recipe,
            },
            main: None,
            async_pool: None,
            cpus: cpus.to_vec(),
            counters,
        }
    }

    /// Returns the stop deadline the app's shutdown timeout gives.
    fn deadline(&self) -> ShutdownDeadline {
        ShutdownDeadline::new(self.recipe.spec().limits.shutdown_timeout.get())
    }
}

/// Measures throughput with each thread serving `conns_per_thread` in-memory connections, each
/// driven by a client task on the same thread.
///
/// # Errors
///
/// Returns [`BenchError`] when a thread does not start or any part of the bench fails.
pub(crate) fn run_inmem_bench(
    conns_per_thread: usize,
    duration: Duration,
    workers: BenchWorkers,
    core_ids: &[CoreId],
    request: Arc<[u8]>,
) -> Result<Measurement, BenchError> {
    serve_inmem_while(conns_per_thread, duration, workers, core_ids, request, |counter| {
        measure(counter, duration)
    })
}

/// Runs [`run_inmem_bench`] and also counts instructions and allocations, each in its own window.
///
/// # Errors
///
/// Returns [`BenchError`] when a thread does not start or any part of the bench fails.
pub(crate) fn run_inmem_profile(
    conns_per_thread: usize,
    duration: Duration,
    workers: BenchWorkers,
    core_ids: &[CoreId],
    request: Arc<[u8]>,
    depth: ProfileDepth,
) -> Result<(Measurement, ProfileReport), BenchError> {
    serve_inmem_while(conns_per_thread, duration, workers, core_ids, request, |counter| {
        measure_profiled(counter, duration, depth)
    })
}

/// Serves `conns_per_thread` in-memory connections on each server thread while `measure` reads
/// the request counter on this thread, then stops the server and joins its threads.
fn serve_inmem_while<R>(
    conns_per_thread: usize,
    duration: Duration,
    workers: BenchWorkers,
    core_ids: &[CoreId],
    request: Arc<[u8]>,
    measure: impl FnOnce(&AtomicU64) -> R,
) -> Result<R, BenchError> {
    println!(
        "\n  Mipyre v{} [in-memory bench] — {} threads × {conns_per_thread} virtual conns, {}s",
        env!("CARGO_PKG_VERSION"),
        workers.sites.len(),
        duration.as_secs()
    );
    let counter = Arc::new(AtomicU64::new(0));
    let failed: Arc<Mutex<Vec<BenchPartError>>> = Arc::default();
    let serve = {
        let (counter, failed) = (Arc::clone(&counter), Arc::clone(&failed));
        move |tpc_thread: Rc<TpcThread>, (), stop: CancellationToken, _deadline| {
            let (counter, failed, request) = (Arc::clone(&counter), Arc::clone(&failed), Arc::clone(&request));
            async move {
                let mut failures = serve_inmem(Rc::clone(&tpc_thread), conns_per_thread, counter, stop, request).await;
                failures.extend(retirement(&tpc_thread));
                failed.lock().extend(failures);
                Ok(())
            }
        }
    };
    let run = ServerRunStop::default();
    let deadline = workers.deadline();
    let threads = vec![(); workers.sites.len()];
    let served = serve_on_workers(
        workers.into_server(core_ids, &run),
        threads,
        &run,
        &deadline,
        &mut mipyre_tpc::OsSpawner,
        serve,
        || {
            let measured = measure(&counter);
            run.cancel();
            measured
        },
    );
    let failed = std::mem::take(&mut *failed.lock());
    settle(served, failed)
}

/// Measures throughput over real TCP on one ephemeral loopback port, with `client_conns`
/// pipelined clients on a separate runtime, and returns the measurement and the port.
///
/// # Errors
///
/// Returns [`BenchError`] when the port cannot be bound, a thread does not start or any part
/// of the bench fails.
pub(crate) fn run_loopback_bench(
    client_conns: usize,
    duration: Duration,
    copies: std::num::NonZeroUsize,
    workers: BenchWorkers,
    core_ids: &[CoreId],
) -> Result<(Measurement, u16), BenchError> {
    // Every worker's socket is bound before any server thread starts.
    let spec = ListenerSpec {
        addr: (Ipv4Addr::LOCALHOST, 0).into(),
        tls: None,
    };
    let listeners = Endpoints::bind(&ListenerSet::one(spec), copies, mipyre_server::bind_options())
        .map_err(BenchError::BindFailed)?;
    let addr = listeners.bound()[0].addr;
    println!(
        "\n  Mipyre v{} [loopback bench] — {} threads, {client_conns} client conns, port {}, {}s",
        env!("CARGO_PKG_VERSION"),
        workers.sites.len(),
        addr.port(),
        duration.as_secs()
    );
    let counter = Arc::new(AtomicU64::new(0));
    let failed: Arc<Mutex<Vec<BenchPartError>>> = Arc::default();
    let gc = workers.recipe.spec().gc;
    let serve = {
        let failed = Arc::clone(&failed);
        move |tpc_thread: Rc<TpcThread>, share, stop, deadline| {
            let failed = Arc::clone(&failed);
            async move {
                let served = tpc_accept_loop_inline(share, Rc::clone(&tpc_thread), stop, gc, deadline).await;
                failed.lock().extend(retirement(&tpc_thread));
                served
            }
        }
    };
    let run = ServerRunStop::default();
    let deadline = workers.deadline();
    let server = workers.into_server(core_ids, &run);
    let served = serve_on_workers(
        server,
        listeners.into_loops(),
        &run,
        &deadline,
        &mut mipyre_tpc::OsSpawner,
        serve,
        || {
            // The clients finish first, because the servers must keep serving until every client has
            // finished its batch.
            let clients = measure_clients(
                || measure(&counter, duration),
                |stop| run_loopback_clients(addr, client_conns, Arc::clone(&counter), stop),
            );
            run.cancel();
            clients
        },
    );
    let failed = std::mem::take(&mut *failed.lock());
    let (measured, client_failures) = settle(served, failed)??;
    fail_on(client_failures)?;
    Ok((measured, addr.port()))
}

/// Runs `clients` on a thread of their own while this thread runs `measure`, then stops and joins
/// them and returns the measurement with what failed on their side.
fn measure_clients(
    measure: impl FnOnce() -> Measurement,
    clients: impl FnOnce(CancellationToken) -> Vec<BenchPartError> + Send,
) -> Result<(Measurement, Vec<BenchPartError>), BenchError> {
    let stop = CancellationToken::new();
    std::thread::scope(|scope| {
        let client_stop = stop.clone();
        let running = std::thread::Builder::new()
            .name(LOOPBACK_CLIENT.to_string())
            .spawn_scoped(scope, move || clients(client_stop))
            .map_err(|source| BenchError::ThreadNotSpawned {
                thread: LOOPBACK_CLIENT.to_string(),
                source,
            })?;
        let measured = measure();
        stop.cancel();
        let failures = running.join().unwrap_or_else(|panic| {
            vec![BenchPartError::Panicked {
                thread: LOOPBACK_CLIENT.to_string(),
                payload: panic_message(panic.as_ref()),
            }]
        });
        Ok((measured, failures))
    })
}

/// Returns the bench's result: the server's failure when it had one, with the parts' failures
/// logged since they followed from it; otherwise `served` unless a part failed.
fn settle<R>(served: Result<R, ServeError>, failed: Vec<BenchPartError>) -> Result<R, BenchError> {
    match served {
        Err(serve) => {
            for failure in &failed {
                tracing::warn!(target: "mipyre::server", %failure, "a bench part failed while the server failed");
            }
            Err(BenchError::Serve(serve))
        }
        Ok(measured) => {
            fail_on(failed)?;
            Ok(measured)
        }
    }
}

fn fail_on(failures: Vec<BenchPartError>) -> Result<(), BenchError> {
    let mut failures = failures.into_iter();
    match failures.next() {
        None => Ok(()),
        Some(first) => Err(BenchError::ThreadsFailed {
            first: Box::new(first),
            rest: failures.collect(),
        }),
    }
}

/// Returns why `tpc_thread`'s worker retired, if it did, as a failure of the bench.
fn retirement(tpc_thread: &TpcThread) -> Option<BenchPartError> {
    let why = tpc_thread.worker.as_ref()?.retirement_cause()?;
    Some(BenchPartError::WorkerRetired {
        worker: tpc_thread.index.get(),
        why,
    })
}

/// Serves `conns` in-memory connections, each with its client task, until every client stops.
async fn serve_inmem(
    tpc_thread: Rc<TpcThread>,
    conns: usize,
    counter: Arc<AtomicU64>,
    stop: CancellationToken,
    request: Arc<[u8]>,
) -> Vec<BenchPartError> {
    // The server stop never fires, because a connection shutting down mid-batch would fail its
    // client.
    let server_stop = mipyre_tpc::LocalStop::never();
    let clients = (0..conns)
        .map(|_| {
            let (server_io, client_io) = tokio::io::duplex(DUPLEX_BUFFER_BYTES);
            tokio::task::spawn_local(mipyre_server::drive_conn(
                server_io,
                mipyre_transport::driver::Peer::InMemory,
                None,
                {
                    let tpc_thread = Rc::clone(&tpc_thread);
                    move |req, peer| mipyre_server::handle_request(req, Rc::clone(&tpc_thread), peer)
                },
                server_stop.stopped(),
            ));
            let (counter, stop, request) = (Arc::clone(&counter), stop.clone(), Arc::clone(&request));
            tokio::task::spawn_local(async move { drive_client(client_io, &counter, &stop, &request).await })
        })
        .collect();
    client_failures(clients).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nothing_measured() -> Measurement {
        Measurement {
            requests: 0,
            elapsed: Duration::ZERO,
        }
    }

    #[test]
    fn a_failed_part_fails_the_bench_with_its_words() {
        let failed = vec![BenchPartError::ClientFailed {
            conn: 3,
            source: ClientError::ServerClosed,
        }];
        let err = settle(Ok(()), failed).expect_err("a failed part fails the bench");
        assert_eq!(
            err.to_string(),
            "the bench failed: client connection 3: the server closed the connection"
        );
    }

    #[test]
    fn a_panicking_client_thread_fails_the_bench_with_its_panic() {
        let (_, failures) =
            measure_clients(nothing_measured, |_| panic!("client exploded")).expect("the client thread starts");
        let err = fail_on(failures).expect_err("a panicked client fails the bench");
        assert_eq!(
            err.to_string(),
            "the bench failed: thread mipyre-lb-client panicked: client exploded"
        );
    }

    #[test]
    fn the_clients_are_stopped_once_the_measurement_is_done() {
        let (_, failures) = measure_clients(nothing_measured, |stop| {
            while !stop.is_cancelled() {
                std::thread::yield_now();
            }
            Vec::new()
        })
        .expect("the client thread starts");
        assert!(failures.is_empty(), "{failures:?}");
    }
}
