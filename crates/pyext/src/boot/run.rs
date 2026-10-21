//! Builds one server's workers, pools and bridge on the main interpreter and runs the TPC or
//! multi-thread server that `Server.serve()` starts.

use std::num::NonZeroUsize;
use std::sync::Arc;

use pyo3::prelude::*;
use tokio_util::sync::CancellationToken;

use crate::boot::config::EngineSettings;
use crate::boot::topology::Topology;
use crate::runtime::{
    AbandonedWorker, InterpreterPool, LiveWorkers, PoolThreads, ServerRunStop, WorkerCounts, WorkerKind,
    WorkerProgram, WorkerRecipe,
};
use crate::serve::AcceptMode;
use crate::serve::{handle_request, handle_request_subinterp, Site, Upgrades};
use mipyre_http::plan::route::{PlaceCounts, RouteManifest};
use mipyre_cpu::{Cpus, PinOrder, PinOrderError};
use mipyre_tpc::ShutdownDeadline;
use mipyre_subinterp::gc::GcPolicy;
use mipyre_transport::driver::TransportDriver;

/// Describes the app a server's workers rebuild by executing its script.
#[derive(Clone, Debug)]
pub(crate) struct AppSource {
    /// Names `set_script_path`'s script; `None` reads `__main__.__file__` when the workers are
    /// built.
    pub(crate) script_path: Option<std::path::PathBuf>,
    /// Lists the `module:attr` routers with sub-interpreter routes, which a worker imports.
    pub(crate) routers: Vec<String>,
    /// Says whether the workers load the isolation adapter for a `runtime="isolated"` route.
    pub(crate) isolation: bool,
}

impl AppSource {
    /// Reads the program workers run, with main's `sys.path` as it is now.
    ///
    /// # Errors
    ///
    /// Returns the Python error when the script path or the program cannot be read.
    pub(crate) fn program(&self, py: Python<'_>) -> PyResult<WorkerProgram> {
        let script_path = match &self.script_path {
            Some(path) => path.clone(),
            None => crate::runtime::main_script(py)?,
        };
        let program = WorkerProgram::read(py, script_path, self.routers.clone(), self.isolation)?;
        for skipped in &program.skipped_import_path {
            tracing::warn!(
                target: "mipyre::server",
                entry = %skipped.entry,
                error = %skipped.reason,
                "a sys.path entry that is not a path is left out of the workers' sys.path"
            );
        }
        Ok(program)
    }
}

/// Keeps the worker threads an app's servers left running past the grace period, for the
/// app's caller. They are kept rather than only returned because a SIGINT's KeyboardInterrupt
/// can land in Python before a return value is stored. A clone shares the same list.
#[derive(Clone, Default)]
pub(crate) struct AbandonedWorkers(Arc<crate::sync::Mutex<Vec<AbandonedWorker>>>);

impl AbandonedWorkers {
    /// Joins a pool's worker threads by `deadline` and keeps the ones still running then.
    /// Every reference to the pool must be gone, and the caller must be detached from Python.
    fn join(&self, threads: PoolThreads, deadline: &ShutdownDeadline) -> Vec<AbandonedWorker> {
        let abandoned = threads.join(deadline);
        self.0.lock().extend(abandoned.iter().cloned());
        abandoned
    }

    /// Keeps the TPC threads a stopped server left running past its join deadline and returns
    /// them.
    pub(crate) fn keep_threads(&self, threads: Vec<String>) -> Vec<AbandonedWorker> {
        let kept: Vec<AbandonedWorker> = threads
            .into_iter()
            .map(|thread| AbandonedWorker {
                thread,
                route: None,
            })
            .collect();
        self.0.lock().extend(kept.iter().cloned());
        kept
    }

    /// Takes the workers kept so far, leaving the list empty.
    pub(crate) fn take(&self) -> Vec<AbandonedWorker> {
        std::mem::take(&mut *self.0.lock())
    }
}

/// Holds one server as `start()` resolved it, taking its connections through the transport `D`.
pub(crate) struct ServerRun<D: TransportDriver> {
    pub(crate) transport: D,
    pub(crate) site: Arc<Site>,
    /// Is cancelled by `shutdown()` or SIGINT.
    pub(crate) stop: CancellationToken,
    pub(crate) topology: Topology,
    pub(crate) tuning: EngineSettings,
    pub(crate) cpus: Cpus,
    pub(crate) pin_order: Result<PinOrder, PinOrderError>,
    pub(crate) app: AppSource,
    /// Holds the app's key-value store, which every worker of this server sees.
    pub(crate) store: crate::runtime::AppStore,
    /// Collects the app's abandoned workers, which this server's pools add theirs to.
    pub(crate) abandoned: AbandonedWorkers,
}

impl<D: TransportDriver> ServerRun<D> {
    pub(crate) fn serve(self, py: Python<'_>) -> PyResult<()> {
        // The route table holds main-interpreter `Py<T>`s, and other threads may drop their
        // clones anywhere, so this reference is the last one and drops here, attached.
        let _site_keepalive = Keepalive::new(vec![Arc::clone(&self.site)]);
        if self.stop.is_cancelled() {
            // A stop during start, such as `TestClient.close()`, leaves nothing to build.
            return Ok(());
        }
        // Every part of the server that stops it for a cause of its own (the lanes, a kind of
        // workers with none left, a failed TPC thread) records the cause here as it stops it, and
        // `serve` returns it. `Server.shutdown()` and the stop signals cancel the token itself.
        let server_stop = ServerRunStop::new(self.stop.clone());
        let deadline = ShutdownDeadline::new(self.site.rules.settings.limits.shutdown_timeout.get());
        // Stops the lanes by the deadline when it drops, however the run ends.
        let _lanes_run = match &self.site.process {
            Some(lanes) if self.site.rules.routes.uses_lane() => {
                let lanes = Arc::clone(lanes);
                let hosting = match self.topology {
                    Topology::TpcWorkers { threads, .. } => crate::lane::HostKind::Tpc {
                        threads: threads.get(),
                    },
                    _ => crate::lane::HostKind::Dedicated,
                };
                py.detach(|| lanes.start_run(hosting, server_stop.clone()))
                    .map_err(|e| {
                        lanes.stop_run(&deadline);
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "process lanes could not start: {e}"
                        ))
                    })?;
                Some(LaneRun {
                    lanes,
                    deadline: &deadline,
                })
            }
            _ => None,
        };
        let _rss = if self.tuning.metrics {
            tracing::info!(target: "mipyre::server", "Metrics enabled (MIPYRE_METRICS=1): passive GIL monitor + RSS sampler");
            match crate::telemetry::RssSamplingGuard::start() {
                Ok(guard) => Some(guard),
                // RSS sampling is passive, so the server serves without it and marks it degraded.
                Err(e) => {
                    tracing::error!(target: "mipyre::server", error = %e,
                        "RSS sampler could not start; serving without RSS metrics");
                    crate::telemetry::set_degraded(
                        crate::telemetry::Component::RssSampling,
                        format!("the sampler thread could not start: {e}"),
                    );
                    None
                }
            }
        } else {
            None
        };
        let served = match self.topology {
            Topology::TpcGil { .. } => serve_tpc_gil(py, self, &server_stop, &deadline, &mut mipyre_tpc::OsSpawner),
            Topology::TpcWorkers { threads, accept } => serve_tpc_workers(
                py,
                self,
                threads,
                accept,
                &server_stop,
                &deadline,
                &mut mipyre_tpc::OsSpawner,
            ),
            Topology::MultiThreadGil { io_threads, .. } => run_gil(py, self, io_threads, &deadline),
            Topology::Pool {
                workers,
                io_threads,
                ..
            } => run_subinterp(py, self, workers, io_threads, &server_stop, &deadline),
        };
        stopped_by(&server_stop, served)
    }
}

/// Stops a server run's process lanes by the run's stop deadline when it drops, however the run
/// ends.
struct LaneRun<'a> {
    lanes: Arc<crate::lane::ServerLanes>,
    deadline: &'a ShutdownDeadline,
}

impl Drop for LaneRun<'_> {
    fn drop(&mut self) {
        self.lanes.stop_run(self.deadline);
    }
}

/// Runs the multi-thread GIL-mode server with every handler on the main interpreter.
fn run_gil<D: TransportDriver>(
    py: Python<'_>,
    run: ServerRun<D>,
    io_threads: NonZeroUsize,
    deadline: &ShutdownDeadline,
) -> PyResult<()> {
    let ServerRun {
        transport,
        site,
        stop,
        cpus,
        ..
    } = run;
    let (io_workers, num_cpus) = (io_threads.get(), cpus.logical().get());
    let listening = transport.listening();
    tracing::info!(
        target: "mipyre::server",
        version = env!("CARGO_PKG_VERSION"),
        listening = %listening.join(", "),
        io_workers,
        cpus = num_cpus,
        mode = "gil",
        "Mipyre started"
    );
    println!("\n  Mipyre v{}", env!("CARGO_PKG_VERSION"));
    crate::serve::print_listening(&listening);
    println!("  IO workers: {io_workers} (CPUs: {num_cpus})\n");

    let counters = Arc::clone(&site.counters);
    py.detach(move || {
        crate::serve::serve_multi_thread(
            transport,
            io_workers,
            stop,
            deadline,
            counters,
            Upgrades::of(&site),
            move |req, peer| handle_request(req, Arc::clone(&site), peer),
        )
    })
}

/// Runs the multi-thread sub-interpreter server: a channel pool of workers behind Tokio accept
/// loops.
fn run_subinterp<D: TransportDriver>(
    py: Python<'_>,
    run: ServerRun<D>,
    workers: NonZeroUsize,
    io_threads: NonZeroUsize,
    server_stop: &ServerRunStop,
    deadline: &ShutdownDeadline,
) -> PyResult<()> {
    let ServerRun {
        transport,
        site,
        stop,
        tuning,
        cpus,
        app,
        store,
        abandoned,
        ..
    } = run;
    let (workers, io_workers, num_cpus) = (workers.get(), io_threads.get(), cpus.logical().get());
    let program = app.program(py)?;

    let counts = PlaceCounts::of(&site.rules.routes);
    let split = crate::runtime::split_workers_for_routes(workers, &counts)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    let gil_count = counts.main();
    let async_count_routes = counts.worker_async;
    let process_count = counts.lane;
    let subinterp_count = counts.worker_sync + counts.worker_async;
    let has_async = async_count_routes > 0;
    let mode_label = if has_async { "hybrid-async" } else { "hybrid" };

    let (pool, threads) = build_pool(py, split, &site, &tuning, &program, &store, server_stop)?;
    let pool = Arc::new(pool);
    // The banner comes after the pool starts, so a failed start never says "Listening on".
    let listening = transport.listening();
    tracing::info!(
        target: "mipyre::server",
        version = env!("CARGO_PKG_VERSION"),
        mode = mode_label,
        listening = %listening.join(", "),
        workers,
        cpus = num_cpus,
        subinterp_routes = subinterp_count,
        gil_routes = gil_count,
        async_routes = async_count_routes,
        "Mipyre started"
    );
    println!(
        "\n  Mipyre v{} [{mode_label} mode]",
        env!("CARGO_PKG_VERSION")
    );
    crate::serve::print_listening(&listening);
    println!("  Sub-interpreters: {workers} | IO threads: {io_workers} (CPUs: {num_cpus})");
    if split.async_workers > 0 {
        println!(
            "  Workers: {} sync + {} async",
            split.sync_workers, split.async_workers
        );
    }
    println!(
        "  Routes: {subinterp_count} sub-interp + {gil_count} GIL + {process_count} process + {async_count_routes} async"
    );
    println!("  Script: {}\n", program.script_path.display());

    let counters = Arc::clone(&site.counters);
    let served = py.detach(move || {
        crate::serve::serve_multi_thread(
            transport,
            io_workers,
            stop,
            deadline,
            counters,
            Upgrades::of(&site),
            move |req, peer| handle_request_subinterp(req, Arc::clone(&pool), Arc::clone(&site), peer),
        )
    });
    // The serving closures held every pool reference, so the workers now finish.
    let abandoned = py.detach(|| abandoned.join(threads, deadline));
    // The failure is not logged here, because the caller gets it with the list and logging it
    // too would record it twice.
    served.map_err(|e| noting_abandoned(py, e, &abandoned))
}

/// Returns `failed` as itself, with the abandoned workers added as a note so the caller keeps
/// its type, traceback and cause. If the note cannot attach because `__notes__` is not a list,
/// it returns a `RuntimeError` naming them, with `failed` as its `__cause__`.
fn noting_abandoned(py: Python<'_>, failed: PyErr, abandoned: &[AbandonedWorker]) -> PyErr {
    if abandoned.is_empty() {
        return failed;
    }
    let note = format!(
        "worker(s) still running after the grace period: {}",
        abandoned
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );
    match failed.add_note(py, note.as_str()) {
        Ok(()) => failed,
        Err(unattached) => {
            let err = pyo3::exceptions::PyRuntimeError::new_err(format!(
                "{note} (the note could not be attached to the error below: {unattached})"
            ));
            err.set_cause(py, Some(failed));
            err
        }
    }
}

/// Returns the server's own cause `stop` was stopped for, so the process exits non-zero, or else
/// `served`, which carries a failed TPC thread or start itself.
fn stopped_by(stop: &ServerRunStop, served: PyResult<()>) -> PyResult<()> {
    match stop.cause() {
        Some(mipyre_tpc::StopCause::Caller(cause)) => Err(pyo3::exceptions::PyRuntimeError::new_err(cause.to_string())),
        Some(
            mipyre_tpc::StopCause::ThreadFailed { .. }
            | mipyre_tpc::StopCause::NotSpawned { .. }
            | mipyre_tpc::StopCause::ThreadsEnded,
        )
        | None => served,
    }
}

/// Runs the TPC GIL server: one TPC thread per accept loop, started by `spawner`, with every
/// handler on the main interpreter.
fn serve_tpc_gil<D: TransportDriver>(
    py: Python<'_>,
    run: ServerRun<D>,
    server_stop: &ServerRunStop,
    deadline: &ShutdownDeadline,
    spawner: &mut (impl mipyre_tpc::Spawner + Send),
) -> PyResult<()> {
    let ServerRun {
        transport,
        site,
        cpus,
        pin_order,
        abandoned,
        ..
    } = run;
    let sites = thread_sites(py, &site, transport.accept_loops());
    // The copies' last references drop here, attached, as `_site_keepalive`'s do.
    let _sites_keepalive = Keepalive::new(sites.clone());
    crate::serve::report_pinning(&pin_order, sites.len(), &site.counters);
    let thread_cpus = crate::serve::thread_cpus(&pin_order);
    let banner = crate::serve::tpc_banner(
        sites.len(),
        cpus.logical(),
        &pin_order,
        true,
        0,
        PlaceCounts::of(&site.rules.routes),
    );
    py.detach(move || {
        crate::serve::run_tpc_gil(transport, sites, thread_cpus, &banner, server_stop, deadline, spawner)
    })
    .map_err(|e| tpc_failure(py, e, &abandoned))
}

/// Runs the TPC sub-interpreter server. Each TPC thread runs `def` routes inline; `async def`
/// routes go to an async pool so their awaits overlap and their 504 is sent on time; and
/// `runtime="main"` routes and the fallback go to the main-interpreter bridge.
fn serve_tpc_workers<D: TransportDriver>(
    py: Python<'_>,
    run: ServerRun<D>,
    threads: NonZeroUsize,
    accept: AcceptMode,
    server_stop: &ServerRunStop,
    deadline: &ShutdownDeadline,
    bridge_spawner: &mut impl mipyre_tpc::Spawner,
) -> PyResult<()> {
    let ServerRun {
        transport,
        site,
        tuning,
        cpus,
        pin_order,
        app,
        store,
        abandoned,
        ..
    } = run;
    let program = app.program(py)?;

    let counts = PlaceCounts::of(&site.rules.routes);
    let kinds = crate::runtime::RouteKinds::of(&counts);
    let async_workers = if counts.worker_async > 0 {
        crate::boot::topology::tpc_async_workers(threads, kinds).get()
    } else {
        0
    };
    // The pool is built first, so if the TPC workers then fail, dropping it ends its workers on
    // their own threads.
    let (async_pool, async_threads) = if async_workers > 0 {
        let split = WorkerCounts {
            sync_workers: 0,
            async_workers,
        };
        let (pool, threads) = build_pool(py, split, &site, &tuning, &program, &store, server_stop)?;
        (Some(Arc::new(pool)), Some(threads))
    } else {
        (None, None)
    };
    let end_async_pool = |async_pool: Option<Arc<crate::runtime::InterpreterPool>>,
                          threads: Option<PoolThreads>| {
        drop(async_pool);
        if let Some(threads) = threads {
            abandoned.join(threads, deadline);
        }
    };
    // An app whose routes are all process or main never executes its code in a
    // sub-interpreter; its TPC threads still parse HTTP, host the lanes and feed the bridge.
    let tpc_threads = if site.rules.routes.uses_workers() {
        let sites = thread_sites(py, &site, threads.get());
        let (_, lives) = LiveWorkers::new(
            WorkerKind::TpcThread,
            sites.len(),
            server_stop.clone(),
            Arc::clone(&site.counters),
        );
        crate::serve::TpcThreads::WithWorkers {
            threads: sites.into_iter().zip(lives).collect(),
            recipe: Arc::new(worker_recipe(
                &program,
                &site,
                tuning.gc,
                &store,
            )),
        }
    } else {
        crate::serve::TpcThreads::NoWorkers(thread_sites(py, &site, threads.get()))
    };

    let spawned = site
        .rules
        .routes
        .uses_main()
        .then(|| {
            crate::runtime::MainInterpBridge::spawn(
                py,
                &site.rules,
                tuning.bridge,
                bridge_spawner,
            )
        });
    let (bridge, bridge_threads) = match spawned.transpose() {
        Ok(Some((bridge, threads))) => (Some(bridge), Some(threads)),
        Ok(None) => (None, None),
        Err(e) => {
            py.detach(|| end_async_pool(async_pool, async_threads));
            return Err(e.into());
        }
    };
    // The copies' last references drop here, attached, as `ServerRun::serve`'s keepalive does.
    let _thread_sites_keepalive = Keepalive::new(tpc_threads.sites());
    crate::serve::report_pinning(&pin_order, threads.get(), &site.counters);
    let thread_cpus = crate::serve::thread_cpus(&pin_order);
    let banner = crate::serve::tpc_banner(
        threads.get(),
        cpus.logical(),
        &pin_order,
        site.rules.routes.uses_workers(),
        async_pool
            .as_deref()
            .map_or(0, crate::runtime::InterpreterPool::async_workers),
        counts,
    );
    let server = crate::serve::TpcServer {
        threads: tpc_threads,
        runners: crate::serve::OffThreadRunners {
            main_bridge: bridge.clone(),
            async_pool: async_pool.clone(),
        },
        gc: tuning.gc,
        cpus: thread_cpus,
        counters: Arc::clone(&site.counters),
    };

    py.detach(|| {
        let served = crate::serve::run_tpc_workers(transport, server, accept, &banner, server_stop, deadline);
        // The TPC threads are joined, so these are the last references to the pool and
        // the bridge. Both are joined by the deadline the TPC threads were, so the three
        // groups together take at most the stop's grace.
        end_async_pool(async_pool, async_threads);
        drop(bridge);
        if let Some(threads) = bridge_threads {
            abandoned.keep_threads(threads.join(deadline));
        }
        served
    })
    .map_err(|e| tpc_failure(py, e, &abandoned))
}

/// Keeps the TPC threads a stopped run left running with the app's abandoned workers and notes
/// them on the error that stopped the run.
fn tpc_failure(
    py: Python<'_>,
    failed: crate::serve::ServeError,
    abandoned: &AbandonedWorkers,
) -> PyErr {
    let kept = abandoned.keep_threads(failed.abandoned_threads().to_vec());
    noting_abandoned(py, failed.into_pyerr(py), &kept)
}

/// Holds a run's references to shared values with main-interpreter `Py<T>`s, so the last
/// reference drops on the run's thread, attached. A value another thread still holds when the
/// run ends is leaked instead: that thread would drop it with no Python thread state, where PyO3
/// panics, possibly while the process finalizes.
struct Keepalive<T>(Vec<Arc<T>>);

impl<T> Keepalive<T> {
    fn new(held: Vec<Arc<T>>) -> Self {
        Keepalive(held)
    }
}

impl<T> Drop for Keepalive<T> {
    fn drop(&mut self) {
        for held in self.0.drain(..) {
            // A value still shared is never dropped, so it is never dropped on the other holder's
            // thread.
            if let Err(shared) = Arc::try_unwrap(held) {
                std::mem::forget(shared);
            }
        }
    }
}

/// Makes `n` copies of `site`, one per TPC thread, each with its own references to the
/// handlers.
pub(crate) fn thread_sites(py: Python<'_>, site: &Site, n: usize) -> Vec<Arc<Site>> {
    (0..n).map(|_| Arc::new(site.for_thread(py))).collect()
}

/// Builds the recipe every worker of a server is created from.
pub(crate) fn worker_recipe(
    program: &WorkerProgram,
    site: &Site,
    gc: GcPolicy,
    store: &crate::runtime::AppStore,
) -> WorkerRecipe {
    let expected = RouteManifest::of(&site.rules.routes);
    WorkerRecipe::new(
        program,
        &expected,
        store,
        gc,
        site.rules.settings.limits,
    )
}

/// Builds a channel pool of `split` workers, each on its own thread, and returns its threads to
/// join once every reference to the pool is gone. When a kind of worker has none left serving,
/// `stop` is stopped.
///
/// # Errors
///
/// Returns the Python error a worker failed to start with.
fn build_pool(
    py: Python<'_>,
    split: WorkerCounts,
    site: &Site,
    tuning: &EngineSettings,
    program: &WorkerProgram,
    store: &crate::runtime::AppStore,
    stop: &ServerRunStop,
) -> PyResult<(InterpreterPool, PoolThreads)> {
    let recipe = Arc::new(worker_recipe(
        program,
        site,
        tuning.gc,
        store,
    ));
    InterpreterPool::new(
        py,
        split,
        &recipe,
        tuning.async_queue_capacity,
        stop,
        &site.counters,
    )
        .map_err(|e| e.into_pyerr(py))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mipyre_transport::driver::AcceptError;
    use std::time::Duration;

    struct Counted(Arc<std::sync::atomic::AtomicUsize>);

    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn a_keepalive_drops_what_it_holds_last_and_leaks_what_another_holder_still_has() {
        let alone_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let shared_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let alone = Arc::new(Counted(Arc::clone(&alone_drops)));
        let shared = Arc::new(Counted(Arc::clone(&shared_drops)));
        let late_holder = Arc::clone(&shared);
        drop(Keepalive::new(vec![alone, shared]));
        assert_eq!(alone_drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(shared_drops.load(std::sync::atomic::Ordering::SeqCst), 0);
        drop(late_holder);
        assert_eq!(
            shared_drops.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the late holder's drop was not the last"
        );
    }

    /// Starts every thread on the OS except the one at the given index, which it refuses with the
    /// given words, because the OS cannot be made to refuse a thread on demand.
    struct RefuseAt(usize, &'static str);

    impl mipyre_tpc::Spawner for RefuseAt {
        fn spawn<R: Send + 'static>(
            &mut self,
            index: usize,
            builder: std::thread::Builder,
            body: impl FnOnce() -> R + Send + 'static,
        ) -> std::io::Result<mipyre_tpc::OsThread<R>> {
            if index == self.0 {
                return Err(std::io::Error::other(self.1));
            }
            mipyre_tpc::spawn(builder, body)
        }
    }

    /// Builds an in-memory one-thread TPC run whose only handler is a main-interpreter fallback,
    /// already stopped, so it stops as soon as it is up. The returned connector keeps its
    /// in-memory listener open.
    fn bridge_only_run(
        py: Python<'_>,
    ) -> (
        ServerRun<mipyre_transport::memory::MemorySource>,
        mipyre_transport::memory::MemoryConnector,
    ) {
        let mut routes = mipyre_http::plan::route::RouteTable::new();
        routes.set_fallback(py.None());
        let site = Arc::new(Site {
            rules: mipyre_http::plan::site::SiteRules {
                routes,
                settings: mipyre_http::plan::site::ServeSettings::defaults(),
                access_log: None,
            },
            ws_connections: crate::serve::OpenConnections::default(),
            process: None,
            counters: Arc::new(crate::telemetry::RunCounters::new(false)),
        });
        let one = NonZeroUsize::new(1).unwrap();
        let mut tuning = crate::boot::config::EnvConfig::parse(|_| None)
            .expect("an empty environment parses")
            .tuning;
        tuning.bridge = crate::runtime::BridgeConfig {
            workers: NonZeroUsize::new(2).unwrap(),
            capacity: one,
        };
        let crate::boot::ServerCpus { cpus, pin_order } = crate::boot::host_cpus();
        let (transport, connector) = mipyre_transport::memory::memory_listeners(one);
        let stop = CancellationToken::new();
        stop.cancel();
        let run = ServerRun {
            transport,
            site,
            stop,
            topology: Topology::TpcWorkers {
                threads: one,
                accept: AcceptMode::PerThreadListener,
            },
            tuning,
            cpus,
            pin_order,
            app: AppSource {
                // The script is read but never run, because the app has no sub-interpreter route.
                script_path: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/src/boot/run.rs").into()),
                routers: Vec::new(),
                isolation: false,
            },
            store: crate::runtime::AppStore::default(),
            abandoned: AbandonedWorkers::default(),
        };
        (run, connector)
    }

    /// Returns the messages of the events `f` emits on this thread.
    fn events_of<T>(f: impl FnOnce() -> T) -> (T, Vec<String>) {
        #[derive(Clone, Default)]
        struct Messages(Arc<std::sync::Mutex<Vec<String>>>);
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Messages {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                struct Message<'a>(&'a mut Vec<String>);
                impl tracing::field::Visit for Message<'_> {
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &dyn std::fmt::Debug,
                    ) {
                        if field.name() == "message" {
                            self.0.push(format!("{value:?}"));
                        }
                    }
                }
                event.record(&mut Message(&mut self.0.lock().unwrap()));
            }
        }
        use tracing_subscriber::layer::SubscriberExt;
        let messages = Messages::default();
        let subscriber = tracing_subscriber::registry().with(messages.clone());
        let out = tracing::subscriber::with_default(subscriber, f);
        let seen = messages.0.lock().unwrap().clone();
        (out, seen)
    }

    #[test]
    fn a_bridge_thread_that_cannot_start_fails_the_start_before_the_server_announces_itself() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let (run, _connector) = bridge_only_run(py);
            let server_stop = ServerRunStop::new(run.stop.clone());
            let (served, events) = events_of(|| {
                serve_tpc_workers(
                    py,
                    run,
                    NonZeroUsize::new(1).unwrap(),
                    AcceptMode::PerThreadListener,
                    &server_stop,
                    &ShutdownDeadline::new(Duration::from_secs(5)),
                    &mut RefuseAt(1, "no thread for the bridge"),
                )
            });
            let Err(raised) = served else {
                panic!("the second bridge thread was refused, and the start went on: {events:?}");
            };
            assert_eq!(
                raised.value(py).str().unwrap().to_string(),
                "could not start main-interpreter bridge thread 1 of 2: no thread for the bridge"
            );
            assert!(!events.iter().any(|m| m == "Mipyre started"), "{events:?}");
        });
    }

    /// Proves the event the test above looks for is the one a successful start emits.
    #[test]
    fn a_tpc_start_whose_bridge_starts_announces_itself() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let (run, _connector) = bridge_only_run(py);
            let server_stop = ServerRunStop::new(run.stop.clone());
            let (served, events) = events_of(|| {
                serve_tpc_workers(
                    py,
                    run,
                    NonZeroUsize::new(1).unwrap(),
                    AcceptMode::PerThreadListener,
                    &server_stop,
                    &ShutdownDeadline::new(Duration::from_secs(5)),
                    &mut mipyre_tpc::OsSpawner,
                )
            });
            served.unwrap_or_else(|e| panic!("the stopped run ended cleanly: {e}"));
            assert!(events.iter().any(|m| m == "Mipyre started"), "{events:?}");
        });
    }

    #[test]
    fn a_gil_tpc_thread_that_cannot_start_fails_the_start_before_the_server_announces_itself() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let (run, _connector) = bridge_only_run(py);
            let server_stop = ServerRunStop::new(run.stop.clone());
            let deadline = ShutdownDeadline::new(Duration::from_secs(5));
            let (served, events) = events_of(|| serve_tpc_gil(py, run, &server_stop, &deadline, &mut RefuseAt(0, "no thread for TPC")));
            let Err(raised) = served else {
                panic!("the TPC thread was refused, and the start went on: {events:?}");
            };
            assert_eq!(raised.value(py).str().unwrap().to_string(), "could not spawn thread mipyre-tpc-0: no thread for TPC");
            assert!(!events.iter().any(|m| m == "Mipyre started"), "{events:?}");
        });
    }

    /// Proves the event the test above looks for is the one a successful start emits.
    #[test]
    fn a_gil_tpc_start_whose_threads_start_announces_itself() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let (run, _connector) = bridge_only_run(py);
            let server_stop = ServerRunStop::new(run.stop.clone());
            let deadline = ShutdownDeadline::new(Duration::from_secs(5));
            let (served, events) = events_of(|| serve_tpc_gil(py, run, &server_stop, &deadline, &mut mipyre_tpc::OsSpawner));
            served.unwrap_or_else(|e| panic!("the stopped run ended cleanly: {e}"));
            assert!(events.iter().any(|m| m == "Mipyre started"), "{events:?}");
        });
    }

    fn stuck(thread: &str) -> AbandonedWorker {
        AbandonedWorker {
            thread: thread.to_owned(),
            route: None,
        }
    }

    #[test]
    fn a_serving_error_keeps_its_type_and_cause_and_notes_the_abandoned_workers() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let cause = pyo3::exceptions::PyValueError::new_err("the root cause");
            let failed = pyo3::exceptions::PyOSError::new_err("listener broke");
            failed.set_cause(py, Some(cause));
            let raised = noting_abandoned(py, failed, &[stuck("w-1"), stuck("w-2")]);
            assert!(
                raised.is_instance_of::<pyo3::exceptions::PyOSError>(py),
                "{raised}"
            );
            assert_eq!(
                raised.value(py).str().unwrap().to_string(),
                "listener broke"
            );
            let cause = raised.cause(py).expect("the cause is kept");
            assert!(cause.is_instance_of::<pyo3::exceptions::PyValueError>(py));
            let notes: Vec<String> = raised
                .value(py)
                .getattr("__notes__")
                .and_then(|n| n.extract())
                .unwrap_or_else(|e| panic!("no notes on {raised}: {e}"));
            assert_eq!(
                notes,
                vec!["worker(s) still running after the grace period: w-1, w-2".to_string()]
            );
        });
    }

    #[test]
    fn an_abandoned_tpc_thread_alone_is_kept_and_named_on_the_error() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let abandoned = AbandonedWorkers::default();
            let failed = crate::serve::ServeError::ThreadsStopped {
                root: None,
                abandoned: vec!["mipyre-tpc-1".to_string()],
            };
            let raised = tpc_failure(py, failed, &abandoned);
            let kept: Vec<String> = abandoned.take().into_iter().map(|w| w.thread).collect();
            assert_eq!(kept, vec!["mipyre-tpc-1".to_string()]);
            assert!(raised.to_string().contains("mipyre-tpc-1"), "{raised}");
        });
    }

    /// Proves the TPC run raises the same `OSError` as the multi-thread server, whether the
    /// stopped accept loop is the run's error or the root cause of threads left running.
    #[test]
    fn a_tpc_accept_loop_that_stopped_raises_the_multi_thread_oserror() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let multi_thread = crate::serve::accept_stopped_error(&AcceptError::ConnectorsGone);
            let failures = [
                crate::serve::ServeError::AcceptStopped(AcceptError::ConnectorsGone),
                crate::serve::ServeError::ThreadsStopped {
                    root: Some(Box::new(crate::serve::ServeError::AcceptStopped(
                        AcceptError::ConnectorsGone,
                    ))),
                    abandoned: vec![],
                },
            ];
            for failed in failures {
                let raised = tpc_failure(py, failed, &AbandonedWorkers::default());
                assert!(
                    raised.is_instance_of::<pyo3::exceptions::PyOSError>(py),
                    "{raised}"
                );
                assert_eq!(raised.to_string(), multi_thread.to_string());
            }
        });
    }

    #[test]
    fn a_serving_error_with_no_abandoned_worker_is_unchanged() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let raised = noting_abandoned(
                py,
                pyo3::exceptions::PyOSError::new_err("listener broke"),
                &[],
            );
            assert!(raised.is_instance_of::<pyo3::exceptions::PyOSError>(py));
            assert!(!raised.value(py).hasattr("__notes__").unwrap());
        });
    }
}
