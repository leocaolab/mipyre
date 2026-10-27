//! Builds one server's workers, pools and main-interpreter threads on the main interpreter and
//! runs the TPC or multi-thread server that `Server.serve()` starts.

use std::num::NonZeroUsize;
use std::sync::Arc;

use pyo3::prelude::*;
use tokio_util::sync::CancellationToken;

use crate::boot::config::EngineSettings;
use crate::boot::serve_exception::serve_exception;
use crate::boot::topology::Topology;
use mipyre_cpu::{Cpus, PinOrder, PinOrderError};
use mipyre_http::plan::route::{PlaceCounts, RouteManifest};
use mipyre_process::LaneRunEvent;
use mipyre_runtime::{
    AbandonedWorker, CommandLine, InterpreterPool, LiveWorkers, MainThreadHandles, MainThreads, PoolError, PoolThreads,
    WorkerCounters, WorkerCounts, WorkerKind, WorkerKindLoss, WorkerProgram, WorkerRecipe,
};
use mipyre_server::{upgrade_target, IoThreadServers, MultiThreadServer, Site};
use mipyre_server::{AcceptMode, Disposition, ServerRunStop, ServerStop, StopSignalDispositions};
use mipyre_subinterp::gc::GcPolicy;
use mipyre_tpc::ShutdownDeadline;
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

/// An app with `runtime="isolated"` routes, but no directory to keep its workers' copies in.
#[derive(Debug, thiserror::Error)]
#[error(
    "this app has runtime=\"isolated\" routes, whose workers keep copies of extensions in a \
     directory of this user's, but there is none: this user has no home directory to find a \
     cache directory under. Set isolate_dir (MIPYRE_ISOLATE_DIR, --isolate-dir) to a directory \
     this user owns"
)]
pub(crate) struct NoIsolateDirError;

impl AppSource {
    /// Reads the program workers run, with main's `sys.path` as it is now and `command_line`;
    /// a worker of an app with isolated routes keeps its copies in `isolate_dir`.
    ///
    /// # Errors
    ///
    /// Returns the Python error when the script path or the program cannot be read, or when
    /// the app isolates and there is no `isolate_dir`.
    pub(crate) fn program(
        &self,
        py: Python<'_>,
        command_line: CommandLine,
        isolate_dir: Option<&std::path::Path>,
    ) -> PyResult<WorkerProgram> {
        let isolation = match (self.isolation, isolate_dir) {
            (false, _) => None,
            (true, Some(dir)) => Some(dir.to_path_buf()),
            (true, None) => return Err(pyo3::exceptions::PyRuntimeError::new_err(NoIsolateDirError.to_string())),
        };
        let script_path = mipyre_runtime::app_file(py, self.script_path.as_deref())?;
        let program = WorkerProgram::read(py, script_path, command_line, self.routers.clone(), isolation)?;
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
        self.keep(&abandoned);
        abandoned
    }

    /// Joins the main interpreter's threads by `deadline` and keeps the ones still running then.
    /// Every [`MainThreads`] must be gone, and the caller must be detached from Python.
    fn join_main(&self, handles: Option<MainThreadHandles>, deadline: &ShutdownDeadline) {
        if let Some(handles) = handles {
            self.keep_threads(handles.join(deadline));
        }
    }

    /// Keeps `workers`, which a failed pool start left running.
    fn keep(&self, workers: &[AbandonedWorker]) {
        self.0.lock().extend(workers.iter().cloned());
    }

    /// Keeps the TPC threads a stopped server left running past its join deadline and returns
    /// them.
    pub(crate) fn keep_threads(&self, threads: Vec<String>) -> Vec<AbandonedWorker> {
        let kept: Vec<AbandonedWorker> = threads
            .into_iter()
            .map(|thread| AbandonedWorker { thread, route: None })
            .collect();
        self.0.lock().extend(kept.iter().cloned());
        kept
    }

    /// Takes the workers kept so far, leaving the list empty.
    pub(crate) fn take(&self) -> Vec<AbandonedWorker> {
        std::mem::take(&mut *self.0.lock())
    }

    fn any(&self) -> bool {
        !self.0.lock().is_empty()
    }
}

/// What `Server.serve(on_serving=)` was given: a callable told once, on the serving thread, that
/// the server is ready (every worker is up and connections are taken next), and what it raised
/// there if the run's thread was detached. It raising is a failed start: the run stops and
/// `serve()` raises that error.
#[derive(Default)]
pub(crate) struct ReadinessCallback {
    callback: Option<Py<PyAny>>,
    raised: crate::sync::Mutex<Option<PyErr>>,
}

impl ReadinessCallback {
    pub(crate) fn new(callback: Option<Py<PyAny>>) -> Self {
        ReadinessCallback {
            callback,
            raised: crate::sync::Mutex::new(None),
        }
    }

    /// Calls the callback, attached to the main interpreter from any state of this thread.
    fn call(&self) -> PyResult<()> {
        match &self.callback {
            Some(callback) => mipyre_subinterp::attach::with_main(|py| callback.call0(py).map(drop)),
            None => Ok(()),
        }
    }

    /// Calls the callback from a run that is already serving; if it raises, keeps the error
    /// and stops `stop`'s run.
    fn call_or_stop(&self, stop: &ServerRunStop) {
        if let Err(raised) = self.call() {
            *self.raised.lock() = Some(raised);
            stop.cancel();
        }
    }

    /// Returns what the callback raised in [`ReadinessCallback::call_or_stop`], else `served`.
    fn raised_or(self, served: PyResult<()>) -> PyResult<()> {
        match self.raised.into_inner() {
            Some(raised) => Err(raised),
            None => served,
        }
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
    /// Is where this run's workers keep their copies of isolated extensions (see
    /// [`ServerRecipe::isolate_dir`](crate::boot::ServerRecipe::isolate_dir)).
    pub(crate) isolate_dir: Option<std::path::PathBuf>,
    /// Is what this run's workers get as their `sys.argv` and flag values.
    pub(crate) command_line: CommandLine,
    /// Holds the app's key-value store, which every worker of this server sees.
    pub(crate) store: mipyre_runtime::AppStore,
    /// Collects the app's abandoned workers, which this server's pools add theirs to.
    pub(crate) abandoned: AbandonedWorkers,
    /// Is told when the server is ready; `serve()` sets it.
    pub(crate) readiness: ReadinessCallback,
    /// Counts this run's workers; the server's own counts are its site's.
    pub(crate) workers: Arc<WorkerCounters>,
}

/// Reads how Python handles SIGINT and SIGTERM: `SIG_DFL` is the OS default; a handler, `SIG_IGN`
/// or a handler Python did not install (`None`) is another.
fn stop_signal_dispositions(py: Python<'_>) -> PyResult<StopSignalDispositions> {
    let signal = py.import("signal")?;
    let default = signal.getattr("SIG_DFL")?;
    let disposition = |name: &str| -> PyResult<Disposition> {
        let handler = signal.call_method1("getsignal", (signal.getattr(name)?,))?;
        Ok(if handler.is(&default) {
            Disposition::Default
        } else {
            Disposition::Other
        })
    };
    Ok(StopSignalDispositions {
        interrupt: disposition("SIGINT")?,
        terminate: disposition("SIGTERM")?,
    })
}

/// Closes a run's Postgres pools when it drops, waiting with the GIL released.
struct RunPoolsGuard<'py> {
    pools: Arc<mipyre_pg::RunPools>,
    py: Python<'py>,
}

impl Drop for RunPoolsGuard<'_> {
    fn drop(&mut self) {
        let pools = &self.pools;
        self.py.detach(|| pools.close());
    }
}

impl<D: TransportDriver> ServerRun<D> {
    /// Serves until stopped, telling `readiness` once the server is ready.
    pub(crate) fn serve(mut self, py: Python<'_>, readiness: ReadinessCallback) -> PyResult<()> {
        self.readiness = readiness;
        // The route table holds main-interpreter `Py<T>`s, and other threads may drop their
        // clones anywhere, so this reference is the last one and drops here, attached.
        let _site_keepalive = Keepalive::new(vec![Arc::clone(&self.site)]);
        // The run's Postgres pools close when it stops, however it stops.
        let _pg = self.site.pg.clone().map(|pools| RunPoolsGuard { pools, py });
        if self.stop.is_cancelled() {
            // A stop during start, such as `TestClient.close()`, leaves nothing to build.
            return Ok(());
        }
        // Every part of the server that stops it for a cause of its own (the lanes, a kind of
        // workers with none left, a failed TPC thread) records the cause here as it stops it, and
        // `serve` returns it. `Server.shutdown()` and the stop signals cancel the token itself.
        let server_stop = ServerRunStop::new(self.stop.clone());
        let deadline = ShutdownDeadline::new(self.site.rules.settings.limits.shutdown_timeout.get());
        let counters = Arc::clone(&self.site.counters.run);
        let abandoned = self.abandoned.clone();
        // Watched from here, a stop signal that comes while the lanes or the workers start stops
        // the run once they are up, rather than reaching only Python, which would not see it
        // until the server it never stopped ends.
        let before = stop_signal_dispositions(py)?;
        let served = mipyre_server::watching_stop_signals(&server_stop, &counters, &deadline, before, || {
            self.start_and_serve(py, &server_stop, &deadline)
        })
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        if !abandoned.any() {
            return served;
        }
        // Armed before returning to Python, where a shutdown hook may hang or exit the process.
        match (served, super::exit::decide_exit_before_finalizing(py)) {
            (served, Ok(())) => served,
            (Ok(()), Err(unarmed)) => Err(unarmed),
            (Err(failed), Err(unarmed)) => {
                unarmed.set_cause(py, Some(failed));
                Err(unarmed)
            }
        }
    }

    /// Starts the lanes and the workers and serves until `server_stop`; the run's own cause, if
    /// one stopped it.
    fn start_and_serve(self, py: Python<'_>, server_stop: &ServerRunStop, deadline: &ShutdownDeadline) -> PyResult<()> {
        // Stops the lanes by the deadline when it drops, however the run ends.
        let _lanes_run = match &self.site.process {
            Some(lanes) if self.site.rules.routes.uses_lane() => {
                let lanes = Arc::clone(lanes);
                let hosting = match self.topology {
                    Topology::Tpc { threads, .. } => mipyre_process::HostKind::Tpc { threads: threads.get() },
                    _ => mipyre_process::HostKind::Dedicated,
                };
                let tell_run = lane_run_listener(server_stop.clone(), Arc::clone(&self.workers));
                py.detach(|| lanes.start_run(hosting, tell_run)).map_err(|e| {
                    lanes.stop_run(deadline);
                    pyo3::exceptions::PyRuntimeError::new_err(format!("process lanes could not start: {e}"))
                })?;
                Some(LaneRun { lanes, deadline })
            }
            _ => None,
        };
        let _rss = if self.tuning.metrics {
            tracing::info!(target: "mipyre::server", "Metrics enabled (MIPYRE_METRICS=1): passive GIL monitor + RSS sampler");
            match crate::telemetry::RssSamplingGuard::start() {
                Ok(guard) => Some(guard),
                // RSS sampling is passive, so the server serves without it; the sampler lists
                // itself degraded.
                Err(e) => {
                    tracing::error!(target: "mipyre::server", error = %e,
                        "RSS sampler could not start; serving without RSS metrics");
                    None
                }
            }
        } else {
            None
        };
        let served = match self.topology {
            Topology::Tpc { threads, accept } => serve_tpc(
                py,
                self,
                threads,
                accept,
                server_stop,
                deadline,
                &mut mipyre_tpc::OsSpawner,
            ),
            Topology::MultiThreadGil { io_threads, .. } => {
                run_gil(py, self, io_threads, deadline, &mut mipyre_tpc::OsSpawner)
            }
            Topology::Pool {
                workers, io_threads, ..
            } => run_subinterp(
                py,
                self,
                workers,
                io_threads,
                server_stop,
                deadline,
                &mut mipyre_tpc::OsSpawner,
            ),
        };
        stopped_by(server_stop, served)
    }
}

/// Stops a server run's process lanes by the run's stop deadline when it drops, however the run
/// ends.
struct LaneRun<'a> {
    lanes: Arc<mipyre_process::ServerLanes>,
    deadline: &'a ShutdownDeadline,
}

impl Drop for LaneRun<'_> {
    fn drop(&mut self) {
        self.lanes.stop_run(self.deadline);
    }
}

/// Starts watching the static mounts' files in the background, so a mount that cannot be watched
/// says so before the first request and is served uncached. It is called once the workers have
/// made their interpreters: the watch's thread ends whenever its watch is up, a thread started
/// around then either takes over that stack or maps a new one, and which of the two happens would
/// move the workers' Python arenas and with them how many instructions freeing an object takes.
fn start_static_watches(site: &Site) {
    for mount in site.rules.routes.static_mounts() {
        mount.start_watching();
    }
}

/// Starts the main interpreter's threads when the table has a main-interpreter route.
///
/// # Errors
///
/// Returns why a thread did not start, with the ones started joined.
fn spawn_main(
    py: Python<'_>,
    site: &Site,
    tuning: &EngineSettings,
    spawner: &mut impl mipyre_tpc::Spawner,
) -> PyResult<(Option<MainThreads>, Option<MainThreadHandles>)> {
    if !site.rules.routes.uses_main() {
        return Ok((None, None));
    }
    let (main, handles) = MainThreads::spawn(py, &site.rules, tuning.main, spawner)?;
    Ok((Some(main), Some(handles)))
}

/// Runs the multi-thread GIL-mode server with every handler on the main interpreter's threads.
fn run_gil<D: TransportDriver>(
    py: Python<'_>,
    run: ServerRun<D>,
    io_threads: NonZeroUsize,
    deadline: &ShutdownDeadline,
    spawner: &mut impl mipyre_tpc::Spawner,
) -> PyResult<()> {
    let ServerRun {
        transport,
        site,
        stop,
        tuning,
        cpus,
        abandoned,
        readiness,
        ..
    } = run;
    let (main, main_handles) = spawn_main(py, &site, &tuning, spawner)?;
    let num_cpus = cpus.logical().get();
    let listening = transport.listening();
    tracing::info!(
        target: "mipyre::server",
        version = env!("CARGO_PKG_VERSION"),
        listening = %listening.join(", "),
        io_threads = io_threads.get(),
        cpus = num_cpus,
        topology = "main",
        "Mipyre started"
    );
    println!("\n  Mipyre v{}", env!("CARGO_PKG_VERSION"));
    mipyre_server::print_listening(&listening);
    println!("  IO threads: {io_threads} (CPUs: {num_cpus})\n");
    start_static_watches(&site);
    if let Err(failed) = readiness.call() {
        drop(main);
        py.detach(|| abandoned.join_main(main_handles, deadline));
        return Err(failed);
    }

    let counters = Arc::clone(&site.counters.run);
    let ws = upgrade_target(&site, None, main.as_ref());
    let servers = io_thread_servers(py, &site, io_threads, None, main.map(Arc::new));
    // The copies' last references drop here, attached, as `ServerRun::serve`'s keepalive does.
    let _io_thread_sites_keepalive = Keepalive::new(servers.sites());
    let served = py.detach(move || mipyre_server::serve_multi_thread(transport, servers, stop, deadline, counters, ws));
    // The serving closures held every reference to the main interpreter's threads, so they now
    // finish.
    py.detach(|| abandoned.join_main(main_handles, deadline));
    served.map_err(|e| serve_exception(py, e))
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
    spawner: &mut impl mipyre_tpc::Spawner,
) -> PyResult<()> {
    let ServerRun {
        transport,
        site,
        stop,
        tuning,
        cpus,
        app,
        isolate_dir,
        command_line,
        store,
        abandoned,
        readiness,
        workers: worker_counters,
        ..
    } = run;
    let (workers, num_cpus) = (workers.get(), cpus.logical().get());
    let program = app.program(py, command_line, isolate_dir.as_deref())?;

    let counts = PlaceCounts::of(&site.rules.routes);
    let split = mipyre_runtime::split_workers_for_routes(workers, &counts)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    let gil_count = counts.main();
    let async_count_routes = counts.worker_async;
    let process_count = counts.lane;
    let subinterp_count = counts.worker_sync + counts.worker_async;
    let has_async = async_count_routes > 0;
    let topology = if has_async { "pool-async" } else { "pool" };

    let recipe = Arc::new(worker_recipe(&program, &site, tuning.gc, &store));
    let (pool, threads) = build_pool(py, split, &recipe, &site, &tuning, server_stop, &worker_counters)
        .map_err(|e| pool_failure(py, e, &abandoned))?;
    let (main, main_handles) = match spawn_main(py, &site, &tuning, spawner) {
        Ok(spawned) => spawned,
        Err(failed) => {
            drop(pool);
            let abandoned = py.detach(|| abandoned.join(threads, deadline));
            return Err(noting_abandoned(py, failed, &abandoned));
        }
    };
    // The banner comes after the pool starts, so a failed start never says "Listening on".
    let listening = transport.listening();
    tracing::info!(
        target: "mipyre::server",
        version = env!("CARGO_PKG_VERSION"),
        topology,
        listening = %listening.join(", "),
        workers,
        cpus = num_cpus,
        subinterp_routes = subinterp_count,
        gil_routes = gil_count,
        async_routes = async_count_routes,
        "Mipyre started"
    );
    println!("\n  Mipyre v{}", env!("CARGO_PKG_VERSION"));
    mipyre_server::print_listening(&listening);
    println!("  Sub-interpreters: {workers} | IO threads: {io_threads} (CPUs: {num_cpus})");
    if split.async_workers > 0 {
        println!("  Workers: {} sync + {} async", split.sync_workers, split.async_workers);
    }
    println!(
        "  Routes: {subinterp_count} sub-interp + {gil_count} main + {process_count} process + {async_count_routes} async"
    );
    println!("  Script: {}\n", program.script_path.display());
    start_static_watches(&site);
    if let Err(failed) = readiness.call() {
        // Nothing serves yet, so the backends' last references drop here and their threads finish.
        drop((pool, main));
        let abandoned = py.detach(|| {
            abandoned.join_main(main_handles, deadline);
            abandoned.join(threads, deadline)
        });
        return Err(noting_abandoned(py, failed, &abandoned));
    }

    let counters = Arc::clone(&site.counters.run);
    let ws = upgrade_target(&site, Some(&pool), main.as_ref());
    let servers = io_thread_servers(py, &site, io_threads, Some(Arc::new(pool)), main.map(Arc::new));
    // The copies' last references drop here, attached, as `ServerRun::serve`'s keepalive does.
    let _io_thread_sites_keepalive = Keepalive::new(servers.sites());
    let served = py.detach(move || mipyre_server::serve_multi_thread(transport, servers, stop, deadline, counters, ws));
    // The serving closures held every reference to the backends, so their threads now finish.
    let abandoned = py.detach(|| {
        abandoned.join_main(main_handles, deadline);
        abandoned.join(threads, deadline)
    });
    // The failure is not logged here, because the caller gets it with the list and logging it
    // too would record it twice.
    served.map_err(|e| noting_abandoned(py, serve_exception(py, e), &abandoned))
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
        abandoned.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
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

/// Hears what a server's lanes tell their run: a worker's death counts in the run's worker counts,
/// and the lanes' failure stops the server with its cause.
fn lane_run_listener(stop: ServerRunStop, workers: Arc<WorkerCounters>) -> Box<dyn Fn(LaneRunEvent) + Send + Sync> {
    Box::new(move |event| match event {
        LaneRunEvent::WorkerDied => workers.process_worker_died(),
        LaneRunEvent::Failed(cause) => stop.stop(ServerStop::LanesFailed(cause)),
    })
}

/// Hears a kind of workers' loss: its routes can never be served again, so the server stops with
/// it.
pub(crate) fn worker_loss_listener(stop: ServerRunStop) -> Arc<dyn Fn(WorkerKindLoss) + Send + Sync> {
    Arc::new(move |loss| {
        let cause = ServerStop::NoWorkerLeft(loss);
        tracing::error!(target: "mipyre::server", error = %cause, "the server stops");
        stop.stop(cause);
    })
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

/// Runs the TPC server, its main-interpreter threads and then its TPC threads started by `spawner`. With a
/// sub-interpreter route each TPC thread has a worker and runs `def` routes inline on it, and
/// `async def` routes go to an async pool so their awaits overlap and their 504 is sent on time;
/// without one the threads have no worker. Either way `runtime="main"` routes and the fallback go
/// to the main interpreter's threads, whose full queue answers 503.
fn serve_tpc<D: TransportDriver>(
    py: Python<'_>,
    run: ServerRun<D>,
    threads: NonZeroUsize,
    accept: AcceptMode,
    server_stop: &ServerRunStop,
    deadline: &ShutdownDeadline,
    spawner: &mut (impl mipyre_tpc::Spawner + Send),
) -> PyResult<()> {
    let ServerRun {
        transport,
        site,
        tuning,
        cpus,
        pin_order,
        app,
        isolate_dir,
        command_line,
        store,
        abandoned,
        readiness,
        workers: worker_counters,
        ..
    } = run;
    let counts = PlaceCounts::of(&site.rules.routes);
    // Workers rebuild the app by executing its script, so it is read only when a route runs on
    // one: an app made in a function or at a prompt has no script to give them. The async pool is
    // built before the TPC threads, so if they then fail, dropping it ends its workers on their
    // own threads.
    let (tpc_threads, async_pool, async_threads) = if site.rules.routes.uses_workers() {
        let program = app.program(py, command_line, isolate_dir.as_deref())?;
        let (async_pool, async_threads) = if counts.worker_async > 0 {
            let kinds = mipyre_runtime::RouteKinds::of(&counts);
            let split = WorkerCounts {
                sync_workers: 0,
                async_workers: crate::boot::topology::tpc_async_workers(threads, kinds).get(),
            };
            let recipe = Arc::new(worker_recipe(&program, &site, tuning.gc, &store));
            let (pool, threads) = build_pool(py, split, &recipe, &site, &tuning, server_stop, &worker_counters)
                .map_err(|e| pool_failure(py, e, &abandoned))?;
            (Some(Arc::new(pool)), Some(threads))
        } else {
            (None, None)
        };
        let sites = thread_sites(py, &site, threads.get());
        let (_, lives) = LiveWorkers::new(
            WorkerKind::TpcThread,
            sites.len(),
            worker_loss_listener(server_stop.clone()),
            &worker_counters,
        );
        let with_workers = mipyre_server::TpcThreads::WithWorkers {
            threads: sites.into_iter().zip(lives).collect(),
            recipe: Arc::new(worker_recipe(&program, &site, tuning.gc, &store)),
        };
        (with_workers, async_pool, async_threads)
    } else {
        // The TPC threads parse HTTP, host the lanes and feed the main interpreter's threads.
        (
            mipyre_server::TpcThreads::NoWorkers(thread_sites(py, &site, threads.get())),
            None,
            None,
        )
    };
    let end_async_pool = |async_pool: Option<Arc<mipyre_runtime::InterpreterPool>>, threads: Option<PoolThreads>| {
        drop(async_pool);
        if let Some(threads) = threads {
            abandoned.join(threads, deadline);
        }
    };

    let (main, main_handles) = match spawn_main(py, &site, &tuning, spawner) {
        Ok((main, handles)) => (main.map(Arc::new), handles),
        Err(failed) => {
            py.detach(|| end_async_pool(async_pool, async_threads));
            return Err(failed);
        }
    };
    // The copies' last references drop here, attached, as `ServerRun::serve`'s keepalive does.
    let _thread_sites_keepalive = Keepalive::new(tpc_threads.sites());
    mipyre_server::report_pinning(&pin_order, threads.get(), &site.counters.run);
    let thread_cpus = mipyre_server::thread_cpus(&pin_order);
    let banner = mipyre_server::tpc_banner(
        threads.get(),
        cpus.logical(),
        &pin_order,
        site.rules.routes.uses_workers(),
        async_pool
            .as_deref()
            .map_or(0, mipyre_runtime::InterpreterPool::async_workers),
        counts,
    );
    let server = mipyre_server::TpcServer {
        threads: tpc_threads,
        main: main.clone(),
        async_pool: async_pool.clone(),
        gc: tuning.gc,
        cpus: thread_cpus,
        counters: Arc::clone(&site.counters.run),
    };

    let announce = |topology: &'static str, listening: &[String]| {
        mipyre_server::log_startup(topology, listening, &banner);
        start_static_watches(&site);
        readiness.call_or_stop(server_stop);
    };
    let served = py
        .detach(|| {
            let served = mipyre_server::run_tpc(transport, server, accept, announce, server_stop, deadline, spawner);
            // The TPC threads are joined, so these are the last references to the pool and
            // the main interpreter's threads. Both are joined by the deadline the TPC threads
            // were, so the three groups together take at most the stop's grace.
            end_async_pool(async_pool, async_threads);
            drop(main);
            abandoned.join_main(main_handles, deadline);
            served
        })
        .map_err(|e| tpc_failure(py, e, &abandoned));
    readiness.raised_or(served)
}

/// Keeps the TPC threads a stopped run left running with the app's abandoned workers and notes
/// them on the error that stopped the run.
fn tpc_failure(py: Python<'_>, failed: mipyre_server::ServeError, abandoned: &AbandonedWorkers) -> PyErr {
    let kept = abandoned.keep_threads(failed.abandoned_threads().to_vec());
    noting_abandoned(py, serve_exception(py, failed), &kept)
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
    (0..n)
        .map(|_| Arc::new(site.for_thread(|rules| mipyre_runtime::copy_rules(rules, py))))
        .collect()
}

/// Makes one server per IO thread of the multi-thread server, each with its own copy of `site`
/// and its own references to the handlers, all sharing the backends.
fn io_thread_servers(
    py: Python<'_>,
    site: &Site,
    io_threads: NonZeroUsize,
    pool: Option<Arc<InterpreterPool>>,
    main: Option<Arc<MainThreads>>,
) -> IoThreadServers {
    IoThreadServers::build(io_threads, || MultiThreadServer {
        site: Arc::new(site.for_io_thread(|rules| mipyre_runtime::copy_rules(rules, py))),
        pool: pool.clone(),
        main: main.clone(),
    })
}

/// Builds the recipe every worker of a server is created from.
pub(crate) fn worker_recipe(
    program: &WorkerProgram,
    site: &Site,
    gc: GcPolicy,
    store: &mipyre_runtime::AppStore,
) -> WorkerRecipe {
    let expected = RouteManifest::of(&site.rules.routes);
    WorkerRecipe::new(
        program,
        &expected,
        store,
        site.pg.clone(),
        gc,
        site.rules.settings.limits,
    )
}

/// Keeps the worker threads a failed pool start left running with the app's abandoned workers
/// and notes them on the start's error, as [`tpc_failure`] does for TPC threads.
fn pool_failure(py: Python<'_>, failed: PoolError, abandoned: &AbandonedWorkers) -> PyErr {
    abandoned.keep(failed.abandoned());
    let kept = failed.abandoned().to_vec();
    noting_abandoned(py, failed.into_pyerr(py), &kept)
}

/// Builds a channel pool of `split` workers, each on its own thread, and returns its threads to
/// join once every reference to the pool is gone. When a kind of worker has none left serving,
/// `stop` is stopped. The site gets a large-body permit per queue slot of the pool.
///
/// # Errors
///
/// Returns why the pool did not start, with the worker threads that start left running.
fn build_pool(
    py: Python<'_>,
    split: WorkerCounts,
    recipe: &Arc<WorkerRecipe>,
    site: &Site,
    tuning: &EngineSettings,
    stop: &ServerRunStop,
    workers: &Arc<WorkerCounters>,
) -> Result<(InterpreterPool, PoolThreads), PoolError> {
    let (pool, threads) = InterpreterPool::new(
        py,
        split,
        recipe,
        tuning.async_queue_capacity,
        &worker_loss_listener(stop.clone()),
        workers,
    )?;
    site.large_body_permits.add(pool.queue_slots());
    Ok((pool, threads))
}

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    use super::*;
    use mipyre_process::{LaneIndex, LaneLossError, LaneRunError};
    use mipyre_transport::driver::AcceptError;
    use std::time::Duration;

    struct Counted(Arc<std::sync::atomic::AtomicUsize>);

    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn a_lane_worker_death_counts_in_the_run_and_a_lane_failure_stops_the_server_with_its_cause() {
        let stop = ServerRunStop::default();
        let counters = Arc::new(WorkerCounters::new());
        let tell_run = lane_run_listener(stop.clone(), Arc::clone(&counters));

        tell_run(LaneRunEvent::WorkerDied);
        tell_run(LaneRunEvent::WorkerDied);
        assert_eq!(counters.read().process_workers_died, 2);
        assert!(!stop.is_stopped());

        tell_run(LaneRunEvent::Failed(LaneRunError::LaneLost {
            lane: LaneIndex(0),
            why: Arc::new(LaneLossError::ReplacementNotStarted {
                failure: "ImportError: 9d2c".into(),
            }),
        }));
        assert!(stop.is_stopped());
        match stop.cause() {
            Some(mipyre_tpc::StopCause::Caller(cause @ ServerStop::LanesFailed(_))) => {
                assert!(cause.to_string().contains("ImportError: 9d2c"), "{cause}");
            }
            other => panic!("expected the lanes' failure, got {other:?}"),
        }
    }

    #[test]
    fn the_last_worker_of_a_kind_stops_the_server_with_the_first_cause_and_says_so() {
        let stop = ServerRunStop::default();
        let counters = Arc::new(WorkerCounters::new());
        let (_, lives) = LiveWorkers::new(WorkerKind::TpcThread, 2, worker_loss_listener(stop.clone()), &counters);
        let mut lives = lives.into_iter();
        lives
            .next()
            .expect("worker 0's life")
            .died("gc.collect() raised A".into());
        assert!(!stop.is_stopped(), "one worker of two is left serving");

        let log = mipyre_runtime::testkit::captured_log(|| {
            lives
                .next()
                .expect("worker 1's life")
                .died("gc.collect() raised B".into());
        });
        assert!(stop.is_stopped());
        let Some(mipyre_tpc::StopCause::Caller(cause)) = stop.cause() else {
            panic!("expected the server's own cause, got {:?}", stop.cause());
        };
        let said = "the server stopped: no TPC thread worker is left serving; the first stopped: gc.collect() raised A";
        assert_eq!(cause.to_string(), said);
        let line = log
            .lines()
            .find(|l| l.contains("the server stops"))
            .unwrap_or_else(|| panic!("the stop was not said:\n{log}"));
        assert!(line.contains(said), "{line}");
        assert!(line.contains("\"level\":\"ERROR\""), "{line}");
    }

    #[test]
    fn of_two_kinds_lost_the_first_loss_is_the_servers_cause() {
        let stop = ServerRunStop::default();
        let counters = Arc::new(WorkerCounters::new());
        let (_, sync) = LiveWorkers::new(WorkerKind::PoolSync, 1, worker_loss_listener(stop.clone()), &counters);
        let (_, r#async) = LiveWorkers::new(WorkerKind::PoolAsync, 1, worker_loss_listener(stop.clone()), &counters);
        for (lives, cause) in [(sync, "sync gone"), (r#async, "async gone")] {
            for life in lives {
                life.died(cause.into());
            }
        }
        match stop.cause() {
            Some(mipyre_tpc::StopCause::Caller(ServerStop::NoWorkerLeft(loss))) => {
                assert_eq!(
                    loss,
                    WorkerKindLoss {
                        kind: WorkerKind::PoolSync,
                        first: "sync gone".into(),
                    }
                );
            }
            other => panic!("expected the sync kind's loss, got {other:?}"),
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

    /// Starts every thread on the OS except the `at`-th it is asked for (counting from 0, across
    /// every group it starts), which it refuses with `said`, because the OS cannot be made to
    /// refuse a thread on demand.
    struct RefuseAt {
        at: usize,
        said: &'static str,
        asked: usize,
    }

    impl RefuseAt {
        fn new(at: usize, said: &'static str) -> Self {
            RefuseAt { at, said, asked: 0 }
        }
    }

    impl mipyre_tpc::Spawner for RefuseAt {
        fn spawn<R: Send + 'static>(
            &mut self,
            _index: usize,
            builder: std::thread::Builder,
            body: impl FnOnce() -> R + Send + 'static,
        ) -> std::io::Result<mipyre_tpc::OsThread<R>> {
            let asked = self.asked;
            self.asked += 1;
            if asked == self.at {
                return Err(std::io::Error::other(self.said));
            }
            mipyre_tpc::spawn(builder, body)
        }
    }

    /// Builds an in-memory one-thread TPC run whose only handler is a main-interpreter fallback,
    /// already stopped, so it stops as soon as it is up. The returned connector keeps its
    /// in-memory listener open.
    fn main_only_run(
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
            ws_connections: mipyre_server::OpenConnections::default(),
            large_body_permits: mipyre_server::LargeBodyPermits::none(),
            process: None,
            counters: Arc::new(mipyre_server::SiteCounters::new(Arc::new(
                mipyre_server::ServerCounters::new(false),
            ))),
            pg: None,
            clock: quanta::Clock::new(),
        });
        let one = NonZeroUsize::new(1).unwrap();
        let mut tuning = crate::boot::ServerRecipe::of(
            crate::boot::Deployment::default().over(&crate::boot::ServerFlags::default()),
        )
        .expect("no flag at all is a recipe")
        .tuning;
        tuning.main = mipyre_runtime::MainSizes {
            max_threads: NonZeroUsize::new(2).unwrap(),
            queue: one,
        };
        let crate::boot::ServerCpus { cpus, pin_order, .. } = crate::boot::host_cpus();
        let (transport, connector) = mipyre_transport::memory::memory_listeners(one);
        let stop = CancellationToken::new();
        stop.cancel();
        let run = ServerRun {
            transport,
            site,
            stop,
            topology: Topology::Tpc {
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
            isolate_dir: None,
            command_line: CommandLine::default(),
            store: mipyre_runtime::AppStore::default(),
            abandoned: AbandonedWorkers::default(),
            readiness: ReadinessCallback::default(),
            workers: Arc::new(WorkerCounters::new()),
        };
        (run, connector)
    }

    /// Returns the messages of the events `f` emits on this thread.
    fn events_of<T>(f: impl FnOnce() -> T) -> (T, Vec<String>) {
        mipyre_runtime::testkit::ask_every_subscriber();
        #[derive(Clone, Default)]
        struct Messages(Arc<std::sync::Mutex<Vec<String>>>);
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Messages {
            fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
                struct Message<'a>(&'a mut Vec<String>);
                impl tracing::field::Visit for Message<'_> {
                    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
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
    fn a_main_thread_that_cannot_start_fails_the_start_before_the_server_announces_itself() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let (run, _connector) = main_only_run(py);
            let server_stop = ServerRunStop::new(run.stop.clone());
            let (served, events) = events_of(|| {
                serve_tpc(
                    py,
                    run,
                    NonZeroUsize::new(1).unwrap(),
                    AcceptMode::PerThreadListener,
                    &server_stop,
                    &ShutdownDeadline::new(Duration::from_secs(5)),
                    &mut RefuseAt::new(1, "no thread for the main interpreter"),
                )
            });
            let Err(raised) = served else {
                panic!("the second main-interpreter thread was refused, and the start went on: {events:?}");
            };
            assert_eq!(
                raised.value(py).str().unwrap().to_string(),
                "could not start main-interpreter thread 1 of 2: no thread for the main interpreter"
            );
            assert!(!events.iter().any(|m| m == "Mipyre started"), "{events:?}");
        });
    }

    /// Proves the event the test above looks for is the one a successful start emits.
    #[test]
    fn a_tpc_start_whose_main_threads_start_announces_itself() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let (run, _connector) = main_only_run(py);
            let server_stop = ServerRunStop::new(run.stop.clone());
            let (served, events) = events_of(|| {
                serve_tpc(
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
    fn a_tpc_thread_that_cannot_start_fails_the_start_before_the_server_announces_itself() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let (run, _connector) = main_only_run(py);
            let server_stop = ServerRunStop::new(run.stop.clone());
            let deadline = ShutdownDeadline::new(Duration::from_secs(5));
            let one = NonZeroUsize::new(1).unwrap();
            // The two main-interpreter threads start first, so the third thread asked for is the TPC thread.
            let refuse = &mut RefuseAt::new(2, "no thread for TPC");
            let (served, events) = events_of(|| {
                serve_tpc(
                    py,
                    run,
                    one,
                    AcceptMode::PerThreadListener,
                    &server_stop,
                    &deadline,
                    refuse,
                )
            });
            let Err(raised) = served else {
                panic!("the TPC thread was refused, and the start went on: {events:?}");
            };
            assert_eq!(
                raised.value(py).str().unwrap().to_string(),
                "could not spawn thread mipyre-tpc-0: no thread for TPC"
            );
            assert!(!events.iter().any(|m| m == "Mipyre started"), "{events:?}");
        });
    }

    /// Returns `(callback, calls)`: a Python callable that appends to the list `calls`, raising
    /// `ValueError("not serving")` after it when `raises`.
    fn readiness_callback(py: Python<'_>, raises: bool) -> (ReadinessCallback, Py<pyo3::types::PyList>) {
        let calls = pyo3::types::PyList::empty(py);
        let code = c"def make(calls, raises):\n    def serving():\n        calls.append(1)\n        if raises:\n            raise ValueError('not serving')\n    return serving\n";
        let module = pyo3::types::PyModule::from_code(py, code, c"serving_test.py", c"serving_test").unwrap();
        let callback = module.getattr("make").unwrap().call1((&calls, raises)).unwrap();
        (ReadinessCallback::new(Some(callback.unbind())), calls.unbind())
    }

    #[test]
    fn a_tpc_start_tells_its_readiness_callback_once() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let (mut run, _connector) = main_only_run(py);
            let (readiness, calls) = readiness_callback(py, false);
            run.readiness = readiness;
            let server_stop = ServerRunStop::new(run.stop.clone());
            let deadline = ShutdownDeadline::new(Duration::from_secs(5));
            let one = NonZeroUsize::new(1).unwrap();
            let served = serve_tpc(
                py,
                run,
                one,
                AcceptMode::PerThreadListener,
                &server_stop,
                &deadline,
                &mut mipyre_tpc::OsSpawner,
            );
            served.unwrap_or_else(|e| panic!("the stopped run ended cleanly: {e}"));
            assert_eq!(calls.bind(py).len(), 1);
        });
    }

    #[test]
    fn a_readiness_callback_that_raises_is_the_runs_error() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let (mut run, _connector) = main_only_run(py);
            let (readiness, calls) = readiness_callback(py, true);
            run.readiness = readiness;
            let server_stop = ServerRunStop::new(run.stop.clone());
            let deadline = ShutdownDeadline::new(Duration::from_secs(5));
            let one = NonZeroUsize::new(1).unwrap();
            let served = serve_tpc(
                py,
                run,
                one,
                AcceptMode::PerThreadListener,
                &server_stop,
                &deadline,
                &mut mipyre_tpc::OsSpawner,
            );
            let Err(raised) = served else {
                panic!("the callback raised and the run ended cleanly");
            };
            assert!(raised.is_instance_of::<pyo3::exceptions::PyValueError>(py), "{raised}");
            assert_eq!(raised.value(py).str().unwrap().to_string(), "not serving");
            assert_eq!(calls.bind(py).len(), 1);
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
            assert!(raised.is_instance_of::<pyo3::exceptions::PyOSError>(py), "{raised}");
            assert_eq!(raised.value(py).str().unwrap().to_string(), "listener broke");
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
            let failed = mipyre_server::ServeError::ThreadsStopped {
                root: None,
                abandoned: vec!["mipyre-tpc-1".to_string()],
            };
            let raised = tpc_failure(py, failed, &abandoned);
            let kept: Vec<String> = abandoned.take().into_iter().map(|w| w.thread).collect();
            assert_eq!(kept, vec!["mipyre-tpc-1".to_string()]);
            assert!(raised.to_string().contains("mipyre-tpc-1"), "{raised}");
        });
    }

    #[test]
    fn a_failed_pool_start_keeps_its_running_threads_and_names_them_on_the_error() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let abandoned = AbandonedWorkers::default();
            let failed = PoolError::ThreadNotSpawned {
                index: mipyre_runtime::WorkerIndex::new(1),
                source: std::io::Error::other("no more threads"),
                abandoned: vec![stuck("mipyre-worker-0")],
            };
            let raised = pool_failure(py, failed, &abandoned);
            let kept: Vec<String> = abandoned.take().into_iter().map(|w| w.thread).collect();
            assert_eq!(kept, vec!["mipyre-worker-0".to_string()]);
            assert!(raised.to_string().contains("no more threads"), "{raised}");
            let notes: Vec<String> = raised
                .value(py)
                .getattr("__notes__")
                .and_then(|n| n.extract())
                .unwrap_or_else(|e| panic!("no notes on {raised}: {e}"));
            assert_eq!(
                notes,
                vec!["worker(s) still running after the grace period: mipyre-worker-0".to_string()]
            );
        });
    }

    #[test]
    fn a_failed_pool_start_with_no_running_thread_keeps_nothing() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let abandoned = AbandonedWorkers::default();
            let failed = PoolError::WatchdogNotSpawned(std::io::Error::other("no thread"));
            let raised = pool_failure(py, failed, &abandoned);
            assert!(abandoned.take().is_empty());
            assert!(!raised.value(py).hasattr("__notes__").unwrap());
        });
    }

    /// Proves the TPC run raises the same `OSError` as the multi-thread server, whether the
    /// stopped accept loop is the run's error or the root cause of threads left running.
    #[test]
    fn a_tpc_accept_loop_that_stopped_raises_the_multi_thread_oserror() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let multi_thread =
                "OSError: the server stopped: accept loop stopped: every connector of this in-memory server is gone";
            let failures = [
                mipyre_server::ServeError::AcceptStopped(AcceptError::ConnectorsGone),
                mipyre_server::ServeError::ThreadsStopped {
                    root: Some(Box::new(mipyre_server::ServeError::AcceptStopped(
                        AcceptError::ConnectorsGone,
                    ))),
                    abandoned: vec![],
                },
            ];
            for failed in failures {
                let raised = tpc_failure(py, failed, &AbandonedWorkers::default());
                assert!(raised.is_instance_of::<pyo3::exceptions::PyOSError>(py), "{raised}");
                assert_eq!(raised.to_string(), multi_thread);
            }
        });
    }

    #[test]
    fn a_serving_error_with_no_abandoned_worker_is_unchanged() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let raised = noting_abandoned(py, pyo3::exceptions::PyOSError::new_err("listener broke"), &[]);
            assert!(raised.is_instance_of::<pyo3::exceptions::PyOSError>(py));
            assert!(!raised.value(py).hasattr("__notes__").unwrap());
        });
    }
}
