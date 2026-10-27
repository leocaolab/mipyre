//! Checks and records an app's registrations and builds what a server run serves from them.

use std::sync::Arc;

use pyo3::prelude::*;
use tokio_util::sync::CancellationToken;

use super::{App, LateRegistrationError, RunSettings};
use crate::boot::{InterpreterMode, ServerRun, Topology};
use mipyre_http::plan::route::{ExpectedRoute, HandlerKind, HandlerPlace, RouteRegistration, RuntimeChoice};
use mipyre_http::plan::site::SiteRules;
use mipyre_runtime::{record_worker_app, AppRoute, AppWebSocket, ScriptRegistrations};
use mipyre_server::Site;
use mipyre_transport::driver::TransportDriver;

/// Returns whether calling `handler` gives an awaitable, which sends its route to the async
/// workers: it is a coroutine function (`async def`, or marked one with
/// `inspect.markcoroutinefunction`), or a class-based view whose `__call__` is. This is the
/// only place that decides it; `_bind.py` asks it, through `mipyre.engine._is_async_handler`, for
/// the wrapper it puts around a handler.
///
/// # Errors
///
/// Whatever `inspect.iscoroutinefunction` or reading `__call__` raises. A failing check fails
/// the registration, because guessing sync would send an async handler to the sync pool.
pub(crate) fn is_async_handler(handler: &Bound<'_, PyAny>) -> PyResult<bool> {
    let py = handler.py();
    let inspect = py.import("inspect")?;
    let is_coroutine_function =
        |f: &Bound<'_, PyAny>| -> PyResult<bool> { inspect.call_method1("iscoroutinefunction", (f,))?.extract() };
    Ok(is_coroutine_function(handler)?
        || match handler.getattr(pyo3::intern!(py, "__call__")) {
            Ok(call) => is_coroutine_function(&call)?,
            Err(e) if e.is_instance_of::<pyo3::exceptions::PyAttributeError>(py) => false,
            Err(e) => return Err(e),
        })
}

impl App {
    /// Records, in a worker, the first app the script registers on as the one the worker
    /// serves; a registration on a second app is an error, and on the main interpreter it does
    /// nothing.
    pub(super) fn serve_in_worker(slf: &Bound<'_, Self>) -> PyResult<()> {
        record_worker_app(slf.as_any(), Self::worker_routes)
    }

    fn worker_routes<'py>(app: &Bound<'py, PyAny>) -> PyResult<ScriptRegistrations<'py>> {
        let py = app.py();
        let app = app.cast::<Self>()?.borrow();
        let table = app.routes.read();
        let bind = |hooks: &[Py<PyAny>]| hooks.iter().map(|h| h.bind(py).clone()).collect();
        Ok(ScriptRegistrations {
            routes: table
                .routes()
                .iter()
                .map(|r| AppRoute {
                    route: ExpectedRoute::of(r),
                    handler: table.handler(r.handler).bind(py).clone(),
                })
                .collect(),
            websockets: table
                .ws_routes()
                .iter()
                .zip(table.ws_handlers())
                .map(|(route, handler)| AppWebSocket {
                    route: route.clone(),
                    handler: handler.bind(py).clone(),
                })
                .collect(),
            before_hooks: bind(table.before_hooks()),
            after_hooks: bind(table.after_hooks()),
            limits: app.settings.limits,
        })
    }

    pub(super) fn register_route(
        slf: &Bound<'_, Self>,
        method: &str,
        path: &str,
        handler: Py<PyAny>,
        runtime: RuntimeChoice,
        stream: bool,
    ) -> PyResult<bool> {
        Self::serve_in_worker(slf)?;
        slf.borrow_mut()
            .add_route(slf.py(), method, path, handler, runtime, stream)
    }

    /// Builds one server run of this app, freezing what it serves and opening its transport
    /// with `open` before anything serves, so a port in use fails here.
    ///
    /// `open` returns the transport and whatever else it made, such as a memory server's
    /// connector.
    pub(super) fn server_run<D: TransportDriver, A>(
        &self,
        py: Python<'_>,
        settings: RunSettings,
        stop: &CancellationToken,
        open: impl FnOnce(std::num::NonZeroUsize) -> PyResult<(D, A)>,
    ) -> PyResult<(ServerRun<D>, A)> {
        let RunSettings {
            recipe,
            names,
            proxies,
            command_line,
            tcp,
        } = settings;
        let server_cpus = crate::boot::host_cpus();

        // A raw-engine script never calls `_seal_registrations`, so the table is sealed here
        // for workers to compare against.
        self.seal_if_unsealed();
        let workers = Arc::new(mipyre_runtime::WorkerCounters::new());
        let mut site = self.snapshot(
            py,
            self.start_counters(recipe.tuning.metrics, &server_cpus),
            Arc::clone(&workers),
        )?;
        let crate::boot::ServerCpus { cpus, pin_order, .. } = server_cpus;
        site.rules.settings.names = names;
        site.rules.settings.proxies = proxies;
        site.pg = self.seal_pg(py)?;
        let site: Arc<Site> = Arc::new(site);
        let topology = Topology::resolve(InterpreterMode::of(&site.rules.routes), &recipe, cpus)?;
        let counts = mipyre_http::plan::route::PlaceCounts::of(&site.rules.routes);
        let layout = crate::boot::ServerLayout {
            topology,
            worker_routes: site.rules.routes.uses_workers(),
            main_routes: site.rules.routes.uses_main(),
            async_worker_routes: counts.worker_async > 0,
            process_routes: site.rules.routes.uses_lane(),
            tcp,
        };
        if let Some(unused) = crate::boot::ConfigError::of(layout.unused(&recipe.values)) {
            return Err(unused.into());
        }
        let isolate_dir = recipe.isolate_dir(crate::boot::user_cache_dir().as_deref());
        if self.source.isolation {
            isolated_start(py, isolate_dir.as_deref(), &layout)?;
        }
        if site.rules.routes.uses_lane() && site.process.is_none() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "this app has runtime='process' routes but no process lanes: they are made by \
                 Mipyre.run() (mipyre._process_lane) before the server starts; a raw \
                 MipyreApp needs _set_process_lanes()",
            ));
        }

        let (transport, opened) = open(topology.accept_loops())?;
        let run = ServerRun {
            transport,
            site,
            stop: stop.clone(),
            topology,
            tuning: recipe.tuning,
            cpus,
            pin_order,
            app: self.source.clone(),
            isolate_dir,
            command_line,
            store: self.store.clone(),
            abandoned: self.abandoned.clone(),
            readiness: crate::boot::ReadinessCallback::default(),
            workers,
        };
        Ok((run, opened))
    }

    /// Opens this run's Postgres pools from every `PgPool.connect()` the main interpreter made,
    /// reporting every conflict at once, and binds those handles to them.
    fn seal_pg(&self, py: Python<'_>) -> PyResult<Option<Arc<mipyre_pg::RunPools>>> {
        let mut modules = self.home_modules.clone();
        // The script workers run: the one set for the app, else `__main__`'s.
        match &self.source.script_path {
            Some(script) => modules.push(crate::db::module_key_of_path(py, script)?),
            None => modules.extend(crate::db::module_key_of_module(py, "__main__")?),
        }
        let handles: Vec<_> = mipyre_runtime::EngineModule::with(py, |module| module.pg_handles())?
            .into_iter()
            .filter(|handle| handle.made_by(&modules))
            .collect();
        let n = self.runs.fetch_add(1, crate::sync::Ordering::Relaxed) + 1;
        let label = mipyre_pg::RunLabel(match &self.source.script_path {
            Some(script) => format!("server run {n} of {}", script.display()),
            None => format!("server run {n} of this app"),
        });
        py.detach(|| mipyre_pg::RunPools::seal(label, &handles, mipyre_subinterp::PYTHON_THREAD_STACK))
            .map_err(|e| match e {
                mipyre_pg::RunPoolsError::ConnectFailed { .. } => {
                    pyo3::exceptions::PyConnectionError::new_err(e.to_string())
                }
                mipyre_pg::RunPoolsError::Runtime(_) => pyo3::exceptions::PyRuntimeError::new_err(e.to_string()),
                mipyre_pg::RunPoolsError::Conflicts(_) | mipyre_pg::RunPoolsError::Shared(_) => {
                    pyo3::exceptions::PyValueError::new_err(e.to_string())
                }
            })
    }

    /// Records the script's registration boundary unless one is already set.
    pub(super) fn seal_if_unsealed(&self) {
        self.routes.write().seal();
    }

    /// Makes the counters of a run starting now, which counts its answers when its `metrics`
    /// flag or `enable_metrics()` says so.
    pub(super) fn start_counters(
        &self,
        metrics: bool,
        cpus: &crate::boot::ServerCpus,
    ) -> Arc<mipyre_server::ServerCounters> {
        Arc::new(
            mipyre_server::ServerCounters::new(metrics || self.metrics_path.is_some())
                .sized_from_fallback(cpus.fallback_reason()),
        )
    }

    /// Builds what one run serves: the app's rules, with the run's metrics page among its routes
    /// when metrics are enabled. The app's own table never holds that route, so the routes
    /// workers are held to and the OpenAPI document leave it out.
    ///
    /// # Errors
    ///
    /// Returns `ValueError` when the app has its own `GET` route at the metrics path.
    pub(super) fn snapshot(
        &self,
        py: Python<'_>,
        counters: Arc<mipyre_server::ServerCounters>,
        workers: Arc<mipyre_runtime::WorkerCounters>,
    ) -> PyResult<Site> {
        let mut rules = self.rules(py);
        // One budget per server: its mounts' caches hold their bytes against it, apart from any
        // other server's in the process. Its copies of the mounts count what they send from disk
        // apart from other servers' copies too.
        let budget = Arc::new(mipyre_http::io::static_files::CacheBudget::new(
            mipyre_http::io::static_files::DEFAULT_CACHE_BUDGET_BYTES,
        ));
        for mount in rules.routes.static_mounts_mut() {
            mount.cache_within(&budget);
            mount.count_own_sends();
        }
        if let Some(path) = &self.metrics_path {
            let page = Py::new(
                py,
                crate::pyapi::metrics_page::MetricsPage::new(
                    Arc::clone(&counters),
                    workers,
                    rules.routes.static_mounts().to_vec(),
                ),
            )?;
            rules
                .routes
                .insert(
                    "GET",
                    path,
                    page.into_any(),
                    "metrics".to_string(),
                    HandlerPlace::Main(mipyre_http::plan::route::MainHandler::Sync(
                        mipyre_http::plan::route::BodyMode::Buffered,
                    )),
                )
                .map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "enable_metrics({path:?}) cannot serve the metrics page: {e}"
                    ))
                })?;
        }
        Ok(Site {
            rules,
            ws_connections: mipyre_server::OpenConnections::default(),
            large_body_permits: mipyre_server::LargeBodyPermits::none(),
            process: self.process_lanes.clone(),
            counters: Arc::new(mipyre_server::SiteCounters::new(counters)),
            pg: None,
            clock: quanta::Clock::new(),
        })
    }

    /// Returns a copy of this app's rules holding its own references to the handlers; a
    /// process-lane worker serves them as the template had them.
    pub(crate) fn rules(
        &self,
        py: Python<'_>,
    ) -> mipyre_http::plan::site::SiteRules<Py<PyAny>, mipyre_http::io::static_files::StaticMount> {
        SiteRules {
            routes: self.routes.read().copy_with(|handler| handler.clone_ref(py)),
            settings: self.settings.clone(),
            access_log: self.access_log.on.then_some(self.access_log.sampling),
        }
    }

    /// Registers a constant response in this app's table, which owns `body` from then on.
    pub(super) fn add_fast_route(
        &mut self,
        method: &str,
        path: &str,
        body: bytes::Bytes,
        content_type: &str,
        status_code: u16,
        headers: &std::collections::HashMap<String, String>,
    ) -> PyResult<()> {
        let method_key = method.to_ascii_uppercase();
        let resp = mipyre_http::plan::route::FastResponse::parse(body, content_type, status_code, headers)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("fast response {method_key} {path}: {e}")))?;
        let added = self
            .routes
            .write()
            .add_fast_response(&method_key, path, resp)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("fast response {e}")))?;
        if !added {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "fast response already registered for {method_key} {path}"
            )));
        }
        Ok(())
    }

    fn add_route(
        &mut self,
        py: Python<'_>,
        method: &str,
        path: &str,
        handler: Py<PyAny>,
        runtime: RuntimeChoice,
        stream: bool,
    ) -> PyResult<bool> {
        let handler_obj = handler.bind(py);
        // A value that cannot be called would otherwise fail only on its first request.
        if !handler_obj.is_callable() {
            return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "{} {path}: the handler {} is not callable",
                method.to_uppercase(),
                handler_obj.repr()?
            )));
        }
        let handler_name = handler.getattr(py, "__name__")?.extract::<String>(py)?;
        let kind = if is_async_handler(handler_obj)? {
            HandlerKind::Async
        } else {
            HandlerKind::Sync
        };
        let registration = RouteRegistration { runtime, stream, kind };
        let place = HandlerPlace::try_from(registration)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{} {path}: {e}", method.to_uppercase())))?;

        let mut routes = self.routes.write();
        // Workers only know the routes the script registered, so a route added after `run()`
        // began exists only on main.
        if routes.is_sealed() && runtime != RuntimeChoice::Main {
            return Err(LateRegistrationError::new_err(format!(
                "{} {path} is registered after app.run() started (for example from an on_startup \
                 hook). Such a route exists only in the main interpreter, so it needs runtime='main'.",
                method.to_uppercase()
            )));
        }
        routes
            .insert(method, path, handler, handler_name, place)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("route error: {e}")))?;
        Ok(kind == HandlerKind::Async)
    }
}

/// Readies a start whose workers load isolated extensions, before any of them starts: the
/// directory their copies go in is checked once now (made private to this user if missing,
/// refused if another user owns it or can write to it), so a bad one fails the start rather
/// than every worker; each worker's claim checks again. With more than one worker, the
/// BLAS thread pools are limited to one thread each and the start says so.
fn isolated_start(
    py: Python<'_>,
    isolate_dir: Option<&std::path::Path>,
    layout: &crate::boot::ServerLayout,
) -> PyResult<()> {
    let Some(dir) = isolate_dir else {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            crate::boot::NoIsolateDirError.to_string(),
        ));
    };
    py.import("mipyre._clone")?.call_method1("ensure_private_dir", (dir,))?;
    if layout.workers() > 1 {
        let limited: String = py
            .import("mipyre._isolated.blas")?
            .call_method0("blas_threads_limited")?
            .extract()?;
        println!("  BLAS: {limited} (runtime='isolated'; override: set OPENBLAS_NUM_THREADS)");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use pyo3::prelude::*;

    use super::App;

    struct DropCounter(Arc<AtomicUsize>);

    impl AsRef<[u8]> for DropCounter {
        fn as_ref(&self) -> &[u8] {
            b"constant"
        }
    }

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    const REGISTERED: usize = 200;

    /// Builds an app with `REGISTERED` fast responses whose bodies count their drops into
    /// `drops`; `keep` sees each body as it goes in.
    fn app_of_counted(py: Python<'_>, drops: &Arc<AtomicUsize>, mut keep: impl FnMut(&bytes::Bytes)) -> App {
        let mut app = App::new(py).expect("the main interpreter's app takes a fresh map");
        for i in 0..REGISTERED {
            let body = bytes::Bytes::from_owner(DropCounter(Arc::clone(drops)));
            keep(&body);
            app.add_fast_route("GET", &format!("/c{i}"), body, "text/plain", 200, &HashMap::new())
                .expect("a valid fast response");
        }
        app
    }

    /// Neither the table's own count nor Python's allocator can see a body the app leaks on
    /// the way in, so this counts the bodies' drops.
    #[test]
    fn every_fast_response_body_is_freed_with_its_app() {
        Python::initialize();
        let drops = Arc::new(AtomicUsize::new(0));
        mipyre_subinterp::attach::with_main(|py| {
            let app = app_of_counted(py, &drops, |_| {});
            assert_eq!(drops.load(Ordering::Relaxed), 0);
            drop(app);
        });
        assert_eq!(drops.load(Ordering::Relaxed), REGISTERED);
    }

    /// Keeps one reference per body, which is what a leak amounts to, and checks that none is
    /// freed with the app.
    #[test]
    fn the_count_sees_a_body_kept_past_its_app() {
        Python::initialize();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut kept = Vec::new();
        mipyre_subinterp::attach::with_main(|py| drop(app_of_counted(py, &drops, |b| kept.push(b.clone()))));
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        kept.truncate(REGISTERED - 1);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}
