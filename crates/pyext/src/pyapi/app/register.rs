//! Checks and records an app's registrations and builds what a server run serves from them.

use std::sync::Arc;

use pyo3::prelude::*;
use tokio_util::sync::CancellationToken;

use super::{App, RegistrationSealed};
use crate::boot::{EnvConfig, InterpreterMode, ServerRun, ServerSizes, Topology};
use mipyre_transport::driver::TransportDriver;
use crate::runtime::{record_worker_app, AppRoute, ScriptRegistrations};
use crate::serve::Site;
use mipyre_http::plan::route::{
    ExpectedRoute, HandlerKind, HandlerPlace, RouteRegistration, RuntimeChoice,
};
use mipyre_http::plan::site::SiteRules;

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
    ) -> PyResult<()> {
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
        sizing: ServerSizes,
        mode: Option<&Bound<'_, PyAny>>,
        stop: &CancellationToken,
        open: impl FnOnce(std::num::NonZeroUsize) -> PyResult<(D, A)>,
    ) -> PyResult<(ServerRun<D>, A)> {
        let env = EnvConfig::from_env()?;
        let mode = mode
            .map(crate::pyapi::config::mode_from_arg)
            .transpose()?
            .unwrap_or(InterpreterMode::Gil);
        let crate::boot::ServerCpus { cpus, pin_order } = crate::boot::host_cpus();
        let topology = Topology::resolve(mode, &env, sizing, cpus)?;

        // A raw-engine script never calls `_seal_registrations`, so the table is sealed here
        // for workers to compare against.
        self.seal_if_unsealed();
        let site: Arc<Site> = Arc::new(
            self.snapshot(py, crate::telemetry::start_run(env.tuning.metrics)),
        );
        // No sub-interpreters are built when no route runs on one; the TPC server already
        // builds none itself.
        let topology = match topology {
            Topology::Pool { .. } if !site.rules.routes.uses_workers() => {
                topology.without_workers()
            }
            other => other,
        };
        for note in topology
            .unused_sizes(sizing)
            .into_iter()
            .chain(topology.unused_settings(&env))
        {
            println!("  Note: {note}");
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
            tuning: env.tuning,
            cpus,
            pin_order,
            app: self.source.clone(),
            store: self.store.clone(),
            abandoned: self.abandoned.clone(),
        };
        Ok((run, opened))
    }

    /// Records the script's registration boundary unless one is already set.
    pub(super) fn seal_if_unsealed(&self) {
        self.routes.write().seal();
    }

    pub(super) fn snapshot(
        &self,
        py: Python<'_>,
        counters: std::sync::Arc<crate::telemetry::RunCounters>,
    ) -> Site {
        Site {
            rules: self.rules(py),
            ws_connections: crate::serve::OpenConnections::default(),
            process: self.process_lanes.clone(),
            counters,
        }
    }

    /// Returns a copy of this app's rules holding its own references to the handlers; a
    /// process-lane worker serves them as the template had them.
    pub(crate) fn rules(
        &self,
        py: Python<'_>,
    ) -> mipyre_http::plan::site::SiteRules<Py<PyAny>, mipyre_http::io::static_files::StaticMount> {
        SiteRules {
            routes: self
                .routes
                .read()
                .copy_with(|handler| handler.clone_ref(py)),
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
        let resp =
            mipyre_http::plan::route::FastResponse::parse(body, content_type, status_code, headers)
                .map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "fast response {method_key} {path}: {e}"
                    ))
                })?;
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
    ) -> PyResult<()> {
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
        // `__call__` is checked too, for class-based views. A failing check fails the
        // registration, because guessing sync would send an async handler to the sync pool.
        let inspect = py.import("inspect")?;
        let is_coroutine_function = |f: &Bound<'_, PyAny>| -> PyResult<bool> {
            inspect.call_method1("iscoroutinefunction", (f,))?.extract()
        };
        let is_async = is_coroutine_function(handler_obj)?
            || match handler_obj.getattr(pyo3::intern!(py, "__call__")) {
                Ok(call) => is_coroutine_function(&call)?,
                Err(e) if e.is_instance_of::<pyo3::exceptions::PyAttributeError>(py) => false,
                Err(e) => return Err(e),
            };

        let kind = if is_async {
            HandlerKind::Async
        } else {
            HandlerKind::Sync
        };
        let registration = RouteRegistration {
            runtime,
            stream,
            kind,
        };
        let place = HandlerPlace::try_from(registration).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "{} {path}: {e}",
                method.to_uppercase()
            ))
        })?;

        let mut routes = self.routes.write();
        // Workers only know the routes the script registered, so a route added after `run()`
        // began exists only on main.
        if routes.is_sealed() && runtime != RuntimeChoice::Main {
            return Err(RegistrationSealed::new_err(format!(
                "{} {path} is registered after app.run() started (for example from an on_startup \
                 hook). Such a route exists only in the main interpreter, so it needs runtime='main'.",
                method.to_uppercase()
            )));
        }
        routes
            .insert(method, path, handler, handler_name, place)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("route error: {e}")))?;
        Ok(())
    }
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
    fn app_of_counted(
        py: Python<'_>,
        drops: &Arc<AtomicUsize>,
        mut keep: impl FnMut(&bytes::Bytes),
    ) -> App {
        let mut app = App::new(py);
        for i in 0..REGISTERED {
            let body = bytes::Bytes::from_owner(DropCounter(Arc::clone(drops)));
            keep(&body);
            app.add_fast_route(
                "GET",
                &format!("/c{i}"),
                body,
                "text/plain",
                200,
                &HashMap::new(),
            )
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
        mipyre_subinterp::attach::with_main(|py| {
            drop(app_of_counted(py, &drops, |b| kept.push(b.clone())))
        });
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        kept.truncate(REGISTERED - 1);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}
