//! Defines `mipyre.engine.MipyreApp`, the app a script registers its routes, hooks and
//! settings on.

use std::sync::Arc;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyFloat, PyInt};
use tokio_util::sync::CancellationToken;

use crate::boot::{AppSource, Deployment, EntryPoint, ServerFlags, ServerRecipe};
use crate::pyapi::state::AppStateView;
use mipyre_http::plan::cors::Cors;
use mipyre_http::plan::cross_origin::HostNames;
use mipyre_http::plan::forwarded::Proxies;
use mipyre_http::plan::limits::{
    connection_limit, BodyGrace, BodyMinRate, BodyTimeLimit, ByteLimit, InterruptGrace, MessageByteLimit, PingAfter,
    PongWithin, RequestBudget, ShutdownTimeout,
};
use mipyre_http::plan::route::{RuntimeChoice, WsPlace};
use mipyre_http::plan::site::{AccessLog, ServeSettings};
use mipyre_runtime::AbandonedWorker;
use mipyre_runtime::Routes;

#[cfg(feature = "bench")]
mod bench;
mod register;
mod server;

pub(crate) use self::register::is_async_handler;
pub(crate) use self::server::Server;
use self::server::Transport;

/// What one run of the app was started with: its flags settled, the names its requests' host
/// may be, the proxies that may name its requests' client, and the command line its workers get.
struct RunSettings {
    recipe: ServerRecipe,
    names: HostNames,
    proxies: Proxies,
    command_line: mipyre_runtime::CommandLine,
    /// The server takes TCP connections, not only in-memory ones.
    tcp: bool,
}

/// The access log's on/off switch and sampling are kept together, so turning it back on keeps
/// the sampling set before.
#[derive(Clone, Copy)]
struct AccessLogSetting {
    on: bool,
    sampling: AccessLog,
}

#[pyclass(name = "MipyreApp", module = "mipyre.engine")]
pub(crate) struct App {
    routes: Arc<crate::sync::RwLock<Routes>>,
    /// The server's workers rebuild the app from this.
    source: AppSource,
    store: mipyre_runtime::AppStore,
    settings: ServeSettings,
    access_log: AccessLogSetting,
    /// Each server's run adds the worker threads it abandoned at shutdown to this.
    abandoned: crate::boot::AbandonedWorkers,
    process_lanes: Option<Arc<mipyre_process::ServerLanes>>,
    /// Each run serves its metrics page at this path, through the app's hooks, when it is set.
    metrics_path: Option<String>,
    /// Counts this app's server runs, to name each in an error.
    runs: crate::sync::AtomicU64,
    /// The modules whose code makes this app's `PgPool` handles (where it was created, its
    /// routers'), besides its script: a run takes only theirs.
    home_modules: Vec<mipyre_pg::ModuleKey>,
}

pyo3::create_exception!(
    mipyre.engine,
    LateRegistrationError,
    mipyre_runtime::DeclarationError,
    "A route or hook registered after the app's registrations were sealed (the first \
     server's start). Workers rebuild the app by running its script, so they only have \
     what the script registered: a late hook would silently skip every worker route. A \
     DeclarationError."
);

#[pymethods]
impl App {
    /// Reports the handlers and hooks to the cyclic GC.
    ///
    /// A handler's module globals usually hold the app, so the app and its handlers form a
    /// cycle the GC cannot see without this edge. In a worker that cycle outlives
    /// `Py_EndInterpreter`, and CPython then keeps all of the interpreter's obmalloc arenas,
    /// leaking a whole interpreter per worker on every start and stop.
    ///
    /// It uses `try_read` because a registration on this thread may hold the write lock and
    /// allocate, starting a collection here; references skipped then only keep the objects
    /// alive for this pass.
    fn __traverse__(&self, visit: pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        if let Some(table) = self.routes.try_read() {
            for obj in table.handlers() {
                visit.call(obj)?;
            }
        }
        Ok(())
    }

    /// Drops the handlers and hooks of an app the GC found unreachable.
    fn __clear__(&mut self) {
        // The table is dropped after the lock is released, because releasing a handler can
        // run Python code.
        let table = self
            .routes
            .try_write()
            .map(|mut t| std::mem::replace(&mut *t, Routes::new()));
        drop(table);
    }

    #[new]
    fn new(py: Python<'_>) -> PyResult<Self> {
        Ok(App {
            routes: Arc::new(crate::sync::RwLock::new(Routes::new())),
            source: AppSource {
                script_path: None,
                routers: Vec::new(),
                isolation: false,
            },
            store: mipyre_runtime::map_for_new(py)?,
            settings: ServeSettings::defaults(),
            access_log: AccessLogSetting {
                on: false,
                sampling: AccessLog::EVERY_RESPONSE,
            },
            abandoned: crate::boot::AbandonedWorkers::default(),
            process_lanes: None,
            metrics_path: None,
            runs: crate::sync::AtomicU64::new(0),
            home_modules: Vec::new(),
        })
    }

    /// Set the routers a sub-interpreter worker imports, which are those with
    /// sub-interpreter routes.
    fn _set_subinterp_routers(&mut self, routers: Vec<String>) {
        self.source.routers = routers;
    }

    /// Make this app's sub-interpreters load the isolation adapter, because a
    /// `runtime="isolated"` route was registered.
    fn _use_isolation(&mut self) {
        self.source.isolation = true;
    }

    /// Set the process lanes this app's `runtime="process"` routes run on; they must be set
    /// before the first server starts.
    fn _set_process_lanes(&mut self, lanes: &crate::pyapi::lane_set::PyLaneSet) -> PyResult<()> {
        self.process_lanes = Some(Arc::clone(lanes.server("_set_process_lanes")?));
        Ok(())
    }

    fn _has_process_routes(&self) -> bool {
        self.routes.read().uses_lane()
    }

    /// Serve the lane of `lanes` for this app in a process-lane worker until killed; it never
    /// returns normally.
    fn _serve_lane_worker(
        &self,
        py: Python<'_>,
        lanes: &crate::pyapi::lane_set::PyLaneSet,
        template_pid: i32,
    ) -> PyResult<()> {
        let template_pid = crate::pyapi::lane_set::pid_from_python(template_pid)?;
        let (peer, bell) = lanes.peer("serving a lane")?;
        mipyre_runtime::serve_lane_worker(py, peer, bell, self.rules(py), template_pid)
    }

    /// Take the worker threads this app's servers abandoned at shutdown; a second call
    /// returns none.
    ///
    /// Their interpreters are still alive and finalizing with a live sub-interpreter aborts,
    /// so `Mipyre.run()` exits non-zero without finalizing when there are any.
    fn _take_abandoned_workers(&self) -> Vec<AbandonedWorker> {
        self.abandoned.take()
    }

    /// Set this app's CORS configuration: its headers go on every response, and the engine
    /// answers a preflight (an `OPTIONS` with `Access-Control-Request-Method`) to a path with no
    /// `OPTIONS` route of its own.
    #[pyo3(signature = (origins, methods, headers, expose_headers=None, allow_credentials=false, max_age=None))]
    fn set_cors_config(
        &mut self,
        origins: Vec<String>,
        methods: String,
        headers: String,
        expose_headers: Option<String>,
        allow_credentials: bool,
        max_age: Option<u32>,
    ) -> PyResult<()> {
        if self.routes.read().is_sealed() {
            return Err(LateRegistrationError::new_err(
                "CORS is set after app.run() started (for example from an on_startup hook); \
                 set it at module level, before the app serves",
            ));
        }
        let cors = Cors::parse(
            &origins,
            &methods,
            &headers,
            expose_headers.as_deref().filter(|s| !s.is_empty()),
            allow_credentials,
            max_age,
        )
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        self.settings.cors = Some(cors);
        Ok(())
    }

    /// Answer HttpArena's `benchmark.BenchmarkService/GetSum` gRPC method.
    ///
    /// Only a POST to that exact path with an `application/grpc*` content type reaches it.
    fn enable_grpc_benchmark(&mut self) {
        self.routes
            .write()
            .add_grpc_method(mipyre_http::plan::route::GrpcMethod::BenchmarkGetSum);
    }

    /// Serve each run's metrics at `GET path` and count every answer of every run; a second call
    /// with the same path changes nothing, and one with another path raises `ValueError`.
    fn enable_metrics(&mut self, path: String) -> PyResult<()> {
        if self.routes.read().is_sealed() {
            return Err(LateRegistrationError::new_err(
                "metrics are enabled after app.run() started (for example from an on_startup \
                 hook); enable them at module level, before the app serves",
            ));
        }
        match &self.metrics_path {
            Some(enabled) if *enabled != path => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "enable_metrics: already enabled at {enabled:?}, asked again at {path:?}"
            ))),
            _ => {
                self.metrics_path = Some(path);
                Ok(())
            }
        }
    }

    /// Take a request's id from the client's `header` when it is a usable id; otherwise the
    /// server mints one. Either way it is `req.request_id`, a 5xx reports it, and every response
    /// carries it in `header`. Naming the same header again changes nothing; naming another
    /// raises `ValueError`.
    fn set_request_id_header(&mut self, header: &str) -> PyResult<()> {
        let name = hyper::header::HeaderName::from_bytes(header.as_bytes()).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid request-id header name {header:?}: {e}"))
        })?;
        match &self.settings.request_id_header {
            Some(set) if *set != name => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "the request-id header is already {set}, asked again as {name}"
            ))),
            _ => {
                self.settings.request_id_header = Some(name);
                Ok(())
            }
        }
    }

    fn enable_request_logging(&mut self, enabled: bool) {
        self.access_log.on = enabled;
    }

    /// Log one in `sample_n` requests, and every response whose status is at least
    /// `always_status`; it has no effect until `enable_request_logging(True)`.
    ///
    /// `always_status=400` logs every 4xx and 5xx while sampling 2xx, and `None` samples all
    /// responses alike. `sample_n=0` or an `always_status` that is not an HTTP status raises
    /// `ValueError`.
    #[pyo3(signature = (sample_n=1, always_status=None))]
    fn set_request_log_sampling(&mut self, sample_n: u64, always_status: Option<u16>) -> PyResult<()> {
        let sample_n = std::num::NonZeroU64::new(sample_n).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("sample_n must be at least 1 (1 logs every request)")
        })?;
        let always_status = always_status
            .map(|code| {
                hyper::StatusCode::from_u16(code).map_err(|_| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "always_status must be an HTTP status (100-999) or None, got {code}"
                    ))
                })
            })
            .transpose()?;
        self.access_log.sampling = AccessLog {
            sample_n,
            always_status,
        };
        Ok(())
    }

    /// Set this app's largest request body in bytes; a larger body is answered 413. The
    /// default is 2.5 MiB.
    fn set_max_body_size(&mut self, size: &Bound<'_, PyAny>) -> PyResult<()> {
        const SETTING: &str = "max_body_size";
        self.settings.limits.max_body = ByteLimit::new(SETTING, int_setting(SETTING, size)?).map_err(refused)?;
        Ok(())
    }

    fn max_body_size(&self) -> u64 {
        self.settings.limits.max_body.0
    }

    /// Set the seconds a request's handler may take to answer (504 past it), and a streamed
    /// body's handler to take each chunk. The default is 30.
    fn set_request_timeout(&mut self, seconds: &Bound<'_, PyAny>) -> PyResult<()> {
        self.settings.limits.request_budget =
            RequestBudget::from_secs(seconds_setting("request_timeout", seconds)?).map_err(refused)?;
        Ok(())
    }

    fn request_timeout(&self) -> f64 {
        self.settings.limits.request_budget.get().as_secs_f64()
    }

    /// Set the seconds a request body may take to arrive however slowly it comes (408 past it,
    /// unless it keeps up the minimum rate). The default is 5.
    fn set_body_grace(&mut self, seconds: &Bound<'_, PyAny>) -> PyResult<()> {
        self.settings.limits.body_grace =
            BodyGrace::from_secs(seconds_setting("body_grace", seconds)?).map_err(refused)?;
        Ok(())
    }

    fn body_grace(&self) -> f64 {
        self.settings.limits.body_grace.get().as_secs_f64()
    }

    /// Set the average rate, in bytes per second, below which a request body is refused once its
    /// grace has passed. The default is 1024.
    fn set_body_min_rate(&mut self, bytes_per_second: &Bound<'_, PyAny>) -> PyResult<()> {
        self.settings.limits.body_min_rate =
            BodyMinRate::new(int_setting("body_min_rate", bytes_per_second)?).map_err(refused)?;
        Ok(())
    }

    fn body_min_rate(&self) -> u64 {
        self.settings.limits.body_min_rate.get().get()
    }

    /// Set the seconds a request body may take at most, however fast it arrives. The
    /// default is 30.
    fn set_body_time_limit(&mut self, seconds: &Bound<'_, PyAny>) -> PyResult<()> {
        self.settings.limits.body_time_limit =
            BodyTimeLimit::from_secs(seconds_setting("body_time_limit", seconds)?).map_err(refused)?;
        Ok(())
    }

    fn body_time_limit(&self) -> f64 {
        self.settings.limits.body_time_limit.get().as_secs_f64()
    }

    /// Set the seconds an interrupted sync request's worker gets to end it before it is
    /// abandoned. The default is 2.
    fn set_interrupt_grace(&mut self, seconds: &Bound<'_, PyAny>) -> PyResult<()> {
        self.settings.limits.interrupt_grace =
            InterruptGrace::from_secs(seconds_setting("interrupt_grace", seconds)?).map_err(refused)?;
        Ok(())
    }

    fn interrupt_grace(&self) -> f64 {
        self.settings.limits.interrupt_grace.get().as_secs_f64()
    }

    /// Set the seconds a shutdown waits for each of the app's threads to finish its requests
    /// and end its interpreter. The default is 5.
    fn set_shutdown_timeout(&mut self, seconds: &Bound<'_, PyAny>) -> PyResult<()> {
        self.settings.limits.shutdown_timeout =
            ShutdownTimeout::from_secs(seconds_setting("shutdown_timeout", seconds)?).map_err(refused)?;
        Ok(())
    }

    fn shutdown_timeout(&self) -> f64 {
        self.settings.limits.shutdown_timeout.get().as_secs_f64()
    }

    /// Set the largest WebSocket message and frame in bytes, in either direction. The default
    /// is 1 MiB.
    fn set_max_websocket_message_size(&mut self, size: &Bound<'_, PyAny>) -> PyResult<()> {
        self.settings.limits.ws.max_message_bytes =
            MessageByteLimit::new(int_setting("max_websocket_message_size", size)?).map_err(refused)?;
        Ok(())
    }

    fn max_websocket_message_size(&self) -> u32 {
        self.settings.limits.ws.max_message_bytes.get()
    }

    /// Set how many WebSocket connections may be open at once; an upgrade beyond it is
    /// answered 503. The default is 1024.
    fn set_max_websocket_connections(&mut self, count: &Bound<'_, PyAny>) -> PyResult<()> {
        self.settings.limits.ws.max_connections =
            connection_limit(int_setting("max_websocket_connections", count)?).map_err(refused)?;
        Ok(())
    }

    fn max_websocket_connections(&self) -> usize {
        self.settings.limits.ws.max_connections.get()
    }

    /// Set the seconds a WebSocket client may go unheard before the server pings it. The
    /// default is 20.
    fn set_websocket_ping_interval(&mut self, seconds: &Bound<'_, PyAny>) -> PyResult<()> {
        self.settings.limits.ws.keepalive.ping_after =
            PingAfter::from_secs(seconds_setting("websocket_ping_interval", seconds)?).map_err(refused)?;
        Ok(())
    }

    fn websocket_ping_interval(&self) -> f64 {
        self.settings.limits.ws.keepalive.ping_after.get().as_secs_f64()
    }

    /// Set the seconds a pinged WebSocket client has to send a frame before the server closes
    /// it with 1011. The default is 20.
    fn set_websocket_ping_timeout(&mut self, seconds: &Bound<'_, PyAny>) -> PyResult<()> {
        self.settings.limits.ws.keepalive.pong_within =
            PongWithin::from_secs(seconds_setting("websocket_ping_timeout", seconds)?).map_err(refused)?;
        Ok(())
    }

    fn websocket_ping_timeout(&self) -> f64 {
        self.settings.limits.ws.keepalive.pong_within.get().as_secs_f64()
    }

    /// Register a constant response for an exact method and path, served without entering
    /// Python. A second registration of the same pair raises `ValueError`.
    #[pyo3(signature = (
        method,
        path,
        body,
        content_type="text/plain".to_string(),
        status_code=200,
        headers=None
    ))]
    fn add_fast_response(
        &mut self,
        method: &str,
        path: &str,
        body: Vec<u8>,
        content_type: String,
        status_code: u16,
        headers: Option<std::collections::HashMap<String, String>>,
    ) -> PyResult<()> {
        self.add_fast_route(
            method,
            path,
            bytes::Bytes::from(body),
            &content_type,
            status_code,
            &headers.unwrap_or_default(),
        )
    }

    /// Turn this app's response compression on with a `Compression`, or off with `None`, the
    /// default.
    #[pyo3(signature = (settings))]
    fn configure_compression(&mut self, settings: Option<&Bound<'_, crate::pyapi::config::Compression>>) {
        self.settings.compression = settings.map(|s| s.get().settings());
    }

    /// Mark the end of the script's registrations, before anything registered at run time
    /// such as `/mcp` or startup hooks; it raises `RuntimeError` outside the main interpreter.
    ///
    /// Sealing twice keeps the first boundary, because the engine's own `run()` also seals an
    /// unsealed table. A worker is never sealed, since its table is only the script's.
    fn _seal_registrations(&self, py: Python<'_>) -> PyResult<()> {
        if !mipyre_subinterp::attach::on_main(py) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "_seal_registrations runs on the main interpreter only",
            ));
        }
        self.seal_if_unsealed();
        Ok(())
    }

    /// Set the script workers execute when it is not `__main__.__file__`, as under
    /// `mipyre run module:app`, where `__main__` is the CLI.
    fn set_script_path(&mut self, path: std::path::PathBuf) {
        self.source.script_path = Some(path);
    }

    /// Returns the app's source file, absolute: the one set by `set_script_path`, else
    /// `__main__.__file__`. Sub-interpreter workers execute it and the reloader watches its
    /// directory.
    fn app_file(&self, py: Python<'_>) -> PyResult<std::ffi::OsString> {
        Ok(mipyre_runtime::app_file(py, self.source.script_path.as_deref())?.into_os_string())
    }

    /// Records a module whose code makes this app's `PgPool` handles, by its globals.
    fn _add_home_module(&mut self, globals: &Bound<'_, pyo3::types::PyDict>) -> PyResult<()> {
        let key = crate::db::module_key_of_globals(globals)?;
        if !self.home_modules.contains(&key) {
            self.home_modules.push(key);
        }
        Ok(())
    }

    /// Return the app's shared state, one map for every interpreter of the process.
    #[getter]
    fn state(&self) -> AppStateView {
        AppStateView::of(self.store.clone())
    }

    /// Registers `handler` for `method` `path` on `runtime`, and returns whether the engine
    /// runs it as an `async def` handler.
    ///
    /// # Errors
    ///
    /// Raises `ValueError` for a combination of `runtime`, `stream` and handler kind no serving
    /// path runs, `TypeError` for a handler that is not callable, and `LateRegistrationError`
    /// for a route off the main interpreter registered once the app is sealed.
    #[pyo3(signature = (method, path, handler, runtime, stream=false))]
    fn route(
        slf: &Bound<'_, Self>,
        method: &str,
        path: &str,
        handler: Py<PyAny>,
        runtime: PyRuntimeChoice,
        stream: bool,
    ) -> PyResult<bool> {
        Self::register_route(slf, method, path, handler, runtime.into(), stream)
    }

    fn before_request(slf: &Bound<'_, Self>, handler: Py<PyAny>) -> PyResult<()> {
        Self::serve_in_worker(slf)?;
        let app = slf.borrow();
        let mut routes = app.routes.write();
        refuse_sealed_hook(&routes, "a before_request hook")?;
        routes.add_before_hook(handler);
        Ok(())
    }

    fn after_request(slf: &Bound<'_, Self>, handler: Py<PyAny>) -> PyResult<()> {
        Self::serve_in_worker(slf)?;
        let app = slf.borrow();
        let mut routes = app.routes.write();
        refuse_sealed_hook(&routes, "an after_request hook")?;
        routes.add_after_hook(handler);
        Ok(())
    }

    fn fallback(slf: &Bound<'_, Self>, handler: Py<PyAny>) -> PyResult<()> {
        Self::serve_in_worker(slf)?;
        let app = slf.borrow();
        let mut routes = app.routes.write();
        refuse_sealed_hook(&routes, "a fallback handler")?;
        routes.set_fallback(handler);
        Ok(())
    }

    /// Register the `async def` WebSocket handler for `path`, whose `{name}` parameters land in
    /// `ws.request.params`, to run on a sub-interpreter worker or, with `runtime=Main`, on the
    /// main interpreter. A `def` handler raises `TypeError`; `runtime=Lane`, or a second handler
    /// for the same path, `ValueError`; and a worker route registered once the app is sealed
    /// `LateRegistrationError`.
    #[pyo3(signature = (path, handler, runtime=PyRuntimeChoice::Worker))]
    fn websocket(
        slf: &Bound<'_, Self>,
        path: &str,
        handler: Bound<'_, PyAny>,
        runtime: PyRuntimeChoice,
    ) -> PyResult<()> {
        Self::serve_in_worker(slf)?;
        if !crate::pyapi::is_async_handler(&handler)? {
            return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "websocket {path}: the handler must be an `async def`: it awaits ws.recv() and ws.send() on an \
                 async engine, where a blocking wait would stall every other connection"
            )));
        }
        let place = match runtime {
            PyRuntimeChoice::Worker => WsPlace::Worker,
            PyRuntimeChoice::Main => WsPlace::Main,
            PyRuntimeChoice::Lane => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "websocket {path}: runtime='process' does not serve WebSocket connections; use \
                     the default (a sub-interpreter) or runtime='main'"
                )))
            }
        };
        let app = slf.borrow();
        let mut routes = app.routes.write();
        if routes.is_sealed() && place == WsPlace::Worker {
            return Err(LateRegistrationError::new_err(format!(
                "websocket {path} is registered after app.run() started (for example from an on_startup \
                 hook). Such a route exists only in the main interpreter, so it needs runtime='main'."
            )));
        }
        routes
            .add_websocket(path, handler.unbind(), place)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    #[pyo3(signature = (prefix, directory, precompressed=false))]
    fn static_dir(&mut self, prefix: &str, directory: &str, precompressed: bool) -> PyResult<()> {
        let copies = if precompressed {
            mipyre_http::plan::static_plan::Copies::Precompressed
        } else {
            mipyre_http::plan::static_plan::Copies::Original
        };
        let mount = mipyre_http::io::static_files::StaticMount::new(prefix, directory, copies)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        self.routes.write().add_static_mount(mount);
        Ok(())
    }

    /// Prepare one server of this app and bind its listeners; a port in use raises
    /// `OSError(EADDRINUSE)` here.
    ///
    /// Its flags are `deployment` (what the entry point read; none by default) laid over
    /// `config` (what the code gives; nothing by default): a deployment value wins.
    /// Nothing serves until the returned `Server`'s `serve()`, and its `shutdown()` stops only
    /// that server. `allowed_hosts` are the DNS names a request's host may be besides
    /// `localhost` and IP addresses (`["*"]`: any); any other host is answered 421.
    /// `trusted_proxies` are the addresses and CIDR blocks whose `X-Forwarded-For` names a
    /// request's client (`req.client_ip`); with none the peer is the client.
    /// `transport="memory"` binds no socket and takes only connections made with the server's
    /// `connect()`, so it refuses the socket flags and checks no host. The workers get
    /// `command_line` (`sys.argv` and the flag values); without one, main's `sys.argv` and no
    /// parsed flags.
    #[pyo3(signature = (deployment=None, config=None, transport=None, command_line=None))]
    fn start(
        &self,
        py: Python<'_>,
        deployment: Option<PyRef<'_, Deployment>>,
        config: Option<PyRef<'_, ServerFlags>>,
        transport: Option<&str>,
        command_line: Option<mipyre_runtime::CommandLine>,
    ) -> PyResult<Server> {
        // A worker executes the whole script, so an unguarded `app.run()` in a raw-engine
        // script reaches here during the worker's init, which must not start a server.
        if !mipyre_subinterp::attach::on_main(py) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "MipyreApp.run() was called inside a sub-interpreter worker. Workers execute \
                 the whole script to register its routes; start the server only in the main \
                 interpreter, e.g. under `if __name__ == \"__main__\":` or with \
                 `if not mipyre.engine._in_worker():`",
            ));
        }
        mipyre_runtime::exchange::require_json(py)?;
        let none = Deployment::default();
        let deployment = deployment.as_deref().unwrap_or(&none);
        let nothing = ServerFlags::default();
        let values = deployment.over(config.as_deref().unwrap_or(&nothing));
        let transport = server::transport(transport)?;
        if transport == Transport::Memory {
            server::refuse_socket_flags(&values)?;
        }
        let recipe = ServerRecipe::of(values)?;
        if deployment.entry != EntryPoint::Embedded {
            for line in recipe.values.summary() {
                println!("  {line}");
            }
        }
        let command_line = match command_line {
            Some(line) => line,
            None => mipyre_runtime::CommandLine::of_main(py)?,
        };
        let stop = CancellationToken::new();
        let (parts, bound) = match transport {
            Transport::Tcp => {
                let specs = server::listener_set(&recipe.values)?;
                let addresses = server::address_flags(&recipe.values);
                let names = match &recipe.values.allowed_hosts {
                    Some(given) => HostNames::parse(given.value())
                        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
                    None => HostNames::LOCALHOST,
                };
                let proxies = Proxies::parse(recipe.values.trusted_proxies.as_ref().map_or(&[][..], |p| p.value()))
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                let settings = RunSettings {
                    recipe,
                    names,
                    proxies,
                    command_line,
                    tcp: true,
                };
                let (run, ()) = self.server_run(py, settings, &stop, |loops| {
                    Ok((server::bind(&specs, loops, &addresses)?, ()))
                })?;
                (
                    server::RunSources::of(&run.site, &run.workers),
                    server::BoundServer::tcp(run),
                )
            }
            Transport::Memory => {
                // No browser reaches a server without a socket, so no host is checked.
                let settings = RunSettings {
                    recipe,
                    names: HostNames::Any,
                    proxies: Proxies::default(),
                    command_line,
                    tcp: false,
                };
                let (run, connector) = self.server_run(py, settings, &stop, |loops| {
                    Ok(mipyre_transport::memory::memory_listeners(loops))
                })?;
                (
                    server::RunSources::of(&run.site, &run.workers),
                    server::BoundServer::memory(run, connector),
                )
            }
        };
        Ok(Server::new(stop, bound, parts))
    }

    /// Bind with `start()`, then serve until SIGINT: a script's entry point, so its flags are
    /// this process's deployment laid over `config`.
    #[pyo3(signature = (config=None))]
    fn run(&self, py: Python<'_>, config: Option<PyRef<'_, ServerFlags>>) -> PyResult<()> {
        let deployment = Py::new(py, Deployment::from_process(&[], EntryPoint::Script)?)?;
        self.start(py, Some(deployment.borrow(py)), config, None, None)?
            .serve(py, None)
    }

    /// Measure the framework without TCP: TPC sub-interpreter workers serve in-memory
    /// connections that pipeline `GET /`, and it returns `(requests, elapsed_s)`.
    ///
    /// Every route must be sync, off the GIL and not streaming; a failed worker or client
    /// raises.
    #[cfg(feature = "bench")]
    #[pyo3(signature = (duration_s=10, threads=None, conns_per_thread=8))]
    fn bench_inmem(
        &self,
        py: Python<'_>,
        duration_s: u64,
        threads: Option<usize>,
        conns_per_thread: usize,
    ) -> PyResult<(u64, f64)> {
        self.run_bench_inmem(py, duration_s, threads, conns_per_thread)
    }

    /// Measure the per-request cost of `request` on one TPC worker over `conns` in-memory
    /// connections, in three windows of `duration_s`.
    ///
    /// The returned dict holds `requests`, `instructions` (`None` off macOS),
    /// `alloc_requests`, `allocs`, `alloc_bytes`, `stack_requests` and, with `stacks`, the
    /// allocating call stacks as `[(count, bytes, [frame, ...]), ...]`, most first.
    #[cfg(feature = "bench")]
    #[pyo3(signature = (request, duration_s=2, conns=8, stacks=false))]
    fn bench_inmem_profile<'py>(
        &self,
        py: Python<'py>,
        request: Vec<u8>,
        duration_s: u64,
        conns: usize,
        stacks: bool,
    ) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        self.run_bench_inmem_profile(py, request, duration_s, conns, stacks)
    }

    /// Measure the server over loopback TCP with `client_conns` pipelining clients in this
    /// process, so no external client competes for CPU; it returns
    /// `(requests, elapsed_s, port)`.
    ///
    /// Every route must be sync, off the GIL and not streaming; a failed worker or client
    /// raises.
    #[cfg(feature = "bench")]
    #[pyo3(signature = (duration_s=10, threads=None, client_conns=32))]
    fn bench_loopback(
        &self,
        py: Python<'_>,
        duration_s: u64,
        threads: Option<usize>,
        client_conns: usize,
    ) -> PyResult<(u64, f64, u16)> {
        self.run_bench_loopback(py, duration_s, threads, client_conns)
    }
}

/// Where the engine runs a route's handler: a sub-interpreter worker, the main interpreter, or a
/// process lane.
#[pyclass(
    module = "mipyre.engine",
    name = "RuntimeChoice",
    eq,
    eq_int,
    frozen,
    hash,
    from_py_object
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PyRuntimeChoice {
    Worker,
    Main,
    Lane,
}

impl From<PyRuntimeChoice> for RuntimeChoice {
    fn from(choice: PyRuntimeChoice) -> Self {
        match choice {
            PyRuntimeChoice::Worker => RuntimeChoice::Worker,
            PyRuntimeChoice::Main => RuntimeChoice::Main,
            PyRuntimeChoice::Lane => RuntimeChoice::Lane,
        }
    }
}

/// Refuses a hook or fallback registered after sealing, because it would exist only on main
/// and every worker route would skip it.
fn refuse_sealed_hook(routes: &Routes, what: &str) -> PyResult<()> {
    match routes.is_sealed() {
        false => Ok(()),
        true => Err(LateRegistrationError::new_err(format!(
            "{what} is registered after app.run() started (for example from an on_startup \
             hook). Workers only run what the script registered, so every worker route would \
             skip it; register it at module level."
        ))),
    }
}

/// Raises a limit's refusal of a value as `ValueError`; the refusal names the setting and the value.
fn refused(e: impl std::error::Error) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(e.to_string())
}

/// Reads a limit set in whole units from Python. A `bool` is an `int` subclass, but a limit set
/// to `True` is a bug, not 1.
fn int_setting(setting: &str, value: &Bound<'_, PyAny>) -> PyResult<i64> {
    if value.is_instance_of::<PyBool>() || !value.is_instance_of::<PyInt>() {
        return Err(PyTypeError::new_err(format!(
            "{setting} must be int, got {}",
            value.get_type().name()?
        )));
    }
    value.extract()
}

/// Reads a limit set in seconds from Python: an `int` or a `float`, never a `bool`.
fn seconds_setting(setting: &str, value: &Bound<'_, PyAny>) -> PyResult<f64> {
    if value.is_instance_of::<PyBool>() || !(value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>()) {
        return Err(PyTypeError::new_err(format!(
            "{setting} must be int or float seconds, got {}",
            value.get_type().name()?
        )));
    }
    value.extract()
}
