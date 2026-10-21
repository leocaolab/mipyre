//! Defines `mipyre.engine.MipyreApp`, the app a script registers its routes, hooks and
//! settings on.

use std::sync::Arc;

use pyo3::prelude::*;
use tokio_util::sync::CancellationToken;

use crate::boot::{AppSource, ServerSizes};
use crate::runtime::AbandonedWorker;
use crate::pyapi::config::PyGivenSize;
use crate::pyapi::state::AppStateView;
use crate::serve::Routes;
use mipyre_http::plan::cors::Cors;
use mipyre_http::plan::limits::{
    connection_limit, BodyMinRate, BodyTimeLimit, ByteLimit, InterruptGrace, MessageByteLimit,
    RequestBudget, ShutdownTimeout, WsLimitError,
};
use mipyre_http::plan::route::RuntimeChoice;
use mipyre_http::plan::site::{AccessLog, ServeSettings};

#[cfg(feature = "bench")]
mod bench;
mod register;
mod server;

pub(crate) use self::server::Server;
use self::server::{positive, Transport};

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
    store: crate::runtime::AppStore,
    settings: ServeSettings,
    access_log: AccessLogSetting,
    /// Each server's run adds the worker threads it abandoned at shutdown to this.
    abandoned: crate::boot::AbandonedWorkers,
    process_lanes: Option<Arc<crate::lane::ServerLanes>>,
}

pyo3::create_exception!(
    mipyre.engine,
    RegistrationSealed,
    pyo3::exceptions::PyValueError,
    "A route or hook registered after the app's registrations were sealed (the first \
     server's start). Workers rebuild the app by running its script, so they only have \
     what the script registered: a late hook would silently skip every worker route. A \
     ValueError."
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
    fn new(py: Python<'_>) -> Self {
        App {
            routes: Arc::new(crate::sync::RwLock::new(Routes::new())),
            source: AppSource {
                script_path: None,
                routers: Vec::new(),
                isolation: false,
            },
            store: crate::runtime::map_for_new(py),
            settings: ServeSettings::defaults(),
            access_log: AccessLogSetting {
                on: false,
                sampling: AccessLog::EVERY_RESPONSE,
            },
            abandoned: crate::boot::AbandonedWorkers::default(),
            process_lanes: None,
        }
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
    fn _set_process_lanes(&mut self, lanes: &crate::lane::LaneSet) -> PyResult<()> {
        self.process_lanes = Some(Arc::clone(lanes.server("_set_process_lanes")?));
        Ok(())
    }

    fn _has_process_routes(&self) -> bool {
        self.routes.read().uses_lane()
    }

    /// Serve `lane` of `lanes` for this app in a process-lane worker until killed; it never
    /// returns normally.
    fn _serve_lane_worker(
        &self,
        py: Python<'_>,
        lanes: &crate::lane::LaneSet,
        lane: usize,
        template_pid: i32,
    ) -> PyResult<()> {
        let template_pid = crate::lane::pid_from_python(template_pid)?;
        lanes.serve_worker(py, lane, self.rules(py), template_pid)
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
    #[pyo3(signature = (origin, methods, headers, expose_headers=None, allow_credentials=false, max_age=None))]
    fn set_cors_config(
        &mut self,
        origin: String,
        methods: String,
        headers: String,
        expose_headers: Option<String>,
        allow_credentials: bool,
        max_age: Option<u32>,
    ) -> PyResult<()> {
        if self.routes.read().is_sealed() {
            return Err(RegistrationSealed::new_err(
                "CORS is set after app.run() started (for example from an on_startup hook); \
                 set it at module level, before the app serves",
            ));
        }
        let cors = Cors::parse(
            &origin,
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

    /// Take a request's id from the client's `header` when it is visible ASCII of at most 128
    /// bytes; otherwise the server mints one. Either way it is `req.request_id`, and a 5xx
    /// reports it.
    fn set_request_id_header(&mut self, header: &str) -> PyResult<()> {
        let name = hyper::header::HeaderName::from_bytes(header.as_bytes()).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "invalid request-id header name {header:?}: {e}"
            ))
        })?;
        self.settings.request_id_header = Some(name);
        Ok(())
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
    fn set_request_log_sampling(
        &mut self,
        sample_n: u64,
        always_status: Option<u16>,
    ) -> PyResult<()> {
        let sample_n = std::num::NonZeroU64::new(sample_n).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "sample_n must be at least 1 (1 logs every request)",
            )
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
    /// default is 10 MiB.
    fn set_max_body_size(&mut self, size: usize) {
        self.settings.limits.max_body = ByteLimit(size as u64);
    }

    fn max_body_size(&self) -> usize {
        self.settings.limits.max_body.as_usize()
    }

    /// Set the seconds each step of a request may take: its whole body arriving (408 past
    /// it), then its handler answering (504 past it). The default is 30.
    fn set_request_timeout(&mut self, seconds: f64) -> PyResult<()> {
        self.settings.limits.request_budget = RequestBudget::from_secs(seconds)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    fn request_timeout(&self) -> f64 {
        self.settings.limits.request_budget.get().as_secs_f64()
    }

    /// Set how many bytes of request body earn one more second past the request timeout. The
    /// default is 1024.
    fn set_body_min_rate(&mut self, bytes_per_second: i64) -> PyResult<()> {
        self.settings.limits.body_min_rate = BodyMinRate::new(bytes_per_second)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    fn body_min_rate(&self) -> u64 {
        self.settings.limits.body_min_rate.get().get()
    }

    /// Set the seconds a request body may take at most, whatever it earns by arriving. The
    /// default is 600.
    fn set_body_time_limit(&mut self, seconds: f64) -> PyResult<()> {
        self.settings.limits.body_time_limit = BodyTimeLimit::from_secs(seconds)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    fn body_time_limit(&self) -> f64 {
        self.settings.limits.body_time_limit.get().as_secs_f64()
    }

    /// Set the seconds an interrupted sync request's worker gets to end it before it is
    /// abandoned. The default is 2.
    fn set_interrupt_grace(&mut self, seconds: f64) -> PyResult<()> {
        self.settings.limits.interrupt_grace = InterruptGrace::from_secs(seconds)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    fn interrupt_grace(&self) -> f64 {
        self.settings.limits.interrupt_grace.get().as_secs_f64()
    }

    /// Set the seconds a shutdown waits for each of the app's threads to finish its requests
    /// and end its interpreter. The default is 5.
    fn set_shutdown_timeout(&mut self, seconds: f64) -> PyResult<()> {
        self.settings.limits.shutdown_timeout = ShutdownTimeout::from_secs(seconds)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    fn shutdown_timeout(&self) -> f64 {
        self.settings.limits.shutdown_timeout.get().as_secs_f64()
    }

    /// Set the largest WebSocket message and frame in bytes, in either direction. The default
    /// is 1 MiB.
    fn set_max_websocket_message_size(&mut self, size: i64) -> PyResult<()> {
        self.settings.limits.ws.max_message_bytes =
            MessageByteLimit::new(size).map_err(ws_limit_error)?;
        Ok(())
    }

    fn max_websocket_message_size(&self) -> u32 {
        self.settings.limits.ws.max_message_bytes.get()
    }

    /// Set how many WebSocket connections may be open at once; an upgrade beyond it is
    /// answered 503. The default is 1024.
    fn set_max_websocket_connections(&mut self, count: i64) -> PyResult<()> {
        self.settings.limits.ws.max_connections =
            connection_limit(count).map_err(ws_limit_error)?;
        Ok(())
    }

    fn max_websocket_connections(&self) -> usize {
        self.settings.limits.ws.max_connections.get()
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
    fn configure_compression(
        &mut self,
        settings: Option<&Bound<'_, crate::pyapi::config::Compression>>,
    ) {
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

    /// Return the app's shared state, one map for every interpreter of the process.
    #[getter]
    fn state(&self) -> AppStateView {
        AppStateView::of(self.store.clone())
    }

    #[pyo3(signature = (path, handler, gil=false, process=false))]
    fn get(
        slf: &Bound<'_, Self>,
        path: &str,
        handler: Py<PyAny>,
        gil: bool,
        process: bool,
    ) -> PyResult<()> {
        Self::register_route(
            slf,
            "GET",
            path,
            handler,
            runtime_choice("GET", path, gil, process)?,
            false,
        )
    }

    #[pyo3(signature = (path, handler, gil=false, stream=false, process=false))]
    fn post(
        slf: &Bound<'_, Self>,
        path: &str,
        handler: Py<PyAny>,
        gil: bool,
        stream: bool,
        process: bool,
    ) -> PyResult<()> {
        Self::register_route(
            slf,
            "POST",
            path,
            handler,
            runtime_choice("POST", path, gil, process)?,
            stream,
        )
    }

    #[pyo3(signature = (path, handler, gil=false, stream=false, process=false))]
    fn put(
        slf: &Bound<'_, Self>,
        path: &str,
        handler: Py<PyAny>,
        gil: bool,
        stream: bool,
        process: bool,
    ) -> PyResult<()> {
        Self::register_route(
            slf,
            "PUT",
            path,
            handler,
            runtime_choice("PUT", path, gil, process)?,
            stream,
        )
    }

    #[pyo3(signature = (path, handler, gil=false))]
    fn delete(slf: &Bound<'_, Self>, path: &str, handler: Py<PyAny>, gil: bool) -> PyResult<()> {
        Self::register_route(
            slf,
            "DELETE",
            path,
            handler,
            runtime_choice("DELETE", path, gil, false)?,
            false,
        )
    }

    #[pyo3(signature = (method, path, handler, gil=false, stream=false, process=false))]
    fn route(
        slf: &Bound<'_, Self>,
        method: &str,
        path: &str,
        handler: Py<PyAny>,
        gil: bool,
        stream: bool,
        process: bool,
    ) -> PyResult<()> {
        Self::register_route(
            slf,
            method,
            path,
            handler,
            runtime_choice(method, path, gil, process)?,
            stream,
        )
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

    /// Register the WebSocket handler for `path`, whose `{name}` parameters land in
    /// `ws.request.params`. A second handler for the same path raises `ValueError`.
    fn websocket(&mut self, path: &str, handler: Py<PyAny>) -> PyResult<()> {
        self.routes
            .write()
            .add_websocket(path, handler)
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
    /// Nothing serves until the returned `Server`'s `serve()`, and its `shutdown()` stops only
    /// that server. `transport="memory"` binds no socket and takes only connections made with
    /// the server's `connect()`, so it refuses host, port and TLS arguments.
    #[pyo3(signature = (
        host=None, port=None, workers=None, mode=None, io_workers=None,
        tls_cert=None, tls_key=None, extra_tls_ports=None, extra_plain_ports=None,
        transport=None,
    ))]
    #[expect(
        clippy::too_many_arguments,
        reason = "one parameter per Python keyword argument of `start()`"
    )]
    fn start(
        &self,
        py: Python<'_>,
        host: Option<&str>,
        port: Option<u16>,
        workers: Option<PyGivenSize>,
        mode: Option<&Bound<'_, PyAny>>,
        io_workers: Option<PyGivenSize>,
        tls_cert: Option<&str>,
        tls_key: Option<&str>,
        extra_tls_ports: Option<Vec<u16>>,
        extra_plain_ports: Option<Vec<u16>>,
        transport: Option<&str>,
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
        crate::exchange::require_json(py)?;
        let sizing = ServerSizes {
            workers: positive("workers", workers)?,
            io_workers: positive("io_workers", io_workers)?,
        };
        let stop = CancellationToken::new();
        let bound = match server::transport(transport)? {
            Transport::Tcp => {
                let specs = server::listener_set(
                    host,
                    port,
                    tls_cert,
                    tls_key,
                    extra_tls_ports,
                    extra_plain_ports,
                )?;
                let (run, ()) =
                    self.server_run(py, sizing, mode, &stop, |loops| Ok((server::bind(&specs, loops)?, ())))?;
                server::BoundServer::tcp(run)
            }
            Transport::Memory => {
                server::refuse_socket_args(
                    host,
                    port,
                    tls_cert,
                    tls_key,
                    extra_tls_ports.as_deref(),
                    extra_plain_ports.as_deref(),
                )?;
                let (run, connector) = self.server_run(py, sizing, mode, &stop, |loops| {
                    Ok(mipyre_transport::memory::memory_listeners(loops))
                })?;
                server::BoundServer::memory(run, connector)
            }
        };
        Ok(Server::new(stop, bound))
    }

    /// Bind with `start()`, then serve until SIGINT.
    #[pyo3(signature = (
        host=None, port=None, workers=None, mode=None, io_workers=None,
        tls_cert=None, tls_key=None, extra_tls_ports=None, extra_plain_ports=None,
    ))]
    #[expect(
        clippy::too_many_arguments,
        reason = "one parameter per Python keyword argument of `run()`"
    )]
    fn run(
        &self,
        py: Python<'_>,
        host: Option<&str>,
        port: Option<u16>,
        workers: Option<i64>,
        mode: Option<&Bound<'_, PyAny>>,
        io_workers: Option<i64>,
        tls_cert: Option<&str>,
        tls_key: Option<&str>,
        extra_tls_ports: Option<Vec<u16>>,
        extra_plain_ports: Option<Vec<u16>>,
    ) -> PyResult<()> {
        let run_arg = |value: i64| PyGivenSize {
            value,
            origin: crate::boot::SizeOrigin::RunArg,
        };
        self.start(
            py,
            host,
            port,
            workers.map(run_arg),
            mode,
            io_workers.map(run_arg),
            tls_cert,
            tls_key,
            extra_tls_ports,
            extra_plain_ports,
            None,
        )?
        .serve(py)
    }

    /// Measure the framework without TCP: TPC sub-interpreter workers serve in-memory
    /// connections that pipeline `GET /`, and it returns `(requests, elapsed_s)`.
    ///
    /// Every route must be sync, off the GIL and not streaming; a failed worker or client
    /// raises.
    #[cfg(feature = "bench")]
    #[pyo3(signature = (duration_s=10, workers=None, conns_per_worker=8))]
    fn bench_inmem(
        &self,
        py: Python<'_>,
        duration_s: u64,
        workers: Option<usize>,
        conns_per_worker: usize,
    ) -> PyResult<(u64, f64)> {
        self.run_bench_inmem(py, duration_s, workers, conns_per_worker)
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
    #[pyo3(signature = (duration_s=10, workers=None, client_conns=32))]
    fn bench_loopback(
        &self,
        py: Python<'_>,
        duration_s: u64,
        workers: Option<usize>,
        client_conns: usize,
    ) -> PyResult<(u64, f64, u16)> {
        self.run_bench_loopback(py, duration_s, workers, client_conns)
    }
}

/// Returns the runtime a route's `gil=` and `process=` arguments name; setting both is a
/// `ValueError`.
fn runtime_choice(method: &str, path: &str, gil: bool, process: bool) -> PyResult<RuntimeChoice> {
    match (gil, process) {
        (false, false) => Ok(RuntimeChoice::Worker),
        (true, false) => Ok(RuntimeChoice::Main),
        (false, true) => Ok(RuntimeChoice::Lane),
        (true, true) => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "{} {path}: runtime='process' and runtime='main' exclude each other: a route runs in one place",
            method.to_uppercase()
        ))),
    }
}

/// Refuses a hook or fallback registered after sealing, because it would exist only on main
/// and every worker route would skip it.
fn refuse_sealed_hook(routes: &Routes, what: &str) -> PyResult<()> {
    match routes.is_sealed() {
        false => Ok(()),
        true => Err(RegistrationSealed::new_err(format!(
            "{what} is registered after app.run() started (for example from an on_startup \
             hook). Workers only run what the script registered, so every worker route would \
             skip it; register it at module level."
        ))),
    }
}

fn ws_limit_error(e: WsLimitError) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(e.to_string())
}

