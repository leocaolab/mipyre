//! Serves requests on a worker from Rust: the hook chain for the TPC threads and the pool's sync
//! workers, and the `gc.collect()` that retires a worker when it fails.

use std::sync::Arc;

use mipyre_subinterp::context::SpareContext;
use mipyre_subinterp::gc::{GcPolicy, GcSchedule};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::start::{StartException, WorkerStartError};
use super::{RequestRunner, SubInterpreterWorker, WorkerIndex};
use crate::exchange::error::{refuse, HandlerError, PyException, RequestStage, ResponseError, UnansweredError};
use crate::exchange::request::Request;
use crate::exchange::websocket::{Outbox, WsClose};
use crate::hook_chain::{
    explain_no_current_loop, explain_not_for_subinterpreters, worker_response, Chain, EventLoopProvider,
    ExceptionMapping,
};
use crate::request_context::{in_reused_context, RequestContext};
use crate::websocket::{self, Handlers, Sessions, WsEvent, WsSession, WsUpgradeAnswer};
use crate::worker_app::WorkerRoutes;
use mipyre_http::plan::grpc::APPLICATION_GRPC;
use mipyre_http::plan::response::{HandlerResponse, HeaderLines};
use mipyre_http::plan::route::{CallConvention, RouteId, RouteKey, WsHandlers, WsRouteId};
use mipyre_util::panic_message;

/// A route of a [`SyncRunner`]; `handler` is `None` for a route that does not run on a worker.
pub(super) struct InlineRoute {
    pub(super) handler: Option<Py<PyAny>>,
    key: RouteKey,
    convention: CallConvention,
}

/// A runner that drives the hook chain from Rust, for the TPC threads and the pool's sync workers.
pub struct SyncRunner {
    /// It is indexed like main's route table.
    pub(super) routes: Vec<InlineRoute>,
    /// It is indexed like main's WebSocket routes, `None` for one that runs on main.
    websockets: Vec<Option<Handlers>>,
    /// Holds the `ws` of every WebSocket connection this worker serves.
    sessions: Sessions,
    /// Both hook lists are in registration order.
    before_hooks: Vec<Py<PyAny>>,
    after_hooks: Vec<Py<PyAny>>,
    /// It is made at start when a handler or hook is `async def`, otherwise on the first
    /// awaitable, so an app that never awaits never imports asyncio (about 3 MiB a worker with
    /// ssl and subprocess). It is never the current loop of the thread a `def` handler runs on.
    event_loop: std::cell::OnceCell<Py<PyAny>>,
    gc: GcSchedule,
    /// It is looked up once, so a collection does not re-import `gc`.
    collect: Py<PyAny>,
    /// The next request enters the context the last one left empty and unreferenced.
    spare_context: SpareContext,
}

impl RequestRunner for SyncRunner {
    fn build(
        py: Python<'_>,
        worker: WorkerIndex,
        routes: WorkerRoutes<'_>,
        gc: GcPolicy,
    ) -> Result<Self, WorkerStartError> {
        let setup = |step| {
            move |e: PyErr| WorkerStartError::SetupRaised {
                worker,
                step,
                raised: StartException::capture(py, &e),
            }
        };
        // `mipyre` itself already imports `inspect`, so the lookup is cheap.
        let iscoroutinefunction = py
            .import("inspect")
            .and_then(|inspect| inspect.getattr("iscoroutinefunction"))
            .map_err(setup("looking up inspect.iscoroutinefunction"))?;
        let mut any_async = false;
        for f in routes
            .routes
            .iter()
            .filter_map(|r| r.handler.as_ref())
            .chain(&routes.before_hooks)
            .chain(&routes.after_hooks)
        {
            let is_async = iscoroutinefunction
                .call1((f,))
                .and_then(|r| r.is_truthy())
                .map_err(setup("checking for async def handlers and hooks"))?;
            if is_async {
                any_async = true;
                break;
            }
        }
        let event_loop = if any_async {
            let made = new_event_loop(py).map_err(setup("creating the asyncio event loop"))?;
            std::cell::OnceCell::from(made.unbind())
        } else {
            std::cell::OnceCell::new()
        };
        // `_worker/bootstrap.py` has called gc.disable(), so this is the worker's only cycle collection.
        let collect = py
            .import("gc")
            .and_then(|gc| gc.getattr("collect"))
            .map_err(setup("looking up gc.collect"))?;

        let owned = |v: Vec<Bound<'_, PyAny>>| -> Vec<Py<PyAny>> { v.into_iter().map(Bound::unbind).collect() };
        let inline_routes = routes
            .routes
            .into_iter()
            .map(|r| InlineRoute {
                handler: r.handler.map(Bound::unbind),
                key: r.key,
                convention: r.convention,
            })
            .collect();
        let websockets = routes
            .websockets
            .into_iter()
            .map(|handlers| handlers.map(|h| h.map(|f| f.clone().unbind())))
            .collect();
        Ok(SyncRunner {
            routes: inline_routes,
            websockets,
            sessions: Sessions::default(),
            before_hooks: owned(routes.before_hooks),
            after_hooks: owned(routes.after_hooks),
            event_loop,
            gc: GcSchedule::new(gc),
            collect: collect.unbind(),
            spare_context: SpareContext::default(),
        })
    }
}

impl mipyre_subinterp::Releasable for SyncRunner {
    fn release(mut self, py: Python<'_>) {
        let event_loop = self.event_loop.take();
        self.sessions.release_all(py);
        let websockets = self.websockets.into_iter().flatten().flat_map(|h| {
            let WsHandlers {
                on_message,
                on_connect,
                on_close,
            } = h;
            std::iter::once(on_message).chain(on_connect).chain(on_close)
        });
        let owned = self
            .routes
            .into_iter()
            .filter_map(|route| route.handler)
            .chain(websockets)
            .chain(self.before_hooks)
            .chain(self.after_hooks)
            .chain(event_loop)
            .chain([self.collect]);
        for reference in owned {
            reference.drop_ref(py);
        }
        self.spare_context.release(py);
    }
}

impl EventLoopProvider for SyncRunner {
    fn event_loop<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>, HandlerError> {
        if let Some(event_loop) = self.event_loop.get() {
            return Ok(event_loop.bind(py).clone());
        }
        let made = new_event_loop(py).map_err(|e| HandlerError::python(py, RequestStage::Setup, &e))?;
        Ok(self.event_loop.get_or_init(|| made.unbind()).bind(py).clone())
    }
}

impl SyncRunner {
    /// Runs `route`'s hooks and handler in a `contextvars.Context` of their own: a fresh one, or
    /// the last request's left empty and unreferenced.
    fn serve(&mut self, py: Python<'_>, route: RouteId, request: Request) -> Result<HandlerResponse, HandlerError> {
        let mut spare = std::mem::take(&mut self.spare_context);
        let served = in_reused_context(py, &mut spare, |rc| self.run(py, rc, route, request));
        self.spare_context = spare;
        served
    }

    fn run<'py>(
        &self,
        py: Python<'py>,
        rc: &mut RequestContext<'py>,
        route: RouteId,
        request: Request,
    ) -> Result<HandlerResponse, HandlerError> {
        let mut chain = Chain {
            py,
            rc,
            event_loop: self,
            to_response: worker_response,
        };
        let req = Bound::new(py, request).map_err(|e| HandlerError::python(py, RequestStage::Setup, &e))?;

        let short_circuit = chain.before(&self.before_hooks, &req)?;
        let response = match short_circuit {
            Some(response) => response,
            // The table was checked index by index against main's at init, so a route with no
            // handler here does not run on a worker and is refused. A gRPC method's id does not
            // come here: the server runs it through `serve_unary`.
            None => {
                let Some(InlineRoute {
                    handler: Some(handler),
                    key,
                    ..
                }) = self.routes.get(route.index())
                else {
                    return Err(refuse(UnansweredError::NotOnWorker(route.index())));
                };
                let value = chain.handler(handler, &req, ExceptionMapping::WorkerDef { key, handler })?;
                chain.after(&self.after_hooks, &req, worker_response(value)?)?
            }
        };

        #[cfg(feature = "leak_detect")]
        crate::leak_detect::record_drop(req.as_any());
        Ok(response)
    }

    /// Calls the gRPC method `method`'s handler with the request's message, in a context of its own
    /// as [`Self::serve`] runs a route, and returns its reply message as the body of the answer.
    /// The `before_request` and `after_request` hooks do not run: they take an HTTP request and
    /// return HTTP responses, and a gRPC call has neither.
    fn serve_unary(
        &mut self,
        py: Python<'_>,
        method: RouteId,
        request: Request,
    ) -> Result<HandlerResponse, HandlerError> {
        // As in `run`, a method with no handler here is not on this worker.
        let Some(InlineRoute {
            handler: Some(handler),
            key,
            convention: CallConvention::GrpcUnary,
        }) = self.routes.get(method.index())
        else {
            return Err(refuse(UnansweredError::NotOnWorker(method.index())));
        };
        in_reused_context(py, &mut self.spare_context, |_| {
            let message = request
                .buffered_body(py)
                .map(|message| PyBytes::new(py, message))
                .map_err(|e| HandlerError::python(py, RequestStage::Setup, &e))?;
            let value = handler.bind(py).call1((message,)).map_err(|e| {
                let explained = explain_not_for_subinterpreters(py, explain_no_current_loop(py, key, handler, e));
                HandlerError::grpc_method(py, RequestStage::Handler, &explained)
            })?;
            let reply = value.cast::<PyBytes>().map_err(|_| ResponseError::NotBytes {
                type_name: crate::exchange::to_response::type_name(&value),
            })?;
            Ok(HandlerResponse::new(
                hyper::StatusCode::OK,
                bytes::Bytes::copy_from_slice(reply.as_bytes()),
                APPLICATION_GRPC,
                HeaderLines::new(),
            ))
        })
    }
}

impl SyncRunner {
    /// Runs `f` in a context of its own, as a request is, with a chain for this worker.
    fn in_context<'py, T>(
        &mut self,
        py: Python<'py>,
        f: impl FnOnce(&Self, &mut Chain<'_, 'py, Self>) -> Result<T, HandlerError>,
    ) -> Result<T, HandlerError> {
        let mut spare = std::mem::take(&mut self.spare_context);
        let ran = in_reused_context(py, &mut spare, |rc| {
            let mut chain = Chain {
                py,
                rc,
                event_loop: &*self,
                to_response: worker_response,
            };
            f(self, &mut chain)
        });
        self.spare_context = spare;
        ran
    }

    fn ws_handlers(&self, route: WsRouteId) -> Result<&Handlers, HandlerError> {
        match self.websockets.get(route.index()) {
            Some(Some(handlers)) => Ok(handlers),
            Some(None) | None => Err(refuse(UnansweredError::WebSocketNotOnWorker(route.index()))),
        }
    }
}

/// A failed `gc.collect()`, which signals OOM, heap corruption or interpreter damage.
#[derive(Debug, thiserror::Error)]
enum GcCollectError {
    #[error("gc.collect() raised {0}")]
    Raised(PyException),
    #[error("a Rust panic in gc.collect(): {0}")]
    Panicked(String),
}

/// A worker stops serving for good when its `gc.collect()` failed, because its interpreter may be
/// damaged. It is isolated, not rebuilt: the others take its share, and the server stops once a
/// kind has no worker left (`LiveWorkers`).
#[derive(Debug, thiserror::Error)]
#[error("worker {worker} retired: {failure}")]
pub struct Retirement {
    worker: WorkerIndex,
    failure: GcCollectError,
}

impl Retirement {
    /// Logs the retirement with the failure's words and traceback; `LiveWorkers::retired`
    /// counts it.
    #[cold]
    fn new(worker: WorkerIndex, failure: GcCollectError) -> Self {
        let retirement = Retirement { worker, failure };
        let traceback = match &retirement.failure {
            GcCollectError::Raised(exception) => exception.traceback(),
            GcCollectError::Panicked(_) => "",
        };
        tracing::error!(
            target: "mipyre::server",
            worker_id = worker.get(),
            error = %retirement.failure,
            traceback,
            "a worker's gc.collect() failed, which signals OOM, heap corruption or interpreter \
             damage: the worker retires and takes no more requests"
        );
        retirement
    }
}

/// Books a `gc.collect()` that returned on `gc`; a failed one is not booked, so it stays due.
fn settle_collect(
    gc: &mut GcSchedule,
    ended: std::thread::Result<Result<(), PyException>>,
) -> Result<(), GcCollectError> {
    match ended {
        Ok(Ok(())) => {
            *gc = gc.collected();
            Ok(())
        }
        Ok(Err(raised)) => Err(GcCollectError::Raised(raised)),
        Err(payload) => Err(GcCollectError::Panicked(panic_message(&*payload))),
    }
}

impl SyncRunner {
    /// Counts a finished request and runs the `gc.collect()` it makes due; a failed collection
    /// retires the worker.
    #[inline]
    fn after_unit(&mut self, py: Python<'_>, worker: WorkerIndex) -> Option<Box<Retirement>> {
        let step = self.gc.after_unit();
        self.gc = step.next;
        if step.due {
            self.collect_due(py, worker)
        } else {
            None
        }
    }

    #[cold]
    #[inline(never)]
    fn collect_due(&mut self, py: Python<'_>, worker: WorkerIndex) -> Option<Box<Retirement>> {
        match self.collect(py) {
            Ok(()) => None,
            Err(failure) => Some(Box::new(Retirement::new(worker, failure))),
        }
    }

    /// Runs a full `gc.collect()`, catching a panic so it is told apart from one in the request.
    /// What it raised is taken off the interpreter, so the next request starts with none set.
    fn collect(&mut self, py: Python<'_>) -> Result<(), GcCollectError> {
        let collect = &self.collect;
        let ended = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            collect
                .bind(py)
                .call0()
                .map(drop)
                .map_err(|raised| PyException::capture(py, &raised))
        }));
        settle_collect(&mut self.gc, ended)
    }
}

/// The response goes to its caller even when the worker retires; `retired` is boxed so a request
/// whose worker goes on serving carries one null pointer.
pub struct ServeOutcome<T = HandlerResponse> {
    pub response: Result<T, HandlerError>,
    pub retired: Option<Box<Retirement>>,
}

impl<T> ServeOutcome<T> {
    #[cold]
    fn panicked(payload: Box<dyn std::any::Any + Send>) -> Self {
        ServeOutcome {
            response: Err(HandlerError::panic(payload)),
            retired: None,
        }
    }
}

/// `interrupted` is decided once the unit ended, so it says whether an error came from the
/// watchdog's interruption.
pub struct UnitOutcome {
    pub(crate) served: ServeOutcome,
    pub(crate) interrupted: bool,
}

impl SubInterpreterWorker<SyncRunner> {
    /// Runs `route` as a unit of work another thread can interrupt, then a `gc.collect()` if one
    /// is due. `begun` gets the unit as soon as it starts. A panic comes back as
    /// [`HandlerError::Panicked`] and a failed collection as [`ServeOutcome::retired`].
    pub fn serve(
        &mut self,
        route: RouteId,
        request: Request,
        begun: impl FnOnce(mipyre_subinterp::InterruptTarget),
    ) -> UnitOutcome {
        self.unit(begun, |inline, py| inline.serve(py, route, request))
    }

    /// Runs the gRPC method `method` as [`Self::serve`] runs a route.
    pub fn serve_unary(
        &mut self,
        method: RouteId,
        request: Request,
        begun: impl FnOnce(mipyre_subinterp::InterruptTarget),
    ) -> UnitOutcome {
        self.unit(begun, |inline, py| inline.serve_unary(py, method, request))
    }

    #[inline(always)]
    fn unit(
        &mut self,
        begun: impl FnOnce(mipyre_subinterp::InterruptTarget),
        call: impl FnOnce(&mut SyncRunner, Python<'_>) -> Result<HandlerResponse, HandlerError>,
    ) -> UnitOutcome {
        let worker = self.worker_id();
        // The unit's slot is made on the first request and reused, so no request allocates one.
        let ran = self.interp.with_unit(|held, unit, py| {
            let inline = &mut held.runner;
            begun(unit.target());
            let response = call(inline, py);
            let interrupted = unit.end().interrupted;
            let retired = inline.after_unit(py, worker);
            UnitOutcome {
                served: ServeOutcome { response, retired },
                interrupted,
            }
        });
        ran.unwrap_or_else(|payload| UnitOutcome {
            served: ServeOutcome::panicked(payload),
            interrupted: false,
        })
    }

    /// Runs `route` as a request nothing can interrupt, so TPC allocates and sets up nothing per
    /// request for a unit of work; otherwise it is as [`Self::serve`].
    pub fn serve_inline(&mut self, route: RouteId, request: Request) -> ServeOutcome {
        let worker = self.worker_id();
        let ran = self.with_gil(|inline, py| {
            let response = inline.serve(py, route, request);
            let retired = inline.after_unit(py, worker);
            ServeOutcome { response, retired }
        });
        ran.unwrap_or_else(ServeOutcome::panicked)
    }

    /// Runs the gRPC method `method` as [`Self::serve_inline`] runs a route.
    pub fn serve_unary_inline(&mut self, method: RouteId, request: Request) -> ServeOutcome {
        let worker = self.worker_id();
        let ran = self.with_gil(|inline, py| {
            let response = inline.serve_unary(py, method, request);
            let retired = inline.after_unit(py, worker);
            ServeOutcome { response, retired }
        });
        ran.unwrap_or_else(ServeOutcome::panicked)
    }

    /// Runs the `before_request` hooks on a WebSocket upgrade and, when none answers, opens the
    /// connection's session; it is a unit of work like a request, nothing can interrupt it.
    pub fn ws_open(
        &mut self,
        route: WsRouteId,
        request: Request,
        outbox: Arc<Outbox>,
    ) -> ServeOutcome<WsUpgradeAnswer> {
        self.ws_unit(|runner, py| {
            runner.ws_handlers(route)?;
            let opened = runner.in_context(py, |runner, chain| {
                websocket::open(chain, &runner.before_hooks, request, outbox)
            })?;
            Ok(match opened {
                Ok(ws) => WsUpgradeAnswer::Accepted(runner.sessions.insert(ws)),
                Err(response) => WsUpgradeAnswer::Refused(response),
            })
        })
    }

    /// Calls the connection's handler for `event`; what it returns is queued on its outbox.
    pub fn ws_call(&mut self, route: WsRouteId, session: &WsSession, event: WsEvent) -> ServeOutcome<()> {
        self.ws_unit(|runner, py| {
            let ws = runner.sessions.ws(py, session.key());
            runner.in_context(py, |runner, chain| {
                websocket::call(chain, runner.ws_handlers(route)?, &ws, event)
            })
        })
    }

    /// Calls the connection's `on_close` and lets go of its session.
    pub fn ws_close(&mut self, route: WsRouteId, session: WsSession, close: &WsClose) -> ServeOutcome<()> {
        self.ws_unit(|runner, py| {
            let ws = runner.sessions.remove(session).into_bound(py);
            runner.in_context(py, |runner, chain| {
                websocket::close(chain, runner.ws_handlers(route)?, &ws, close)
            })
        })
    }

    /// Lets go of a connection's session without calling `on_close`, as a retired worker does.
    pub fn ws_release(&mut self, session: WsSession) {
        if let Err(payload) = self.with_gil(|runner, py| runner.sessions.remove(session).drop_ref(py)) {
            tracing::error!(target: "mipyre::server", panic = %panic_message(&*payload),
                "a Rust panic releasing a WebSocket session; its ws object stays allocated");
        }
    }

    fn ws_unit<T>(
        &mut self,
        call: impl FnOnce(&mut SyncRunner, Python<'_>) -> Result<T, HandlerError>,
    ) -> ServeOutcome<T> {
        let worker = self.worker_id();
        let ran = self.with_gil(|runner, py| {
            let response = call(runner, py);
            let retired = runner.after_unit(py, worker);
            ServeOutcome { response, retired }
        });
        ran.unwrap_or_else(ServeOutcome::panicked)
    }

    /// Runs a `gc.collect()` between requests if the worker went quiet (TPC idle mode); a failure
    /// retires the worker.
    pub fn on_idle_tick(&mut self) -> Result<(), Retirement> {
        let worker = self.worker_id();
        let ticked = self.with_gil(|inline, py| {
            let step = inline.gc.on_idle_tick();
            inline.gc = step.next;
            if step.due {
                inline.collect(py)
            } else {
                Ok(())
            }
        });
        let gc = match ticked {
            Ok(gc) => gc,
            Err(payload) => Err(GcCollectError::Panicked(panic_message(&*payload))),
        };
        gc.map_err(|failure| Retirement::new(worker, failure))
    }
}

/// Creates an asyncio event loop and sets it as this interpreter's current one.
fn new_event_loop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let asyncio = py.import("asyncio")?;
    let event_loop = asyncio.call_method0("new_event_loop")?;
    asyncio.call_method1("set_event_loop", (&event_loop,))?;
    Ok(event_loop)
}

#[cfg(test)]
#[cfg(not(sync_model))]
pub(super) mod tests {
    use super::*;
    use mipyre_subinterp::SubInterpreter;
    use std::ffi::CStr;

    #[test]
    fn a_retirement_counts_its_gc_failure_and_the_worker_as_retired_and_died() {
        let counters = std::sync::Arc::new(crate::WorkerCounters::new());
        let (_, lives) =
            crate::LiveWorkers::new(crate::WorkerKind::PoolSync, 2, std::sync::Arc::new(|_| {}), &counters);
        let life = lives.into_iter().next().expect("a worker's life");
        let retirement = crate::testkit::captured_log_value(|| {
            Retirement::new(WorkerIndex::new(1), GcCollectError::Panicked("boom".into()))
        });
        assert_eq!(life.retired(&retirement), None, "one worker of two is left serving");
        let counted = counters.read();
        assert_eq!(
            (
                counted.gc_failures(),
                counted.workers_retired,
                counted.workers_died,
                counted.workers_serving
            ),
            (1, 1, 1, 1)
        );
        assert_eq!(
            counted.last_worker_retirement.as_deref(),
            Some("worker 1 retired: a Rust panic in gc.collect(): boom")
        );
    }

    /// Returns a schedule with a collection due after two units.
    fn due_schedule() -> GcSchedule {
        let mut gc = GcSchedule::new(GcPolicy::Count {
            every: std::num::NonZeroU64::new(2).expect("Proof: 2 is not 0"),
        });
        assert!(!unit(&mut gc));
        assert!(unit(&mut gc));
        gc
    }

    /// Counts a unit on `gc` and returns whether a collection is due.
    fn unit(gc: &mut GcSchedule) -> bool {
        let step = gc.after_unit();
        *gc = step.next;
        step.due
    }

    #[test]
    fn a_collection_that_returned_is_booked_and_the_count_starts_again() {
        let mut gc = due_schedule();
        assert!(settle_collect(&mut gc, Ok(Ok(()))).is_ok());
        assert!(!unit(&mut gc), "one unit after the collection");
        assert!(unit(&mut gc));
    }

    #[test]
    fn a_collection_that_raised_is_booked_as_nothing_and_hands_over_the_exception() {
        Python::initialize();
        let raised = mipyre_subinterp::attach::with_main(|py| {
            PyException::capture(py, &pyo3::exceptions::PyMemoryError::new_err("gc broke"))
        });
        let mut gc = due_schedule();
        let failure = settle_collect(&mut gc, Ok(Err(raised))).expect_err("it raised");
        assert!(matches!(failure, GcCollectError::Raised(_)), "{failure:?}");
        assert!(
            failure.to_string().starts_with("gc.collect() raised MemoryError"),
            "{failure}"
        );
        assert!(unit(&mut gc), "still due");
    }

    #[test]
    fn a_collection_that_panicked_is_booked_as_nothing_and_its_payload_is_put_into_words() {
        let mut gc = due_schedule();
        let payload: Box<dyn std::any::Any + Send> = Box::new("boom");
        let failure = settle_collect(&mut gc, Err(payload)).expect_err("it panicked");
        assert_eq!(failure.to_string(), "a Rust panic in gc.collect(): boom");
        assert!(unit(&mut gc), "still due");
    }

    fn sync_worker_with_one_route() -> (SubInterpreterWorker<SyncRunner>, RouteId) {
        sync_worker(c"handler = lambda req: 'ok'\nbefore = []\nafter = []\n")
    }

    /// Builds a worker with one route `GET /` whose handler and hook lists are what `app` names
    /// `handler`, `before` and `after`, handing them to `SyncRunner::build` without a script.
    pub(in crate::worker) fn sync_worker(app: &CStr) -> (SubInterpreterWorker<SyncRunner>, RouteId) {
        use mipyre_http::plan::route::{HandlerKind, HandlerPlace, RouteTable};
        let mut table = RouteTable::<(), ()>::new();
        table
            .insert("GET", "/", (), "index".into(), HandlerPlace::Worker(HandlerKind::Sync))
            .expect("one route");
        worker_serving(table, app)
    }

    /// Builds a worker whose one route is the gRPC method `/s.S/M`, its handler what `app` names
    /// `handler`.
    pub(super) fn grpc_worker(app: &CStr) -> (SubInterpreterWorker<SyncRunner>, RouteId) {
        let mut table = mipyre_http::plan::route::RouteTable::<(), ()>::new();
        table.add_grpc_method("/s.S/M", (), "m".into()).expect("one method");
        worker_serving(table, app)
    }

    fn worker_serving(
        table: mipyre_http::plan::route::RouteTable<(), ()>,
        app: &CStr,
    ) -> (SubInterpreterWorker<SyncRunner>, RouteId) {
        let route = table.route_id(0);
        let index = WorkerIndex::new(0);
        let interp = SubInterpreter::create_on_this_thread(
            |source| WorkerStartError::CreationFailed { worker: index, source },
            |py| {
                let raised = |step| {
                    move |e: PyErr| WorkerStartError::SetupRaised {
                        worker: index,
                        step,
                        raised: StartException::capture(py, &e),
                    }
                };
                let names = pyo3::types::PyDict::new(py);
                // The app may raise `GrpcError`, which a worker's script imports from `mipyre.engine`.
                names
                    .set_item("GrpcError", py.get_type::<crate::exchange::GrpcError>())
                    .map_err(raised("naming GrpcError"))?;
                py.run(app, Some(&names), None)
                    .map_err(raised("running the app's source"))?;
                let named = |name: &str| {
                    names
                        .get_item(name)
                        .and_then(|v| v.ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(name.to_owned())))
                        .map_err(raised("reading the app's handler and hooks"))
                };
                let hooks = |name: &str| -> Result<Vec<Bound<'_, PyAny>>, WorkerStartError> {
                    named(name)?
                        .try_iter()
                        .and_then(|hooks| hooks.collect())
                        .map_err(raised("reading the app's hooks"))
                };
                let routes = WorkerRoutes {
                    routes: vec![crate::worker_app::WorkerRoute {
                        key: table.route(route).key.clone(),
                        convention: table.route(route).convention,
                        handler: Some(named("handler")?),
                    }],
                    websockets: Vec::new(),
                    before_hooks: hooks("before")?,
                    after_hooks: hooks("after")?,
                    limits: None,
                };
                SyncRunner::build(py, index, routes, GcPolicy::Off)
                    .map(|runner| super::super::Held { worker: index, runner })
            },
        )
        .map_err(|e: WorkerStartError| e.to_string())
        .expect("a SyncRunner worker on its serving thread");
        (SubInterpreterWorker { index, interp }, route)
    }

    /// Returns a gRPC call to `/s.S/M` whose message is `message`, as the server hands it on.
    pub(super) fn grpc_call(message: &'static [u8]) -> Request {
        let (parts, ()) = hyper::Request::post("/s.S/M")
            .body(())
            .expect("a valid request")
            .into_parts();
        let head = mipyre_http::io::request_head::RequestHead {
            parts,
            params: Default::default(),
            client: mipyre_transport::driver::Peer::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
            request_id: mipyre_http::plan::request_id::RequestId::mint(),
        };
        Request::new(
            head,
            crate::exchange::RequestBody::Buffered(bytes::Bytes::from_static(message)),
        )
    }

    fn get_root() -> Request {
        let (parts, ()) = hyper::Request::get("/").body(()).expect("a valid request").into_parts();
        let head = mipyre_http::io::request_head::RequestHead {
            parts,
            params: Default::default(),
            client: mipyre_transport::driver::Peer::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
            request_id: mipyre_http::plan::request_id::RequestId::mint(),
        };
        Request::new(head, crate::exchange::RequestBody::Buffered(Default::default()))
    }

    /// Returns `BENCH_N`, or 16 for the ordinary test run.
    fn bench_n() -> u64 {
        std::env::var("BENCH_N").map_or(16, |n| n.parse().expect("BENCH_N: a number of calls"))
    }

    fn serve_calls(call: fn(&mut SubInterpreterWorker<SyncRunner>, RouteId)) {
        Python::initialize();
        std::thread::Builder::new()
            .name("serve-calls".into())
            .stack_size(16 << 20)
            .spawn(move || {
                let (mut worker, route) = sync_worker_with_one_route();
                for _ in 0..bench_n() {
                    call(&mut worker, route);
                }
                drop(worker);
            })
            .expect("spawn the worker thread")
            .join()
            .expect("the worker thread ran its calls");
    }

    /// This is also the instruction-count loop of `serve_inline`: run it under `perf stat` and
    /// subtract the `BENCH_N=0` run, which is Python's start-up and the harness.
    ///
    ///   BENCH_N=200000 perf stat -x, -e instructions:u -- target/release/deps/mipyre_runtime-* \
    ///       --exact worker::sync_runner::tests::serve_inline_calls
    #[test]
    fn serve_inline_calls() {
        serve_calls(|worker, route| {
            let served = std::hint::black_box(worker.serve_inline(route, get_root()));
            assert!(served.response.is_ok() && served.retired.is_none());
        });
    }

    /// This is the loop of [`serve_inline_calls`] for `serve`, the pool's call.
    #[test]
    fn serve_calls_as_units() {
        serve_calls(|worker, route| {
            let ran = std::hint::black_box(worker.serve(route, get_root(), |unit| {
                std::hint::black_box(unit);
            }));
            assert!(ran.served.response.is_ok() && ran.served.retired.is_none() && !ran.interrupted);
        });
    }

    /// The last step checks the probe itself, since `serve` does make the `UnitSlot`.
    #[test]
    fn serve_inline_makes_no_unit_of_work() {
        Python::initialize();
        std::thread::Builder::new()
            .name("sync-runner-test".into())
            .stack_size(16 << 20)
            .spawn(|| {
                let (mut worker, route) = sync_worker_with_one_route();
                let served = worker.serve_inline(route, get_root());
                let body = served.response.map(|r| r.body).map_err(|e| e.to_string());
                assert_eq!(body.as_deref(), Ok(&b"ok"[..]), "the route ran");
                assert!(
                    !worker.interp.unit_slot_made(),
                    "serve_inline must not construct or touch UnitSlot"
                );
                let ran = worker.serve(route, get_root(), |_| ());
                assert!(ran.served.response.is_ok());
                assert!(
                    worker.interp.unit_slot_made(),
                    "serve makes the worker's UnitSlot on its first request"
                );
                drop(worker);
            })
            .expect("spawn the worker thread")
            .join()
            .expect("the worker thread's assertions hold");
    }
}

#[cfg(test)]
#[cfg(not(sync_model))]
mod grpc_tests {
    use super::tests::grpc_call;
    use super::*;

    /// Runs `check` on a worker serving the gRPC method of `app`, on a thread of its own.
    fn on_grpc_worker(app: &'static std::ffi::CStr, check: fn(Result<HandlerResponse, HandlerError>)) {
        Python::initialize();
        std::thread::Builder::new()
            .name("grpc-worker-test".into())
            .stack_size(16 << 20)
            .spawn(move || {
                let (mut worker, route) = super::tests::grpc_worker(app);
                check(worker.serve_unary_inline(route, grpc_call(b"\x08\x02")).response);
                drop(worker);
            })
            .expect("spawn the worker thread")
            .join()
            .expect("the worker thread's assertions hold");
    }

    #[test]
    fn a_method_gets_the_message_and_its_bytes_are_the_reply() {
        on_grpc_worker(
            c"handler = lambda m: b'got ' + m\nbefore = [lambda req: 'a hook answered']\nafter = []\n",
            |answer| {
                let answer = answer.expect("the handler answered");
                assert_eq!(&answer.body[..], b"got \x08\x02", "the hooks did not run");
                assert_eq!(answer.content_type(), "application/grpc");
            },
        );
    }

    #[test]
    fn a_reply_that_is_not_bytes_names_its_type() {
        on_grpc_worker(c"handler = lambda m: {'a': 1}\nbefore = []\nafter = []\n", |answer| {
            let e = answer.expect_err("a dict is no reply");
            assert!(
                matches!(e, HandlerError::Response(ResponseError::NotBytes { .. })),
                "{e:?}"
            );
            assert!(e.to_string().contains("returned dict, not bytes"), "{e}");
        });
    }

    #[test]
    fn a_raised_grpc_error_carries_its_code_and_details() {
        on_grpc_worker(
            c"def handler(m): raise GrpcError(5, 'no such sum')\nbefore = []\nafter = []\n",
            |answer| match answer {
                Err(HandlerError::Grpc { code, details, .. }) => {
                    assert_eq!(code, mipyre_http::plan::grpc::GrpcCode::NotFound);
                    assert_eq!(details.as_deref(), Some("no such sum"));
                }
                other => panic!("expected the GrpcError, got {other:?}"),
            },
        );
    }

    #[test]
    fn any_other_exception_is_raised_with_its_text() {
        on_grpc_worker(
            c"def handler(m): raise ValueError('bad sum')\nbefore = []\nafter = []\n",
            |answer| {
                let e = answer.expect_err("it raised");
                assert!(matches!(e, HandlerError::Raised { .. }), "{e:?}");
                assert!(e.to_string().contains("ValueError: bad sum"), "{e}");
            },
        );
    }
}
