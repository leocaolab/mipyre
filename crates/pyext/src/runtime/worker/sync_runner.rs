//! Serves requests on a worker from Rust: the hook chain for the TPC threads and the pool's sync
//! workers, and the `gc.collect()` that retires a worker when it fails.

use mipyre_subinterp::context::SpareContext;
use mipyre_subinterp::gc::{GcPolicy, GcSchedule};
use pyo3::prelude::*;

use super::start::{StartException, WorkerStartError};
use super::{RequestRunner, SubInterpreterWorker, WorkerIndex};
use crate::exchange::error::{refuse, HandlerError, PyException, RequestStage, UnansweredError};
use crate::exchange::request::Request;
use crate::runtime::hook_chain::{worker_response, Chain, EventLoopProvider, ExceptionMapping};
use crate::runtime::request_context::{in_reused_context, RequestContext};
use crate::runtime::worker_app::WorkerRoutes;
use mipyre_http::plan::response::HandlerResponse;
use mipyre_http::plan::route::{RouteId, RouteKey};
use mipyre_tpc::panic_message;

/// A route of a [`SyncRunner`]; `handler` is `None` for a route that does not run on a worker.
pub(super) struct InlineRoute {
    pub(super) handler: Option<Py<PyAny>>,
    key: RouteKey,
}

/// A runner that drives the hook chain from Rust, for the TPC threads and the pool's sync workers.
pub(crate) struct SyncRunner {
    /// It is indexed like main's route table.
    pub(super) routes: Vec<InlineRoute>,
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
            })
            .collect();
        Ok(SyncRunner {
            routes: inline_routes,
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
        let owned = self
            .routes
            .into_iter()
            .filter_map(|route| route.handler)
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
            // handler here does not run on a worker and is refused.
            None => {
                let Some(InlineRoute {
                    handler: Some(handler),
                    key,
                }) = self.routes.get(route.index())
                else {
                    return Err(refuse(UnansweredError::NotOnWorker(route.index())));
                };
                let value = chain.handler(handler, &req, ExceptionMapping::NoLoopHint { key, handler })?;
                chain.after(&self.after_hooks, &req, worker_response(value)?)?
            }
        };

        #[cfg(feature = "leak_detect")]
        crate::telemetry::record_drop(req.as_any());
        Ok(response)
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
pub(crate) struct Retirement {
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
            gc.collected();
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
        if self.gc.after_unit() {
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
pub(crate) struct ServeOutcome {
    pub(crate) response: Result<HandlerResponse, HandlerError>,
    pub(crate) retired: Option<Box<Retirement>>,
}

impl ServeOutcome {
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
pub(crate) struct UnitOutcome {
    pub(crate) served: ServeOutcome,
    pub(crate) interrupted: bool,
}

impl SubInterpreterWorker<SyncRunner> {
    /// Runs `route` as a unit of work another thread can interrupt, then a `gc.collect()` if one
    /// is due. `begun` gets the unit as soon as it starts. A panic comes back as
    /// [`HandlerError::Panicked`] and a failed collection as [`ServeOutcome::retired`].
    pub(crate) fn serve(
        &mut self,
        route: RouteId,
        request: Request,
        begun: impl FnOnce(mipyre_subinterp::InterruptTarget),
    ) -> UnitOutcome {
        let worker = self.worker_id();
        // The unit's slot is made on the first request and reused, so no request allocates one.
        let ran = self.interp.with_unit(|held, unit, py| {
            let inline = &mut held.runner;
            let target = unit.target();
            begun(target.clone());
            let response = inline.serve(py, route, request);
            unit.end();
            let interrupted = target.interrupted();
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
    pub(crate) fn serve_inline(&mut self, route: RouteId, request: Request) -> ServeOutcome {
        let worker = self.worker_id();
        let ran = self.with_gil(|inline, py| {
            let response = inline.serve(py, route, request);
            let retired = inline.after_unit(py, worker);
            ServeOutcome { response, retired }
        });
        ran.unwrap_or_else(ServeOutcome::panicked)
    }

    /// Runs a `gc.collect()` between requests if the worker went quiet (TPC idle mode); a failure
    /// retires the worker.
    pub(crate) fn on_idle_tick(&mut self) -> Result<(), Retirement> {
        let worker = self.worker_id();
        let ticked = self.with_gil(|inline, py| {
            if inline.gc.on_idle_tick() {
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
pub(super) mod tests {
    use super::*;
    use mipyre_subinterp::SubInterpreter;
    use std::ffi::CStr;

    #[test]
    fn a_retirement_counts_its_gc_failure_and_the_worker_as_retired_and_died() {
        let counters = std::sync::Arc::new(crate::telemetry::RunCounters::new(false));
        let (_, lives) = crate::runtime::LiveWorkers::new(
            crate::runtime::WorkerKind::PoolSync,
            2,
            <crate::runtime::ServerRunStop>::default(),
            std::sync::Arc::clone(&counters),
        );
        let life = lives.into_iter().next().expect("a worker's life");
        let retirement = crate::exchange::captured_log_value(|| {
            Retirement::new(WorkerIndex::new(1), GcCollectError::Panicked("boom".into()))
        });
        assert_eq!(life.retired(&retirement), None, "one worker of two is left serving");
        let counted = counters.read();
        assert_eq!(
            (
                counted.gc_failures,
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
        assert!(!gc.after_unit());
        assert!(gc.after_unit());
        gc
    }

    #[test]
    fn a_collection_that_returned_is_booked_and_the_count_starts_again() {
        let mut gc = due_schedule();
        assert!(settle_collect(&mut gc, Ok(Ok(()))).is_ok());
        assert!(!gc.after_unit(), "one unit after the collection");
        assert!(gc.after_unit());
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
        assert!(gc.after_unit(), "still due");
    }

    #[test]
    fn a_collection_that_panicked_is_booked_as_nothing_and_its_payload_is_put_into_words() {
        let mut gc = due_schedule();
        let payload: Box<dyn std::any::Any + Send> = Box::new("boom");
        let failure = settle_collect(&mut gc, Err(payload)).expect_err("it panicked");
        assert_eq!(failure.to_string(), "a Rust panic in gc.collect(): boom");
        assert!(gc.after_unit(), "still due");
    }

    fn sync_worker_with_one_route() -> (SubInterpreterWorker<SyncRunner>, RouteId) {
        sync_worker(c"handler = lambda req: 'ok'\nbefore = []\nafter = []\n")
    }

    /// Builds a worker with one route `GET /` whose handler and hook lists are what `app` names
    /// `handler`, `before` and `after`, handing them to `SyncRunner::build` without a script.
    pub(in crate::runtime::worker) fn sync_worker(app: &CStr) -> (SubInterpreterWorker<SyncRunner>, RouteId) {
        use mipyre_http::plan::route::{HandlerKind, HandlerPlace, RouteTable};
        let mut table = RouteTable::<(), ()>::new();
        table
            .insert("GET", "/", (), "index".into(), HandlerPlace::Worker(HandlerKind::Sync))
            .expect("one route");
        let route = table.route_id(0);
        let index = WorkerIndex::new(0);
        let interp = SubInterpreter::create_on_this_thread(
            "b287".into(),
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
                    routes: vec![crate::runtime::worker_app::WorkerRoute {
                        key: table.route(route).key.clone(),
                        handler: Some(named("handler")?),
                    }],
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

    fn get_root() -> Request {
        let (parts, ()) = hyper::Request::get("/").body(()).expect("a valid request").into_parts();
        let head = crate::exchange::RequestHead {
            parts,
            params: Vec::new(),
            client: mipyre_transport::driver::Peer::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
            request_id: mipyre_http::io::request_id::RequestId::mint(),
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
                worker.end_on_own_thread();
            })
            .expect("spawn the worker thread")
            .join()
            .expect("the worker thread ran its calls");
    }

    /// This is also the instruction-count loop of `serve_inline`: run it under `perf stat` and
    /// subtract the `BENCH_N=0` run, which is Python's start-up and the harness.
    ///
    ///   BENCH_N=200000 perf stat -x, -e instructions:u -- target/release/deps/mipyre_engine-* \
    ///       --exact runtime::worker::sync_runner::tests::serve_inline_calls
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
            .name("b287-worker".into())
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
                worker.end_on_own_thread();
            })
            .expect("spawn the worker thread")
            .join()
            .expect("the worker thread's assertions hold");
    }
}
