//! Serves one request of any server: the pipeline answers it, or it goes to the backend that runs
//! its route's handler, and its response is finished here. Every server's requests enter and leave
//! through this one place.

use std::future::Future;
use std::ops::Deref;

use hyper::body::Incoming;
use hyper::{Request, Response};
use tokio_util::task::AbortOnDropHandle;

use crate::site::Site;
use mipyre_http::io::body::ResponseBody;
use mipyre_http::plan::route::{HandlerCall, HandlerKind, RouteId, RuntimeChoice};
use mipyre_runtime::exchange::{refuse, UnansweredError};
use mipyre_runtime::{InterpreterPool, MainThreads};

use super::on_main::run_on_main;
use super::on_pool::serve_on_pool;
use super::on_process::serve_on_process;
use super::pipeline::{fail, finish, preprocess, Disposition, HandlerRequest, Requester, ResponseSource};

/// The backends one server built for its route table, and where a task of one of its requests runs.
/// A server knows which of its backends runs a worker route; it does not choose for a request.
pub trait Backends {
    /// The worker that runs a `def` route on the request's own thread; a server with none says
    /// `Infallible`, so its requests carry no inline call.
    type Inline;

    fn site(&self) -> &Site;

    /// Returns the backend of this server's `def` worker routes.
    fn sync_worker(&self) -> Option<WorkerBackend<'_, Self::Inline>>;

    /// Returns the pool of this server's `async def` worker routes.
    fn async_pool(&self) -> Option<&InterpreterPool>;

    /// Runs `prepared` on `worker`, on the request's own thread, and finishes its response.
    fn run_inline<'a>(
        worker: &'a Self::Inline,
        site: &'a Site,
        prepared: HandlerRequest,
        route: RouteId,
    ) -> impl Future<Output = Response<ResponseBody>> + 'a;

    fn main(&self) -> Option<&MainThreads>;

    fn spawn_feeder<F: Future<Output = ()> + Send + 'static>(feeder: F) -> AbortOnDropHandle<()>;
}

/// The backend that runs a server's `def` sub-interpreter routes.
pub enum WorkerBackend<'a, I> {
    /// The server's worker on the request's own thread.
    Inline(&'a I),
    Pool(&'a InterpreterPool),
}

/// Answers `req`, or hands it to the backend `server` has for its route, and finishes the response.
///
/// It is generic over the server so that each server's requests poll their own copy: one future
/// shared by two servers stays out of line and costs both of them.
pub async fn handle_request<B: Backends>(
    req: Request<Incoming>,
    server: impl Deref<Target = B>,
    client: mipyre_transport::driver::Peer,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let site = server.site();
    let prepared = match preprocess(req, site, client).await? {
        Disposition::Answered(r) => return Ok(r),
        Disposition::ForHandler(p) => p,
    };
    // Each arm awaits one future that finishes the response itself, so what it holds for the
    // access log is not kept beside every arm's future in this one.
    let resp = match prepared.call {
        HandlerCall::Worker(route, kind @ HandlerKind::Sync) => match server.sync_worker() {
            Some(WorkerBackend::Inline(worker)) => B::run_inline(worker, site, prepared, route).await,
            Some(WorkerBackend::Pool(pool)) => serve_on_pool(pool, site, prepared, route, kind).await,
            None => without_backend(site, prepared, RuntimeChoice::Worker),
        },
        HandlerCall::Worker(route, kind @ HandlerKind::Async) => match server.async_pool() {
            Some(pool) => serve_on_pool(pool, site, prepared, route, kind).await,
            None => without_backend(site, prepared, RuntimeChoice::Worker),
        },
        HandlerCall::Lane(route) => match site.process.as_deref() {
            Some(lanes) => serve_on_process(site, lanes, prepared, route).await,
            None => without_backend(site, prepared, RuntimeChoice::Lane),
        },
        HandlerCall::Main(target, handler) => match server.main() {
            Some(main) => run_on_main::<B>(site, main, prepared, target, handler).await,
            None => without_backend(site, prepared, RuntimeChoice::Main),
        },
    };
    Ok(resp)
}

/// Answers 500 for a request the route table sent to a backend this server was built without.
/// Each backend is built whenever the table has a route for it, so this is a bug in building the
/// server: that request fails and the server keeps serving. No handler ran, so the access log says
/// the engine answered.
#[cold]
#[inline(never)]
fn without_backend(site: &Site, prepared: HandlerRequest, runtime: RuntimeChoice) -> Response<ResponseBody> {
    let label = prepared.head.label();
    let resp = fail(refuse(UnansweredError::NoWorkers(runtime)), &label, &site.counters);
    finish(
        resp,
        site,
        &label.method,
        label.uri.path(),
        Requester::Given(&label.id, &prepared.head.client),
        prepared.timer,
        prepared.origin,
        ResponseSource::Engine,
    )
}
