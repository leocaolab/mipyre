//! Handles requests on a TPC thread.
//!
//! A `def` worker route runs inline on the TPC thread, in its own sub-interpreter, with no
//! cross-thread wake; an `async def` route runs as a Task on that worker's event loop, on the same
//! thread; a `runtime="main"` route or the fallback goes to the main interpreter's threads (an
//! `async def` one to main's async engine).
//!
//! The inline path allocates nothing of its own per request: the handler's `Request` takes the
//! method, URI, headers and body bytes hyper already holds, and the log line keeps only a copy of
//! the method and reference counts of the URI and id.

use std::future::Future;
use std::time::Duration;

use hyper::Response;
use tokio_util::task::AbortOnDropHandle;

use crate::site::Site;
use crate::tpc::{run_on_loop, LoopWorker, ThreadWorker, TpcThread, WorkerLease};
use mipyre_core::InlineTimer;
use mipyre_http::io::body::ResponseBody;
use mipyre_http::io::request_head::{RequestHead, RequestLabel};
use mipyre_http::plan::response::HandlerResponse;
use mipyre_http::plan::route::RouteId;
use mipyre_runtime::exchange;
use mipyre_runtime::exchange::RequestBody;
use mipyre_runtime::exchange::{refuse, HandlerError, UnansweredError};
use mipyre_runtime::MainThreads;

use super::pipeline::{collect_body, fail, finish, AcceptEncoding, HandlerRequest, Requester, ResponseSource};
use super::request::{Backends, LoopBackend, WorkerBackend};
use super::respond::http_response;

impl Backends for TpcThread {
    type Inline = ThreadWorker;
    type OnLoop<'a> = LoopWorker<'a>;

    #[inline]
    fn site(&self) -> &Site {
        &self.site
    }

    #[inline]
    fn sync_worker(&self) -> Option<WorkerBackend<'_, ThreadWorker>> {
        self.worker.as_ref().map(WorkerBackend::Inline)
    }

    /// Returns the event loop an `async def` route runs on, on this thread, so the handler's
    /// awaits overlap other requests.
    #[inline]
    fn async_worker(&self) -> Option<LoopBackend<'_, LoopWorker<'_>>> {
        self.on_loop().map(LoopBackend::OnLoop)
    }

    #[inline]
    fn run_inline<'a>(
        worker: &'a ThreadWorker,
        site: &'a Site,
        prepared: HandlerRequest,
        route: RouteId,
    ) -> impl Future<Output = Response<ResponseBody>> + 'a {
        run_inline(site, worker, prepared, route)
    }

    #[inline]
    fn run_on_loop<'a>(
        on: LoopWorker<'a>,
        site: &'a Site,
        prepared: HandlerRequest,
        route: RouteId,
    ) -> impl Future<Output = Response<ResponseBody>> + 'a {
        run_on_loop(site, on, prepared, route)
    }

    #[inline]
    fn call_unary_inline(
        worker: &ThreadWorker,
        site: &Site,
        method: RouteId,
        request: exchange::Request,
    ) -> Result<HandlerResponse, HandlerError> {
        call_worker(site, worker, method, request, WorkerLease::serve_unary)
    }

    #[inline]
    fn main(&self) -> Option<&MainThreads> {
        self.main.as_deref()
    }

    /// Spawns `feeder` on this thread's `LocalSet`, which every task of its connections runs on.
    #[inline]
    fn spawn_feeder<F: Future<Output = ()> + Send + 'static>(feeder: F) -> AbortOnDropHandle<()> {
        AbortOnDropHandle::new(tokio::task::spawn_local(feeder))
    }
}

/// Runs a worker route on this thread's sub-interpreter.
///
/// The call blocks this thread (which is the point: peer TPC threads keep serving), so
/// nothing on it can answer before the handler returns. The request budget is enforced
/// when it does: a handler that ran past the app's request budget gets the same 504 as on
/// the other paths, not its late result.
async fn run_inline(
    site: &Site,
    worker: &ThreadWorker,
    prepared: HandlerRequest,
    route: RouteId,
) -> Response<ResponseBody> {
    let HandlerRequest {
        head,
        body,
        timer,
        origin,
        ..
    } = prepared;
    let label = head.label();
    let client = head.client;
    let resp = match collect_body(body, &site.rules.settings.limits).await {
        Ok(frames) => call_inline(site, worker, route, head, RequestBody::buffered(frames), &label).await,
        Err(e) => fail(e, &label, &site.counters),
    };
    finish(
        resp,
        site,
        &label.method,
        label.uri.path(),
        Requester::Given(&label.id, &client),
        timer,
        origin,
        ResponseSource::Inline,
    )
}

async fn call_inline(
    site: &Site,
    thread_worker: &ThreadWorker,
    route: RouteId,
    head: RequestHead,
    body: RequestBody,
    label: &RequestLabel,
) -> Response<ResponseBody> {
    let accept_encoding = AcceptEncoding::of(&head, site.rules.settings.compression.as_ref());
    let result = call_worker(
        site,
        thread_worker,
        route,
        exchange::Request::new(head, body),
        WorkerLease::serve,
    );
    // A failure is logged here, on this thread, once.
    http_response(result.map_err(|e| e.log(label)), &accept_encoding, &site.counters).await
}

/// Runs `route` on this thread's worker by `serve` and holds its answer to the request budget: a
/// handler that ran past it gets the budget's refusal, not its late answer.
#[inline]
fn call_worker<'w>(
    site: &Site,
    thread_worker: &'w ThreadWorker,
    route: RouteId,
    request: exchange::Request,
    serve: impl FnOnce(WorkerLease<'w>, RouteId, exchange::Request) -> Result<HandlerResponse, HandlerError>,
) -> Result<HandlerResponse, HandlerError> {
    // This is checked after the body is read, because another request on this thread may have
    // retired the worker meanwhile.
    let lease = thread_worker
        .lease()
        .map_err(|why| refuse(UnansweredError::WorkerRetired(why)))?;
    let timer = InlineTimer::start();

    let response = serve(lease, route, request);

    let budget = site.rules.settings.limits.request_budget;
    match timer.overran(budget) {
        None => response,
        Some(took) => Err(refuse(overrun(
            response,
            took,
            budget.get(),
            &site.rules.routes.route(route).name,
        ))),
    }
}

/// Builds the error for a call to `handler` that ran past `budget`, carrying what the handler
/// raised so the overrun's one log line shows it.
#[cold]
#[inline(never)]
fn overrun<T>(result: Result<T, HandlerError>, took: Duration, budget: Duration, handler: &str) -> UnansweredError {
    UnansweredError::Overran {
        handler: handler.to_owned(),
        budget,
        took,
        raised: result.err().map(Box::new),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUDGET: Duration = Duration::from_secs(30);

    fn raised() -> HandlerError {
        HandlerError::Panicked {
            payload: "late and broken".into(),
        }
    }

    /// An overrun names the handler, the budget and how long the call took; the result is not sent.
    #[test]
    fn a_call_past_its_budget_is_an_overrun_and_its_result_is_dropped() {
        let took = BUDGET + Duration::from_nanos(1);
        let UnansweredError::Overran {
            handler,
            budget,
            took: t,
            raised,
        } = overrun(Ok::<_, HandlerError>(7), took, BUDGET, "slow")
        else {
            panic!("an overrun");
        };
        assert_eq!((handler.as_str(), budget, t), ("slow", BUDGET, took));
        assert!(raised.is_none());
    }

    /// What a late handler raised is carried in the overrun, for its one log line.
    #[test]
    fn an_overrun_carries_what_the_handler_raised() {
        let late = BUDGET + Duration::from_secs(1);
        let UnansweredError::Overran {
            raised: Some(raised), ..
        } = overrun::<()>(Err(raised()), late, BUDGET, "slow")
        else {
            panic!("an overrun carrying the error");
        };
        assert!(raised.to_string().contains("late and broken"), "{raised}");
    }
}
