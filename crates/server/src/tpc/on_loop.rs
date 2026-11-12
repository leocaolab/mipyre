//! Runs a TPC thread's `async def` routes as Tasks on its worker's event loop. A request here owns
//! its seat, its budget, its 503 and its 504, and never cancels its Task; it holds only the reply's
//! future, not asyncio's Task. When the loop turns is its driver's (`loop_driver.rs`).

use hyper::body::Incoming;
use hyper::Response;
use tokio::time::Instant;

use crate::on_pool::{takes_upfront_permit, ADMISSION_SKIP_BYTES};
use crate::pipeline::{fail, finish, AcceptEncoding, HandlerRequest, Requester, ResponseSource};
use crate::respond::http_response;
use crate::site::Site;
use mipyre_http::io::body::{collect, ResponseBody};
use mipyre_http::io::request_head::{RequestHead, RequestLabel};
use mipyre_http::plan::response::HandlerResponse;
use mipyre_http::plan::route::RouteId;
use mipyre_runtime::exchange::{self, refuse, ErrorRecord, RequestBody, UnansweredError};

use super::admission::{LoopAdmission, LoopSeat};
use super::conn::LoopWorker;

/// Runs `route`'s `async def` handler on `on`'s loop and finishes its response.
pub(crate) async fn run_on_loop(
    site: &Site,
    on: LoopWorker<'_>,
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
    let resp = serve_on_loop(site, on, head, body, route, &label).await;
    finish(
        resp,
        site,
        &label.method,
        label.uri.path(),
        Requester::Given(&label.id, &client),
        timer,
        origin,
        ResponseSource::Loop,
    )
}

/// Seats the request, reads its body under the seat's gate and answers it from the loop.
async fn serve_on_loop(
    site: &Site,
    on: LoopWorker<'_>,
    head: RequestHead,
    body: Incoming,
    route: RouteId,
    label: &RequestLabel,
) -> Response<ResponseBody> {
    let settings = &site.rules.settings;
    let limits = on.event_loop.limits;
    let Some(live) = on.taking() else {
        return fail(refuse(UnansweredError::LoopClosed), label, &site.counters);
    };
    let declared_large = takes_upfront_permit(&head.parts.headers);
    let mut seat = match on.event_loop.seats.take(live, limits, declared_large) {
        Ok(seat) => seat,
        Err(refused) => return fail(refused, label, &site.counters),
    };
    let accept_encoding = AcceptEncoding::of(&head, settings.compression.as_ref());
    let mut admission = LoopAdmission {
        seat: &mut seat,
        skip_bytes: ADMISSION_SKIP_BYTES,
        limits,
        live: || on.taking(),
    };
    let body = match collect(body, &mut admission, &settings.limits).await {
        Ok(body) => body,
        Err(refused) => return fail(refused, label, &site.counters),
    };
    let request = exchange::Request::new(head, RequestBody::buffered(body));
    let answer = call_on_loop(site, on, route, request, seat, label).await;
    http_response(answer, &accept_encoding, &site.counters).await
}

/// Starts the request's Task and waits for its reply within the budget, counted from before the
/// Task's first step. Past the budget the request gets 504 and the Task runs on, its reply dropped;
/// a reply that comes after the budget, because the thread was held, is an overrun.
async fn call_on_loop(
    site: &Site,
    on: LoopWorker<'_>,
    route: RouteId,
    request: exchange::Request,
    seat: LoopSeat<'_>,
    label: &RequestLabel,
) -> Result<HandlerResponse, ErrorRecord> {
    let budget = site.rules.settings.limits.request_budget.get();
    let failure = 'failed: {
        let lease = match on.worker.lease() {
            Ok(lease) => lease,
            Err(why) => break 'failed refuse(UnansweredError::WorkerRetired(why)),
        };
        let started_at = Instant::now();
        let reply = match lease.start_task(on.event_loop, route, request, seat.large()) {
            Ok(reply) => reply,
            Err(refused) => break 'failed refused,
        };
        // From here the loop counts the Task as live, so the seat is given back.
        drop(seat);
        // The deadline is watched only when the reply is not there at once.
        let expiry = async { on.event_loop.deadlines.watch(started_at + budget).await };
        let replied = tokio::select! {
            biased;
            replied = reply => Some(replied),
            () = expiry => None,
        };
        let answer = match replied {
            Some(Ok(answer)) => answer,
            Some(Err(lost)) => break 'failed lost,
            None => break 'failed refuse(UnansweredError::TimedOut(budget)),
        };
        let took = started_at.elapsed();
        if took <= budget {
            return answer;
        }
        // What the handler raised was logged with its answer, so the overrun does not repeat it.
        refuse(UnansweredError::Overran {
            handler: site.rules.routes.route(route).name.clone(),
            budget,
            took,
            raised: None,
        })
    };
    Err(failure.log(label))
}
