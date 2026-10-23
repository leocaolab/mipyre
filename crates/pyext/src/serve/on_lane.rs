//! Hands `runtime="process"` requests to a process-lane worker and awaits the answer without
//! blocking the thread; CORS, the access log, admission (503), the budget (504) and the request id
//! stay in the server.

use crate::exchange::{ReplyHolder, RequestLabel, WorkerGroup};
use crate::runtime::process::{LaneJob, LaneRequest};
use crate::serve::site::Site;
use mipyre_http::io::body::ResponseBody;
use mipyre_http::plan::route::RouteId;

use super::pipeline::{
    await_reply, collect_body, fail, finish, reply_or_lost, without_workers, AcceptEncoding, HandlerRequest, Requester,
    ResponseSource,
};
use super::respond::http_response;

pub(crate) async fn serve_on_lane(
    site: &Site,
    prepared: HandlerRequest,
    route: RouteId,
) -> hyper::Response<ResponseBody> {
    // A served table with a `runtime="process"` route always has its lanes: `start` refuses
    // one without (and the benches serve only inline sync routes).
    let Some(lanes) = site.process.as_ref() else {
        return without_workers(site, prepared, WorkerGroup::Lanes, ResponseSource::Process);
    };
    let HandlerRequest {
        head,
        body,
        timer,
        origin,
        ..
    } = prepared;
    let label = head.label();
    let client = head.client;
    let resp = run(site, lanes, head, body, route, &label).await;
    finish(
        resp,
        site,
        &label.method,
        label.uri.path(),
        Requester::Given(&label.id, &client),
        timer,
        origin,
        ResponseSource::Process,
    )
}

async fn run(
    site: &Site,
    lanes: &std::sync::Arc<crate::runtime::process::ServerLanes>,
    head: crate::exchange::RequestHead,
    body: hyper::body::Incoming,
    route: RouteId,
    label: &RequestLabel,
) -> hyper::Response<ResponseBody> {
    let accept_encoding = AcceptEncoding::of(&head, site.rules.settings.compression.as_ref());
    let body = match collect_body(body, &site.rules.settings.limits).await {
        Ok(b) => b,
        Err(e) => return fail(e, label, &site.counters),
    };
    let (tx, rx) = tokio::sync::oneshot::channel();
    let key = &site.rules.routes.route(route).key;
    let job = LaneJob {
        request: LaneRequest {
            route_key: key.clone(),
            head,
            body,
        },
        reply: tx,
    };
    if let Err((failure, _reply)) = lanes.submit(job) {
        return fail(failure.into_handler_error(), label, &site.counters);
    }
    let reply = reply_or_lost(rx, ReplyHolder::Lane);
    let outcome = match await_reply(site.rules.settings.limits.request_budget, reply).await {
        Ok(o) => o,
        Err(e) => return fail(e, label, &site.counters),
    };
    match outcome {
        Ok(data) => http_response(Ok(data), &accept_encoding, &site.counters).await,
        Err(failure) => fail(failure.into_handler_error(), label, &site.counters),
    }
}
