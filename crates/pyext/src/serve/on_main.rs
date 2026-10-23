//! Runs handlers on the main interpreter from a Tokio thread.
//!
//! GIL mode runs every handler this way, TPC or not; the sub-interpreter pool runs its
//! `runtime="main"` routes and the fallback this way too.

use mipyre_http::plan::limits::Limits;
use std::sync::Arc;

use hyper::body::Incoming;
use hyper::{Request, Response};

use crate::exchange;
use crate::exchange::RequestBody;
use crate::exchange::{refuse, HandlerError, ReplyHolder, RequestHead, UnansweredError};
use crate::runtime::call_handler_with_hooks;
use crate::serve::site::Site;
use mipyre_http::io::body::{body_channel, stream_body_feeder, ResponseBody};
use mipyre_http::plan::route::{BodyMode, HandlerCall, HandlerTarget};

use super::pipeline::{
    await_reply, await_streamed_reply, collect_body, fail, finish, preprocess, AcceptEncoding, Disposition,
    HandlerRequest, Requester, ResponseSource,
};
use super::respond::build_main_http_response;

pub(crate) async fn handle_request(
    req: Request<Incoming>,
    site: Arc<Site>,
    client: mipyre_transport::driver::Peer,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let prepared = match preprocess(req, &site, client).await? {
        Disposition::Answered(r) => return Ok(r),
        Disposition::ForHandler(p) => p,
    };
    // In GIL mode every handler runs on main; a worker route's body is buffered.
    let (target, body) = match prepared.call {
        HandlerCall::Lane(route) => return Ok(super::on_lane::serve_on_lane(&site, prepared, route).await),
        HandlerCall::Worker(id, _) => (site.rules.routes.route(id).handler, BodyMode::Buffered),
        HandlerCall::Main(target, handler) => (target, handler.body()),
    };
    Ok(run_on_main(&site, prepared, target, body, ResponseSource::Main).await)
}

/// Runs `target`'s handler on the main interpreter from a blocking-pool thread, within the
/// request budget, and finishes the response.
pub(crate) async fn run_on_main(
    site: &Arc<Site>,
    prepared: HandlerRequest,
    target: HandlerTarget,
    body: BodyMode,
    served: ResponseSource,
) -> Response<ResponseBody> {
    let HandlerRequest {
        head,
        body: incoming,
        timer,
        origin,
        ..
    } = prepared;
    // The `Request` takes the head; the log lines and the response keep their own copies.
    let label = head.label();
    let client = head.client;
    let accept_encoding = AcceptEncoding::of(&head, site.rules.settings.compression.as_ref());

    let limits = site.rules.settings.limits;
    let resp = match main_request(head, incoming, body, &limits).await {
        Ok((sky_req, feeder)) => {
            let site_ref = Arc::clone(site);
            let task = tokio::task::spawn_blocking(move || call_handler_with_hooks(&site_ref.rules, target, sky_req));
            let task = async { task.await.map_err(task_lost) };
            let reply = match feeder {
                Some(mut feeder) => await_streamed_reply(limits.request_budget, &mut feeder, task).await,
                None => await_reply(limits.request_budget, task).await,
            };
            match reply {
                Ok(result) => build_main_http_response(result, &accept_encoding, &site.counters).await,
                Err(e) => fail(e, &label, &site.counters),
            }
        }
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
        served,
    )
}

/// Builds the `Request` a main-interpreter handler gets, with the body collected, or fed through
/// `req.stream` by the returned task for a `stream=True` route.
async fn main_request(
    head: RequestHead,
    incoming: Incoming,
    body: BodyMode,
    limits: &Limits,
) -> Result<(exchange::Request, Option<tokio::task::JoinHandle<()>>), HandlerError> {
    let (body, feeder) = match body {
        BodyMode::Streamed => {
            let (tx, rx) = body_channel();
            let feeder = tokio::spawn(stream_body_feeder(
                incoming,
                tx,
                limits.max_body.as_usize(),
                limits.body_time(),
            ));
            (RequestBody::streamed(rx), Some(feeder))
        }
        BodyMode::Buffered => (RequestBody::Buffered(collect_body(incoming, limits).await?), None),
    };
    Ok((exchange::Request::new(head, body), feeder))
}

/// Turns a blocking task that ended without a reply into its panic with the payload, or into a
/// lost worker when it was cancelled.
fn task_lost(e: tokio::task::JoinError) -> HandlerError {
    match e.try_into_panic() {
        Ok(payload) => HandlerError::panic(payload),
        Err(_) => refuse(UnansweredError::WorkerLost(ReplyHolder::TaskCancelled)),
    }
}
