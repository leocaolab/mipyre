//! Handles requests on a TPC thread.
//!
//! A `def` worker route runs inline on the TPC thread, in its own sub-interpreter, with no
//! cross-thread wake; an `async def` route goes to the async worker pool, and a `runtime="main"`
//! route or the fallback to the main-interpreter bridge (an `async def` one to main's async
//! engine).
//!
//! The inline path allocates nothing of its own per request: the handler's `Request` takes the
//! method, URI, headers and body bytes hyper already holds, and the log line keeps only a copy of
//! the method and reference counts of the URI and id.

use std::rc::Rc;
use std::time::{Duration, Instant};

use hyper::body::Incoming;
use hyper::{Request, Response};

use crate::exchange;
use crate::exchange::RequestBody;
use crate::exchange::{refuse, HandlerError, ReplyHolder, RequestHead, RequestLabel, UnansweredError, WorkerGroup};
use crate::runtime::{BridgeJob, MainInterpBridge, ServeOutcome};
use crate::serve::site::Site;
use crate::serve::tpc::{ThreadWorker, TpcThread};
use mipyre_http::io::body::{body_channel, stream_body_feeder, ResponseBody};
use mipyre_http::plan::route::{BodyMode, HandlerCall, HandlerKind, HandlerTarget, MainHandler, RouteId};

use super::on_pool::serve_on_pool;
use super::pipeline::{
    await_reply, await_streamed_reply, collect_body, fail, finish, preprocess, reply_or_lost, without_workers,
    AcceptEncoding, Disposition, HandlerRequest, Requester, ResponseSource,
};
use super::respond::{build_main_http_response, http_response};

pub(crate) async fn handle_request_tpc_inline(
    req: Request<Incoming>,
    tpc_thread: Rc<TpcThread>,
    client: mipyre_transport::driver::Peer,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let site = &*tpc_thread.site;
    let prepared = match preprocess(req, site, client).await? {
        Disposition::Answered(r) => return Ok(r),
        Disposition::ForHandler(p) => p,
    };
    // Each kind of worker is built whenever the table has a route for it, and the benches refuse
    // tables with async or main routes, so a missing one is a bug in building the server: that
    // request gets a 500 and the thread keeps serving.
    let resp = match prepared.call {
        HandlerCall::Worker(route, HandlerKind::Sync) => match tpc_thread.worker.as_ref() {
            Some(worker) => run_inline(site, worker, prepared, route).await,
            None => without_workers(site, prepared, WorkerGroup::Subinterp, ResponseSource::Inline),
        },
        // It runs off this thread so the handler's awaits overlap other requests and its budget
        // is enforced on time.
        HandlerCall::Worker(route, kind @ HandlerKind::Async) => match tpc_thread.async_pool.as_ref() {
            Some(pool) => serve_on_pool(pool, site, prepared, route, kind).await,
            None => without_workers(site, prepared, WorkerGroup::Subinterp, ResponseSource::Pool),
        },
        HandlerCall::Lane(route) => super::on_lane::serve_on_lane(site, prepared, route).await,
        HandlerCall::Main(target, handler) => match tpc_thread.main_bridge.as_deref() {
            Some(bridge) => run_on_bridge(site, bridge, prepared, target, handler).await,
            None => without_workers(site, prepared, WorkerGroup::MainBridge, ResponseSource::Bridge),
        },
    };
    Ok(resp)
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
        Ok(bytes) => call_inline(site, worker, route, head, RequestBody::Buffered(bytes), &label).await,
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
    // This is checked after the body is read, because another request on this thread may have
    // retired the worker meanwhile.
    let worker = match thread_worker.serving() {
        Ok(worker) => worker,
        Err(why) => {
            let refused = refuse(UnansweredError::WorkerRetired(why));
            return http_response(Err(refused.log(label)), &accept_encoding, &site.counters).await;
        }
    };
    let request = exchange::Request::new(head, body);
    let called = Instant::now();

    // No watchdog can interrupt an inline call, so its budget is checked after it returns.
    let ServeOutcome { response, retired } = worker.borrow_mut().serve_inline(route, request);
    // The response still goes out; the thread takes no more requests on this worker.
    if let Some(retirement) = retired {
        thread_worker.retire(&retirement);
    }

    let took = called.elapsed();
    let budget = site.rules.settings.limits.request_budget.get();
    let result = if overran(took, budget) {
        Err(refuse(overrun(
            response,
            took,
            budget,
            &site.rules.routes.route(route).name,
        )))
    } else {
        response
    };
    // A failure is logged here, on this thread, once.
    http_response(result.map_err(|e| e.log(label)), &accept_encoding, &site.counters).await
}

/// Returns whether an inline call that took `took` ran past `budget`, in which case its result is
/// not sent.
#[inline]
fn overran(took: Duration, budget: Duration) -> bool {
    took > budget
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

async fn run_on_bridge(
    site: &Site,
    bridge: &MainInterpBridge,
    prepared: HandlerRequest,
    target: HandlerTarget,
    handler: MainHandler,
) -> Response<ResponseBody> {
    let HandlerRequest {
        head,
        body: incoming,
        timer,
        origin,
        ..
    } = prepared;
    let label = head.label();
    let client = head.client;
    let resp = dispatch_to_bridge(bridge, head, incoming, target, handler, site, &label).await;
    finish(
        resp,
        site,
        &label.method,
        label.uri.path(),
        Requester::Given(&label.id, &client),
        timer,
        origin,
        ResponseSource::Bridge,
    )
}

async fn dispatch_to_bridge(
    bridge: &MainInterpBridge,
    head: RequestHead,
    incoming: Incoming,
    target: HandlerTarget,
    handler: MainHandler,
    site: &Site,
    label: &RequestLabel,
) -> Response<ResponseBody> {
    let settings = &site.rules.settings;
    // A streamed body is fed on this thread's LocalSet while the bridge's handler reads it.
    // The feeder's handle is kept so a rejected dispatch can stop it: dropping the receiver
    // alone isn't seen while the feeder waits on the client's next frame.
    let (body, feeder) = match handler.body() {
        BodyMode::Streamed => {
            let (tx, rx) = body_channel();
            let feeder = tokio::task::spawn_local(stream_body_feeder(
                incoming,
                tx,
                settings.limits.max_body.as_usize(),
                settings.limits.body_time(),
            ));
            (RequestBody::streamed(rx), Some(feeder))
        }
        BodyMode::Buffered => match collect_body(incoming, &settings.limits).await {
            Ok(bytes) => (RequestBody::Buffered(bytes), None),
            Err(e) => return fail(e, label, &site.counters),
        },
    };

    let accept_encoding = AcceptEncoding::of(&head, settings.compression.as_ref());
    let (reply, response_rx) = tokio::sync::oneshot::channel();
    let item = BridgeJob {
        target,
        request: exchange::Request::new(head, body),
        reply,
    };
    if let Err(err) = bridge.try_dispatch(item, handler) {
        if let Some(feeder) = feeder {
            feeder.abort();
        }
        return fail(refuse(err), label, &site.counters);
    }

    let reply = reply_or_lost(response_rx, ReplyHolder::MainBridge);
    let reply = match feeder {
        Some(mut feeder) => await_streamed_reply(settings.limits.request_budget, &mut feeder, reply).await,
        None => await_reply(settings.limits.request_budget, reply).await,
    };
    match reply {
        Ok(result) => build_main_http_response(result, &accept_encoding, &site.counters).await,
        Err(e) => fail(e, label, &site.counters),
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

    /// A call that took exactly its budget, or a nanosecond less, is in time: its result goes
    /// out as it came, an error too.
    #[test]
    fn a_call_at_its_budget_is_in_time() {
        assert!(!overran(BUDGET, BUDGET));
        assert!(!overran(BUDGET - Duration::from_nanos(1), BUDGET));
    }

    /// A nanosecond past its budget, a call is an overrun: its result is not sent.
    #[test]
    fn a_call_past_its_budget_is_an_overrun_and_its_result_is_dropped() {
        let took = BUDGET + Duration::from_nanos(1);
        assert!(overran(took, BUDGET));
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

    /// An inline call that came back in time keeps its result, through the lines of `call_inline`
    /// after `serve_inline` on a one-route table. The test is also the instruction-count loop for
    /// those lines: run it under `perf stat -e instructions:u` with `BENCH_N` checks (16 when
    /// unset), subtract the `BENCH_N=0` run, and divide by the checks:
    ///
    ///   BENCH_N=2000000 perf stat -x, -e instructions:u -- target/release/deps/mipyre_engine-* \
    ///       --exact serve::on_tpc::tests::inline_budget_checks
    #[test]
    fn inline_budget_checks() {
        use mipyre_http::plan::response::HandlerResponse;
        use mipyre_http::plan::route::{HandlerKind, HandlerPlace, RouteTable};
        use std::hint::black_box;
        let mut table = RouteTable::<(), ()>::new();
        table
            .insert("GET", "/", (), "index".into(), HandlerPlace::Worker(HandlerKind::Sync))
            .expect("one route");
        let route = table.route_id(0);
        let n: u64 = std::env::var("BENCH_N").map_or(16, |n| n.parse().expect("BENCH_N: a number of checks"));
        for _ in 0..n {
            let response: Result<HandlerResponse, HandlerError> = Ok(HandlerResponse::new(
                hyper::StatusCode::OK,
                bytes::Bytes::from_static(b"ok"),
                hyper::header::HeaderValue::from_static("text/plain"),
                mipyre_http::plan::response::HeaderLines::new(),
            ));
            let took = black_box(Duration::from_micros(20));
            let budget = black_box(BUDGET);
            let table = black_box(&table);
            let result = if overran(took, budget) {
                Err(refuse(overrun(response, took, budget, &table.route(route).name)))
            } else {
                response
            };
            assert!(black_box(result).is_ok(), "in time: the result as it came");
        }
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
