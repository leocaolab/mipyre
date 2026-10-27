//! Hands `runtime="process"` requests to a process-lane worker and awaits the answer without
//! blocking the thread; CORS, the access log, admission (503), the budget (504) and the request id
//! stay in the server.

use std::future::ready;

use crate::site::Site;
use mipyre_core::dispatch;
use mipyre_http::io::body::{BodyReadError, ResponseBody};
use mipyre_http::io::request_head::{RequestHead, RequestLabel};
use mipyre_http::plan::response::HandlerResponse;
use mipyre_http::plan::route::RouteId;
use mipyre_process::{LaneRequest, LaneRequestError, ServerLanes};
use mipyre_runtime::exchange::{refuse, HandlerError, UnansweredError};

use super::pipeline::{collect_body, fail, finish, AcceptEncoding, HandlerRequest, Requester, ResponseSource};
use super::respond::http_response;

pub(super) async fn serve_on_process(
    site: &Site,
    lanes: &ServerLanes,
    prepared: HandlerRequest,
    route: RouteId,
) -> hyper::Response<ResponseBody> {
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
    lanes: &ServerLanes,
    head: RequestHead,
    body: hyper::body::Incoming,
    route: RouteId,
    label: &RequestLabel,
) -> hyper::Response<ResponseBody> {
    let accept_encoding = AcceptEncoding::of(&head, site.rules.settings.compression.as_ref());
    match answer(site, lanes, head, body, route).await {
        Ok(data) => http_response(Ok(data), &accept_encoding, &site.counters).await,
        Err(error) => fail(error, label, &site.counters),
    }
}

async fn answer(
    site: &Site,
    lanes: &ServerLanes,
    head: RequestHead,
    body: hyper::body::Incoming,
    route: RouteId,
) -> Result<HandlerResponse, HandlerError> {
    let body = collect_body(body, &site.rules.settings.limits).await?;
    let request = LaneRequest {
        route_key: site.rules.routes.route(route).key.clone(),
        head,
        body,
    };
    let budget = site.rules.settings.limits.request_budget;
    // A refusal now and a failure in the reply are one value: the lanes refuse the same way after
    // taking a request (another host they forwarded it to was full), and either is answered alike.
    let failure = 'failed: {
        let wait = match dispatch(lanes, request, budget, ready(())) {
            Ok(wait) => wait,
            Err(refusal) => break 'failed refusal,
        };
        match wait.await {
            Some(Ok(response)) => return Ok(response),
            Some(Err(failure)) => failure,
            None => return Err(refuse(UnansweredError::TimedOut(budget.get()))),
        }
    };
    Err(handler_error(failure))
}

/// Converts a lane failure into the request's error, counting a refusal here on the request's side so a
/// request that already timed out is not counted twice.
fn handler_error(failure: LaneRequestError) -> HandlerError {
    match failure {
        LaneRequestError::Rejected(refusal) => refuse(UnansweredError::Lane(refusal)),
        LaneRequestError::TooLarge(too_large) => {
            tracing::debug!(
                target: "mipyre::server",
                needed = too_large.needed,
                room = too_large.room,
                "a request does not fit a process-lane slot"
            );
            HandlerError::BodyRejected(BodyReadError::TooLarge)
        }
        LaneRequestError::WorkerFailed(failure) => HandlerError::LaneWorkerFailed(failure),
        LaneRequestError::Malformed(malformed) => HandlerError::LaneAnswerMalformed(malformed),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use mipyre_process::{LaneDecodeError, LaneIndex, LaneUnansweredError, TaggedField, WorkerExit, WorkerFailure};
    use mipyre_shm::channel::{PeerFault, SlotRoomError};
    use mipyre_shm::plan::ring::{PeerCounterError, Seq};

    /// Returns the status and body a lane failure is answered with, through the request's error.
    fn response_of(failure: LaneRequestError) -> (u16, String) {
        use http_body_util::BodyExt;
        let id = mipyre_http::plan::request_id::RequestId::mint();
        let resp = mipyre_runtime::testkit::captured_log_value(|| {
            crate::error_response::render(
                handler_error(failure).log(&mipyre_runtime::testkit::label(&id)),
                &crate::SiteCounters::of_own_run(false),
            )
        });
        let status = resp.status().as_u16();
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let body = rt.block_on(resp.into_body().collect()).unwrap().to_bytes();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    fn status_of(failure: LaneRequestError) -> u16 {
        response_of(failure).0
    }

    /// Each way a lane request fails is its request's error and status: the lanes' refusal as it
    /// is (a dead worker 502), a request too large for a slot 413, the worker's failure and an
    /// answer that cannot be read 500, whose body says neither (the log line does).
    #[test]
    fn every_lane_failure_is_its_request_error_and_status() {
        let died = || {
            LaneRequestError::Rejected(LaneUnansweredError::WorkerDied {
                pid: 42,
                lane: LaneIndex(0),
                ran: Some(Duration::from_millis(3)),
                exit: WorkerExit::Exited(3),
            })
        };
        assert!(matches!(
            handler_error(died()),
            HandlerError::Refused(UnansweredError::Lane(LaneUnansweredError::WorkerDied {
                pid: 42,
                lane: LaneIndex(0),
                ..
            }))
        ));
        assert_eq!(status_of(died()), 502);

        let too_large = || LaneRequestError::TooLarge(SlotRoomError { needed: 9, room: 8 });
        assert!(matches!(
            handler_error(too_large()),
            HandlerError::BodyRejected(BodyReadError::TooLarge)
        ));
        assert_eq!(status_of(too_large()), 413);

        let failed = || {
            LaneRequestError::WorkerFailed(WorkerFailure {
                error: "ValueError: 7f3a".into(),
                traceback: Some("Traceback ...".into()),
            })
        };
        match handler_error(failed()) {
            HandlerError::LaneWorkerFailed(f) => {
                assert_eq!(f.error, "ValueError: 7f3a");
                assert_eq!(f.traceback.as_deref(), Some("Traceback ..."));
            }
            other => panic!("expected the worker's failure, got {other:?}"),
        }
        let (status, body) = response_of(failed());
        assert_eq!(status, 500);
        assert!(!body.contains("7f3a") && !body.contains("Traceback"), "{body}");

        let malformed = || {
            LaneRequestError::Malformed(LaneDecodeError::UnknownTag {
                field: TaggedField::AnswerKind,
                tag: 9,
            })
        };
        assert!(matches!(
            handler_error(malformed()),
            HandlerError::LaneAnswerMalformed(LaneDecodeError::UnknownTag { tag: 9, .. })
        ));
        assert_eq!(status_of(malformed()), 500);
    }

    /// A 502 is what a request of a corrupted lane is answered, and its body never names the
    /// worker.
    #[test]
    fn a_request_of_a_corrupted_lane_is_502() {
        let corrupted = LaneRequestError::Rejected(LaneUnansweredError::PeerCorrupted {
            pid: 4242,
            lane: LaneIndex(0),
            fault: PeerFault::Counter(PeerCounterError::AnswersPastSent {
                read: Seq::new(0),
                sent: Seq::new(1),
                published: Seq::new(7),
            }),
        });
        let (status, body) = response_of(corrupted);
        assert_eq!(status, 502);
        assert!(!body.contains("4242"), "{body}");
    }
}
