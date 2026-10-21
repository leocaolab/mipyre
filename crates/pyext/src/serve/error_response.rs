//! Renders a logged error as the response the client gets: a 4xx body carries the reason, and a
//! 5xx body is generic plus the request id that the log line holding the real error also carries.

use bytes::Bytes;
use http_body_util::Full;
use hyper::Response;

use crate::exchange::{ErrorRecord, HandlerError, UnansweredError};
use crate::telemetry::RunCounters;
use mipyre_http::plan::response;
use mipyre_http::io::body::BodyReadError;

pub(crate) const GENERIC_500: &str = "Internal Server Error";

impl ErrorRecord {
    /// Returns the response for this error and counts a refusal in `counters`; the error was
    /// already logged where it happened. This match is the one place that maps an error to its
    /// status.
    pub(crate) fn into_response(self, counters: &RunCounters) -> Response<Full<Bytes>> {
        let (error, request_id) = self.into_error_and_id();
        let resp = match error {
            HandlerError::Refused(refusal) => {
                counters.count_refused();
                match refusal {
                    UnansweredError::Overloaded(_) => {
                        response::overloaded_response("server overloaded", &request_id)
                    }
                    UnansweredError::WorkersGone(_) => {
                        response::unavailable_response("server shutting down", &request_id)
                    }
                    UnansweredError::WorkersDied { .. } => {
                        response::unavailable_response("server workers stopped", &request_id)
                    }
                    // Like WorkersDied this is the server's state, not the request's, so it is a
                    // 503; its own text says these lanes may come back while other hosts serve.
                    UnansweredError::NoUsableLane { .. } => {
                        response::unavailable_response("server lanes unavailable", &request_id)
                    }
                    // The lane comes back once its wait is over, so the client is told when.
                    UnansweredError::LaneCrashLooping { retry_in, .. } => response::retry_later_response(
                        "server lane restarting",
                        &request_id,
                        retry_in,
                    ),
                    UnansweredError::WorkerRetired(_) => {
                        response::unavailable_response("server worker retired", &request_id)
                    }
                    UnansweredError::TimedOut(_) | UnansweredError::Overran { .. } | UnansweredError::Interrupted { .. } => {
                        response::gateway_timeout_response(&request_id)
                    }
                    UnansweredError::WorkerLost(_) | UnansweredError::NoWorkers(_) | UnansweredError::NotOnWorker(_) => {
                        response::error_response(GENERIC_500, &request_id)
                    }
                    UnansweredError::WorkerDied { .. }
                    | UnansweredError::PeerCorrupted { .. }
                    | UnansweredError::LaneLost { .. } => {
                        response::bad_gateway_response(&request_id)
                    }
                    UnansweredError::LaneTimedOut { .. } => response::gateway_timeout_response(&request_id),
                }
            }
            HandlerError::BodyRejected(BodyReadError::TooLarge) => {
                response::payload_too_large_response()
            }
            HandlerError::BodyRejected(BodyReadError::TimedOut(_)) => {
                response::request_timeout_response()
            }
            HandlerError::BodyRejected(reject @ BodyReadError::ConnectionLost(_)) => {
                response::bad_request_response(&reject.to_string())
            }
            HandlerError::Raised { .. }
            | HandlerError::ContextNotExited { .. }
            | HandlerError::ContextLost { .. }
            | HandlerError::Panicked { .. }
            | HandlerError::Response(_)
            | HandlerError::ThreadNotSpawned(_)
            | HandlerError::LaneWorkerFailed(_)
            | HandlerError::LaneAnswerMalformed(_)
            | HandlerError::ForeignEventLoop { .. } => {
                response::error_response(GENERIC_500, &request_id)
            }
        };
        resp.map(Full::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::{captured_log_value, label};
    use crate::exchange::{refuse, Capacity, LaneIndex, ReplyHolder, WorkerGroup};
    use mipyre_http::io::request_id::RequestId;

    fn body(resp: Response<Full<Bytes>>) -> serde_json::Value {
        use http_body_util::BodyExt;
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let bytes = rt.block_on(resp.into_body().collect()).unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn a_5xx_body_is_generic_with_the_logged_id() {
        let id = RequestId::mint();
        let cases = [
            (
                HandlerError::Panicked {
                    payload: "secret payload".into(),
                },
                500,
                GENERIC_500,
            ),
            (
                refuse(UnansweredError::WorkerLost(ReplyHolder::PoolWorker)),
                500,
                GENERIC_500,
            ),
            (
                refuse(UnansweredError::Overloaded(Capacity::WorkQueue(
                    WorkerGroup::Subinterp,
                ))),
                503,
                "server overloaded",
            ),
            (
                refuse(UnansweredError::WorkersGone(WorkerGroup::Subinterp)),
                503,
                "server shutting down",
            ),
            (
                refuse(UnansweredError::NoUsableLane {
                    group: WorkerGroup::Lanes,
                    cause: "secret lane cause".into(),
                }),
                503,
                "server lanes unavailable",
            ),
            (refuse(UnansweredError::TimedOut(std::time::Duration::from_secs(30))), 504, "request timeout"),
            (
                refuse(UnansweredError::Overran {
                    handler: "slow".into(),
                    budget: std::time::Duration::from_secs(30),
                    took: std::time::Duration::from_secs(31),
                    raised: Some(Box::new(HandlerError::Panicked {
                        payload: "secret payload".into(),
                    })),
                }),
                504,
                "request timeout",
            ),
            (
                refuse(UnansweredError::Interrupted {
                    budget: std::time::Duration::from_secs(4),
                    raised: Box::new(HandlerError::Panicked {
                        payload: "secret payload".into(),
                    }),
                }),
                504,
                "request timeout",
            ),
        ];
        for (err, status, text) in cases {
            let resp = captured_log_value(|| err.log(&label(&id)).into_response(&crate::telemetry::RunCounters::new(false)));
            assert_eq!(resp.status().as_u16(), status);
            let body = body(resp);
            assert_eq!(
                body,
                serde_json::json!({"error": text, "request_id": id.to_string()})
            );
        }
    }

    /// A lane's worker dying under the request, or the lane losing its worker, is a 502; a
    /// worker killed past the lane timeout is a 504. The body never names the worker.
    #[test]
    fn process_lane_failures_answer_502_and_504() {
        use crate::exchange::WorkerExit;
        let id = RequestId::mint();
        let exit = WorkerExit::Signaled { signal: 9, name: "Killed".to_owned() };
        let ran = std::time::Duration::from_millis(7);
        let cases = [
            (
                UnansweredError::WorkerDied {
                    pid: 4242,
                    lane: LaneIndex(1),
                    ran,
                    exit: exit.clone(),
                },
                502,
            ),
            (
                UnansweredError::LaneLost {
                    lane: LaneIndex(1),
                    cause: "respawn failed".into(),
                },
                502,
            ),
            (
                UnansweredError::LaneTimedOut {
                    pid: 4242,
                    lane: LaneIndex(1),
                    ran,
                    timeout: std::time::Duration::from_millis(5),
                    exit,
                },
                504,
            ),
        ];
        for (refusal, status) in cases {
            let resp = captured_log_value(|| refuse(refusal).log(&label(&id)).into_response(&crate::telemetry::RunCounters::new(false)));
            assert_eq!(resp.status().as_u16(), status);
            assert!(!body(resp).to_string().contains("4242"));
        }
    }

    /// A lane waiting out its crash loop is a 503 that says when to retry; the body never names
    /// the lane's worker or how it ended (that is on the log line).
    #[test]
    fn a_crash_looping_lane_answers_503_with_retry_after() {
        let id = RequestId::mint();
        let refusal = UnansweredError::LaneCrashLooping {
            lane: LaneIndex(1),
            crashes: 4,
            exit: crate::exchange::WorkerExit::Exited(3),
            retry_in: std::time::Duration::from_millis(1200),
        };
        let resp = captured_log_value(|| refuse(refusal).log(&label(&id)).into_response(&crate::telemetry::RunCounters::new(false)));
        assert_eq!(resp.status().as_u16(), 503);
        assert_eq!(resp.headers()["retry-after"], "2");
        let body = body(resp).to_string();
        assert!(!body.contains("status 3"), "{body}");
    }

    #[test]
    fn a_4xx_body_carries_the_reason() {
        let id = RequestId::mint();
        let resp = captured_log_value(|| {
            HandlerError::BodyRejected(BodyReadError::TooLarge)
                .log(&label(&id))
                .into_response(&crate::telemetry::RunCounters::new(false))
        });
        assert_eq!(resp.status().as_u16(), 413);
        assert_eq!(
            body(resp),
            serde_json::json!({"error": "payload too large"})
        );
        let resp = captured_log_value(|| {
            HandlerError::BodyRejected(BodyReadError::TimedOut(std::time::Duration::from_secs(30)))
                .log(&label(&id))
                .into_response(&crate::telemetry::RunCounters::new(false))
        });
        assert_eq!(resp.status().as_u16(), 408);
        assert_eq!(
            body(resp),
            serde_json::json!({"error": "request body timeout"})
        );
    }

    #[test]
    fn every_refusal_is_answered_as_one_and_nothing_else_is() {
        let id = RequestId::mint();
        let answered = |err: HandlerError| {
            let counters = RunCounters::new(true);
            captured_log_value(|| err.log(&label(&id)).into_response(&counters));
            counters.read().refused
        };
        let refusals = [
            UnansweredError::Overloaded(Capacity::WorkQueue(WorkerGroup::Subinterp)),
            UnansweredError::WorkersGone(WorkerGroup::Subinterp),
            UnansweredError::WorkersDied {
                group: WorkerGroup::Subinterp,
                cause: "gone".into(),
            },
            UnansweredError::NoUsableLane {
                group: WorkerGroup::Lanes,
                cause: "gone".into(),
            },
            UnansweredError::WorkerRetired("gc.collect() raised".into()),
            UnansweredError::TimedOut(std::time::Duration::from_secs(30)),
            UnansweredError::WorkerLost(ReplyHolder::PoolWorker),
            UnansweredError::NoWorkers(WorkerGroup::Subinterp),
            UnansweredError::NotOnWorker(0),
            UnansweredError::Overran {
                handler: "slow".into(),
                budget: std::time::Duration::from_secs(1),
                took: std::time::Duration::from_secs(2),
                raised: None,
            },
            UnansweredError::Interrupted {
                budget: std::time::Duration::from_secs(1),
                raised: Box::new(HandlerError::Panicked {
                    payload: "interrupted".into(),
                }),
            },
            UnansweredError::WorkerDied {
                pid: 1,
                lane: LaneIndex(0),
                ran: std::time::Duration::from_secs(1),
                exit: crate::exchange::lane_failure::WorkerExit::Exited(1),
            },
            UnansweredError::LaneTimedOut {
                pid: 1,
                lane: LaneIndex(0),
                ran: std::time::Duration::from_secs(2),
                timeout: std::time::Duration::from_secs(1),
                exit: crate::exchange::lane_failure::WorkerExit::Exited(1),
            },
            UnansweredError::LaneLost {
                lane: LaneIndex(0),
                cause: "gone".into(),
            },
            UnansweredError::PeerCorrupted {
                pid: 1,
                lane: LaneIndex(0),
                fault: mipyre_shm::plan::ring::PeerCounterError::AnswersPastSent {
                    read: mipyre_shm::plan::ring::Seq::new(0),
                    sent: mipyre_shm::plan::ring::Seq::new(1),
                    published: mipyre_shm::plan::ring::Seq::new(2),
                },
            },
            UnansweredError::LaneCrashLooping {
                lane: LaneIndex(0),
                crashes: 3,
                exit: crate::exchange::lane_failure::WorkerExit::Exited(3),
                retry_in: std::time::Duration::from_millis(100),
            },
        ];
        for refusal in refusals {
            assert_eq!(answered(refuse(refusal)), 1);
        }
        for err in [
            HandlerError::Panicked {
                payload: "boom".into(),
            },
            HandlerError::BodyRejected(BodyReadError::TooLarge),
        ] {
            assert_eq!(answered(err), 0);
        }
    }

    #[test]
    fn a_foreign_event_loop_is_a_generic_500() {
        let id = RequestId::mint();
        let resp = captured_log_value(|| {
            HandlerError::ForeignEventLoop {
                loop_interp: 0,
                request_interp: 3,
            }
            .log(&label(&id))
            .into_response(&crate::telemetry::RunCounters::new(false))
        });
        assert_eq!(resp.status().as_u16(), 500);
        assert_eq!(
            body(resp),
            serde_json::json!({"error": GENERIC_500, "request_id": id.to_string()})
        );
    }

    #[test]
    fn a_logged_error_falls_back_to_500() {
        let id = RequestId::mint();
        let resp = captured_log_value(|| {
            refuse(UnansweredError::WorkerLost(ReplyHolder::PoolWorker))
                .log(&label(&id))
                .into_response(&crate::telemetry::RunCounters::new(false))
        });
        assert_eq!(resp.status().as_u16(), 500);
    }
}
