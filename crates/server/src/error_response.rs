//! Renders a logged error as the response the client gets: a 4xx body carries the reason, and a
//! 5xx body is generic plus the request id that the log line holding the real error also carries.

use bytes::Bytes;
use http_body_util::Full;
use hyper::Response;

use crate::SiteCounters;
use mipyre_http::io::body::BodyReadError;
use mipyre_http::plan::response;
use mipyre_process::LaneUnansweredError;
use mipyre_runtime::exchange::{ErrorRecord, HandlerError, UnansweredError};

pub(crate) const GENERIC_500: &str = "Internal Server Error";

/// Returns the response for `record`'s error and counts a refusal in `counters`; the error was
/// already logged where it happened. This match is the one place that maps an error to its status.
pub(crate) fn render(record: ErrorRecord, counters: &SiteCounters) -> Response<Full<Bytes>> {
    let (error, request_id) = record.into_error_and_id();
    let resp = match error {
        HandlerError::Refused(refusal) => {
            counters.count_refused();
            match refusal {
                UnansweredError::Overloaded(_) | UnansweredError::Lane(LaneUnansweredError::Full) => {
                    response::overloaded_response("server overloaded", &request_id)
                }
                UnansweredError::WorkersGone(_) | UnansweredError::Lane(LaneUnansweredError::ShuttingDown) => {
                    response::unavailable_response("server shutting down", &request_id)
                }
                UnansweredError::WorkersDied { .. }
                | UnansweredError::Lane(LaneUnansweredError::WorkersDied { .. }) => {
                    response::unavailable_response("server workers stopped", &request_id)
                }
                // Like WorkersDied this is the server's state, not the request's, so it is a
                // 503; its own text says these lanes may come back while other hosts serve.
                UnansweredError::Lane(LaneUnansweredError::NoUsableLane { .. }) => {
                    response::unavailable_response("server lanes unavailable", &request_id)
                }
                // The lane comes back once its wait is over, so the client is told when.
                UnansweredError::Lane(LaneUnansweredError::LaneCrashLooping { retry_in, .. }) => {
                    response::retry_later_response("server lane restarting", &request_id, retry_in)
                }
                UnansweredError::WorkerRetired(_) => {
                    response::unavailable_response("server worker retired", &request_id)
                }
                UnansweredError::TimedOut(_)
                | UnansweredError::Overran { .. }
                | UnansweredError::Interrupted { .. } => response::gateway_timeout_response(&request_id),
                UnansweredError::WorkerLost(_)
                | UnansweredError::NoWorkers(_)
                | UnansweredError::NotOnWorker(_)
                | UnansweredError::Lane(LaneUnansweredError::Dropped) => {
                    response::error_response(GENERIC_500, &request_id)
                }
                UnansweredError::Lane(
                    LaneUnansweredError::WorkerDied { .. }
                    | LaneUnansweredError::WorkerDiedBeforeTaking { .. }
                    | LaneUnansweredError::PeerCorrupted { .. }
                    | LaneUnansweredError::LaneLost { .. },
                ) => response::bad_gateway_response(&request_id),
                UnansweredError::Lane(LaneUnansweredError::LaneTimedOut { .. }) => {
                    response::gateway_timeout_response(&request_id)
                }
            }
        }
        HandlerError::BodyRejected(BodyReadError::TooLarge) => response::payload_too_large_response(),
        HandlerError::BodyRejected(BodyReadError::TimedOut(_)) => response::request_timeout_response(),
        HandlerError::BodyRejected(reject @ BodyReadError::ConnectionLost(_)) => {
            response::bad_request_response(&reject.to_string())
        }
        HandlerError::Http { answer, .. } => response::build_response(*answer),
        HandlerError::Raised { .. }
        | HandlerError::ContextNotExited { .. }
        | HandlerError::ContextLost { .. }
        | HandlerError::Panicked { .. }
        | HandlerError::Response(_)
        | HandlerError::ThreadNotSpawned(_)
        | HandlerError::LaneWorkerFailed(_)
        | HandlerError::LaneAnswerMalformed(_)
        | HandlerError::ForeignEventLoop { .. } => response::error_response(GENERIC_500, &request_id),
    };
    resp.map(Full::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mipyre_http::plan::request_id::RequestId;
    use mipyre_http::plan::route::RuntimeChoice;
    use mipyre_process::{LaneIndex, WorkerExit};
    use mipyre_runtime::exchange::error::RequestStage;
    use mipyre_runtime::exchange::error::WorkerGroup;
    use mipyre_runtime::exchange::{refuse, Capacity, ReplyHolder};
    use mipyre_runtime::testkit::{captured_log, captured_log_value, label};
    use mipyre_runtime::testkit::{raised, with_a_context_left_entered};
    use pyo3::Python;

    fn body(resp: Response<Full<Bytes>>) -> serde_json::Value {
        use http_body_util::BodyExt;
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
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
                refuse(UnansweredError::Overloaded(Capacity::WorkQueue(WorkerGroup::Subinterp))),
                503,
                "server overloaded",
            ),
            (
                refuse(UnansweredError::WorkersGone(WorkerGroup::Subinterp)),
                503,
                "server shutting down",
            ),
            (
                refuse(UnansweredError::Lane(LaneUnansweredError::NoUsableLane {
                    cause: "secret lane cause".into(),
                })),
                503,
                "server lanes unavailable",
            ),
            (
                refuse(UnansweredError::TimedOut(std::time::Duration::from_secs(30))),
                504,
                "request timeout",
            ),
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
            let resp = captured_log_value(|| render(err.log(&label(&id)), &crate::SiteCounters::of_own_run(false)));
            assert_eq!(resp.status().as_u16(), status);
            let body = body(resp);
            assert_eq!(body, serde_json::json!({"detail": text, "request_id": id.to_string()}));
        }
    }

    /// A lane's worker dying under the request, or the lane losing its worker, is a 502; a
    /// worker killed past the lane timeout is a 504. The body never names the worker.
    #[test]
    fn process_lane_failures_answer_502_and_504() {
        let id = RequestId::mint();
        let exit = WorkerExit::Signaled {
            signal: 9,
            name: "Killed".to_owned(),
        };
        let ran = std::time::Duration::from_millis(7);
        let cases = [
            (
                UnansweredError::Lane(LaneUnansweredError::WorkerDied {
                    pid: 4242,
                    lane: LaneIndex(1),
                    ran: Some(ran),
                    exit: exit.clone(),
                }),
                502,
            ),
            (
                UnansweredError::Lane(LaneUnansweredError::LaneLost {
                    lane: LaneIndex(1),
                    cause: "respawn failed".into(),
                }),
                502,
            ),
            (
                UnansweredError::Lane(LaneUnansweredError::WorkerDiedBeforeTaking {
                    pid: 4242,
                    lane: LaneIndex(1),
                    exit: exit.clone(),
                }),
                502,
            ),
            (
                UnansweredError::Lane(LaneUnansweredError::LaneTimedOut {
                    pid: 4242,
                    lane: LaneIndex(1),
                    ran,
                    timeout: std::time::Duration::from_millis(5),
                    exit,
                }),
                504,
            ),
        ];
        for (refusal, status) in cases {
            let resp = captured_log_value(|| {
                render(
                    refuse(refusal).log(&label(&id)),
                    &crate::SiteCounters::of_own_run(false),
                )
            });
            assert_eq!(resp.status().as_u16(), status);
            assert!(!body(resp).to_string().contains("4242"));
        }
    }

    /// A lane waiting out its crash loop is a 503 that says when to retry; the body never names
    /// the lane's worker or how it ended (that is on the log line).
    #[test]
    fn a_crash_looping_lane_answers_503_with_retry_after() {
        let id = RequestId::mint();
        let refusal = UnansweredError::Lane(LaneUnansweredError::LaneCrashLooping {
            lane: LaneIndex(1),
            crashes: 4,
            exit: WorkerExit::Exited(3),
            retry_in: std::time::Duration::from_millis(1200),
        });
        let resp = captured_log_value(|| {
            render(
                refuse(refusal).log(&label(&id)),
                &crate::SiteCounters::of_own_run(false),
            )
        });
        assert_eq!(resp.status().as_u16(), 503);
        assert_eq!(resp.headers()["retry-after"], "2");
        let body = body(resp).to_string();
        assert!(!body.contains("status 3"), "{body}");
    }

    #[test]
    fn every_lane_refusal_keeps_its_status_and_body() {
        let id = RequestId::mint();
        let exit = || WorkerExit::Exited(1);
        let cases = [
            (LaneUnansweredError::Full, 503, "server overloaded"),
            (LaneUnansweredError::ShuttingDown, 503, "server shutting down"),
            (
                LaneUnansweredError::WorkersDied { cause: "gone".into() },
                503,
                "server workers stopped",
            ),
            (
                LaneUnansweredError::NoUsableLane { cause: "gone".into() },
                503,
                "server lanes unavailable",
            ),
            (
                LaneUnansweredError::LaneCrashLooping {
                    lane: LaneIndex(0),
                    crashes: 3,
                    exit: exit(),
                    retry_in: std::time::Duration::from_secs(1),
                },
                503,
                "server lane restarting",
            ),
            (LaneUnansweredError::Dropped, 500, GENERIC_500),
            (
                LaneUnansweredError::WorkerDied {
                    pid: 1,
                    lane: LaneIndex(0),
                    ran: None,
                    exit: exit(),
                },
                502,
                "worker process died",
            ),
            (
                LaneUnansweredError::WorkerDiedBeforeTaking {
                    pid: 1,
                    lane: LaneIndex(0),
                    exit: exit(),
                },
                502,
                "worker process died",
            ),
            (
                LaneUnansweredError::PeerCorrupted {
                    pid: 1,
                    lane: LaneIndex(0),
                    fault: mipyre_shm::channel::PeerFault::Counter(
                        mipyre_shm::plan::ring::PeerCounterError::AnswersPastSent {
                            read: mipyre_shm::plan::ring::Seq::new(0),
                            sent: mipyre_shm::plan::ring::Seq::new(1),
                            published: mipyre_shm::plan::ring::Seq::new(2),
                        },
                    ),
                },
                502,
                "worker process died",
            ),
            (
                LaneUnansweredError::LaneLost {
                    lane: LaneIndex(0),
                    cause: "gone".into(),
                },
                502,
                "worker process died",
            ),
            (
                LaneUnansweredError::LaneTimedOut {
                    pid: 1,
                    lane: LaneIndex(0),
                    ran: std::time::Duration::from_secs(2),
                    timeout: std::time::Duration::from_secs(1),
                    exit: exit(),
                },
                504,
                "request timeout",
            ),
        ];
        for (refusal, status, text) in cases {
            let resp = captured_log_value(|| {
                render(
                    refuse(UnansweredError::Lane(refusal)).log(&label(&id)),
                    &crate::SiteCounters::of_own_run(false),
                )
            });
            assert_eq!(resp.status().as_u16(), status);
            assert_eq!(
                body(resp),
                serde_json::json!({"detail": text, "request_id": id.to_string()})
            );
        }
    }

    #[test]
    fn a_4xx_body_carries_the_reason() {
        let id = RequestId::mint();
        let resp = captured_log_value(|| {
            render(
                HandlerError::BodyRejected(BodyReadError::TooLarge).log(&label(&id)),
                &crate::SiteCounters::of_own_run(false),
            )
        });
        assert_eq!(resp.status().as_u16(), 413);
        assert_eq!(body(resp), serde_json::json!({"detail": "payload too large"}));
        let resp = captured_log_value(|| {
            render(
                HandlerError::BodyRejected(BodyReadError::TimedOut(std::time::Duration::from_secs(30)))
                    .log(&label(&id)),
                &crate::SiteCounters::of_own_run(false),
            )
        });
        assert_eq!(resp.status().as_u16(), 408);
        assert_eq!(body(resp), serde_json::json!({"detail": "request body timeout"}));
    }

    /// Returns the error hyper hands a body read when the client shuts the connection three bytes
    /// into a body that announced eight.
    fn body_cut_short() -> hyper::Error {
        use http_body_util::BodyExt;
        use tokio::io::AsyncWriteExt;

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a test runtime builds");
        rt.block_on(async {
            let (done, done_rx) = tokio::sync::oneshot::channel();
            let handed = std::sync::Mutex::new(Some(done));
            let service = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let done = handed.lock().expect("one request at a time").take();
                async move {
                    if let Some(done) = done {
                        let read = req.into_body().collect().await.map(|_| ());
                        done.send(read)
                            .unwrap_or_else(|_| panic!("the test waits for the read"));
                    }
                    Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::new())))
                }
            });
            let (mut client, server) = tokio::io::duplex(4096);
            let conn = tokio::spawn(
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(server), service),
            );
            client
                .write_all(b"POST / HTTP/1.1\r\nhost: t\r\ncontent-length: 8\r\n\r\nabc")
                .await
                .expect("the pipe holds it");
            client.shutdown().await.expect("the pipe shuts");
            let read = done_rx.await.expect("hyper served the request");
            conn.abort();
            read.expect_err("the body was cut short")
        })
    }

    #[test]
    fn a_body_cut_short_is_a_400_with_the_reason() {
        let id = RequestId::mint();
        let reject = BodyReadError::ConnectionLost(std::sync::Arc::new(body_cut_short()));
        let resp = captured_log_value(|| {
            render(
                HandlerError::BodyRejected(reject).log(&label(&id)),
                &crate::SiteCounters::of_own_run(false),
            )
        });
        assert_eq!(resp.status().as_u16(), 400);
        assert_eq!(
            body(resp),
            serde_json::json!({"detail": "request body read failed: error reading a body from connection"})
        );
    }

    #[test]
    fn an_http_exception_is_its_own_status_headers_and_body_logged_at_debug() {
        use mipyre_http::plan::response::{HandlerResponse, HeaderLines, JSON};
        let id = RequestId::mint();
        let mut lines = HeaderLines::new();
        assert!(lines
            .append(
                hyper::header::WWW_AUTHENTICATE,
                hyper::header::HeaderValue::from_static("Bearer")
            )
            .is_none());
        let err = HandlerError::Http {
            stage: mipyre_runtime::exchange::RequestStage::BeforeHook,
            answer: Box::new(HandlerResponse::new(
                hyper::StatusCode::UNAUTHORIZED,
                Bytes::from_static(b"{\"detail\":\"login required\"}"),
                JSON,
                lines,
            )),
        };
        let counters = SiteCounters::of_own_run(true);
        let mut resp = None;
        let log = mipyre_runtime::testkit::captured_log_from(tracing::Level::DEBUG, || {
            resp = Some(render(err.log(&label(&id)), &counters));
        });
        let resp = resp.unwrap();
        assert_eq!(resp.status().as_u16(), 401);
        assert_eq!(resp.headers()["www-authenticate"], "Bearer");
        assert_eq!(resp.headers()["content-type"], "application/json");
        assert_eq!(body(resp), serde_json::json!({"detail": "login required"}));
        assert_eq!(counters.run.read().refused, 0);
        let lines: Vec<_> = log.lines().collect();
        assert_eq!(lines.len(), 1, "{log}");
        assert!(
            lines[0].contains("\"level\":\"DEBUG\"") && !lines[0].contains("traceback"),
            "{log}"
        );
    }

    #[test]
    fn every_refusal_is_answered_as_one_and_nothing_else_is() {
        let id = RequestId::mint();
        let answered = |err: HandlerError| {
            let counters = SiteCounters::of_own_run(true);
            captured_log_value(|| render(err.log(&label(&id)), &counters));
            counters.run.read().refused
        };
        let refusals = [
            UnansweredError::Overloaded(Capacity::WorkQueue(WorkerGroup::Subinterp)),
            UnansweredError::WorkersGone(WorkerGroup::Subinterp),
            UnansweredError::WorkersDied {
                group: WorkerGroup::Subinterp,
                cause: "gone".into(),
            },
            UnansweredError::Lane(LaneUnansweredError::NoUsableLane { cause: "gone".into() }),
            UnansweredError::WorkerRetired("gc.collect() raised".into()),
            UnansweredError::TimedOut(std::time::Duration::from_secs(30)),
            UnansweredError::WorkerLost(ReplyHolder::PoolWorker),
            UnansweredError::NoWorkers(RuntimeChoice::Worker),
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
            UnansweredError::Lane(LaneUnansweredError::WorkerDied {
                pid: 1,
                lane: LaneIndex(0),
                ran: Some(std::time::Duration::from_secs(1)),
                exit: WorkerExit::Exited(1),
            }),
            UnansweredError::Lane(LaneUnansweredError::WorkerDiedBeforeTaking {
                pid: 1,
                lane: LaneIndex(0),
                exit: WorkerExit::Exited(1),
            }),
            UnansweredError::Lane(LaneUnansweredError::LaneTimedOut {
                pid: 1,
                lane: LaneIndex(0),
                ran: std::time::Duration::from_secs(2),
                timeout: std::time::Duration::from_secs(1),
                exit: WorkerExit::Exited(1),
            }),
            UnansweredError::Lane(LaneUnansweredError::LaneLost {
                lane: LaneIndex(0),
                cause: "gone".into(),
            }),
            UnansweredError::Lane(LaneUnansweredError::PeerCorrupted {
                pid: 1,
                lane: LaneIndex(0),
                fault: mipyre_shm::channel::PeerFault::Counter(
                    mipyre_shm::plan::ring::PeerCounterError::AnswersPastSent {
                        read: mipyre_shm::plan::ring::Seq::new(0),
                        sent: mipyre_shm::plan::ring::Seq::new(1),
                        published: mipyre_shm::plan::ring::Seq::new(2),
                    },
                ),
            }),
            UnansweredError::Lane(LaneUnansweredError::LaneCrashLooping {
                lane: LaneIndex(0),
                crashes: 3,
                exit: WorkerExit::Exited(3),
                retry_in: std::time::Duration::from_millis(100),
            }),
        ];
        for refusal in refusals {
            assert_eq!(answered(refuse(refusal)), 1);
        }
        for err in [
            HandlerError::Panicked { payload: "boom".into() },
            HandlerError::BodyRejected(BodyReadError::TooLarge),
        ] {
            assert_eq!(answered(err), 0);
        }
    }

    #[test]
    fn a_foreign_event_loop_is_a_generic_500() {
        let id = RequestId::mint();
        let resp = captured_log_value(|| {
            render(
                HandlerError::ForeignEventLoop {
                    loop_interp: 0,
                    request_interp: 3,
                }
                .log(&label(&id)),
                &crate::SiteCounters::of_own_run(false),
            )
        });
        assert_eq!(resp.status().as_u16(), 500);
        assert_eq!(
            body(resp),
            serde_json::json!({"detail": GENERIC_500, "request_id": id.to_string()})
        );
    }

    #[test]
    fn a_logged_error_falls_back_to_500() {
        let id = RequestId::mint();
        let resp = captured_log_value(|| {
            render(
                refuse(UnansweredError::WorkerLost(ReplyHolder::PoolWorker)).log(&label(&id)),
                &crate::SiteCounters::of_own_run(false),
            )
        });
        assert_eq!(resp.status().as_u16(), 500);
    }

    #[test]
    fn a_context_not_left_after_the_handler_raised_keeps_both_errors() {
        Python::initialize();
        std::thread::spawn(|| {
            mipyre_subinterp::attach::with_main(|py| {
                let failed = with_a_context_left_entered::<()>(py, |py| Err(raised(py, "the handler's own failure")))
                    .expect_err("the context could not be left");
                let HandlerError::ContextNotExited { exit, handler } = &failed else {
                    panic!("not ContextNotExited: {failed}");
                };
                assert!(exit.to_string().starts_with("RuntimeError: "), "{exit}");
                assert!(
                    matches!(
                        **handler,
                        HandlerError::Raised {
                            stage: RequestStage::Handler,
                            ..
                        }
                    ),
                    "{handler}"
                );
                assert!(handler.to_string().contains("the handler's own failure"), "{handler}");

                let id = RequestId::mint();
                let mut record = None;
                let log = captured_log(|| record = Some(failed.log(&label(&id))));
                let lines: Vec<_> = log.lines().collect();
                assert_eq!(lines.len(), 1, "{log}");
                let line = lines[0];
                assert!(line.contains(&id.to_string()), "{line}");
                assert!(line.contains("the handler's own failure"), "{line}");
                assert!(
                    line.contains("then leaving the request's contextvars.Context raised RuntimeError"),
                    "{line}"
                );
                let resp = render(record.expect("logged"), &SiteCounters::of_own_run(false));
                assert_eq!(resp.status().as_u16(), 500);
            })
        })
        .join()
        .expect("the test thread does not panic");
    }

    #[test]
    fn a_context_not_left_after_the_handler_succeeded_fails_the_request() {
        Python::initialize();
        std::thread::spawn(|| {
            mipyre_subinterp::attach::with_main(|py| {
                let failed = with_a_context_left_entered(py, |_py| Ok("the response"))
                    .expect_err("the context could not be left");
                let HandlerError::Raised {
                    stage: RequestStage::ContextExit,
                    exception,
                } = &failed
                else {
                    panic!("not a ContextExit failure: {failed}");
                };
                assert!(exception.to_string().starts_with("RuntimeError: "), "{exception}");
                let id = RequestId::mint();
                let mut record = None;
                let log = captured_log(|| record = Some(failed.log(&label(&id))));
                assert!(log.contains(&id.to_string()), "{log}");
                assert!(
                    log.contains("leaving the request's contextvars.Context raised"),
                    "{log}"
                );
                assert_eq!(
                    render(record.expect("logged"), &SiteCounters::of_own_run(false))
                        .status()
                        .as_u16(),
                    500
                );
            })
        })
        .join()
        .expect("the test thread does not panic");
    }
}
