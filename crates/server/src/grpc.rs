//! Serves a gRPC unary call: reads and unframes the request message, runs the method's handler on
//! the worker a `def` route runs on, and sends the reply message back framed, with `grpc-status`
//! and, on failure, `grpc-message` in the HTTP/2 trailers.
//!
//! It does not read the messages: the app encodes and decodes them (with `protobuf`), and its
//! handler takes and returns `bytes`.

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::stream;
use http_body_util::StreamBody;
use hyper::body::{Frame, Incoming};
use hyper::header::{HeaderValue, InvalidHeaderValue, CONTENT_TYPE};
use hyper::{HeaderMap, Method, Response};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};

use crate::pipeline::{finish, HandlerRequest, Requester, ResponseSource};
use crate::request::{Backends, WorkerBackend};
use crate::SiteCounters;
use mipyre_http::io::body::{read_body, BodyReadError, ResponseBody};
use mipyre_http::io::request_head::{RequestHead, RequestLabel};
use mipyre_http::plan::grpc::{GrpcCode, APPLICATION_GRPC};
use mipyre_http::plan::limits::Limits;
use mipyre_http::plan::route::{RouteId, RuntimeChoice};
use mipyre_process::LaneUnansweredError;
use mipyre_runtime::exchange::{self, refuse, ErrorRecord, HandlerError, RequestBody, UnansweredError};
use mipyre_runtime::PoolCall;

/// Why a call ended without a reply: its code, and the real cause the caller reads as
/// `grpc-message` when there is one.
#[derive(Debug, PartialEq, Eq)]
struct GrpcStatus {
    code: GrpcCode,
    message: Option<String>,
}

/// Why the request message could not be read; its `Display` text is sent as `grpc-message`.
#[derive(Debug, thiserror::Error)]
enum ReadMessageError {
    #[error("request body exceeds max_body_size ({limit} bytes)")]
    BodyTooLarge { limit: usize },
    #[error("request body did not arrive within {0:?}")]
    BodyTimedOut(std::time::Duration),
    #[error("request body read failed: {0}")]
    ConnectionLost(#[source] std::sync::Arc<hyper::Error>),
    #[error("request is {len} bytes, shorter than the 5-byte gRPC frame header")]
    ShortFrame { len: usize },
    #[error("message is compressed (flag {flag}); no grpc-encoding is supported")]
    Compressed { flag: u8 },
    #[error("frame declares a {declared}-byte message but carries {actual} bytes")]
    TruncatedFrame { declared: usize, actual: usize },
    #[error(
        "frame declares a {declared}-byte message but carries {actual} bytes: a unary call \
         sends exactly one message, nothing after it"
    )]
    TrailingBytes { declared: usize, actual: usize },
}

impl ReadMessageError {
    fn code(&self) -> GrpcCode {
        match self {
            Self::BodyTooLarge { .. } => GrpcCode::ResourceExhausted,
            Self::BodyTimedOut(_) => GrpcCode::DeadlineExceeded,
            Self::ConnectionLost(_) => GrpcCode::Unavailable,
            Self::Compressed { .. } => GrpcCode::Unimplemented,
            Self::ShortFrame { .. } | Self::TruncatedFrame { .. } | Self::TrailingBytes { .. } => GrpcCode::Internal,
        }
    }

    fn status(&self) -> GrpcStatus {
        GrpcStatus {
            code: self.code(),
            message: Some(self.to_string()),
        }
    }
}

/// Serves the call to the gRPC method `method` and finishes its response.
pub(crate) async fn serve_grpc<B: Backends>(
    server: &B,
    prepared: HandlerRequest,
    method: RouteId,
) -> Response<ResponseBody> {
    let site = server.site();
    let HandlerRequest {
        head,
        body,
        timer,
        origin,
        ..
    } = prepared;
    let label = head.label();
    let client = head.client;
    let (reply, served) = call(server, head, body, method, &label).await;
    finish(
        grpc_reply(reply),
        site,
        &Method::POST,
        label.uri.path(),
        Requester::Given(&label.id, &client),
        timer,
        origin,
        served,
    )
}

/// Reads the message and runs the handler on this server's `def` worker backend; returns the
/// reply message or why there is none, and who answered.
async fn call<B: Backends>(
    server: &B,
    head: RequestHead,
    body: Incoming,
    method: RouteId,
    label: &RequestLabel,
) -> (Result<Bytes, GrpcStatus>, ResponseSource) {
    let site = server.site();
    let message = match read_message(body, &site.rules.settings.limits).await {
        Ok(message) => message,
        Err(e) => return (Err(e.status()), ResponseSource::Engine),
    };
    let request = exchange::Request::new(head, RequestBody::Buffered(message));
    let (answer, served) = match server.sync_worker() {
        Some(WorkerBackend::Inline(worker)) => (
            B::call_unary_inline(worker, site, method, request).map_err(|e| e.log(label)),
            ResponseSource::Inline,
        ),
        Some(WorkerBackend::Pool(pool)) => (
            crate::on_pool::call_on_pool(pool, site, PoolCall::Unary(method), request, label).await,
            ResponseSource::Pool,
        ),
        None => (
            Err(refuse(UnansweredError::NoWorkers(RuntimeChoice::Worker)).log(label)),
            ResponseSource::Engine,
        ),
    };
    (
        answer.map(|reply| reply.body).map_err(|e| status_of(e, &site.counters)),
        served,
    )
}

/// Reads the body under the limits every request body gets and unframes it: the size cap keeps a
/// multi-GB body behind an `application/grpc` content type from exhausting memory, and the time
/// budget keeps a stalled one from holding the connection.
async fn read_message(body: Incoming, limits: &Limits) -> Result<Bytes, ReadMessageError> {
    let collected = read_body(body, limits).await.map_err(|reject| match reject {
        BodyReadError::TooLarge => ReadMessageError::BodyTooLarge {
            limit: limits.max_body.as_usize(),
        },
        BodyReadError::TimedOut(budget) => ReadMessageError::BodyTimedOut(budget),
        BodyReadError::ConnectionLost(e) => ReadMessageError::ConnectionLost(e),
    })?;
    unframe(collected.into_bytes())
}

/// Every gRPC message is prefixed by `[compressed flag: u8][length: u32 BE]`.
const HEADER_LEN: usize = 5;

/// Strips the frame header from exactly one uncompressed message.
fn unframe(framed: Bytes) -> Result<Bytes, ReadMessageError> {
    if framed.len() < HEADER_LEN {
        return Err(ReadMessageError::ShortFrame { len: framed.len() });
    }
    if framed[0] != 0 {
        return Err(ReadMessageError::Compressed { flag: framed[0] });
    }
    let declared = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]) as usize;
    let actual = framed.len() - HEADER_LEN;
    match actual.cmp(&declared) {
        std::cmp::Ordering::Less => Err(ReadMessageError::TruncatedFrame { declared, actual }),
        std::cmp::Ordering::Greater => Err(ReadMessageError::TrailingBytes { declared, actual }),
        std::cmp::Ordering::Equal => Ok(framed.slice(HEADER_LEN..)),
    }
}

/// Returns the status a call that failed with `record` ends with, and counts a refusal in
/// `counters`; the error was logged where it happened. This match is the one place that maps a
/// handler's failure to a gRPC code, as `render` is for an HTTP status.
fn status_of(record: ErrorRecord, counters: &SiteCounters) -> GrpcStatus {
    let (error, _) = record.into_error_and_id();
    if let HandlerError::Refused(_) = error {
        counters.count_refused();
    }
    let code = code_of(&error);
    let message = match error {
        HandlerError::Grpc { details, .. } => details,
        other => Some(other.to_string()),
    };
    GrpcStatus { code, message }
}

fn code_of(error: &HandlerError) -> GrpcCode {
    match error {
        HandlerError::Grpc { code, .. } => *code,
        HandlerError::Raised { .. } => GrpcCode::Unknown,
        HandlerError::Http { answer, .. } => GrpcCode::of_http(answer.status),
        HandlerError::Refused(refusal) => match refusal {
            UnansweredError::Overloaded(_)
            | UnansweredError::WorkersGone(_)
            | UnansweredError::WorkersDied { .. }
            | UnansweredError::WorkerRetired(_)
            | UnansweredError::LoopClosed
            | UnansweredError::Lane(
                LaneUnansweredError::Full
                | LaneUnansweredError::ShuttingDown
                | LaneUnansweredError::WorkersDied { .. }
                | LaneUnansweredError::NoUsableLane { .. }
                | LaneUnansweredError::LaneCrashLooping { .. },
            ) => GrpcCode::Unavailable,
            UnansweredError::TimedOut(_)
            | UnansweredError::Overran { .. }
            | UnansweredError::Interrupted { .. }
            | UnansweredError::Lane(LaneUnansweredError::LaneTimedOut { .. }) => GrpcCode::DeadlineExceeded,
            UnansweredError::WorkerLost(_)
            | UnansweredError::NoWorkers(_)
            | UnansweredError::NotOnWorker(_)
            | UnansweredError::WebSocketNotOnWorker(_)
            | UnansweredError::Lane(
                LaneUnansweredError::Dropped
                | LaneUnansweredError::WorkerDied { .. }
                | LaneUnansweredError::WorkerDiedBeforeTaking { .. }
                | LaneUnansweredError::PeerCorrupted { .. }
                | LaneUnansweredError::LaneLost { .. },
            ) => GrpcCode::Internal,
        },
        HandlerError::BodyRejected(BodyReadError::TooLarge) => GrpcCode::ResourceExhausted,
        HandlerError::BodyRejected(BodyReadError::TimedOut(_)) => GrpcCode::DeadlineExceeded,
        HandlerError::BodyRejected(BodyReadError::ConnectionLost(_)) => GrpcCode::Unavailable,
        HandlerError::ContextNotExited { .. }
        | HandlerError::ContextLost { .. }
        | HandlerError::Panicked { .. }
        | HandlerError::Response(_)
        | HandlerError::ThreadNotSpawned(_)
        | HandlerError::ForeignEventLoop { .. }
        | HandlerError::LaneWorkerFailed(_)
        | HandlerError::LaneAnswerMalformed(_) => GrpcCode::Internal,
    }
}

fn frame(message: &[u8]) -> Bytes {
    let mut framed = BytesMut::with_capacity(HEADER_LEN + message.len());
    framed.put_u8(0);
    framed.put_u32(message.len() as u32);
    framed.extend_from_slice(message);
    framed.freeze()
}

/// `grpc-message` is percent-encoded UTF-8: everything outside printable ASCII, plus
/// `%` itself (gRPC over HTTP/2 spec, "Responses").
const GRPC_MESSAGE_ENCODE: &AsciiSet = &CONTROLS.add(b'%');

fn grpc_message_value(message: &str) -> Result<HeaderValue, InvalidHeaderValue> {
    HeaderValue::try_from(utf8_percent_encode(message, GRPC_MESSAGE_ENCODE).to_string())
}

/// Builds the response: the framed reply and `grpc-status: 0`, or no message and the failure's
/// code and message.
fn grpc_reply(reply: Result<Bytes, GrpcStatus>) -> Response<ResponseBody> {
    let (data, status) = match reply {
        Ok(message) => (
            Some(frame(&message)),
            GrpcStatus {
                code: GrpcCode::Ok,
                message: None,
            },
        ),
        Err(status) => (None, status),
    };
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", status.code.header_value());
    // Percent-encoding leaves only printable ASCII, so the Err arm is unreachable in
    // practice; if it ever fires, the real message still reaches the log.
    match status.message.as_deref().map(|m| (m, grpc_message_value(m))) {
        Some((_, Ok(value))) => {
            trailers.insert("grpc-message", value);
        }
        Some((message, Err(e))) => {
            tracing::error!(target: "mipyre::server", error = %e, message,
                "grpc-message not header-safe after percent-encoding");
        }
        None => {}
    }

    let data_frame = data.map(Frame::data);
    let trailer_frame: Frame<Bytes> = Frame::trailers(trailers);

    let body = StreamBody::new(stream::iter(
        data_frame
            .into_iter()
            .chain(std::iter::once(trailer_frame))
            .map(Ok::<_, std::convert::Infallible>),
    ));
    let mut response = Response::new(ResponseBody::boxed(body));
    response.headers_mut().insert(CONTENT_TYPE, APPLICATION_GRPC);
    response
}

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use hyper::Request;
    use mipyre_runtime::exchange::error::{RequestStage, WorkerGroup};
    use mipyre_runtime::exchange::Capacity;

    async fn trailers(resp: Response<ResponseBody>) -> HeaderMap {
        resp.into_body().collect().await.unwrap().trailers().cloned().unwrap()
    }

    /// Returns a real `hyper::Error`, which cannot be made by hand, from a connection that sends
    /// bytes that are not HTTP.
    fn a_hyper_error() -> hyper::Error {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                use tokio::io::AsyncWriteExt;
                let (mut client, server) = tokio::io::duplex(4096);
                client.write_all(b"\x00\x01 not http\r\n\r\n").await.unwrap();
                let svc = hyper::service::service_fn(|_req| async {
                    Ok::<_, std::convert::Infallible>(hyper::Response::new(http_body_util::Empty::<Bytes>::new()))
                });
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(server), svc)
                    .await
                    .expect_err("bytes that are not HTTP fail the connection")
            })
    }

    #[test]
    fn body_read_error_carries_the_real_cause() {
        let cause = std::sync::Arc::new(a_hyper_error());
        let e = ReadMessageError::ConnectionLost(cause.clone());
        assert_eq!(e.code(), GrpcCode::Unavailable);
        assert!(e.to_string().contains(&cause.to_string()), "{e}");
    }

    #[test]
    fn frame_errors_say_what_was_wrong() {
        let e = unframe(Bytes::from_static(&[0, 0, 0, 0, 9, 1])).unwrap_err();
        assert_eq!(e.to_string(), "frame declares a 9-byte message but carries 1 bytes");
        assert_eq!(e.code(), GrpcCode::Internal);
        let e = unframe(Bytes::from_static(&[1, 0, 0, 0, 0])).unwrap_err();
        assert_eq!(e.code(), GrpcCode::Unimplemented);
    }

    #[test]
    fn a_frame_header_is_five_bytes() {
        assert!(matches!(
            unframe(Bytes::from_static(&[0, 0, 0, 0])),
            Err(ReadMessageError::ShortFrame { len: 4 })
        ));
        assert_eq!(unframe(Bytes::from_static(&[0, 0, 0, 0, 0])).unwrap(), Bytes::new());
    }

    #[test]
    fn bytes_after_the_message_are_an_error() {
        let e = unframe(Bytes::from_static(&[0, 0, 0, 0, 2, 8, 1, 0xff])).unwrap_err();
        assert!(matches!(e, ReadMessageError::TrailingBytes { declared: 2, actual: 3 }));
        assert_eq!(e.code(), GrpcCode::Internal);
        assert_eq!(
            unframe(Bytes::from_static(&[0, 0, 0, 0, 2, 8, 1])).unwrap(),
            Bytes::from_static(&[8, 1])
        );
    }

    #[test]
    fn a_reply_is_framed_back() {
        assert_eq!(&frame(&[8, 5])[..], &[0, 0, 0, 0, 2, 8, 5]);
        assert_eq!(unframe(frame(b"reply")).unwrap(), Bytes::from_static(b"reply"));
    }

    /// Checks that a body that stops arriving is given up after the request budget as
    /// `DEADLINE_EXCEEDED`; the paused clock advances once the connection idles.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_body_is_deadline_exceeded() {
        use tokio::io::AsyncWriteExt;

        let (client_io, server_io) = tokio::io::duplex(1024);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = std::sync::Mutex::new(Some(tx));
        let service = hyper::service::service_fn(move |req: Request<Incoming>| {
            let tx = tx.lock().unwrap().take();
            async move {
                let started = tokio::time::Instant::now();
                let limits = Limits {
                    max_body: mipyre_http::plan::limits::ByteLimit(1024),
                    ..Limits::DEFAULT
                };
                let read = read_message(req.into_body(), &limits).await;
                if let Some(tx) = tx {
                    tx.send((read, started.elapsed())).unwrap();
                }
                Ok::<_, std::convert::Infallible>(Response::new(http_body_util::Empty::<Bytes>::new()))
            }
        });
        tokio::spawn(
            hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(server_io), service),
        );

        // The client promises an 8-byte body, sends 3 bytes, and keeps the writer open.
        let (_client_read, mut client_write) = tokio::io::split(client_io);
        client_write
            .write_all(b"POST /x HTTP/1.1\r\nhost: x\r\ncontent-length: 8\r\n\r\n\x00\x00\x00")
            .await
            .unwrap();
        let grace = Limits::DEFAULT.body_grace.get();
        let (read, waited) = tokio::time::timeout(2 * grace, rx)
            .await
            .expect("the body read gave up by its deadline")
            .unwrap();

        let err = read.unwrap_err();
        assert!(
            matches!(err, ReadMessageError::BodyTimedOut(b) if b == grace),
            "{err:?}"
        );
        assert_eq!(err.code(), GrpcCode::DeadlineExceeded);
        assert!(waited >= grace, "{waited:?}");
    }

    #[tokio::test]
    async fn multiline_non_ascii_message_is_percent_encoded_not_dropped() {
        let resp = grpc_reply(Err(GrpcStatus {
            code: GrpcCode::Internal,
            message: Some("line one\nline two: é 100%".into()),
        }));
        assert_eq!(resp.headers()[CONTENT_TYPE], "application/grpc");
        let t = trailers(resp).await;
        assert_eq!(t.get("grpc-status").unwrap(), "13");
        assert_eq!(t.get("grpc-message").unwrap(), "line one%0Aline two: %C3%A9 100%25");
    }

    #[tokio::test]
    async fn a_reply_has_its_message_and_status_0_and_no_grpc_message() {
        let collected = grpc_reply(Ok(Bytes::from_static(&[8, 5])))
            .into_body()
            .collect()
            .await
            .unwrap();
        let t = collected.trailers().cloned().unwrap();
        assert_eq!(t.get("grpc-status").unwrap(), "0");
        assert!(t.get("grpc-message").is_none());
        assert_eq!(&collected.to_bytes()[..], &[0, 0, 0, 0, 2, 8, 5]);
    }

    #[tokio::test]
    async fn a_status_with_no_message_has_no_grpc_message() {
        let t = trailers(grpc_reply(Err(GrpcStatus {
            code: GrpcCode::NotFound,
            message: None,
        })))
        .await;
        assert_eq!(t.get("grpc-status").unwrap(), "5");
        assert!(t.get("grpc-message").is_none());
    }

    /// Returns what a handler that raised `KeyError(message)` failed with.
    fn raised(message: &'static str) -> HandlerError {
        pyo3::Python::initialize();
        std::thread::spawn(move || {
            mipyre_subinterp::attach::with_main(|py| mipyre_runtime::testkit::raised(py, message))
        })
        .join()
        .unwrap()
    }

    #[test]
    fn a_handler_failure_takes_its_family_code() {
        assert_eq!(code_of(&raised("bad sum")), GrpcCode::Unknown);
        assert_eq!(
            code_of(&HandlerError::Grpc {
                stage: RequestStage::Handler,
                code: GrpcCode::InvalidArgument,
                details: None,
            }),
            GrpcCode::InvalidArgument
        );
        let refused = |r| code_of(&refuse(r));
        assert_eq!(
            refused(UnansweredError::Overloaded(Capacity::WorkQueue(WorkerGroup::Subinterp))),
            GrpcCode::Unavailable
        );
        assert_eq!(
            refused(UnansweredError::TimedOut(std::time::Duration::from_secs(1))),
            GrpcCode::DeadlineExceeded
        );
        assert_eq!(refused(UnansweredError::NotOnWorker(0)), GrpcCode::Internal);
        assert_eq!(
            code_of(&HandlerError::Panicked { payload: "boom".into() }),
            GrpcCode::Internal
        );
    }

    #[test]
    fn a_failure_tells_the_caller_its_real_cause() {
        let counters = SiteCounters::of_own_run(false);
        let label = mipyre_runtime::testkit::label(&mipyre_http::plan::request_id::RequestId::mint());
        let failure = raised("bad sum");
        let record = mipyre_runtime::testkit::captured_log_value(|| failure.log(&label));
        let status = status_of(record, &counters);
        assert_eq!(status.code, GrpcCode::Unknown);
        assert!(
            status.message.as_deref().unwrap().contains("KeyError: 'bad sum'"),
            "{status:?}"
        );

        let chosen = HandlerError::Grpc {
            stage: RequestStage::Handler,
            code: GrpcCode::NotFound,
            details: Some("no such sum".into()),
        };
        let record = mipyre_runtime::testkit::captured_log_value(|| chosen.log(&label));
        assert_eq!(
            status_of(record, &counters),
            GrpcStatus {
                code: GrpcCode::NotFound,
                message: Some("no such sum".into())
            }
        );
    }
}
