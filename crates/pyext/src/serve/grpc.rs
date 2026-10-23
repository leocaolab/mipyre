//! Answers the one unary gRPC method that HttpArena's `unary-grpc` profiles call.
//!
//! It is opt-in (`app.enable_grpc_benchmark()`), and every other request, gRPC or not, is routed
//! like any other. It is hand-rolled rather than built on `tonic`/`prost` because the protobuf
//! surface is one pair of messages, a few lines of varint I/O that cost less binary size and
//! compile time than the tonic stack:
//!
//! ```proto
//! message SumRequest  { int32 a = 1; int32 b = 2; }
//! message SumReply    { int32 result = 1; }
//! rpc   GetSum (SumRequest) returns (SumReply);
//! ```
//!
//! The status travels in HTTP/2 trailers: `grpc-status` and, on failure, a percent-encoded
//! `grpc-message` with the real cause.

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::stream;
use http_body_util::StreamBody;
use hyper::body::{Frame, Incoming};
use hyper::header::{HeaderValue, InvalidHeaderValue};
use hyper::{HeaderMap, Request, Response};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};

use mipyre_http::io::body::{read_body, BodyReadError, ResponseBody};
use mipyre_http::plan::limits::Limits;

/// The canonical gRPC status codes this server emits
/// (<https://grpc.github.io/grpc/core/md_doc_statuscodes.html>).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GrpcStatus {
    Ok,
    DeadlineExceeded,
    ResourceExhausted,
    Unimplemented,
    Internal,
    Unavailable,
}

impl GrpcStatus {
    fn header_value(self) -> HeaderValue {
        HeaderValue::from_static(match self {
            Self::Ok => "0",
            Self::DeadlineExceeded => "4",
            Self::ResourceExhausted => "8",
            Self::Unimplemented => "12",
            Self::Internal => "13",
            Self::Unavailable => "14",
        })
    }
}

/// Its `Display` text is sent to the client as `grpc-message`.
#[derive(Debug, thiserror::Error)]
enum GrpcError {
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
    #[error("malformed SumRequest: {0}")]
    MalformedSumRequest(#[from] DecodeError),
}

impl GrpcError {
    fn status(&self) -> GrpcStatus {
        match self {
            Self::BodyTooLarge { .. } => GrpcStatus::ResourceExhausted,
            Self::BodyTimedOut(_) => GrpcStatus::DeadlineExceeded,
            Self::ConnectionLost(_) => GrpcStatus::Unavailable,
            Self::Compressed { .. } => GrpcStatus::Unimplemented,
            Self::ShortFrame { .. }
            | Self::TruncatedFrame { .. }
            | Self::TrailingBytes { .. }
            | Self::MalformedSumRequest(_) => GrpcStatus::Internal,
        }
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum DecodeError {
    #[error("truncated or over-long varint")]
    MalformedVarint,
    #[error("field {field} runs past the end of the message")]
    Truncated { field: u64 },
    #[error("field {field} uses wire type {wire_type}, which proto3 does not allow")]
    UnsupportedWireType { field: u64, wire_type: u8 },
}

/// Answers the benchmark method, reading the body under the app's `max_body_size` and request
/// budget.
pub(crate) async fn handle_get_sum(
    req: Request<Incoming>,
    limits: &Limits,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let reply = read_message(req.into_body(), limits)
        .await
        .and_then(|message| get_sum(&message));
    Ok(match reply {
        Ok(reply) => grpc_reply(Some(frame(&reply)), GrpcStatus::Ok, None),
        Err(e) => grpc_reply(None, e.status(), Some(&e.to_string())),
    })
}

/// Reads the body under the limits every request body gets and unframes it: the size cap keeps a
/// multi-GB body behind an `application/grpc` content-type from exhausting memory, and the time
/// budget keeps a stalled one from holding the connection.
async fn read_message(body: Incoming, limits: &Limits) -> Result<Bytes, GrpcError> {
    let collected = read_body(body, limits).await.map_err(|reject| match reject {
        BodyReadError::TooLarge => GrpcError::BodyTooLarge {
            limit: limits.max_body.as_usize(),
        },
        BodyReadError::TimedOut(budget) => GrpcError::BodyTimedOut(budget),
        BodyReadError::ConnectionLost(e) => GrpcError::ConnectionLost(e),
    })?;
    unframe(collected)
}

/// Every gRPC message is prefixed by `[compressed flag: u8][length: u32 BE]`.
const HEADER_LEN: usize = 5;

/// Strips the frame header from exactly one uncompressed message.
fn unframe(framed: Bytes) -> Result<Bytes, GrpcError> {
    if framed.len() < HEADER_LEN {
        return Err(GrpcError::ShortFrame { len: framed.len() });
    }
    if framed[0] != 0 {
        return Err(GrpcError::Compressed { flag: framed[0] });
    }
    let declared = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]) as usize;
    let actual = framed.len() - HEADER_LEN;
    match actual.cmp(&declared) {
        std::cmp::Ordering::Less => Err(GrpcError::TruncatedFrame { declared, actual }),
        std::cmp::Ordering::Greater => Err(GrpcError::TrailingBytes { declared, actual }),
        std::cmp::Ordering::Equal => Ok(framed.slice(HEADER_LEN..)),
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SumRequest {
    a: i32,
    b: i32,
}

fn get_sum(message: &[u8]) -> Result<Bytes, GrpcError> {
    let SumRequest { a, b } = decode_sum_request(message)?;
    // `SumReply { int32 result = 1; }` is tag 0x08 (field 1, varint) and a varint. A negative
    // int32 is a 10-byte sign-extended varint, as tonic and grpc-go emit it.
    let mut reply = BytesMut::with_capacity(16);
    reply.put_u8(0x08);
    write_varint_i32(&mut reply, a.wrapping_add(b));
    Ok(reply.freeze())
}

/// These are the proto3 wire types (<https://protobuf.dev/programming-guides/encoding/#structure>).
mod wire {
    pub(crate) const VARINT: u8 = 0;
    pub(crate) const I64: u8 = 1;
    pub(crate) const LEN: u8 = 2;
    pub(crate) const I32: u8 = 5;
}

/// Decodes `SumRequest`, skipping unknown fields of any proto3 wire type as the spec requires; the
/// deprecated group types (3, 4) and invalid ones are errors.
fn decode_sum_request(mut cursor: &[u8]) -> Result<SumRequest, DecodeError> {
    let mut request = SumRequest::default();
    while !cursor.is_empty() {
        let (tag, rest) = read_varint(cursor).ok_or(DecodeError::MalformedVarint)?;
        let (field, wire_type) = (tag >> 3, (tag & 0x07) as u8);
        cursor = match (field, wire_type) {
            (1 | 2, wire::VARINT) => {
                let (value, rest) = read_varint(rest).ok_or(DecodeError::MalformedVarint)?;
                // int32 on the wire: the low 32 bits of a sign-extended varint.
                let value = value as i32;
                if field == 1 {
                    request.a = value;
                } else {
                    request.b = value;
                }
                rest
            }
            _ => skip_field(field, wire_type, rest)?,
        };
    }
    Ok(request)
}

fn skip_field(field: u64, wire_type: u8, input: &[u8]) -> Result<&[u8], DecodeError> {
    let len = match wire_type {
        wire::VARINT => {
            return read_varint(input)
                .map(|(_, rest)| rest)
                .ok_or(DecodeError::MalformedVarint)
        }
        wire::I64 => 8,
        wire::I32 => 4,
        wire::LEN => {
            let (len, rest) = read_varint(input).ok_or(DecodeError::MalformedVarint)?;
            let len = usize::try_from(len).map_err(|_| DecodeError::Truncated { field })?;
            return rest.get(len..).ok_or(DecodeError::Truncated { field });
        }
        _ => return Err(DecodeError::UnsupportedWireType { field, wire_type }),
    };
    input.get(len..).ok_or(DecodeError::Truncated { field })
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

fn grpc_reply(data: Option<Bytes>, status: GrpcStatus, message: Option<&str>) -> Response<ResponseBody> {
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", status.header_value());
    // Percent-encoding leaves only printable ASCII, so the Err arm is unreachable in
    // practice; if it ever fires, the real message still reaches the log.
    match message.map(|m| (m, grpc_message_value(m))) {
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
    let boxed = ResponseBody::boxed(body);

    let mut response = Response::new(boxed);
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static("application/grpc"));
    response
}

fn read_varint(input: &[u8]) -> Option<(u64, &[u8])> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in input.iter().enumerate() {
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((result, &input[i + 1..]));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

fn write_varint_i32(buf: &mut BytesMut, value: i32) {
    // A negative int32 sign-extends to u64 before encoding.
    let mut v = value as i64 as u64;
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            buf.put_u8(byte);
            return;
        }
        buf.put_u8(byte | 0x80);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[test]
    fn varint_roundtrip_positive() {
        for v in [0_i32, 1, 127, 128, 12345, 1 << 30] {
            let mut buf = BytesMut::new();
            write_varint_i32(&mut buf, v);
            let (decoded, rest) = read_varint(&buf).unwrap();
            assert!(rest.is_empty());
            assert_eq!(decoded as i32, v, "value {v}");
        }
    }

    #[test]
    fn varint_negative_is_10_bytes() {
        let mut buf = BytesMut::new();
        write_varint_i32(&mut buf, -1);
        // -1 sign-extends to all ones, which takes 10 bytes.
        assert_eq!(buf.len(), 10);
        let (decoded, _) = read_varint(&buf).unwrap();
        assert_eq!(decoded as i32, -1);
    }

    #[test]
    fn read_varint_stops_at_shift_overflow() {
        // Eleven bytes with every continuation bit set must fail rather than overflow the shift.
        let bad = vec![0xFF; 11];
        assert!(read_varint(&bad).is_none());
    }

    async fn trailers(resp: Response<ResponseBody>) -> HeaderMap {
        resp.into_body().collect().await.unwrap().trailers().cloned().unwrap()
    }

    #[test]
    fn unknown_fields_of_every_proto3_wire_type_are_skipped() {
        let message = [
            0x08, 0x02, // a = 2
            0x1a, 0x02, b'h', b'i', // field 3, LEN "hi"
            0x21, 1, 2, 3, 4, 5, 6, 7, 8, // field 4, I64
            0x2d, 1, 2, 3, 4, // field 5, I32
            0x30, 0x7f, // field 6, VARINT
            0x80, 0x01, 0x00, // field 16 (two-byte tag), VARINT 0
            0x10, 0x03, // b = 3
        ];
        assert_eq!(decode_sum_request(&message), Ok(SumRequest { a: 2, b: 3 }));
    }

    #[test]
    fn group_and_invalid_wire_types_are_errors() {
        assert_eq!(
            decode_sum_request(&[0x1b]),
            Err(DecodeError::UnsupportedWireType { field: 3, wire_type: 3 })
        );
        assert_eq!(
            decode_sum_request(&[0x1e]),
            Err(DecodeError::UnsupportedWireType { field: 3, wire_type: 6 })
        );
    }

    #[test]
    fn truncated_unknown_field_is_an_error() {
        assert_eq!(
            decode_sum_request(&[0x1a, 0x05, b'h']),
            Err(DecodeError::Truncated { field: 3 })
        );
        assert_eq!(
            decode_sum_request(&[0x21, 1, 2]),
            Err(DecodeError::Truncated { field: 4 })
        );
    }

    #[test]
    fn negative_int32_sums_wrap() {
        let mut message = BytesMut::new();
        message.put_u8(0x08);
        write_varint_i32(&mut message, i32::MAX);
        message.put_u8(0x10);
        write_varint_i32(&mut message, 1);
        let reply = get_sum(&message).unwrap();
        let (value, _) = read_varint(&reply[1..]).unwrap();
        assert_eq!(value as i32, i32::MIN);
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
                let (mut client, server) = tokio::io::duplex(64);
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
        let e = GrpcError::ConnectionLost(cause.clone());
        assert_eq!(e.status(), GrpcStatus::Unavailable);
        assert!(e.to_string().contains(&cause.to_string()), "{e}");
    }

    #[test]
    fn frame_errors_say_what_was_wrong() {
        let e = unframe(Bytes::from_static(&[0, 0, 0, 0, 9, 1])).unwrap_err();
        assert_eq!(e.to_string(), "frame declares a 9-byte message but carries 1 bytes");
        assert_eq!(e.status(), GrpcStatus::Internal);
        let e = unframe(Bytes::from_static(&[1, 0, 0, 0, 0])).unwrap_err();
        assert_eq!(e.status(), GrpcStatus::Unimplemented);
    }

    #[test]
    fn bytes_after_the_message_are_an_error() {
        let e = unframe(Bytes::from_static(&[0, 0, 0, 0, 2, 8, 1, 0xff])).unwrap_err();
        assert!(matches!(e, GrpcError::TrailingBytes { declared: 2, actual: 3 }));
        assert_eq!(e.status(), GrpcStatus::Internal);
        assert_eq!(
            unframe(Bytes::from_static(&[0, 0, 0, 0, 2, 8, 1])).unwrap(),
            Bytes::from_static(&[8, 1])
        );
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
        let budget = Limits::DEFAULT.request_budget.get();
        let (read, waited) = tokio::time::timeout(2 * budget, rx)
            .await
            .expect("the body read gave up by its deadline")
            .unwrap();

        let err = read.unwrap_err();
        assert!(matches!(err, GrpcError::BodyTimedOut(b) if b == budget), "{err:?}");
        assert_eq!(err.status(), GrpcStatus::DeadlineExceeded);
        assert!(waited >= budget, "{waited:?}");
    }

    #[tokio::test]
    async fn multiline_non_ascii_message_is_percent_encoded_not_dropped() {
        let resp = grpc_reply(None, GrpcStatus::Internal, Some("line one\nline two: é 100%"));
        let t = trailers(resp).await;
        assert_eq!(t.get("grpc-status").unwrap(), "13");
        assert_eq!(t.get("grpc-message").unwrap(), "line one%0Aline two: %C3%A9 100%25");
    }

    #[tokio::test]
    async fn ok_reply_has_no_message() {
        let t = trailers(grpc_reply(None, GrpcStatus::Ok, None)).await;
        assert_eq!(t.get("grpc-status").unwrap(), "0");
        assert!(t.get("grpc-message").is_none());
    }
}
