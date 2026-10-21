//! Encodes the requests and answers that cross a lane as little-endian, length-prefixed fields,
//! written straight into a slot and read straight out of one.

use bytes::Bytes;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};

use crate::exchange::{LaneDecodeError, RequestHead, TaggedField, WorkerFailure};
use mipyre_http::plan::response::{HandlerResponse, HeaderLines};
use mipyre_http::plan::route::RouteKey;
use mipyre_transport::driver::Peer;
use mipyre_http::io::request_id::{ClientRequestId, RequestId, ServerRequestId};

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("{needed} bytes do not fit a process-lane slot of {room} bytes")]
pub(crate) struct TooLarge {
    pub(crate) needed: usize,
    pub(crate) room: usize,
}

/// Writes a message into a slot; `at` grows only through [`MessageWriter::put`], which refuses to
/// pass the room or `u32::MAX`, so the recorded length always lies within the slot.
pub(crate) struct MessageWriter<'a> {
    buf: &'a mut [u8],
    at: u32,
}

impl<'a> MessageWriter<'a> {
    pub(crate) fn new(buf: &'a mut [u8]) -> Self {
        MessageWriter { buf, at: 0 }
    }

    pub(crate) fn room(&self) -> usize {
        self.buf.len()
    }

    pub(crate) fn len(&self) -> u32 {
        self.at
    }

    #[cfg(test)]
    pub(crate) fn written(&self) -> &[u8] {
        &self.buf[..self.at as usize]
    }

    pub(crate) fn restart(&mut self) {
        self.at = 0;
    }

    pub(crate) fn put(&mut self, bytes: &[u8]) -> Result<(), TooLarge> {
        let start = self.at as usize;
        let room = self.buf.len();
        let too_large = || TooLarge {
            needed: start.saturating_add(bytes.len()),
            room,
        };
        let end = start.checked_add(bytes.len()).ok_or_else(too_large)?;
        let (Some(dst), Ok(at)) = (self.buf.get_mut(start..end), u32::try_from(end)) else {
            return Err(too_large());
        };
        dst.copy_from_slice(bytes);
        self.at = at;
        Ok(())
    }
    fn u8(&mut self, v: u8) -> Result<(), TooLarge> {
        self.put(&[v])
    }
    fn u16(&mut self, v: u16) -> Result<(), TooLarge> {
        self.put(&v.to_le_bytes())
    }
    fn u32(&mut self, v: u32) -> Result<(), TooLarge> {
        self.put(&v.to_le_bytes())
    }
    fn field(&mut self, bytes: &[u8]) -> Result<(), TooLarge> {
        let len = u32::try_from(bytes.len()).map_err(|_| TooLarge {
            needed: bytes.len(),
            room: self.buf.len(),
        })?;
        self.u32(len)?;
        self.put(bytes)
    }
}

struct MessageReader<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> MessageReader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        MessageReader { buf, at: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], LaneDecodeError> {
        let end = self.at + n;
        let out = self
            .buf
            .get(self.at..end)
            .ok_or(LaneDecodeError::Truncated {
                wanted: n,
                at: self.at,
                len: self.buf.len(),
            })?;
        self.at = end;
        Ok(out)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], LaneDecodeError> {
        let mut out = [0; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }
    fn u8(&mut self) -> Result<u8, LaneDecodeError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, LaneDecodeError> {
        Ok(u16::from_le_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32, LaneDecodeError> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn field(&mut self) -> Result<&'a [u8], LaneDecodeError> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    fn text(&mut self) -> Result<String, LaneDecodeError> {
        std::str::from_utf8(self.field()?)
            .map(str::to_owned)
            .map_err(LaneDecodeError::NotUtf8)
    }
}

fn put_headers(w: &mut MessageWriter<'_>, headers: &HeaderMap) -> Result<(), TooLarge> {
    w.u32(headers.len() as u32)?;
    for (name, value) in headers {
        w.field(name.as_str().as_bytes())?;
        w.field(value.as_bytes())?;
    }
    Ok(())
}

fn read_headers(r: &mut MessageReader<'_>) -> Result<HeaderMap, LaneDecodeError> {
    let n = r.u32()? as usize;
    let mut map = HeaderMap::with_capacity(n);
    for _ in 0..n {
        let name = HeaderName::from_bytes(r.field()?).map_err(LaneDecodeError::BadHeaderName)?;
        let value = HeaderValue::from_bytes(r.field()?).map_err(LaneDecodeError::BadHeaderValue)?;
        map.append(name, value);
    }
    Ok(map)
}

/// Reads a response's header lines; a `content-type` among them makes the message malformed,
/// because it travels in its own field.
fn read_lines(r: &mut MessageReader<'_>) -> Result<HeaderLines, LaneDecodeError> {
    let n = r.u32()? as usize;
    let mut lines = HeaderLines::with_capacity(n);
    for _ in 0..n {
        let name = HeaderName::from_bytes(r.field()?).map_err(LaneDecodeError::BadHeaderName)?;
        let value = HeaderValue::from_bytes(r.field()?).map_err(LaneDecodeError::BadHeaderValue)?;
        if let Some(value) = lines.append(name, value) {
            return Err(LaneDecodeError::TypeAmongLines(value));
        }
    }
    Ok(lines)
}

const SERVER_ID: u8 = 0;
const CLIENT_ID: u8 = 1;

const PEER_IPV4: u8 = 4;
const PEER_IPV6: u8 = 6;
const PEER_IN_MEMORY: u8 = 0;

/// A request as it crosses a lane, naming its route by key because the worker imported the app
/// itself and its route table's indices are its own.
pub(crate) struct LaneRequest {
    pub(crate) route_key: RouteKey,
    pub(crate) head: RequestHead,
    pub(crate) body: Bytes,
}

/// A request as the worker reads it; the body borrows the slot, which the server leaves alone until
/// it has read the answer, so the body is copied only once, into the handler's Python `bytes`.
pub(crate) struct SlotRequest<'a> {
    pub(crate) route_key: RouteKey,
    pub(crate) head: RequestHead,
    pub(crate) body: &'a [u8],
}

pub(crate) fn encode_request(w: &mut MessageWriter<'_>, req: &LaneRequest) -> Result<(), TooLarge> {
    w.restart();
    let head = &req.head;
    w.field(req.route_key.method.as_str().as_bytes())?;
    w.field(req.route_key.path.as_bytes())?;
    w.field(head.parts.method.as_str().as_bytes())?;
    // An origin-form target is written as it came; only a target with a scheme or authority is
    // rendered.
    match (head.parts.uri.scheme(), head.parts.uri.authority(), head.parts.uri.path_and_query()) {
        (None, None, Some(target)) => w.field(target.as_str().as_bytes())?,
        _ => w.field(head.parts.uri.to_string().as_bytes())?,
    }
    put_headers(w, &head.parts.headers)?;
    w.u16(head.params.len() as u16)?;
    for (k, v) in &head.params {
        w.field(k.as_bytes())?;
        w.field(v.as_bytes())?;
    }
    match head.client {
        Peer::Ip(std::net::IpAddr::V4(ip)) => {
            w.u8(PEER_IPV4)?;
            w.put(&ip.octets())?;
        }
        Peer::Ip(std::net::IpAddr::V6(ip)) => {
            w.u8(PEER_IPV6)?;
            w.put(&ip.octets())?;
        }
        Peer::InMemory => w.u8(PEER_IN_MEMORY)?,
    }
    match &head.request_id {
        RequestId::Server(id) => {
            w.u8(SERVER_ID)?;
            w.put(&id.process.to_le_bytes())?;
            w.put(&id.number.to_le_bytes())?;
        }
        RequestId::Client(id) => {
            w.u8(CLIENT_ID)?;
            w.field(id.as_bytes())?;
        }
    }
    w.field(&req.body)
}

pub(crate) fn decode_request(buf: &[u8]) -> Result<SlotRequest<'_>, LaneDecodeError> {
    let mut r = MessageReader::new(buf);
    let route_key = RouteKey {
        method: hyper::Method::from_bytes(r.field()?).map_err(LaneDecodeError::BadMethod)?,
        path: r.text()?,
    };
    let method = hyper::Method::from_bytes(r.field()?).map_err(LaneDecodeError::BadMethod)?;
    let uri: hyper::Uri = std::str::from_utf8(r.field()?)
        .map_err(LaneDecodeError::NotUtf8)?
        .parse()
        .map_err(LaneDecodeError::BadUri)?;
    let headers = read_headers(&mut r)?;
    let n = r.u16()?;
    let mut params = Vec::with_capacity(n as usize);
    for _ in 0..n {
        params.push((r.text()?, r.text()?));
    }
    let client = match r.u8()? {
        PEER_IPV4 => Peer::Ip(std::net::IpAddr::from(r.array::<4>()?)),
        PEER_IPV6 => Peer::Ip(std::net::IpAddr::from(r.array::<16>()?)),
        PEER_IN_MEMORY => Peer::InMemory,
        tag => {
            return Err(LaneDecodeError::UnknownTag {
                field: TaggedField::PeerKind,
                tag,
            })
        }
    };
    let request_id = match r.u8()? {
        SERVER_ID => RequestId::Server(ServerRequestId {
            process: u64::from_le_bytes(r.array()?),
            number: u64::from_le_bytes(r.array()?),
        }),
        CLIENT_ID => {
            let text = r.text()?;
            RequestId::Client(
                ClientRequestId::parse(&text).ok_or(LaneDecodeError::BadClientRequestId(text))?,
            )
        }
        tag => {
            return Err(LaneDecodeError::UnknownTag {
                field: TaggedField::RequestIdKind,
                tag,
            })
        }
    };
    let body = r.field()?;
    let (mut parts, ()) = hyper::Request::new(()).into_parts();
    parts.method = method;
    parts.uri = uri;
    parts.headers = headers;
    Ok(SlotRequest {
        route_key,
        head: RequestHead {
            parts,
            params,
            client,
            request_id,
        },
        body,
    })
}

const OK: u8 = 0;
const FAILED: u8 = 1;

pub(crate) fn encode_response(
    w: &mut MessageWriter<'_>,
    data: &HandlerResponse,
) -> Result<(), TooLarge> {
    w.restart();
    w.u8(OK)?;
    w.u16(data.status.as_u16())?;
    w.field(data.content_type().as_bytes())?;
    put_headers(w, data.headers())?;
    w.field(&data.body)
}

/// Fits `text` into `room` bytes, cutting it at a character boundary with a note of how much was
/// left out.
pub(crate) fn cut_to_fit(text: &str, room: usize) -> std::borrow::Cow<'_, str> {
    if text.len() <= room {
        return std::borrow::Cow::Borrowed(text);
    }
    let mut end = room.saturating_sub(64);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    std::borrow::Cow::Owned(format!(
        "{}… [cut: {} bytes did not fit the slot]",
        &text[..end],
        text.len() - end
    ))
}

/// Writes a failure over whatever `w` held, cutting each text to (room - 16) / 2 bytes so the
/// message always fits a slot of at least `MIN_SLOT_BYTES`.
///
/// # Errors
///
/// Returns [`TooLarge`] for a slot smaller than that.
pub(crate) fn encode_failure(
    w: &mut MessageWriter<'_>,
    error: &str,
    traceback: Option<&str>,
) -> Result<(), TooLarge> {
    let room = w.room().saturating_sub(16) / 2;
    w.restart();
    w.u8(FAILED)?;
    w.field(cut_to_fit(error, room).as_bytes())?;
    match traceback {
        Some(traceback) => {
            w.u8(1)?;
            w.field(cut_to_fit(traceback, room).as_bytes())
        }
        None => w.u8(0),
    }
}

/// Writes the response or the failure; a response that does not fit is replaced by a failure
/// saying so.
pub(crate) fn encode_answer(
    w: &mut MessageWriter<'_>,
    result: &Result<HandlerResponse, WorkerFailure>,
) -> Result<(), TooLarge> {
    match result {
        Ok(data) => match encode_response(w, data) {
            Ok(()) => Ok(()),
            Err(e) => encode_failure(
                w,
                &format!("the response does not fit the process lane: {e}"),
                None,
            ),
        },
        Err(failure) => encode_failure(w, &failure.error, failure.traceback.as_deref()),
    }
}

pub(crate) fn decode_answer(
    buf: &[u8],
) -> Result<Result<HandlerResponse, WorkerFailure>, LaneDecodeError> {
    let mut r = MessageReader::new(buf);
    match r.u8()? {
        OK => {
            let status = hyper::StatusCode::from_u16(r.u16()?).map_err(LaneDecodeError::BadStatus)?;
            let content_type =
                HeaderValue::from_bytes(r.field()?).map_err(LaneDecodeError::BadHeaderValue)?;
            let headers = read_lines(&mut r)?;
            // The body is copied out so the slot goes back to the lane at once, rather than
            // being held for as long as a slow client takes to read.
            let body = Bytes::copy_from_slice(r.field()?);
            Ok(Ok(HandlerResponse::new(
                status,
                body,
                content_type,
                headers,
            )))
        }
        FAILED => {
            let error = r.text()?;
            let traceback = match r.u8()? {
                0 => None,
                1 => Some(r.text()?),
                tag => {
                    return Err(LaneDecodeError::UnknownTag {
                        field: TaggedField::TracebackFlag,
                        tag,
                    })
                }
            };
            Ok(Err(WorkerFailure { error, traceback }))
        }
        tag => Err(LaneDecodeError::UnknownTag {
            field: TaggedField::AnswerKind,
            tag,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lane::shm::MIN_SLOT_BYTES;
    use hyper::StatusCode;

    fn request(body: &'static [u8]) -> LaneRequest {
        let (parts, ()) = hyper::Request::builder()
            .method("POST")
            .uri("/x?y=1")
            .header("x-a", "b")
            .body(())
            .unwrap()
            .into_parts();
        LaneRequest {
            route_key: RouteKey {
                method: hyper::Method::POST,
                path: "/x".into(),
            },
            head: RequestHead {
                parts,
                params: Default::default(),
                client: Peer::Ip("10.0.0.1".parse().unwrap()),
                request_id: RequestId::Client(ClientRequestId::parse("abc").unwrap()),
            },
            body: Bytes::from_static(body),
        }
    }

    #[test]
    fn a_request_bigger_than_its_slot_is_refused_with_both_sizes() {
        let mut buf = vec![0u8; 256];
        let err =
            encode_request(&mut MessageWriter::new(&mut buf), &request(&[7u8; 1024])).unwrap_err();
        assert_eq!(err.room, 256);
        assert!(err.needed > 1024, "{err:?}");
    }

    #[test]
    fn a_request_round_trips_through_its_slot() {
        let mut buf = vec![0u8; MIN_SLOT_BYTES];
        let mut w = MessageWriter::new(&mut buf);
        encode_request(&mut w, &request(b"hello")).unwrap();
        let back = decode_request(w.written()).unwrap();
        assert_eq!(back.body, b"hello");
        assert_eq!(back.route_key.path, "/x");
    }

    #[test]
    fn the_peer_round_trips_through_its_slot() {
        for client in [
            Peer::Ip("10.0.0.1".parse().unwrap()),
            Peer::Ip("::1".parse().unwrap()),
            Peer::InMemory,
        ] {
            let mut sent = request(b"");
            sent.head.client = client;
            let mut buf = vec![0u8; MIN_SLOT_BYTES];
            let mut w = MessageWriter::new(&mut buf);
            encode_request(&mut w, &sent).unwrap();
            assert_eq!(decode_request(w.written()).unwrap().head.client, client);
        }
    }

    /// The text must not depend on the decoding process's seed.
    #[test]
    fn the_request_id_round_trips_through_its_slot() {
        for id in [
            RequestId::Server(ServerRequestId {
                process: 0x0123_4567_89ab_cdef,
                number: 42,
            }),
            RequestId::Client(ClientRequestId::parse("trace-7f/abc=").unwrap()),
        ] {
            let mut sent = request(b"");
            sent.head.request_id = id.clone();
            let mut buf = vec![0u8; MIN_SLOT_BYTES];
            let mut w = MessageWriter::new(&mut buf);
            encode_request(&mut w, &sent).unwrap();
            let back = decode_request(w.written()).unwrap().head.request_id;
            assert_eq!(back, id);
            assert_eq!(back.to_string(), id.to_string());
        }
    }

    #[test]
    fn a_truncated_request_is_malformed_at_every_cut() {
        let mut buf = vec![0u8; MIN_SLOT_BYTES];
        let mut w = MessageWriter::new(&mut buf);
        encode_request(&mut w, &request(b"hello")).unwrap();
        let n = w.len() as usize;
        for cut in 0..n {
            assert!(decode_request(&buf[..cut]).is_err(), "cut at {cut} decoded");
        }
    }

    #[test]
    fn an_answer_of_an_unknown_kind_or_empty_is_malformed() {
        assert!(decode_answer(&[9]).is_err());
        assert!(decode_answer(&[]).is_err());
    }

    #[test]
    fn a_malformed_answer_says_which_way_by_its_variant() {
        assert!(matches!(
            decode_answer(&[9]),
            Err(LaneDecodeError::UnknownTag {
                field: TaggedField::AnswerKind,
                tag: 9
            })
        ));
        assert!(matches!(
            decode_answer(&[]),
            Err(LaneDecodeError::Truncated {
                wanted: 1,
                at: 0,
                len: 0
            })
        ));
        assert!(matches!(
            decode_answer(&[FAILED, 1, 0, 0, 0, 0xff]),
            Err(LaneDecodeError::NotUtf8(_))
        ));
    }

    #[test]
    fn a_truncated_ok_answer_is_malformed_at_every_cut() {
        let mut buf = vec![0u8; MIN_SLOT_BYTES];
        let response = HandlerResponse::new(
            StatusCode::OK,
            Bytes::from_static(b"body"),
            HeaderValue::from_static("text/plain"),
            HeaderLines::new(),
        );
        let mut w = MessageWriter::new(&mut buf);
        encode_response(&mut w, &response).unwrap();
        let n = w.len() as usize;
        assert!(decode_answer(&buf[..n]).unwrap().is_ok());
        for cut in 0..n {
            assert!(decode_answer(&buf[..cut]).is_err(), "cut at {cut} decoded");
        }
    }

    #[test]
    fn a_failure_too_long_for_its_slot_is_cut_to_fit_and_says_so() {
        let mut buf = vec![0u8; MIN_SLOT_BYTES];
        let text = "e".repeat(10_000);
        let mut w = MessageWriter::new(&mut buf);
        encode_failure(&mut w, &text, Some(&text)).unwrap();
        let failure = decode_answer(w.written()).unwrap().unwrap_err();
        assert!(failure.error.contains("[cut:"), "{}", failure.error);
        assert!(failure.error.len() < text.len());
        let traceback = failure.traceback.expect("the traceback is kept");
        assert!(traceback.contains("[cut:"), "{traceback}");
    }

    #[test]
    fn a_failure_after_a_response_that_did_not_fit_replaces_it() {
        let mut buf = vec![0u8; MIN_SLOT_BYTES];
        let response = HandlerResponse::new(
            StatusCode::OK,
            Bytes::from(vec![7u8; 2 * MIN_SLOT_BYTES]),
            HeaderValue::from_static("text/plain"),
            HeaderLines::new(),
        );
        let mut w = MessageWriter::new(&mut buf);
        assert!(encode_response(&mut w, &response).is_err());
        encode_failure(&mut w, "too large", None).unwrap();
        let failure = decode_answer(w.written()).unwrap().unwrap_err();
        assert_eq!(failure.error, "too large");
    }

    #[test]
    fn the_writer_length_never_passes_its_room() {
        let mut buf = vec![0u8; 8];
        let mut w = MessageWriter::new(&mut buf);
        w.put(&[1; 6]).unwrap();
        let err = w.put(&[2; 3]).unwrap_err();
        assert_eq!((err.needed, err.room), (9, 8));
        assert_eq!(w.len(), 6);
        w.put(&[3; 2]).unwrap();
        assert_eq!(w.len(), 8);
    }

    #[test]
    fn a_failure_of_any_length_fits_a_minimal_slot_and_reads_back_cut() {
        let mut buf = vec![0u8; MIN_SLOT_BYTES];
        let long = "é".repeat(100_000);
        let mut w = MessageWriter::new(&mut buf);
        encode_failure(&mut w, &long, Some(&long)).unwrap();
        match decode_answer(w.written()).unwrap() {
            Err(failure) => {
                assert!(failure.error.contains("[cut:"));
                assert!(failure.traceback.as_deref().unwrap().contains("[cut:"));
            }
            Ok(_) => panic!("a failure decoded as a response"),
        }
    }

    #[test]
    fn a_failure_without_a_traceback_reads_back_none_and_an_empty_one_reads_back_empty() {
        let mut buf = vec![0u8; MIN_SLOT_BYTES];
        for traceback in [None, Some("")] {
            let mut w = MessageWriter::new(&mut buf);
            encode_failure(&mut w, "boom", traceback).unwrap();
            let failure = decode_answer(w.written()).unwrap().unwrap_err();
            assert_eq!(failure.error, "boom");
            assert_eq!(failure.traceback.as_deref(), traceback);
        }
        // A worker's failure takes the `encode_answer` path, so it is checked too.
        let mut w = MessageWriter::new(&mut buf);
        let failed = Err(WorkerFailure {
            error: "boom".into(),
            traceback: None,
        });
        encode_answer(&mut w, &failed).unwrap();
        assert_eq!(decode_answer(w.written()).unwrap().unwrap_err().traceback, None);
    }

    #[test]
    fn a_slot_too_small_for_a_failure_is_refused_not_panicked() {
        let mut buf = vec![0u8; 8];
        assert!(encode_failure(&mut MessageWriter::new(&mut buf), "boom", None).is_err());
    }
}
