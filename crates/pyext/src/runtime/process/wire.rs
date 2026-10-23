//! Encodes the requests and answers that cross a lane as little-endian, length-prefixed fields, written straight
//! into a channel slot's writer and read out of the bytes a slot held.

use bytes::Bytes;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};

use crate::exchange::{LaneDecodeError, RequestHead, TaggedField, WorkerFailure};
use mipyre_http::io::request_id::{ClientRequestId, RequestId, ServerRequestId};
use mipyre_http::plan::response::{HandlerResponse, HeaderLines};
use mipyre_http::plan::route::RouteKey;
use mipyre_shm::channel::{SlotRoomError, SlotWriter};
use mipyre_transport::driver::Peer;

/// Appends `v`.
fn put_u8(w: &mut SlotWriter<'_>, v: u8) -> Result<(), SlotRoomError> {
    w.put(&[v])
}

fn put_u16(w: &mut SlotWriter<'_>, v: u16) -> Result<(), SlotRoomError> {
    w.put(&v.to_le_bytes())
}

fn put_u32(w: &mut SlotWriter<'_>, v: u32) -> Result<(), SlotRoomError> {
    w.put(&v.to_le_bytes())
}

/// Appends `bytes` after their length.
fn put_field(w: &mut SlotWriter<'_>, bytes: &[u8]) -> Result<(), SlotRoomError> {
    let len = u32::try_from(bytes.len()).map_err(|_| SlotRoomError {
        room: w.room(),
        needed: bytes.len() as u64,
    })?;
    put_u32(w, len)?;
    w.put(bytes)
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
        let out = self.buf.get(self.at..end).ok_or(LaneDecodeError::Truncated {
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
    /// Reads a field and returns where its bytes lie in the message, for a caller that slices them out of it.
    fn field_range(&mut self) -> Result<std::ops::Range<usize>, LaneDecodeError> {
        let n = self.u32()? as usize;
        let start = self.at;
        self.take(n)?;
        Ok(start..start + n)
    }
    /// Takes a count of `count` entries that each take at least `least` bytes, when the rest of the message can hold
    /// them: what the count sizes is then never larger than the message the peer wrote.
    fn count(&self, count: usize, least: usize) -> Result<usize, LaneDecodeError> {
        let rest = self.buf.len() - self.at;
        if count > rest / least {
            return Err(LaneDecodeError::CountPastEnd {
                count,
                least,
                at: self.at,
                len: self.buf.len(),
            });
        }
        Ok(count)
    }
    fn text(&mut self) -> Result<String, LaneDecodeError> {
        std::str::from_utf8(self.field()?)
            .map(str::to_owned)
            .map_err(LaneDecodeError::NotUtf8)
    }
}

/// The fewest bytes a header line or a route parameter takes: the lengths of its two fields.
const PAIR_MIN_BYTES: usize = 8;

/// The most lines a header map takes room for at once: `HeaderMap::with_capacity(n)` rounds `n + n / 3` up to a
/// power of two and panics past 32,768 (`http`'s `MAX_SIZE`). With room for `n` lines, `n` appends never grow the
/// map, so none of them can pass that limit either. A test holds this to `http`'s own `try_with_capacity`.
const MAX_LINES: usize = 24_576;

/// Room for a count of `n` header lines, the count already held to the message's bytes.
fn lines_room(n: usize) -> Result<usize, LaneDecodeError> {
    if n > MAX_LINES {
        return Err(LaneDecodeError::TooManyLines {
            count: n,
            max: MAX_LINES,
        });
    }
    Ok(n)
}

fn put_headers(w: &mut SlotWriter<'_>, headers: &HeaderMap) -> Result<(), SlotRoomError> {
    put_u32(w, headers.len() as u32)?;
    for (name, value) in headers {
        put_field(w, name.as_str().as_bytes())?;
        put_field(w, value.as_bytes())?;
    }
    Ok(())
}

fn read_headers(r: &mut MessageReader<'_>) -> Result<HeaderMap, LaneDecodeError> {
    let n = r.u32()? as usize;
    let n = lines_room(r.count(n, PAIR_MIN_BYTES)?)?;
    let mut map = HeaderMap::with_capacity(n);
    for _ in 0..n {
        let name = HeaderName::from_bytes(r.field()?).map_err(LaneDecodeError::BadHeaderName)?;
        let value = HeaderValue::from_bytes(r.field()?).map_err(LaneDecodeError::BadHeaderValue)?;
        map.append(name, value);
    }
    Ok(map)
}

/// Reads a response's header lines; a `content-type` among them makes the message malformed,
/// because it travels in its own field. The worker writes the count, so it sizes nothing the rest of the message
/// cannot hold, nor more than a header map holds (b/926).
fn read_lines(r: &mut MessageReader<'_>) -> Result<HeaderLines, LaneDecodeError> {
    let n = r.u32()? as usize;
    let n = lines_room(r.count(n, PAIR_MIN_BYTES)?)?;
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

pub(crate) fn encode_request(w: &mut SlotWriter<'_>, req: &LaneRequest) -> Result<(), SlotRoomError> {
    w.restart();
    let head = &req.head;
    put_field(w, req.route_key.method.as_str().as_bytes())?;
    put_field(w, req.route_key.path.as_bytes())?;
    put_field(w, head.parts.method.as_str().as_bytes())?;
    // An origin-form target is written as it came; only a target with a scheme or authority is
    // rendered.
    match (
        head.parts.uri.scheme(),
        head.parts.uri.authority(),
        head.parts.uri.path_and_query(),
    ) {
        (None, None, Some(target)) => put_field(w, target.as_str().as_bytes())?,
        _ => put_field(w, head.parts.uri.to_string().as_bytes())?,
    }
    put_headers(w, &head.parts.headers)?;
    put_u16(w, head.params.len() as u16)?;
    for (k, v) in &head.params {
        put_field(w, k.as_bytes())?;
        put_field(w, v.as_bytes())?;
    }
    match head.client {
        Peer::Ip(std::net::IpAddr::V4(ip)) => {
            put_u8(w, PEER_IPV4)?;
            w.put(&ip.octets())?;
        }
        Peer::Ip(std::net::IpAddr::V6(ip)) => {
            put_u8(w, PEER_IPV6)?;
            w.put(&ip.octets())?;
        }
        Peer::InMemory => put_u8(w, PEER_IN_MEMORY)?,
    }
    match &head.request_id {
        RequestId::Server(id) => {
            put_u8(w, SERVER_ID)?;
            w.put(&id.process.to_le_bytes())?;
            w.put(&id.number.to_le_bytes())?;
        }
        RequestId::Client(id) => {
            put_u8(w, CLIENT_ID)?;
            put_field(w, id.as_bytes())?;
        }
    }
    put_field(w, &req.body)
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
    let n = usize::from(r.u16()?);
    let n = r.count(n, PAIR_MIN_BYTES)?;
    let mut params = Vec::with_capacity(n);
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
            RequestId::Client(ClientRequestId::parse(&text).ok_or(LaneDecodeError::BadClientRequestId(text))?)
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

pub(crate) fn encode_response(w: &mut SlotWriter<'_>, data: &HandlerResponse) -> Result<(), SlotRoomError> {
    w.restart();
    put_u8(w, OK)?;
    put_u16(w, data.status.as_u16())?;
    put_field(w, data.content_type().as_bytes())?;
    put_headers(w, data.headers())?;
    put_field(w, &data.body)
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
/// message always fits a slot of at least `MIN_SLOT_BYTES` (`lane_set.rs`).
///
/// # Errors
///
/// Returns [`SlotRoomError`] for a slot smaller than that.
pub(crate) fn encode_failure(
    w: &mut SlotWriter<'_>,
    error: &str,
    traceback: Option<&str>,
) -> Result<(), SlotRoomError> {
    let room = (w.room() as usize).saturating_sub(16) / 2;
    w.restart();
    put_u8(w, FAILED)?;
    put_field(w, cut_to_fit(error, room).as_bytes())?;
    match traceback {
        Some(traceback) => {
            put_u8(w, 1)?;
            put_field(w, cut_to_fit(traceback, room).as_bytes())
        }
        None => put_u8(w, 0),
    }
}

/// Writes the response or the failure; a response that does not fit is replaced by a failure
/// saying so.
pub(crate) fn encode_answer(
    w: &mut SlotWriter<'_>,
    result: &Result<HandlerResponse, WorkerFailure>,
) -> Result<(), SlotRoomError> {
    match result {
        Ok(data) => match encode_response(w, data) {
            Ok(()) => Ok(()),
            Err(e) => encode_failure(w, &format!("the response does not fit the process lane: {e}"), None),
        },
        Err(failure) => encode_failure(w, &failure.error, failure.traceback.as_deref()),
    }
}

/// Returns the content type `msg[at]` says: one of the common ones is built at compile time and compared here, so it
/// costs no allocation and no check; any other shares `msg`, the message it lies in.
fn content_type(msg: &Bytes, at: std::ops::Range<usize>) -> Result<HeaderValue, LaneDecodeError> {
    Ok(match &msg[at.clone()] {
        b"application/json" => const { HeaderValue::from_static("application/json") },
        b"text/plain; charset=utf-8" => const { HeaderValue::from_static("text/plain; charset=utf-8") },
        b"text/html; charset=utf-8" => const { HeaderValue::from_static("text/html; charset=utf-8") },
        b"text/plain" => const { HeaderValue::from_static("text/plain") },
        b"application/octet-stream" => const { HeaderValue::from_static("application/octet-stream") },
        _ => HeaderValue::from_maybe_shared(msg.slice(at)).map_err(LaneDecodeError::BadHeaderValue)?,
    })
}

/// Decodes an answer the host copied out of its slot; the body and an uncommon content type are slices of `msg`,
/// so nothing of it is copied again.
pub(crate) fn decode_answer(msg: Bytes) -> Result<Result<HandlerResponse, WorkerFailure>, LaneDecodeError> {
    let mut r = MessageReader::new(&msg);
    match r.u8()? {
        OK => {
            let status = hyper::StatusCode::from_u16(r.u16()?).map_err(LaneDecodeError::BadStatus)?;
            let content_type = content_type(&msg, r.field_range()?)?;
            let headers = read_lines(&mut r)?;
            let body = msg.slice(r.field_range()?);
            Ok(Ok(HandlerResponse::new(status, body, content_type, headers)))
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
pub(crate) mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::runtime::process::lane_set::MIN_SLOT_BYTES;
    use hyper::StatusCode;
    use mipyre_shm::adapter::doorbell::FutexDoorbell;
    use mipyre_shm::adapter::parker::FutexParker;
    use mipyre_shm::channel::{Channel, HeapMemory, HostEnd, PeerEnd, Wait};
    use mipyre_shm::plan::geometry::{Geometry, SlotBytes, SLOT_HEADER};
    use mipyre_shm::plan::ring::RingDepth;
    use mipyre_shm::plan::spin_window::SpinWindow;

    /// Lets `fill` write into a request slot of `room` bytes of a channel on the heap, publishes it, and returns
    /// what `fill` returned and the message the slot holds, as its peer reads it: the encoding is tested in a real
    /// slot, the one a lane writes into.
    pub(crate) fn written<R>(room: usize, fill: impl FnOnce(&mut SlotWriter<'_>) -> R) -> (R, Bytes) {
        let slot_bytes = SlotBytes::new(room + SLOT_HEADER).expect("a slot holds its header");
        let geometry = Geometry::new(RingDepth::new(1).expect("1 is a power of two"), slot_bytes).unwrap();
        let channel = Arc::new(Channel::new(HeapMemory::new(&geometry)));
        let mut host = HostEnd::new(Arc::clone(&channel), FutexDoorbell);
        let mut slot = host.request_slot().expect("an empty ring has room");
        let out = slot.write(fill);
        slot.publish().unwrap_or_else(|never| match never {});
        let mut peer = PeerEnd::new(channel, FutexDoorbell, FutexParker);
        let Ok(Wait::Request(request)) = peer.wait_for_request(SpinWindow::until(0), || 0, 1) else {
            panic!("the request was published");
        };
        (
            out,
            Bytes::copy_from_slice(&request.bytes().expect("the slot's length is its own")),
        )
    }

    /// What a message may take in a slot of `MIN_SLOT_BYTES`: the slot without its 8-byte header.
    const MIN_ROOM: usize = MIN_SLOT_BYTES - SLOT_HEADER;

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

    fn encoded_request(sent: &LaneRequest) -> Bytes {
        let (fits, bytes) = written(MIN_ROOM, |w| encode_request(w, sent));
        fits.expect("the request fits");
        bytes
    }

    fn encoded_response(response: &HandlerResponse) -> Bytes {
        let (fits, bytes) = written(MIN_ROOM, |w| encode_response(w, response));
        fits.expect("the response fits");
        bytes
    }

    #[test]
    fn a_request_bigger_than_its_slot_is_refused_with_both_sizes() {
        let (refused, _) = written(256, |w| encode_request(w, &request(&[7u8; 1024])));
        let err = refused.unwrap_err();
        assert_eq!(err.room, 256);
        assert!(err.needed > 1024, "{err:?}");
    }

    #[test]
    fn a_request_round_trips_through_its_slot() {
        let bytes = encoded_request(&request(b"hello"));
        let back = decode_request(&bytes).unwrap();
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
            assert_eq!(decode_request(&encoded_request(&sent)).unwrap().head.client, client);
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
            let back = decode_request(&encoded_request(&sent)).unwrap().head.request_id;
            assert_eq!(back, id);
            assert_eq!(back.to_string(), id.to_string());
        }
    }

    #[test]
    fn a_truncated_request_is_malformed_at_every_cut() {
        let bytes = encoded_request(&request(b"hello"));
        for cut in 0..bytes.len() {
            assert!(decode_request(&bytes[..cut]).is_err(), "cut at {cut} decoded");
        }
    }

    #[test]
    fn an_answer_of_an_unknown_kind_or_empty_is_malformed() {
        assert!(decode_answer(Bytes::from_static(&[9])).is_err());
        assert!(decode_answer(Bytes::new()).is_err());
    }

    #[test]
    fn a_malformed_answer_says_which_way_by_its_variant() {
        assert!(matches!(
            decode_answer(Bytes::from_static(&[9])),
            Err(LaneDecodeError::UnknownTag {
                field: TaggedField::AnswerKind,
                tag: 9
            })
        ));
        assert!(matches!(
            decode_answer(Bytes::new()),
            Err(LaneDecodeError::Truncated {
                wanted: 1,
                at: 0,
                len: 0
            })
        ));
        assert!(matches!(
            decode_answer(Bytes::from_static(&[FAILED, 1, 0, 0, 0, 0xff])),
            Err(LaneDecodeError::NotUtf8(_))
        ));
    }

    #[test]
    fn a_truncated_ok_answer_is_malformed_at_every_cut() {
        let response = HandlerResponse::new(
            StatusCode::OK,
            Bytes::from_static(b"body"),
            HeaderValue::from_static("text/plain"),
            HeaderLines::new(),
        );
        let bytes = encoded_response(&response);
        assert!(decode_answer(bytes.clone()).unwrap().is_ok());
        for cut in 0..bytes.len() {
            assert!(decode_answer(bytes.slice(..cut)).is_err(), "cut at {cut} decoded");
        }
    }

    #[test]
    fn a_failure_too_long_for_its_slot_is_cut_to_fit_and_says_so() {
        let text = "e".repeat(10_000);
        let (fits, bytes) = written(MIN_ROOM, |w| encode_failure(w, &text, Some(&text)));
        fits.unwrap();
        let failure = decode_answer(bytes).unwrap().unwrap_err();
        assert!(failure.error.contains("[cut:"), "{}", failure.error);
        assert!(failure.error.len() < text.len());
        let traceback = failure.traceback.expect("the traceback is kept");
        assert!(traceback.contains("[cut:"), "{traceback}");
    }

    #[test]
    fn a_failure_after_a_response_that_did_not_fit_replaces_it() {
        let response = HandlerResponse::new(
            StatusCode::OK,
            Bytes::from(vec![7u8; 2 * MIN_ROOM]),
            HeaderValue::from_static("text/plain"),
            HeaderLines::new(),
        );
        let (refused_then_fits, bytes) = written(MIN_ROOM, |w| {
            (
                encode_response(w, &response).is_err(),
                encode_failure(w, "too large", None),
            )
        });
        assert!(refused_then_fits.0, "the response does not fit");
        refused_then_fits.1.unwrap();
        let failure = decode_answer(bytes).unwrap().unwrap_err();
        assert_eq!(failure.error, "too large");
    }

    /// A field longer than the room is refused with what it would have needed.
    #[test]
    fn a_field_past_the_room_is_refused_with_both_sizes() {
        let (refused, _) = written(8, |w| {
            w.put(&[1; 2]).unwrap();
            put_field(w, &[2; 3])
        });
        let err = refused.unwrap_err();
        assert_eq!((err.needed, err.room), (9, 8));
    }

    /// A common content type is the static one and any other is read from the message; both read back as sent.
    #[test]
    fn a_content_type_reads_back_whether_common_or_not() {
        for content_type in ["application/json", "text/plain; charset=utf-8", "application/x-mipyre"] {
            let response = HandlerResponse::new(
                StatusCode::OK,
                Bytes::from_static(b"body"),
                HeaderValue::from_static(content_type),
                HeaderLines::new(),
            );
            let back = decode_answer(encoded_response(&response)).unwrap().unwrap();
            assert_eq!(back.content_type().as_bytes(), content_type.as_bytes());
            assert_eq!(&back.body[..], b"body");
        }
    }

    #[test]
    fn a_failure_of_any_length_fits_a_minimal_slot_and_reads_back_cut() {
        let long = "é".repeat(100_000);
        let (fits, bytes) = written(MIN_ROOM, |w| encode_failure(w, &long, Some(&long)));
        fits.unwrap();
        match decode_answer(bytes).unwrap() {
            Err(failure) => {
                assert!(failure.error.contains("[cut:"));
                assert!(failure.traceback.as_deref().unwrap().contains("[cut:"));
            }
            Ok(_) => panic!("a failure decoded as a response"),
        }
    }

    #[test]
    fn a_failure_without_a_traceback_reads_back_none_and_an_empty_one_reads_back_empty() {
        for traceback in [None, Some("")] {
            let (fits, bytes) = written(MIN_ROOM, |w| encode_failure(w, "boom", traceback));
            fits.unwrap();
            let failure = decode_answer(bytes).unwrap().unwrap_err();
            assert_eq!(failure.error, "boom");
            assert_eq!(failure.traceback.as_deref(), traceback);
        }
        // A worker's failure takes the `encode_answer` path, so it is checked too.
        let failed = Err(WorkerFailure {
            error: "boom".into(),
            traceback: None,
        });
        let (fits, bytes) = written(MIN_ROOM, |w| encode_answer(w, &failed));
        fits.unwrap();
        assert_eq!(decode_answer(bytes).unwrap().unwrap_err().traceback, None);
    }

    /// The b/926 answer: OK, status 200, an empty content type and 65,536 header lines, in 11 bytes. It is
    /// `__fuzz__/runtime__process__wire_fuzz__any_answer_bytes_decode_or_are_refused/corpus/` 's regression input too.
    const CLAIMS_65536_LINES: [u8; 11] = [0x00, 0xc8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00];

    #[test]
    fn an_answer_claiming_more_lines_than_its_bytes_hold_is_malformed_not_a_panic() {
        assert!(matches!(
            decode_answer(Bytes::from_static(&CLAIMS_65536_LINES)),
            Err(LaneDecodeError::CountPastEnd {
                count: 65_536,
                least: PAIR_MIN_BYTES,
                at: 11,
                len: 11
            })
        ));
    }

    /// An OK answer of `lines` header lines whose fields are all empty, with `pad` bytes after the count.
    fn answer_of_empty_lines(lines: u32, pad: usize) -> Bytes {
        let mut msg = vec![OK];
        msg.extend_from_slice(&200u16.to_le_bytes());
        msg.extend_from_slice(&0u32.to_le_bytes());
        msg.extend_from_slice(&lines.to_le_bytes());
        msg.resize(msg.len() + pad, 0);
        Bytes::from(msg)
    }

    /// Each line takes at least its two lengths: a count the rest holds goes on to the lines (an empty name is
    /// malformed), and one more line is refused from the count alone.
    #[test]
    fn a_line_count_is_held_to_what_the_rest_of_the_answer_holds() {
        let held = answer_of_empty_lines(3, 3 * PAIR_MIN_BYTES);
        assert!(matches!(decode_answer(held), Err(LaneDecodeError::BadHeaderName(_))));
        let past = answer_of_empty_lines(4, 3 * PAIR_MIN_BYTES);
        assert!(matches!(
            decode_answer(past),
            Err(LaneDecodeError::CountPastEnd {
                count: 4,
                least: PAIR_MIN_BYTES,
                at: 11,
                len: 35
            })
        ));
    }

    /// A slot holds more lines than a header map does (a 256 KiB slot, the default, holds some 32,000): the count is
    /// refused before any line is read, never passed to an allocation that panics.
    #[test]
    fn an_answer_with_more_lines_than_a_header_map_holds_is_malformed() {
        let lines = 24_577;
        let answer = answer_of_empty_lines(lines, lines as usize * PAIR_MIN_BYTES);
        assert!(matches!(
            decode_answer(answer),
            Err(LaneDecodeError::TooManyLines {
                count: 24_577,
                max: MAX_LINES
            })
        ));
    }

    /// `MAX_LINES` is the most a header map takes room for: one more is refused by `http` itself.
    #[test]
    fn max_lines_is_the_most_a_header_map_takes_room_for() {
        assert!(HeaderMap::<HeaderValue>::try_with_capacity(MAX_LINES).is_ok());
        assert!(HeaderMap::<HeaderValue>::try_with_capacity(MAX_LINES + 1).is_err());
    }

    /// The worker's side holds the host's counts to the same bounds.
    #[test]
    fn a_request_whose_counts_pass_its_bytes_is_malformed() {
        let bytes = encoded_request(&request(b""));
        // The header count follows the route key, the method and the target.
        let at = [b"POST".len(), b"/x".len(), b"POST".len(), b"/x?y=1".len()]
            .iter()
            .map(|n| 4 + n)
            .sum::<usize>();
        let mut claims = bytes.to_vec();
        claims[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode_request(&claims),
            Err(LaneDecodeError::CountPastEnd { count, least: PAIR_MIN_BYTES, .. }) if count == u32::MAX as usize
        ));
        // One header line, `x-a: b`, then the parameter count.
        let params_at = at + 4 + (4 + b"x-a".len()) + (4 + b"b".len());
        let mut claims = bytes.to_vec();
        claims[params_at..params_at + 2].copy_from_slice(&u16::MAX.to_le_bytes());
        assert!(matches!(
            decode_request(&claims),
            Err(LaneDecodeError::CountPastEnd {
                count: 65_535,
                least: PAIR_MIN_BYTES,
                ..
            })
        ));
    }

    #[test]
    fn a_slot_too_small_for_a_failure_is_refused_not_panicked() {
        assert!(written(8, |w| encode_failure(w, "boom", None)).0.is_err());
    }
}
