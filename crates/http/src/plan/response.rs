//! Builds the HTTP responses for a handler's result and for the errors the engine answers by itself.

use std::time::Duration;

use bytes::Bytes;
use http::header::{HeaderMap, HeaderName, HeaderValue, ALLOW, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER, SERVER};
use http::{Response, StatusCode};

use crate::plan::header::MAX_HEADER_LINES;
use crate::plan::reject::{RouteMiss, SiteMiss};
use crate::plan::request_id::RequestId;

pub const SERVER_HEADER: &str = concat!("Mipyre/", env!("CARGO_PKG_VERSION"));

pub const JSON: HeaderValue = HeaderValue::from_static("application/json");
pub const TEXT: HeaderValue = HeaderValue::from_static("text/plain; charset=utf-8");
pub const OCTET_STREAM: HeaderValue = HeaderValue::from_static("application/octet-stream");
/// Room for the lines the server puts on a handler's response besides the handler's own:
/// `content-type`, `server`, `content-length`, `content-encoding`, `vary`, a stream's
/// `cache-control` and `connection`, the request-id header and the CORS lines (five names).
const SERVER_LINES: usize = 16;

/// The most header lines a handler's response carries (`Response`, a stream, an `HTTPException`,
/// a route's fast response), so that with the server's own lines the response's header map stays
/// within [`MAX_HEADER_LINES`]; past it the handler gets an error, never a panic.
pub const MAX_HANDLER_LINES: usize = MAX_HEADER_LINES - SERVER_LINES;

/// The `Retry-After` of an overload 503, in seconds.
const OVERLOAD_RETRY_AFTER_SECS: HeaderValue = HeaderValue::from_static("1");

/// A response's header lines in the order given, never holding `content-type`.
///
/// [`HeaderLines::append`] hands a `content-type` back instead of storing it, because taking it
/// out later with `HeaderMap::remove` would move the map's last name into its place and reorder
/// the lines.
#[derive(Clone, Debug, Default)]
pub struct HeaderLines(HeaderMap);

impl HeaderLines {
    pub fn new() -> Self {
        HeaderLines(HeaderMap::new())
    }

    pub fn with_capacity(n: usize) -> Self {
        HeaderLines(HeaderMap::with_capacity(n))
    }

    /// Appends a line of `name`, except that a `content-type` is not added and is handed back.
    #[inline]
    #[must_use = "a content-type handed back is the response's type"]
    pub fn append(&mut self, name: HeaderName, value: HeaderValue) -> Option<HeaderValue> {
        if name == CONTENT_TYPE {
            return Some(value);
        }
        self.0.append(name, value);
        None
    }

    pub fn as_map(&self) -> &HeaderMap {
        &self.0
    }
}

#[derive(Debug)]
pub struct HandlerResponse {
    pub body: Bytes,
    pub status: StatusCode,
    content_type: HeaderValue,
    /// It never holds `content-type`, so it cannot disagree with `content_type`.
    headers: HeaderMap,
    /// The handler refused the request as invalid (a `body=` model or `query=` string), with
    /// this many errors: the server counts it in the run that served it.
    validation_errors: Option<std::num::NonZeroU32>,
}

impl HandlerResponse {
    #[inline]
    pub fn new(status: StatusCode, body: Bytes, content_type: HeaderValue, headers: HeaderLines) -> Self {
        HandlerResponse {
            body,
            status,
            content_type,
            headers: headers.0,
            validation_errors: None,
        }
    }

    /// Marks this answer as the refusal of a request that failed validation with `errors`
    /// errors.
    pub fn failed_validation(mut self, errors: std::num::NonZeroU32) -> Self {
        self.validation_errors = Some(errors);
        self
    }

    /// How many errors the request this answer refuses failed validation with, or `None` for an
    /// answer that is no such refusal.
    #[inline]
    pub fn validation_errors(&self) -> Option<std::num::NonZeroU32> {
        self.validation_errors
    }

    pub fn content_type(&self) -> &HeaderValue {
        &self.content_type
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub fn header_lines(&self) -> HeaderLines {
        HeaderLines(self.headers.clone())
    }

    pub(crate) fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }
}

/// Builds the HTTP response, adding `server` unless the handler set one.
pub fn build_response(data: HandlerResponse) -> Response<Bytes> {
    let mut headers = data.headers;
    // A handler's map is a clone with no spare room, so reserving once avoids two reallocations.
    // An empty map's default room on its first insert already fits the CORS lines added later.
    if !headers.is_empty() {
        headers.reserve(2);
    }
    headers.insert(CONTENT_TYPE, data.content_type);
    headers.entry(SERVER).or_insert(HeaderValue::from_static(SERVER_HEADER));
    let mut resp = Response::new(data.body);
    *resp.status_mut() = data.status;
    *resp.headers_mut() = headers;
    resp
}

/// Returns a 500 carrying the request id, which the client can quote to an operator to find
/// the real error in the log.
pub fn error_response(msg: &str, request_id: &RequestId) -> Response<Bytes> {
    json_error(StatusCode::INTERNAL_SERVER_ERROR, server_error_body(msg, request_id))
}

/// Escapes `msg`, because it may carry client input.
fn error_json_body(msg: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(ERROR_BODY_ROOM + msg.len());
    body.extend_from_slice(b"{\"detail\":\"");
    escape_json_into(&mut body, msg);
    body.extend_from_slice(b"\"}");
    body
}

/// Writes straight into the buffer that becomes the body, because a 503 is made most often
/// when the server has least to spare.
#[expect(
    clippy::expect_used,
    reason = "JsonEscaper never fails a write, so only a Display impl that fails on its own could: \
              that is a broken Display, which `to_string` panics on too"
)]
fn server_error_body(msg: &str, request_id: &RequestId) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut body = Vec::with_capacity(ERROR_BODY_ROOM + msg.len());
    body.extend_from_slice(b"{\"detail\":\"");
    escape_json_into(&mut body, msg);
    body.extend_from_slice(b"\",\"request_id\":\"");
    write!(JsonEscaper(&mut body), "{request_id}").expect("a Display impl does not fail on its own");
    body.extend_from_slice(b"\"}");
    body
}

/// Fits an error body's keys, quotes and a minted 32-hex-digit request id.
const ERROR_BODY_ROOM: usize = 64;

/// Escapes what is written to it as the inside of a JSON string.
struct JsonEscaper<'a>(&'a mut Vec<u8>);

impl std::fmt::Write for JsonEscaper<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        escape_json_into(self.0, s);
        Ok(())
    }
}

/// Escapes `s` as the inside of a JSON string (RFC 8259 §7).
fn escape_json_into(out: &mut Vec<u8>, s: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = s.as_bytes();
    let mut plain = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let short: &[u8] = match b {
            b'"' => b"\\\"",
            b'\\' => b"\\\\",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            b'\t' => b"\\t",
            0x08 => b"\\b",
            0x0c => b"\\f",
            0x00..=0x1f => b"",
            _ => continue,
        };
        out.extend_from_slice(&bytes[plain..i]);
        if short.is_empty() {
            out.extend_from_slice(&[
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[usize::from(b >> 4)],
                HEX[usize::from(b & 0xf)],
            ]);
        } else {
            out.extend_from_slice(short);
        }
        plain = i + 1;
    }
    out.extend_from_slice(&bytes[plain..]);
}

/// Returns a 503 for a request no queue slot or permit was free for.
#[inline]
pub fn overloaded_response(msg: &str, request_id: &RequestId) -> Response<Bytes> {
    let mut resp = json_error(StatusCode::SERVICE_UNAVAILABLE, server_error_body(msg, request_id));
    resp.headers_mut().insert(RETRY_AFTER, OVERLOAD_RETRY_AFTER_SECS);
    resp
}

/// Returns `wait` in whole seconds rounded up, and at least 1 so that a wait already over still
/// asks the client to come back later.
pub fn retry_after_secs(wait: Duration) -> u64 {
    let secs = wait.as_secs().saturating_add(u64::from(wait.subsec_nanos() > 0));
    secs.max(1)
}

/// Returns a 503 for a request whose workers come back after `wait`.
#[inline]
pub fn retry_later_response(msg: &str, request_id: &RequestId, wait: Duration) -> Response<Bytes> {
    let mut resp = json_error(StatusCode::SERVICE_UNAVAILABLE, server_error_body(msg, request_id));
    resp.headers_mut()
        .insert(RETRY_AFTER, HeaderValue::from(retry_after_secs(wait)));
    resp
}

/// Returns a 503 for a request whose workers are gone because the server is shutting down.
#[inline]
pub fn unavailable_response(msg: &str, request_id: &RequestId) -> Response<Bytes> {
    json_error(StatusCode::SERVICE_UNAVAILABLE, server_error_body(msg, request_id))
}

#[inline]
pub fn bad_request_response(msg: &str) -> Response<Bytes> {
    json_error(StatusCode::BAD_REQUEST, error_json_body(msg))
}

#[inline]
pub fn request_timeout_response() -> Response<Bytes> {
    json_error(StatusCode::REQUEST_TIMEOUT, error_json_body("request body timeout"))
}

fn json_error(status: StatusCode, body: impl Into<Bytes>) -> Response<Bytes> {
    let mut resp = Response::new(body.into());
    *resp.status_mut() = status;
    let headers = resp.headers_mut();
    headers.insert(CONTENT_TYPE, JSON);
    headers.insert(SERVER, HeaderValue::from_static(SERVER_HEADER));
    resp
}

#[inline]
pub fn payload_too_large_response() -> Response<Bytes> {
    json_error(
        StatusCode::PAYLOAD_TOO_LARGE,
        Bytes::from_static(b"{\"detail\":\"payload too large\"}"),
    )
}

/// Returns a 502 for a request whose process-lane worker died while running it.
#[inline]
pub fn bad_gateway_response(request_id: &RequestId) -> Response<Bytes> {
    json_error(
        StatusCode::BAD_GATEWAY,
        server_error_body("worker process died", request_id),
    )
}

#[inline]
pub fn gateway_timeout_response(request_id: &RequestId) -> Response<Bytes> {
    json_error(
        StatusCode::GATEWAY_TIMEOUT,
        server_error_body("request timeout", request_id),
    )
}

pub fn reject_response(reject: RouteMiss) -> Response<Bytes> {
    match reject {
        RouteMiss::NotFound => not_found_response(),
        RouteMiss::MethodNotAllowed(allow) => method_not_allowed_response(allow),
    }
}

/// The most bytes of a refused request's own text that its answer quotes.
const QUOTED_MAX: usize = 256;

/// Answers a request [`SiteMiss`] turns away: `421` for a host that is none of this server's
/// names, `403` for a page from a site it does not trust. The body is `{"detail": …}`, the shape
/// FastAPI answers its own refusals in; it quotes what the request sent,
/// cut to [`QUOTED_MAX`] bytes, and names the setting that would let it through.
pub fn site_miss_response(miss: SiteMiss<'_>) -> Response<Bytes> {
    let (status, before, quoted, after): (_, &[u8], _, &[u8]) = match miss {
        SiteMiss::UnknownHost(host) => (
            StatusCode::MISDIRECTED_REQUEST,
            b"this server does not answer to host \\\"",
            host,
            b"\\\"; list the name in allowed_hosts",
        ),
        SiteMiss::CrossSite(Some(origin)) => (
            StatusCode::FORBIDDEN,
            b"cross-site request from origin \\\"",
            origin,
            b"\\\" refused; list the origin in enable_cors(allow_origins=...)",
        ),
        SiteMiss::CrossSite(None) => (
            StatusCode::FORBIDDEN,
            b"",
            &[][..],
            b"cross-site request without an Origin refused; a page's request must carry an Origin listed in enable_cors(allow_origins=...)",
        ),
    };
    let quoted = String::from_utf8_lossy(&quoted[..quoted.len().min(QUOTED_MAX)]);
    let mut body = Vec::with_capacity(ERROR_BODY_ROOM + before.len() + 2 * quoted.len() + after.len());
    body.extend_from_slice(b"{\"detail\":\"");
    body.extend_from_slice(before);
    escape_json_into(&mut body, &quoted);
    body.extend_from_slice(after);
    body.extend_from_slice(b"\"}");
    json_error(status, body)
}

#[inline]
fn not_found_response() -> Response<Bytes> {
    json_error(StatusCode::NOT_FOUND, Bytes::from_static(b"{\"detail\":\"not found\"}"))
}

fn method_not_allowed_response(allow: HeaderValue) -> Response<Bytes> {
    let mut resp = json_error(
        StatusCode::METHOD_NOT_ALLOWED,
        Bytes::from_static(b"{\"detail\":\"method not allowed\"}"),
    );
    resp.headers_mut().insert(ALLOW, allow);
    resp
}

/// Adds the `Content-Length` a `GET` body would carry, unless one is set; the engine must then
/// drop the body itself, because HTTP/2 would send it. A `304` gets none, because its empty body
/// is not the length of the representation (RFC 9110 §8.6).
#[inline]
pub fn head_headers(headers: &mut HeaderMap, status: StatusCode, body_len: Option<u64>) {
    if status == StatusCode::NOT_MODIFIED {
        return;
    }
    if let Some(len) = body_len {
        headers.entry(CONTENT_LENGTH).or_insert_with(|| HeaderValue::from(len));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::request_id::{ClientRequestId, ServerRequestId};

    fn body_bytes(resp: Response<Bytes>) -> Vec<u8> {
        resp.into_body().to_vec()
    }

    #[test]
    fn a_site_miss_quotes_what_was_sent_cut_and_escaped_and_names_the_setting() {
        let resp = site_miss_response(SiteMiss::UnknownHost(b"evil.example:8000"));
        assert_eq!(resp.status(), StatusCode::MISDIRECTED_REQUEST);
        assert_eq!(resp.headers()[CONTENT_TYPE], JSON);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(
            body["detail"],
            "this server does not answer to host \"evil.example:8000\"; list the name in allowed_hosts"
        );
        let resp = site_miss_response(SiteMiss::CrossSite(Some(b"https://e\"vil")));
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        let detail = body["detail"].as_str().unwrap();
        assert!(
            detail.contains("\"https://e\"vil\"") && detail.contains("enable_cors(allow_origins=...)"),
            "{detail}"
        );
        let body: serde_json::Value =
            serde_json::from_slice(site_miss_response(SiteMiss::CrossSite(None)).body()).unwrap();
        assert!(body["detail"].as_str().unwrap().contains("without an Origin"));
        let long = [b'a'; 1000];
        let body: serde_json::Value =
            serde_json::from_slice(site_miss_response(SiteMiss::UnknownHost(&long)).body()).unwrap();
        let quoted = body["detail"].as_str().unwrap().matches('a').count();
        assert!((QUOTED_MAX..QUOTED_MAX + 10).contains(&quoted), "{quoted}");
    }

    #[test]
    fn a_head_answer_gets_the_length_a_get_would_and_a_304_none() {
        let mut ok = HeaderMap::new();
        head_headers(&mut ok, StatusCode::OK, Some(1024));
        assert_eq!(ok[CONTENT_LENGTH], "1024");
        let mut not_modified = HeaderMap::new();
        head_headers(&mut not_modified, StatusCode::NOT_MODIFIED, Some(0));
        assert_eq!(not_modified.get(CONTENT_LENGTH), None);
    }

    const REQUEST_ID: RequestId = RequestId::Server(ServerRequestId {
        process: 0x7f3c_0a5e_9b1d_4e2f,
        number: 42,
    });

    #[test]
    fn not_found_status_and_body() {
        let resp = not_found_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(resp.headers()["content-type"], "application/json");
        assert!(resp.headers()["server"].to_str().unwrap().starts_with("Mipyre/"));
        assert_eq!(body_bytes(resp), b"{\"detail\":\"not found\"}");
    }

    #[test]
    fn error_bodies_are_the_json_of_their_object() {
        let mut nasty: String = (0u8..0x20).map(char::from).collect();
        nasty.push_str("\"\\/ plain é \u{2028} end");
        for msg in ["server overloaded", "", nasty.as_str()] {
            assert_eq!(
                error_json_body(msg),
                serde_json::to_vec(&serde_json::json!({ "detail": msg })).unwrap(),
                "{msg:?}"
            );
            let client = RequestId::Client(ClientRequestId::parse("Root=1-5759e988;a_b.c:d/e+f@g~h").unwrap());
            for id in [REQUEST_ID, client] {
                assert_eq!(
                    server_error_body(msg, &id),
                    serde_json::to_vec(&serde_json::json!({ "detail": msg, "request_id": id.to_string() })).unwrap(),
                    "{msg:?} {id:?}"
                );
            }
        }
    }

    #[test]
    fn error_response_500() {
        let resp = error_response("something broke", &REQUEST_ID);
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(resp.headers()["content-type"], "application/json");
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        assert!(body.contains("something broke"));
    }

    #[test]
    fn error_response_escapes_quotes() {
        let resp = error_response(r#"bad "input""#, &REQUEST_ID);
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        assert!(body.contains(r#"bad \"input\""#));
    }

    #[test]
    fn error_response_escapes_control_chars_and_backslashes() {
        let resp = error_response("back\\slash\nnewline\ttab", &REQUEST_ID);
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("must parse");
        assert_eq!(parsed["detail"], "back\\slash\nnewline\ttab");
    }

    #[test]
    fn overloaded_503_with_retry_after() {
        let resp = overloaded_response("too busy", &REQUEST_ID);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.headers()["retry-after"], "1");
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        assert!(body.contains("too busy"));
    }

    #[test]
    fn retry_after_rounds_the_wait_up_to_whole_seconds_and_never_says_0() {
        assert_eq!(retry_after_secs(Duration::ZERO), 1);
        assert_eq!(retry_after_secs(Duration::from_millis(1)), 1);
        assert_eq!(retry_after_secs(Duration::from_secs(1)), 1);
        assert_eq!(retry_after_secs(Duration::from_millis(1001)), 2);
        assert_eq!(retry_after_secs(Duration::from_millis(4999)), 5);
        assert_eq!(retry_after_secs(Duration::MAX), u64::MAX);
    }

    #[test]
    fn a_retry_later_503_says_when_in_retry_after() {
        let resp = retry_later_response("lane restarting", &REQUEST_ID, Duration::from_millis(1500));
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.headers()["retry-after"], "2");
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        assert!(body.contains("lane restarting"));
    }

    #[test]
    fn payload_too_large_413() {
        let resp = payload_too_large_response();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        assert!(body.contains("payload too large"));
    }

    #[test]
    fn gateway_timeout_504() {
        let resp = gateway_timeout_response(&REQUEST_ID);
        assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
        let body = String::from_utf8(body_bytes(resp)).unwrap();
        assert!(body.contains("request timeout"));
    }

    #[test]
    fn build_response_ok() {
        let data = HandlerResponse::new(
            StatusCode::OK,
            Bytes::from("hello"),
            HeaderValue::from_static("text/plain"),
            HeaderLines::new(),
        );
        let resp = build_response(data);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()["content-type"], "text/plain");
    }

    #[test]
    fn build_response_custom_status_and_headers() {
        let mut headers = HeaderLines::new();
        let added = headers.append(HeaderName::from_static("x-custom"), HeaderValue::from_static("value"));
        assert!(added.is_none());
        let data = HandlerResponse::new(StatusCode::CREATED, Bytes::from("created"), JSON, headers);
        let resp = build_response(data);
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(resp.headers()["x-custom"], "value");
    }

    #[test]
    fn handler_content_type_and_server_replace_the_defaults() {
        let mut headers = HeaderLines::new();
        let given = headers.append(CONTENT_TYPE, HeaderValue::from_static("text/csv"));
        assert!(headers.append(SERVER, HeaderValue::from_static("mine")).is_none());
        let resp = build_response(HandlerResponse::new(
            StatusCode::OK,
            Bytes::from("a,b"),
            given.unwrap_or(TEXT),
            headers,
        ));
        let all = |name| {
            resp.headers()
                .get_all(name)
                .iter()
                .map(|v| v.to_str().unwrap().to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(all(CONTENT_TYPE), ["text/csv"]);
        assert_eq!(all(SERVER), ["mine"]);
    }

    fn lines(headers: &HeaderMap) -> Vec<(String, String)> {
        headers
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_str().unwrap().to_string()))
            .collect()
    }

    #[test]
    fn a_type_is_handed_back_and_the_lines_keep_their_order() {
        let given = [
            ("content-type", "t"),
            ("x-a", "1"),
            ("content-type", "u"),
            ("x-b", "2"),
            ("x-a", "3"),
            ("content-type", "v"),
        ];
        let mut headers = HeaderLines::new();
        let mut handed = Vec::new();
        for (name, value) in given {
            let value = HeaderValue::from_static(value);
            if let Some(t) = headers.append(HeaderName::from_static(name), value) {
                handed.push(t.to_str().unwrap().to_string());
            }
        }
        assert_eq!(handed, ["t", "u", "v"]);
        let want = [("x-a", "1"), ("x-a", "3"), ("x-b", "2")].map(|(n, v)| (n.to_string(), v.to_string()));
        assert_eq!(lines(headers.as_map()), want);
    }

    #[test]
    fn repeated_header_lines_stay_separate() {
        let mut headers = HeaderLines::new();
        let cookie = HeaderName::from_static("set-cookie");
        assert!(headers
            .append(cookie.clone(), HeaderValue::from_static("a=1"))
            .is_none());
        assert!(headers.append(cookie, HeaderValue::from_static("b=2")).is_none());
        let resp = build_response(HandlerResponse::new(StatusCode::OK, Bytes::new(), TEXT, headers));
        let cookies: Vec<_> = resp.headers().get_all("set-cookie").iter().collect();
        assert_eq!(cookies, ["a=1", "b=2"]);
    }
}
