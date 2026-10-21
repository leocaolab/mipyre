//! Builds the HTTP responses for a handler's result and for the errors the engine answers by itself.

use std::fmt::Display;
use std::time::Duration;

use bytes::Bytes;
use http::header::{
    HeaderMap, HeaderName, HeaderValue, ALLOW, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER, SERVER,
};
use http::{Response, StatusCode};

use crate::plan::reject::RouteMiss;

pub const SERVER_HEADER: &str = concat!("Mipyre/", env!("CARGO_PKG_VERSION"));

pub const JSON: HeaderValue = HeaderValue::from_static("application/json");
pub const TEXT: HeaderValue = HeaderValue::from_static("text/plain; charset=utf-8");
pub const OCTET_STREAM: HeaderValue = HeaderValue::from_static("application/octet-stream");
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
}

impl HandlerResponse {
    #[inline]
    pub fn new(
        status: StatusCode,
        body: Bytes,
        content_type: HeaderValue,
        headers: HeaderLines,
    ) -> Self {
        HandlerResponse {
            body,
            status,
            content_type,
            headers: headers.0,
        }
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
    headers
        .entry(SERVER)
        .or_insert(HeaderValue::from_static(SERVER_HEADER));
    let mut resp = Response::new(data.body);
    *resp.status_mut() = data.status;
    *resp.headers_mut() = headers;
    resp
}

/// Returns a 500 carrying the request id, which the client can quote to an operator to find
/// the real error in the log.
pub fn error_response(msg: &str, request_id: &impl Display) -> Response<Bytes> {
    json_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        server_error_body(msg, request_id),
    )
}

/// Escapes `msg`, because it may carry client input.
fn error_json_body(msg: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(ERROR_BODY_ROOM + msg.len());
    body.extend_from_slice(b"{\"error\":\"");
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
fn server_error_body(msg: &str, request_id: &impl Display) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut body = Vec::with_capacity(ERROR_BODY_ROOM + msg.len());
    body.extend_from_slice(b"{\"error\":\"");
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
            out.extend_from_slice(&[b'\\', b'u', b'0', b'0', HEX[usize::from(b >> 4)], HEX[usize::from(b & 0xf)]]);
        } else {
            out.extend_from_slice(short);
        }
        plain = i + 1;
    }
    out.extend_from_slice(&bytes[plain..]);
}

/// Returns a 503 for a request no queue slot or permit was free for.
#[inline]
pub fn overloaded_response(msg: &str, request_id: &impl Display) -> Response<Bytes> {
    let mut resp = json_error(
        StatusCode::SERVICE_UNAVAILABLE,
        server_error_body(msg, request_id),
    );
    resp.headers_mut()
        .insert(RETRY_AFTER, OVERLOAD_RETRY_AFTER_SECS);
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
pub fn retry_later_response(msg: &str, request_id: &impl Display, wait: Duration) -> Response<Bytes> {
    let mut resp = json_error(
        StatusCode::SERVICE_UNAVAILABLE,
        server_error_body(msg, request_id),
    );
    resp.headers_mut()
        .insert(RETRY_AFTER, HeaderValue::from(retry_after_secs(wait)));
    resp
}

/// Returns a 503 for a request whose workers are gone because the server is shutting down.
#[inline]
pub fn unavailable_response(msg: &str, request_id: &impl Display) -> Response<Bytes> {
    json_error(
        StatusCode::SERVICE_UNAVAILABLE,
        server_error_body(msg, request_id),
    )
}

#[inline]
pub fn bad_request_response(msg: &str) -> Response<Bytes> {
    json_error(StatusCode::BAD_REQUEST, error_json_body(msg))
}

#[inline]
pub fn request_timeout_response() -> Response<Bytes> {
    json_error(
        StatusCode::REQUEST_TIMEOUT,
        error_json_body("request body timeout"),
    )
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
        Bytes::from_static(b"{\"error\":\"payload too large\"}"),
    )
}

/// Returns a 502 for a request whose process-lane worker died while running it.
#[inline]
pub fn bad_gateway_response(request_id: &impl Display) -> Response<Bytes> {
    json_error(
        StatusCode::BAD_GATEWAY,
        server_error_body("worker process died", request_id),
    )
}

#[inline]
pub fn gateway_timeout_response(request_id: &impl Display) -> Response<Bytes> {
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

#[inline]
fn not_found_response() -> Response<Bytes> {
    json_error(
        StatusCode::NOT_FOUND,
        Bytes::from_static(b"{\"error\":\"not found\"}"),
    )
}

fn method_not_allowed_response(allow: HeaderValue) -> Response<Bytes> {
    let mut resp = json_error(
        StatusCode::METHOD_NOT_ALLOWED,
        Bytes::from_static(b"{\"error\":\"method not allowed\"}"),
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
        headers
            .entry(CONTENT_LENGTH)
            .or_insert_with(|| HeaderValue::from(len));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn body_bytes(resp: Response<Bytes>) -> Vec<u8> {
        resp.into_body().to_vec()
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

    const REQUEST_ID: &str = "7f3c0a5e9b1d4e2f";

    #[test]
    fn not_found_status_and_body() {
        let resp = not_found_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(resp.headers()["content-type"], "application/json");
        assert!(resp.headers()["server"]
            .to_str()
            .unwrap()
            .starts_with("Mipyre/"));
        assert_eq!(body_bytes(resp), b"{\"error\":\"not found\"}");
    }

    #[test]
    fn error_bodies_are_the_json_of_their_object() {
        let mut nasty: String = (0u8..0x20).map(char::from).collect();
        nasty.push_str("\"\\/ plain é \u{2028} end");
        for msg in ["server overloaded", "", nasty.as_str()] {
            assert_eq!(
                error_json_body(msg),
                serde_json::to_vec(&serde_json::json!({ "error": msg })).unwrap(),
                "{msg:?}"
            );
            for id in [REQUEST_ID, nasty.as_str()] {
                assert_eq!(
                    server_error_body(msg, &id),
                    serde_json::to_vec(&serde_json::json!({ "error": msg, "request_id": id })).unwrap(),
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
        assert_eq!(parsed["error"], "back\\slash\nnewline\ttab");
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
        let want = [("x-a", "1"), ("x-a", "3"), ("x-b", "2")]
            .map(|(n, v)| (n.to_string(), v.to_string()));
        assert_eq!(lines(headers.as_map()), want);
    }

    #[test]
    fn repeated_header_lines_stay_separate() {
        let mut headers = HeaderLines::new();
        let cookie = HeaderName::from_static("set-cookie");
        assert!(headers.append(cookie.clone(), HeaderValue::from_static("a=1")).is_none());
        assert!(headers.append(cookie, HeaderValue::from_static("b=2")).is_none());
        let resp = build_response(HandlerResponse::new(
            StatusCode::OK,
            Bytes::new(),
            TEXT,
            headers,
        ));
        let cookies: Vec<_> = resp.headers().get_all("set-cookie").iter().collect();
        assert_eq!(cookies, ["a=1", "b=2"]);
    }
}
