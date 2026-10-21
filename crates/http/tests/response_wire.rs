//! Pins the bytes a client receives for each response the engine builds without a handler's own
//! HTTP code, so that a change to how they are built changes nothing on the wire.
//!
//! Hyper's HTTP/1.1 serves them over an in-memory pipe, as it serves the engine's connections, and
//! they are compared byte for byte, header case and order included; only the `date` value is
//! masked.
// The package's clippy.toml is plan/'s list; this test drives a hyper connection.
#![expect(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "an integration test drives a hyper connection over a pipe, on a task of its own"
)]
// `allow-expect-in-tests` does not reach `wire`, which is not a `#[test]`.
#![expect(clippy::expect_used, reason = "the test's helper fails the test by panicking")]

use std::collections::HashMap;
use std::convert::Infallible;

use bytes::Bytes;
use http::header::{HeaderName, HeaderValue, CONTENT_TYPE, SERVER};
use http::{Response, StatusCode};
use http_body_util::Full;
use mipyre_http::plan::reject::RouteMiss;
use mipyre_http::plan::response::{
    build_response, reject_response, HandlerResponse, HeaderLines, JSON, SERVER_HEADER, TEXT,
};
use mipyre_http::plan::route::FastResponse;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Returns `response` as hyper writes it in answer to one `GET /` on a keep-alive connection, with
/// the `date` value replaced by `<date>`.
async fn wire(response: Response<Bytes>) -> String {
    let response = std::sync::Mutex::new(Some(response.map(Full::new)));
    let service = hyper::service::service_fn(move |_: hyper::Request<hyper::body::Incoming>| {
        let taken = response.lock().ok().and_then(|mut r| r.take());
        async move {
            Ok::<_, Infallible>(taken.unwrap_or_else(|| Response::new(Full::new(Bytes::new()))))
        }
    });
    let (mut client, server) = tokio::io::duplex(64 * 1024);
    let conn = tokio::spawn(
        hyper::server::conn::http1::Builder::new()
            .half_close(true)
            .serve_connection(hyper_util::rt::TokioIo::new(server), service),
    );
    client
        .write_all(b"GET / HTTP/1.1\r\nhost: t\r\n\r\n")
        .await
        .expect("the request is written");
    // Hyper reads the half-close as the end of the requests, not as the connection gone, so it
    // answers and then closes.
    client.shutdown().await.expect("the write half is closed");
    let mut raw = Vec::new();
    client
        .read_to_end(&mut raw)
        .await
        .expect("the response is read");
    conn.await
        .expect("the connection task")
        .expect("the connection is served");
    let text = String::from_utf8(raw).expect("the response is text");
    text.split("\r\n")
        .map(|line| match line.strip_prefix("date: ") {
            Some(_) => "date: <date>",
            None => line,
        })
        .collect::<Vec<_>>()
        .join("\r\n")
}

fn expected(lines: &[&str], body: &str) -> String {
    let mut out = lines.join("\r\n");
    out.push_str("\r\n\r\n");
    out.push_str(body);
    out
}

#[tokio::test]
async fn a_fast_response_goes_out_as_registered() {
    let headers = HashMap::from([("X-Probe".to_string(), "yes".to_string())]);
    let fast = FastResponse::parse(Bytes::from(b"ok".to_vec()), "text/plain", 200, &headers)
        .expect("a valid response");
    let server = format!("server: {SERVER_HEADER}");
    let want = expected(
        &[
            "HTTP/1.1 200 OK",
            "content-type: text/plain",
            &server,
            "x-probe: yes",
            "content-length: 2",
            "date: <date>",
        ],
        "ok",
    );
    assert_eq!(wire(fast.to_response()).await, want);
    assert_eq!(wire(fast.own_copy().to_response()).await, want, "a thread's copy");
}

#[tokio::test]
async fn a_fast_response_with_another_status_goes_out_with_it() {
    let fast = FastResponse::parse(Bytes::from_static(b""), "text/html", 503, &HashMap::new())
        .expect("a valid response");
    let server = format!("server: {SERVER_HEADER}");
    let want = expected(
        &[
            "HTTP/1.1 503 Service Unavailable",
            "content-type: text/html",
            &server,
            "content-length: 0",
            "date: <date>",
        ],
        "",
    );
    assert_eq!(wire(fast.own_copy().to_response()).await, want);
}

#[tokio::test]
async fn a_handlers_dict_goes_out_as_json() {
    let body = br#"{"id":1}"#;
    let data = HandlerResponse::new(
        StatusCode::OK,
        Bytes::copy_from_slice(body),
        JSON,
        HeaderLines::new(),
    );
    let server = format!("server: {SERVER_HEADER}");
    let want = expected(
        &[
            "HTTP/1.1 200 OK",
            "content-type: application/json",
            &server,
            "content-length: 8",
            "date: <date>",
        ],
        r#"{"id":1}"#,
    );
    assert_eq!(wire(build_response(data)).await, want);
}

/// The handler's own lines go first, in its order; its `content-type` goes out once after them,
/// and its `server` replaces ours.
#[tokio::test]
async fn a_handlers_headers_go_out_in_its_order() {
    let given = [
        (HeaderName::from_static("x-first"), "1"),
        (CONTENT_TYPE, "text/html"),
        (SERVER, "theirs"),
        (HeaderName::from_static("x-first"), "2"),
        (HeaderName::from_static("x-last"), "3"),
    ];
    let mut headers = HeaderLines::new();
    let mut content_type = None;
    for (name, value) in given {
        if let Some(t) = headers.append(name, HeaderValue::from_static(value)) {
            content_type.get_or_insert(t);
        }
    }
    let data = HandlerResponse::new(
        StatusCode::CREATED,
        Bytes::from_static(b"hi"),
        content_type.unwrap_or(TEXT),
        headers,
    );
    let want = expected(
        &[
            "HTTP/1.1 201 Created",
            "x-first: 1",
            "x-first: 2",
            "server: theirs",
            "x-last: 3",
            "content-type: text/html",
            "content-length: 2",
            "date: <date>",
        ],
        "hi",
    );
    assert_eq!(wire(build_response(data)).await, want);
}

#[tokio::test]
async fn a_path_no_route_has_is_a_404() {
    let server = format!("server: {SERVER_HEADER}");
    let want = expected(
        &[
            "HTTP/1.1 404 Not Found",
            "content-type: application/json",
            &server,
            "content-length: 21",
            "date: <date>",
        ],
        r#"{"error":"not found"}"#,
    );
    assert_eq!(wire(reject_response(RouteMiss::NotFound)).await, want);
}

#[tokio::test]
async fn a_method_no_route_has_is_a_405_with_allow() {
    let server = format!("server: {SERVER_HEADER}");
    let want = expected(
        &[
            "HTTP/1.1 405 Method Not Allowed",
            "content-type: application/json",
            &server,
            "allow: GET, HEAD",
            "content-length: 30",
            "date: <date>",
        ],
        r#"{"error":"method not allowed"}"#,
    );
    let miss = RouteMiss::MethodNotAllowed(HeaderValue::from_static("GET, HEAD"));
    assert_eq!(wire(reject_response(miss)).await, want);
}
