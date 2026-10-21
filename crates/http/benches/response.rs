//! Measures building the responses the engine sends, with and without CORS headers; the bodies the
//! server answers by itself are measured in `error_body`.
// The package's clippy.toml is plan/'s list; a bench measures the world through io/.
#![expect(
    clippy::disallowed_macros,
    reason = "a bench prints what it measured"
)]

use std::alloc::System;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use hyper::StatusCode;
use mipyre_http::plan::cors::Cors;
use mipyre_http::plan::reject::RouteMiss;
use mipyre_http::plan::response::{build_response, reject_response, HandlerResponse, HeaderLines, JSON};
use mipyre_http::plan::route::FastResponse;
use mipyre_microbench::{measure, run, AllocationCounter, Case, Must, ALLOCATIONS};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

/// Serves a thread's own copy of a pre-built response, as the fast path does per request.
fn fast_to_response(n: u64) {
    let fast = FastResponse::parse(
        Bytes::from(b"ok".to_vec()),
        "text/plain",
        200,
        &HashMap::new(),
    )
    .must("a valid fast response")
    .own_copy();
    measure("fast_to_response", n, |_| {
        black_box(black_box(&fast).to_response());
    });
}

/// Builds the response for a handler's dict; the body is a fresh allocation each call, as a
/// serialized dict is.
fn build_response_json(n: u64) {
    const BODY: &[u8] = br#"{"id":1,"name":"mipyre","tags":["a","b","c"],"price":12.5,"ok":true}"#;
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let body = Bytes::copy_from_slice(BODY);
    let per_body = ALLOCATIONS.load(Ordering::Relaxed) - allocations;
    drop(body);
    println!("build_response_json: the body itself is {per_body} allocation(s) of each call");
    measure("build_response_json", n, |_| {
        let data = HandlerResponse::new(
            StatusCode::OK,
            Bytes::copy_from_slice(black_box(BODY)),
            JSON,
            HeaderLines::new(),
        );
        black_box(build_response(black_box(data)));
    });
}

/// Builds a response with three header lines the handler gave, parsed once and cloned per request
/// as `exchange` clones them; with `with_type` the middle line is `content-type`, which
/// `HeaderLines` hands back as the type.
fn build_response_handler_headers(n: u64, name: &str, with_type: bool) {
    let middle = if with_type {
        (hyper::header::CONTENT_TYPE, HeaderValue::from_static("text/html"))
    } else {
        (HeaderName::from_static("x-middle"), HeaderValue::from_static("2"))
    };
    let given = [
        (HeaderName::from_static("x-first"), HeaderValue::from_static("1")),
        middle,
        (HeaderName::from_static("x-last"), HeaderValue::from_static("3")),
    ];
    let mut headers = HeaderLines::with_capacity(given.len());
    let mut content_type = None;
    for (name, value) in given {
        if let Some(t) = headers.append(name, value) {
            content_type = Some(t);
        }
    }
    measure(name, n, |_| {
        let data = HandlerResponse::new(
            StatusCode::OK,
            Bytes::from_static(b"hi"),
            black_box(&content_type).clone().unwrap_or(JSON),
            black_box(&headers).clone(),
        );
        black_box(build_response(black_box(data)));
    });
}

fn build_response_headers(n: u64) {
    build_response_handler_headers(n, "build_response_headers", false);
}

fn build_response_headers_typed(n: u64) {
    build_response_handler_headers(n, "build_response_headers_typed", true);
}

fn not_found(n: u64) {
    measure("not_found", n, |_| {
        black_box(reject_response(black_box(RouteMiss::NotFound)));
    });
}

/// Returns a thread's own copy of CORS settings that add three headers.
fn cors() -> Cors {
    Cors::parse("*", "GET, POST", "content-type", None, false, None)
        .must("valid CORS settings")
        .own_copy()
}

/// Adds the CORS headers to `build_response_json`'s response, as the engine's `finish` does to every
/// response, with the origin it allows decided from the request's headers as `preprocess` does.
fn build_response_json_cors(n: u64) {
    const BODY: &[u8] = br#"{"id":1,"name":"mipyre","tags":["a","b","c"],"price":12.5,"ok":true}"#;
    let cors = cors();
    let request = HeaderMap::new();
    measure("build_response_json_cors", n, |_| {
        let data = HandlerResponse::new(
            StatusCode::OK,
            Bytes::copy_from_slice(black_box(BODY)),
            JSON,
            HeaderLines::new(),
        );
        let mut resp = build_response(black_box(data));
        cors.apply(resp.headers_mut(), cors.origin_of(black_box(&request)));
        black_box(resp);
    });
}

/// `build_response_json_cors` on an app that allows several origins, for a request from the last
/// of them: the request's `Origin` is looked up and compared with each, and `Vary` is added.
fn build_response_json_cors_several(n: u64) {
    const BODY: &[u8] = br#"{"id":1,"name":"mipyre","tags":["a","b","c"],"price":12.5,"ok":true}"#;
    let cors = Cors::parse(
        "https://a.example, https://b.example, https://app.example",
        "GET, POST",
        "content-type",
        None,
        false,
        None,
    )
    .must("valid CORS settings")
    .own_copy();
    let mut request = HeaderMap::new();
    request.insert(hyper::header::ORIGIN, HeaderValue::from_static("https://app.example"));
    measure("build_response_json_cors_several", n, |_| {
        let data = HandlerResponse::new(
            StatusCode::OK,
            Bytes::copy_from_slice(black_box(BODY)),
            JSON,
            HeaderLines::new(),
        );
        let mut resp = build_response(black_box(data));
        cors.apply(resp.headers_mut(), cors.origin_of(black_box(&request)));
        black_box(resp);
    });
}

fn not_found_cors(n: u64) {
    let cors = cors();
    let request = HeaderMap::new();
    measure("not_found_cors", n, |_| {
        let mut resp = reject_response(black_box(RouteMiss::NotFound));
        cors.apply(resp.headers_mut(), cors.origin_of(black_box(&request)));
        black_box(resp);
    });
}

fn main() {
    let cases: [Case; 8] = [
        ("fast_to_response", fast_to_response),
        ("build_response_json", build_response_json),
        ("not_found", not_found),
        ("build_response_json_cors", build_response_json_cors),
        ("build_response_json_cors_several", build_response_json_cors_several),
        ("not_found_cors", not_found_cors),
        ("build_response_headers", build_response_headers),
        ("build_response_headers_typed", build_response_headers_typed),
    ];
    run(&cases);
}
