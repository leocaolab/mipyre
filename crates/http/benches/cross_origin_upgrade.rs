//! Measures the site check `handle_websocket` runs on every WebSocket upgrade. It is a bench of
//! its own so that `cross_origin`'s binary, whose one call is `preprocess`'s, keeps its code.
use std::alloc::System;
use std::hint::black_box;

use hyper::header::{HeaderMap, HeaderValue};
use hyper::{Method, Uri};
use mipyre_http::plan::cors::Cors;
use mipyre_http::plan::cross_origin::{site_miss, HostNames};
use mipyre_microbench::{measure, run, AllocationCounter, Case, Must};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

/// Checks a WebSocket opening handshake to an IP literal with `origin` as its extra headers, on
/// an app whose CORS rule trusts one site.
fn upgrade(name: &str, origin: &[(&'static str, &'static str)], n: u64) {
    let cors = Cors::parse(
        &["https://app.example".to_string()],
        "GET, POST",
        "content-type",
        None,
        false,
        None,
    )
    .must("valid CORS settings")
    .own_copy();
    let uri = Uri::from_static("/ws");
    let handshake = [
        ("host", "127.0.0.1:8000"),
        ("upgrade", "websocket"),
        ("connection", "Upgrade"),
        ("sec-websocket-version", "13"),
        ("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="),
    ];
    let mut headers = HeaderMap::new();
    for (header, value) in handshake.iter().chain(origin) {
        headers.insert(*header, HeaderValue::from_static(value));
    }
    let names = HostNames::LOCALHOST;
    measure(name, n, |_| {
        black_box(site_miss(
            black_box(&Method::GET),
            true,
            black_box(&uri),
            black_box(&headers),
            black_box(&names),
            black_box(Some(&cors)),
        ));
    });
}

/// Checks an upgrade with no `Origin`, as a client that is no browser sends it.
fn no_origin(n: u64) {
    upgrade("no_origin", &[], n);
}

/// Checks an upgrade from the server's own page.
fn same_origin(n: u64) {
    upgrade(
        "same_origin",
        &[("origin", "http://127.0.0.1:8000"), ("sec-fetch-site", "same-origin")],
        n,
    );
}

/// Checks an upgrade from a page of the one site the CORS rule trusts.
fn trusted_origin(n: u64) {
    upgrade(
        "trusted_origin",
        &[("origin", "https://app.example"), ("sec-fetch-site", "cross-site")],
        n,
    );
}

fn main() {
    let cases: [Case; 3] = [
        ("no_origin", no_origin),
        ("same_origin", same_origin),
        ("trusted_origin", trusted_origin),
    ];
    run(&cases);
}
