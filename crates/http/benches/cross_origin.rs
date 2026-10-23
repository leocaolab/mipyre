//! Measures the site check `preprocess` runs on every request before routing it.
use std::alloc::System;
use std::hint::black_box;

use hyper::header::{HeaderMap, HeaderValue};
use hyper::{Method, Uri};
use mipyre_http::plan::cors::Cors;
use mipyre_http::plan::cross_origin::{site_miss, HostNames};
use mipyre_microbench::{measure, run, AllocationCounter, Case, Must};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

/// Checks one request with `sent` as its headers, on an app whose CORS rule trusts one site.
fn site(name: &str, method: Method, sent: &[(&'static str, &'static str)], names: HostNames, n: u64) {
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
    let uri = Uri::from_static("/json");
    let mut headers = HeaderMap::new();
    for (header, value) in sent {
        headers.insert(*header, HeaderValue::from_static(value));
    }
    measure(name, n, |_| {
        black_box(site_miss(
            black_box(&method),
            false,
            black_box(&uri),
            black_box(&headers),
            black_box(&names),
            black_box(Some(&cors)),
        ));
    });
}

/// Checks a `GET` sent to an IP literal, as the bench client sends every request.
fn get_ip_host(n: u64) {
    site(
        "get_ip_host",
        Method::GET,
        &[("host", "127.0.0.1:8000")],
        HostNames::LOCALHOST,
        n,
    );
}

fn get_localhost(n: u64) {
    site(
        "get_localhost",
        Method::GET,
        &[("host", "localhost:8000")],
        HostNames::LOCALHOST,
        n,
    );
}

/// Checks a `POST` with no `Origin`, as curl, an SDK or another server sends it.
fn post_no_origin(n: u64) {
    let sent = [("host", "127.0.0.1:8000"), ("content-type", "application/json")];
    site("post_no_origin", Method::POST, &sent, HostNames::LOCALHOST, n);
}

/// Checks a `POST` from the server's own page.
fn post_same_origin(n: u64) {
    let sent = [
        ("host", "127.0.0.1:8000"),
        ("content-type", "application/json"),
        ("origin", "http://127.0.0.1:8000"),
        ("sec-fetch-site", "same-origin"),
    ];
    site("post_same_origin", Method::POST, &sent, HostNames::LOCALHOST, n);
}

/// Checks a `POST` from a page of the one site the CORS rule trusts.
fn post_trusted_origin(n: u64) {
    let sent = [
        ("host", "127.0.0.1:8000"),
        ("content-type", "application/json"),
        ("origin", "https://app.example"),
        ("sec-fetch-site", "cross-site"),
    ];
    site("post_trusted_origin", Method::POST, &sent, HostNames::LOCALHOST, n);
}

/// Checks a `GET` where the deployment turned the host check off.
fn any_host(n: u64) {
    site(
        "any_host",
        Method::GET,
        &[("host", "api.example.com")],
        HostNames::Any,
        n,
    );
}

fn main() {
    let cases: [Case; 6] = [
        ("get_ip_host", get_ip_host),
        ("get_localhost", get_localhost),
        ("post_no_origin", post_no_origin),
        ("post_same_origin", post_same_origin),
        ("post_trusted_origin", post_trusted_origin),
        ("any_host", any_host),
    ];
    run(&cases);
}
