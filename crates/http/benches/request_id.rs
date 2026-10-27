//! Measures taking a client's request id, cloning it, and writing a minted one.
use std::alloc::System;
use std::fmt::Write as _;
use std::hint::black_box;

use bytes::Bytes;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use mipyre_http::plan::request_id::RequestId;
use mipyre_microbench::{measure, run, AllocationCounter, Case, Must};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

const HEADER: HeaderName = HeaderName::from_static("x-request-id");

fn client_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    // Hyper hands header values over as shared slices of its read buffer, so the bench does too.
    let value = HeaderValue::from_maybe_shared(Bytes::copy_from_slice(b"4bf92f3577b34da6a3ce929d0e0e4736"))
        .must("a valid header value");
    headers.insert(HEADER, value);
    headers
}

fn request_id_of_client(n: u64) {
    let headers = client_headers();
    measure("request_id_of_client", n, |_| {
        black_box(RequestId::of(black_box(&headers), Some(&HEADER)));
    });
}

/// Clones the client's id, as a response header and a log line each hold one.
fn request_id_clone_client(n: u64) {
    let id = RequestId::of(&client_headers(), Some(&HEADER));
    measure("request_id_clone_client", n, |_| {
        black_box(black_box(&id).clone());
    });
}

/// Writes a minted id into a reused buffer, as an error body or a log line does.
fn request_id_write_minted(n: u64) {
    let id = RequestId::mint();
    let mut out = String::with_capacity(64);
    measure("request_id_write_minted", n, |_| {
        out.clear();
        let written = write!(out, "{}", black_box(&id));
        black_box((&out, written.is_ok()));
    });
}

/// Writes a minted id as a response header's value, as every response does once the app names a
/// request-id header.
fn request_id_header_minted(n: u64) {
    let id = RequestId::mint();
    measure("request_id_header_minted", n, |_| {
        black_box(black_box(&id).header_value());
    });
}

fn main() {
    let cases: [Case; 4] = [
        ("request_id_of_client", request_id_of_client),
        ("request_id_clone_client", request_id_clone_client),
        ("request_id_write_minted", request_id_write_minted),
        ("request_id_header_minted", request_id_header_minted),
    ];
    run(&cases);
}
