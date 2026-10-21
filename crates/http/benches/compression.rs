//! Measures deciding whether and how to compress a response, and compressing JSON bodies of 1 to
//! 64 KiB with gzip-6 and brotli-4.
use std::alloc::System;
use std::hint::black_box;

use bytes::Bytes;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use mipyre_http::plan::compression::{self, CompressionSettings};
use mipyre_http::plan::response::{HandlerResponse, HeaderLines};
use mipyre_microbench::{measure, run, AllocationCounter, Case, Must};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

/// Returns a browser's request headers as shared bytes, as hyper hands them over.
fn browser_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("host", "127.0.0.1:8000"),
        ("user-agent", "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0"),
        ("accept", "application/json"),
        ("accept-language", "en-US,en;q=0.5"),
        ("accept-encoding", "gzip, deflate, br, zstd"),
        ("connection", "keep-alive"),
    ] {
        let value = HeaderValue::from_maybe_shared(Bytes::copy_from_slice(value.as_bytes()))
            .must("a valid header value");
        headers.insert(HeaderName::from_static(name), value);
    }
    headers
}

fn compression_settings() -> CompressionSettings {
    CompressionSettings::parse(1024, true, true, 6, 4).must("valid compression settings")
}

/// Decides whether a response is compressed, as every handler request does: the request keeps a
/// refcounted copy of its `Accept-Encoding` while the handler runs, and the gate before
/// `compression::plan` reads it once the response is built.
#[inline(never)]
fn response_coding(headers: &HeaderMap, settings: Option<&CompressionSettings>) -> bool {
    let kept = compression::accept_encoding(headers, settings).map(|(v, s)| (v.clone(), s));
    // The kept value waits in the request's future while the handler runs.
    let kept = black_box(kept);
    let gate = kept
        .as_ref()
        .and_then(|(value, settings)| Some((value.to_str().ok()?, *settings)));
    black_box(gate).is_some()
}

/// Measures what every handler request pays when the app does not compress.
fn response_coding_off(n: u64) {
    let headers = browser_headers();
    measure("response_coding_off", n, |_| {
        black_box(response_coding(black_box(&headers), black_box(None)));
    });
}

fn response_coding_on(n: u64) {
    let headers = browser_headers();
    let settings = compression_settings();
    measure("response_coding_on", n, |_| {
        black_box(response_coding(black_box(&headers), black_box(Some(&settings))));
    });
}

fn response_coding_on_absent(n: u64) {
    let mut headers = browser_headers();
    headers.remove(hyper::header::ACCEPT_ENCODING);
    let settings = compression_settings();
    measure("response_coding_on_absent", n, |_| {
        black_box(response_coding(black_box(&headers), black_box(Some(&settings))));
    });
}

/// Decides how to compress a 4 KiB JSON body for a browser that accepts brotli.
fn compress_plan_json(n: u64) {
    let data = HandlerResponse::new(
        hyper::StatusCode::OK,
        Bytes::from(vec![b'x'; 4096]),
        HeaderValue::from_static("application/json"),
        HeaderLines::new(),
    );
    let settings = compression_settings();
    measure("compress_plan_json", n, |_| {
        black_box(compression::plan(
            black_box(&data),
            black_box("gzip, deflate, br, zstd"),
            black_box(&settings),
        ));
    });
}

/// Returns a handler's JSON answer of `items` records; 13 records are about 1 KiB, 64 about 4.8 KiB
/// (the `json4k` path's body), 211 about 16 KiB and 843 about 64 KiB.
fn json_body(items: u32) -> Vec<u8> {
    let mut body = String::from("{\"items\":[");
    for id in 0..items {
        if id > 0 {
            body.push(',');
        }
        body.push_str(&format!(
            "{{\"id\":{id},\"name\":\"mipyre\",\"tags\":[\"a\",\"b\",\"c\"],\"price\":12.5,\"ok\":true}}"
        ));
    }
    body.push_str("]}");
    body.into_bytes()
}

/// Compresses the body into a buffer reused from call to call, as the engine's per-thread pool
/// lends it, and gzips through a kept stream, as each engine thread does.
fn encode_json(name: &str, n: u64, codec: compression::Codec, items: u32) {
    let body = json_body(items);
    let mut out = Vec::with_capacity(body.len());
    let mut gzip = compression::GzipStream::default();
    measure(name, n, |_| {
        out.clear();
        match black_box(codec) {
            compression::Codec::Gzip(level) => gzip.encode(black_box(&body), level, &mut out),
            compression::Codec::Brotli(quality) => compression::brotli(black_box(&body), quality, &mut out),
        }
        .must("encodes");
        black_box(&out);
    });
}

fn gzip_6() -> compression::Codec {
    compression::Codec::Gzip(compression::GzipLevel::new(6).must("a gzip level"))
}

fn brotli_4() -> compression::Codec {
    compression::Codec::Brotli(compression::BrotliQuality::new(4).must("a brotli quality"))
}

fn encode_gzip_1k(n: u64) {
    encode_json("encode_gzip_1k", n, gzip_6(), 13);
}

fn encode_gzip_4k8(n: u64) {
    encode_json("encode_gzip_4k8", n, gzip_6(), 64);
}

fn encode_gzip_16k(n: u64) {
    encode_json("encode_gzip_16k", n, gzip_6(), 211);
}

fn encode_gzip_64k(n: u64) {
    encode_json("encode_gzip_64k", n, gzip_6(), 843);
}

fn encode_br_1k(n: u64) {
    encode_json("encode_br_1k", n, brotli_4(), 13);
}

fn encode_br_4k8(n: u64) {
    encode_json("encode_br_4k8", n, brotli_4(), 64);
}

fn encode_br_16k(n: u64) {
    encode_json("encode_br_16k", n, brotli_4(), 211);
}

fn encode_br_64k(n: u64) {
    encode_json("encode_br_64k", n, brotli_4(), 843);
}

fn main() {
    let cases: [Case; 12] = [
        ("response_coding_off", response_coding_off),
        ("response_coding_on", response_coding_on),
        ("response_coding_on_absent", response_coding_on_absent),
        ("compress_plan_json", compress_plan_json),
        ("encode_gzip_1k", encode_gzip_1k),
        ("encode_gzip_4k8", encode_gzip_4k8),
        ("encode_gzip_16k", encode_gzip_16k),
        ("encode_gzip_64k", encode_gzip_64k),
        ("encode_br_1k", encode_br_1k),
        ("encode_br_4k8", encode_br_4k8),
        ("encode_br_16k", encode_br_16k),
        ("encode_br_64k", encode_br_64k),
    ];
    run(&cases);
}
