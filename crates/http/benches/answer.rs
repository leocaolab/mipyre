//! Measures sorting an answer into its method kind, status class and duration bucket, as the
//! engine does for every response it counts.
use std::alloc::System;
use std::hint::black_box;

use hyper::{Method, StatusCode};
use mipyre_http::plan::answer::{duration_bucket, MethodKind, StatusClass};
use mipyre_microbench::{measure, run, AllocationCounter, Case, Must};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

fn method_kind_get(n: u64) {
    let method = Method::GET;
    measure("method_kind_get", n, |_| {
        black_box(MethodKind::of(black_box(&method)));
    });
}

/// Sorts a method no kind names, which is matched against every standard one first.
fn method_kind_other(n: u64) {
    let method = Method::from_bytes(b"PROPFIND").must("a valid method");
    measure("method_kind_other", n, |_| {
        black_box(MethodKind::of(black_box(&method)));
    });
}

fn status_class_of(n: u64) {
    measure("status_class_of", n, |_| {
        black_box(StatusClass::of(black_box(StatusCode::OK)));
    });
}

/// Buckets 300 µs, a common duration, which takes the search past the first bounds.
fn duration_bucket_300us(n: u64) {
    measure("duration_bucket_300us", n, |_| {
        black_box(duration_bucket(black_box(300)));
    });
}

fn main() {
    let cases: [Case; 4] = [
        ("method_kind_get", method_kind_get),
        ("method_kind_other", method_kind_other),
        ("status_class_of", status_class_of),
        ("duration_bucket_300us", duration_bucket_300us),
    ];
    run(&cases);
}
