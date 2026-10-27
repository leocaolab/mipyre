//! Measures the 503 an overloaded server sends to every request it turns away, which is made most
//! often when the server has least to spare.
//!
//! It is a binary of its own, not a case of `response`, because every bench binary is built with
//! LTO and a case that calls the error bodies changes how the compiler inlines the code the
//! `response` cases share, which moved their counts by 17–127 instructions.
use std::alloc::System;
use std::hint::black_box;

use mipyre_http::plan::request_id::RequestId;
use mipyre_http::plan::response;
use mipyre_microbench::{measure, run, AllocationCounter, Case};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

fn overloaded_response(n: u64) {
    let id = RequestId::mint();
    measure("overloaded_response", n, |_| {
        black_box(response::overloaded_response(
            black_box("server overloaded"),
            black_box(&id),
        ));
    });
}

fn main() {
    let cases: [Case; 1] = [("overloaded_response", overloaded_response)];
    run(&cases);
}
