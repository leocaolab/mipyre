//! Measures what one [`HostCpus::read`] costs; `BENCH_N` sets the number of calls.

use std::hint::black_box;
use std::time::Instant;

use mipyre_cpu::{HostCpus, Quota};
use mipyre_microbench::calls;

const DEFAULT_N: u64 = 1000;

fn main() {
    let n = calls(DEFAULT_N);
    let started = Instant::now();
    for _ in 0..n {
        black_box(HostCpus::read(black_box(Quota::Unlimited)));
    }
    let us = started.elapsed().as_secs_f64() * 1e6 / n.max(1) as f64;
    println!("host_cpus_read: {us:.2} µs/call ({n} calls)");
}
