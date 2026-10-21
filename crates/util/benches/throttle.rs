//! Measures the log throttle where a flood keeps calling it: a line held back and a line let out.
use std::alloc::System;
use std::hint::black_box;
use std::time::Duration;

use mipyre_microbench::{measure, run, AllocationCounter, Case};
use mipyre_util::throttle::LogThrottle;

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

const PERIOD: Duration = Duration::from_secs(60);

/// Holds every line back after the first, as under a sustained overload.
fn log_throttle_admit_held(n: u64) {
    let throttle = LogThrottle::new(PERIOD);
    throttle.admit(Duration::ZERO);
    measure("log_throttle_admit_held", n, |i| {
        black_box(throttle.admit(black_box(Duration::from_nanos(i))));
    });
}

/// Lets every line out, each a period after the last.
fn log_throttle_admit_open(n: u64) {
    let throttle = LogThrottle::new(PERIOD);
    measure("log_throttle_admit_open", n, |i| {
        black_box(throttle.admit(black_box(PERIOD * (i as u32))));
    });
}

fn main() {
    let cases: [Case; 2] = [
        ("log_throttle_admit_held", log_throttle_admit_held),
        ("log_throttle_admit_open", log_throttle_admit_open),
    ];
    run(&cases);
}
