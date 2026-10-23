//! Measures the per-key log decision where a scanner keeps asking for a refused path: a
//! remembered path held back in a full table.
use std::alloc::System;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use mipyre_http::io::static_files::keyed_log_throttle::PerKeyThrottles;
use mipyre_microbench::{measure, run, AllocationCounter, Case};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

const PERIOD: Duration = Duration::from_secs(60);
const KEYS: usize = 256;
const CAPACITY: NonZeroUsize = NonZeroUsize::new(KEYS).expect("256 is not zero");

/// Holds back a remembered path's lines in a full table of 256 paths.
fn per_key_throttles_admit_held(n: u64) {
    let mut throttles: PerKeyThrottles<PathBuf> = PerKeyThrottles::new(PERIOD, CAPACITY);
    let paths: Vec<PathBuf> = (0..KEYS)
        .map(|i| PathBuf::from(format!("/srv/static/private/{i}.html")))
        .collect();
    for path in &paths {
        throttles.admit(path.as_path(), Duration::ZERO);
    }
    let asked = &paths[KEYS / 2];
    measure("per_key_throttles_admit_held", n, |i| {
        black_box(throttles.admit(black_box(asked.as_path()), Duration::from_nanos(i)));
    });
}

fn main() {
    let cases: [Case; 1] = [("per_key_throttles_admit_held", per_key_throttles_admit_held)];
    run(&cases);
}
