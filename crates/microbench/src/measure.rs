//! Runs a call in a loop and prints its time and allocations per call.

use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::counting::ALLOCATIONS;

pub fn measure(name: &str, n: u64, mut body: impl FnMut(u64)) {
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let started = Instant::now();
    for i in 0..n {
        body(i);
    }
    let elapsed = started.elapsed();
    let allocated = ALLOCATIONS.load(Ordering::Relaxed) - allocations;
    let per = |x: f64| if n == 0 { 0.0 } else { x / n as f64 };
    println!(
        "{name}: {:.2} ns/call, ALLOCATIONS_PER_CALL={:.3} ({n} calls)",
        per(elapsed.as_nanos() as f64),
        per(allocated as f64),
    );
}
