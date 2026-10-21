//! Counts the instructions of `GcSchedule::after_unit` when no collection is due.
//!
//! The merge gate runs it through `benchmarks/bench_micro_count.py`. By hand:
//!
//!   BENCH_N=2000000 cargo bench -p mipyre_subinterp --bench gc -- gc_after_unit

use std::alloc::System;
use std::hint::black_box;

use mipyre_microbench::{run, AllocationCounter, Case};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

fn gc_after_unit(n: u64) {
    let mut schedule = mipyre_subinterp::gc::GcSchedule::new(mipyre_subinterp::gc::GcPolicy::Count {
        every: std::num::NonZeroU64::MAX,
    });
    for _ in 0..n {
        black_box(black_box(&mut schedule).after_unit());
    }
}

fn main() {
    let cases: [Case; 1] = [("gc_after_unit", gc_after_unit)];
    run(&cases);
}
