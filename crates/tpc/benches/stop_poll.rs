//! Measures a connection's stop waiter: `stop_poll` is the poll of a registered `StopWaiter`
//! whose stop has not come, which a connection's `select!` makes on every wake; `waiter_life` is
//! one waiter's whole life, its first pending poll (which takes a waker slot) and its drop (which
//! gives the slot back), which every connection goes through once.
//!
//! `cargo bench -p mipyre_tpc --bench stop_poll [CASE]` prints ns per call over `BENCH_N` calls.

use std::alloc::System;
use std::future::Future;
use std::task::{Context, Waker};
use std::time::Instant;

use mipyre_microbench::{run, AllocationCounter, Case};
use mipyre_tpc::LocalStop;

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

fn main() {
    let cases: [Case; 2] = [("stop_poll", stop_poll), ("waiter_life", waiter_life)];
    run(&cases);
}

fn stop_poll(n: u64) {
    let start = Instant::now();
    registered_polls(n);
    report("stop_poll", n, start);
}

fn waiter_life(n: u64) {
    let start = Instant::now();
    waiter_lives(n);
    report("waiter_life", n, start);
}

fn report(case: &str, n: u64, start: Instant) {
    let ns = start.elapsed().as_nanos() as f64 / n.max(1) as f64;
    println!("{case}: {ns:.2} ns/call ({n} calls)");
}

fn registered_polls(n: u64) {
    let stop = LocalStop::never();
    let mut cx = Context::from_waker(Waker::noop());
    let mut waiter = std::pin::pin!(stop.stopped());
    // The first poll registers the waker; the loop measures the polls after it.
    assert!(waiter.as_mut().poll(&mut cx).is_pending());
    let mut pending: u64 = 0;
    for _ in 0..n {
        let polled = std::hint::black_box(waiter.as_mut().poll(&mut cx));
        pending += u64::from(polled.is_pending());
    }
    assert_eq!(pending, n, "every poll of a live waiter is pending");
}

/// Keeps one other waiter registered throughout, as a thread with open connections does, so a
/// waiter's slot comes from and goes back to a pool that is not empty.
fn waiter_lives(n: u64) {
    let stop = LocalStop::never();
    let mut cx = Context::from_waker(Waker::noop());
    let mut held = std::pin::pin!(stop.stopped());
    assert!(held.as_mut().poll(&mut cx).is_pending());
    let mut pending: u64 = 0;
    for _ in 0..n {
        let mut waiter = std::pin::pin!(std::hint::black_box(stop.stopped()));
        pending += u64::from(waiter.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(pending, n, "every first poll of a live waiter is pending");
}
