//! Measures what running a request through the core costs over the code each path runs today:
//! `wait_*` awaits a queued run through `dispatch`, against `timeout_*` through
//! `tokio::time::timeout` as `run_on_pool` does; `timed_inline` runs the handler on the calling
//! thread between the core's `InlineTimer::start` and `overran`, against `direct_inline`, which
//! reads the clock in the request's own code. No case may count more than its counterpart.
//!
//! `cargo bench -p mipyre_core --bench reply_wait [CASE]` prints ns and allocations per call over
//! `BENCH_N` calls.

#[path = "../tests/support/mod.rs"]
mod support;

use mipyre_core::InlineTimer;
use mipyre_http::plan::limits::RequestBudget;
use mipyre_microbench::{measure, run, AllocationCounter, Case, Must};
use std::alloc::System;
use std::hint::black_box;
use std::time::Duration;
use support::{poll_to_ready, serve_inline, through_timeout, through_wait, ReadyBackend, SecondPollBackend};
use tokio::time::Instant;

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

fn main() {
    let cases: [Case; 6] = [
        ("wait_ready", wait_ready),
        ("timeout_ready", timeout_ready),
        ("wait_second_poll", wait_second_poll),
        ("timeout_second_poll", timeout_second_poll),
        ("timed_inline", timed_inline),
        ("direct_inline", direct_inline),
    ];
    // A running clock makes the count follow timing: a deadline becomes a wheel tick, and a call's
    // time a duration, by std's subtraction, which takes a borrow branch once the run crosses a whole
    // second. Paused, every call reads the same time, and both sides of a pair pay the same. The
    // frozen time then sits on a whole second: tokio rounds a deadline up by 999,999 ns, which
    // carries into the seconds when the time is that close below one, about one run in a thousand.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .must("a current-thread runtime with a timer builds outside any runtime");
    let _entered = runtime.enter();
    let past_second = subsec_nanos(Instant::now()).must("std's Instant prints its tv_nsec on unix");
    runtime.block_on(tokio::time::advance(Duration::from_nanos(NANOS_PER_SEC - past_second)));
    run(&cases);
}

const NANOS_PER_SEC: u64 = 1_000_000_000;

/// Reads the part of `at` past its whole second, which std does not expose but prints.
fn subsec_nanos(at: Instant) -> Option<u64> {
    let printed = format!("{:?}", at.into_std());
    let digits = printed.split("tv_nsec: ").nth(1)?;
    let end = digits.find(|c: char| !c.is_ascii_digit()).unwrap_or(digits.len());
    digits[..end].parse().ok()
}

fn budget() -> RequestBudget {
    RequestBudget::from_secs(30.0).must("30 s is a budget")
}

fn wait_ready(n: u64) {
    let budget = budget();
    measure("wait_ready", n, |i| {
        let reply = poll_to_ready(through_wait(black_box(&ReadyBackend), black_box([i; 8]), budget));
        assert!(matches!(black_box(reply), Ok(Some(_))));
    });
}

fn timeout_ready(n: u64) {
    let budget = budget();
    measure("timeout_ready", n, |i| {
        let reply = poll_to_ready(through_timeout(black_box(&ReadyBackend), black_box([i; 8]), budget));
        assert!(matches!(black_box(reply), Ok(Some(_))));
    });
}

fn wait_second_poll(n: u64) {
    let budget = budget();
    measure("wait_second_poll", n, |i| {
        let reply = poll_to_ready(through_wait(black_box(&SecondPollBackend), black_box([i; 8]), budget));
        assert!(matches!(black_box(reply), Ok(Some(_))));
    });
}

fn timeout_second_poll(n: u64) {
    let budget = budget();
    measure("timeout_second_poll", n, |i| {
        let reply = poll_to_ready(through_timeout(
            black_box(&SecondPollBackend),
            black_box([i; 8]),
            budget,
        ));
        assert!(matches!(black_box(reply), Ok(Some(_))));
    });
}

fn timed_inline(n: u64) {
    let budget = budget();
    measure("timed_inline", n, |i| {
        let timer = InlineTimer::start();
        let reply = serve_inline(black_box([i; 8]));
        let overran = timer.overran(budget);
        assert!(matches!(black_box((reply, overran)), (Ok([b, ..]), None) if b == i));
    });
}

fn direct_inline(n: u64) {
    let budget = budget();
    measure("direct_inline", n, |i| {
        let called = Instant::now();
        let reply = serve_inline(black_box([i; 8]));
        let took = called.elapsed();
        let overran = (took > budget.get()).then_some(took);
        assert!(matches!(black_box((reply, overran)), (Ok([b, ..]), None) if b == i));
    });
}
