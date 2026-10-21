//! Measures `tcp::events::count` from 1, 2 and 4 threads at once, each counting its own kind, to
//! show what the counters' cache-line layout costs when events come from several cores together.
//!
//! `degrade` is not measured here: it reads the clock, so its count would follow the clock. Its two
//! halves are measured with nothing timed in the loop: the count here, and the throttle with the
//! time handed in by `mipyre_util`'s `throttle` bench (`log_throttle_admit_held` / `_open`).

use std::alloc::System;
use std::hint::black_box;
use std::time::Instant;

use mipyre_microbench::{run, AllocationCounter, Case};
use mipyre_transport::tcp::events::{self, TcpEvent};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

fn tcp_event_count(n: u64) {
    const KINDS: [TcpEvent; 4] = [
        TcpEvent::PeerGoneInQueue,
        TcpEvent::AcceptFailed,
        TcpEvent::QuickackFailed,
        TcpEvent::NodelayFailed,
    ];
    for threads in [1, 2, 4] {
        let started = Instant::now();
        std::thread::scope(|scope| {
            for kind in KINDS.iter().take(threads) {
                scope.spawn(move || {
                    for _ in 0..n {
                        events::count(black_box(*kind));
                    }
                });
            }
        });
        let ns = started.elapsed().as_nanos() as f64 / n.max(1) as f64;
        println!("tcp_event_count: {threads} threads, {ns:.2} ns/call per thread ({n} calls each)");
    }
}

fn main() {
    let cases: [Case; 1] = [
        ("tcp_event_count", tcp_event_count),
    ];
    run(&cases);
}
