//! Running a request through the core allocates nothing: a queued run awaited through it, ready at
//! once or at its second poll, and a call on the calling thread timed by it. The backends here allocate
//! nothing, so every allocation counted would be the core's.
//!
//! One test in its own binary, so no other test allocates while it counts.

#[path = "support/mod.rs"]
#[expect(
    dead_code,
    reason = "today's timeout is the bench's and the size test's comparison, not a path counted here"
)]
mod support;

use std::alloc::System;
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Waker};

use mipyre_core::InlineTimer;
use mipyre_http::plan::limits::RequestBudget;
use mipyre_microbench::{allocations_counted, AllocationCounter, Must, ALLOCATIONS};

use support::{poll_to_ready, serve_inline, through_wait, ReadyBackend, SecondPollBackend};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

#[test]
fn running_a_request_through_the_core_allocates_nothing() {
    assert!(allocations_counted(), "the counting allocator is the global one");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .must("a current-thread runtime with a timer builds");
    let _entered = runtime.enter();
    let budget = RequestBudget::from_secs(1.0).must("1 s is a budget");

    // The runtime's timer allocates once, at the first deadline it registers, whoever sets it.
    let warm_up = pin!(tokio::time::sleep(budget.get()));
    assert!(warm_up.poll(&mut Context::from_waker(Waker::noop())).is_pending());
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let ready_reply = poll_to_ready(through_wait(&ReadyBackend, [7; 8], budget));
    let second_poll_reply = poll_to_ready(through_wait(&SecondPollBackend, [7; 8], budget));
    let timer = InlineTimer::start();
    let inline_reply = serve_inline([7; 8]);
    let overran = timer.overran(budget);
    let allocated = ALLOCATIONS.load(Ordering::Relaxed) - before;

    assert!(matches!(ready_reply, Ok(Some([7, ..]))));
    assert!(matches!(second_poll_reply, Ok(Some([7, ..]))));
    assert!(matches!((inline_reply, overran), (Ok([7, ..]), None)));
    assert_eq!(allocated, 0, "running a request through the core allocated");
}
