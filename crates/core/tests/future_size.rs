//! Awaiting a queued run through the core makes a request's future no larger than awaiting it the
//! way each path does today, through `tokio::time::timeout`. A call on the calling thread is no
//! future at all.

#[path = "support/mod.rs"]
#[expect(
    dead_code,
    reason = "sizes are read off futures never polled, and no call runs inline"
)]
mod support;

use std::future::Ready;
use std::mem::{size_of, size_of_val};

use mipyre_core::ReplyWait;
use mipyre_http::plan::limits::RequestBudget;
use mipyre_microbench::Must;

use support::{through_timeout, through_wait, Body, ReadyBackend, SecondPollBackend};

#[test]
fn the_wait_is_no_larger_than_tokio_timeout() {
    assert!(size_of::<ReplyWait<Ready<Body>, Ready<()>>>() <= size_of::<tokio::time::Timeout<Ready<Body>>>());
}

#[test]
fn a_request_future_awaiting_through_the_core_is_no_larger_than_today() {
    let budget = RequestBudget::from_secs(1.0).must("1 s is a budget");
    for (core, today) in [
        (
            size_of_val(&through_wait(&ReadyBackend, [0; 8], budget)),
            size_of_val(&through_timeout(&ReadyBackend, [0; 8], budget)),
        ),
        (
            size_of_val(&through_wait(&SecondPollBackend, [0; 8], budget)),
            size_of_val(&through_timeout(&SecondPollBackend, [0; 8], budget)),
        ),
    ] {
        assert!(
            core <= today,
            "a request's future awaiting through the core is {core} bytes, today's {today}"
        );
    }
}
