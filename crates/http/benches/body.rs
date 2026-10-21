//! Measures reading a request body, streamed frame by frame or already whole.
// The package's clippy.toml is plan/'s list; a bench measures the world through io/.
#![expect(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::disallowed_macros,
    reason = "a bench reads clocks, opens sockets and prints"
)]

use std::alloc::System;
use std::future::Future;
use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use mipyre_microbench::{measure, run, AllocationCounter, Case, Must, ALLOCATIONS};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

/// Feeds a streamed body to a reader that keeps up; the cost per frame includes the feed's timer.
fn stream_feed_per_frame(n: u64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .must("build the runtime");
    let frames = (0..n).map(|_| Ok::<_, hyper::Error>(hyper::body::Frame::data(Bytes::from_static(b"x"))));
    let body = http_body_util::StreamBody::new(tokio_stream::iter(frames));
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let started = Instant::now();
    let received = rt.block_on(async move {
        let (tx, mut rx) = mipyre_http::io::body::body_channel();
        let feeder = tokio::spawn(mipyre_http::io::body::stream_body_feeder(
            body,
            tx,
            usize::MAX,
            mipyre_http::plan::limits::BodyTimeAllowance::absolute(mipyre_http::plan::limits::RequestBudget::from_secs(3600.0).must("an hour is a budget")),
        ));
        let mut chunks: u64 = 0;
        while let Some(message) = rx.recv().await {
            if let mipyre_http::io::body::BodyMessage::Chunk(_) = message {
                chunks += 1;
            }
        }
        feeder.await.must("the feeder task");
        chunks
    });
    let elapsed = started.elapsed();
    let allocated = ALLOCATIONS.load(Ordering::Relaxed) - allocations;
    assert_eq!(received, n, "every frame arrives");
    println!(
        "stream_feed_per_frame: {:.2} ns/frame, ALLOCATIONS_PER_FRAME={:.3} ({n} frames)",
        elapsed.as_nanos() as f64 / n.max(1) as f64,
        allocated as f64 / n.max(1) as f64,
    );
}

/// Reads a body that came in with the head as one frame, as hyper hands it over, under the app's
/// limits the way the handler paths call `read_body`.
fn read_body_ready(n: u64) {
    let limits = black_box(mipyre_http::plan::limits::Limits::DEFAULT);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    measure("read_body_ready", n, |_| {
        let body = http_body_util::BodyExt::map_err(
            http_body_util::Full::new(Bytes::from_static(b"{\"a\":1}")),
            |never| match never {},
        );
        let read = std::pin::pin!(mipyre_http::io::body::read_body(body, limits.max_body.as_usize(), || limits.body_time()));
        assert!(matches!(black_box(read.poll(&mut cx)), Poll::Ready(Ok(_))));
    });
}

fn main() {
    let cases: [Case; 2] = [
        ("stream_feed_per_frame", stream_feed_per_frame),
        ("read_body_ready", read_body_ready),
    ];
    run(&cases);
}
