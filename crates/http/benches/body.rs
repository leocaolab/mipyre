//! Measures reading a request body, streamed frame by frame or already whole.

use std::alloc::System;
use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use mipyre_microbench::{measure, run, AllocationCounter, Case, Must};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

/// Hands one frame per poll, then has none until polled again, as a socket read that drained the
/// kernel's buffer.
struct FramePerPoll {
    ready: bool,
}

impl hyper::body::Body for FramePerPoll {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, hyper::Error>>> {
        self.ready = !self.ready;
        if self.ready {
            Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_static(b"x")))))
        } else {
            Poll::Pending
        }
    }
}

/// Feeds a streamed body to a reader that takes each chunk before the next frame comes, so the
/// feeder never waits for room: a frame costs its poll, the hand-off and the poll of the idle
/// timer. The feeder is polled by hand, so the runtime's time driver never turns.
fn stream_feed_per_frame(n: u64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .must("build the runtime");
    let _entered = rt.enter();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let (tx, mut rx) = mipyre_http::io::body::body_channel();
    let mut feeder = std::pin::pin!(mipyre_http::io::body::stream_body_feeder(
        FramePerPoll { ready: false },
        tx,
        usize::MAX,
        mipyre_http::plan::limits::BodyTimeAllowance::absolute(
            mipyre_http::plan::limits::BodyGrace::from_secs(3600.0).must("an hour is a grace"),
        ),
        mipyre_http::plan::limits::RequestBudget::from_secs(3600.0).must("an hour is a budget"),
    ));
    measure("stream_feed_per_frame", n, |_| {
        assert!(feeder.as_mut().poll(&mut cx).is_pending(), "the body never ends");
        assert!(
            matches!(rx.try_recv(), Ok(mipyre_http::io::body::BodyMessage::Chunk(_))),
            "every frame arrives"
        );
    });
}

/// Reads a body that came in with the head as one frame, as hyper hands it over, under the app's
/// limits the way the handler paths call `read_body`.
fn read_body_ready(n: u64) {
    let limits = black_box(mipyre_http::plan::limits::Limits::DEFAULT);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    measure("read_body_ready", n, |_| {
        let body =
            http_body_util::BodyExt::map_err(http_body_util::Full::new(Bytes::from_static(b"{\"a\":1}")), |never| {
                match never {}
            });
        let read = std::pin::pin!(mipyre_http::io::body::read_body(body, &limits));
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
