//! Measures reading a request body that came in several frames and copying it into one buffer.
//! It is a target of its own because a second body type changes how `read_body_ready`, in
//! `benches/body.rs`, compiles, and with it that case's count.

use std::alloc::System;
use std::future::Future;
use std::hint::black_box;
use std::task::{Context, Poll};

use bytes::Bytes;
use mipyre_microbench::{measure, run, AllocationCounter, Case};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

const FRAME: [u8; 4096] = [b'x'; 4096];
const FRAMES: usize = 16;

/// A `Content-Length` body whose [`FRAMES`] frames are all there, as hyper hands over a large body
/// that came in several reads.
struct FrameSource {
    left: usize,
}

impl hyper::body::Body for FrameSource {
    type Data = Bytes;
    type Error = hyper::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, hyper::Error>>> {
        if self.left == 0 {
            return Poll::Ready(None);
        }
        self.left -= 1;
        Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_static(&FRAME)))))
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        hyper::body::SizeHint::with_exact((self.left * FRAME.len()) as u64)
    }
}

/// Reads a 64 KiB body of 4 KiB frames and copies it into a buffer of its length, as a handler's
/// `req.body` gets it.
fn read_body_frames(n: u64) {
    let limits = black_box(mipyre_http::plan::limits::Limits::DEFAULT);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    measure("read_body_frames", n, |_| {
        let read = std::pin::pin!(mipyre_http::io::body::read_body(FrameSource { left: FRAMES }, &limits));
        let Poll::Ready(Ok(frames)) = read.poll(&mut cx) else {
            panic!("a body that is all there is read in one poll");
        };
        let mut whole = vec![0u8; frames.len()];
        frames.copy_to_slice(&mut whole);
        black_box(whole);
    });
}

fn main() {
    let cases: [Case; 1] = [("read_body_frames", read_body_frames)];
    run(&cases);
}
