//! Measures a vectored write through the write timeout, which every response's write goes through.
use std::alloc::System;
use std::hint::black_box;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use mipyre_microbench::{measure, run, AllocationCounter, Case};
use tokio::io::AsyncWrite as _;

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

struct Sink;

impl tokio::io::AsyncWrite for Sink {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        // The kernel works out a writev's count, so the sink returns it without adding the slices up.
        Poll::Ready(Ok(black_box(RESPONSE_HEAD.len() + RESPONSE_BODY.len())))
    }
    fn is_write_vectored(&self) -> bool {
        true
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

const RESPONSE_HEAD: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 27\r\n\r\n";
const RESPONSE_BODY: &[u8] = b"{\"message\":\"Hello, World!\"}";

/// Writes a response's head and body as two slices, as hyper hands them over.
fn write_stall_vectored(n: u64) {
    let mut io = mipyre_transport::stream::stall::WriteStall::new(Sink, Duration::from_secs(30));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let bufs = [
        std::io::IoSlice::new(RESPONSE_HEAD),
        std::io::IoSlice::new(RESPONSE_BODY),
    ];
    measure("write_stall_vectored", n, |_| {
        let polled = Pin::new(&mut io).poll_write_vectored(&mut cx, black_box(&bufs));
        assert!(matches!(black_box(polled), Poll::Ready(Ok(_))));
    });
}

fn main() {
    let cases: [Case; 1] = [("write_stall_vectored", write_stall_vectored)];
    run(&cases);
}
