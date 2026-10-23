//! Measures HTTP/1 responses through the write-coalescing layer, alone or 16 pipelined, with the
//! locked and the one-thread layer.
use std::alloc::System;
use std::hint::black_box;

use mipyre_microbench::{measure, run, AllocationCounter, Case};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

struct Socket {
    pending_next: bool,
}

const REQUEST: &[u8] = b"GET /plaintext HTTP/1.1\r\nHost: b\r\n\r\n";
const HEAD: &[u8] =
    b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 13\r\ndate: Sun, 18 Oct 2026 00:00:00 GMT\r\n\r\n";
const BODY: &[u8] = b"Hello, World!";

impl tokio::io::AsyncRead for Socket {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let pending = self.pending_next;
        self.pending_next = !pending;
        if pending {
            return std::task::Poll::Pending;
        }
        buf.put_slice(REQUEST);
        std::task::Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncWrite for Socket {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Ok(black_box(buf).len()))
    }
    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Ok(black_box(bufs).iter().map(|b| b.len()).sum()))
    }
    fn is_write_vectored(&self) -> bool {
        true
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// Does on `io` what hyper does for `responses` requests that arrived together, including the EOF
/// probe it makes right after parsing the last request of a batch, before that request's answer.
fn one_round<IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    io: &mut IO,
    cx: &mut std::task::Context<'_>,
    responses: usize,
) {
    use std::pin::Pin;
    let mut space = [0u8; 64];
    let mut read = tokio::io::ReadBuf::new(&mut space);
    assert!(Pin::new(&mut *io).poll_read(cx, &mut read).is_ready());
    for i in 0..responses {
        if i + 1 == responses {
            let mut probe = tokio::io::ReadBuf::new(&mut space);
            assert!(Pin::new(&mut *io).poll_read(cx, &mut probe).is_pending());
        }
        let bufs = [std::io::IoSlice::new(HEAD), std::io::IoSlice::new(BODY)];
        let written = Pin::new(&mut *io).poll_write_vectored(cx, &bufs);
        assert!(
            matches!(written, std::task::Poll::Ready(Ok(n)) if n == HEAD.len() + BODY.len()),
            "{written:?}"
        );
        assert!(Pin::new(&mut *io).poll_flush(cx).is_ready());
    }
}

/// Runs `one_round` inside `CoalescedPoll`, as the engine drives a connection.
macro_rules! coalesce_bench {
    ($name:literal, $make:path, $n:expr, $responses:expr) => {{
        let (mut io, flusher) = $make(Socket { pending_next: false });
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        measure($name, $n, |_| {
            let mut round = std::future::poll_fn(|cx| {
                one_round(&mut io, cx, $responses);
                std::task::Poll::Ready(())
            });
            let polled = std::future::Future::poll(
                std::pin::Pin::new(&mut mipyre_http::io::coalesce::CoalescedPoll::new(
                    std::pin::Pin::new(&mut round),
                    &flusher,
                )),
                &mut cx,
            );
            assert!(polled.is_ready());
        });
    }};
}

fn coalesce_lone_response(n: u64) {
    coalesce_bench!("coalesce_lone_response", mipyre_http::io::coalesce::coalesce, n, 1);
}

fn coalesce_pipelined_16(n: u64) {
    coalesce_bench!("coalesce_pipelined_16", mipyre_http::io::coalesce::coalesce, n, 16);
}

fn coalesce_lone_response_local(n: u64) {
    coalesce_bench!(
        "coalesce_lone_response_local",
        mipyre_http::io::coalesce::coalesce_local,
        n,
        1
    );
}

fn coalesce_pipelined_16_local(n: u64) {
    coalesce_bench!(
        "coalesce_pipelined_16_local",
        mipyre_http::io::coalesce::coalesce_local,
        n,
        16
    );
}

fn main() {
    let cases: [Case; 4] = [
        ("coalesce_lone_response", coalesce_lone_response),
        ("coalesce_pipelined_16", coalesce_pipelined_16),
        ("coalesce_lone_response_local", coalesce_lone_response_local),
        ("coalesce_pipelined_16_local", coalesce_pipelined_16_local),
    ];
    run(&cases);
}
