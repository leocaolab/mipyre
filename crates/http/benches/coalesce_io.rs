//! Measures the coalescing layer and the write timeout per poll, over scripted IO with no socket.
//!
//! `blocked_poll` counts the inner writes per poll of a connection whose socket is full, each of
//! which would be a `write` syscall on a real socket. `h2_poll` times an HTTP/2 connection's read
//! and write through the layer. `stall_cycle` counts allocations per blocked-then-written cycle of
//! `WriteStall`, and `batch_18k` per batch of one 18 KiB response. `BENCH_N` sets the number of
//! polls or cycles.
// The package's clippy.toml is plan/'s list; a bench measures the world through io/.
#![expect(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::disallowed_macros,
    reason = "a bench reads clocks, opens sockets and prints"
)]

use std::future::Future;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use mimalloc::MiMalloc;
use mipyre_http::io::coalesce::{coalesce, CoalescedPoll, CoalescingIo};
use mipyre_microbench::{calls, AllocationCounter, ALLOCATIONS};
use mipyre_transport::stream::stall::WriteStall;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Counts over mimalloc, the allocator the server runs on, so a time per allocation here is the
/// production one.
#[global_allocator]
static GLOBAL: AllocationCounter<MiMalloc> = AllocationCounter(MiMalloc);

const H2_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// A socket whose reads hand out `script` once and then `chunk` bytes forever, and whose writes are
/// refused with `Pending` and no wake while `full`.
struct ScriptIo {
    script: &'static [u8],
    chunk: usize,
    full: bool,
    writes: Arc<AtomicUsize>,
}

impl AsyncRead for ScriptIo {
    fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if !self.script.is_empty() {
            let n = self.script.len().min(buf.remaining());
            buf.put_slice(&self.script[..n]);
            self.script = &self.script[n..];
        } else {
            let n = self.chunk.min(buf.remaining());
            buf.initialize_unfilled_to(n);
            buf.advance(n);
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ScriptIo {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        if self.full {
            Poll::Pending
        } else {
            Poll::Ready(Ok(buf.len()))
        }
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        if self.full {
            Poll::Pending
        } else {
            Poll::Ready(Ok(bufs.iter().map(|b| b.len()).sum()))
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// What one poll of the stand-in connection does with its IO.
type Step = fn(&mut CoalescingIo<ScriptIo>, &mut Context<'_>);

/// A connection future that runs `step` on every poll and never ends.
struct Conn {
    io: CoalescingIo<ScriptIo>,
    step: Step,
}

impl Future for Conn {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        (this.step)(&mut this.io, cx);
        Poll::Pending
    }
}

/// Returns the ns and allocations per poll over `n` polls of `step`, after one poll of `warmup`.
fn polls(io: ScriptIo, step: Step, warmup: Step, n: u64) -> (f64, f64) {
    let (io, flusher) = coalesce(io);
    let mut conn = Box::pin(Conn { io, step: warmup });
    let mut cx = Context::from_waker(Waker::noop());
    let warm = Pin::new(&mut CoalescedPoll::new(conn.as_mut(), &flusher)).poll(&mut cx);
    assert!(warm.is_pending(), "the stand-in connection never ends");
    conn.step = step;
    let allocs = ALLOCATIONS.load(Ordering::Relaxed);
    let start = Instant::now();
    let mut pending: u64 = 0;
    for _ in 0..n {
        let polled = Pin::new(&mut CoalescedPoll::new(conn.as_mut(), &flusher)).poll(&mut cx);
        pending += u64::from(std::hint::black_box(polled).is_pending());
    }
    assert_eq!(pending, n, "the stand-in connection never ends");
    let ns = start.elapsed().as_nanos() as f64 / n as f64;
    let per = (ALLOCATIONS.load(Ordering::Relaxed) - allocs) as f64 / n as f64;
    (ns, per)
}

fn write_some(io: &mut CoalescingIo<ScriptIo>, cx: &mut Context<'_>) {
    let kept = Pin::new(io).poll_write(cx, &[b'x'; 100]);
    assert!(matches!(kept, Poll::Ready(Ok(100))), "kept: {kept:?}");
}

fn flush(io: &mut CoalescingIo<ScriptIo>, cx: &mut Context<'_>) {
    let flushed = Pin::new(io).poll_flush(cx);
    assert!(flushed.is_pending(), "the socket is full: {flushed:?}");
}

fn read_preface(io: &mut CoalescingIo<ScriptIo>, cx: &mut Context<'_>) {
    let mut space = [0u8; 24];
    let mut buf = ReadBuf::new(&mut space);
    let read = Pin::new(io).poll_read(cx, &mut buf);
    assert!(matches!(read, Poll::Ready(Ok(()))), "read: {read:?}");
}

fn h2_round(io: &mut CoalescingIo<ScriptIo>, cx: &mut Context<'_>) {
    let mut space = [0u8; 64];
    let mut buf = ReadBuf::new(&mut space);
    let read = Pin::new(&mut *io).poll_read(cx, &mut buf);
    assert!(matches!(read, Poll::Ready(Ok(()))), "read: {read:?}");
    let written = Pin::new(io).poll_write(cx, &space);
    assert!(matches!(written, Poll::Ready(Ok(64))), "written: {written:?}");
}

static BODY: [u8; 18 * 1024] = [b'b'; 18 * 1024];

fn response_18k(io: &mut CoalescingIo<ScriptIo>, cx: &mut Context<'_>) {
    let mut pinned = Pin::new(io);
    let head = pinned.as_mut().poll_write(cx, &[b'h'; 200]);
    assert!(matches!(head, Poll::Ready(Ok(200))), "head: {head:?}");
    let body = pinned.as_mut().poll_write(cx, &BODY);
    assert!(matches!(body, Poll::Ready(Ok(n)) if n == BODY.len()), "body: {body:?}");
    let flushed = pinned.poll_flush(cx);
    assert!(matches!(flushed, Poll::Ready(Ok(()))), "flushed: {flushed:?}");
}

fn script(full: bool, script: &'static [u8], chunk: usize) -> (ScriptIo, Arc<AtomicUsize>) {
    let writes = Arc::new(AtomicUsize::new(0));
    let io = ScriptIo {
        script,
        chunk,
        full,
        writes: Arc::clone(&writes),
    };
    (io, writes)
}

fn blocked_poll(n: u64) {
    let (io, writes) = script(true, b"", 0);
    let (ns, _) = polls(io, flush, write_some, n);
    // The count includes the warm-up poll, which writes once too.
    let inner = writes.load(Ordering::Relaxed);
    println!(
        "blocked_poll: {:.2} inner writes/poll, {ns:.1} ns/poll ({n} polls)",
        inner as f64 / (n + 1) as f64
    );
}

fn h2_poll(n: u64) {
    let (io, _) = script(false, H2_PREFACE, 64);
    let (ns, allocs) = polls(io, h2_round, read_preface, n);
    println!("h2_poll: {ns:.1} ns/poll, {allocs:.2} allocations/poll ({n} polls)");
}

fn batch_18k(n: u64) {
    let (io, _) = script(false, b"", 0);
    let (ns, allocs) = polls(io, response_18k, response_18k, n);
    println!("batch_18k: {allocs:.2} allocations/batch, {ns:.1} ns/batch ({n} batches)");
}

/// Refuses every other write, as a client that reads in bursts does.
struct BurstReader {
    refuse: bool,
}

impl AsyncWrite for BurstReader {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.refuse = !self.refuse;
        if self.refuse {
            Poll::Pending
        } else {
            Poll::Ready(Ok(buf.len()))
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn stall_cycle(n: u64) {
    let rt = match tokio::runtime::Builder::new_current_thread().enable_time().build() {
        Ok(rt) => rt,
        Err(e) => panic!("no runtime for the bench: {e}"),
    };
    let _entered = rt.enter();
    let mut io = WriteStall::new(BurstReader { refuse: false }, Duration::from_secs(10));
    let mut cx = Context::from_waker(Waker::noop());
    let allocs = ALLOCATIONS.load(Ordering::Relaxed);
    let start = Instant::now();
    for _ in 0..n {
        // A blocked write arms the budget, and the next one, which goes through, disarms it.
        let blocked = Pin::new(&mut io).poll_write(&mut cx, b"x");
        assert!(blocked.is_pending(), "the scripted write is refused");
        let written = Pin::new(&mut io).poll_write(&mut cx, b"x");
        assert!(written.is_ready(), "the scripted write goes through");
    }
    let ns = start.elapsed().as_nanos() as f64 / n as f64;
    let per = (ALLOCATIONS.load(Ordering::Relaxed) - allocs) as f64 / n as f64;
    println!("stall_cycle: {per:.2} allocations/cycle, {ns:.1} ns/cycle ({n} cycles)");
}

fn main() {
    let only = std::env::args().nth(1).filter(|a| !a.starts_with('-'));
    type Bench = (&'static str, fn(u64), u64);
    let benches: [Bench; 4] = [
        ("blocked_poll", blocked_poll, 1_000_000),
        ("h2_poll", h2_poll, 10_000_000),
        ("stall_cycle", stall_cycle, 1_000_000),
        ("batch_18k", batch_18k, 1_000_000),
    ];
    for (name, bench, default) in benches {
        if only.as_deref().is_none_or(|o| o == name) {
            bench(calls(default));
        }
    }
}
