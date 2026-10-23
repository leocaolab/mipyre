//! Fails a write or flush that makes no progress for a time limit.
//!
//! hyper has no write timeout, because its header-read timer runs only while it waits for a
//! request head, so a client that never reads would hold the connection's fd and task forever.

use std::future::Future;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Instant, Sleep};

use crate::stream::events::{self, StreamEvent};

/// Bounds each write stall; any progress resets it. hyper has no write timeout, so without it
/// a client that never reads would hold the connection forever.
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// The source of the stream's `TimedOut` error, so a caller can tell a client that stopped
/// reading from other errors. A client can cause it at will, so whoever logs it must throttle.
#[derive(Debug, thiserror::Error)]
#[error("write stalled for {limit:?}: the peer did not read")]
pub struct WriteStallError {
    pub limit: Duration,
}

/// Fails writes with `TimedOut` once they have been pending for `limit` without progress;
/// reads are not limited.
///
/// After a timeout every later write, flush and shutdown fails the same way without touching the
/// inner stream, because the connection is to be closed. Each stall is counted once as
/// [`StreamEvent::WriteStalled`].
pub struct WriteStall<IO> {
    inner: IO,
    limit: Duration,
    stall: Stall,
    /// The timer is reset for each stall after the first, so a client that reads in bursts costs
    /// one allocation per connection, not one per stall.
    timer: Option<Pin<Box<Sleep>>>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Stall {
    Clear,
    Running(Call),
    Spent,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Call {
    Write,
    /// A shutdown counts as a flush.
    Flush,
}

impl<IO> WriteStall<IO> {
    pub fn new(inner: IO, limit: Duration) -> Self {
        WriteStall {
            inner,
            limit,
            stall: Stall::Clear,
            timer: None,
        }
    }

    fn stalled(&self) -> io::Error {
        io::Error::new(io::ErrorKind::TimedOut, WriteStallError { limit: self.limit })
    }

    fn spent<T>(&self) -> Option<Poll<io::Result<T>>> {
        (self.stall == Stall::Spent).then(|| Poll::Ready(Err(self.stalled())))
    }

    /// Passes a call's result through, starting, keeping or clearing the stall budget.
    ///
    /// A completed flush clears only a stall a flush started, because with a write blocked, a
    /// plain socket's flush completes at once and moves nothing.
    fn settle<T>(
        &mut self,
        cx: &mut Context<'_>,
        call: Call,
        polled: Poll<io::Result<T>>,
        progress: impl FnOnce(&io::Result<T>) -> bool,
    ) -> Poll<io::Result<T>> {
        let Poll::Ready(result) = polled else {
            let limit = self.limit;
            let timer = match &mut self.timer {
                Some(timer) => {
                    if self.stall == Stall::Clear {
                        timer.as_mut().reset(Instant::now() + limit);
                    }
                    timer
                }
                None => self.timer.insert(Box::pin(tokio::time::sleep(limit))),
            };
            if timer.as_mut().poll(cx).is_ready() {
                self.stall = Stall::Spent;
                events::count(StreamEvent::WriteStalled);
                return Poll::Ready(Err(self.stalled()));
            }
            if self.stall == Stall::Clear {
                self.stall = Stall::Running(call);
            }
            return Poll::Pending;
        };
        let cleared = match self.stall {
            Stall::Running(Call::Write) => call == Call::Write,
            Stall::Running(Call::Flush) | Stall::Clear | Stall::Spent => true,
        };
        if cleared && progress(&result) {
            // A timer left registered causes at most one spurious wake at its old deadline.
            self.stall = Stall::Clear;
        }
        Poll::Ready(result)
    }
}

/// Counts a write as progress when it moved bytes or had none to move; `nothing_to_write` is
/// asked only for an `Ok(0)`, so a vectored write rarely adds up its slices.
fn wrote(nothing_to_write: impl FnOnce() -> bool) -> impl FnOnce(&io::Result<usize>) -> bool {
    move |result| !matches!(result, Ok(0)) || nothing_to_write()
}

fn done(_: &io::Result<()>) -> bool {
    true
}

impl<IO: AsyncRead + Unpin> AsyncRead for WriteStall<IO> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for WriteStall<IO> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        if let Some(spent) = self.spent() {
            return spent;
        }
        let polled = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.settle(cx, Call::Write, polled, wrote(|| buf.is_empty()))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if let Some(spent) = self.spent() {
            return spent;
        }
        let polled = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        self.settle(cx, Call::Write, polled, wrote(|| bufs.iter().all(|b| b.is_empty())))
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(spent) = self.spent() {
            return spent;
        }
        let polled = Pin::new(&mut self.inner).poll_flush(cx);
        self.settle(cx, Call::Flush, polled, done)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(spent) = self.spent() {
            return spent;
        }
        let polled = Pin::new(&mut self.inner).poll_shutdown(cx);
        self.settle(cx, Call::Flush, polled, done)
    }
}

#[cfg(test)]
mod tests;
