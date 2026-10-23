//! Closes a stream the server is done with while its peer may still be sending.
//!
//! A server that answers before it has read the whole request (a 413 refused by its
//! `Content-Length`, a 400) and then closes at once loses the answer: with unread bytes in its
//! receive buffer, TCP's close sends a reset, which discards the response the client has not read
//! yet, and on a Unix socket the client's next write fails with `EPIPE` before it reads at all.
//! RFC 9112 §9.6 says to close in stages instead: shut down the write side, read and discard what
//! the client still sends until it closes, then close. hyper shuts down the write side; this does
//! the rest, bounded, since a client could otherwise hold the stream by sending forever.

use std::future::{poll_fn, Future};
use std::io;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use tokio::io::{AsyncRead, ReadBuf};
use tokio::time::Instant;

use crate::stream::events::{self, StreamEvent};

/// Bounds one read; what is read is thrown away, so the size only sets how many reads a large
/// unread body takes.
const DISCARD_CHUNK: usize = 4096;

/// Says how a server closes a stream it is done with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Close {
    /// Nothing the peer may still send matters, or no one waits for an answer.
    Now,
    /// The peer may still be sending a request the server answered before reading it whole, so
    /// the stream is closed through [`linger`].
    Lingering,
}

/// Bounds how long [`linger`] waits for the peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LingerBounds {
    /// Bounds the gap between the peer's bytes: a client that has finished sending its request
    /// and is reading the answer sends nothing more, so the wait ends this long after its last
    /// bytes.
    pub idle: Duration,
    /// Bounds the whole wait, so a client that keeps sending holds the stream no longer than this.
    pub limit: Duration,
}

impl LingerBounds {
    /// Returns when to stop waiting for the peer's next bytes, for a wait that `started` and last
    /// received bytes at `last_bytes`.
    #[inline]
    fn deadline(self, started: Instant, last_bytes: Instant) -> Instant {
        (last_bytes + self.idle).min(started + self.limit)
    }
}

/// Reads and discards what the peer sends until it closes its side, a read fails, `bounds` pass
/// or `stop` completes, so the stream can then be closed without a reset destroying what was
/// written to it. A wait cut short by `bounds.limit` while the peer was still sending counts as
/// [`StreamEvent::LingerCutShort`]; one `stop` ends is the server's own choice and is not counted.
pub async fn linger<IO, F>(io: &mut IO, bounds: LingerBounds, stop: F)
where
    IO: AsyncRead + Unpin,
    F: Future<Output = ()>,
{
    let started = Instant::now();
    let mut until = bounds.deadline(started, started);
    let mut stop = std::pin::pin!(stop);
    loop {
        let wakeup = poll_fn(|cx| {
            if stop.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Wakeup::Stop);
            }
            discard(io, cx).map(Wakeup::Read)
        });
        match tokio::time::timeout_at(until, wakeup).await {
            Ok(Wakeup::Stop | Wakeup::Read(Ok(0) | Err(_))) => return,
            Ok(Wakeup::Read(Ok(_))) => until = bounds.deadline(started, Instant::now()),
            Err(_) => {
                if until == started + bounds.limit {
                    events::count(StreamEvent::LingerCutShort);
                    tracing::debug!(limit = ?bounds.limit, "the peer kept sending past the lingering close's limit");
                }
                return;
            }
        }
    }
}

/// Says what woke a lingering wait before its deadline.
enum Wakeup {
    Stop,
    Read(io::Result<usize>),
}

/// Reads one chunk into a buffer that lives only for the call, so a lingering stream's future
/// carries no buffer of its own, and returns how many bytes came; 0 is the peer's close.
fn discard<IO: AsyncRead + Unpin>(io: &mut IO, cx: &mut std::task::Context<'_>) -> Poll<io::Result<usize>> {
    let mut space = [0u8; DISCARD_CHUNK];
    let mut buf = ReadBuf::new(&mut space);
    std::task::ready!(Pin::new(io).poll_read(cx, &mut buf))?;
    Poll::Ready(Ok(buf.filled().len()))
}

#[cfg(test)]
mod tests;
