//! Sends the responses hyper produces in one poll of an HTTP/1 connection in one write, without
//! waiting on a timer.
//!
//! hyper writes and flushes each response on its own, so 16 pipelined requests cost 16 syscalls
//! and 16 TCP segments. [`CoalescingIo`] keeps a write only when another response may share it;
//! a write with nothing to share it goes straight to the inner IO, uncopied. The kept bytes go
//! out when the poll of the connection ends ([`CoalescedPoll`]), at a flush once the client has
//! nothing more queued, when they would pass the coalescing limit, or at shutdown or
//! [`Flusher::finish`]. Writing out includes the inner IO's flush, because a TLS stream may hold
//! ciphertext until it is flushed.
//!
//! A `Pending` read does not write by itself: hyper probes for EOF right after parsing the last
//! request of a batch, before its response exists, and a write there would split the batch.
//! Unlike hyper's `pipeline_flush`, nothing here depends on what the client sends next, so half a
//! request cannot hold a response until the header-read timeout. `docs/write-coalescing.md`
//! analyses hyper's call order.
//!
//! An HTTP/2 connection, recognised by its preface, passes straight through, because h2 buffers
//! its frames itself.
//!
//! [`SendShare`] puts the shared state behind a mutex that is never contended in practice: hyper
//! requires an upgradable connection's IO to be `Send`, and an upgraded IO may be used by another
//! task while the connection's last poll still runs. [`LocalShare`] saves the lock for a
//! connection that stays on one thread and is never upgraded. Outside a poll a flush always
//! writes, so an upgraded stream never holds bytes back.
//!
//! A failed write fails the connection: the kept bytes are dropped and the error is returned
//! once, to the call that met it or, when a poll's end met it, to the next call.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::cell::RefCell;
use std::ops::DerefMut;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{ready, Context, Poll, Waker};

use crate::sync::{AtomicBool, Mutex, MutexGuard, Ordering};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use mipyre_transport::stream::events::{self, StreamEvent};
use mipyre_transport::stream::peer_gone;
use mipyre_util::clock;
use crate::plan::coalesce::{
    first_allocation, flush_holds, keeps_allocation, poll_end_writes, sniffed, ConnectionFacts,
    PartialPreface, PollPhase, Protocol, WriteAction,
};
use mipyre_util::throttle::LogThrottle;

const DROP_WARNING_PERIOD: std::time::Duration = std::time::Duration::from_secs(60);

/// Throttles the warning for a dropped IO whose last write failed, apart from the full-socket one
/// so neither holds the other back or folds its count into it.
static WRITE_FAILED_THROTTLE: LogThrottle = LogThrottle::new(DROP_WARNING_PERIOD);
/// Throttles the warning for a dropped IO whose socket was full.
static SOCKET_FULL_THROTTLE: LogThrottle = LogThrottle::new(DROP_WARNING_PERIOD);

mod sealed {
    pub trait Sealed {}
}

/// Chooses, by type and so without dynamic dispatch, how the IO and its [`Flusher`] share the
/// connection's core.
pub trait Sharing: sealed::Sealed + Sized + 'static {
    #[doc(hidden)]
    type Share<X>;
    #[doc(hidden)]
    fn new<X>(state: X) -> Self::Share<X>;
    #[doc(hidden)]
    fn share<X>(shared: &Self::Share<X>) -> Self::Share<X>;
    /// Every borrow is given back within one call of the IO or the [`Flusher`], and neither
    /// calls the other, so a borrow never conflicts.
    #[doc(hidden)]
    type Guard<'a, X: 'a>: DerefMut<Target = X>
    where
        Self::Share<X>: 'a;
    #[doc(hidden)]
    fn lock<X>(shared: &Self::Share<X>) -> Self::Guard<'_, X>;
    #[doc(hidden)]
    fn passthrough<X>(shared: &Self::Share<X>) -> &AtomicBool;
    #[doc(hidden)]
    fn sole<X>(shared: Self::Share<X>) -> Option<X>;
}

/// Makes the IO `Send` when its stream is, so it can be upgraded or served on a multi-thread
/// runtime.
pub enum SendShare {}

/// Makes the IO not `Send`, for connections that stay on their thread's `LocalSet` and serve no
/// WebSocket route; it saves two locked read-modify-writes per call.
pub enum LocalShare {}

impl sealed::Sealed for SendShare {}
impl sealed::Sealed for LocalShare {}

/// `passthrough` caches the state's `protocol` once it is `Http2`, so the [`Flusher`] of an
/// HTTP/2 connection returns without taking the lock twice per poll. Keeping the protocol only
/// outside the lock would cost every HTTP/1 request 4 to 5 instructions, and only inside would
/// cost every HTTP/2 poll 14. A relaxed atomic is a plain load or store, so the one-thread kind
/// pays nothing for it.
#[doc(hidden)]
pub struct ConnectionCore<L> {
    passthrough: AtomicBool,
    state: L,
}

impl Sharing for SendShare {
    type Share<X> = Arc<ConnectionCore<Mutex<X>>>;
    #[inline]
    fn new<X>(state: X) -> Self::Share<X> {
        Arc::new(ConnectionCore {
            passthrough: AtomicBool::new(false),
            state: Mutex::new(state),
        })
    }
    #[inline]
    fn share<X>(shared: &Self::Share<X>) -> Self::Share<X> {
        Arc::clone(shared)
    }
    type Guard<'a, X: 'a> = MutexGuard<'a, X>;
    #[inline]
    fn lock<X>(shared: &Self::Share<X>) -> Self::Guard<'_, X> {
        shared.state.lock()
    }
    #[inline]
    fn passthrough<X>(shared: &Self::Share<X>) -> &AtomicBool {
        &shared.passthrough
    }
    #[inline]
    fn sole<X>(shared: Self::Share<X>) -> Option<X> {
        Arc::try_unwrap(shared).ok().map(|core| core.state.into_inner())
    }
}

impl Sharing for LocalShare {
    type Share<X> = Rc<ConnectionCore<RefCell<X>>>;
    #[inline]
    fn new<X>(state: X) -> Self::Share<X> {
        Rc::new(ConnectionCore {
            passthrough: AtomicBool::new(false),
            state: RefCell::new(state),
        })
    }
    #[inline]
    fn share<X>(shared: &Self::Share<X>) -> Self::Share<X> {
        Rc::clone(shared)
    }
    type Guard<'a, X: 'a> = std::cell::RefMut<'a, X>;
    #[inline]
    fn lock<X>(shared: &Self::Share<X>) -> Self::Guard<'_, X> {
        shared.state.borrow_mut()
    }
    #[inline]
    fn passthrough<X>(shared: &Self::Share<X>) -> &AtomicBool {
        &shared.passthrough
    }
    #[inline]
    fn sole<X>(shared: Self::Share<X>) -> Option<X> {
        Rc::try_unwrap(shared).ok().map(|core| core.state.into_inner())
    }
}

#[derive(Debug)]
enum WriteOut {
    Idle,
    /// `out[sent..]` is still owed, and every call continues the write-out until `out` is empty.
    Draining { sent: usize },
    /// A write-out started at the end of a poll failed; the next call reports it, once.
    Failed(io::Error),
}

struct CoalesceState<T: AsyncWrite + Unpin> {
    inner: T,
    out: Vec<u8>,
    write: WriteOut,
    unflushed: bool,
    conn: ConnectionFacts,
}

impl<T: AsyncWrite + Unpin> CoalesceState<T> {
    #[inline]
    fn sent(&self) -> usize {
        match self.write {
            WriteOut::Draining { sent } => sent,
            WriteOut::Idle | WriteOut::Failed(_) => 0,
        }
    }

    #[inline]
    fn draining(&self) -> bool {
        matches!(self.write, WriteOut::Draining { .. })
    }

    /// Takes the failure a poll's end met, so it is reported once; only a failed state is moved.
    #[inline]
    fn take_failure(&mut self) -> Option<io::Error> {
        if !matches!(self.write, WriteOut::Failed(_)) {
            return None;
        }
        match std::mem::replace(&mut self.write, WriteOut::Idle) {
            WriteOut::Failed(e) => Some(e),
            WriteOut::Idle | WriteOut::Draining { .. } => None,
        }
    }

    /// Records that `inner` blocked, so the poll's end need not try again.
    #[inline]
    fn note_blocked(&mut self) {
        if let PollPhase::Inside { blocked } = &mut self.conn.poll {
            *blocked = true;
        }
    }

    #[inline]
    fn has_pending(&self) -> bool {
        self.sent() < self.out.len()
    }

    #[inline]
    fn owes(&self) -> bool {
        self.has_pending() || self.unflushed
    }

    /// Drops what is kept, because the connection is broken, so no later call writes it again.
    fn fail(&mut self, e: io::Error) -> io::Error {
        self.out.clear();
        self.write = WriteOut::Idle;
        self.unflushed = false;
        e
    }

    /// Writes `out[sent..]` to the inner IO; `Pending` keeps the rest for when it is writable.
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut sent = self.sent();
        while sent < self.out.len() {
            match Pin::new(&mut self.inner).poll_write(cx, &self.out[sent..]) {
                Poll::Pending => {
                    self.write = WriteOut::Draining { sent };
                    self.note_blocked();
                    return Poll::Pending;
                }
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(self.fail(io::ErrorKind::WriteZero.into())));
                }
                Poll::Ready(Ok(n)) => {
                    sent += n;
                    self.unflushed = true;
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(self.fail(e))),
            }
        }
        self.out.clear();
        if !keeps_allocation(self.out.capacity()) {
            self.out = Vec::new();
        }
        self.write = WriteOut::Idle;
        Poll::Ready(Ok(()))
    }

    fn poll_flush_inner(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.inner).poll_flush(cx) {
            Poll::Pending => {
                self.note_blocked();
                Poll::Pending
            }
            Poll::Ready(Ok(())) => {
                self.unflushed = false;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(self.fail(e))),
        }
    }

    fn poll_write_out(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.poll_drain(cx))?;
        if self.unflushed {
            return self.poll_flush_inner(cx);
        }
        Poll::Ready(Ok(()))
    }

    /// Continues a write-out that has started, or reports the one that failed. Kept bytes not yet
    /// started stay kept, because a read must not split a batch.
    fn poll_continue(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(e) = self.take_failure() {
            return Poll::Ready(Err(e));
        }
        if self.draining() {
            ready!(self.poll_drain(cx))?;
        }
        if self.unflushed {
            return self.poll_flush_inner(cx);
        }
        Poll::Ready(Ok(()))
    }

    fn sniff(&mut self, got: &[u8]) {
        if let Protocol::Sniffing(seen) = self.conn.protocol {
            self.conn.protocol = sniffed(seen, got);
        }
    }

    fn append(&mut self, bufs: &[IoSlice<'_>], total: usize) {
        if self.out.capacity() == 0 {
            self.out.reserve(first_allocation(total));
        }
        for b in bufs {
            self.out.extend_from_slice(b);
        }
    }
}

impl<T: AsyncWrite + Unpin> Drop for CoalesceState<T> {
    /// Makes one non-blocking attempt to write what is owed when the task was cancelled before
    /// the connection finished.
    fn drop(&mut self) {
        if !self.owes() {
            return;
        }
        let kept = self.out.len() - self.sent();
        let mut cx = Context::from_waker(Waker::noop());
        let written = self.poll_write_out(&mut cx);
        say_dropped_write_out(written, kept, [&WRITE_FAILED_THROTTLE, &SOCKET_FULL_THROTTLE], clock::now());
    }
}

/// Logs how a dropped IO's last attempt went, since a drop has no caller to return it to. A
/// failed write warns through the first of `throttles` and a full socket through the second; a peer
/// that went away is only counted.
fn say_dropped_write_out(
    written: Poll<io::Result<()>>,
    kept: usize,
    [write_failed, socket_full]: [&LogThrottle; 2],
    now: std::time::Duration,
) {
    match written {
        Poll::Ready(Ok(())) => {}
        Poll::Ready(Err(e)) if peer_gone(&e) => {
            events::count(StreamEvent::PeerGone);
            tracing::debug!(error = %e, kept, "the kept bytes of a connection its peer left were not written");
        }
        Poll::Ready(Err(e)) => {
            if let Some(held) = write_failed.admit(now) {
                tracing::warn!(error = %e, kept, held, "the kept bytes of a dropped connection were not written");
            }
        }
        Poll::Pending => {
            if let Some(held) = socket_full.admit(now) {
                tracing::warn!(
                    kept,
                    held,
                    "a dropped connection's socket was full: its kept bytes were not written"
                );
            }
        }
    }
}

pub struct CoalescingIo<T: AsyncWrite + Unpin, S: Sharing = SendShare> {
    shared: S::Share<CoalesceState<T>>,
}

/// Marks the polls of the connection, through [`CoalescedPoll`], and writes what the connection
/// left when it ended.
pub struct Flusher<T: AsyncWrite + Unpin, S: Sharing = SendShare> {
    shared: S::Share<CoalesceState<T>>,
}

/// Wraps `inner` in a coalescing IO and returns it with the [`Flusher`] its [`CoalescedPoll`] uses.
pub fn coalesce<T: AsyncWrite + Unpin>(inner: T) -> (CoalescingIo<T>, Flusher<T>) {
    coalesce_shared(inner)
}

/// Wraps `inner` like [`coalesce`] for a connection that stays on one thread and is never upgraded.
pub fn coalesce_local<T: AsyncWrite + Unpin>(inner: T) -> (CoalescingIo<T, LocalShare>, Flusher<T, LocalShare>) {
    coalesce_shared(inner)
}

fn coalesce_shared<T: AsyncWrite + Unpin, S: Sharing>(inner: T) -> (CoalescingIo<T, S>, Flusher<T, S>) {
    let vectored = inner.is_write_vectored();
    let shared = S::new(CoalesceState {
        inner,
        out: Vec::new(),
        write: WriteOut::Idle,
        unflushed: false,
        conn: ConnectionFacts {
            protocol: Protocol::Sniffing(PartialPreface::EMPTY),
            poll: PollPhase::Outside,
            input_unread: false,
            vectored,
        },
    });
    (
        CoalescingIo {
            shared: S::share(&shared),
        },
        Flusher { shared },
    )
}

impl<T: AsyncRead + AsyncWrite + Unpin, S: Sharing> AsyncRead for CoalescingIo<T, S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut core = S::lock(&self.shared);
        if core.conn.protocol == Protocol::Http2 {
            return Pin::new(&mut core.inner).poll_read(cx, buf);
        }
        if let Poll::Ready(Err(e)) = core.poll_continue(cx) {
            return Poll::Ready(Err(e));
        }
        let before = buf.filled().len();
        let read = Pin::new(&mut core.inner).poll_read(cx, buf);
        match read {
            Poll::Pending => {
                // The client has nothing more queued, so the next flush hyper asks for writes.
                core.conn.input_unread = false;
            }
            Poll::Ready(Ok(())) => {
                let got = &buf.filled()[before..];
                if got.is_empty() {
                    // At EOF nothing more is coming, so writing what is kept starts now.
                    core.conn.input_unread = false;
                    if core.owes() {
                        if let Poll::Ready(Err(e)) = core.poll_write_out(cx) {
                            return Poll::Ready(Err(e));
                        }
                    }
                } else {
                    core.conn.input_unread = true;
                    if let Protocol::Sniffing(_) = core.conn.protocol {
                        core.sniff(got);
                        if core.conn.protocol == Protocol::Http2 {
                            S::passthrough(&self.shared).store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
            Poll::Ready(Err(_)) => {}
        }
        read
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin, S: Sharing> AsyncWrite for CoalescingIo<T, S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored(cx, &[IoSlice::new(buf)])
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut core = S::lock(&self.shared);
        if let Some(e) = core.take_failure() {
            return Poll::Ready(Err(e));
        }
        let decision = crate::plan::coalesce::write(&core.conn, core.out.len(), core.draining(), bufs);
        if decision.drain_first() {
            ready!(core.poll_drain(cx))?;
        }
        match decision.action() {
            WriteAction::Pass => Pin::new(&mut core.inner).poll_write_vectored(cx, bufs),
            WriteAction::Direct => match Pin::new(&mut core.inner).poll_write_vectored(cx, bufs) {
                Poll::Pending => {
                    core.note_blocked();
                    Poll::Pending
                }
                Poll::Ready(Ok(n)) => {
                    core.unflushed |= n > 0;
                    Poll::Ready(Ok(n))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(core.fail(e))),
            },
            WriteAction::Keep { total } => {
                let total = total as usize;
                core.append(bufs, total);
                Poll::Ready(Ok(total))
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut core = S::lock(&self.shared);
        if let Some(e) = core.take_failure() {
            return Poll::Ready(Err(e));
        }
        if core.has_pending() {
            if flush_holds(&core.conn, core.draining()) {
                // More requests may be waiting in hyper's buffer, so this response waits to share
                // their write; the poll's end writes it at the latest.
                return Poll::Ready(Ok(()));
            }
            ready!(core.poll_drain(cx))?;
        }
        core.poll_flush_inner(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut core = S::lock(&self.shared);
        if let Some(e) = core.take_failure() {
            return Poll::Ready(Err(e));
        }
        if core.has_pending() {
            ready!(core.poll_drain(cx))?;
        }
        Pin::new(&mut core.inner).poll_shutdown(cx)
    }
}

impl<T: AsyncWrite + Unpin, S: Sharing> Flusher<T, S> {
    fn enter(&self) {
        if S::passthrough(&self.shared).load(Ordering::Relaxed) {
            return;
        }
        let mut core = S::lock(&self.shared);
        core.conn.poll = PollPhase::Inside { blocked: false };
    }

    /// Writes out what the poll kept, when [`poll_end_writes`] says so. A write-out that cannot
    /// finish now is continued by the next call; one that fails is reported by the next call or
    /// by [`Flusher::finish`].
    fn leave(&self, cx: &mut Context<'_>) {
        if S::passthrough(&self.shared).load(Ordering::Relaxed) {
            return;
        }
        let mut core = S::lock(&self.shared);
        let phase = std::mem::replace(&mut core.conn.poll, PollPhase::Outside);
        if !poll_end_writes(phase, core.owes()) {
            return;
        }
        if let Poll::Ready(Err(e)) = core.poll_write_out(cx) {
            core.write = WriteOut::Failed(e);
        }
    }

    /// Writes out what is still owed once the connection future has ended. It waits as long as
    /// the inner IO blocks, so a write timeout below must bound it. When an upgrade handed the IO
    /// to another task, that task's calls carry the write-out instead and this returns at once.
    ///
    /// # Errors
    ///
    /// Returns the error of a write-out that failed, here or at an earlier poll's end.
    pub async fn finish(self) -> io::Result<()> {
        let Some(mut state) = S::sole(self.shared) else {
            return Ok(());
        };
        if let Some(e) = state.take_failure() {
            return Err(e);
        }
        std::future::poll_fn(|cx| state.poll_write_out(cx)).await
    }
}

/// Brackets one poll of the connection future: the IO keeps bytes only inside it, and its end
/// writes them.
pub struct CoalescedPoll<'a, F: ?Sized, T: AsyncWrite + Unpin, S: Sharing = SendShare> {
    conn: Pin<&'a mut F>,
    flusher: &'a Flusher<T, S>,
}

impl<'a, F: ?Sized, T: AsyncWrite + Unpin, S: Sharing> CoalescedPoll<'a, F, T, S> {
    pub fn new(conn: Pin<&'a mut F>, flusher: &'a Flusher<T, S>) -> Self {
        CoalescedPoll { conn, flusher }
    }
}

impl<F: std::future::Future + ?Sized, T: AsyncWrite + Unpin, S: Sharing> std::future::Future
    for CoalescedPoll<'_, F, T, S>
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.get_mut();
        this.flusher.enter();
        let polled = this.conn.as_mut().poll(cx);
        this.flusher.leave(cx);
        polled
    }
}

#[cfg(test)]
mod tests;
