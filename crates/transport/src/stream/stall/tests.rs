//! Tests the write timeout over a scripted socket on a paused clock.

use super::*;

use std::collections::VecDeque;
use std::task::Waker;

/// Answers each write from the script, where `None` is `Pending`; once the script runs out,
/// every write is `Pending`.
struct Socket {
    script: VecDeque<Option<usize>>,
    calls: usize,
}

fn socket(script: &[Option<usize>]) -> Socket {
    Socket {
        script: script.iter().copied().collect(),
        calls: 0,
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.calls += 1;
        match self.script.pop_front().flatten() {
            Some(n) => Poll::Ready(Ok(n.min(buf.len()))),
            None => Poll::Pending,
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

const LIMIT: Duration = Duration::from_secs(10);
const TICK: Duration = Duration::from_millis(1);

fn write(io: &mut WriteStall<Socket>) -> Poll<io::Result<usize>> {
    let mut cx = Context::from_waker(Waker::noop());
    Pin::new(io).poll_write(&mut cx, b"response")
}

#[tokio::test(start_paused = true)]
async fn a_write_blocked_for_the_limit_fails_with_timed_out() {
    let mut io = WriteStall::new(socket(&[]), LIMIT);
    assert!(write(&mut io).is_pending());
    tokio::time::advance(LIMIT - TICK).await;
    assert!(write(&mut io).is_pending(), "not yet at the limit");
    tokio::time::advance(TICK).await;
    match write(&mut io) {
        Poll::Ready(Err(e)) => assert_eq!(e.kind(), io::ErrorKind::TimedOut),
        other => panic!("expected TimedOut, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn progress_restarts_the_budget() {
    let mut io = WriteStall::new(socket(&[None, None, Some(3), None, None]), LIMIT);
    assert!(write(&mut io).is_pending());
    tokio::time::advance(LIMIT - TICK).await;
    assert!(write(&mut io).is_pending());
    assert!(
        matches!(write(&mut io), Poll::Ready(Ok(3))),
        "the client read a little"
    );
    assert!(write(&mut io).is_pending());
    tokio::time::advance(LIMIT - TICK).await;
    assert!(
        write(&mut io).is_pending(),
        "the budget counts from the last progress, not the first stall"
    );
}

#[tokio::test(start_paused = true)]
async fn a_flush_that_moves_nothing_does_not_restart_a_blocked_write() {
    let mut io = WriteStall::new(socket(&[]), LIMIT);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(write(&mut io).is_pending());
    tokio::time::advance(LIMIT - TICK).await;
    assert!(Pin::new(&mut io).poll_flush(&mut cx).is_ready());
    tokio::time::advance(TICK).await;
    match write(&mut io) {
        Poll::Ready(Err(e)) => assert_eq!(e.kind(), io::ErrorKind::TimedOut),
        other => panic!("the flush restarted the blocked write's budget: {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn a_zero_byte_write_is_not_progress() {
    let mut io = WriteStall::new(socket(&[None, Some(0), None]), LIMIT);
    assert!(write(&mut io).is_pending());
    assert!(matches!(write(&mut io), Poll::Ready(Ok(0))));
    tokio::time::advance(LIMIT).await;
    match write(&mut io) {
        Poll::Ready(Err(e)) => assert_eq!(e.kind(), io::ErrorKind::TimedOut),
        other => panic!("a write that took nothing restarted the budget: {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn after_a_stall_the_stream_fails_every_write_without_trying_the_socket() {
    let mut io = WriteStall::new(socket(&[]), LIMIT);
    let counted = events::total(StreamEvent::WriteStalled);
    assert!(write(&mut io).is_pending());
    tokio::time::advance(LIMIT).await;
    match write(&mut io) {
        Poll::Ready(Err(e)) => {
            let why = e.get_ref().and_then(|s| s.downcast_ref::<WriteStallError>());
            assert_eq!(
                why.map(|w| w.limit),
                Some(LIMIT),
                "the error says it was a stall"
            );
        }
        other => panic!("expected the stall, got {other:?}"),
    }
    // Other tests in this process may count stalls too.
    assert!(events::total(StreamEvent::WriteStalled) > counted);
    let calls = io.inner.calls;
    match write(&mut io) {
        Poll::Ready(Err(e)) => assert_eq!(e.kind(), io::ErrorKind::TimedOut),
        other => panic!("expected TimedOut again, got {other:?}"),
    }
    assert_eq!(
        io.inner.calls, calls,
        "a stalled stream is not written to again"
    );
}

impl AsyncRead for Socket {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

#[test]
fn a_write_of_nothing_is_progress_and_only_ok_zero_asks() {
    assert!(wrote(|| true)(&Ok(0)));
    assert!(!wrote(|| false)(&Ok(0)));
    let unasked = || -> bool { unreachable!("asked for the length of a write that moved bytes") };
    assert!(wrote(unasked)(&Ok(1)));
    assert!(wrote(unasked)(&Err(io::Error::from(io::ErrorKind::BrokenPipe))));
}
