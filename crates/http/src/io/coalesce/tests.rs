//! Tests the coalescing layer over a scripted IO; the test under a real hyper connection needs
//! the server's connection driver, so it lives with the server.

use super::*;
use crate::plan::coalesce::{COALESCE_LIMIT, H2_PREFACE};

use mipyre_transport::stream::stall::WriteStall;
use tokio::io::AsyncWriteExt;

fn run<F: std::future::Future<Output = ()>>(test: F) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tokio::task::LocalSet::new().block_on(&rt, test);
}

/// Accepts at most `per_call` bytes per write and refuses every other write with `Pending`,
/// waking the task at once like a socket that drains immediately.
struct ChokedIo {
    written: Vec<u8>,
    per_call: usize,
    refuse_next: bool,
    writes: usize,
}

impl AsyncRead for ChokedIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for ChokedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.refuse_next {
            self.refuse_next = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.refuse_next = true;
        let n = buf.len().min(self.per_call);
        self.written.extend_from_slice(&buf[..n]);
        self.writes += 1;
        Poll::Ready(Ok(n))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn partial_and_refused_writes_keep_the_rest_and_its_order() {
    run(async {
        let choked = ChokedIo {
            written: Vec::new(),
            per_call: 7,
            refuse_next: true,
            writes: 0,
        };
        let (mut io, _flusher) = coalesce(choked);
        let mut want = Vec::new();
        for i in 0..20 {
            let piece = format!("piece {i};");
            io.write_all(piece.as_bytes()).await.unwrap();
            want.extend_from_slice(piece.as_bytes());
        }
        // A write past the limit drains what is kept, then goes straight through.
        let big = vec![b'z'; COALESCE_LIMIT];
        io.write_all(&big).await.unwrap();
        want.extend_from_slice(&big);
        io.write_all(b"tail").await.unwrap();
        want.extend_from_slice(b"tail");
        io.shutdown().await.unwrap();
        assert_eq!(io.shared.state.lock().inner.written, want);
    });
}

#[test]
fn the_http2_preface_switches_the_layer_to_passthrough() {
    let choked = || ChokedIo {
        written: Vec::new(),
        per_call: usize::MAX,
        refuse_next: false,
        writes: 0,
    };
    let (io, _flusher) = coalesce(choked());
    let mut core = io.shared.state.lock();
    core.sniff(&H2_PREFACE[..10]);
    assert_eq!(core.conn.protocol, sniffed(PartialPreface::EMPTY, &H2_PREFACE[..10]));
    assert!(matches!(core.conn.protocol, Protocol::Sniffing(_)));
    core.sniff(&H2_PREFACE[10..]);
    assert_eq!(core.conn.protocol, Protocol::Http2);

    let (io, _flusher) = coalesce(choked());
    let mut core = io.shared.state.lock();
    core.sniff(b"POST / HTTP/1.1\r\n");
    assert_eq!(core.conn.protocol, Protocol::Http1);
}

use std::collections::VecDeque;

#[derive(Clone, Copy, Debug)]
enum WriteStep {
    Take(usize),
    /// Refuses with `Pending` and registers no waker, so the test polls again by hand.
    Full,
    Fail(io::ErrorKind),
}

#[derive(Default)]
struct Wire {
    landed: Vec<u8>,
    /// Makes written bytes wait in `buffered` until a flush lands them, as a TLS stream does.
    buffers: bool,
    buffered: Vec<u8>,
    writes: VecDeque<WriteStep>,
    flushes_refused: usize,
    reads: VecDeque<Option<Vec<u8>>>,
    write_calls: usize,
}

struct ScriptedIo(Arc<Mutex<Wire>>);

fn wire() -> (ScriptedIo, Arc<Mutex<Wire>>) {
    let wire = Arc::new(Mutex::new(Wire::default()));
    (ScriptedIo(Arc::clone(&wire)), wire)
}

impl AsyncRead for ScriptedIo {
    /// Delivers the next scripted read, where `None` is EOF and an empty script is `Pending`.
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.0.lock().reads.pop_front() {
            Some(Some(bytes)) => {
                buf.put_slice(&bytes);
                Poll::Ready(Ok(()))
            }
            Some(None) => Poll::Ready(Ok(())),
            None => Poll::Pending,
        }
    }
}

impl AsyncWrite for ScriptedIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut wire = self.0.lock();
        wire.write_calls += 1;
        match wire
            .writes
            .pop_front()
            .unwrap_or(WriteStep::Take(usize::MAX))
        {
            WriteStep::Take(max) => {
                let n = buf.len().min(max);
                if wire.buffers {
                    wire.buffered.extend_from_slice(&buf[..n]);
                } else {
                    wire.landed.extend_from_slice(&buf[..n]);
                }
                Poll::Ready(Ok(n))
            }
            WriteStep::Full => Poll::Pending,
            WriteStep::Fail(kind) => Poll::Ready(Err(kind.into())),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut wire = self.0.lock();
        if wire.flushes_refused > 0 {
            wire.flushes_refused -= 1;
            return Poll::Pending;
        }
        let buffered = std::mem::take(&mut wire.buffered);
        wire.landed.extend_from_slice(&buffered);
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

#[derive(Clone)]
enum Call {
    Write(&'static [u8]),
    Flush,
    Read,
}

/// Stands in for hyper's connection future: each poll makes the next group of calls, and the
/// last group ends it. Results are recorded, not acted on, as hyper ignores a `Pending`.
struct Conn<'a> {
    io: &'a mut CoalescingIo<ScriptedIo>,
    polls: VecDeque<Vec<Call>>,
    errors: Vec<io::ErrorKind>,
}

impl std::future::Future for Conn<'_> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        let calls = this.polls.pop_front().unwrap_or_default();
        for call in calls {
            let failed = match call {
                Call::Write(bytes) => match Pin::new(&mut *this.io).poll_write(cx, bytes) {
                    Poll::Ready(Err(e)) => Some(e.kind()),
                    _ => None,
                },
                Call::Flush => match Pin::new(&mut *this.io).poll_flush(cx) {
                    Poll::Ready(Err(e)) => Some(e.kind()),
                    _ => None,
                },
                Call::Read => {
                    let mut space = [0u8; 64];
                    let mut buf = ReadBuf::new(&mut space);
                    match Pin::new(&mut *this.io).poll_read(cx, &mut buf) {
                        Poll::Ready(Err(e)) => Some(e.kind()),
                        _ => None,
                    }
                }
            };
            this.errors.extend(failed);
        }
        if this.polls.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

fn bracket(conn: &mut Conn<'_>, flusher: &Flusher<ScriptedIo>) -> Poll<()> {
    let mut cx = Context::from_waker(Waker::noop());
    let pinned = Pin::new(conn);
    std::future::Future::poll(Pin::new(&mut CoalescedPoll::new(pinned, flusher)), &mut cx)
}

fn polls(groups: &[&[Call]]) -> VecDeque<Vec<Call>> {
    groups.iter().map(|g| g.to_vec()).collect()
}

/// While the second request is still unread, an answer is written into a batch the layer keeps.
const PIPELINED: &[u8] = b"GET /a HTTP/1.1\r\n\r\nGET /b HTTP/1.1\r\n\r\n";

#[test]
fn the_end_of_a_poll_writes_what_the_poll_kept_in_one_write() {
    let (io, wire) = wire();
    let (mut io, flusher) = coalesce(io);
    let mut conn = Conn {
        io: &mut io,
        polls: polls(&[&[Call::Read, Call::Write(b"one;"), Call::Write(b"two;")], &[]]),
        errors: Vec::new(),
    };
    wire.lock().reads.push_back(Some(PIPELINED.to_vec()));
    assert!(bracket(&mut conn, &flusher).is_pending());
    let wire = wire.lock();
    assert_eq!(wire.landed, b"one;two;");
    assert_eq!(wire.write_calls, 1);
}

#[test]
fn a_flush_while_requests_are_unread_waits_and_a_pending_read_lets_the_next_flush_write() {
    let (io, wire) = wire();
    wire.lock()
        .reads
        .push_back(Some(b"GET / HTTP/1.1\r\n\r\n".to_vec()));
    let (mut io, flusher) = coalesce(io);
    let mut conn = Conn {
        io: &mut io,
        // The first answer is kept, because no read has yet found the client's queue empty.
        polls: polls(&[&[Call::Read, Call::Write(b"first;"), Call::Flush], &[]]),
        errors: Vec::new(),
    };
    {
        let mut cx = Context::from_waker(Waker::noop());
        flusher.enter();
        let polled = std::future::Future::poll(Pin::new(&mut conn), &mut cx);
        assert!(polled.is_pending());
        assert_eq!(
            wire.lock().write_calls,
            0,
            "kept while a request may be unread"
        );
        let mut space = [0u8; 8];
        let mut buf = ReadBuf::new(&mut space);
        assert!(Pin::new(&mut *conn.io)
            .poll_read(&mut cx, &mut buf)
            .is_pending());
        assert!(Pin::new(&mut *conn.io).poll_flush(&mut cx).is_ready());
        assert_eq!(
            wire.lock().landed,
            b"first;",
            "written once nothing is queued"
        );
        flusher.leave(&mut cx);
    }
}

#[test]
fn bytes_a_buffering_inner_holds_are_flushed_when_the_connection_ends() {
    let (io, wire) = wire();
    wire.lock().buffers = true;
    wire.lock()
        .reads
        .push_back(Some(b"GET /ws HTTP/1.1\r\n\r\n".to_vec()));
    let (mut io, flusher) = coalesce(io);
    let mut conn = Conn {
        io: &mut io,
        // The 101 is kept because a request was just read, and the connection future ends
        // without a shutdown.
        polls: polls(&[&[
            Call::Read,
            Call::Write(b"HTTP/1.1 101\r\n\r\n"),
            Call::Flush,
        ]]),
        errors: Vec::new(),
    };
    assert!(bracket(&mut conn, &flusher).is_ready());
    assert_eq!(wire.lock().landed, b"HTTP/1.1 101\r\n\r\n");
}

#[test]
fn an_upgraded_stream_carries_on_a_write_out_the_connection_left_blocked() {
    let (io, wire) = wire();
    {
        let mut w = wire.lock();
        w.buffers = true;
        w.reads
            .push_back(Some(b"GET /ws HTTP/1.1\r\n\r\n".to_vec()));
        w.writes.push_back(WriteStep::Full);
    }
    let (mut io, flusher) = coalesce(io);
    let mut conn = Conn {
        io: &mut io,
        polls: polls(&[&[
            Call::Read,
            Call::Write(b"HTTP/1.1 101\r\n\r\n"),
            Call::Flush,
        ]]),
        errors: Vec::new(),
    };
    assert!(bracket(&mut conn, &flusher).is_ready());
    assert!(wire.lock().landed.is_empty(), "the socket was full");
    // A WebSocket task waits for the client, so its read alone has to send the 101, or both
    // sides wait for each other.
    let mut cx = Context::from_waker(Waker::noop());
    let mut space = [0u8; 8];
    let mut buf = ReadBuf::new(&mut space);
    assert!(Pin::new(&mut io).poll_read(&mut cx, &mut buf).is_pending());
    assert_eq!(wire.lock().landed, b"HTTP/1.1 101\r\n\r\n");
}

#[test]
fn partial_writes_with_new_bytes_keep_the_buffer_bounded() {
    let (io, wire) = wire();
    let (mut io, _flusher) = coalesce(io);
    let mut cx = Context::from_waker(Waker::noop());
    let mut want = Vec::new();
    for _ in 0..20_000 {
        // The client reads exactly as fast as the server writes, so every flush gets out ten
        // bytes and the socket is full again.
        wire.lock()
            .writes
            .extend([WriteStep::Take(10), WriteStep::Full]);
        if let Poll::Ready(Ok(n)) = Pin::new(&mut io).poll_write(&mut cx, b"0123456789") {
            want.extend_from_slice(&b"0123456789"[..n]);
        }
        let flushed = Pin::new(&mut io).poll_flush(&mut cx);
        assert!(!matches!(flushed, Poll::Ready(Err(_))), "{flushed:?}");
        assert!(
            io.shared.state.lock().out.len() <= 2 * COALESCE_LIMIT,
            "kept {} bytes",
            io.shared.state.lock().out.len()
        );
    }
    wire.lock().writes.clear();
    assert!(Pin::new(&mut io).poll_flush(&mut cx).is_ready());
    assert_eq!(wire.lock().landed, want);
}

#[test]
fn a_failed_write_is_tried_once_and_reported_once() {
    let (io, wire) = wire();
    wire.lock()
        .writes
        .push_back(WriteStep::Fail(io::ErrorKind::BrokenPipe));
    let (mut io, flusher) = coalesce(io);
    let mut conn = Conn {
        io: &mut io,
        polls: polls(&[&[Call::Write(b"response"), Call::Flush]]),
        errors: Vec::new(),
    };
    assert!(bracket(&mut conn, &flusher).is_ready());
    assert_eq!(
        conn.errors,
        [io::ErrorKind::BrokenPipe],
        "the connection sees the error"
    );
    drop(conn);
    drop(io);
    drop(flusher);
    assert_eq!(
        wire.lock().write_calls,
        1,
        "neither the poll's end nor the drop writes again"
    );
}

#[test]
fn a_poll_whose_write_came_back_pending_does_not_write_again_at_its_end() {
    let (io, wire) = wire();
    let (mut io, flusher) = coalesce(io);
    let mut conn = Conn {
        io: &mut io,
        polls: polls(&[&[Call::Read, Call::Write(b"response")], &[Call::Flush], &[]]),
        errors: Vec::new(),
    };
    wire.lock().reads.push_back(Some(PIPELINED.to_vec()));
    wire.lock().writes.extend([WriteStep::Full; 8]);
    assert!(bracket(&mut conn, &flusher).is_pending());
    assert_eq!(
        wire.lock().write_calls,
        1,
        "the first poll's end tries once"
    );
    assert!(bracket(&mut conn, &flusher).is_pending());
    assert_eq!(
        wire.lock().write_calls,
        2,
        "the flush's write came back pending: the poll's end does not try again"
    );
}

#[test]
fn a_switch_to_http2_writes_a_kept_tail_before_passing_through() {
    let (io, wire) = wire();
    let (mut io, flusher) = coalesce(io);
    let mut cx = Context::from_waker(Waker::noop());
    // Bytes written while sniffing are kept, half of them go out before the socket fills, and
    // the socket is still full when the read that completes the preface tries to continue.
    wire.lock()
        .writes
        .extend([WriteStep::Take(4), WriteStep::Full, WriteStep::Full]);
    wire.lock().reads.push_back(Some(H2_PREFACE[..10].to_vec()));
    flusher.enter();
    let mut space = [0u8; 64];
    let mut buf = ReadBuf::new(&mut space);
    assert!(Pin::new(&mut io).poll_read(&mut cx, &mut buf).is_ready());
    assert!(Pin::new(&mut io)
        .poll_write(&mut cx, b"SETTINGS")
        .is_ready());
    let mut buf = ReadBuf::new(&mut space);
    assert!(Pin::new(&mut io).poll_read(&mut cx, &mut buf).is_pending());
    assert!(Pin::new(&mut io).poll_flush(&mut cx).is_pending());
    flusher.leave(&mut cx);
    wire.lock().reads.push_back(Some(H2_PREFACE[10..].to_vec()));
    let mut space = [0u8; 64];
    let mut buf = ReadBuf::new(&mut space);
    assert!(Pin::new(&mut io).poll_read(&mut cx, &mut buf).is_ready());
    assert_eq!(io.shared.state.lock().conn.protocol, Protocol::Http2);
    let mut written = 0;
    while written == 0 {
        if let Poll::Ready(Ok(n)) = Pin::new(&mut io).poll_write(&mut cx, b"WINDOW") {
            written = n;
        }
    }
    assert_eq!(wire.lock().landed, b"SETTINGSWINDOW");
}

/// A poll in between that makes no call does not write again, because the kept bytes went with
/// the failure.
#[test]
fn a_write_out_a_poll_end_failed_is_reported_once_by_the_next_call() {
    let (io, wire) = wire();
    wire.lock()
        .writes
        .push_back(WriteStep::Fail(io::ErrorKind::BrokenPipe));
    wire.lock().reads.push_back(Some(PIPELINED.to_vec()));
    let (mut io, flusher) = coalesce(io);
    let mut conn = Conn {
        io: &mut io,
        polls: polls(&[
            &[Call::Read, Call::Write(b"response")],
            &[],
            &[Call::Flush, Call::Flush],
            &[],
        ]),
        errors: Vec::new(),
    };
    assert!(bracket(&mut conn, &flusher).is_pending());
    assert!(conn.errors.is_empty(), "the poll's end met the error, not the connection");
    assert!(bracket(&mut conn, &flusher).is_pending());
    assert_eq!(wire.lock().write_calls, 1, "a poll with no call does not write again");
    assert!(bracket(&mut conn, &flusher).is_pending());
    assert_eq!(conn.errors, [io::ErrorKind::BrokenPipe], "reported by the next call, once");
    assert_eq!(wire.lock().write_calls, 1);
}

#[test]
fn finish_writes_what_a_connection_that_ended_without_a_shutdown_left_blocked() {
    run(async {
        let (io, wire) = wire();
        wire.lock().writes.push_back(WriteStep::Full);
        wire.lock().reads.push_back(Some(PIPELINED.to_vec()));
        let (mut io, flusher) = coalesce(io);
        let mut conn = Conn {
            io: &mut io,
            polls: polls(&[&[Call::Read, Call::Write(b"HTTP/1.1 200 OK\r\n\r\n")]]),
            errors: Vec::new(),
        };
        assert!(bracket(&mut conn, &flusher).is_ready());
        drop(conn);
        drop(io);
        assert!(
            wire.lock().landed.is_empty(),
            "the socket was full at the poll's end"
        );
        flusher.finish().await.unwrap();
        assert_eq!(wire.lock().landed, b"HTTP/1.1 200 OK\r\n\r\n");
    });
}

#[test]
fn finish_reports_a_write_out_the_last_poll_failed_and_nothing_else_does() {
    run(async {
        let (io, wire) = wire();
        wire.lock()
            .writes
            .push_back(WriteStep::Fail(io::ErrorKind::PermissionDenied));
        wire.lock().reads.push_back(Some(PIPELINED.to_vec()));
        let (mut io, flusher) = coalesce(io);
        let mut conn = Conn {
            io: &mut io,
            polls: polls(&[&[Call::Read, Call::Write(b"response")]]),
            errors: Vec::new(),
        };
        assert!(bracket(&mut conn, &flusher).is_ready());
        assert!(
            conn.errors.is_empty(),
            "the poll's end met the error, not the connection"
        );
        drop(conn);
        drop(io);
        let e = flusher.finish().await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(wire.lock().write_calls, 1);
    });
}

#[test]
fn finish_leaves_an_upgraded_stream_to_its_new_owner() {
    run(async {
        let (io, wire) = wire();
        wire.lock().writes.push_back(WriteStep::Full);
        wire.lock().reads.push_back(Some(PIPELINED.to_vec()));
        let (mut io, flusher) = coalesce(io);
        let mut conn = Conn {
            io: &mut io,
            polls: polls(&[&[Call::Read, Call::Write(b"HTTP/1.1 101\r\n\r\n")]]),
            errors: Vec::new(),
        };
        assert!(bracket(&mut conn, &flusher).is_ready());
        drop(conn);
        // The upgrade's owner still holds `io`, so finish does not wait on it.
        flusher.finish().await.unwrap();
        io.flush().await.unwrap();
        assert_eq!(wire.lock().landed, b"HTTP/1.1 101\r\n\r\n");
    });
}

#[test]
fn a_keep_alive_connection_reuses_its_buffer_for_batches_up_to_the_limit() {
    let (io, wire) = wire();
    let (mut io, flusher) = coalesce(io);
    let mut cx = Context::from_waker(Waker::noop());
    let body = vec![b'b'; 18 * 1024];
    let mut capacity = None;
    for _ in 0..3 {
        flusher.enter();
        wire.lock().reads.push_back(Some(PIPELINED.to_vec()));
        let mut space = [0u8; 64];
        let mut buf = ReadBuf::new(&mut space);
        assert!(Pin::new(&mut io).poll_read(&mut cx, &mut buf).is_ready());
        assert!(matches!(
            Pin::new(&mut io).poll_write(&mut cx, &body),
            Poll::Ready(Ok(n)) if n == body.len()
        ));
        let mut buf = ReadBuf::new(&mut space);
        assert!(Pin::new(&mut io).poll_read(&mut cx, &mut buf).is_pending());
        assert!(matches!(
            Pin::new(&mut io).poll_flush(&mut cx),
            Poll::Ready(Ok(()))
        ));
        flusher.leave(&mut cx);
        let kept = io.shared.state.lock().out.capacity();
        assert!(
            kept >= body.len(),
            "the written-out buffer was freed (capacity {kept})"
        );
        assert_eq!(
            *capacity.get_or_insert(kept),
            kept,
            "the buffer was reallocated"
        );
    }
}

fn captured_log(f: impl FnOnce()) -> String {
    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Buf {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            self.0.lock().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let buf = Buf::default();
    let sink = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || sink.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, f);
    let bytes = buf.0.lock().clone();
    String::from_utf8(bytes).unwrap()
}

/// A second warning inside the throttle's period is held back.
#[test]
fn a_dropped_connections_failed_last_write_out_is_a_warning_with_the_error() {
    let throttles = (LogThrottle::new(DROP_WARNING_PERIOD), LogThrottle::new(DROP_WARNING_PERIOD));
    let now = std::time::Duration::from_secs(1);
    let failed = || Poll::Ready(Err(io::Error::new(io::ErrorKind::PermissionDenied, "egress refused")));
    let log = captured_log(|| say_dropped_write_out(failed(), 12, [&throttles.0, &throttles.1], now));
    let line = log.lines().find(|l| l.contains("egress refused")).expect(&log);
    assert!(line.contains("WARN"), "{line}");
    assert!(line.contains("kept=12"), "{line}");

    let log = captured_log(|| say_dropped_write_out(failed(), 12, [&throttles.0, &throttles.1], now));
    assert!(!log.contains("WARN"), "held back inside the period: {log}");
}

#[test]
fn a_dropped_connections_full_socket_warns_and_a_gone_peer_does_not() {
    let now = std::time::Duration::from_secs(1);
    let log = captured_log(|| say_dropped_write_out(Poll::Pending, 5, [&LogThrottle::new(DROP_WARNING_PERIOD), &LogThrottle::new(DROP_WARNING_PERIOD)], now));
    assert!(log.lines().any(|l| l.contains("WARN") && l.contains("socket was full")), "{log}");

    let reset = Poll::Ready(Err(io::Error::from(io::ErrorKind::ConnectionReset)));
    let log = captured_log(|| say_dropped_write_out(reset, 5, [&LogThrottle::new(DROP_WARNING_PERIOD), &LogThrottle::new(DROP_WARNING_PERIOD)], now));
    assert!(!log.contains("WARN"), "{log}");
}

#[test]
fn a_full_sockets_warning_does_not_hold_back_a_failed_writes() {
    let throttles = (LogThrottle::new(DROP_WARNING_PERIOD), LogThrottle::new(DROP_WARNING_PERIOD));
    let now = std::time::Duration::from_secs(1);
    let log = captured_log(|| say_dropped_write_out(Poll::Pending, 5, [&throttles.0, &throttles.1], now));
    assert!(log.lines().any(|l| l.contains("WARN") && l.contains("socket was full")), "{log}");

    let failed = Poll::Ready(Err(io::Error::new(io::ErrorKind::PermissionDenied, "egress refused")));
    let log = captured_log(|| say_dropped_write_out(failed, 5, [&throttles.0, &throttles.1], now));
    let line = log.lines().find(|l| l.contains("egress refused")).expect(&log);
    assert!(line.contains("WARN") && line.contains("held=0"), "{line}");
}

/// Takes no write, as a peer that stopped reading.
struct NeverWrites;

impl AsyncRead for NeverWrites {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for NeverWrites {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Pending
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

const STALL_LIMIT: std::time::Duration = std::time::Duration::from_secs(10);

#[tokio::test(start_paused = true)]
async fn under_the_coalescing_layer_a_stall_is_reported_once_and_not_written_again() {
    use std::future::Future as _;

    let (mut io, flusher) = coalesce(WriteStall::new(NeverWrites, STALL_LIMIT));
    let mut cx = Context::from_waker(Waker::noop());
    let errors = std::cell::RefCell::new(Vec::new());
    let met = &errors;
    let mut written = false;
    // Like hyper, the connection retries its write on each poll until the socket takes it, and
    // then flushes on every poll.
    let mut conn = Box::pin(std::future::poll_fn(move |cx| {
        if !written {
            match Pin::new(&mut io).poll_write(cx, b"response") {
                Poll::Ready(Ok(_)) => written = true,
                Poll::Ready(Err(e)) => {
                    met.borrow_mut().push(e.kind());
                    return Poll::Ready(());
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        if let Poll::Ready(Err(e)) = Pin::new(&mut io).poll_flush(cx) {
            met.borrow_mut().push(e.kind());
            return Poll::Ready(());
        }
        Poll::Pending
    }));
    assert!(Pin::new(&mut CoalescedPoll::new(conn.as_mut(), &flusher))
        .poll(&mut cx)
        .is_pending());
    tokio::time::advance(STALL_LIMIT).await;
    assert!(Pin::new(&mut CoalescedPoll::new(conn.as_mut(), &flusher))
        .poll(&mut cx)
        .is_ready());
    drop(conn);
    assert_eq!(*errors.borrow(), [io::ErrorKind::TimedOut]);
    flusher
        .finish()
        .await
        .expect("the stall was reported to the connection, not again");
}
