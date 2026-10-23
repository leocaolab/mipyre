//! Tests the coalescing layer under the real hyper connection, since what it decides is the order
//! of hyper's reads, writes and flushes within one poll.
//!
//! The transport is an in-memory pipe and the clock is paused, so a response "sent at once"
//! arrives before the clock moves at all and the write timeout elapses without waiting for it. The
//! layer alone, over a scripted IO, is tested in `crates/http/src/io/coalesce/tests.rs`.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::service::service_fn;
use proptest::strategy::Strategy as _;
use tokio::io::DuplexStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::serve::conn::{drive_connection, drive_local_connection, ConnectionTimeouts, LocalExec};
use mipyre_http::io::body::ResponseBody;
use mipyre_http::plan::limits::HEADER_READ_TIMEOUT;
use mipyre_transport::stream::stall::WRITE_TIMEOUT;

/// How long the client waits for a response it expects; the clock is paused, so this is how far
/// the runtime may jump while nothing else is ready, never time the test spends.
const PATIENCE: Duration = Duration::from_secs(2);

/// Each direction of the in-memory connection holds this much: past it a write waits for the
/// peer to read, as a full socket buffer does.
const PIPE_BYTES: usize = 64 * 1024;

/// Counts the writes that reach the server's end of the pipe and those that found it full. With
/// `vectored` it takes several slices in one write, as a socket's `writev` does; the pipe itself
/// takes one slice per write.
struct CountingIo {
    inner: DuplexStream,
    writes: Arc<AtomicUsize>,
    blocked: Arc<AtomicUsize>,
    vectored: bool,
}

impl CountingIo {
    fn count(&self, r: &Poll<io::Result<usize>>) {
        match r {
            Poll::Ready(Ok(n)) if *n > 0 => {
                self.writes.fetch_add(1, Ordering::Relaxed);
            }
            Poll::Pending => {
                self.blocked.fetch_add(1, Ordering::Relaxed);
            }
            Poll::Ready(_) => {}
        }
    }
}

impl AsyncRead for CountingIo {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for CountingIo {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let r = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.count(&r);
        r
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let r = if self.vectored {
            // This writes all the slices at once, as far as the pipe takes them.
            let joined: Vec<u8> = bufs.iter().flat_map(|b| b.iter().copied()).collect();
            Pin::new(&mut self.inner).poll_write(cx, &joined)
        } else {
            Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
        };
        self.count(&r);
        r
    }
    fn is_write_vectored(&self) -> bool {
        self.vectored
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

fn full(body: impl Into<Bytes>) -> hyper::Response<ResponseBody> {
    hyper::Response::new(ResponseBody::Full(Full::new(body.into())))
}

/// The body `/big/<n>` answers: `n` bytes that differ from their neighbours, so a byte lost,
/// repeated or out of order shows.
fn big_body(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}

/// Answers `/slow/<ms>` after a sleep, sends `first` and then `second` 300 ms later on `/stream`,
/// never finishes `/stalled`'s body, serves [`big_body`] on `/big/<n>`, switches protocols on
/// `/upgrade` and then writes `after`, refuses `/refuse` with a 413 without reading its body, and
/// echoes any other path.
async fn answer(mut req: hyper::Request<Incoming>) -> Result<hyper::Response<ResponseBody>, hyper::Error> {
    let path = req.uri().path().to_owned();
    if let Some(n) = path.strip_prefix("/big/") {
        return Ok(full(big_body(n.parse().unwrap())));
    }
    if path == "/refuse" {
        let mut resp = full("");
        *resp.status_mut() = hyper::StatusCode::PAYLOAD_TOO_LARGE;
        return Ok(resp);
    }
    if path == "/upgrade" {
        let upgrade = hyper::upgrade::on(&mut req);
        tokio::task::spawn_local(async move {
            let mut upgraded = hyper_util::rt::TokioIo::new(upgrade.await.unwrap());
            upgraded.write_all(b"after").await.unwrap();
            upgraded.flush().await.unwrap();
        });
        let mut resp = hyper::Response::new(ResponseBody::empty());
        *resp.status_mut() = hyper::StatusCode::SWITCHING_PROTOCOLS;
        resp.headers_mut().insert(
            hyper::header::CONNECTION,
            hyper::header::HeaderValue::from_static("upgrade"),
        );
        resp.headers_mut()
            .insert(hyper::header::UPGRADE, hyper::header::HeaderValue::from_static("test"));
        return Ok(resp);
    }
    if let Some(ms) = path.strip_prefix("/slow/") {
        tokio::time::sleep(Duration::from_millis(ms.parse().unwrap())).await;
        return Ok(full(path));
    }
    if path == "/stalled" {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, std::convert::Infallible>>(2);
        tokio::task::spawn_local(async move {
            // Far more than the pipe holds, so the server's write blocks.
            tx.send(Ok(Frame::data(Bytes::from(vec![b'x'; 64 << 20]))))
                .await
                .unwrap();
            // The body never ends: this keeps `tx` open for the whole test.
            std::future::pending::<()>().await;
        });
        let body = StreamBody::new(tokio_stream::wrappers::ReceiverStream::new(rx));
        return Ok(hyper::Response::new(ResponseBody::boxed(body)));
    }
    if path == "/stream" {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, std::convert::Infallible>>(2);
        tokio::task::spawn_local(async move {
            tx.send(Ok(Frame::data(Bytes::from_static(b"first")))).await.unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
            #[expect(
                clippy::let_underscore_must_use,
                reason = "the client may have read what it needed and closed by now"
            )]
            let _ = tx.send(Ok(Frame::data(Bytes::from_static(b"second")))).await;
        });
        let body = StreamBody::new(tokio_stream::wrappers::ReceiverStream::new(rx));
        return Ok(hyper::Response::new(ResponseBody::boxed(body)));
    }
    Ok(full(path))
}

struct Server {
    client: DuplexStream,
    writes: Arc<AtomicUsize>,
    blocked: Arc<AtomicUsize>,
    token: CancellationToken,
    done: tokio::task::JoinHandle<()>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Writes {
    Coalesced,
    /// It coalesces on one thread without a lock or upgrades (`drive_local_connection`).
    CoalescedLocal,
    /// hyper writes alone, for comparison.
    Direct,
}

/// Serves one connection on the current `LocalSet`, over a pipe that takes one slice per write.
async fn serve_one(writes_mode: Writes) -> Server {
    serve_over(writes_mode, false).await
}

/// Works like [`serve_one`], over a pipe that takes several slices in one write when `vectored`.
async fn serve_over(writes_mode: Writes, vectored: bool) -> Server {
    let (client, stream) = tokio::io::duplex(PIPE_BYTES);
    let writes = Arc::new(AtomicUsize::new(0));
    let blocked = Arc::new(AtomicUsize::new(0));
    let io = CountingIo {
        inner: stream,
        writes: Arc::clone(&writes),
        blocked: Arc::clone(&blocked),
        vectored,
    };
    let token = CancellationToken::new();
    let done = match writes_mode {
        Writes::Coalesced => tokio::task::spawn_local(drive_connection(
            io,
            service_fn(answer),
            LocalExec,
            ConnectionTimeouts::DEFAULT,
            token.clone().cancelled_owned(),
        )),
        Writes::CoalescedLocal => tokio::task::spawn_local(drive_local_connection(
            io,
            service_fn(answer),
            ConnectionTimeouts::DEFAULT,
            token.clone().cancelled_owned(),
        )),
        // hyper alone is the baseline, one write per response; how it ends (a request the client
        // cut short) is not what the comparison is about.
        Writes::Direct => tokio::task::spawn_local(async move {
            let mut builder = hyper_util::server::conn::auto::Builder::new(LocalExec);
            builder.http1().timer(hyper_util::rt::TokioTimer::new());
            let _ended = builder
                .serve_connection(hyper_util::rt::TokioIo::new(io), service_fn(answer))
                .await;
        }),
    };
    Server {
        client,
        writes,
        blocked,
        token,
        done,
    }
}

fn get(path: &str) -> String {
    format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
}

/// Holds a response's body as the client read it, de-chunked.
#[derive(Debug)]
struct Got {
    body: Vec<u8>,
}

/// Parses complete responses off the front of `buf`.
fn parse_responses(buf: &mut Vec<u8>, out: &mut Vec<Got>) {
    loop {
        let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
            return;
        };
        let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
        let rest = end + 4;
        if let Some(len) = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .map(|v| v.trim().parse::<usize>().unwrap())
        {
            if buf.len() < rest + len {
                return;
            }
            out.push(Got {
                body: buf[rest..rest + len].to_vec(),
            });
            buf.drain(..rest + len);
        } else {
            assert!(head.contains("transfer-encoding: chunked"), "{head}");
            let mut body = Vec::new();
            let mut at = rest;
            loop {
                let Some(nl) = buf[at..].windows(2).position(|w| w == b"\r\n") else {
                    return;
                };
                let size = usize::from_str_radix(std::str::from_utf8(&buf[at..at + nl]).unwrap(), 16).unwrap();
                let data = at + nl + 2;
                if buf.len() < data + size + 2 {
                    return;
                }
                body.extend_from_slice(&buf[data..data + size]);
                at = data + size + 2;
                if size == 0 {
                    break;
                }
            }
            out.push(Got { body });
            buf.drain(..at);
        }
    }
}

/// Reads until `n` complete responses have arrived, panicking once `within` passes.
async fn read_responses(client: &mut DuplexStream, n: usize, within: Duration) -> Vec<Got> {
    let mut buf = Vec::new();
    let mut got = Vec::new();
    let deadline = tokio::time::Instant::now() + within;
    let mut chunk = vec![0u8; 64 * 1024];
    while got.len() < n {
        let read = tokio::time::timeout_at(deadline, client.read(&mut chunk))
            .await
            .unwrap_or_else(|_| panic!("{} of {n} responses within {within:?}; buffered {buf:?}", got.len()))
            .unwrap();
        assert!(read > 0, "EOF after {} of {n} responses", got.len());
        buf.extend_from_slice(&chunk[..read]);
        parse_responses(&mut buf, &mut got);
    }
    got
}

fn run<F: std::future::Future<Output = ()>>(test: F) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    tokio::task::LocalSet::new().block_on(&rt, test);
}

#[test]
fn a_client_that_never_reads_is_disconnected_after_the_write_timeout() {
    // `/stalled` answers with a body that is never finished, its first frame bigger than the
    // pipe holds. Once the server is blocked writing, hyper's header timeout is not
    // running (it covers only waiting for the next request head), so with no write timeout
    // the connection stays open for good.
    run(async {
        let mut s = serve_one(Writes::Coalesced).await;
        s.client.write_all(get("/stalled").as_bytes()).await.unwrap();
        // The budget restarts at each progress, so allow for the writes that got through first.
        tokio::time::timeout(2 * WRITE_TIMEOUT, &mut s.done)
            .await
            .expect("the connection is still open after the write timeout")
            .unwrap();
    });
}

#[test]
fn a_response_followed_by_half_a_request_is_sent_at_once() {
    run(async {
        let mut s = serve_one(Writes::Coalesced).await;
        let sent = Instant::now();
        s.client
            .write_all(format!("{}GET /b HT", get("/a")).as_bytes())
            .await
            .unwrap();
        let got = read_responses(&mut s.client, 1, PATIENCE).await;
        assert_eq!(got[0].body, b"/a");
        assert_eq!(sent.elapsed(), Duration::ZERO, "sent only once the clock moved");
        // The rest of the second request completes it.
        s.client.write_all(b"TP/1.1\r\nHost: 127.0.0.1\r\n\r\n").await.unwrap();
        let got = read_responses(&mut s.client, 1, PATIENCE).await;
        assert_eq!(got[0].body, b"/b");
    });
}

#[test]
fn sixteen_pipelined_requests_are_answered_in_order_in_one_write() {
    for (mode, expected_writes) in [(Writes::Coalesced, 1), (Writes::Direct, 16)] {
        run(async move {
            let mut s = serve_one(mode).await;
            let batch: String = (0..16).map(|i| get(&format!("/{i}"))).collect();
            s.client.write_all(batch.as_bytes()).await.unwrap();
            let got = read_responses(&mut s.client, 16, PATIENCE).await;
            let bodies: Vec<String> = got.iter().map(|g| String::from_utf8(g.body.clone()).unwrap()).collect();
            let want: Vec<String> = (0..16).map(|i| format!("/{i}")).collect();
            assert_eq!(bodies, want);
            assert_eq!(s.writes.load(Ordering::Relaxed), expected_writes);
        });
    }
}

#[test]
fn headers_dribbled_a_byte_at_a_time_still_complete() {
    run(async {
        let mut s = serve_one(Writes::Coalesced).await;
        for b in get("/slowclient").bytes() {
            s.client.write_all(&[b]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        let got = read_responses(&mut s.client, 1, PATIENCE).await;
        assert_eq!(got[0].body, b"/slowclient");
    });
}

#[test]
fn connection_close_and_client_eof_lose_no_kept_response() {
    run(async {
        // The last request asks to close: hyper shuts the connection down after it.
        let mut s = serve_one(Writes::Coalesced).await;
        let batch = format!(
            "{}{}GET /3 HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
            get("/1"),
            get("/2")
        );
        s.client.write_all(batch.as_bytes()).await.unwrap();
        let mut all = Vec::new();
        tokio::time::timeout(PATIENCE, s.client.read_to_end(&mut all))
            .await
            .unwrap()
            .unwrap();
        let mut got = Vec::new();
        parse_responses(&mut all, &mut got);
        assert_eq!(
            got.iter().map(|g| g.body.clone()).collect::<Vec<_>>(),
            [b"/1".to_vec(), b"/2".to_vec(), b"/3".to_vec()]
        );

        // The client sends three requests and closes its write side. hyper has no half-close and
        // probes for EOF once it has parsed the third; if the EOF is already there, hyper drops
        // the third as cut short. Either way the client gets /1 and /2, with and without the
        // layer, so the layer adds no loss of its own.
        for mode in [Writes::Direct, Writes::Coalesced] {
            let mut s = serve_one(mode).await;
            let batch = format!("{}{}{}", get("/1"), get("/2"), get("/3"));
            s.client.write_all(batch.as_bytes()).await.unwrap();
            s.client.shutdown().await.unwrap();
            let mut all = Vec::new();
            tokio::time::timeout(PATIENCE, s.client.read_to_end(&mut all))
                .await
                .unwrap()
                .unwrap();
            let mut got = Vec::new();
            parse_responses(&mut all, &mut got);
            let bodies: Vec<Vec<u8>> = got.into_iter().map(|g| g.body).collect();
            let all3 = [b"/1".to_vec(), b"/2".to_vec(), b"/3".to_vec()];
            assert!(
                bodies[..] == all3[..2] || bodies[..] == all3[..],
                "{mode:?}: {bodies:?}"
            );
            s.done.await.unwrap();
        }
    });
}

#[test]
fn graceful_shutdown_sends_what_was_kept_and_the_in_flight_response() {
    run(async {
        let mut s = serve_one(Writes::Coalesced).await;
        let batch = format!("{}{}", get("/a"), get("/slow/200"));
        s.client.write_all(batch.as_bytes()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        s.token.cancel();
        let mut all = Vec::new();
        tokio::time::timeout(PATIENCE, s.client.read_to_end(&mut all))
            .await
            .unwrap()
            .unwrap();
        let mut got = Vec::new();
        parse_responses(&mut all, &mut got);
        assert_eq!(
            got.iter().map(|g| g.body.clone()).collect::<Vec<_>>(),
            [b"/a".to_vec(), b"/slow/200".to_vec()]
        );
        s.done.await.unwrap();
    });
}

#[test]
fn a_kept_response_is_not_held_behind_a_slow_handler() {
    // `/slow` is in flight and `/c` sits unparsed in hyper's buffer, so hyper reads nothing; the
    // layer must send `/a` because the turn ends with bytes kept.
    run(async {
        let mut s = serve_one(Writes::Coalesced).await;
        let sent = Instant::now();
        let batch = format!("{}{}{}", get("/a"), get("/slow/300"), get("/c"));
        s.client.write_all(batch.as_bytes()).await.unwrap();
        let got = read_responses(&mut s.client, 1, PATIENCE).await;
        assert_eq!(got[0].body, b"/a");
        assert_eq!(
            sent.elapsed(),
            Duration::ZERO,
            "held until the clock moved (the slow handler's sleep)"
        );
        let got = read_responses(&mut s.client, 2, PATIENCE).await;
        assert_eq!(got[0].body, b"/slow/300");
        assert_eq!(got[1].body, b"/c");
    });
}

#[test]
fn a_streamed_body_goes_out_chunk_by_chunk_even_with_requests_queued_behind_it() {
    run(async {
        let mut s = serve_one(Writes::Coalesced).await;
        let sent = Instant::now();
        let batch = format!("{}{}", get("/stream"), get("/after"));
        s.client.write_all(batch.as_bytes()).await.unwrap();
        // The first chunk is on the wire while the body waits for the second.
        let mut buf = vec![0u8; 4096];
        let mut seen = Vec::new();
        while !seen.windows(5).any(|w| w == b"first") {
            let n = tokio::time::timeout(PATIENCE, s.client.read(&mut buf))
                .await
                .unwrap_or_else(|_| panic!("first chunk not in {PATIENCE:?}; got {seen:?}"))
                .unwrap();
            assert!(n > 0);
            seen.extend_from_slice(&buf[..n]);
        }
        // This is still before the body's 300 ms wait for its second chunk.
        assert_eq!(sent.elapsed(), Duration::ZERO);
        assert!(!seen.windows(6).any(|w| w == b"second"));
        let mut rest = Vec::new();
        let mut got = Vec::new();
        rest.extend_from_slice(&seen);
        while got.len() < 2 {
            let n = tokio::time::timeout(PATIENCE, s.client.read(&mut buf))
                .await
                .unwrap()
                .unwrap();
            assert!(n > 0);
            rest.extend_from_slice(&buf[..n]);
            parse_responses(&mut rest, &mut got);
        }
        assert_eq!(got[0].body, b"firstsecond");
        assert_eq!(got[1].body, b"/after");
    });
}

// ── A write with nothing to share it goes straight through. ──
//
// A response with nothing kept before it and no request unread behind it is handed to the socket
// uncopied; what the socket does not take stays with hyper, which writes it again when the socket
// is writable. It must still be one write per poll, and no byte may be lost or repeated however
// the socket blocks, whether the connection ends or upgrades. Each case holds for both ways of
// sharing the layer's state, locked or thread-local, and over sockets with and without writev.

const COALESCED_OVER_BOTH: [(Writes, bool); 4] = [
    (Writes::Coalesced, false),
    (Writes::Coalesced, true),
    (Writes::CoalescedLocal, false),
    (Writes::CoalescedLocal, true),
];

#[test]
fn a_lone_response_is_one_write_over_either_kind_of_socket() {
    for (mode, vectored) in COALESCED_OVER_BOTH {
        run(async move {
            let mut s = serve_over(mode, vectored).await;
            s.client.write_all(get("/a").as_bytes()).await.unwrap();
            let got = read_responses(&mut s.client, 1, PATIENCE).await;
            assert_eq!(got[0].body, b"/a");
            assert_eq!(s.writes.load(Ordering::Relaxed), 1, "{mode:?}, vectored: {vectored}");
        });
    }
}

#[test]
fn sixteen_pipelined_requests_are_one_write_over_a_writev_socket_too() {
    for (mode, vectored) in COALESCED_OVER_BOTH {
        run(async move {
            let mut s = serve_over(mode, vectored).await;
            let batch: String = (0..16).map(|i| get(&format!("/{i}"))).collect();
            s.client.write_all(batch.as_bytes()).await.unwrap();
            let got = read_responses(&mut s.client, 16, PATIENCE).await;
            let bodies: Vec<Vec<u8>> = got.into_iter().map(|g| g.body).collect();
            let want: Vec<Vec<u8>> = (0..16).map(|i| format!("/{i}").into_bytes()).collect();
            assert_eq!(bodies, want);
            assert_eq!(s.writes.load(Ordering::Relaxed), 1, "{mode:?}, vectored: {vectored}");
        });
    }
}

/// Checks that a response larger than the socket holds stops at the write that finds it full,
/// since the task is registered for writability and another try is a certain `EAGAIN`, and that
/// once the client reads every byte arrives once, in order.
#[test]
fn a_write_that_finds_the_socket_full_is_not_tried_again_until_it_drains_and_loses_nothing() {
    for (mode, vectored) in COALESCED_OVER_BOTH {
        run(async move {
            const N: usize = 5 * PIPE_BYTES + 123;
            let mut s = serve_over(mode, vectored).await;
            s.client.write_all(get(&format!("/big/{N}")).as_bytes()).await.unwrap();
            // Let the server run until it blocks, with the client reading nothing.
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            assert_eq!(s.blocked.load(Ordering::Relaxed), 1, "{mode:?}, vectored: {vectored}");
            let got = read_responses(&mut s.client, 1, PATIENCE).await;
            assert!(
                got[0].body == big_body(N),
                "{mode:?}, vectored: {vectored}: the body arrived changed"
            );
        });
    }
}

/// Checks that a response still blocked on a full socket when the connection closes arrives
/// whole, once, before the end.
#[test]
fn a_response_still_blocked_when_the_connection_closes_arrives_whole() {
    for (mode, vectored) in COALESCED_OVER_BOTH {
        run(async move {
            const N: usize = 3 * PIPE_BYTES + 7;
            let mut s = serve_over(mode, vectored).await;
            let req = format!("GET /big/{N} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
            s.client.write_all(req.as_bytes()).await.unwrap();
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            let mut all = Vec::new();
            tokio::time::timeout(PATIENCE, s.client.read_to_end(&mut all))
                .await
                .unwrap()
                .unwrap();
            let mut got = Vec::new();
            parse_responses(&mut all, &mut got);
            assert_eq!(got.len(), 1, "{mode:?}, vectored: {vectored}");
            assert!(
                got[0].body == big_body(N),
                "{mode:?}, vectored: {vectored}: the body arrived changed"
            );
            assert!(all.is_empty(), "nothing after the response");
            s.done.await.unwrap();
        });
    }
}

/// Checks that a client gone while its response is blocked ends the connection, and nothing
/// waits on the gone socket.
#[test]
fn a_client_gone_mid_response_ends_the_connection() {
    for (mode, vectored) in COALESCED_OVER_BOTH {
        run(async move {
            let s = serve_over(mode, vectored).await;
            let Server { mut client, done, .. } = s;
            client
                .write_all(get(&format!("/big/{}", 4 * PIPE_BYTES)).as_bytes())
                .await
                .unwrap();
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            drop(client);
            tokio::time::timeout(PATIENCE, done)
                .await
                .expect("the connection ended once its client was gone")
                .unwrap();
        });
    }
}

/// Checks that the whole 101 is on the wire before the first byte the upgraded stream writes.
#[test]
fn an_upgrade_sends_the_whole_101_before_the_upgraded_streams_bytes() {
    for vectored in [false, true] {
        run(async move {
            let mut s = serve_over(Writes::Coalesced, vectored).await;
            s.client
                .write_all(b"GET /upgrade HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: upgrade\r\nUpgrade: test\r\n\r\n")
                .await
                .unwrap();
            let mut seen = Vec::new();
            let mut buf = [0u8; 1024];
            while !seen.ends_with(b"after") {
                let n = tokio::time::timeout(PATIENCE, s.client.read(&mut buf))
                    .await
                    .unwrap_or_else(|_| panic!("vectored: {vectored}: got {seen:?}"))
                    .unwrap();
                assert!(n > 0, "EOF after {seen:?}");
                seen.extend_from_slice(&buf[..n]);
            }
            let head_end = seen.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
            assert!(seen.starts_with(b"HTTP/1.1 101"), "{seen:?}");
            assert_eq!(&seen[head_end..], b"after", "vectored: {vectored}");
        });
    }
}

/// Checks that HTTP/2 with prior knowledge passes through: the client's preface and SETTINGS get
/// the server's SETTINGS back.
#[test]
fn an_http2_connection_passes_through_the_layer() {
    for mode in [Writes::Coalesced, Writes::CoalescedLocal] {
        run(async move {
            let mut s = serve_over(mode, true).await;
            let mut hello = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
            // An empty SETTINGS frame: length 0, type 4, no flags, stream 0.
            hello.extend_from_slice(&[0, 0, 0, 4, 0, 0, 0, 0, 0]);
            s.client.write_all(&hello).await.unwrap();
            let mut frame = [0u8; 9];
            tokio::time::timeout(PATIENCE, s.client.read_exact(&mut frame))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(frame[3], 4, "the server's first frame is SETTINGS: {frame:?}");
            assert_eq!(&frame[5..9], &[0, 0, 0, 0], "on stream 0");
        });
    }
}

/// It is the smallest step of the paused clock that tokio's timer tells apart.
const TICK: Duration = Duration::from_millis(1);

/// Asserts that the connection is still served just before `deadline` and has ended, with the
/// client reading EOF, just after it.
async fn closed_at(s: &mut Server, deadline: Instant) {
    assert!(
        tokio::time::timeout_at(deadline - TICK, &mut s.done).await.is_err(),
        "the connection ended before {:?}",
        deadline - TICK
    );
    tokio::time::timeout_at(deadline + TICK, &mut s.done)
        .await
        .unwrap_or_else(|_| panic!("the connection is still open after {:?}", deadline + TICK))
        .unwrap();
    let mut rest = Vec::new();
    s.client.read_to_end(&mut rest).await.unwrap();
}

/// Checks that a Slowloris client, which starts a request head and never finishes it, holds the
/// connection for `HEADER_READ_TIMEOUT` and no longer.
#[test]
fn a_request_head_never_finished_is_closed_at_the_header_read_timeout() {
    run(async {
        let mut s = serve_one(Writes::Coalesced).await;
        let started = Instant::now();
        s.client
            .write_all(b"GET /slowloris HTTP/1.1\r\nHost: 127.0.0.1\r\nX-A: ")
            .await
            .unwrap();
        closed_at(&mut s, started + HEADER_READ_TIMEOUT).await;
    });
}

/// Checks that an idle keep-alive connection, waiting for the next request head, is closed
/// `HEADER_READ_TIMEOUT` after the response, as a slow head is.
#[test]
fn an_idle_keep_alive_connection_is_closed_at_the_header_read_timeout() {
    run(async {
        let mut s = serve_one(Writes::Coalesced).await;
        s.client.write_all(get("/a").as_bytes()).await.unwrap();
        let got = read_responses(&mut s.client, 1, PATIENCE).await;
        assert_eq!(got[0].body, b"/a");
        let answered = Instant::now();
        closed_at(&mut s, answered + HEADER_READ_TIMEOUT).await;
    });
}

/// Checks that a stop coming while a connection lingers ends it at once: the 413 has gone out
/// before, and the connection does not wait out the rest of a body its client is still sending.
#[test]
fn a_stop_ends_a_lingering_close_at_once_after_its_answer_went_out() {
    run(async {
        let mut s = serve_one(Writes::Coalesced).await;
        s.client
            .write_all(b"POST /refuse HTTP/1.1\r\nHost: t\r\nContent-Length: 1000000\r\n\r\nthe start of a body")
            .await
            .unwrap();
        let mut answer = Vec::new();
        let mut chunk = [0u8; 1024];
        while !answer.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = tokio::time::timeout(PATIENCE, s.client.read(&mut chunk))
                .await
                .unwrap()
                .unwrap();
            assert!(n > 0, "EOF before the answer; read {answer:?}");
            answer.extend_from_slice(&chunk[..n]);
        }
        assert!(
            answer.starts_with(b"HTTP/1.1 413"),
            "{}",
            String::from_utf8_lossy(&answer)
        );
        s.client.write_all(b"more of the body").await.unwrap();
        assert!(
            tokio::time::timeout(PATIENCE / 2, &mut s.done).await.is_err(),
            "the connection closed without lingering"
        );

        s.token.cancel();
        let stopped = Instant::now();
        (&mut s.done).await.unwrap();
        assert_eq!(Instant::now() - stopped, Duration::ZERO);
        let mut rest = Vec::new();
        s.client.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, b"", "nothing follows the 413");
    });
}

/// Returns every byte the client reads until `n` complete responses are in, without the `date:`
/// lines, which say when rather than what.
async fn read_raw_responses(client: &mut DuplexStream, n: usize) -> Vec<u8> {
    let mut raw = Vec::new();
    let mut buf = Vec::new();
    let mut got = Vec::new();
    let mut chunk = vec![0u8; 64 * 1024];
    while got.len() < n {
        let read = tokio::time::timeout(PATIENCE, client.read(&mut chunk))
            .await
            .unwrap_or_else(|_| panic!("{} of {n} responses; read {raw:?}", got.len()))
            .unwrap();
        assert!(read > 0, "EOF after {} of {n} responses", got.len());
        raw.extend_from_slice(&chunk[..read]);
        buf.extend_from_slice(&chunk[..read]);
        parse_responses(&mut buf, &mut got);
    }
    let text = String::from_utf8(raw).unwrap();
    text.split_inclusive("\r\n")
        .filter(|line| !line.to_ascii_lowercase().starts_with("date:"))
        .collect::<String>()
        .into_bytes()
}

/// Sends two pipelined requests, whole or cut at `cuts` (each a cut point wrapped to the request's
/// length and the pause after it), and returns what the client reads back.
fn answered(cuts: &[(usize, Duration)]) -> Vec<u8> {
    let request = format!("{}{}", get("/a"), get("/b"));
    let request = request.as_bytes().to_vec();
    let mut cuts: Vec<(usize, Duration)> = cuts
        .iter()
        .map(|&(at, pause)| (at % (request.len() + 1), pause))
        .collect();
    cuts.sort_by_key(|&(at, _)| at);
    let mut out = Vec::new();
    run(async {
        let mut s = serve_one(Writes::Coalesced).await;
        let mut from = 0;
        for (at, pause) in cuts {
            s.client.write_all(&request[from..at]).await.unwrap();
            from = at;
            tokio::time::sleep(pause).await;
        }
        s.client.write_all(&request[from..]).await.unwrap();
        out = read_raw_responses(&mut s.client, 2).await;
    });
    out
}

proptest::proptest! {
    /// Checks that requests cut anywhere (inside `\r\n` too), with any pause under the
    /// header-read timeout between the pieces, are answered byte for byte as the same requests
    /// sent whole; the clock is paused, so the pauses take no time.
    #[test]
    fn requests_cut_anywhere_are_answered_as_if_sent_whole(
        cuts in proptest::collection::vec(
            (0usize..256, (0u64..400).prop_map(Duration::from_millis)),
            0..16,
        ),
    ) {
        let whole = answered(&[]);
        proptest::prop_assert_eq!(answered(&cuts), whole);
    }
}
