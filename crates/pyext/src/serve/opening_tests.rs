//! Tests that `drive_connection` bounds a connection from its start to its first request by the
//! request-head timeout, whatever the client sent meanwhile.
//!
//! hyper bounds an HTTP/1 head only once it has told HTTP/1 from HTTP/2 by the first bytes, and an
//! HTTP/2 connection's wait for a stream not at all; these are the cases it leaves open. The
//! transport is an in-memory pipe and the clock is paused, so each test waits for the connection's
//! task to end while the runtime jumps the clock to the next timer.

use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::serve::conn::{drive_connection, ConnectionTimeouts, LocalExec};
use mipyre_http::io::body::ResponseBody;
use mipyre_http::io::events::{self, HttpEvent};

/// The production timeout: what is tested is the value the server runs with.
const TIMEOUTS: ConnectionTimeouts = ConnectionTimeouts::DEFAULT;

/// One tick of the paused clock: the smallest step tokio's timer tells apart.
const TICK: Duration = Duration::from_millis(1);

/// The HTTP/2 connection preface a client sends first (RFC 9113 §3.4).
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
/// An empty SETTINGS frame: the client's half of the HTTP/2 handshake.
const SETTINGS: &[u8] = &[0, 0, 0, 0x4, 0, 0, 0, 0, 0];
/// A SETTINGS frame with ACK: the client acknowledging the server's settings.
const SETTINGS_ACK: &[u8] = &[0, 0, 0, 0x4, 0x1, 0, 0, 0, 0];
/// `GET /` on stream 1, END_HEADERS | END_STREAM, HPACK: `:method GET`, `:scheme http`, `:path /`
/// (static table 2, 6, 4) and `:authority t` (literal, name 1).
const GET_ON_STREAM_1: &[u8] = &[0, 0, 6, 0x1, 0x5, 0, 0, 0, 1, 0x82, 0x86, 0x84, 0x41, 0x01, b't'];

/// `/slow/<ms>` answers after a sleep; anything else echoes its path.
async fn answer(req: hyper::Request<Incoming>) -> Result<hyper::Response<ResponseBody>, hyper::Error> {
    let path = req.uri().path().to_owned();
    if let Some(ms) = path.strip_prefix("/slow/") {
        tokio::time::sleep(Duration::from_millis(ms.parse().expect("a test path's ms is a number"))).await;
    }
    Ok(hyper::Response::new(ResponseBody::Full(Full::new(Bytes::from(path)))))
}

/// One connection served by `drive_connection`, and when it started.
struct Conn {
    client: DuplexStream,
    done: JoinHandle<()>,
    started: Instant,
}

fn open() -> Conn {
    let (client, server) = tokio::io::duplex(64 * 1024);
    // The task is polled before the paused clock can move, so this is its start.
    let started = Instant::now();
    let done = tokio::task::spawn_local(drive_connection(
        server,
        service_fn(answer),
        LocalExec,
        TIMEOUTS,
        std::future::pending::<()>(),
    ));
    Conn { client, done, started }
}

fn run<F: std::future::Future<Output = ()>>(test: F) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("a test runtime builds");
    tokio::task::LocalSet::new().block_on(&rt, test);
}

/// Asserts the connection is still served at `deadline - TICK` and has ended by `deadline + TICK`,
/// its task done and the server's end read closed by the client.
async fn closed_at(c: &mut Conn, deadline: Instant) {
    assert!(
        tokio::time::timeout_at(deadline - TICK, &mut c.done).await.is_err(),
        "the connection ended before {:?}",
        deadline - TICK - c.started
    );
    tokio::time::timeout_at(deadline + TICK, &mut c.done)
        .await
        .unwrap_or_else(|_| panic!("the connection is still open at {:?}", deadline + TICK - c.started))
        .expect("the connection task ends without a panic");
    let mut rest = Vec::new();
    c.client
        .read_to_end(&mut rest)
        .await
        .expect("the client reads to the server's close");
}

/// Asserts the connection is still served at `at`.
async fn open_at(c: &mut Conn, at: Instant) {
    assert!(
        tokio::time::timeout_at(at, &mut c.done).await.is_err(),
        "the connection ended before {:?}",
        at - c.started
    );
}

/// Reads one HTTP/1 response with a `content-length` body, and returns the body.
async fn read_h1_response(client: &mut DuplexStream) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
            let len: usize = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .map(|v| v.trim().parse().expect("a content-length is a number"))
                .expect("the response has a content-length");
            if buf.len() >= end + 4 + len {
                return buf[end + 4..end + 4 + len].to_vec();
            }
        }
        let n = client.read(&mut chunk).await.expect("the pipe reads");
        assert!(n > 0, "EOF before a whole response: {buf:?}");
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// Reads HTTP/2 frames until one on stream 1 ends the stream (the response is whole).
async fn read_h2_response(client: &mut DuplexStream) {
    loop {
        let mut head = [0u8; 9];
        client.read_exact(&mut head).await.expect("a frame header");
        let len = usize::from(head[0]) << 16 | usize::from(head[1]) << 8 | usize::from(head[2]);
        let mut payload = vec![0u8; len];
        client.read_exact(&mut payload).await.expect("a frame payload");
        let (kind, flags) = (head[3], head[4]);
        let stream = u32::from_be_bytes([head[5] & 0x7f, head[6], head[7], head[8]]);
        // DATA (0) or HEADERS (1) with END_STREAM.
        if stream == 1 && (kind == 0 || kind == 1) && flags & 0x1 != 0 {
            return;
        }
    }
}

/// Runs `case` and returns how many connections it added to `kind`'s count. Every case of a kind
/// runs in one test, one after the other, because the count is the process's.
async fn counted<F: std::future::Future<Output = ()>>(kind: HttpEvent, case: F) -> u64 {
    let before = events::total(kind);
    case.await;
    events::total(kind) - before
}

/// A client that connects and then sends nothing, part of the HTTP/2 preface, all of it, or the
/// whole HTTP/2 handshake, but no request, holds the connection for the request-head timeout
/// from its start and no longer; each such connection is counted once as having had no first
/// request.
#[test]
fn a_connection_without_a_first_request_is_closed_at_the_request_head_timeout() {
    run(async {
        let cases: [(&str, &[&[u8]]); 5] = [
            ("nothing", &[]),
            ("part of the HTTP/2 preface", &[&PREFACE[..16]]),
            ("the HTTP/2 preface", &[PREFACE]),
            ("the HTTP/2 handshake", &[PREFACE, SETTINGS, SETTINGS_ACK]),
            ("part of an HTTP/1 head", &[b"GET / HTTP/1.1\r\nHost: t\r\nX-A: "]),
        ];
        for (what, sent) in cases {
            let added = counted(HttpEvent::FirstRequestTimedOut, async {
                let mut c = open();
                for bytes in sent {
                    c.client.write_all(bytes).await.expect("the pipe holds it");
                }
                let deadline = c.started + TIMEOUTS.request_head;
                closed_at(&mut c, deadline).await;
            })
            .await;
            assert_eq!(added, 1, "{what}: counted once as no first request");
        }
    });
}

/// A first request whose head is complete just inside the timeout is served, and so is one
/// whose handler outlasts it: the opening deadline is for the request to come, not to be
/// answered. The kept-alive connection then gets the request-head timeout from its response, as
/// hyper times it.
#[test]
fn a_slow_first_request_in_time_is_served_and_the_connection_kept() {
    run(async {
        // The head finished one tick before the deadline.
        let mut c = open();
        c.client.write_all(b"GET /a HTTP/1.1\r\n").await.expect("the pipe holds it");
        tokio::time::sleep_until(c.started + TIMEOUTS.request_head - TICK).await;
        c.client.write_all(b"Host: t\r\n\r\n").await.expect("the pipe holds it");
        assert_eq!(read_h1_response(&mut c.client).await, b"/a");
        let answered = Instant::now();
        let past_deadline = c.started + TIMEOUTS.request_head + TICK;
        open_at(&mut c, past_deadline).await;
        let idle = counted(HttpEvent::RequestHeadTimedOut, async {
            closed_at(&mut c, answered + TIMEOUTS.request_head).await;
        })
        .await;
        assert!(idle >= 1, "the idle kept-alive connection is counted as no next request head");

        // A handler that takes twice the timeout.
        let mut c = open();
        let slow = 2 * TIMEOUTS.request_head;
        let get = format!("GET /slow/{} HTTP/1.1\r\nHost: t\r\n\r\n", slow.as_millis());
        c.client.write_all(get.as_bytes()).await.expect("the pipe holds it");
        let body = read_h1_response(&mut c.client).await;
        assert_eq!(body, format!("/slow/{}", slow.as_millis()).as_bytes());
        assert_eq!(Instant::now() - c.started, slow, "answered when the handler was done");
    });
}

/// An HTTP/2 connection whose first stream opens just inside the timeout is served and is not
/// closed at the opening deadline.
#[test]
fn an_http2_stream_in_time_is_served_and_the_connection_kept() {
    run(async {
        let mut c = open();
        c.client.write_all(PREFACE).await.expect("the pipe holds it");
        c.client.write_all(SETTINGS).await.expect("the pipe holds it");
        tokio::time::sleep_until(c.started + TIMEOUTS.request_head - TICK).await;
        c.client.write_all(GET_ON_STREAM_1).await.expect("the pipe holds it");
        read_h2_response(&mut c.client).await;
        let past_deadline = c.started + TIMEOUTS.request_head + TICK;
        open_at(&mut c, past_deadline).await;
    });
}
