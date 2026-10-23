//! Drives every hyper connection the server serves, the in-memory bench's included, so a bench
//! number is the production per-request cost.

use crate::sync::{AtomicBool, Ordering};
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

use hyper::body::Incoming;
use hyper::Request;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::{Builder as AutoBuilder, HttpServerConnExec};
use tokio::io::{AsyncRead, AsyncWrite};

use mipyre_http::io::body::ResponseBody;
use mipyre_http::io::coalesce::{coalesce, coalesce_local, CoalescedPoll, Flusher, Sharing};
use mipyre_http::io::events::{self, HttpEvent};
use mipyre_http::io::static_files::FileSendError;

use mipyre_http::plan::limits::{HEADER_READ_TIMEOUT, LINGER_IDLE_TIMEOUT};
use mipyre_transport::stream::events::{self as stream_events, StreamEvent};
use mipyre_transport::stream::linger::{Close, LingerBounds};
use mipyre_transport::stream::stall::WriteStall;
use mipyre_transport::stream::stall::WRITE_TIMEOUT;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConnectionTimeouts {
    /// It bounds the wait from the accept to the first request, whatever the client sent
    /// meanwhile, and on HTTP/1 from each response to the next complete head. Without it a client
    /// holds a connection's fd and task for as long as it likes by sending nothing.
    pub(crate) request_head: std::time::Duration,
    /// It bounds the gap between the client's bytes while a connection the server is done with
    /// reads off what the client still sends.
    pub(crate) linger_idle: std::time::Duration,
}

impl ConnectionTimeouts {
    /// It is the listener's header-read timeout, the same number as `TCP_DEFER_ACCEPT`, so it is
    /// kept with the listener.
    pub(crate) const DEFAULT: Self = Self {
        request_head: HEADER_READ_TIMEOUT,
        linger_idle: LINGER_IDLE_TIMEOUT,
    };

    /// Returns the bounds of a lingering close: a client that keeps sending holds the connection
    /// no longer than an idle one is held waiting for its next request head.
    fn linger(self) -> LingerBounds {
        LingerBounds {
            idle: self.linger_idle,
            limit: self.request_head,
        }
    }
}

/// Spawns hyper's tasks on the TPC thread's `LocalSet`, since connection futures hold the
/// thread's `Rc`s and are not `Send`.
#[derive(Clone, Copy)]
pub(crate) struct LocalExec;

impl<F> hyper::rt::Executor<F> for LocalExec
where
    F: std::future::Future + 'static,
    F::Output: 'static,
{
    fn execute(&self, fut: F) {
        tokio::task::spawn_local(fut);
    }
}

/// Serves one HTTP/1+2 connection, with upgrades for WebSocket, until it ends. When `stop`
/// completes, hyper stops taking new requests and the in-flight ones drain. A client disconnect is
/// not an error; anything else is logged.
///
/// `stop` is polled on every wake of the connection's task, so on a TPC thread it is the thread's
/// own `LocalStop`, never the shared token. HTTP/1 responses produced in one poll share one write
/// (docs/write-coalescing.md). A connection with no first request within `timeouts.request_head`
/// is closed and counted.
pub(crate) async fn drive_connection<IO, S, E, F>(io: IO, svc: S, exec: E, timeouts: ConnectionTimeouts, stop: F)
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: hyper::service::Service<Request<Incoming>, Response = hyper::Response<ResponseBody>, Error = hyper::Error>,
    S::Future: 'static,
    E: HttpServerConnExec<S::Future, ResponseBody>,
    F: std::future::Future<Output = ()>,
{
    let (io, flusher) = coalesce(WriteStall::new(io, WRITE_TIMEOUT));
    let requested = AtomicBool::new(false);
    let svc = RequestNoter {
        inner: svc,
        requested: &requested,
    };
    let builder = connection_builder(exec, timeouts);
    let conn = builder.serve_connection_with_upgrades(TokioIo::new(io), svc);
    serve(conn, &requested, flusher, timeouts, stop).await;
}

/// Works like [`drive_connection`] for a connection that stays on this thread and is never
/// upgraded: the IO and its flusher share state without a lock, so the IO need not be `Send`.
pub(crate) async fn drive_local_connection<IO, S, F>(io: IO, svc: S, timeouts: ConnectionTimeouts, stop: F)
where
    IO: AsyncRead + AsyncWrite + Unpin + 'static,
    S: hyper::service::Service<Request<Incoming>, Response = hyper::Response<ResponseBody>, Error = hyper::Error>,
    S::Future: 'static,
    F: std::future::Future<Output = ()>,
{
    let (io, flusher) = coalesce_local(WriteStall::new(io, WRITE_TIMEOUT));
    let requested = AtomicBool::new(false);
    let svc = RequestNoter {
        inner: svc,
        requested: &requested,
    };
    let builder = connection_builder(LocalExec, timeouts);
    let conn = builder.serve_connection(TokioIo::new(io), svc);
    serve(conn, &requested, flusher, timeouts, stop).await;
}

fn connection_builder<E>(exec: E, timeouts: ConnectionTimeouts) -> AutoBuilder<E> {
    let mut builder = AutoBuilder::new(exec);
    builder
        .http1()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(timeouts.request_head);
    builder
}

/// Lets `serve` drive both of hyper's connection types.
trait GracefulConnection: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    fn graceful_shutdown(self: std::pin::Pin<&mut Self>);
}

impl<I, S, E> GracefulConnection for hyper_util::server::conn::auto::Connection<'_, I, S, E>
where
    S: hyper::service::Service<Request<Incoming>, Response = hyper::Response<ResponseBody>>,
    S::Future: 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    I: hyper::rt::Read + hyper::rt::Write + Unpin + 'static,
    E: HttpServerConnExec<S::Future, ResponseBody>,
{
    fn graceful_shutdown(self: std::pin::Pin<&mut Self>) {
        hyper_util::server::conn::auto::Connection::graceful_shutdown(self);
    }
}

impl<I, S, E> GracefulConnection for hyper_util::server::conn::auto::UpgradeableConnection<'_, I, S, E>
where
    S: hyper::service::Service<Request<Incoming>, Response = hyper::Response<ResponseBody>>,
    S::Future: 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
    E: HttpServerConnExec<S::Future, ResponseBody>,
{
    fn graceful_shutdown(self: std::pin::Pin<&mut Self>) {
        hyper_util::server::conn::auto::UpgradeableConnection::graceful_shutdown(self);
    }
}

/// Polls `conn` to its end through the coalescing bracket, then writes out what it left.
async fn serve<C, T, K, F>(
    conn: C,
    requested: &AtomicBool,
    flusher: Flusher<T, K>,
    timeouts: ConnectionTimeouts,
    stop: F,
) where
    C: GracefulConnection,
    T: AsyncRead + AsyncWrite + Unpin,
    K: Sharing,
    F: std::future::Future<Output = ()>,
{
    // hyper times an HTTP/1 head only once it has told HTTP/1 from HTTP/2 by the first bytes,
    // and never an HTTP/2 connection's wait for a stream: until the first request, the deadline
    // below is the connection's only bound.
    let first_request_by = tokio::time::Instant::now() + timeouts.request_head;
    tokio::pin!(stop);
    // The connection (and the IO it holds) ends with this block, so `finish` below sees
    // whether anyone else (an upgrade) still holds the IO.
    let (ended, stopping) = {
        let conn = NewConnection {
            conn,
            deadline: Some(tokio::time::sleep_until(first_request_by)),
            requested,
        };
        tokio::pin!(conn);
        let mut graceful_sent = false;
        loop {
            let polled = CoalescedPoll::new(conn.as_mut(), &flusher);
            tokio::select! {
                end = polled => break (end, graceful_sent),
                () = &mut stop, if !graceful_sent => {
                    conn.as_mut().project().conn.graceful_shutdown();
                    graceful_sent = true;
                }
            }
        }
    };
    // This is read only on the rare error path, once the connection is gone.
    let stage = || {
        if requested.load(Ordering::Relaxed) {
            Stage::Serving
        } else {
            Stage::Opening
        }
    };
    let close = match ended {
        End::Ended(Ok(())) => close_after(None, stopping),
        End::Ended(Err(e)) => {
            let stage = stage();
            report_connection_error(e.as_ref(), stage, timeouts.request_head);
            close_after(Some(connection_end(e.as_ref(), stage)), stopping)
        }
        End::NoFirstRequest => {
            report_no_first_request(timeouts.request_head);
            Close::Now
        }
    };
    // A stop that comes while the connection lingers ends the wait: the answer is out by then,
    // and what the client still sends is no reason to hold the stop up.
    if let Err(e) = flusher.finish(close, timeouts.linger(), stop).await {
        report_connection_error(&e, Stage::Serving, timeouts.request_head);
    }
}

pin_project_lite::pin_project! {
    /// Bounds a connection by `deadline` until its first request. The deadline and its timer entry
    /// are dropped once `requested` is set, after which a poll costs one branch.
    struct NewConnection<'a, C> {
        #[pin]
        conn: C,
        #[pin]
        deadline: Option<tokio::time::Sleep>,
        requested: &'a AtomicBool,
    }
}

impl<C, E> Future for NewConnection<'_, C>
where
    C: Future<Output = Result<(), E>>,
{
    type Output = End<E>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<End<E>> {
        let mut this = self.project();
        if let Poll::Ready(res) = this.conn.poll(cx) {
            return Poll::Ready(End::Ended(res));
        }
        if let Some(sleep) = this.deadline.as_mut().as_pin_mut() {
            if this.requested.load(Ordering::Relaxed) {
                this.deadline.set(None);
            } else if sleep.poll(cx).is_ready() {
                return Poll::Ready(End::NoFirstRequest);
            }
        }
        Poll::Pending
    }
}

enum End<E> {
    Ended(Result<(), E>),
    NoFirstRequest,
}

/// Says whether a connection had had its first request when it ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Opening,
    Serving,
}

/// Notes that a request came, which hyper signals once an HTTP/1 head is complete or an HTTP/2
/// stream opens. It costs one relaxed store per request to a flag on the connection's own task.
struct RequestNoter<'a, S> {
    inner: S,
    requested: &'a AtomicBool,
}

impl<S, R> hyper::service::Service<R> for RequestNoter<'_, S>
where
    S: hyper::service::Service<R>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    #[inline]
    fn call(&self, req: R) -> Self::Future {
        self.requested.store(true, Ordering::Relaxed);
        self.inner.call(req)
    }
}

/// Counts a connection that had no first request within `limit`, with a debug line each and a
/// throttled warning, since a client can open them by the thousand.
fn report_no_first_request(limit: std::time::Duration) {
    let warn = events::degrade(HttpEvent::FirstRequestTimedOut);
    tracing::debug!(target: "mipyre::server", ?limit, "no first request; closing the connection");
    if let Some(held) = warn {
        tracing::warn!(
            target: "mipyre::server",
            ?limit,
            held,
            "no first request: the client sent no complete request for the request-head timeout \
             after connecting; closed the connection"
        );
    }
}

/// What ended a connection whose serve failed, which decides whether and how it is counted and
/// logged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnectionEnd {
    /// The client closed, reset or aborted, or cut a message short.
    PeerGone,
    /// The client stopped reading for `limit`.
    WriteStalled {
        limit: std::time::Duration,
    },
    /// hyper refused the request head and has already answered 400, 414 or 431.
    BadRequest(HttpEvent),
    NoFirstRequest,
    /// A kept-alive connection went idle, or left a head unfinished, past the head timeout.
    NoNextRequest,
    /// A static file's answer was cut off while it was sent, because the file changed or failed;
    /// the body reports it.
    AnswerCut,
    Failed,
}

/// Returns what ended a connection that failed with `e` at `stage`.
fn connection_end(e: &(dyn std::error::Error + 'static), stage: Stage) -> ConnectionEnd {
    if answer_cut(e).is_some() {
        ConnectionEnd::AnswerCut
    } else if events::peer_went_away(e) {
        ConnectionEnd::PeerGone
    } else if let Some(stalled) = write_stalled(e) {
        ConnectionEnd::WriteStalled { limit: stalled.limit }
    } else if let Some(kind) = events::bad_request(e) {
        ConnectionEnd::BadRequest(kind)
    } else if events::timed_out(e) {
        // hyper's HTTP/1 header-read timer fired: before the first request it is the opening
        // deadline.
        match stage {
            Stage::Opening => ConnectionEnd::NoFirstRequest,
            Stage::Serving => ConnectionEnd::NoNextRequest,
        }
    } else {
        ConnectionEnd::Failed
    }
}

/// Returns how to close a connection hyper is done with, by what ended it: `None` when hyper
/// ended it cleanly. hyper ends cleanly after the client closed (then nothing is waited for) or
/// after it answered and chose to close, as for `Connection: close` or a body it did not read,
/// and it answers a bad request before it errs: in those cases the client may still be sending.
/// Once the server is stopping nothing is waited for, so a stop is not held up by clients that
/// keep their connections open.
fn close_after(end: Option<ConnectionEnd>, stopping: bool) -> Close {
    match end {
        _ if stopping => Close::Now,
        None | Some(ConnectionEnd::BadRequest(_)) => Close::Lingering,
        Some(
            ConnectionEnd::PeerGone
            | ConnectionEnd::WriteStalled { .. }
            | ConnectionEnd::NoFirstRequest
            | ConnectionEnd::NoNextRequest
            | ConnectionEnd::AnswerCut
            | ConnectionEnd::Failed,
        ) => Close::Now,
    }
}

/// Counts and logs a connection that failed with `e`, by what ended it.
fn report_connection_error(e: &(dyn std::error::Error + 'static), stage: Stage, request_head: std::time::Duration) {
    match connection_end(e, stage) {
        // A client going away is normal under load and is not logged.
        ConnectionEnd::PeerGone => {}
        // This comes in floods when one client stops reading on many connections, so it is
        // throttled; `write_stall` already counted it.
        ConnectionEnd::WriteStalled { limit } => {
            if let Some(held) = stream_events::admit(StreamEvent::WriteStalled) {
                tracing::warn!(
                    target: "mipyre::server",
                    ?limit,
                    held,
                    "write stalled: the client did not read for the write timeout; closing the connection"
                );
            }
        }
        // A scanner sends these by the thousand, so they are counted by kind with a debug line
        // each and a throttled warning.
        ConnectionEnd::BadRequest(kind) => {
            let warn = events::degrade(kind);
            tracing::debug!(target: "mipyre::server", error = %e, "bad request; closing the connection");
            if let Some(held) = warn {
                tracing::warn!(
                    target: "mipyre::server",
                    error = %e,
                    held,
                    "bad request: the client sent what is no HTTP/1 request; answered and closed the connection"
                );
            }
        }
        ConnectionEnd::NoFirstRequest => report_no_first_request(request_head),
        // This is the usual end of a kept-alive connection its client stopped using, so it is
        // counted but not warned about.
        ConnectionEnd::NoNextRequest => {
            events::count(HttpEvent::RequestHeadTimedOut);
            tracing::debug!(target: "mipyre::server", error = %e, "no next request head; closing the connection");
        }
        // It is counted and logged per file, throttled, since every client of a file being
        // rewritten meets it.
        ConnectionEnd::AnswerCut => {
            if let Some(cut) = answer_cut(e) {
                cut.report();
            }
        }
        ConnectionEnd::Failed => tracing::warn!(target: "mipyre::server", error = %e, "Connection error"),
    }
}

/// Finds the static file answer that was cut off along `e`'s source chain, if that is why the
/// connection failed.
fn answer_cut<'a>(e: &'a (dyn std::error::Error + 'static)) -> Option<&'a FileSendError> {
    std::iter::successors(Some(e), |e| e.source()).find_map(|e| e.downcast_ref::<FileSendError>())
}

/// Finds the write stall along `e`'s source chain, if that is why the connection failed.
fn write_stalled<'a>(
    e: &'a (dyn std::error::Error + 'static),
) -> Option<&'a mipyre_transport::stream::stall::WriteStallError> {
    use mipyre_transport::stream::stall::WriteStallError;
    let mut source = Some(e);
    while let Some(e) = source {
        if let Some(stalled) = e.downcast_ref::<WriteStallError>() {
            return Some(stalled);
        }
        // `io::Error::source` skips the error it carries (it returns that error's source).
        let carried = e
            .downcast_ref::<std::io::Error>()
            .and_then(|io| io.get_ref())
            .and_then(|inner| inner.downcast_ref::<WriteStallError>());
        if carried.is_some() {
            return carried;
        }
        source = e.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::service::service_fn;

    /// Mimics a `hyper::Error` from a failed write, which has its own text and the io error as
    /// its cause.
    #[derive(Debug)]
    struct Wrapper(std::io::Error);
    impl std::fmt::Display for Wrapper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("error writing a body to connection")
        }
    }
    impl std::error::Error for Wrapper {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn a_write_stall_is_found_inside_the_io_error_hyper_wraps() {
        let limit = std::time::Duration::from_secs(10);
        let stalled = std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            mipyre_transport::stream::stall::WriteStallError { limit },
        );
        let wrapped = Wrapper(stalled);
        assert_eq!(write_stalled(&wrapped).map(|s| s.limit), Some(limit));
        let other = Wrapper(std::io::Error::from(std::io::ErrorKind::TimedOut));
        assert!(write_stalled(&other).is_none());
    }

    /// Returns what `report_connection_error` logs at debug and above for `e` met while serving.
    fn reported(e: &(dyn std::error::Error + 'static)) -> String {
        crate::exchange::captured_log_from(tracing::Level::DEBUG, || {
            report_connection_error(e, Stage::Serving, ConnectionTimeouts::DEFAULT.request_head)
        })
    }

    /// Checks that a reset is not logged, a stall is logged with its limit and no failure line,
    /// and a timeout that is no stall is a warning.
    #[test]
    fn a_connection_end_is_classed_by_what_ended_it() {
        let limit = std::time::Duration::from_secs(10);
        let reset = Wrapper(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        assert_eq!(reported(&reset), "", "the client going away is not logged");
        let stalled = Wrapper(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            mipyre_transport::stream::stall::WriteStallError { limit },
        ));
        let log = reported(&stalled);
        assert!(
            log.lines().all(|l| l.contains("write stalled") && l.contains("10s")),
            "{log}"
        );
        let timed_out = Wrapper(std::io::Error::from(std::io::ErrorKind::TimedOut));
        let log = reported(&timed_out);
        let line = log.lines().find(|l| l.contains("Connection error")).expect(&log);
        assert!(line.contains("\"level\":\"WARN\""), "{line}");
    }

    /// Checks that a failed last write-out at the connection's end is a warning carrying the
    /// error rather than dropped.
    #[test]
    fn a_failed_last_write_out_at_the_connections_end_is_a_warning_with_the_error() {
        let failed = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "egress refused");
        let log = crate::exchange::captured_log(|| {
            report_connection_error(&failed, Stage::Serving, ConnectionTimeouts::DEFAULT.request_head)
        });
        let line = log.lines().find(|l| l.contains("egress refused")).expect(&log);
        assert!(line.contains("\"level\":\"WARN\""), "{line}");
    }

    /// Answers every request 200 with an empty body.
    async fn ok(_: Request<Incoming>) -> Result<hyper::Response<ResponseBody>, hyper::Error> {
        Ok(hyper::Response::new(ResponseBody::Full(http_body_util::Full::new(
            bytes::Bytes::new(),
        ))))
    }

    /// Sends `bytes` on a new connection to `addr` and returns what came back before the server
    /// closed it, or the read's error.
    async fn exchange(addr: std::net::SocketAddr, bytes: &[u8]) -> Result<Vec<u8>, std::io::Error> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut client = tokio::net::TcpStream::connect(addr).await?;
        client.write_all(bytes).await?;
        let mut got = Vec::new();
        client.read_to_end(&mut got).await?;
        Ok(got)
    }

    /// Checks over a real socket that garbage, a bad request line or header, or an oversized head
    /// closes the connection, with its 4xx unless the reset overtakes it, is counted by kind, and
    /// leaves the server answering the next connection.
    #[test]
    fn a_bad_request_is_answered_closed_and_counted_and_the_server_answers_the_next() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a test runtime builds");
        tokio::task::LocalSet::new().block_on(&rt, async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("a loopback port is free");
            let addr = listener.local_addr().expect("a bound listener has an address");
            // Each connection's task is sent here, so the test can wait for it to end and count.
            let (ended_tx, mut ended_rx) = tokio::sync::mpsc::unbounded_channel();
            tokio::task::spawn_local(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let task = tokio::task::spawn_local(drive_connection(
                        stream,
                        service_fn(ok),
                        LocalExec,
                        ConnectionTimeouts::DEFAULT,
                        std::future::pending::<()>(),
                    ));
                    if ended_tx.send(task).is_err() {
                        return;
                    }
                }
            });
            let long_uri = format!("GET /{} HTTP/1.1\r\nhost: t\r\n\r\n", "a".repeat(70_000));
            let many_headers = format!(
                "GET / HTTP/1.1\r\n{}\r\n",
                (0..200).map(|i| format!("x-{i}: v\r\n")).collect::<String>()
            );
            let malformed = HttpEvent::MalformedRequest;
            let oversized = HttpEvent::OversizedRequestHead;
            let cases: [(&str, &[u8], HttpEvent, &str); 5] = [
                (
                    "a TLS ClientHello",
                    b"\x16\x03\x01\x02\x00\x01\x00\x01\xfc\x03\x03",
                    malformed,
                    "400",
                ),
                ("a bad version", b"GET / HTTP/9.9\r\nhost: t\r\n\r\n", malformed, "400"),
                (
                    "a header without a colon",
                    b"GET / HTTP/1.1\r\nno colon here\r\n\r\n",
                    malformed,
                    "400",
                ),
                ("a URI too long", long_uri.as_bytes(), oversized, "414"),
                ("too many headers", many_headers.as_bytes(), oversized, "431"),
            ];
            for (what, sent, kind, status) in cases {
                let before = events::total(kind);
                // The connection is closed either way: with the 4xx, or reset when hyper stopped
                // reading at the error with bytes still unread (the kernel's reset can overtake
                // the 4xx).
                match exchange(addr, sent).await {
                    Ok(answer) => {
                        let head = String::from_utf8_lossy(&answer);
                        assert!(head.starts_with(&format!("HTTP/1.1 {status} ")), "{what}: {head}");
                    }
                    Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::ConnectionReset, "{what}: {e}"),
                }
                let task = ended_rx.recv().await.expect("the accept loop runs");
                task.await.expect("the connection task ends without a panic");
                // Other tests in this process may count too, so this checks only for an increase.
                assert!(events::total(kind) > before, "{what}: counted as {kind:?}");

                let next = exchange(addr, b"GET / HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n")
                    .await
                    .expect("the server still serves");
                let head = String::from_utf8_lossy(&next);
                assert!(head.starts_with("HTTP/1.1 200 "), "after {what}: {head}");
                ended_rx.recv().await.expect("the accept loop runs");
            }
        });
    }

    /// Returns the error hyper ends an HTTP/1 connection with when a head stays unfinished past
    /// its header-read timeout, on a paused clock so nothing waits.
    fn header_read_timeout() -> hyper::Error {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("a test runtime builds");
        rt.block_on(async {
            use tokio::io::AsyncWriteExt;

            let (mut client, server) = tokio::io::duplex(4096);
            let conn = hyper::server::conn::http1::Builder::new()
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(HEADER_READ_TIMEOUT)
                .serve_connection(TokioIo::new(server), service_fn(ok));
            client
                .write_all(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n")
                .await
                .expect("the pipe holds it");
            let err = conn.await.expect_err("the head never finished");
            drop(client);
            err
        })
    }

    /// Returns the error hyper ends an HTTP/1 connection with when the request line is not HTTP.
    fn parse_error() -> hyper::Error {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a test runtime builds");
        rt.block_on(async {
            use tokio::io::AsyncWriteExt;

            let (mut client, server) = tokio::io::duplex(4096);
            let conn =
                hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(server), service_fn(ok));
            client
                .write_all(b"GET / HTTP/9.9\r\n\r\n")
                .await
                .expect("the pipe holds it");
            // The client stays open until the server is done: had it closed, the 400 would fail
            // to go out and end the connection as a broken pipe (the peer going away) instead.
            let err = conn.await.expect_err("no request");
            drop(client);
            err
        })
    }

    /// Checks that hyper's own header-read timeout counts as a timeout, not as a failure or a bad
    /// request.
    #[test]
    fn a_header_read_timeout_ends_the_connection_as_timed_out() {
        let err = header_read_timeout();
        let before = events::total(HttpEvent::RequestHeadTimedOut);
        let log = reported(&err);
        assert!(log.contains("no next request head"), "{err:?}: {log}");
        assert!(events::total(HttpEvent::RequestHeadTimedOut) > before, "{err:?}");
    }

    /// Checks that a real hyper parse error counts as a bad request, not as a failure.
    #[test]
    fn a_parse_error_ends_the_connection_as_a_bad_request() {
        let err = parse_error();
        let before = events::total(HttpEvent::MalformedRequest);
        let log = reported(&err);
        assert!(log.contains("bad request; closing the connection"), "{err:?}: {log}");
        assert!(events::total(HttpEvent::MalformedRequest) > before, "{err:?}");
    }

    #[test]
    fn a_header_read_timeout_is_no_first_request_while_opening_and_no_next_request_after() {
        let err = header_read_timeout();
        assert_eq!(connection_end(&err, Stage::Opening), ConnectionEnd::NoFirstRequest);
        assert_eq!(connection_end(&err, Stage::Serving), ConnectionEnd::NoNextRequest);
    }

    #[test]
    fn a_failed_connection_ends_by_what_its_error_chain_says() {
        let limit = std::time::Duration::from_secs(10);
        let io = std::io::Error::from;
        let stall = || {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                mipyre_transport::stream::stall::WriteStallError { limit },
            )
        };
        let cases: [(&str, Box<dyn std::error::Error>, ConnectionEnd); 7] = [
            (
                "a reset",
                Box::new(Wrapper(io(std::io::ErrorKind::ConnectionReset))),
                ConnectionEnd::PeerGone,
            ),
            (
                "a broken pipe",
                Box::new(io(std::io::ErrorKind::BrokenPipe)),
                ConnectionEnd::PeerGone,
            ),
            (
                "a stall hyper wraps",
                Box::new(Wrapper(stall())),
                ConnectionEnd::WriteStalled { limit },
            ),
            ("a bare stall", Box::new(stall()), ConnectionEnd::WriteStalled { limit }),
            (
                "a timeout that is no stall",
                Box::new(Wrapper(io(std::io::ErrorKind::TimedOut))),
                ConnectionEnd::Failed,
            ),
            (
                "a refused write",
                Box::new(io(std::io::ErrorKind::PermissionDenied)),
                ConnectionEnd::Failed,
            ),
            (
                "a parse error",
                Box::new(parse_error()),
                ConnectionEnd::BadRequest(HttpEvent::MalformedRequest),
            ),
        ];
        for (what, e, end) in cases {
            assert_eq!(connection_end(e.as_ref(), Stage::Serving), end, "{what}");
            assert_eq!(connection_end(e.as_ref(), Stage::Opening), end, "{what} while opening");
        }
    }

    #[test]
    fn a_connection_lingers_only_where_the_client_may_still_be_sending_and_not_once_stopping() {
        let cases = [
            (None, Close::Lingering),
            (
                Some(ConnectionEnd::BadRequest(HttpEvent::MalformedRequest)),
                Close::Lingering,
            ),
            (Some(ConnectionEnd::PeerGone), Close::Now),
            (
                Some(ConnectionEnd::WriteStalled {
                    limit: std::time::Duration::from_secs(10),
                }),
                Close::Now,
            ),
            (Some(ConnectionEnd::NoFirstRequest), Close::Now),
            (Some(ConnectionEnd::NoNextRequest), Close::Now),
            (Some(ConnectionEnd::Failed), Close::Now),
        ];
        for (end, close) in cases {
            assert_eq!(close_after(end, false), close, "{end:?}");
            assert_eq!(close_after(end, true), Close::Now, "{end:?} while stopping");
        }
    }

    #[test]
    fn a_lingering_close_is_bounded_by_the_request_head_timeout_and_the_idle_gap_given() {
        let secs = std::time::Duration::from_secs;
        for (request_head, linger_idle) in [(secs(10), secs(2)), (secs(3), secs(1))] {
            let timeouts = ConnectionTimeouts {
                request_head,
                linger_idle,
            };
            assert_eq!(
                timeouts.linger(),
                LingerBounds {
                    idle: linger_idle,
                    limit: request_head
                }
            );
        }
    }
}
