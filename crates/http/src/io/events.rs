//! Counts what HTTP refused or gave up on, per kind, throttles each kind's log line to one a
//! minute, and tells what a hyper error was.

use std::error::Error;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use mipyre_transport::stream::peer_gone;
use mipyre_util::clock;
use mipyre_util::throttle::LogThrottle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpEvent {
    /// The bytes do not parse as an HTTP/1 request, so the client gets a 400 and the connection is
    /// closed.
    MalformedRequest,
    /// The request head is past hyper's limits, so the client gets a 414 or 431 and the
    /// connection is closed.
    OversizedRequestHead,
    /// The client sent no complete first request within the request-head timeout, so the
    /// connection is closed.
    FirstRequestTimedOut,
    /// A kept-alive HTTP/1 client sent no next request head within the request-head timeout, so
    /// the connection is closed.
    RequestHeadTimedOut,
}

const LOG_PERIOD: Duration = Duration::from_secs(60);

/// Holds a value alone on a 128-byte line, which covers x86's paired 64-byte lines and Apple's
/// line size, so one kind's writes on one core do not stall another kind's on another. It is
/// `mipyre_cpu::CacheLine` again, which this package may not depend on; where the one definition
/// lives is b/633's question, and until then each events module pads its own.
#[repr(align(128))]
struct OwnLine<T>(T);

/// Holds one value per kind, so a kind added without a field does not compile. The counts, which
/// telemetry reads, and the log throttles, which are logging policy, are two of these, kept apart.
struct HttpEventTable<T> {
    malformed_request: T,
    oversized_request_head: T,
    first_request_timed_out: T,
    request_head_timed_out: T,
}

impl<T> HttpEventTable<T> {
    fn of(&self, event: HttpEvent) -> &T {
        match event {
            HttpEvent::MalformedRequest => &self.malformed_request,
            HttpEvent::OversizedRequestHead => &self.oversized_request_head,
            HttpEvent::FirstRequestTimedOut => &self.first_request_timed_out,
            HttpEvent::RequestHeadTimedOut => &self.request_head_timed_out,
        }
    }
}

impl HttpEventTable<OwnLine<AtomicU64>> {
    const fn counts() -> Self {
        HttpEventTable {
            malformed_request: OwnLine(AtomicU64::new(0)),
            oversized_request_head: OwnLine(AtomicU64::new(0)),
            first_request_timed_out: OwnLine(AtomicU64::new(0)),
            request_head_timed_out: OwnLine(AtomicU64::new(0)),
        }
    }
}

impl HttpEventTable<OwnLine<LogThrottle>> {
    const fn throttles() -> Self {
        HttpEventTable {
            malformed_request: OwnLine(LogThrottle::new(LOG_PERIOD)),
            oversized_request_head: OwnLine(LogThrottle::new(LOG_PERIOD)),
            first_request_timed_out: OwnLine(LogThrottle::new(LOG_PERIOD)),
            request_head_timed_out: OwnLine(LogThrottle::new(LOG_PERIOD)),
        }
    }
}

static HTTP_EVENT_COUNTS: HttpEventTable<OwnLine<AtomicU64>> = HttpEventTable::counts();
static HTTP_EVENT_THROTTLES: HttpEventTable<OwnLine<LogThrottle>> = HttpEventTable::throttles();

/// Counts one `event` without deciding its log line.
pub fn count(event: HttpEvent) {
    HTTP_EVENT_COUNTS.of(event).0.fetch_add(1, Ordering::Relaxed);
}

/// Counts one `event` and returns, once a minute per kind, how many lines were held back since
/// the last one went out.
pub fn degrade(event: HttpEvent) -> Option<u64> {
    count(event);
    admit(event)
}

/// Decides the log line as [`degrade`] does, for an event already counted.
fn admit(event: HttpEvent) -> Option<u64> {
    HTTP_EVENT_THROTTLES.of(event).0.admit(clock::now())
}

pub fn total(event: HttpEvent) -> u64 {
    HTTP_EVENT_COUNTS.of(event).0.load(Ordering::Relaxed)
}

/// Returns whether `err` or an error in its source chain says the peer closed, reset or aborted
/// the connection, or ended it mid-message.
///
/// hyper can say so by its own predicates with no `io::Error` on the chain; otherwise the first
/// `io::Error` on the chain decides, as the stream layer judges it.
pub fn peer_went_away(err: &(dyn Error + 'static)) -> bool {
    let mut current: Option<&(dyn Error + 'static)> = Some(err);
    while let Some(e) = current {
        if let Some(hyper_err) = e.downcast_ref::<hyper::Error>() {
            if hyper_err.is_incomplete_message()
                || hyper_err.is_closed()
                || hyper_err.is_canceled()
                || hyper_err.is_body_write_aborted()
            {
                return true;
            }
        }
        if let Some(io_err) = e.downcast_ref::<io::Error>() {
            return peer_gone(io_err);
        }
        current = e.source();
    }
    false
}

/// Returns the kind to count when `err` or an error in its source chain is hyper refusing a
/// request head, which it has already answered with a 400, 414 or 431.
pub fn bad_request(err: &(dyn Error + 'static)) -> Option<HttpEvent> {
    let mut current: Option<&(dyn Error + 'static)> = Some(err);
    while let Some(e) = current {
        if let Some(hyper_err) = e.downcast_ref::<hyper::Error>() {
            if hyper_err.is_parse_too_large() {
                return Some(HttpEvent::OversizedRequestHead);
            }
            if hyper_err.is_parse() {
                return Some(HttpEvent::MalformedRequest);
            }
        }
        current = e.source();
    }
    None
}

/// Returns whether `err` or an error in its source chain is hyper's HTTP/1 header-read timeout.
pub fn timed_out(err: &(dyn Error + 'static)) -> bool {
    let mut current: Option<&(dyn Error + 'static)> = Some(err);
    while let Some(e) = current {
        if let Some(hyper_err) = e.downcast_ref::<hyper::Error>() {
            if hyper_err.is_timeout() {
                return true;
            }
        }
        current = e.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Holds a fresh count and throttle per kind, decided at the time a test hands in, as the
    /// statics are by [`degrade`].
    struct Tables {
        counts: HttpEventTable<OwnLine<AtomicU64>>,
        throttles: HttpEventTable<OwnLine<LogThrottle>>,
    }

    impl Tables {
        fn new() -> Self {
            Tables { counts: HttpEventTable::counts(), throttles: HttpEventTable::throttles() }
        }

        fn degrade(&self, event: HttpEvent, now: Duration) -> Option<u64> {
            self.counts.of(event).0.fetch_add(1, Ordering::Relaxed);
            self.throttles.of(event).0.admit(now)
        }

        fn total(&self, event: HttpEvent) -> u64 {
            self.counts.of(event).0.load(Ordering::Relaxed)
        }
    }
    #[test]
    fn an_error_whose_source_is_the_peer_going_away_is_the_peer_going_away() {
        // A `hyper::Error` from a reset connection has its own text and the io error as cause.
        #[derive(Debug)]
        struct Wrapper(io::Error);
        impl std::fmt::Display for Wrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("connection error")
            }
        }
        impl Error for Wrapper {
            fn source(&self) -> Option<&(dyn Error + 'static)> {
                Some(&self.0)
            }
        }
        let reset = Wrapper(io::Error::from(io::ErrorKind::ConnectionReset));
        assert!(peer_went_away(&reset));
        let refused = Wrapper(io::Error::from(io::ErrorKind::PermissionDenied));
        assert!(!peer_went_away(&refused));
    }

    /// Serves a real connection, because there is no other way to make a `hyper::Error`.
    #[tokio::test]
    async fn a_request_cut_short_by_the_client_is_the_peer_going_away() {
        use tokio::io::AsyncWriteExt;

        let service = hyper::service::service_fn(|_: hyper::Request<hyper::body::Incoming>| async {
            Ok::<_, std::convert::Infallible>(hyper::Response::new(http_body_util::Empty::<bytes::Bytes>::new()))
        });
        let (mut client, server) = tokio::io::duplex(4096);
        let conn = hyper::server::conn::http1::Builder::new()
            .serve_connection(hyper_util::rt::TokioIo::new(server), service);
        client.write_all(b"POST / HTTP/1.1\r\nhost: t\r\n").await.unwrap();
        drop(client);
        let err = conn.await.expect_err("the head never ended");
        assert!(err.is_incomplete_message(), "{err:?}");
        let mut chain: Option<&(dyn Error + 'static)> = Some(&err);
        let mut io_on_chain = false;
        while let Some(e) = chain {
            io_on_chain |= e.downcast_ref::<io::Error>().is_some();
            chain = e.source();
        }
        assert!(
            !io_on_chain,
            "no io::Error to judge by: only hyper's predicate says it: {err:?}"
        );
        assert!(peer_went_away(&err), "{err:?}");
    }

    #[derive(Clone, Copy)]
    enum Client {
        Closes,
        StaysOpen,
    }

    /// Returns the error hyper's HTTP/1 server ends a connection with after `sent`.
    ///
    /// A client that has closed makes hyper's 4xx answer fail as a broken pipe, so a bad request
    /// must come from one that stays open.
    async fn served_error(sent: &[u8], client_end: Client) -> hyper::Error {
        use tokio::io::AsyncWriteExt;

        let service = hyper::service::service_fn(|_: hyper::Request<hyper::body::Incoming>| async {
            Ok::<_, std::convert::Infallible>(hyper::Response::new(http_body_util::Empty::<bytes::Bytes>::new()))
        });
        let (mut client, server) = tokio::io::duplex(256 * 1024);
        let conn = hyper::server::conn::http1::Builder::new()
            .serve_connection(hyper_util::rt::TokioIo::new(server), service);
        client.write_all(sent).await.expect("the pipe holds what is sent");
        match client_end {
            Client::Closes => {
                drop(client);
                conn.await.expect_err("what was sent is no whole request")
            }
            Client::StaysOpen => {
                let err = conn.await.expect_err("what was sent is no request");
                drop(client);
                err
            }
        }
    }

    #[tokio::test]
    async fn a_request_hyper_cannot_parse_is_a_bad_request_by_kind() {
        let long_uri = format!("GET /{} HTTP/1.1\r\nhost: t\r\n\r\n", "a".repeat(70_000));
        let many_headers = format!(
            "GET / HTTP/1.1\r\n{}\r\n",
            (0..200).map(|i| format!("x-{i}: v\r\n")).collect::<String>()
        );
        let cases: [(&[u8], HttpEvent); 5] = [
            (b"\x16\x03\x01\x02\x00\x01\x00\x01\xfc\x03\x03", HttpEvent::MalformedRequest),
            (b"GET / HTTP/9.9\r\nhost: t\r\n\r\n", HttpEvent::MalformedRequest),
            (b"GET / HTTP/1.1\r\nno colon here\r\n\r\n", HttpEvent::MalformedRequest),
            (long_uri.as_bytes(), HttpEvent::OversizedRequestHead),
            (many_headers.as_bytes(), HttpEvent::OversizedRequestHead),
        ];
        for (sent, kind) in cases {
            let err = served_error(sent, Client::StaysOpen).await;
            assert_eq!(bad_request(&err), Some(kind), "{err:?}");
            assert!(!peer_went_away(&err), "{err:?}");
        }
        let cut_short = served_error(b"POST / HTTP/1.1\r\nhost: t\r\n", Client::Closes).await;
        assert_eq!(bad_request(&cut_short), None, "{cut_short:?}");
        assert_eq!(bad_request(&io::Error::from(io::ErrorKind::InvalidData)), None);
    }

    #[test]
    fn a_flood_of_bad_requests_is_every_one_counted_and_one_line_a_period() {
        let d = Tables::new();
        let kind = HttpEvent::MalformedRequest;
        let lines = (0..1000).filter(|_| d.degrade(kind, at(0)).is_some()).count();
        assert_eq!((d.total(kind), lines), (1000, 1));
        let just_before = LOG_PERIOD - Duration::from_nanos(1);
        assert_eq!(d.degrade(kind, just_before), None, "the period is not over");
        assert_eq!(d.degrade(kind, LOG_PERIOD), Some(1000), "the 999 after the first, and the one just before");
        assert_eq!(d.total(HttpEvent::OversizedRequestHead), 0, "the other kind is counted apart");
    }

    fn at(secs: u64) -> Duration {
        Duration::from_secs(secs)
    }
}
