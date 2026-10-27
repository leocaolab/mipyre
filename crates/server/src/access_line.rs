//! Logs the access line of a response and writes it in the JSON format by hand.
//!
//! tracing-subscriber's JSON format checks and copies every piece serde_json writes (a quote, a
//! key, a comma) on its own and formats the timestamp into a `String` first, which is most of what
//! the access log costs a request. [`JsonLines`] writes the access line straight into the buffer,
//! byte for byte as tracing's JSON format would, and hands every other event to tracing's format.

use std::fmt::{self, Write as _};
use std::net::IpAddr;

use mipyre_transport::driver::Peer;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::{Format, Json, Writer};
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// The filter turns this target off when the access log is off.
pub const ACCESS_TARGET: &str = "mipyre::access";

/// Logs one response's access line. `request_id` is the id the response and the handler's logs
/// carry, so the line can be found by it; `client` is who sent the request, and an in-memory
/// connection, which has no address, writes no `client_ip`. Both are written as plain strings,
/// which the line's format copies without going through `fmt`.
#[inline]
pub(crate) fn log_access(
    method: &str,
    path: &str,
    status: u16,
    latency_us: u64,
    source: &'static str,
    request_id: &str,
    client: Peer,
) {
    match client {
        Peer::Ip(ip) => {
            let mut text = [0u8; IP_TEXT_LEN];
            let client_ip = ip_text(ip, &mut text);
            tracing::info!(
                target: ACCESS_TARGET,
                method,
                path,
                status,
                latency_us,
                source,
                request_id,
                client_ip,
                "Request handled"
            );
        }
        Peer::InMemory => tracing::info!(
            target: ACCESS_TARGET,
            method,
            path,
            status,
            latency_us,
            source,
            request_id,
            "Request handled"
        ),
    }
}

/// The longest address `ip_text` writes: an IPv6 address with an embedded IPv4 one.
const IP_TEXT_LEN: usize = 45;

/// Writes `ip` into `out` as `IpAddr`'s `Display` does and returns it; an IPv4 address, the common
/// case, is written digit by digit rather than through `fmt`.
#[expect(
    clippy::expect_used,
    reason = "IP_TEXT_LEN is the longest address Display writes, and what it writes is ASCII"
)]
fn ip_text(ip: IpAddr, out: &mut [u8; IP_TEXT_LEN]) -> &str {
    let len = match ip {
        IpAddr::V4(v4) => {
            let mut at = 0;
            for (i, octet) in v4.octets().into_iter().enumerate() {
                if i > 0 {
                    out[at] = b'.';
                    at += 1;
                }
                if octet >= 100 {
                    out[at] = b'0' + octet / 100;
                    at += 1;
                }
                if octet >= 10 {
                    out[at] = b'0' + octet / 10 % 10;
                    at += 1;
                }
                out[at] = b'0' + octet % 10;
                at += 1;
            }
            at
        }
        IpAddr::V6(v6) => {
            let mut rest: &mut [u8] = out;
            std::io::Write::write_fmt(&mut rest, format_args!("{v6}")).expect("an IPv6 address fits");
            IP_TEXT_LEN - rest.len()
        }
    };
    std::str::from_utf8(&out[..len]).expect("an address is ASCII")
}

/// The log's JSON format, which shares one timer with the tracing format it falls back to.
pub struct JsonLines<T> {
    tracing: Format<Json, T>,
    timer: T,
}

impl<T: FormatTime + Clone> JsonLines<T> {
    pub fn new(timer: T) -> Self {
        JsonLines {
            tracing: tracing_subscriber::fmt::format().json().with_timer(timer.clone()),
            timer,
        }
    }
}

impl<S, N, T> FormatEvent<S, N> for JsonLines<T>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'w> FormatFields<'w> + 'static,
    T: FormatTime,
{
    fn format_event(&self, ctx: &FmtContext<'_, S, N>, mut writer: Writer<'_>, event: &Event<'_>) -> fmt::Result {
        let meta = event.metadata();
        // Tracing's format adds the span an event is in, and the hand-written line has none, so
        // an event in a span is left to tracing.
        if meta.target() != ACCESS_TARGET || event.parent().is_some() || ctx.lookup_current().is_some() {
            return self.tracing.format_event(ctx, writer, event);
        }
        writer.write_str("{\"timestamp\":\"")?;
        self.timer
            .format_time(&mut Writer::new(&mut JsonEscaper(&mut writer)))?;
        writer.write_str("\",\"level\":\"")?;
        writer.write_str(meta.level().as_str())?;
        writer.write_str("\",\"fields\":{")?;
        let mut fields = JsonMembers {
            out: &mut writer,
            first: true,
            written: Ok(()),
        };
        event.record(&mut fields);
        fields.written?;
        writer.write_str("},\"target\":")?;
        write_json_str(&mut writer, meta.target())?;
        writer.write_str("}\n")
    }
}

/// Writes an event's fields as JSON object members the way tracing-serde does, with strings and
/// `Debug` values as JSON strings and numbers and booleans as they are. The first failed write
/// stops it and stays in `written`.
struct JsonMembers<'a, W: fmt::Write> {
    out: &'a mut W,
    first: bool,
    written: fmt::Result,
}

impl<W: fmt::Write> JsonMembers<'_, W> {
    fn member(&mut self, field: &Field, value: impl FnOnce(&mut W) -> fmt::Result) {
        if self.written.is_err() {
            return;
        }
        let out = &mut *self.out;
        let open = if self.first { "\"" } else { ",\"" };
        self.first = false;
        self.written = out
            .write_str(open)
            .and_then(|()| JsonEscaper(&mut *out).write_str(field.name()))
            .and_then(|()| out.write_str("\":"))
            .and_then(|()| value(out));
    }
}

impl<W: fmt::Write> Visit for JsonMembers<'_, W> {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.member(field, |out| out.write_str(if value { "true" } else { "false" }));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.member(field, |out| {
            out.write_char('"')?;
            write!(JsonEscaper(&mut *out), "{value:?}")?;
            out.write_char('"')
        });
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.member(field, |out| out.write_str(itoa::Buffer::new().format(value)));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.member(field, |out| out.write_str(itoa::Buffer::new().format(value)));
    }

    /// Writes a non-finite float as `null`, as serde_json does.
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.member(field, |out| match serde_json::Number::from_f64(value) {
            Some(number) => write!(out, "{number}"),
            None => out.write_str("null"),
        });
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.member(field, |out| write_json_str(out, value));
    }
}

fn write_json_str(out: &mut impl fmt::Write, s: &str) -> fmt::Result {
    out.write_char('"')?;
    JsonEscaper(&mut *out).write_str(s)?;
    out.write_char('"')
}

/// Escapes what it writes as the inside of a JSON string, as serde_json does: `"`, `\` and the
/// control characters below U+0020, the common ones by their short form and the rest as `\u00XX`.
struct JsonEscaper<'a, W: fmt::Write>(&'a mut W);

/// Maps a byte to 0 when it is written as it is, to `u` for `\u00XX`, and otherwise to the letter
/// after the backslash of its short form, as serde_json's table does.
const ESCAPE: [u8; 256] = {
    let mut table = [0u8; 256];
    let mut byte = 0;
    while byte < 0x20 {
        table[byte] = b'u';
        byte += 1;
    }
    table[0x08] = b'b';
    table[0x09] = b't';
    table[0x0a] = b'n';
    table[0x0c] = b'f';
    table[0x0d] = b'r';
    table[b'"' as usize] = b'"';
    table[b'\\' as usize] = b'\\';
    table
};

const HEX: &[u8; 16] = b"0123456789abcdef";

impl<W: fmt::Write> fmt::Write for JsonEscaper<'_, W> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let mut plain = 0;
        for (at, &byte) in s.as_bytes().iter().enumerate() {
            let escape = ESCAPE[usize::from(byte)];
            if escape == 0 {
                continue;
            }
            // Every byte escaped is ASCII, so `plain..at` and `at + 1..` are on char boundaries.
            self.0.write_str(&s[plain..at])?;
            self.0.write_char('\\')?;
            if escape == b'u' {
                self.0.write_str("u00")?;
                self.0.write_char(char::from(HEX[usize::from(byte >> 4)]))?;
                self.0.write_char(char::from(HEX[usize::from(byte & 0xf)]))?;
            } else {
                self.0.write_char(char::from(escape))?;
            }
            plain = at + 1;
        }
        self.0.write_str(&s[plain..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mipyre_http::plan::request_id::{ClientRequestId, RequestId, ServerRequestId};
    use std::io;
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::layer::SubscriberExt;

    /// A timestamp with characters JSON escapes, so both formats are compared on it too.
    const ESCAPED_STAMP: Stamp = Stamp("2000-01-01T00:00:00.000001Z \"\\\u{1}");

    #[derive(Clone)]
    struct Stamp(&'static str);

    impl FormatTime for Stamp {
        fn format_time(&self, w: &mut Writer<'_>) -> fmt::Result {
            w.write_str(self.0)
        }
    }

    #[derive(Clone, Default)]
    struct Lines(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Lines {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Lines {
        type Writer = Lines;
        fn make_writer(&'a self) -> Lines {
            self.clone()
        }
    }

    /// Returns what tracing's JSON format and [`JsonLines`] write for what `log` logs.
    fn both_formats(log: impl Fn()) -> (String, String) {
        let tracing_lines = Lines::default();
        let ours = Lines::default();
        let tracing_format = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_timer(ESCAPED_STAMP)
                .with_writer(tracing_lines.clone()),
        );
        let our_format = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .event_format(JsonLines::new(ESCAPED_STAMP))
                .with_writer(ours.clone()),
        );
        mipyre_runtime::testkit::ask_every_subscriber();
        tracing::subscriber::with_default(tracing_format, &log);
        tracing::subscriber::with_default(our_format, &log);
        let text = |lines: Lines| String::from_utf8(lines.0.lock().unwrap().clone()).unwrap();
        (text(tracing_lines), text(ours))
    }

    const SOURCES: [&str; 4] = ["rust", "tpc-inline", "subinterp", "main"];

    proptest::proptest! {
        /// The method and path are whatever a client sends.
        #[test]
        fn the_access_line_is_what_tracings_json_format_writes(
            method in ".*",
            path in proptest::string::string_regex("[\\x00-\\x7f\u{80}-\u{10ffff}]{0,40}").unwrap(),
            status: u16,
            latency_us: u64,
            source in 0..SOURCES.len(),
            client_id in proptest::option::of("[A-Za-z0-9._:/+=;@~-]{1,128}"),
            number: u64,
            ip: Option<std::net::IpAddr>,
        ) {
            let id = match client_id {
                Some(text) => RequestId::Client(ClientRequestId::parse(&text).unwrap()),
                None => RequestId::Server(ServerRequestId { process: 7, number }),
            };
            let client = ip.map_or(Peer::InMemory, Peer::Ip);
            let (tracing_line, ours) = both_formats(|| {
                id.with_text(|id| log_access(&method, &path, status, latency_us, SOURCES[source], id, client));
            });
            proptest::prop_assert!(tracing_line.contains("Request handled"));
            proptest::prop_assert_eq!(ours, tracing_line);
        }

        #[test]
        fn a_string_is_escaped_as_serde_json_escapes_it(s in ".*") {
            let mut ours = String::new();
            write_json_str(&mut ours, &s).unwrap();
            proptest::prop_assert_eq!(ours, serde_json::to_string(&s).unwrap());
        }
    }

    proptest::proptest! {
        /// Every address is written as `IpAddr`'s `Display` writes it.
        #[test]
        fn an_address_is_written_as_display_writes_it(ip: IpAddr) {
            let mut text = [0u8; IP_TEXT_LEN];
            proptest::prop_assert_eq!(ip_text(ip, &mut text), ip.to_string());
        }
    }

    #[test]
    fn the_longest_address_fits() {
        let ip: IpAddr = "ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255".parse().unwrap();
        let mut text = [0u8; IP_TEXT_LEN];
        assert_eq!(ip_text(ip, &mut text), ip.to_string());
    }

    #[test]
    fn each_control_character_is_escaped_as_serde_json_escapes_it() {
        let s: String = (0u8..0x20).map(char::from).chain(['"', '\\', '\u{7f}', 'é']).collect();
        let mut ours = String::new();
        write_json_str(&mut ours, &s).unwrap();
        assert_eq!(ours, serde_json::to_string(&s).unwrap());
    }

    /// The line carries the id the response carries and the client's address, so it can be
    /// joined with the trace header and the handler's lines; an in-memory client has no address.
    #[test]
    fn the_access_line_names_the_request_id_and_the_client() {
        let id = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        let (_, ours) = both_formats(|| log_access("GET", "/hello", 200, 7, "rust", id, Peer::Ip(ip)));
        assert!(
            ours.contains(
                r#""source":"rust","request_id":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","client_ip":"203.0.113.9"},"#
            ),
            "{ours}"
        );

        let (_, ours) = both_formats(|| log_access("GET", "/hello", 200, 7, "rust", "a1", Peer::InMemory));
        assert!(ours.contains(r#""request_id":"a1"},"#), "{ours}");
        assert!(!ours.contains("client_ip"), "{ours}");
    }

    /// An access line in a span is left to tracing, which adds the span.
    #[test]
    fn every_other_line_is_tracings_own() {
        let (tracing_line, ours) = both_formats(|| {
            let span = tracing::info_span!("request", id = 7);
            let _in_span = span.enter();
            log_access("GET", "/", 200, 1, "rust", "a1", Peer::InMemory);
        });
        assert!(tracing_line.contains("\"span\""), "{tracing_line}");
        assert_eq!(ours, tracing_line);

        let (tracing_line, ours) = both_formats(|| {
            tracing::warn!(target: "mipyre::server", flag = true, n = -3i64, x = 1.5f64,
                nan = f64::NAN, err = %"a \"b\"", "something {}", 1);
        });
        assert!(tracing_line.contains("mipyre::server"), "{tracing_line}");
        assert_eq!(ours, tracing_line);
    }

    /// The fallback's timer writes another stamp, so only the access line carries [`JsonLines`]'
    /// own.
    #[test]
    fn only_the_access_line_is_written_by_hand() {
        let lines = Lines::default();
        let format = JsonLines {
            tracing: tracing_subscriber::fmt::format().json().with_timer(Stamp("tracing's")),
            timer: Stamp("by hand"),
        };
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .event_format(format)
                .with_writer(lines.clone()),
        );
        mipyre_runtime::testkit::ask_every_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            log_access("GET", "/", 200, 1, "rust", "a1", Peer::InMemory);
            tracing::warn!(target: "mipyre::server", "not an access line");
        });
        let text = String::from_utf8(lines.0.lock().unwrap().clone()).unwrap();
        let written: Vec<&str> = text.lines().collect();
        assert_eq!(written.len(), 2, "{text}");
        assert!(written[0].starts_with("{\"timestamp\":\"by hand\""), "{text}");
        assert!(written[1].starts_with("{\"timestamp\":\"tracing's\""), "{text}");
    }

    /// An access-target event may carry fields beyond the access line's own.
    #[test]
    fn every_kind_of_field_is_written_as_tracing_serde_writes_it() {
        let (tracing_line, ours) = both_formats(|| {
            tracing::info!(target: ACCESS_TARGET, flag = false, n = -3i64, u = 4u64, x = 1.5f64,
                inf = f64::INFINITY, s = "q\"\n", d = ?("x", 1), shown = %"a\\b", big = 1u128 << 100,
                "line {}", "\u{1}");
        });
        assert!(tracing_line.contains(ACCESS_TARGET), "{tracing_line}");
        assert_eq!(ours, tracing_line);
    }
}
