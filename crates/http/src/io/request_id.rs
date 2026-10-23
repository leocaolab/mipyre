//! Mints or takes the correlation id that an error response and its log line both carry.
//!
//! Taking the client's id is opt-in, because an id the client picks puts the client's words
//! into the server's log.

use std::cell::Cell;
use std::fmt::{self, Write as _};
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

use hyper::header::{HeaderMap, HeaderName, HeaderValue};

const MAX_CLIENT_ID_LEN: usize = 128;

/// Threads take minted numbers in blocks of this many, so the shared atomic moves once per block.
const BLOCK: u64 = 1 << 20;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestId {
    Server(ServerRequestId),
    Client(ClientRequestId),
}

/// An id a server process minted. It carries the process's seed, so a process-lane worker it is
/// handed to writes it the same way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerRequestId {
    pub process: u64,
    pub number: u64,
}

/// A request id a client sent: 1..=128 bytes of letters, digits and `- _ . : / + = ; @ ~`.
///
/// That covers UUID, hex, base64, W3C `traceparent` and AWS `X-Amzn-Trace-Id` ids. Spaces,
/// quotes, backslashes and brackets are refused, because the id goes into key=value and JSON log
/// lines, where they could end its field and forge another (`abc status=200 user=admin`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientRequestId(HeaderValue);

fn usable(bytes: &[u8]) -> bool {
    (1..=MAX_CLIENT_ID_LEN).contains(&bytes.len())
        && bytes.iter().all(|&b| {
            b.is_ascii_alphanumeric()
                || matches!(b, b'-' | b'_' | b'.' | b':' | b'/' | b'+' | b'=' | b';' | b'@' | b'~')
        })
}

impl ClientRequestId {
    pub fn parse(text: &str) -> Option<Self> {
        if !usable(text.as_bytes()) {
            return None;
        }
        HeaderValue::from_str(text).ok().map(ClientRequestId)
    }

    /// Shares the header value's bytes rather than copying them.
    pub fn from_header(value: &HeaderValue) -> Option<Self> {
        usable(value.as_bytes()).then(|| ClientRequestId(value.clone()))
    }

    /// Returns the id's bytes, which are always ASCII.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for ClientRequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Every byte is ASCII, so each is its own char.
        self.as_bytes().iter().try_for_each(|&b| f.write_char(char::from(b)))
    }
}

/// Distinguishes this process's minted ids from those of restarts and replicas.
static PROCESS_SEED: LazyLock<u64> = LazyLock::new(|| RandomState::new().hash_one(0u8));

static NEXT_BLOCK: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// This thread's next number and block end; it starts used up, so a thread takes a block
    /// only when it first mints.
    static BLOCK_LEFT: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
}

/// Returns the next number, taking a fresh block from `next_block` when `block` is used up.
///
/// Blocks never overlap, because the shared counter would take 2^44 blocks to wrap.
fn next_number(block: &Cell<(u64, u64)>, next_block: &AtomicU64) -> u64 {
    let (mut next, mut end) = block.get();
    if next == end {
        next = next_block.fetch_add(BLOCK, Ordering::Relaxed);
        end = next.wrapping_add(BLOCK);
    }
    block.set((next.wrapping_add(1), end));
    next
}

impl RequestId {
    /// Mints a fresh id without allocating or touching a shared cache line.
    #[inline]
    pub fn mint() -> Self {
        let number = BLOCK_LEFT.with(|block| next_number(block, &NEXT_BLOCK));
        RequestId::Server(ServerRequestId {
            process: *PROCESS_SEED,
            number,
        })
    }

    /// Hands `f` the id as text, as the header and a log line write it, without allocating: a
    /// client's is its own bytes, a minted one its 32 hex digits on the stack.
    #[expect(
        clippy::expect_used,
        reason = "a client's id passed `usable`, so it is ASCII, and a minted one is hex digits: \
                  both are UTF-8"
    )]
    #[inline]
    pub fn with_text<R>(&self, f: impl FnOnce(&str) -> R) -> R {
        match self {
            RequestId::Client(id) => f(std::str::from_utf8(id.as_bytes()).expect("an ASCII id")),
            RequestId::Server(id) => f(std::str::from_utf8(&id.hex()).expect("hex digits")),
        }
    }

    /// Returns the id as a response header's value: a client's shares its bytes, and a minted
    /// one is written into an allocation of its own.
    #[expect(
        clippy::expect_used,
        reason = "a client's id is a header value already, and a minted one is 32 lower-case hex \
                  digits, which are a valid header value: the parse cannot fail"
    )]
    pub fn header_value(&self) -> HeaderValue {
        match self {
            RequestId::Client(id) => id.0.clone(),
            RequestId::Server(id) => HeaderValue::from_bytes(&id.hex()).expect("hex digits are a valid header value"),
        }
    }

    /// Takes the client's id from `header` when the app named one and the value is usable, and
    /// mints one otherwise.
    #[inline]
    pub fn of(headers: &HeaderMap, header: Option<&HeaderName>) -> Self {
        header
            .and_then(|name| headers.get(name))
            .and_then(ClientRequestId::from_header)
            .map_or_else(RequestId::mint, RequestId::Client)
    }
}

impl ServerRequestId {
    /// Returns the id as its 32 lower-case hex digits, the process's seed first.
    fn hex(&self) -> [u8; 32] {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let bytes = [self.process.to_be_bytes(), self.number.to_be_bytes()];
        let mut out = [0; 32];
        for (pair, byte) in out.as_chunks_mut::<2>().0.iter_mut().zip(bytes.iter().flatten()) {
            pair[0] = DIGITS[usize::from(byte >> 4)];
            pair[1] = DIGITS[usize::from(byte & 0xf)];
        }
        out
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestId::Server(id) => write!(f, "{:016x}{:016x}", id.process, id.number),
            RequestId::Client(id) => id.fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::HeaderValue;

    const HEADER: HeaderName = HeaderName::from_static("x-request-id");

    fn headers(value: &'static [u8]) -> HeaderMap {
        let mut map = HeaderMap::new();
        map.insert(HEADER, HeaderValue::from_bytes(value).unwrap());
        map
    }

    #[test]
    fn the_header_value_is_the_id_as_written() {
        let minted = RequestId::Server(ServerRequestId {
            process: 0x0123_4567_89ab_cdef,
            number: 42,
        });
        assert_eq!(minted.header_value(), "0123456789abcdef000000000000002a");
        assert_eq!(minted.header_value(), minted.to_string().as_str());
        let fresh = RequestId::mint();
        assert_eq!(fresh.header_value(), fresh.to_string().as_str());
        let client = RequestId::of(&headers(b"trace-7f"), Some(&HEADER));
        assert_eq!(client.header_value(), "trace-7f");
    }

    #[test]
    fn the_text_is_the_id_as_displayed() {
        let minted = RequestId::Server(ServerRequestId {
            process: 0x0123_4567_89ab_cdef,
            number: 42,
        });
        assert_eq!(minted.with_text(str::to_owned), minted.to_string());
        let client = RequestId::of(&headers(b"Root=1-67891233;Sampled=1"), Some(&HEADER));
        assert_eq!(client.with_text(str::to_owned), "Root=1-67891233;Sampled=1");
    }

    #[test]
    fn minted_ids_are_distinct_and_fixed_width() {
        let a = RequestId::mint();
        let b = RequestId::mint();
        assert_ne!(a, b);
        assert_ne!(a.to_string(), b.to_string());
        assert_eq!(a.to_string().len(), 32);
        assert!(a.to_string().bytes().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn minted_ids_of_two_threads_never_collide() {
        let other = std::thread::spawn(|| (0..1000).map(|_| RequestId::mint()).collect::<Vec<_>>())
            .join()
            .unwrap();
        let here: Vec<_> = (0..1000).map(|_| RequestId::mint()).collect();
        assert!(here.iter().all(|id| !other.contains(id)));
    }

    #[test]
    fn the_client_id_is_used_only_when_the_app_names_the_header() {
        let map = headers(b"abc-123");
        assert_eq!(RequestId::of(&map, Some(&HEADER)).to_string(), "abc-123");
        assert!(matches!(RequestId::of(&map, None), RequestId::Server(_)));
    }

    #[test]
    fn an_unusable_client_id_is_replaced_by_a_minted_one() {
        let long = [b'a'; MAX_CLIENT_ID_LEN + 1];
        for value in [&b""[..], &b"caf\xc3\xa9"[..], &long[..]] {
            let mut map = HeaderMap::new();
            map.insert(HEADER, HeaderValue::from_bytes(value).unwrap());
            assert!(
                matches!(RequestId::of(&map, Some(&HEADER)), RequestId::Server(_)),
                "{value:?}"
            );
        }
        let absent = HeaderMap::new();
        assert!(matches!(RequestId::of(&absent, Some(&HEADER)), RequestId::Server(_)));
    }

    #[test]
    fn a_client_id_that_could_forge_a_log_field_is_replaced() {
        for forged in [
            &b"abc status=200 user=admin"[..],
            b"   ",
            b"a\tb",
            b"a\"b",
            b"a\\b",
            b"[ok]",
            b"{x}",
        ] {
            let mut map = HeaderMap::new();
            map.insert(HEADER, HeaderValue::from_bytes(forged).unwrap());
            assert!(
                matches!(RequestId::of(&map, Some(&HEADER)), RequestId::Server(_)),
                "{forged:?}"
            );
        }
        for kept in [
            "4bf92f35-77b3-4da6-a3ce-929d0e0e4736",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1",
            "dGhpcyBpcyBhbiBpZA==",
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        ] {
            let mut map = HeaderMap::new();
            map.insert(HEADER, HeaderValue::from_static(kept));
            assert_eq!(RequestId::of(&map, Some(&HEADER)).to_string(), kept);
        }
    }

    #[test]
    fn a_used_up_block_is_followed_by_a_fresh_one() {
        let next_block = AtomicU64::new(0);
        let (a, b) = (Cell::new((0, 0)), Cell::new((0, 0)));
        let first_a = next_number(&a, &next_block);
        let first_b = next_number(&b, &next_block);
        assert_eq!((first_a, first_b), (0, BLOCK));
        for _ in 1..BLOCK {
            next_number(&a, &next_block);
        }
        assert_eq!(next_number(&a, &next_block), 2 * BLOCK, "a's second block, past b's");
        assert_eq!(next_number(&b, &next_block), BLOCK + 1, "b still in its first");
    }
}
