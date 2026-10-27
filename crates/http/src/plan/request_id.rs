//! Holds the correlation id that an error response and its log line both carry: its value, its
//! text, and which ids a client may send. Minting one reads process and thread state, so it is
//! `io::request_id`'s.

use std::fmt::{self, Write as _};

use http::header::HeaderValue;

pub(crate) const MAX_CLIENT_ID_LEN: usize = 128;

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

impl RequestId {
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
            RequestId::Server(_) => self.with_text(|text| f.write_str(text)),
            RequestId::Client(id) => id.fmt(f),
        }
    }
}

#[cfg(test)]
mod fuzz;
