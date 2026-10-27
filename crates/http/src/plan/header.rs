//! Copies header names and values into allocations of their own, and reads the authority a
//! request was sent to.
//!
//! A clone of a custom `HeaderName` or of any `HeaderValue` shares one `Bytes` refcount with the
//! original, so every thread serving the clone writes the same counter; a copy shares nothing.

use http::header::{HeaderMap, HeaderName, HeaderValue, HOST};
use http::Uri;

/// The most header lines a header map takes room for at once, and so the most a response carries:
/// `HeaderMap::with_capacity(n)` rounds `n + n / 3` up to a power of two and panics past 32,768
/// (`http`'s `MAX_SIZE`). With room for `n` lines, `n` appends never grow the map, so none of them
/// passes that limit either.
pub const MAX_HEADER_LINES: usize = 24_576;

/// Copies `name`; a standard name is a constant and allocates nothing.
#[expect(
    clippy::expect_used,
    reason = "the bytes of a valid header name are a valid header name: the parse cannot fail"
)]
pub fn own_header_name(name: &HeaderName) -> HeaderName {
    HeaderName::from_bytes(name.as_str().as_bytes()).expect("the bytes of a valid header name are a valid header name")
}

/// Copies `value`, keeping its sensitivity.
#[expect(
    clippy::expect_used,
    reason = "the bytes of a valid header value are a valid header value: the parse cannot fail"
)]
pub fn own_header_value(value: &HeaderValue) -> HeaderValue {
    let mut owned =
        HeaderValue::from_bytes(value.as_bytes()).expect("the bytes of a valid header value are a valid header value");
    owned.set_sensitive(value.is_sensitive());
    owned
}

/// Returns the `host[:port]` a request was sent to: the target's authority (absolute-form, HTTP/2
/// `:authority`) before `Host`, as RFC 9112 §3.2.2 requires, or `None` with neither.
#[inline]
pub fn request_authority<'a>(uri: &'a Uri, headers: &'a HeaderMap) -> Option<&'a [u8]> {
    if let Some(authority) = uri.authority() {
        return Some(authority.as_str().as_bytes());
    }
    // A client sends `Host` first (RFC 9112 §3.2) and a `HeaderMap` iterates in the order headers
    // came in, so the first header is looked at before the map is searched.
    match headers.iter().next() {
        Some((name, value)) if name == HOST => Some(value.as_bytes()),
        _ => headers.get(HOST).map(HeaderValue::as_bytes),
    }
}

#[cfg(test)]
mod fuzz;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_header_lines_is_the_most_a_header_map_takes_room_for() {
        assert!(HeaderMap::<HeaderValue>::try_with_capacity(MAX_HEADER_LINES).is_ok());
        assert!(HeaderMap::<HeaderValue>::try_with_capacity(MAX_HEADER_LINES + 1).is_err());
    }

    fn sent(headers: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.append(*name, HeaderValue::from_static(value));
        }
        map
    }

    #[test]
    fn host_is_found_first_or_anywhere_else() {
        let origin_form = Uri::from_static("/json");
        for headers in [
            sent(&[("host", "a.test"), ("connection", "keep-alive")]),
            sent(&[("connection", "keep-alive"), ("host", "a.test")]),
            sent(&[
                ("accept", "*/*"),
                ("connection", "x"),
                ("host", "a.test"),
                ("host", "b.test"),
            ]),
        ] {
            assert_eq!(
                request_authority(&origin_form, &headers),
                Some(&b"a.test"[..]),
                "{headers:?}"
            );
        }
        assert_eq!(request_authority(&origin_form, &sent(&[("accept", "*/*")])), None);
        assert_eq!(request_authority(&origin_form, &HeaderMap::new()), None);
    }
}
