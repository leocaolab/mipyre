//! Copies header names and values into allocations of their own.
//!
//! A clone of a custom `HeaderName` or of any `HeaderValue` shares one `Bytes` refcount with the
//! original, so every thread serving the clone writes the same counter; a copy shares nothing.

use http::header::{HeaderName, HeaderValue};

/// Copies `name`; a standard name is a constant and allocates nothing.
#[expect(
    clippy::expect_used,
    reason = "the bytes of a valid header name are a valid header name: the parse cannot fail"
)]
pub fn own_header_name(name: &HeaderName) -> HeaderName {
    HeaderName::from_bytes(name.as_str().as_bytes())
        .expect("the bytes of a valid header name are a valid header name")
}

/// Copies `value`, keeping its sensitivity.
#[expect(
    clippy::expect_used,
    reason = "the bytes of a valid header value are a valid header value: the parse cannot fail"
)]
pub fn own_header_value(value: &HeaderValue) -> HeaderValue {
    let mut owned = HeaderValue::from_bytes(value.as_bytes())
        .expect("the bytes of a valid header value are a valid header value");
    owned.set_sensitive(value.is_sensitive());
    owned
}
