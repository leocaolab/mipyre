//! The header fuzz target: whatever target and headers a client sends, the authority read is the target's
//! own, else the first `Host` line, else none; and a header name or value it sent is copied byte for byte, keeping
//! a value's sensitivity, into bytes the original does not share.
//!
//! `cargo test` replays the committed corpus (`__fuzz__/.../corpus`) and nothing else, so the merge runs the same
//! inputs every time; `cargo bolero test -p mipyre_http --lib plan::header::fuzz::...` mutates it under libFuzzer,
//! and an input it finds that fails goes into the corpus as a regression.

use http::header::{HeaderName, HeaderValue, ACCEPT, CONNECTION, HOST};
use http::Uri;

use super::{own_header_name, own_header_value, request_authority};
use crate::plan::fuzz_headers::header_map;

const HEADERS: [HeaderName; 3] = [HOST, CONNECTION, ACCEPT];

type Input = (Vec<u8>, Vec<(u8, Vec<u8>)>, Vec<u8>, bool);

fn read_authority(target: &[u8], fields: &[(u8, Vec<u8>)]) {
    let Ok(uri) = Uri::try_from(target) else {
        return;
    };
    let headers = header_map(&HEADERS, fields);
    let expected = match uri.authority() {
        Some(authority) => Some(authority.as_str().as_bytes()),
        None => headers.get_all(HOST).iter().next().map(HeaderValue::as_bytes),
    };
    assert_eq!(request_authority(&uri, &headers), expected, "{uri:?} {headers:?}");
}

fn copy_name(raw: &[u8]) {
    let Ok(name) = HeaderName::from_bytes(raw) else {
        return;
    };
    assert_eq!(own_header_name(&name), name);
}

fn copy_value(raw: &[u8], sensitive: bool) {
    let Ok(mut value) = HeaderValue::from_bytes(raw) else {
        return;
    };
    value.set_sensitive(sensitive);
    let owned = own_header_value(&value);
    assert_eq!(owned.as_bytes(), raw);
    assert_eq!(owned.is_sensitive(), sensitive);
    if !raw.is_empty() {
        assert_ne!(owned.as_bytes().as_ptr(), value.as_bytes().as_ptr(), "{raw:?}");
    }
}

fn decide((target, fields, raw, sensitive): &Input) {
    read_authority(target, fields);
    copy_name(raw);
    copy_value(raw, *sensitive);
}

#[test]
fn any_target_and_headers_read_one_authority_and_copy_as_sent() {
    bolero::check!()
        .with_type::<Input>()
        .with_iterations(0)
        .for_each(decide);
}
