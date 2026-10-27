//! The client request id fuzz target: whatever a client sends as its id, the id is taken only when it is 1 to
//! 128 bytes of the characters a log line cannot be forged with, and a taken id is written back exactly as sent, in
//! the response header, the log text and its display alike.
//!
//! `cargo test` replays the committed corpus (`__fuzz__/.../corpus`) and nothing else, so the merge runs the same
//! inputs every time; `cargo bolero test -p mipyre_http --lib plan::request_id::fuzz::...` mutates it under
//! libFuzzer, and an input it finds that fails goes into the corpus as a regression.

use http::header::HeaderValue;

use super::{ClientRequestId, RequestId, MAX_CLIENT_ID_LEN};

fn is_id_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"-_.:/+=;@~".contains(&b)
}

fn take_client_id(raw: &[u8]) {
    let Ok(value) = HeaderValue::from_bytes(raw) else {
        return;
    };
    let taken = ClientRequestId::from_header(&value);
    let usable = (1..=MAX_CLIENT_ID_LEN).contains(&raw.len()) && raw.iter().copied().all(is_id_byte);
    assert_eq!(taken.is_some(), usable, "{raw:?}");
    if let Ok(text) = std::str::from_utf8(raw) {
        assert_eq!(ClientRequestId::parse(text), taken, "{text:?}");
    }
    let Some(id) = taken else {
        return;
    };
    assert_eq!(id.as_bytes(), raw);
    let id = RequestId::Client(id);
    assert_eq!(id.header_value(), value);
    assert_eq!(id.to_string().as_bytes(), raw);
    id.with_text(|text| assert_eq!(text.as_bytes(), raw));
}

#[test]
fn any_header_value_is_taken_as_sent_or_refused() {
    bolero::check!().with_iterations(0).for_each(take_client_id);
}
