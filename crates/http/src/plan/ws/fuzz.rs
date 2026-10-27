//! The WebSocket opening-handshake fuzz target: whatever a request carries, the handshake parses only when
//! every requirement of RFC 6455 §4.2.1 holds, its key is the request's own, and a refusal answers 426 for the
//! version alone and 400 for anything else.
//!
//! `cargo test` replays the committed corpus (`__fuzz__/.../corpus`) and nothing else, so the merge runs the same
//! inputs every time; `cargo bolero test -p mipyre_http --lib plan::ws::fuzz::...` mutates it under libFuzzer, and
//! an input it finds that fails goes into the corpus as a regression.

use http::header::{HeaderName, CONNECTION, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE};
use http::{Method, StatusCode, Version};

use super::{asks_for_websocket, UpgradeError, WsHandshake};
use crate::plan::fuzz_headers::header_map;

const HEADERS: [HeaderName; 4] = [UPGRADE, CONNECTION, SEC_WEBSOCKET_VERSION, SEC_WEBSOCKET_KEY];
const METHODS: [Method; 3] = [Method::GET, Method::POST, Method::CONNECT];
const VERSIONS: [Version; 3] = [Version::HTTP_11, Version::HTTP_10, Version::HTTP_2];

fn parse_request((method, version, fields): &(u8, u8, Vec<(u8, Vec<u8>)>)) {
    let mut parts = http::Request::new(()).into_parts().0;
    parts.method = METHODS[usize::from(*method) % METHODS.len()].clone();
    parts.version = VERSIONS[usize::from(*version) % VERSIONS.len()];
    parts.headers = header_map(&HEADERS, fields);

    match WsHandshake::parse(&parts) {
        Ok(handshake) => {
            assert!(asks_for_websocket(&parts.headers));
            assert_eq!(parts.method, Method::GET);
            assert_eq!(parts.version, Version::HTTP_11);
            assert_eq!(parts.headers[SEC_WEBSOCKET_VERSION], "13");
            assert_eq!(Some(handshake.key()), parts.headers.get(SEC_WEBSOCKET_KEY));
            assert!(!handshake.key().is_empty());
        }
        Err(refused) => {
            assert_eq!(
                matches!(refused, UpgradeError::NotWebsocket),
                !asks_for_websocket(&parts.headers),
                "{refused:?}"
            );
            let status = refused.response().status();
            let expected = match refused {
                UpgradeError::UnsupportedVersion(_) => StatusCode::UPGRADE_REQUIRED,
                _ => StatusCode::BAD_REQUEST,
            };
            assert_eq!(status, expected, "{refused:?}");
        }
    }
}

#[test]
fn any_request_is_a_full_handshake_or_refused_with_its_reason() {
    bolero::check!()
        .with_type::<(u8, u8, Vec<(u8, Vec<u8>)>)>()
        .with_iterations(0)
        .for_each(parse_request);
}
