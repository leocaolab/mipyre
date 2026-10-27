//! Decides whether a request is a valid WebSocket opening handshake.

use bytes::Bytes;
use http::header::{HeaderMap, HeaderValue, CONNECTION, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE};
use http::request::Parts;
use http::{Method, Response, StatusCode, Version};

const WS_VERSION: &str = "13";

/// A valid opening handshake (RFC 6455 §4.2.1); it is checked before a connection slot, a hook
/// or a thread is spent on the request.
#[derive(Debug)]
pub struct WsHandshake {
    key: HeaderValue,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum UpgradeError {
    #[error("a WebSocket upgrade needs `Upgrade: websocket`")]
    NotWebsocket,
    #[error("a WebSocket upgrade needs HTTP/1.1, got {0:?}")]
    NotHttp11(Version),
    #[error("a WebSocket upgrade must be a GET, got {0}")]
    NotGet(Method),
    #[error("a WebSocket upgrade needs `Connection: upgrade`")]
    NoConnectionUpgrade,
    #[error("a WebSocket upgrade needs a Sec-WebSocket-Key")]
    MissingKey,
    #[error("this server speaks WebSocket version {WS_VERSION}")]
    UnsupportedVersion(Option<HeaderValue>),
}

/// Returns whether an `Upgrade` header names `websocket` among its comma-separated,
/// case-insensitive protocols (RFC 9110 §7.8).
///
/// The `Connection: upgrade` check in [`WsHandshake::parse`] repeats this scan on purpose: a
/// shared helper cost the request decision one instruction per request.
#[inline]
pub fn asks_for_websocket(headers: &HeaderMap) -> bool {
    headers
        .get_all(UPGRADE)
        .iter()
        .flat_map(|v| v.as_bytes().split(|&b| b == b','))
        .any(|t| t.trim_ascii().eq_ignore_ascii_case(b"websocket"))
}

impl WsHandshake {
    /// Checks every requirement of RFC 6455 §4.2.1.
    ///
    /// # Errors
    ///
    /// Returns the first requirement `parts` miss.
    pub fn parse(parts: &Parts) -> Result<Self, UpgradeError> {
        if !asks_for_websocket(&parts.headers) {
            return Err(UpgradeError::NotWebsocket);
        }
        if parts.version != Version::HTTP_11 {
            return Err(UpgradeError::NotHttp11(parts.version));
        }
        if parts.method != Method::GET {
            return Err(UpgradeError::NotGet(parts.method.clone()));
        }
        let headers = &parts.headers;
        let connection_upgrade = headers
            .get_all(CONNECTION)
            .iter()
            .flat_map(|v| v.as_bytes().split(|&b| b == b','))
            .any(|token| token.trim_ascii().eq_ignore_ascii_case(b"upgrade"));
        if !connection_upgrade {
            return Err(UpgradeError::NoConnectionUpgrade);
        }
        let version = headers.get(SEC_WEBSOCKET_VERSION);
        if version.is_none_or(|v| v.as_bytes() != WS_VERSION.as_bytes()) {
            return Err(UpgradeError::UnsupportedVersion(version.cloned()));
        }
        let key = headers
            .get(SEC_WEBSOCKET_KEY)
            .filter(|k| !k.is_empty())
            .ok_or(UpgradeError::MissingKey)?;
        Ok(WsHandshake { key: key.clone() })
    }

    pub fn key(&self) -> &HeaderValue {
        &self.key
    }
}

impl UpgradeError {
    /// Returns 426 naming the supported version for an unsupported version (RFC 6455 §4.2.2), and
    /// 400 with the reason for anything else.
    pub fn response(&self) -> Response<Bytes> {
        let mut resp = Response::new(Bytes::from(self.to_string()));
        if matches!(self, UpgradeError::UnsupportedVersion(_)) {
            *resp.status_mut() = StatusCode::UPGRADE_REQUIRED;
            resp.headers_mut()
                .insert(SEC_WEBSOCKET_VERSION, HeaderValue::from_static(WS_VERSION));
        } else {
            *resp.status_mut() = StatusCode::BAD_REQUEST;
        }
        resp
    }
}

#[cfg(test)]
mod fuzz;

#[cfg(test)]
mod tests {
    use super::*;

    fn handshake_parts(edit: impl FnOnce(http::request::Builder) -> http::request::Builder) -> Parts {
        let valid = http::Request::builder()
            .method("GET")
            .uri("/ws")
            .version(Version::HTTP_11)
            .header("upgrade", "websocket")
            .header("connection", "keep-alive, Upgrade")
            .header("sec-websocket-version", "13")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==");
        edit(valid).body(()).unwrap().into_parts().0
    }

    #[test]
    fn a_valid_handshake_parses() {
        let parsed = WsHandshake::parse(&handshake_parts(|b| b)).unwrap();
        assert_eq!(parsed.key, "dGhlIHNhbXBsZSBub25jZQ==");
    }

    #[test]
    fn an_invalid_handshake_is_refused_with_its_reason() {
        let refused = |edit: fn(http::request::Builder) -> http::request::Builder| {
            WsHandshake::parse(&handshake_parts(edit)).err().unwrap()
        };
        assert_eq!(refused(|b| b.method("POST")), UpgradeError::NotGet(Method::POST));
        assert_eq!(
            refused(|b| b.version(Version::HTTP_10)),
            UpgradeError::NotHttp11(Version::HTTP_10)
        );
        assert_eq!(
            refused(|b| b.version(Version::HTTP_2)),
            UpgradeError::NotHttp11(Version::HTTP_2)
        );
        let mut no_connection = handshake_parts(|b| b);
        no_connection.headers.remove("connection");
        assert_eq!(
            WsHandshake::parse(&no_connection).err(),
            Some(UpgradeError::NoConnectionUpgrade)
        );
        let mut old_version = handshake_parts(|b| b);
        old_version
            .headers
            .insert("sec-websocket-version", HeaderValue::from_static("8"));
        assert_eq!(
            WsHandshake::parse(&old_version).err(),
            Some(UpgradeError::UnsupportedVersion(Some(HeaderValue::from_static("8"))))
        );
        let mut h2c = handshake_parts(|b| b);
        h2c.headers.insert("upgrade", HeaderValue::from_static("h2c"));
        assert_eq!(WsHandshake::parse(&h2c).err(), Some(UpgradeError::NotWebsocket));
        let mut no_key = handshake_parts(|b| b);
        no_key.headers.remove("sec-websocket-key");
        assert_eq!(WsHandshake::parse(&no_key).err(), Some(UpgradeError::MissingKey));
    }

    #[test]
    fn upgrade_names_websocket_among_its_protocols_in_any_case() {
        let asks = |values: &[&'static str]| {
            let mut headers = HeaderMap::new();
            for v in values {
                headers.append(UPGRADE, HeaderValue::from_static(v));
            }
            asks_for_websocket(&headers)
        };
        assert!(asks(&["websocket"]));
        assert!(asks(&["WebSocket"]));
        assert!(asks(&["h2c, websocket"]));
        assert!(asks(&["h2c", "websocket"]));
        assert!(!asks(&["h2c"]));
        assert!(!asks(&["websockets"]));
        assert!(!asks(&[]));
    }

    #[test]
    fn an_unsupported_version_is_426_naming_13_and_the_rest_400() {
        let resp = UpgradeError::UnsupportedVersion(None).response();
        assert_eq!(resp.status(), StatusCode::UPGRADE_REQUIRED);
        assert_eq!(resp.headers()["sec-websocket-version"], "13");
        let resp = UpgradeError::NoConnectionUpgrade.response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
