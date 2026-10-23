//! Decides whether a request is one a web page may make this server do: its host must be one of
//! the server's names, which stops DNS rebinding, and a request that acts (an unsafe method or a
//! WebSocket upgrade) must come from this site or from a site the CORS rule trusts.
//!
//! The checks read what a browser writes itself: `Host` from the URL, `Origin` and
//! `Sec-Fetch-Site`, which a page's script cannot set. A client that is no browser can send
//! anything, but it needs no visitor's browser to reach the server. The order is Go 1.25's
//! `net/http.CrossOriginProtection`, with the host check in front of it.

// `core::net`: the address is a value; `std::net` is the socket half the plan never names.
use core::net::Ipv6Addr;

use http::header::{HeaderMap, HeaderName, ORIGIN};
use http::{Method, Uri};

use crate::plan::cors::Cors;
use crate::plan::header::request_authority;
use crate::plan::reject::SiteMiss;

const SEC_FETCH_SITE: HeaderName = HeaderName::from_static("sec-fetch-site");

/// The longest DNS name (RFC 1035 §2.3.4, without the root's trailing dot).
const MAX_NAME_LEN: usize = 253;

/// The DNS names the deployment says this server is reached by. `localhost` and IP literals are
/// always served and never listed: no DNS answer a page's attacker controls stands behind them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostNames {
    /// The deployment checks the host itself, as a reverse proxy does.
    Any,
    Only(Vec<HostName>),
}

/// One DNS name of this server: RFC 1123 characters, lower case, without a port or a trailing
/// dot, at most 253 bytes. Only [`HostNames::parse`] makes one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostName(Box<[u8]>);

#[derive(Debug, thiserror::Error)]
pub enum HostNameError {
    #[error("allowed host {entry:?} is not a host name: expected e.g. \"api.example.com\" (no scheme, port or path)")]
    NotAName { entry: String },
    #[error("allowed hosts list \"*\" together with names {names:?}: \"*\" alone turns the check off")]
    AnyWithNames { names: Vec<String> },
}

impl HostName {
    fn parse(entry: &str) -> Option<HostName> {
        let name = entry.strip_suffix('.').unwrap_or(entry);
        let is_name = !name.is_empty()
            && name.len() <= MAX_NAME_LEN
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.');
        is_name.then(|| HostName(name.to_ascii_lowercase().into_bytes().into_boxed_slice()))
    }

    /// Returns whether `host`, a request's host without port or trailing dot, is this name in any case.
    fn matches(&self, host: &[u8]) -> bool {
        host.eq_ignore_ascii_case(&self.0)
    }
}

impl HostNames {
    /// Serves `localhost` and IP literals only.
    pub const LOCALHOST: HostNames = HostNames::Only(Vec::new());

    /// Parses the deployment's names; `["*"]` turns the check off.
    ///
    /// # Errors
    ///
    /// Returns [`HostNameError::NotAName`] for an entry that is not an RFC 1123 host name (a URL,
    /// a port, an empty one) and [`HostNameError::AnyWithNames`] for `*` beside names.
    pub fn parse(entries: &[String]) -> Result<HostNames, HostNameError> {
        if entries.iter().any(|e| e == "*") {
            let names: Vec<String> = entries.iter().filter(|e| *e != "*").cloned().collect();
            return match names.is_empty() {
                true => Ok(HostNames::Any),
                false => Err(HostNameError::AnyWithNames { names }),
            };
        }
        let names = entries
            .iter()
            .map(|entry| HostName::parse(entry).ok_or_else(|| HostNameError::NotAName { entry: entry.clone() }))
            .collect::<Result<_, _>>()?;
        Ok(HostNames::Only(names))
    }

    /// Returns whether a request sent to `authority` (`host[:port]`) is sent to this server. The
    /// port is not compared: the attacker controls the name, not the port, and a port mapping
    /// changes it.
    pub fn serves(&self, authority: &[u8]) -> bool {
        let HostNames::Only(names) = self else {
            return true;
        };
        let host = host_of(authority);
        let host = host.strip_suffix(b".").unwrap_or(host);
        // An IP literal is tried first: a name's first letter ends that try at once.
        is_ip_literal(host) || host.eq_ignore_ascii_case(b"localhost") || names.iter().any(|n| n.matches(host))
    }
}

/// Returns the host of `host[:port]`: a bracketed IPv6 address with its brackets, or what comes
/// before the last `:`. A host left with a colon in it (an IPv6 address without brackets) is no
/// name and no IPv4 address, so it is not served.
fn host_of(authority: &[u8]) -> &[u8] {
    if authority.starts_with(b"[") {
        return match authority.iter().position(|&b| b == b']') {
            Some(end) => &authority[..=end],
            None => authority,
        };
    }
    match authority.iter().rposition(|&b| b == b':') {
        Some(at) => &authority[..at],
        None => authority,
    }
}

/// Returns whether `host` is an IP literal: a bracketed IPv6 address, or a host whose last label
/// is a number, which a browser's URL parser (WHATWG URL, "ends in a number") reads as an IPv4
/// address or refuses, so it is never a DNS name a page could rebind.
fn is_ip_literal(host: &[u8]) -> bool {
    match host.strip_prefix(b"[").and_then(|h| h.strip_suffix(b"]")) {
        Some(v6) => std::str::from_utf8(v6).is_ok_and(|v6| v6.parse::<Ipv6Addr>().is_ok()),
        None => ends_in_a_number(host),
    }
}

/// Returns whether the last `.`-separated label of `host` is decimal digits. Read from the end, so
/// a name, which ends in a letter, is answered at its last byte.
fn ends_in_a_number(host: &[u8]) -> bool {
    let digits = host.iter().rev().take_while(|b| b.is_ascii_digit()).count();
    digits > 0 && (digits == host.len() || host[host.len() - digits - 1] == b'.')
}

/// Returns whether a request acts: a method other than `GET`, `HEAD` and `OPTIONS` (RFC 9110
/// §9.2.1), or a WebSocket upgrade, which is a `GET` that browsers send cross-site without CORS.
pub fn acts(method: &Method, upgrade: bool) -> bool {
    upgrade || !matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

/// Returns whether a request that acts comes from a page this server trusts: its own site, or a
/// site `cors` allows. Without `cors` no other site is trusted. A request without
/// `Sec-Fetch-Site` and `Origin` is no browser's and is admitted.
pub fn admits_cross_origin(headers: &HeaderMap, authority: Option<&[u8]>, cors: Option<&Cors>) -> bool {
    let origin = headers.get(ORIGIN);
    let trusted = || matches!((origin, cors), (Some(o), Some(c)) if c.trusts(o));
    match headers.get(SEC_FETCH_SITE).map(|v| v.as_bytes()) {
        Some(b"same-origin" | b"none") => true,
        Some(_) => trusted(),
        None => {
            let Some(origin) = origin else {
                return true;
            };
            let same = match (split_scheme(origin.as_bytes()), authority) {
                (Some(host), Some(authority)) => host == authority,
                _ => false,
            };
            same || trusted()
        }
    }
}

/// Returns what follows `scheme://` in an origin.
fn split_scheme(origin: &[u8]) -> Option<&[u8]> {
    let at = origin.windows(3).position(|w| w == b"://")?;
    Some(&origin[at + 3..])
}

/// Returns why the engine turns a request away before routing it, or `None` to serve it: first
/// its host, for every request, then, for a request that acts, the page it comes from.
#[inline]
pub fn site_miss<'a>(
    method: &Method,
    upgrade: bool,
    uri: &'a Uri,
    headers: &'a HeaderMap,
    names: &HostNames,
    cors: Option<&Cors>,
) -> Option<SiteMiss<'a>> {
    let acts = acts(method, upgrade);
    // The authority is read only when a check needs it: with the host check off, a request that
    // does not act needs nothing. A request with no authority is served: every browser sends `Host`.
    let authority = match names {
        HostNames::Any if !acts => return None,
        HostNames::Any => request_authority(uri, headers),
        HostNames::Only(_) => {
            let authority = request_authority(uri, headers);
            if let Some(host) = authority.filter(|a| !names.serves(a)) {
                return Some(SiteMiss::UnknownHost(host));
            }
            authority
        }
    };
    if acts && !admits_cross_origin(headers, authority, cors) {
        return Some(SiteMiss::CrossSite(headers.get(ORIGIN).map(|o| o.as_bytes())));
    }
    None
}

#[cfg(test)]
mod tests {
    use http::header::{HeaderValue, HOST};

    use super::*;

    fn names(entries: &[&str]) -> HostNames {
        HostNames::parse(&entries.iter().map(|e| e.to_string()).collect::<Vec<_>>()).unwrap()
    }

    fn served(names: &HostNames, authority: &str) -> bool {
        names.serves(authority.as_bytes())
    }

    #[test]
    fn an_ip_literal_is_always_served() {
        let n = HostNames::LOCALHOST;
        for host in [
            "127.0.0.1",
            "127.0.0.1:8000",
            "[::1]:8000",
            "10.0.0.5",
            "[fe80::1]",
            "127.0.0.1.",
            "2130706433",
            "0x7f.1",
            "999.1.1.1",
        ] {
            assert!(served(&n, host), "{host}");
        }
        for host in ["127.0.0.1.evil.example", "::1", "[::1", "[evil.example]"] {
            assert!(!served(&n, host), "{host}");
        }
    }

    #[test]
    fn localhost_is_served_by_default_in_any_case_and_with_a_trailing_dot() {
        let n = HostNames::LOCALHOST;
        for host in ["localhost", "LOCALHOST:8000", "localhost.", "localhost.:8000"] {
            assert!(served(&n, host), "{host}");
        }
        for host in [
            "localhost.evil.example",
            "evil-localhost",
            "localhost..",
            "evil.example",
        ] {
            assert!(!served(&n, host), "{host}");
        }
    }

    #[test]
    fn a_listed_name_is_served_without_regard_to_port() {
        let n = names(&["api.example.com"]);
        for host in [
            "api.example.com:443",
            "API.example.com",
            "api.example.com",
            "localhost:8000",
        ] {
            assert!(served(&n, host), "{host}");
        }
        for host in ["api.example.com.evil.example", "xapi.example.com", "example.com"] {
            assert!(!served(&n, host), "{host}");
        }
        assert!(served(&names(&["API.Example.com."]), "api.example.com"));
    }

    #[test]
    fn any_serves_every_name() {
        assert!(served(&HostNames::Any, "evil.example"));
        assert!(served(&HostNames::Any, ""));
    }

    #[test]
    fn no_authority_is_served_and_an_empty_one_is_not() {
        let none = (Uri::from_static("/"), HeaderMap::new());
        assert_eq!(
            site_miss(&Method::GET, false, &none.0, &none.1, &HostNames::LOCALHOST, None),
            None
        );
        assert!(!served(&HostNames::LOCALHOST, ""));
    }

    #[test]
    fn host_names_parse_refuses_what_is_no_name() {
        assert!(matches!(names(&["*"]), HostNames::Any));
        assert_eq!(names(&[]), HostNames::LOCALHOST);
        let long = "a".repeat(MAX_NAME_LEN);
        assert!(matches!(names(&[&long]), HostNames::Only(n) if n.len() == 1));
        for bad in [
            "https://api.example.com",
            "api.example.com:8000",
            "a/b",
            "",
            ".",
            "a b",
            &"a".repeat(MAX_NAME_LEN + 1),
        ] {
            assert!(
                matches!(
                    HostNames::parse(&[bad.to_string()]),
                    Err(HostNameError::NotAName { .. })
                ),
                "{bad:?}"
            );
        }
        assert!(matches!(
            HostNames::parse(&["*".to_string(), "a".to_string()]),
            Err(HostNameError::AnyWithNames { names }) if names == ["a"]
        ));
    }

    const APP: &str = "https://app.example.com";

    fn cors(origins: &[&str]) -> Cors {
        let origins: Vec<String> = origins.iter().map(|o| o.to_string()).collect();
        Cors::parse(&origins, "GET, POST", "content-type", None, false, None).unwrap()
    }

    fn request(host: &str, sent: &[(&'static str, &'static str)]) -> (Uri, HeaderMap) {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_str(host).unwrap());
        for (name, value) in sent {
            headers.insert(*name, HeaderValue::from_static(value));
        }
        (Uri::from_static("/run"), headers)
    }

    fn miss<'a>(
        method: &Method,
        upgrade: bool,
        req: &'a (Uri, HeaderMap),
        cors: Option<&Cors>,
    ) -> Option<SiteMiss<'a>> {
        site_miss(method, upgrade, &req.0, &req.1, &HostNames::LOCALHOST, cors)
    }

    const EVIL: [(&str, &str); 2] = [("origin", "https://evil.example"), ("sec-fetch-site", "cross-site")];

    #[test]
    fn safe_methods_never_look_at_origin() {
        let req = request("127.0.0.1:8000", &EVIL);
        for method in [Method::GET, Method::HEAD, Method::OPTIONS] {
            assert_eq!(miss(&method, false, &req, None), None, "{method}");
        }
        let custom = Method::from_bytes(b"PURGE").unwrap();
        for method in [
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::PATCH,
            Method::TRACE,
            custom,
        ] {
            assert_eq!(
                miss(&method, false, &req, None),
                Some(SiteMiss::CrossSite(Some(b"https://evil.example"))),
                "{method}"
            );
        }
    }

    #[test]
    fn a_websocket_upgrade_acts() {
        let same = request("127.0.0.1:8000", &[("origin", "http://127.0.0.1:8000")]);
        assert_eq!(miss(&Method::GET, true, &same, None), None);
        let evil = request("127.0.0.1:8000", &[("origin", "https://evil.example")]);
        assert_eq!(
            miss(&Method::GET, true, &evil, None),
            Some(SiteMiss::CrossSite(Some(b"https://evil.example")))
        );
    }

    #[test]
    fn an_unknown_host_is_refused_before_anything_else_whatever_the_method() {
        let req = request("evil.example:8000", &[("sec-fetch-site", "same-origin")]);
        for method in [Method::GET, Method::POST] {
            assert_eq!(
                miss(&method, false, &req, None),
                Some(SiteMiss::UnknownHost(b"evil.example:8000"))
            );
        }
    }

    #[test]
    fn the_target_s_authority_is_preferred_over_host() {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("127.0.0.1"));
        let uri = Uri::from_static("http://evil.example/run");
        assert_eq!(
            site_miss(&Method::GET, false, &uri, &headers, &HostNames::LOCALHOST, None),
            Some(SiteMiss::UnknownHost(b"evil.example"))
        );
    }

    #[test]
    fn fetch_site_same_origin_and_none_are_admitted() {
        for site in ["same-origin", "none"] {
            let req = request(
                "localhost",
                &[("sec-fetch-site", site), ("origin", "https://evil.example")],
            );
            assert_eq!(miss(&Method::POST, false, &req, None), None, "{site}");
        }
        for site in ["same-site", "cross-site", "", "SAME-ORIGIN", "garbage"] {
            let mut req = request("localhost", &[("origin", "https://evil.example")]);
            req.1.insert(SEC_FETCH_SITE, HeaderValue::from_str(site).unwrap());
            assert!(miss(&Method::POST, false, &req, None).is_some(), "{site:?}");
        }
    }

    #[test]
    fn fetch_site_cross_site_is_admitted_only_for_a_trusted_origin() {
        let c = cors(&[APP]);
        let req = request("localhost", &[("sec-fetch-site", "cross-site"), ("origin", APP)]);
        assert_eq!(miss(&Method::POST, false, &req, Some(&c)), None);
        for origin in ["https://app.example.com.evil.example", "http://app.example.com", "null"] {
            let mut req = request("localhost", &[("sec-fetch-site", "cross-site")]);
            req.1.insert(ORIGIN, HeaderValue::from_static(origin));
            assert_eq!(
                miss(&Method::POST, false, &req, Some(&c)),
                Some(SiteMiss::CrossSite(Some(origin.as_bytes()))),
                "{origin}"
            );
        }
        let no_origin = request("localhost", &[("sec-fetch-site", "cross-site")]);
        assert_eq!(
            miss(&Method::POST, false, &no_origin, Some(&c)),
            Some(SiteMiss::CrossSite(None))
        );
    }

    #[test]
    fn without_fetch_site_no_origin_is_admitted() {
        assert_eq!(miss(&Method::POST, false, &request("localhost", &[]), None), None);
        let no_host = (Uri::from_static("/run"), HeaderMap::new());
        assert_eq!(miss(&Method::POST, false, &no_host, None), None);
    }

    #[test]
    fn without_fetch_site_the_same_origin_is_admitted() {
        let same = request("localhost:8000", &[("origin", "http://localhost:8000")]);
        assert_eq!(miss(&Method::POST, false, &same, None), None);
        let other_port = request("localhost:8001", &[("origin", "http://localhost:8000")]);
        assert!(miss(&Method::POST, false, &other_port, None).is_some());
        let no_port = request("localhost:8000", &[("origin", "http://localhost")]);
        assert!(miss(&Method::POST, false, &no_port, None).is_some());
        let v6 = request("[::1]:8000", &[("origin", "http://[::1]:8000")]);
        assert_eq!(miss(&Method::POST, false, &v6, None), None);
        let default_port = request("localhost:80", &[("origin", "http://localhost")]);
        assert!(miss(&Method::POST, false, &default_port, None).is_some());
        let no_scheme = request("localhost:8000", &[("origin", "localhost:8000")]);
        assert!(miss(&Method::POST, false, &no_scheme, None).is_some());
    }

    #[test]
    fn without_fetch_site_a_trusted_origin_is_admitted_and_null_never() {
        let c = cors(&[APP]);
        let trusted = request("localhost", &[("origin", APP)]);
        assert_eq!(miss(&Method::POST, false, &trusted, Some(&c)), None);
        let null = request("localhost", &[("origin", "null")]);
        assert_eq!(
            miss(&Method::POST, false, &null, Some(&c)),
            Some(SiteMiss::CrossSite(Some(b"null")))
        );
    }

    #[test]
    fn without_cors_no_other_site_is_trusted() {
        let req = request("localhost", &[("origin", APP), ("sec-fetch-site", "cross-site")]);
        assert!(miss(&Method::POST, false, &req, None).is_some());
    }

    #[test]
    fn any_origin_trusts_every_site() {
        let c = cors(&["*"]);
        let req = request("localhost", &EVIL);
        assert_eq!(miss(&Method::POST, false, &req, Some(&c)), None);
    }

    #[test]
    fn a_host_ends_in_a_number_when_its_last_label_is_digits() {
        for host in ["0.0.0.0", "1.22.199.250", "2130706433", "0x7f.1", "a.b.3", "999.1.1.1"] {
            assert!(ends_in_a_number(host.as_bytes()), "{host}");
        }
        for host in [
            "",
            ".",
            "1.2.3.",
            "1.2.3.4a",
            "localhost",
            "a.b.c.d",
            "1.2.3.-4",
            "example.com1x",
        ] {
            assert!(!ends_in_a_number(host.as_bytes()), "{host}");
        }
    }

    proptest::proptest! {
        #[test]
        fn every_ipv4_address_ends_in_a_number(a: u8, b: u8, c: u8, d: u8) {
            let host = format!("{a}.{b}.{c}.{d}");
            proptest::prop_assert!(ends_in_a_number(host.as_bytes()), "{}", host);
        }

        #[test]
        fn a_host_whose_last_label_has_a_letter_does_not_end_in_a_number(
            front in "[0-9a-z.-]{0,16}", last in "[0-9a-z-]{0,6}[a-z][0-9a-z-]{0,6}",
        ) {
            let host = if front.is_empty() { last } else { format!("{front}.{last}") };
            proptest::prop_assert!(!ends_in_a_number(host.as_bytes()), "{}", host);
        }

        #[test]
        fn every_byte_string_gets_an_answer(
            host in proptest::collection::vec(proptest::num::u8::ANY, 0..64),
            origin in proptest::collection::vec(proptest::num::u8::ANY, 0..64),
            site in proptest::collection::vec(proptest::num::u8::ANY, 0..16),
            names_any in proptest::bool::ANY,
        ) {
            let mut headers = HeaderMap::new();
            for (name, value) in [(HOST, &host), (ORIGIN, &origin), (SEC_FETCH_SITE, &site)] {
                if let Ok(value) = HeaderValue::from_bytes(value) {
                    headers.insert(name, value);
                }
            }
            let uri = Uri::from_static("/");
            let names = if names_any { HostNames::Any } else { HostNames::LOCALHOST };
            let c = cors(&[APP]);
            for upgrade in [false, true] {
                if let Some(miss) = site_miss(&Method::POST, upgrade, &uri, &headers, &names, Some(&c)) {
                    let resp = crate::plan::response::site_miss_response(miss);
                    proptest::prop_assert!(resp.body().len() < 512);
                    proptest::prop_assert!(serde_json::from_slice::<serde_json::Value>(resp.body()).is_ok());
                }
            }
        }
    }
}
