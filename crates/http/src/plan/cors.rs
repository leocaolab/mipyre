//! Parses the app's CORS rule once: the headers a response carries, and the answer to a
//! preflight.

use bytes::Bytes;
use http::header::{
    HeaderMap, HeaderName, HeaderValue, InvalidHeaderValue, ACCESS_CONTROL_ALLOW_CREDENTIALS,
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_MAX_AGE, ACCESS_CONTROL_REQUEST_METHOD, ORIGIN, VARY,
};
// `core::net`: the address is a value; `std::net` is the socket half the plan never names.
use core::net::Ipv6Addr;

use http::{Method, Response, StatusCode};

use crate::plan::header::{own_header_name, own_header_value};

/// `headers` go on every response that allows its request's origin, not only the preflight's,
/// because W3C CORS wants `Access-Control-Allow-Credentials` and `Access-Control-Expose-Headers`
/// on the actual response.
#[derive(Clone, Debug)]
pub struct Cors {
    /// `Access-Control-Allow-Origin` holds one origin or `*` (W3C Fetch): a browser takes a list
    /// there for no match. So one (`*`, or the one origin named) goes on every response whatever
    /// the request's origin, and of several a response names its request's origin when it is one
    /// of them, with `Vary: Origin`, since what it says then depends on that header.
    origins: Vec<HeaderValue>,
    headers: Vec<(HeaderName, HeaderValue)>,
    /// How long a browser may keep a preflight's answer; `None` sends no `Access-Control-Max-Age`.
    max_age: Option<HeaderValue>,
}

/// One of the origins a [`Cors`] allows, by its place among them: the origin a response names.
/// A place rather than the value, so that what a request carries to its response is `Copy`, with
/// no reference count to move and drop on every path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CorsOrigin(usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorsSetting {
    Methods,
    Headers,
    ExposeHeaders,
}

impl std::fmt::Display for CorsSetting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            CorsSetting::Methods => "methods",
            CorsSetting::Headers => "headers",
            CorsSetting::ExposeHeaders => "expose_headers",
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CorsError {
    #[error("invalid CORS {setting} {value:?}: {source}")]
    InvalidValue {
        setting: CorsSetting,
        value: String,
        source: InvalidHeaderValue,
    },
    /// The Fetch standard has browsers refuse a credentialed request whose answer allows the
    /// origin `*`, so the app would fail in every browser while the server answers 200.
    #[error(
        "CORS origins {origins:?} allow any origin (\"*\") with allow_credentials, which every browser \
         refuses (W3C Fetch): name the origins, or turn allow_credentials off"
    )]
    AnyOriginWithCredentials { origins: Vec<String> },
    #[error("CORS allow_origins names no origin: give \"*\" or the origins")]
    EmptyOrigin,
    /// A browser's `Origin` is `scheme://host[:port]` and nothing else, so an entry of another
    /// shape would never match one.
    #[error(
        "CORS origin {origin:?} is not an origin: expected \"http[s]://host[:port]\" with no path, \
         e.g. \"https://app.example.com\""
    )]
    NotAnOrigin { origin: String },
}

impl Cors {
    /// Parses the app's CORS settings into headers. Each of `origins` is an origin, or `*`, which
    /// allows any; an origin is kept as a browser writes its `Origin` (lower-case, without the
    /// scheme's default port), which is the form both the answer and [`Cors::trusts`] compare.
    ///
    /// # Errors
    ///
    /// Returns [`CorsError::InvalidValue`] when a setting is not a valid header value,
    /// [`CorsError::EmptyOrigin`] when `origins` is empty, [`CorsError::NotAnOrigin`] for an
    /// entry that is not `http[s]://host[:port]`, and [`CorsError::AnyOriginWithCredentials`]
    /// when `origins` lists `*` with credentials allowed.
    pub fn parse(
        origins: &[String],
        methods: &str,
        headers: &str,
        expose_headers: Option<&str>,
        allow_credentials: bool,
        max_age: Option<u32>,
    ) -> Result<Self, CorsError> {
        if origins.is_empty() {
            return Err(CorsError::EmptyOrigin);
        }
        let any = origins.iter().any(|o| o == "*");
        if allow_credentials && any {
            return Err(CorsError::AnyOriginWithCredentials {
                origins: origins.to_vec(),
            });
        }
        let value = |setting: CorsSetting, value: &str| {
            HeaderValue::from_str(value).map_err(|source| CorsError::InvalidValue {
                setting,
                value: value.to_string(),
                source,
            })
        };
        let origins = match any {
            true => vec![HeaderValue::from_static("*")],
            false => origins
                .iter()
                .map(|o| {
                    browser_origin(o)
                        .and_then(|o| HeaderValue::try_from(o).ok())
                        .ok_or_else(|| CorsError::NotAnOrigin { origin: o.clone() })
                })
                .collect::<Result<_, _>>()?,
        };
        let mut set = vec![
            (ACCESS_CONTROL_ALLOW_METHODS, value(CorsSetting::Methods, methods)?),
            (ACCESS_CONTROL_ALLOW_HEADERS, value(CorsSetting::Headers, headers)?),
        ];
        if allow_credentials {
            set.push((ACCESS_CONTROL_ALLOW_CREDENTIALS, HeaderValue::from_static("true")));
        }
        if let Some(expose) = expose_headers {
            set.push((
                ACCESS_CONTROL_EXPOSE_HEADERS,
                value(CorsSetting::ExposeHeaders, expose)?,
            ));
        }
        Ok(Cors {
            origins,
            headers: set,
            max_age: max_age.map(HeaderValue::from),
        })
    }

    /// Returns whether a page from `origin` may make this server act: any origin under `*`,
    /// otherwise one of the origins, byte for byte as [`Cors::origin_of`] compares.
    pub fn trusts(&self, origin: &HeaderValue) -> bool {
        match self.origins.as_slice() {
            [any] if any == "*" => true,
            origins => origins.contains(origin),
        }
    }

    /// Copies the headers into allocations of their own, so each serving thread applies its own.
    pub fn own_copy(&self) -> Cors {
        let headers = self.headers.iter();
        Cors {
            origins: self.origins.iter().map(own_header_value).collect(),
            headers: headers
                .map(|(n, v)| (own_header_name(n), own_header_value(v)))
                .collect(),
            max_age: self.max_age.as_ref().map(own_header_value),
        }
    }

    /// The origin a response to a request with `request` headers names: the one this rule
    /// allows, or, of several, the request's own `Origin` when it is one of them byte for byte.
    /// `None` when the request names none of them, or no origin.
    #[inline]
    pub fn origin_of(&self, request: &HeaderMap) -> Option<CorsOrigin> {
        if let [_] = self.origins.as_slice() {
            return Some(CorsOrigin(0));
        }
        let asked = request.get(ORIGIN)?;
        self.origins.iter().position(|o| o == asked).map(CorsOrigin)
    }

    /// Puts the CORS headers on a response that names `origin` (from [`Cors::origin_of`] of this
    /// rule or of its [`Cors::own_copy`]); with `None` it gets none of them. Sets rather than
    /// appends each, so a handler's own value is not duplicated, and with several origins appends
    /// `Vary: Origin`, so a cache keeps one answer per origin.
    #[inline]
    pub fn apply(&self, map: &mut HeaderMap, origin: Option<CorsOrigin>) {
        if self.origins.len() > 1 {
            map.append(VARY, HeaderValue::from_static("Origin"));
        }
        let Some(value) = origin.and_then(|CorsOrigin(at)| self.origins.get(at)) else {
            return;
        };
        map.insert(ACCESS_CONTROL_ALLOW_ORIGIN, value.clone());
        for (name, value) in &self.headers {
            map.insert(name.clone(), value.clone());
        }
    }

    /// Answers a preflight: an `OPTIONS` that carries `Access-Control-Request-Method` (W3C
    /// Fetch). It answers `204` with `Access-Control-Max-Age`; [`Cors::apply`] adds the rest, as
    /// to every response. Any other request gets `None`.
    pub fn preflight(&self, method: &Method, headers: &HeaderMap) -> Option<Response<Bytes>> {
        if *method != Method::OPTIONS || headers.get(ACCESS_CONTROL_REQUEST_METHOD).is_none() {
            return None;
        }
        let mut resp = Response::new(Bytes::new());
        *resp.status_mut() = StatusCode::NO_CONTENT;
        if let Some(max_age) = &self.max_age {
            resp.headers_mut().insert(ACCESS_CONTROL_MAX_AGE, max_age.clone());
        }
        Some(resp)
    }
}

/// Returns `entry` as a browser serializes an origin (WHATWG URL): `scheme://host[:port]` with
/// the scheme and host in lower case, an IPv6 host in its canonical form and the scheme's default
/// port left out; `None` when `entry` is anything else (another scheme, a path, user info, `null`,
/// a host that is not ASCII).
fn browser_origin(entry: &str) -> Option<String> {
    let (scheme, rest) = entry.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    let default_port = match scheme.as_str() {
        "http" => 80,
        "https" => 443,
        _ => return None,
    };
    let (host, after) = match rest.strip_prefix('[') {
        Some(v6) => {
            let (addr, after) = v6.split_once(']')?;
            let addr: Ipv6Addr = addr.parse().ok()?;
            (format!("[{addr}]"), after)
        }
        None => {
            let (host, after) = rest.split_at(rest.find(':').unwrap_or(rest.len()));
            let is_name = !host.is_empty()
                && host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.');
            if !is_name {
                return None;
            }
            (host.to_ascii_lowercase(), after)
        }
    };
    let port = match after {
        "" => None,
        _ => {
            let digits = after
                .strip_prefix(':')
                .filter(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()))?;
            Some(digits.parse::<u16>().ok()?)
        }
    };
    Some(match port {
        Some(port) if port != default_port => format!("{scheme}://{host}:{port}"),
        _ => format!("{scheme}://{host}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses `origins` written as one string, separated by `", "`.
    fn cors(origins: &str, allow_credentials: bool, max_age: Option<u32>) -> Result<Cors, CorsError> {
        let origins: Vec<String> = origins.split(", ").map(str::to_string).collect();
        Cors::parse(&origins, "GET, POST", "content-type", None, allow_credentials, max_age)
    }

    fn preflight_headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(ACCESS_CONTROL_REQUEST_METHOD, HeaderValue::from_static("POST"));
        h
    }

    #[test]
    fn any_origin_with_credentials_is_refused_wherever_it_is_listed() {
        for origin in ["*", "https://a.example, *"] {
            assert!(
                matches!(
                    cors(origin, true, None),
                    Err(CorsError::AnyOriginWithCredentials { .. })
                ),
                "{origin:?}"
            );
        }
        assert!(cors("*", false, None).is_ok());
        assert!(cors("https://a.example", true, None).is_ok());
    }

    #[test]
    fn a_value_that_is_no_header_value_names_its_setting() {
        assert!(matches!(
            Cors::parse(&["*".into()], "GET\n", "content-type", None, false, None),
            Err(CorsError::InvalidValue {
                setting: CorsSetting::Methods,
                ..
            })
        ));
    }

    #[test]
    fn an_options_with_the_request_method_is_answered_204_with_max_age() {
        let resp = cors("*", false, Some(600))
            .unwrap()
            .preflight(&Method::OPTIONS, &preflight_headers())
            .expect("a preflight");
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(resp.headers()[ACCESS_CONTROL_MAX_AGE], "600");
        assert!(resp.body().is_empty());
    }

    #[test]
    fn without_max_age_the_preflight_sends_none() {
        let resp = cors("*", false, None)
            .unwrap()
            .preflight(&Method::OPTIONS, &preflight_headers())
            .expect("a preflight");
        assert!(!resp.headers().contains_key(ACCESS_CONTROL_MAX_AGE));
    }

    #[test]
    fn an_options_without_the_request_method_or_another_method_is_no_preflight() {
        let c = cors("*", false, Some(600)).unwrap();
        assert!(c.preflight(&Method::OPTIONS, &HeaderMap::new()).is_none());
        assert!(c.preflight(&Method::POST, &preflight_headers()).is_none());
    }

    fn from(origin: &'static str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(ORIGIN, HeaderValue::from_static(origin));
        h
    }

    /// The response headers `c` puts on the answer to a request with `request` headers.
    fn answer(c: &Cors, request: &HeaderMap) -> HeaderMap {
        let mut map = HeaderMap::new();
        c.apply(&mut map, c.origin_of(request));
        map
    }

    const AB: &str = "https://a.example, https://b.example";

    #[test]
    fn of_several_origins_a_listed_one_is_named_back_alone_and_varies_by_origin() {
        let c = cors(AB, true, None).unwrap();
        for origin in ["https://a.example", "https://b.example"] {
            let h = answer(&c, &from(origin));
            assert_eq!(h[ACCESS_CONTROL_ALLOW_ORIGIN], origin);
            assert_eq!(h[ACCESS_CONTROL_ALLOW_CREDENTIALS], "true");
            assert_eq!(h[VARY], "Origin");
        }
    }

    #[test]
    fn of_several_origins_any_other_or_none_gets_no_cors_header_but_still_varies_by_origin() {
        let c = cors(AB, true, None).unwrap();
        // Byte for byte, as a browser compares: a prefix, another scheme or a case differs.
        for request in [
            from("https://a.example.evil"),
            from("http://a.example"),
            from("https://A.example"),
            from("null"),
            HeaderMap::new(),
        ] {
            let h = answer(&c, &request);
            assert_eq!(h.len(), 1, "{request:?}: {h:?}");
            assert_eq!(h[VARY], "Origin");
        }
    }

    #[test]
    fn one_origin_or_any_is_on_every_response_whatever_the_request_s_origin() {
        for (origin, sent) in [
            ("https://a.example", "https://a.example"),
            ("*", "*"),
            ("https://a.example, *", "*"),
        ] {
            let c = cors(origin, false, None).unwrap();
            for request in [from("https://other.example"), HeaderMap::new()] {
                let h = answer(&c, &request);
                assert_eq!(h[ACCESS_CONTROL_ALLOW_ORIGIN], sent, "{origin:?}");
                assert_eq!(h[ACCESS_CONTROL_ALLOW_METHODS], "GET, POST");
                assert!(!h.contains_key(VARY), "{origin:?}");
            }
        }
    }

    #[test]
    fn the_vary_of_a_handler_is_kept() {
        let c = cors(AB, false, None).unwrap();
        let mut map = HeaderMap::new();
        map.insert(VARY, HeaderValue::from_static("Accept-Encoding"));
        c.apply(&mut map, c.origin_of(&from("https://a.example")));
        let vary: Vec<_> = map.get_all(VARY).iter().collect();
        assert_eq!(vary, ["Accept-Encoding", "Origin"]);
    }

    #[test]
    fn no_origin_at_all_is_refused() {
        assert!(matches!(
            Cors::parse(&[], "GET", "*", None, false, None),
            Err(CorsError::EmptyOrigin)
        ));
    }

    #[test]
    fn an_origin_entry_is_written_as_a_browser_sends_it() {
        for (entry, sent) in [
            ("https://A.example:443", "https://a.example"),
            ("HTTP://a.example:80", "http://a.example"),
            ("http://a.example:8080", "http://a.example:8080"),
            ("https://a.example:80", "https://a.example:80"),
            ("http://[0:0::1]:3000", "http://[::1]:3000"),
            ("http://127.0.0.1:8000", "http://127.0.0.1:8000"),
        ] {
            let c = cors(entry, false, None).unwrap();
            assert_eq!(
                answer(&c, &HeaderMap::new())[ACCESS_CONTROL_ALLOW_ORIGIN],
                sent,
                "{entry}"
            );
        }
    }

    #[test]
    fn an_entry_that_is_no_origin_is_refused() {
        for entry in [
            "",
            " ",
            " https://a.example",
            "a.example",
            "https://a.example/",
            "https://a.example/p",
            "null",
            "ftp://a",
            "https://u@a",
            "https://a?q",
            "https://a#f",
            "https://",
            "https://a:",
            "https://a:99999",
            "https://a:8x",
            "https://a:1:2",
            "http://[::1",
            "http://[nope]",
            "http://[::1]x",
            "https://bücher.example",
            "https://a.example\n",
            "javascript:alert(1)",
        ] {
            assert!(
                matches!(cors(entry, false, None), Err(CorsError::NotAnOrigin { origin }) if origin == entry),
                "{entry:?}"
            );
        }
        assert!(matches!(
            cors("https://a.example, https://b\x7f", false, None),
            Err(CorsError::NotAnOrigin { .. })
        ));
    }

    #[test]
    fn trusts_is_byte_for_byte() {
        let c = cors(AB, false, None).unwrap();
        assert!(c.trusts(&HeaderValue::from_static("https://a.example")));
        assert!(c.trusts(&HeaderValue::from_static("https://b.example")));
        for origin in [
            "https://A.example",
            "https://a.example:443",
            "http://a.example",
            "https://a.example.evil",
            "null",
        ] {
            assert!(!c.trusts(&HeaderValue::from_static(origin)), "{origin}");
        }
        let any = cors("*", false, None).unwrap();
        assert!(any.trusts(&HeaderValue::from_static("https://evil.example")));
    }

    #[test]
    fn the_own_copy_keeps_several_origins() {
        let c = cors(AB, false, None).unwrap().own_copy();
        let h = answer(&c, &from("https://b.example"));
        assert_eq!(h[ACCESS_CONTROL_ALLOW_ORIGIN], "https://b.example");
    }

    #[test]
    fn the_own_copy_keeps_max_age() {
        let c = cors("*", false, Some(5)).unwrap().own_copy();
        let resp = c.preflight(&Method::OPTIONS, &preflight_headers()).unwrap();
        assert_eq!(resp.headers()[ACCESS_CONTROL_MAX_AGE], "5");
    }
}
