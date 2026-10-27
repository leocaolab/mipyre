//! Defines the Python `Cookie`, one `Set-Cookie` line that is checked against RFC 6265 when it
//! is made, and `SameSite`, the word its `SameSite` attribute carries.

use std::time::{Duration, UNIX_EPOCH};

use hyper::header::HeaderValue;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDateTime, PyInt};

/// Says which sites may send the cookie along; Python sees `SameSite.STRICT`, `.LAX` and `.NONE`.
#[pyclass(module = "mipyre.engine", name = "SameSite", eq, frozen, hash, from_py_object)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SameSite {
    #[pyo3(name = "STRICT")]
    Strict,
    #[pyo3(name = "LAX")]
    Lax,
    #[pyo3(name = "NONE")]
    None,
}

impl SameSite {
    fn attribute(self) -> &'static str {
        match self {
            SameSite::Strict => "SameSite=Strict",
            SameSite::Lax => "SameSite=Lax",
            SameSite::None => "SameSite=None",
        }
    }
}

/// HTTP-dates are four-digit years: this is 10000-01-01T00:00:00Z.
const FIRST_SECOND_PAST_9999: u64 = 253_402_300_800;

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
enum CookieError {
    #[error("cookie name must not be empty (RFC 6265 §4.1.1)")]
    EmptyName,
    #[error("cookie name {name:?} holds {found:?}, which an RFC 7230 token cannot")]
    NameNotToken { name: String, found: char },
    #[error("cookie {name:?}: value {value:?} holds {found:?}, which is not a cookie-octet (RFC 6265 §4.1.1)")]
    ValueNotCookieOctets { name: String, value: String, found: char },
    #[error("cookie {name:?}: path {path:?} holds {found:?}; a path holds no control character or ';'")]
    ForbiddenCharInPath { name: String, path: String, found: char },
    #[error("cookie {name:?}: domain {domain:?} holds {found:?}; a domain holds no control character or ';'")]
    ForbiddenCharInDomain { name: String, domain: String, found: char },
    #[error("cookie {name:?}: expires is {seconds} s from 1970, outside the years 1970-9999 an HTTP-date can hold")]
    ExpiresOutOfRange { name: String, seconds: f64 },
    #[error(
        "cookie {name:?}: SameSite=None requires secure=True; browsers drop a SameSite=None cookie that is not Secure"
    )]
    SameSiteNoneWithoutSecure { name: String },
}

impl From<CookieError> for PyErr {
    fn from(e: CookieError) -> PyErr {
        PyValueError::new_err(e.to_string())
    }
}

/// The attributes of one `Set-Cookie` line (RFC 6265's cookie-av), as given.
#[derive(Clone, Copy, Debug)]
struct CookieAttributes<'a> {
    max_age: Option<i64>,
    /// Seconds since 1970, from an aware `datetime`.
    expires: Option<f64>,
    path: Option<&'a str>,
    domain: Option<&'a str>,
    secure: bool,
    http_only: bool,
    same_site: SameSite,
}

/// `tchar` of RFC 7230 §3.2.6.
fn is_token_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || "!#$%&'*+-.^_`|~".contains(c)
}

/// `cookie-octet` of RFC 6265 §4.1.1: visible ASCII but `"`, `,`, `;` and `\`.
fn is_cookie_octet(c: char) -> bool {
    matches!(c, '\x21' | '\x23'..='\x2B' | '\x2D'..='\x3A' | '\x3C'..='\x5B' | '\x5D'..='\x7E')
}

/// Any CHAR but CTLs and `;` (RFC 6265 §4.1.1, `path-value` and the domain's text).
fn is_attribute_char(c: char) -> bool {
    matches!(c, '\x20'..='\x7E') && c != ';'
}

fn check_name(name: &str) -> Result<(), CookieError> {
    if name.is_empty() {
        return Err(CookieError::EmptyName);
    }
    match name.chars().find(|c| !is_token_char(*c)) {
        Some(found) => Err(CookieError::NameNotToken {
            name: name.to_owned(),
            found,
        }),
        None => Ok(()),
    }
}

/// Accepts `*cookie-octet` or the same wrapped in one pair of `"`.
fn check_value(name: &str, value: &str) -> Result<(), CookieError> {
    let inner = value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(value);
    match inner.chars().find(|c| !is_cookie_octet(*c)) {
        Some(found) => Err(CookieError::ValueNotCookieOctets {
            name: name.to_owned(),
            value: value.to_owned(),
            found,
        }),
        None => Ok(()),
    }
}

fn forbidden_attribute_char(value: &str) -> Option<char> {
    value.chars().find(|c| !is_attribute_char(*c))
}

fn http_date(name: &str, seconds: f64) -> Result<String, CookieError> {
    let out_of_range = || CookieError::ExpiresOutOfRange {
        name: name.to_owned(),
        seconds,
    };
    let whole = seconds.floor();
    if !(0.0..FIRST_SECOND_PAST_9999 as f64).contains(&whole) {
        return Err(out_of_range());
    }
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "whole is a whole number in 0..FIRST_SECOND_PAST_9999, checked just above"
    )]
    let whole = whole as u64;
    Ok(httpdate::fmt_http_date(UNIX_EPOCH + Duration::from_secs(whole)))
}

/// Returns the text of the one `Set-Cookie` line, or the first rule it breaks.
fn set_cookie_line(name: &str, value: &str, a: CookieAttributes<'_>) -> Result<String, CookieError> {
    check_name(name)?;
    check_value(name, value)?;
    if a.same_site == SameSite::None && !a.secure {
        return Err(CookieError::SameSiteNoneWithoutSecure { name: name.to_owned() });
    }
    let mut line = format!("{name}={value}");
    if let Some(max_age) = a.max_age {
        line.push_str(&format!("; Max-Age={max_age}"));
    }
    if let Some(expires) = a.expires {
        line.push_str("; Expires=");
        line.push_str(&http_date(name, expires)?);
    }
    if let Some(path) = a.path {
        if let Some(found) = forbidden_attribute_char(path) {
            return Err(CookieError::ForbiddenCharInPath {
                name: name.to_owned(),
                path: path.to_owned(),
                found,
            });
        }
        line.push_str("; Path=");
        line.push_str(path);
    }
    if let Some(domain) = a.domain {
        if let Some(found) = forbidden_attribute_char(domain) {
            return Err(CookieError::ForbiddenCharInDomain {
                name: name.to_owned(),
                domain: domain.to_owned(),
                found,
            });
        }
        line.push_str("; Domain=");
        line.push_str(domain);
    }
    if a.secure {
        line.push_str("; Secure");
    }
    if a.http_only {
        line.push_str("; HttpOnly");
    }
    line.push_str("; ");
    line.push_str(a.same_site.attribute());
    Ok(line)
}

/// Reads `max_age=`: an `int`, never a `bool` (an `int` subclass that is never a Max-Age).
fn max_age_from_py(max_age: Option<&Bound<'_, PyAny>>) -> PyResult<Option<i64>> {
    let Some(max_age) = max_age else {
        return Ok(None);
    };
    if max_age.is_instance_of::<PyBool>() || !max_age.is_instance_of::<PyInt>() {
        return Err(PyValueError::new_err(format!(
            "cookie max_age must be an int (got {})",
            max_age.get_type().name()?
        )));
    }
    max_age.extract().map(Some)
}

/// Reads `expires=` as seconds since 1970; a naive `datetime` would leave the GMT time sent to
/// the browser to the server's local zone.
fn expires_from_py(expires: Option<&Bound<'_, PyAny>>) -> PyResult<Option<f64>> {
    let Some(expires) = expires else {
        return Ok(None);
    };
    if !expires.is_instance_of::<PyDateTime>() {
        return Err(PyTypeError::new_err(format!(
            "cookie expires must be a datetime, got {}",
            expires.get_type().name()?
        )));
    }
    if expires.call_method0("utcoffset")?.is_none() {
        return Err(PyValueError::new_err(format!(
            "cookie expires={} has no timezone; pass an aware datetime so the GMT time sent to the browser is unambiguous",
            expires.repr()?
        )));
    }
    expires.call_method0("timestamp")?.extract().map(Some)
}

/// `None` is the default, `SameSite.LAX`.
fn same_site_from_py(same_site: Option<&Bound<'_, PyAny>>) -> PyResult<SameSite> {
    match same_site {
        None => Ok(SameSite::Lax),
        Some(given) => given.extract().map_err(|_| {
            let shown = given
                .repr()
                .map_or_else(|e| format!("<repr failed: {e}>"), |r| r.to_string());
            PyValueError::new_err(format!(
                "cookie samesite must be SameSite.STRICT, .LAX or .NONE, got {shown}"
            ))
        }),
    }
}

/// One `Set-Cookie` line; `Response(headers={"set-cookie": [...]})` sends each on its own line.
#[pyclass(frozen, eq, hash, name = "Cookie", module = "mipyre.engine")]
#[derive(PartialEq, Eq, Hash)]
pub struct Cookie {
    pub(crate) line: HeaderValue,
}

impl Cookie {
    fn from_parts(name: &str, value: &str, attributes: CookieAttributes<'_>) -> PyResult<Self> {
        let line = set_cookie_line(name, value, attributes)?;
        let line = HeaderValue::try_from(line)
            .map_err(|e| PyValueError::new_err(format!("cookie {name:?}: invalid Set-Cookie line: {e}")))?;
        Ok(Cookie { line })
    }
}

#[pymethods]
impl Cookie {
    /// Checks every part when it is made: the name is an RFC 7230 token, the value cookie-octets
    /// (optionally in one pair of `"`), `path` / `domain` free of control characters and `;`,
    /// `max_age` an `int`, `expires` an aware `datetime`; `SameSite.NONE` needs `secure=True`.
    /// Raises `ValueError` naming the cookie and the rule broken.
    #[new]
    #[pyo3(signature = (
        name, value, *, max_age=None, expires=None, path=Some("/"), domain=None,
        secure=false, httponly=false, samesite=None,
    ))]
    #[expect(
        clippy::too_many_arguments,
        reason = "one per Set-Cookie attribute, as Python passes them"
    )]
    fn new(
        name: &str,
        value: &str,
        max_age: Option<&Bound<'_, PyAny>>,
        expires: Option<&Bound<'_, PyAny>>,
        path: Option<&str>,
        domain: Option<&str>,
        secure: bool,
        httponly: bool,
        samesite: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let attributes = CookieAttributes {
            max_age: max_age_from_py(max_age)?,
            expires: expires_from_py(expires)?,
            path,
            domain,
            secure,
            http_only: httponly,
            same_site: same_site_from_py(samesite)?,
        };
        Cookie::from_parts(name, value, attributes)
    }

    /// Returns the cookie that tells the browser to drop `name` (empty value, `Max-Age=0`).
    ///
    /// The browser drops only the cookie with the same path and domain, and refuses to replace
    /// an `HttpOnly` cookie with one that is not, so pass the attributes it was set with.
    #[staticmethod]
    #[pyo3(signature = (name, *, path=Some("/"), domain=None, secure=false, httponly=false, samesite=None))]
    fn expired(
        name: &str,
        path: Option<&str>,
        domain: Option<&str>,
        secure: bool,
        httponly: bool,
        samesite: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let attributes = CookieAttributes {
            max_age: Some(0),
            expires: None,
            path,
            domain,
            secure,
            http_only: httponly,
            same_site: same_site_from_py(samesite)?,
        };
        Cookie::from_parts(name, "", attributes)
    }

    fn __str__(&self) -> PyResult<&str> {
        crate::exchange::response::header_text(&self.line)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Cookie({:?})", self.__str__()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attributes() -> CookieAttributes<'static> {
        CookieAttributes {
            max_age: None,
            expires: None,
            path: Some("/"),
            domain: None,
            secure: false,
            http_only: false,
            same_site: SameSite::Lax,
        }
    }

    #[test]
    fn a_plain_cookie_is_one_line_with_its_path_and_same_site() {
        assert_eq!(
            set_cookie_line("sid", "abc123", attributes()).unwrap(),
            "sid=abc123; Path=/; SameSite=Lax"
        );
    }

    #[test]
    fn every_attribute_goes_out_in_its_place() {
        let a = CookieAttributes {
            max_age: Some(3600),
            expires: Some(1_792_567_680.0),
            path: Some("/app"),
            domain: Some("example.com"),
            secure: true,
            http_only: true,
            same_site: SameSite::None,
        };
        assert_eq!(
            set_cookie_line("sid", "v", a).unwrap(),
            "sid=v; Max-Age=3600; Expires=Wed, 21 Oct 2026 07:28:00 GMT; Path=/app; \
             Domain=example.com; Secure; HttpOnly; SameSite=None"
        );
    }

    #[test]
    fn a_name_is_a_token() {
        assert_eq!(set_cookie_line("", "v", attributes()), Err(CookieError::EmptyName));
        for (name, found) in [("a=b", '='), ("a b", ' '), ("a;b", ';'), ("a\r\nb", '\r'), ("é", 'é')] {
            assert_eq!(
                set_cookie_line(name, "v", attributes()),
                Err(CookieError::NameNotToken {
                    name: name.to_owned(),
                    found
                }),
            );
        }
        assert!(set_cookie_line("!#$%&'*+-.^_`|~Az09", "v", attributes()).is_ok());
    }

    #[test]
    fn a_value_is_cookie_octets_bare_or_in_one_pair_of_quotes() {
        for value in ["", "abc", "a=b", "\"quoted\"", "\"\"", "!#$%&'()*+-./:<=>?@[]^_`{|}~"] {
            assert!(set_cookie_line("n", value, attributes()).is_ok(), "{value:?}");
        }
        for (value, found) in [
            ("a b", ' '),
            ("a,b", ','),
            ("a;b", ';'),
            ("a\\b", '\\'),
            ("a\"b", '"'),
            ("\"", '"'),
            ("bad\r\nSet-Cookie: evil=1", '\r'),
            ("a\0b", '\0'),
            ("é", 'é'),
        ] {
            assert_eq!(
                set_cookie_line("n", value, attributes()),
                Err(CookieError::ValueNotCookieOctets {
                    name: "n".to_owned(),
                    value: value.to_owned(),
                    found
                }),
            );
        }
    }

    #[test]
    fn path_and_domain_hold_no_control_character_or_semicolon() {
        let path = |p: &'static str| CookieAttributes {
            path: Some(p),
            ..attributes()
        };
        assert!(set_cookie_line("n", "v", path("/a b,c")).is_ok());
        for (p, found) in [("/a;b", ';'), ("/a\r\n", '\r'), ("/\x7f", '\x7f'), ("/é", 'é')] {
            assert_eq!(
                set_cookie_line("n", "v", path(p)),
                Err(CookieError::ForbiddenCharInPath {
                    name: "n".to_owned(),
                    path: p.to_owned(),
                    found
                }),
            );
        }
        let domain = CookieAttributes {
            domain: Some("a.com; Secure"),
            ..attributes()
        };
        assert!(matches!(
            set_cookie_line("n", "v", domain),
            Err(CookieError::ForbiddenCharInDomain { found: ';', .. })
        ));
    }

    #[test]
    fn same_site_none_needs_secure() {
        let none = CookieAttributes {
            same_site: SameSite::None,
            ..attributes()
        };
        assert_eq!(
            set_cookie_line("n", "v", none),
            Err(CookieError::SameSiteNoneWithoutSecure { name: "n".to_owned() })
        );
        let secure = CookieAttributes { secure: true, ..none };
        assert!(set_cookie_line("n", "v", secure).is_ok());
    }

    #[test]
    fn expires_is_an_http_date_within_its_four_digit_years() {
        assert_eq!(http_date("n", 0.0).unwrap(), "Thu, 01 Jan 1970 00:00:00 GMT");
        assert_eq!(http_date("n", 0.999).unwrap(), "Thu, 01 Jan 1970 00:00:00 GMT");
        let last = (FIRST_SECOND_PAST_9999 - 1) as f64;
        assert_eq!(http_date("n", last).unwrap(), "Fri, 31 Dec 9999 23:59:59 GMT");
        for seconds in [-0.5, -1.0, FIRST_SECOND_PAST_9999 as f64, f64::NAN, f64::INFINITY] {
            assert!(
                matches!(http_date("n", seconds), Err(CookieError::ExpiresOutOfRange { .. })),
                "{seconds}"
            );
        }
    }
}
