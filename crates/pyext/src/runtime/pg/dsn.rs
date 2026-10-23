//! Keeps the password of a Postgres connection string out of logs and errors.

use std::fmt;

const MASK: &str = "***";

/// A Postgres connection string in URL form (`postgres://user:pass@host/db`) or libpq
/// key=value form (`host=... password=...`). `Debug` and `Display` mask the password; the raw
/// text is only reachable through [`Dsn::expose_secret`].
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Dsn(String);

impl Dsn {
    pub(crate) fn new(raw: String) -> Self {
        Self(raw)
    }

    /// Returns the connection string with its password. Only the code that opens the
    /// connection calls this; anything that may reach a log or an error uses `Display`.
    pub(crate) fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Dsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&mask_password(&self.0))
    }
}

impl fmt::Debug for Dsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Dsn({})", mask_password(&self.0))
    }
}

fn mask_password(dsn: &str) -> String {
    match dsn.split_once("://") {
        Some((scheme, rest)) if !scheme.contains(['=', ' ']) => mask_url(scheme, rest),
        _ => mask_libpq(dsn),
    }
}

/// Masks the password in the authority (`user:pass@host`) and in a `password` query parameter.
fn mask_url(scheme: &str, rest: &str) -> String {
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    // The password may itself contain an unencoded '@', so the host starts after the last one.
    let authority = match authority.rsplit_once('@') {
        Some((userinfo, host)) => match userinfo.split_once(':') {
            Some((user, _)) => format!("{user}:{MASK}@{host}"),
            None => format!("{userinfo}@{host}"),
        },
        None => authority.to_owned(),
    };
    let tail = match tail.split_once('?') {
        Some((path, query)) => format!("{path}?{}", mask_query(query)),
        None => tail.to_owned(),
    };
    format!("{scheme}://{authority}{tail}")
}

fn mask_query(query: &str) -> String {
    query
        .split('&')
        .map(|pair| match pair.split_once('=') {
            Some((key, _)) if key.eq_ignore_ascii_case("password") => format!("{key}={MASK}"),
            _ => pair.to_owned(),
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// Masks the `password` value; pairs are separated by whitespace, and a single-quoted value
/// (with `\` escapes) may contain spaces.
fn mask_libpq(dsn: &str) -> String {
    let mut out = String::with_capacity(dsn.len());
    let mut rest = dsn;
    while let Some(eq) = rest.find('=') {
        let key = rest[..eq].rsplit(char::is_whitespace).next().unwrap_or("");
        let value_start = eq + 1;
        let value_end = value_start + libpq_value_len(&rest[value_start..]);
        out.push_str(&rest[..value_start]);
        if key.eq_ignore_ascii_case("password") {
            out.push_str(MASK);
        } else {
            out.push_str(&rest[value_start..value_end]);
        }
        rest = &rest[value_end..];
    }
    out.push_str(rest);
    out
}

/// Returns the byte length of the libpq value at the start of `s`: a quoted value runs to its
/// closing quote (an unterminated one to the end), a bare value to the next whitespace.
fn libpq_value_len(s: &str) -> usize {
    if !s.starts_with('\'') {
        return s.find(char::is_whitespace).unwrap_or(s.len());
    }
    let bytes = s.as_bytes();
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'\'' => return i + 1,
            _ => i += 1,
        }
    }
    bytes.len()
}

#[cfg(test)]
mod tests {
    use super::Dsn;

    fn dsn(raw: &str) -> Dsn {
        Dsn::new(raw.to_owned())
    }

    #[test]
    fn url_password_is_masked_in_display_and_debug() {
        let d = dsn("postgres://app:s3cret@localhost:5432/appdb");
        assert_eq!(d.to_string(), "postgres://app:***@localhost:5432/appdb");
        assert_eq!(format!("{d:?}"), "Dsn(postgres://app:***@localhost:5432/appdb)");
    }

    #[test]
    fn url_without_password_is_unchanged() {
        assert_eq!(dsn("postgres://localhost/sky").to_string(), "postgres://localhost/sky");
        assert_eq!(
            dsn("postgres://sky@localhost/sky").to_string(),
            "postgres://sky@localhost/sky"
        );
    }

    #[test]
    fn url_password_containing_at_is_masked_whole() {
        assert_eq!(dsn("postgres://u:p@ss@h/db").to_string(), "postgres://u:***@h/db");
    }

    #[test]
    fn url_password_query_parameter_is_masked() {
        assert_eq!(
            dsn("postgres://h/sky?password=abc&sslmode=require").to_string(),
            "postgres://h/sky?password=***&sslmode=require"
        );
    }

    #[test]
    fn libpq_password_is_masked() {
        assert_eq!(
            dsn("host=localhost user=sky password=s3cret dbname=sky").to_string(),
            "host=localhost user=sky password=*** dbname=sky"
        );
    }

    #[test]
    fn libpq_quoted_password_is_masked_whole() {
        assert_eq!(
            dsn(r"host=h password='a b\'c' dbname=x").to_string(),
            "host=h password=*** dbname=x"
        );
    }

    #[test]
    fn expose_secret_is_the_raw_text() {
        let raw = "postgres://u:s3cret@h/db";
        assert_eq!(dsn(raw).expose_secret(), raw);
        assert!(!format!("{:?}", dsn(raw)).contains("s3cret"));
    }
}
