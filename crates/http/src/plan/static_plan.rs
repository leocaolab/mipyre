//! Decides which static file a request path names under which mount, and how it is encoded.
//!
//! The only way to a [`StaticLookup`] is the [`StaticLookups`] iterator, so every key the engine
//! reads has been through [`relative_key`].

use std::borrow::Cow;

use http::header::{HeaderMap, ACCEPT_ENCODING};
use http::StatusCode;
use percent_encoding::percent_decode_str;

use crate::plan::compression::{negotiate, Coding, CodingSet};

/// A URL prefix that serves files; the engine keeps the directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticMountRule {
    /// The prefix starts and ends with `/`, so `/static/` never matches `/staticfoo`.
    prefix: String,
    copies: Copies,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("static prefix {0:?} must start with '/'")]
pub struct StaticPrefixError(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Copies {
    Original,
    /// The mount also serves a `.br` or `.gz` twin from beside the file when the client accepts it.
    Precompressed,
}

impl StaticMountRule {
    pub fn new(prefix: &str, copies: Copies) -> Result<Self, StaticPrefixError> {
        if !prefix.starts_with('/') {
            return Err(StaticPrefixError(prefix.to_string()));
        }
        let prefix = if prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{prefix}/")
        };
        Ok(Self { prefix, copies })
    }

    pub fn copies(&self) -> Copies {
        self.copies
    }

    /// Returns the still percent-encoded part of `req_path` under this mount.
    pub fn relative<'p>(&self, req_path: &'p str) -> Option<&'p str> {
        req_path
            .strip_prefix(self.prefix.as_str())
            .filter(|rel| !rel.is_empty())
    }
}

/// A request path refused before any file is looked at; a missing file is not one, because it
/// lets the next mount, then routing, answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StaticPathError {
    #[error("request path contains an encoded NUL byte")]
    NulByte,
    #[error("request path climbs out of the static root with '..'")]
    Traversal,
}

impl StaticPathError {
    pub fn status(self) -> StatusCode {
        match self {
            Self::NulByte => StatusCode::BAD_REQUEST,
            Self::Traversal => StatusCode::FORBIDDEN,
        }
    }
}

/// One file asked of one mount; only [`StaticLookups`] makes one.
#[derive(Debug)]
pub struct StaticLookup<'r, 'p, D> {
    mount: &'r D,
    key: Cow<'p, [u8]>,
    accept_encoding: Option<&'p str>,
    headers: &'p HeaderMap,
}

impl<'r, 'p, D> StaticLookup<'r, 'p, D> {
    pub fn mount(&self) -> &'r D {
        self.mount
    }

    /// Returns the normalized path under the mount, which is both the cache key and what is
    /// joined to the directory.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn into_key(self) -> Cow<'p, [u8]> {
        self.key
    }

    /// Returns the request's `Accept-Encoding` only when the mount serves pre-compressed twins
    /// and the value is visible ASCII.
    pub fn accept_encoding(&self) -> Option<&'p str> {
        self.accept_encoding
    }

    pub fn headers(&self) -> &'p HeaderMap {
        self.headers
    }
}

/// A file asked of one mount, or a refusal, which answers the request so the caller stops there.
#[derive(Debug)]
pub enum Lookup<'r, 'p, D> {
    File(StaticLookup<'r, 'p, D>),
    Refused(StaticPathError),
}

/// Yields a [`Lookup`] for each mount a request path is under, in registration order.
pub struct StaticLookups<'r, 'p, D> {
    mounts: std::slice::Iter<'r, D>,
    path: &'p str,
    headers: &'p HeaderMap,
}

pub(crate) fn lookups<'r, 'p, D: AsRef<StaticMountRule>>(
    path: &'p str,
    headers: &'p HeaderMap,
    mounts: &'r [D],
) -> StaticLookups<'r, 'p, D> {
    StaticLookups {
        mounts: mounts.iter(),
        path,
        headers,
    }
}

impl<'r, 'p, D> StaticLookups<'r, 'p, D> {
    /// Yields nothing, for a request no static file answers.
    pub(crate) fn none(headers: &'p HeaderMap) -> Self {
        StaticLookups {
            mounts: <&[D]>::default().iter(),
            path: "",
            headers,
        }
    }
}

impl<'r, 'p, D: AsRef<StaticMountRule>> Iterator for StaticLookups<'r, 'p, D> {
    type Item = Lookup<'r, 'p, D>;

    fn next(&mut self) -> Option<Self::Item> {
        let path = self.path;
        let (mount, rel) = self
            .mounts
            .by_ref()
            .find_map(|mount| mount.as_ref().relative(path).map(|rel| (mount, rel)))?;
        let rule = mount.as_ref();
        let accept_encoding = if rule.copies() == Copies::Precompressed {
            self.headers.get(ACCEPT_ENCODING).and_then(|v| v.to_str().ok())
        } else {
            None
        };
        Some(match relative_key(rel) {
            RelativeKey::Named(key) => Lookup::File(StaticLookup {
                mount,
                key,
                accept_encoding,
                headers: self.headers,
            }),
            RelativeKey::Refused(refusal) => Lookup::Refused(refusal),
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RelativeKey<'p> {
    /// The key is plain names joined by `/`, such as `a/b.css`.
    Named(Cow<'p, [u8]>),
    Refused(StaticPathError),
}

/// Percent-decodes `rel` and normalizes it to plain names joined by `/`, borrowing it when it
/// is already in that form.
///
/// Decoding comes first, so neither `%2e%2e` nor `%2f` can climb out of the root. The bytes are
/// not checked to be a valid platform file name; the engine does that when it joins the key to
/// the directory.
pub fn relative_key(rel: &str) -> RelativeKey<'_> {
    let decoded: Cow<'_, [u8]> = percent_decode_str(rel).into();
    if decoded.contains(&0) {
        return RelativeKey::Refused(StaticPathError::NulByte);
    }
    let mut plain = true;
    for segment in decoded.split(|&b| b == b'/') {
        match segment {
            b"" | b"." => plain = false,
            b".." => return RelativeKey::Refused(StaticPathError::Traversal),
            _ => {}
        }
    }
    if plain {
        return RelativeKey::Named(decoded);
    }
    let names: Vec<&[u8]> = decoded
        .split(|&b| b == b'/')
        .filter(|segment| !matches!(*segment, b"" | b"."))
        .collect();
    RelativeKey::Named(Cow::Owned(names.join(&b'/')))
}

/// Returns the MIME type for a lowercase extension without its dot, and
/// `application/octet-stream` for an unknown one.
pub fn mime_from_ext(ext: &str) -> &'static str {
    match ext {
        "html" | "htm" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "webp" => "image/webp",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "pdf" => "application/pdf",
        "xml" => "application/xml; charset=utf-8",
        "txt" => "text/plain; charset=utf-8",
        "wasm" => "application/wasm",
        "map" => "application/json",
        _ => "application/octet-stream",
    }
}

/// Picks the pre-compressed copy by the same rules as dynamic compression; `None` means the
/// original.
pub fn choose_precompressed(accept_encoding: Option<&str>, has: CodingSet) -> Option<Coding> {
    negotiate(accept_encoding?, has)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equivalent_spellings_of_a_path_share_one_entry() {
        assert!(matches!(relative_key("a/b.css"), RelativeKey::Named(Cow::Borrowed(_))));
        for rel in ["a//b.css", "./a/b.css", "a/./b.css", "%61/b.css", "a%2fb.css"] {
            assert_eq!(
                relative_key(rel),
                RelativeKey::Named(Cow::Borrowed(b"a/b.css")),
                "{rel}"
            );
        }
    }

    #[test]
    fn mount_rejects_bad_prefix() {
        assert!(matches!(
            StaticMountRule::new("static", Copies::Original),
            Err(StaticPrefixError(_))
        ));
        let rule = StaticMountRule::new("/s", Copies::Original).unwrap();
        assert_eq!(rule.relative("/s/a.txt"), Some("a.txt"));
        assert_eq!(rule.relative("/sa.txt"), None);
        assert_eq!(rule.relative("/s/"), None);
    }

    struct Mount(StaticMountRule, &'static str);

    impl AsRef<StaticMountRule> for Mount {
        fn as_ref(&self) -> &StaticMountRule {
            &self.0
        }
    }

    #[test]
    fn mounts_are_asked_in_order_and_accept_encoding_only_on_a_precompressed_one() {
        let mounts = [
            Mount(StaticMountRule::new("/a", Copies::Original).unwrap(), "first"),
            Mount(StaticMountRule::new("/b", Copies::Precompressed).unwrap(), "second"),
            Mount(StaticMountRule::new("/a", Copies::Precompressed).unwrap(), "third"),
        ];
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT_ENCODING, http::HeaderValue::from_static("br"));

        let got: Vec<_> = lookups("/a/x.css", &headers, &mounts)
            .map(|lookup| {
                let Lookup::File(lookup) = lookup else {
                    panic!("a plain path under the mount is refused");
                };
                (lookup.mount.1, lookup.key.into_owned(), lookup.accept_encoding)
            })
            .collect();
        assert_eq!(
            got,
            [
                ("first", b"x.css".to_vec(), None),
                ("third", b"x.css".to_vec(), Some("br")),
            ]
        );
        assert_eq!(lookups("/c/x.css", &headers, &mounts).count(), 0);
    }

    #[test]
    fn a_refused_path_is_yielded_as_its_refusal() {
        let mounts = [Mount(StaticMountRule::new("/a", Copies::Original).unwrap(), "only")];
        let headers = HeaderMap::new();
        let mut asked = lookups("/a/%2e%2e/etc/passwd", &headers, &mounts);
        assert!(matches!(
            asked.next(),
            Some(Lookup::Refused(StaticPathError::Traversal))
        ));
        let mut asked = lookups("/a/x%00.txt", &headers, &mounts);
        assert!(matches!(asked.next(), Some(Lookup::Refused(StaticPathError::NulByte))));
        assert_eq!(StaticPathError::Traversal.status(), StatusCode::FORBIDDEN);
        assert_eq!(StaticPathError::NulByte.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn mime_html() {
        assert_eq!(mime_from_ext("html"), "text/html; charset=utf-8");
        assert_eq!(mime_from_ext("htm"), "text/html; charset=utf-8");
    }

    #[test]
    fn mime_js_css() {
        assert_eq!(mime_from_ext("css"), "text/css; charset=utf-8");
        assert_eq!(mime_from_ext("js"), "application/javascript; charset=utf-8");
        assert_eq!(mime_from_ext("mjs"), "application/javascript; charset=utf-8");
    }

    #[test]
    fn mime_images() {
        assert_eq!(mime_from_ext("png"), "image/png");
        assert_eq!(mime_from_ext("jpg"), "image/jpeg");
        assert_eq!(mime_from_ext("jpeg"), "image/jpeg");
        assert_eq!(mime_from_ext("gif"), "image/gif");
        assert_eq!(mime_from_ext("svg"), "image/svg+xml");
        assert_eq!(mime_from_ext("webp"), "image/webp");
        assert_eq!(mime_from_ext("ico"), "image/x-icon");
    }

    #[test]
    fn mime_fonts() {
        assert_eq!(mime_from_ext("woff"), "font/woff");
        assert_eq!(mime_from_ext("woff2"), "font/woff2");
        assert_eq!(mime_from_ext("ttf"), "font/ttf");
        assert_eq!(mime_from_ext("otf"), "font/otf");
    }

    #[test]
    fn mime_application() {
        assert_eq!(mime_from_ext("json"), "application/json; charset=utf-8");
        assert_eq!(mime_from_ext("pdf"), "application/pdf");
        assert_eq!(mime_from_ext("xml"), "application/xml; charset=utf-8");
        assert_eq!(mime_from_ext("wasm"), "application/wasm");
        assert_eq!(mime_from_ext("map"), "application/json");
    }

    #[test]
    fn mime_unknown_fallback() {
        assert_eq!(mime_from_ext("xyz"), "application/octet-stream");
        assert_eq!(mime_from_ext(""), "application/octet-stream");
        assert_eq!(mime_from_ext("bin"), "application/octet-stream");
    }

    #[test]
    fn the_twin_is_the_first_in_server_preference_that_exists_and_is_accepted() {
        let both = CodingSet::ALL;
        assert_eq!(
            choose_precompressed(Some("br;q=1, gzip;q=0.8"), both),
            Some(Coding::Brotli)
        );
        assert_eq!(choose_precompressed(Some("gzip"), both), Some(Coding::Gzip));
        assert_eq!(choose_precompressed(Some("br;q=0, gzip;q=0"), both), None);
        assert_eq!(choose_precompressed(None, both), None);
        let only_gz = CodingSet::only(Coding::Gzip);
        assert_eq!(choose_precompressed(Some("br, gzip"), only_gz), Some(Coding::Gzip));
        assert_eq!(choose_precompressed(Some("br"), only_gz), None);
    }
}
