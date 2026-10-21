//! Decides whether a conditional `GET` or `HEAD` (RFC 9110 §13) is answered `304 Not Modified`.

use std::str::FromStr;

use http::header::{HeaderMap, HeaderValue, IF_MODIFIED_SINCE, IF_NONE_MATCH};
use httpdate::HttpDate;

/// A strong entity-tag (RFC 9110 §8.8.3) of 16 hex digits in double quotes. It is always well
/// formed, because only [`EntityTag::of_digest`] makes one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityTag(HeaderValue);

impl EntityTag {
    #[expect(
        clippy::expect_used,
        reason = "quotes and hex digits are visible ASCII: the header-value parse cannot fail"
    )]
    pub fn of_digest(digest: u64) -> Self {
        EntityTag(
            HeaderValue::try_from(format!("\"{digest:016x}\""))
                .expect("quotes and hex digits are visible ASCII"),
        )
    }

    pub fn header_value(&self) -> HeaderValue {
        self.0.clone()
    }

    fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// Tells whether the client already has the representation tagged `etag` that last changed at
/// `modified` (RFC 9110 §13.2.2, steps 3 and 4).
///
/// `If-None-Match`, when present, alone decides, by weak comparison. A list that does not parse
/// matches nothing past where it stops, so a malformed header never earns a `304`. Without it,
/// an `If-Modified-Since` that does not parse is ignored.
pub fn not_modified(request: &HeaderMap, etag: &EntityTag, modified: Option<HttpDate>) -> bool {
    let mut lines = request.get_all(IF_NONE_MATCH).iter().peekable();
    if lines.peek().is_some() {
        return lines.any(|line| none_match_lists(line.as_bytes(), etag.as_bytes()));
    }
    let Some(since) = request.get(IF_MODIFIED_SINCE) else {
        return false;
    };
    let (Some(modified), Ok(since)) = (
        modified,
        since.to_str().map_err(drop).and_then(|s| HttpDate::from_str(s).map_err(drop)),
    ) else {
        return false;
    };
    modified <= since
}

/// Walks the list tag by tag rather than splitting it, because an entity-tag may hold a comma.
fn none_match_lists(line: &[u8], etag: &[u8]) -> bool {
    let mut rest = trim_list_start(line.trim_ascii());
    if rest == b"*" {
        return true;
    }
    while !rest.is_empty() {
        let unweakened = rest.strip_prefix(b"W/").unwrap_or(rest);
        let Some(after_quote) = unweakened.strip_prefix(b"\"") else {
            return false;
        };
        let Some(close) = after_quote.iter().position(|&b| b == b'"') else {
            return false;
        };
        let Some((tag, after)) = unweakened.split_at_checked(close + 2) else {
            return false;
        };
        if tag == etag {
            return true;
        }
        rest = trim_list_start(after);
    }
    false
}

/// Skips optional whitespace and empty list members (RFC 9110 §5.6.1).
fn trim_list_start(list: &[u8]) -> &[u8] {
    let start = list
        .iter()
        .position(|&b| !matches!(b, b' ' | b'\t' | b','))
        .unwrap_or(list.len());
    list.get(start..).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn etag() -> EntityTag {
        EntityTag::of_digest(0x0123_4567_89ab_cdef)
    }

    fn headers(fields: &[(http::HeaderName, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in fields {
            map.append(name.clone(), HeaderValue::from_str(value).unwrap());
        }
        map
    }

    fn date(text: &str) -> HttpDate {
        HttpDate::from_str(text).unwrap()
    }

    fn not_modified(fields: &[(http::HeaderName, &str)], modified: Option<HttpDate>) -> bool {
        super::not_modified(&headers(fields), &etag(), modified)
    }

    #[test]
    fn a_tag_is_its_digest_in_hex_between_quotes() {
        assert_eq!(etag().header_value(), "\"0123456789abcdef\"");
        assert_eq!(EntityTag::of_digest(0xa).header_value(), "\"000000000000000a\"");
    }

    #[test]
    fn no_preconditions_sends_the_representation() {
        assert!(!not_modified(&[], Some(date("Sun, 06 Nov 1994 08:49:37 GMT"))));
    }

    #[test]
    fn if_none_match_names_the_tag_strong_or_weak() {
        assert!(not_modified(&[(IF_NONE_MATCH, "\"0123456789abcdef\"")], None));
        assert!(not_modified(&[(IF_NONE_MATCH, "W/\"0123456789abcdef\"")], None));
        assert!(!not_modified(&[(IF_NONE_MATCH, "\"0123456789abcdee\"")], None));
        assert!(!not_modified(&[(IF_NONE_MATCH, "0123456789abcdef")], None));
    }

    #[test]
    fn if_none_match_lists_and_lines() {
        assert!(not_modified(&[(IF_NONE_MATCH, "\"a\", W/\"b,c\" ,\"0123456789abcdef\"")], None));
        assert!(!not_modified(&[(IF_NONE_MATCH, "\"a\", W/\"b,c\"")], None));
        assert!(not_modified(
            &[(IF_NONE_MATCH, "\"a\""), (IF_NONE_MATCH, "\"0123456789abcdef\"")],
            None
        ));
        // A comma inside a tag is the tag's: this is one tag, not `"x` and `0123…"`.
        assert!(!not_modified(&[(IF_NONE_MATCH, "\"x,0123456789abcdef\"")], None));
    }

    #[test]
    fn a_star_matches_any_representation() {
        assert!(not_modified(&[(IF_NONE_MATCH, "*")], None));
        assert!(not_modified(&[(IF_NONE_MATCH, " * ")], None));
    }

    #[test]
    fn a_malformed_list_matches_nothing_past_where_it_stops() {
        assert!(!not_modified(&[(IF_NONE_MATCH, "\"unterminated")], None));
        assert!(!not_modified(&[(IF_NONE_MATCH, "bare, \"0123456789abcdef\"")], None));
        assert!(not_modified(&[(IF_NONE_MATCH, "\"0123456789abcdef\", bare")], None));
        assert!(!not_modified(&[(IF_NONE_MATCH, "")], None));
    }

    #[test]
    fn if_none_match_overrides_if_modified_since() {
        let modified = Some(date("Sun, 06 Nov 1994 08:49:37 GMT"));
        let later = "Mon, 07 Nov 1994 08:49:37 GMT";
        assert!(!not_modified(&[(IF_NONE_MATCH, "\"other\""), (IF_MODIFIED_SINCE, later)], modified));
        assert!(not_modified(&[(IF_MODIFIED_SINCE, later)], modified));
    }

    #[test]
    fn if_modified_since_on_either_side_of_the_change() {
        let modified = Some(date("Sun, 06 Nov 1994 08:49:37 GMT"));
        assert!(not_modified(&[(IF_MODIFIED_SINCE, "Sun, 06 Nov 1994 08:49:37 GMT")], modified));
        assert!(!not_modified(&[(IF_MODIFIED_SINCE, "Sun, 06 Nov 1994 08:49:36 GMT")], modified));
        // The obsolete formats a recipient must accept.
        assert!(not_modified(&[(IF_MODIFIED_SINCE, "Sunday, 06-Nov-94 08:49:37 GMT")], modified));
        assert!(not_modified(&[(IF_MODIFIED_SINCE, "Sun Nov  6 08:49:37 1994")], modified));
    }

    #[test]
    fn an_unusable_if_modified_since_is_ignored() {
        let modified = Some(date("Sun, 06 Nov 1994 08:49:37 GMT"));
        assert!(!not_modified(&[(IF_MODIFIED_SINCE, "yesterday")], modified));
        assert!(!not_modified(&[(IF_MODIFIED_SINCE, "Mon, 07 Nov 1994 08:49:37 GMT")], None));
    }
}
