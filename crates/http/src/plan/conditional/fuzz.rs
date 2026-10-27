//! The conditional-request fuzz target: whatever `If-None-Match` and `If-Modified-Since` a client sends,
//! `If-None-Match` alone decides when present, a `304` needs the tag or `*` in it, and a line naming the tag, weak or
//! strong, earns one.
//!
//! `cargo test` replays the committed corpus (`__fuzz__/.../corpus`) and nothing else, so the merge runs the same
//! inputs every time; `cargo bolero test -p mipyre_http --lib plan::conditional::fuzz::...` mutates it under
//! libFuzzer, and an input it finds that fails goes into the corpus as a regression.

use std::str::FromStr;

use http::header::{HeaderMap, HeaderName, HeaderValue, IF_MODIFIED_SINCE, IF_NONE_MATCH};
use httpdate::HttpDate;

use super::{not_modified, EntityTag};
use crate::plan::fuzz_headers::header_map;

const HEADERS: [HeaderName; 2] = [IF_NONE_MATCH, IF_MODIFIED_SINCE];
const MODIFIED: [Option<&str>; 3] = [
    None,
    Some("Sun, 06 Nov 1994 08:49:37 GMT"),
    Some("Fri, 01 Jan 2100 00:00:00 GMT"),
];

fn with_line(request: &HeaderMap, line: &str) -> HeaderMap {
    let mut more = request.clone();
    more.append(IF_NONE_MATCH, HeaderValue::from_str(line).unwrap());
    more
}

fn decide((digest, modified, fields): &(u64, u8, Vec<(u8, Vec<u8>)>)) {
    let etag = EntityTag::of_digest(*digest);
    let modified = MODIFIED[usize::from(*modified) % MODIFIED.len()].map(|d| HttpDate::from_str(d).unwrap());
    let request = header_map(&HEADERS, fields);
    let answer = not_modified(&request, &etag, modified);

    if request.contains_key(IF_NONE_MATCH) {
        let mut without_since = request.clone();
        without_since.remove(IF_MODIFIED_SINCE);
        assert_eq!(not_modified(&without_since, &etag, modified), answer);
        let tag = etag.as_bytes();
        let names_it = request.get_all(IF_NONE_MATCH).iter().any(|line| {
            line.as_bytes().trim_ascii().ends_with(b"*") || line.as_bytes().windows(tag.len()).any(|w| w == tag)
        });
        assert!(
            !answer || names_it,
            "a 304 for {request:?} that names neither {etag:?} nor *"
        );
    } else if answer {
        assert!(modified.is_some() && request.contains_key(IF_MODIFIED_SINCE));
    }

    let tag = std::str::from_utf8(etag.as_bytes()).unwrap();
    for line in [tag.to_owned(), format!("W/{tag}"), format!(" , W/\"other\", {tag}")] {
        assert!(
            not_modified(&with_line(&request, &line), &etag, modified),
            "{line:?} after {request:?}"
        );
    }
}

#[test]
fn any_conditional_request_earns_a_304_only_for_the_tag_it_names() {
    bolero::check!()
        .with_type::<(u64, u8, Vec<(u8, Vec<u8>)>)>()
        .with_iterations(0)
        .for_each(decide);
}
