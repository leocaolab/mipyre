//! The static path fuzz target: whatever path a client asks a mount for, it is refused for an encoded NUL
//! or a `..` segment, or named by a key of plain names that, joined to the mount's root, stays under it; and a key
//! is its own normal form.
//!
//! `cargo test` replays the committed corpus (`__fuzz__/.../corpus`) and nothing else, so the merge runs the same
//! inputs every time; `cargo bolero test -p mipyre_http --lib plan::static_plan::fuzz::...` mutates it under
//! libFuzzer, and an input it finds that fails goes into the corpus as a regression.

use std::borrow::Cow;
use std::path::{Component, Path};

use http::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING};
use percent_encoding::percent_decode_str;

use super::{lookups, relative_key, Copies, Lookup, RelativeKey, StaticMountRule, StaticPathError};

const ROOT: &str = "/srv/static-root";

#[derive(Debug)]
struct Mount(StaticMountRule);

impl AsRef<StaticMountRule> for Mount {
    fn as_ref(&self) -> &StaticMountRule {
        &self.0
    }
}

fn mounts() -> [Mount; 3] {
    [
        Mount(StaticMountRule::new("/static", Copies::Precompressed).unwrap()),
        Mount(StaticMountRule::new("/", Copies::Original).unwrap()),
        Mount(StaticMountRule::new("/static/deep/", Copies::Original).unwrap()),
    ]
}

fn expected_refusal(rel: &str) -> Option<StaticPathError> {
    let decoded: Vec<u8> = percent_decode_str(rel).collect();
    if decoded.contains(&0) {
        Some(StaticPathError::NulByte)
    } else if decoded.split(|&b| b == b'/').any(|segment| segment == b"..") {
        Some(StaticPathError::Traversal)
    } else {
        None
    }
}

fn assert_plain_names(key: &[u8], rel: &str) {
    assert!(!key.contains(&0), "{rel:?} named {key:?}");
    if key.is_empty() {
        return;
    }
    for segment in key.split(|&b| b == b'/') {
        assert!(
            !matches!(segment, b"" | b"." | b".."),
            "{rel:?} named {key:?}, with the segment {segment:?}"
        );
    }
}

fn assert_joined_under_root(key: &[u8], rel: &str) {
    let root = Path::new(ROOT);
    let joined = root.join(&*String::from_utf8_lossy(key));
    assert!(joined.starts_with(root), "{rel:?} joined to {joined:?}");
    let below = joined.strip_prefix(root).unwrap();
    assert!(
        below.components().all(|c| matches!(c, Component::Normal(_))),
        "{rel:?} joined to {joined:?}"
    );
}

fn assert_normal_form(key: &[u8], rel: &str) {
    let Ok(text) = std::str::from_utf8(key) else {
        return;
    };
    if text.contains('%') {
        return;
    }
    assert_eq!(
        relative_key(text),
        RelativeKey::Named(Cow::Borrowed(key)),
        "{rel:?} named {key:?}, which is not its own normal form"
    );
}

fn look_up((raw_path, raw_accept): &(Vec<u8>, Vec<u8>)) {
    let path = String::from_utf8_lossy(raw_path);
    let mut headers = HeaderMap::new();
    if let Ok(accept) = HeaderValue::from_bytes(raw_accept) {
        headers.insert(ACCEPT_ENCODING, accept);
    }
    let mounts = mounts();
    let under: Vec<(&Mount, &str)> = mounts
        .iter()
        .filter_map(|mount| mount.0.relative(&path).map(|rel| (mount, rel)))
        .collect();

    let asked: Vec<_> = lookups(&path, &headers, &mounts).collect();
    assert_eq!(asked.len(), under.len(), "{path:?}");
    for (lookup, (mount, rel)) in asked.into_iter().zip(under) {
        match (lookup, expected_refusal(rel)) {
            (Lookup::Refused(refusal), Some(expected)) => assert_eq!(refusal, expected, "{rel:?}"),
            (Lookup::File(file), None) => {
                assert!(std::ptr::eq(file.mount(), mount), "{path:?}");
                let precompressed = mount.0.copies() == Copies::Precompressed;
                assert_eq!(
                    file.accept_encoding().is_some(),
                    precompressed && headers.get(ACCEPT_ENCODING).is_some_and(|v| v.to_str().is_ok()),
                    "{path:?}"
                );
                assert_plain_names(file.key(), rel);
                assert_joined_under_root(file.key(), rel);
                assert_normal_form(file.key(), rel);
            }
            (lookup, expected) => panic!("{rel:?} gave {lookup:?}, expected the refusal {expected:?}"),
        }
    }
}

#[test]
fn any_static_path_is_refused_or_named_under_the_root() {
    bolero::check!()
        .with_type::<(Vec<u8>, Vec<u8>)>()
        .with_iterations(0)
        .for_each(look_up);
}

#[test]
fn the_known_climbs_out_of_a_root_are_refused_or_stay_under_it() {
    let long = format!("/static/{}", "a/".repeat(4096));
    let spellings = [
        "/static/../etc/passwd",
        "/static/a/../../etc/passwd",
        "/static/%2e%2e/etc/passwd",
        "/static/%2E%2e%2fetc%2fpasswd",
        "/static/.%2e/x",
        "/static/..\\..\\etc\\passwd",
        "/static/%5c..%5c..%5cetc",
        "/static/x%00.txt",
        "/static/%2fetc%2fpasswd",
        "/static//etc/passwd",
        "/static/./.",
        "/static/%c0%ae%c0%ae/etc",
        "/static/..%00/x",
        long.as_str(),
    ];
    for path in spellings {
        look_up(&(path.as_bytes().to_vec(), b"br, gzip".to_vec()));
    }
}
