//! What any static path a client asks the mounts for must come to: refused for an encoded NUL or a `..` segment, or
//! named by a key of plain names that, joined to the mount's root, stays under it; and a key is its own normal form.
//! The known climbs in `static_plan`'s tests and mipyre_http_harness's fuzz target both check against it.

use std::borrow::Cow;
use std::path::{Component, Path};

use http::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING};
use percent_encoding::percent_decode_str;

use super::{lookups, relative_key, Copies, Lookup, RelativeKey, StaticMountRule, StaticPathError};

const ROOT: &str = "/srv/static-root";

#[derive(Debug)]
struct StubMount(StaticMountRule);

impl AsRef<StaticMountRule> for StubMount {
    fn as_ref(&self) -> &StaticMountRule {
        &self.0
    }
}

fn mounts() -> [StubMount; 3] {
    [
        StubMount(StaticMountRule::new("/static", Copies::Precompressed).unwrap()),
        StubMount(StaticMountRule::new("/", Copies::Original).unwrap()),
        StubMount(StaticMountRule::new("/static/deep/", Copies::Original).unwrap()),
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

pub(super) fn look_up((raw_path, raw_accept): &(Vec<u8>, Vec<u8>)) {
    let path = String::from_utf8_lossy(raw_path);
    let mut headers = HeaderMap::new();
    if let Ok(accept) = HeaderValue::from_bytes(raw_accept) {
        headers.insert(ACCEPT_ENCODING, accept);
    }
    let mounts = mounts();
    let under: Vec<(&StubMount, &str)> = mounts
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
