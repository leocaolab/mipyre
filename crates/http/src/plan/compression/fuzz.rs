//! The `Accept-Encoding` fuzz target: whatever a client sends, negotiation picks one of the allowed codings,
//! the one it picks from all codings whenever that one is allowed, and a coding the client names with no weight is
//! accepted however the header weighed it before.
//!
//! `cargo test` replays the committed corpus (`__fuzz__/.../corpus`) and nothing else, so the merge runs the same
//! inputs every time; `cargo bolero test -p mipyre_http --lib plan::compression::fuzz::...` mutates it under
//! libFuzzer, and an input it finds that fails goes into the corpus as a regression.

use super::{negotiate, Coding, CodingSet};

fn allowed_sets() -> [CodingSet; 4] {
    [
        CodingSet::ALL,
        CodingSet::only(Coding::Gzip),
        CodingSet::only(Coding::Brotli),
        CodingSet::of(|_| false),
    ]
}

fn negotiate_any(raw: &[u8]) {
    let header = String::from_utf8_lossy(raw);
    let best = negotiate(&header, CodingSet::ALL);
    for allowed in allowed_sets() {
        let picked = negotiate(&header, allowed);
        assert!(
            picked.is_none_or(|c| allowed.contains(c)),
            "{header:?} picked {picked:?} outside {allowed:?}"
        );
        match best {
            Some(c) if allowed.contains(c) => assert_eq!(picked, best, "{header:?} under {allowed:?}"),
            None => assert_eq!(picked, None, "{header:?} accepts nothing, yet under {allowed:?}"),
            Some(_) => {}
        }
    }
    for coding in Coding::PREFERENCE {
        let named = format!("{header}, {}", coding.name());
        assert_eq!(negotiate(&named, CodingSet::only(coding)), Some(coding), "{named:?}");
    }
    assert_eq!(
        negotiate(&format!("{header}, br"), CodingSet::ALL),
        Some(Coding::Brotli),
        "{header:?}, br"
    );
}

#[test]
fn any_accept_encoding_picks_an_allowed_coding_the_client_accepts() {
    bolero::check!().with_iterations(0).for_each(negotiate_any);
}
