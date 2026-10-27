//! The NUMERIC decoder's fuzz target: whatever bytes the database sends, the decoder answers decimal text or
//! refuses, its text is no longer than the header's widest value, and text it writes reads back to the same value.
//!
//! `cargo test` replays the committed corpus (`__fuzz__/.../corpus`) and nothing else, so the merge runs the same
//! inputs every time; `cargo bolero test -p mipyre_pyext --lib db::numeric::fuzz::...` mutates it under libFuzzer,
//! and an input it finds that fails goes into the corpus as a regression.

use super::proptests::normal_form;
use super::{decode, encode, DIGITS_PER_GROUP};

/// The longest text a header can ask for: a sign, `i16::MAX + 1` integer groups, a point and `u16::MAX` places.
const MAX_TEXT: usize = 1 + DIGITS_PER_GROUP * (i16::MAX as usize + 1) + 1 + u16::MAX as usize;

fn decode_any(raw: &[u8]) {
    let Ok(text) = decode(raw) else {
        return;
    };
    assert!(text.len() <= MAX_TEXT, "{} bytes of text from {raw:?}", text.len());
    let mut again = Vec::new();
    if encode(&text, &mut again).is_err() {
        return;
    }
    let expected = match text.as_str() {
        "NaN" | "Infinity" | "-Infinity" => text.clone(),
        number => normal_form(number),
    };
    assert_eq!(decode(&again).unwrap(), expected, "{raw:?}");
}

#[test]
fn any_wire_decodes_to_text_that_reads_back_or_is_refused() {
    bolero::check!().with_iterations(0).for_each(decode_any);
}
