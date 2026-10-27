//! The fuzz target of the answer decoder: the host decodes bytes a
//! worker wrote, and a worker runs user code, so whatever the bytes, the decoder answers or refuses; it never panics.
//!
//! `cargo test` replays the committed corpus (`__fuzz__/.../corpus`) and nothing else, so the merge runs the same
//! inputs every time; `cargo bolero test -p mipyre_process --lib wire_fuzz::...` mutates it under libFuzzer, and
//! an input it finds that fails goes into the corpus as a regression.

use bytes::Bytes;

use super::wire::decode_answer;

#[test]
fn any_answer_bytes_decode_or_are_refused() {
    bolero::check!().with_iterations(0).for_each(|raw: &[u8]| {
        let msg = Bytes::copy_from_slice(raw);
        if let Ok(response) = decode_answer(msg.clone()) {
            assert!(response.body.len() <= msg.len(), "the body is a slice of the message");
        }
    });
}
