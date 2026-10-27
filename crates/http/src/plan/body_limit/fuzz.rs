//! The body-limit fuzz target: whatever size a client announces and however it splits the body into frames,
//! the body starts only when the announcement fits the limit, and a frame is taken only while the total stays within
//! it.
//!
//! `cargo test` replays the committed corpus (`__fuzz__/.../corpus`) and nothing else, so the merge runs the same
//! inputs every time; `cargo bolero test -p mipyre_http --lib plan::body_limit::fuzz::...` mutates it under
//! libFuzzer, and an input it finds that fails goes into the corpus as a regression.

use super::{BodySizeLimit, TooLarge};

type Input = (usize, u64, Option<u64>, Vec<u32>);

fn receive(max: usize, lower: u64, exact: Option<u64>, frames: &[u32]) {
    let cap = max as u128;
    let fits = |announced: u64| u128::from(announced) <= cap;
    let started = BodySizeLimit::start(max, lower, exact);
    assert_eq!(
        started.is_ok(),
        fits(lower) && exact.is_none_or(fits),
        "max {max}, at least {lower}, exactly {exact:?}"
    );
    let Ok(limit) = started else {
        return;
    };

    let mut received = 0usize;
    for &len in frames {
        let len = len as usize;
        let sum = received as u128 + len as u128;
        match limit.take(received, len) {
            Ok(now) => {
                assert!(sum <= cap, "{received} + {len} taken past {cap}");
                assert_eq!(now as u128, sum);
                received = now;
            }
            Err(TooLarge) => {
                assert!(sum > cap, "{received} + {len} refused within {cap}");
                return;
            }
        }
    }
}

fn decide((max, lower, exact, frames): &Input) {
    receive(*max, *lower, *exact, frames);
}

#[test]
fn any_announced_size_and_frames_stay_within_the_limit() {
    bolero::check!()
        .with_type::<Input>()
        .with_iterations(0)
        .for_each(decide);
}
