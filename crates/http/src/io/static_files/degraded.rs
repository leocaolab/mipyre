//! Counts what static file serving gave up on, for the engine's stats to read. Why one mount is
//! served uncached is held by that mount (`StaticMount::uncached_because`).

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StaticDegradation {
    /// A pre-compressed copy could not be read, so the original was served instead.
    PrecompressedFallback,
    /// A mount lost its file watch or never got one, so it is served uncached.
    UnwatchedMount,
    /// An answer sent from disk was cut off: its file changed, ended early or could not be read
    /// while it was sent.
    AnswerCut,
}

static PRECOMPRESSED_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static UNWATCHED_MOUNTS: AtomicU64 = AtomicU64::new(0);
static ANSWERS_CUT: AtomicU64 = AtomicU64::new(0);

fn counter(kind: StaticDegradation) -> &'static AtomicU64 {
    match kind {
        StaticDegradation::PrecompressedFallback => &PRECOMPRESSED_FALLBACKS,
        StaticDegradation::UnwatchedMount => &UNWATCHED_MOUNTS,
        StaticDegradation::AnswerCut => &ANSWERS_CUT,
    }
}

pub(super) fn count(kind: StaticDegradation) {
    counter(kind).fetch_add(1, Ordering::Relaxed);
}

pub fn total(kind: StaticDegradation) -> u64 {
    counter(kind).load(Ordering::Relaxed)
}
