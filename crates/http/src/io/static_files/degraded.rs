//! Counts what static file serving gave up on, for the engine's stats to read.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StaticDegradation {
    /// A pre-compressed copy could not be read, so the original was served instead.
    PrecompressedFallback,
    /// A mount lost its file watch or never got one, so it is served uncached.
    UnwatchedMount,
}

static PRECOMPRESSED_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static UNWATCHED_MOUNTS: AtomicU64 = AtomicU64::new(0);

fn counter(kind: StaticDegradation) -> &'static AtomicU64 {
    match kind {
        StaticDegradation::PrecompressedFallback => &PRECOMPRESSED_FALLBACKS,
        StaticDegradation::UnwatchedMount => &UNWATCHED_MOUNTS,
    }
}

pub(super) fn count(kind: StaticDegradation) {
    counter(kind).fetch_add(1, Ordering::Relaxed);
}

pub fn total(kind: StaticDegradation) -> u64 {
    counter(kind).load(Ordering::Relaxed)
}

/// Maps a mount root to why it is served uncached; it is empty while every mount is watched.
static UNCACHED: parking_lot::Mutex<BTreeMap<PathBuf, String>> =
    parking_lot::Mutex::new(BTreeMap::new());

/// Records why the mount at `root` is served uncached, replacing its previous reason.
pub(super) fn mark_uncached(root: &Path, reason: String) {
    UNCACHED.lock().insert(root.to_path_buf(), reason);
}

pub fn uncached_mounts() -> Vec<(PathBuf, String)> {
    UNCACHED
        .lock()
        .iter()
        .map(|(root, reason)| (root.clone(), reason.clone()))
        .collect()
}
