//! Reads the monotonic clock that [`crate::gate`]'s log throttles run on.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Returns the time since the first call in this process.
pub fn now() -> Duration {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    ORIGIN.get_or_init(Instant::now).elapsed()
}
