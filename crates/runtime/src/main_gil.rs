//! Measures the main interpreter's GIL as the handler calls that take it on the real request path,
//! so measuring adds no contention of its own.

use std::sync::atomic::{AtomicIsize, AtomicU64, Ordering};
use std::time::Duration;

use mipyre_util::clock;
use mipyre_util::throttle::LogThrottle;

/// A GIL wait longer than this is congestion.
const GIL_CONGESTED_WARN_US: u64 = 50_000;

/// The main interpreter's GIL as the handler calls that take it measure it, on the real request
/// path. CPython has one main interpreter per process, so this is one lock for every server in
/// the process: two servers' bridges (and a multi-thread server's blocking calls) wait on the same
/// GIL, and these are that lock's facts, not any one server's.
pub struct MainGil {
    last_wait_us: AtomicU64,
    /// The peak lasts until [`MainGil::reset_peaks`].
    peak_wait_us: AtomicU64,
    /// Counts the handler calls that acquired the GIL.
    waits: AtomicU64,
    total_wait_us: AtomicU64,
    /// Handler calls waiting for the GIL now.
    queue_length: AtomicIsize,
    /// The peak handler GIL hold lasts until [`MainGil::reset_peaks`].
    peak_hold_us: AtomicU64,
    /// Waits past [`GIL_CONGESTED_WARN_US`].
    congested_waits: AtomicU64,
    /// Congestion logs one line a minute, because under load every request past the threshold
    /// would.
    congested_log: LogThrottle,
}

/// The main GIL's counts, read at one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MainGilCounts {
    pub last_wait_us: u64,
    pub peak_wait_us: u64,
    pub waits: u64,
    pub total_wait_us: u64,
    pub queue_length: isize,
    pub peak_hold_us: u64,
    pub congested_waits: u64,
}

/// The process's main interpreter's GIL, one per process as CPython has it.
pub static MAIN_GIL: MainGil = MainGil::new();

impl MainGil {
    const fn new() -> Self {
        MainGil {
            last_wait_us: AtomicU64::new(0),
            peak_wait_us: AtomicU64::new(0),
            waits: AtomicU64::new(0),
            total_wait_us: AtomicU64::new(0),
            queue_length: AtomicIsize::new(0),
            peak_hold_us: AtomicU64::new(0),
            congested_waits: AtomicU64::new(0),
            congested_log: LogThrottle::new(Duration::from_secs(60)),
        }
    }

    /// Counts a handler call that starts waiting for the GIL.
    #[inline]
    pub(crate) fn queued(&self) {
        self.queue_length.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts a handler call that got the GIL after waiting `wait_us`.
    #[inline]
    pub(crate) fn acquired(&self, wait_us: u64) {
        self.queue_length.fetch_sub(1, Ordering::Relaxed);
        self.record_wait(wait_us);
    }

    /// Records how long a handler call held the GIL.
    #[inline]
    pub(crate) fn released(&self, hold_us: u64) {
        self.peak_hold_us.fetch_max(hold_us, Ordering::Relaxed);
    }

    /// Records one GIL acquisition wait. A wait past [`GIL_CONGESTED_WARN_US`] is counted and
    /// logged at most once a minute, with how many such waits were held back since the last line.
    #[inline]
    fn record_wait(&self, wait_us: u64) {
        self.last_wait_us.store(wait_us, Ordering::Relaxed);
        self.peak_wait_us.fetch_max(wait_us, Ordering::Relaxed);
        self.total_wait_us.fetch_add(wait_us, Ordering::Relaxed);
        self.waits.fetch_add(1, Ordering::Relaxed);

        if wait_us > GIL_CONGESTED_WARN_US {
            self.congested_waits.fetch_add(1, Ordering::Relaxed);
            if let Some(held) = self.congested_log.admit(clock::now()) {
                tracing::warn!(
                    target: "mipyre::server",
                    latency_ms = wait_us / 1000,
                    held,
                    "GIL congested (measured on real request)"
                );
            }
        }
    }

    pub fn read(&self) -> MainGilCounts {
        MainGilCounts {
            last_wait_us: self.last_wait_us.load(Ordering::Relaxed),
            peak_wait_us: self.peak_wait_us.load(Ordering::Relaxed),
            waits: self.waits.load(Ordering::Relaxed),
            total_wait_us: self.total_wait_us.load(Ordering::Relaxed),
            queue_length: self.queue_length.load(Ordering::Relaxed),
            peak_hold_us: self.peak_hold_us.load(Ordering::Relaxed),
            congested_waits: self.congested_waits.load(Ordering::Relaxed),
        }
    }

    /// Clears the wait and hold peaks, starting a new peak window.
    pub fn reset_peaks(&self) {
        self.peak_wait_us.store(0, Ordering::Relaxed);
        self.peak_hold_us.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wait_is_congestion_only_past_the_threshold() {
        let gil = MainGil::new();
        gil.record_wait(GIL_CONGESTED_WARN_US);
        assert_eq!(gil.read().congested_waits, 0);
        gil.record_wait(GIL_CONGESTED_WARN_US + 1);
        assert_eq!(gil.read().congested_waits, 1);
    }

    #[test]
    fn waits_add_up_and_the_peaks_hold_until_reset() {
        let gil = MainGil::new();
        gil.record_wait(30);
        gil.record_wait(10);
        gil.released(7);
        let counts = gil.read();
        assert_eq!(
            (
                counts.last_wait_us,
                counts.peak_wait_us,
                counts.waits,
                counts.total_wait_us,
                counts.peak_hold_us
            ),
            (10, 30, 2, 40, 7)
        );
        gil.reset_peaks();
        let counts = gil.read();
        assert_eq!((counts.peak_wait_us, counts.peak_hold_us, counts.waits), (0, 0, 2));
    }
}
