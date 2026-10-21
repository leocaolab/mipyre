//! Throttles a log line that repeats while its condition lasts to one per period, counting the
//! lines held back.
//!
//! The caller hands in `now` as the time since a fixed origin, so a throttle never reads a clock.
//! [`LogThrottle`] is the lock-free form for one condition; [`ThrottleState`] is the same rule as
//! a plain value, for a caller that keeps many under its own lock.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Returns `time` in nanoseconds, saturating 584 years after the origin.
const fn nanos(time: Duration) -> u64 {
    let nanos = time.as_nanos();
    if nanos > u64::MAX as u128 {
        u64::MAX
    } else {
        nanos as u64
    }
}

/// Returns when the period a line at `now` opens is over, and `None` while the period that is over
/// at `open_at` lasts.
fn reopens_at(open_at: u64, now: u64, period_ns: u64) -> Option<u64> {
    (now >= open_at).then(|| now.saturating_add(period_ns))
}

/// Holds a throttle's period in nanoseconds, converted once where the period is set, so deciding a
/// line never converts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ThrottlePeriod {
    nanos: u64,
}

impl ThrottlePeriod {
    pub const fn new(period: Duration) -> Self {
        ThrottlePeriod { nanos: nanos(period) }
    }
}

/// Throttles one condition with no lock and no allocation.
pub struct LogThrottle {
    period: ThrottlePeriod,
    open_at: AtomicU64,
    held: AtomicU64,
}

impl LogThrottle {
    pub const fn new(period: Duration) -> Self {
        LogThrottle {
            period: ThrottlePeriod::new(period),
            open_at: AtomicU64::new(0),
            held: AtomicU64::new(0),
        }
    }

    /// Returns how many lines were held back since the last logged one when this line should be
    /// logged, and `None` when it is held back.
    ///
    /// A line another thread holds back while this one goes out may be counted in this line
    /// rather than the next: every held line is counted once, but at a period's edge one can land
    /// a line early.
    pub fn admit(&self, now: Duration) -> Option<u64> {
        let now = nanos(now);
        let mut open = self.open_at.load(Ordering::Acquire);
        loop {
            let Some(next) = reopens_at(open, now, self.period.nanos) else {
                self.held.fetch_add(1, Ordering::Relaxed);
                return None;
            };
            match self.open_at.compare_exchange_weak(open, next, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Some(self.held.swap(0, Ordering::Relaxed)),
                Err(current) => open = current,
            }
        }
    }
}

/// Holds one condition's throttle as a plain value, for a caller that already holds a lock and
/// keeps the period, which many such throttles share; it decides as [`LogThrottle`] does.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ThrottleState {
    open_at: u64,
    held: u64,
}

impl ThrottleState {
    pub const fn new() -> Self {
        ThrottleState { open_at: 0, held: 0 }
    }

    /// Returns how many lines were held back since the last logged one when this line should be
    /// logged, and `None` when it is held back.
    pub fn admit(&mut self, now: Duration, period: ThrottlePeriod) -> Option<u64> {
        self.admit_at(nanos(now), period.nanos)
    }

    fn admit_at(&mut self, at: u64, period_ns: u64) -> Option<u64> {
        match reopens_at(self.open_at, at, period_ns) {
            Some(next) => {
                self.open_at = next;
                Some(std::mem::take(&mut self.held))
            }
            None => {
                self.held = self.held.saturating_add(1);
                None
            }
        }
    }

    /// Returns whether a line at `now` would go out, without deciding it.
    pub fn is_over(&self, now: Duration) -> bool {
        nanos(now) >= self.open_at
    }

    /// Returns the lines held back that no line has carried out yet.
    pub fn held(&self) -> u64 {
        self.held
    }

    /// Counts `lines` held back elsewhere into the next line that goes out, as when the condition
    /// they were held for is merged into this one.
    pub fn add_held(&mut self, lines: u64) {
        self.held = self.held.saturating_add(lines);
    }
}

#[cfg(test)]
mod tests;

#[cfg(kani)]
mod proofs {
    use super::*;

    /// Proves that a throttle lets a line out exactly when its period is over, that the line
    /// carries every line held since the last one, and that the next period starts at the line.
    #[kani::proof]
    fn a_throttle_lets_a_line_out_once_its_period_is_over_with_every_held_line() {
        let (open_at, held, at, period_ns): (u64, u64, u64, u64) = kani::any();
        kani::assume(held < u64::MAX);
        let mut throttle = ThrottleState { open_at, held };
        match throttle.admit_at(at, period_ns) {
            Some(carried) => {
                assert!(at >= open_at);
                assert_eq!(carried, held);
                assert_eq!(throttle, ThrottleState { open_at: at.saturating_add(period_ns), held: 0 });
                assert!(throttle.open_at >= open_at);
            }
            None => {
                assert!(at < open_at);
                assert_eq!(throttle, ThrottleState { open_at, held: held + 1 });
            }
        }
    }

    /// Proves that a period converts to nanoseconds exactly below 584 years and saturates above.
    #[kani::proof]
    fn a_period_is_its_nanoseconds_saturating() {
        let (secs, subsec): (u64, u32) = kani::any();
        kani::assume(subsec < 1_000_000_000);
        let period = Duration::new(secs, subsec);
        let exact = u128::from(secs) * 1_000_000_000 + u128::from(subsec);
        let converted = nanos(period);
        if exact <= u128::from(u64::MAX) {
            assert_eq!(u128::from(converted), exact);
        } else {
            assert_eq!(converted, u64::MAX);
        }
    }
}
