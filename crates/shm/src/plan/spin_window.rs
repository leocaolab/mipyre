//! Decides how long one side of the channel spins before it parks while it waits for the other.
//!
//! The caller passes the clock in, so the decision reads no clock of its own and can be tested on both sides of
//! its deadline.

/// The waiter looks at the clock once per this many empty polls: a clock read costs more than a poll, and the
/// window is far longer than this many polls.
pub const CLOCK_EVERY: u32 = 32;

/// A spin deadline on the caller's monotonic clock, in nanoseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpinWindow {
    deadline_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpinStep {
    Spin,
    Park,
}

impl SpinWindow {
    #[inline]
    pub const fn until(deadline_ns: u64) -> Self {
        SpinWindow { deadline_ns }
    }

    /// Returns `Park` once the clock is at the deadline; it calls `now` only when `rounds` is a multiple of
    /// [`CLOCK_EVERY`], including 0, so a window already over is never spun in.
    #[inline]
    pub fn step(self, rounds: u32, now: impl FnOnce() -> u64) -> SpinStep {
        if rounds.is_multiple_of(CLOCK_EVERY) && now() >= self.deadline_ns {
            SpinStep::Park
        } else {
            SpinStep::Spin
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn between_clock_looks_it_spins_without_reading_the_clock() {
        let w = SpinWindow::until(1_000);
        for rounds in 1..CLOCK_EVERY {
            let step = w.step(rounds, || panic!("read the clock at poll {rounds}"));
            assert_eq!(step, SpinStep::Spin);
        }
    }

    #[test]
    fn at_a_clock_look_it_parks_from_the_deadline_on() {
        let w = SpinWindow::until(1_000);
        assert_eq!(w.step(CLOCK_EVERY, || 999), SpinStep::Spin);
        assert_eq!(w.step(CLOCK_EVERY, || 1_000), SpinStep::Park);
        assert_eq!(w.step(2 * CLOCK_EVERY, || 1_001), SpinStep::Park);
    }

    #[test]
    fn before_spinning_it_looks_at_the_clock() {
        let w = SpinWindow::until(1_000);
        assert_eq!(w.step(0, || 1_000), SpinStep::Park);
        assert_eq!(w.step(0, || 999), SpinStep::Spin);
    }
}
