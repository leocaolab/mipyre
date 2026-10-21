//! Spins a lane's host or worker until the other side's counter moves or the spin window is over;
//! the window itself ([`SpinWindow`]) is the channel's decision.

use mipyre_shm::plan::spin_window::{SpinStep, SpinWindow};

/// Records how a spin ended; `ready` is `false` when the window ran out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SpinEnd {
    pub(crate) rounds: u32,
    pub(crate) ready: bool,
}

/// Polls `ready` until it returns `true` or `window` is over; the caller handles what was found
/// after the spin, so the loop stays a bare readiness check.
#[inline(always)]
pub(crate) fn spin(
    window: SpinWindow,
    mut now: impl FnMut() -> u64,
    mut ready: impl FnMut() -> bool,
) -> SpinEnd {
    if window.step(0, &mut now) == SpinStep::Park {
        return SpinEnd {
            rounds: 0,
            ready: ready(),
        };
    }
    let mut rounds = 0u32;
    loop {
        if ready() {
            return SpinEnd {
                rounds,
                ready: true,
            };
        }
        rounds = rounds.wrapping_add(1);
        if window.step(rounds, &mut now) == SpinStep::Park {
            return SpinEnd {
                rounds,
                ready: false,
            };
        }
        std::hint::spin_loop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_spin_counts_the_empty_polls_before_the_ready_one() {
        let mut polls = 0;
        let spun = spin(
            SpinWindow::until(u64::MAX),
            || 0,
            || {
                polls += 1;
                polls == 5
            },
        );
        assert_eq!(
            spun,
            SpinEnd {
                rounds: 4,
                ready: true
            }
        );
    }

    #[test]
    fn a_spin_that_finds_nothing_ends_at_the_deadline() {
        let mut clock = [0, 99, 100].into_iter();
        let spun = spin(SpinWindow::until(100), || clock.next().unwrap(), || false);
        assert_eq!(
            spun,
            SpinEnd {
                rounds: 2 * mipyre_shm::plan::spin_window::CLOCK_EVERY,
                ready: false
            }
        );
    }

    #[test]
    fn a_window_already_over_polls_once() {
        assert_eq!(
            spin(SpinWindow::until(5), || 5, || true),
            SpinEnd {
                rounds: 0,
                ready: true
            }
        );
        assert_eq!(
            spin(SpinWindow::until(5), || 5, || false),
            SpinEnd {
                rounds: 0,
                ready: false
            }
        );
    }
}
