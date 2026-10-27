//! Lets a stopped server's open connections finish, within the run's one stop deadline.
//!
//! A thread that sees the stop waits for its connections until the deadline less a teardown
//! margin, and shuts its runtime down by the deadline itself, so the threads that see the stop
//! late still end by the instant the run is joined at.

use std::time::{Duration, Instant};

use mipyre_tpc::ShutdownDeadline;

use crate::ServerCounters;

/// The part of a server's shutdown grace a stopped thread keeps for after its connections
/// drained, to drop its runtime and end its worker. That takes tens of milliseconds, and nears a
/// second when a higher-priority task holds the thread's pinned CPU.
const TEARDOWN_MARGIN: Duration = Duration::from_secs(1);

/// Returns how much of `grace` a stopped thread keeps for its teardown: [`TEARDOWN_MARGIN`], or
/// half a grace under twice the margin.
pub(crate) fn teardown_margin(grace: Duration) -> Duration {
    if grace >= TEARDOWN_MARGIN * 2 {
        TEARDOWN_MARGIN
    } else {
        grace - grace / 2
    }
}

/// Returns how long a thread that sees the stop at `now` waits for its connections: until the
/// run's deadline less the teardown margin.
pub(crate) fn drain_timeout(deadline: &ShutdownDeadline, now: Instant) -> Duration {
    let end = deadline.fix(now);
    // The margin is at most the grace, so this is never before the deadline was fixed.
    (end - teardown_margin(deadline.grace())).saturating_duration_since(now)
}

/// Returns how long a thread whose serve is over at `now` may wait for its runtime's blocking
/// calls: until the run's deadline.
pub(crate) fn teardown_timeout(deadline: &ShutdownDeadline, now: Instant) -> Duration {
    deadline.fix(now).saturating_duration_since(now)
}

/// Counts and logs the `open` connections a stopped accept loop still had when its drain gave up
/// at `timeout`; they are closed as their tasks drop.
pub(crate) fn report_drain_timeout(open: usize, timeout: Duration, counters: &ServerCounters) {
    counters.drain_timeout(open);
    tracing::warn!(
        target: "mipyre::server",
        open,
        "connections did not finish within {timeout:?} of the stop; closing them"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The cases sit just either side of twice the margin, at the default 5 s grace, and below
    /// any margin.
    #[test]
    fn the_margin_is_the_teardown_margin_or_half_a_short_grace() {
        let ns = Duration::from_nanos;
        let s = Duration::from_secs;
        let cases = [
            (s(5), s(1)),
            (s(2) + ns(1), s(1)),
            (s(2), s(1)),
            (s(2) - ns(2), s(1) - ns(1)),
            (Duration::from_millis(200), Duration::from_millis(100)),
            (ns(1), ns(1)),
            (Duration::ZERO, Duration::ZERO),
        ];
        for (grace, margin) in cases {
            assert_eq!(teardown_margin(grace), margin, "grace {grace:?}");
        }
    }

    proptest! {
        /// A drain to the deadline less the margin always leaves the thread time inside the
        /// grace, and never more than the margin.
        #[test]
        fn the_margin_fits_inside_every_grace(secs in 0u64..=u64::MAX, nanos in 0u32..1_000_000_000) {
            let grace = Duration::new(secs, nanos);
            let margin = teardown_margin(grace);
            prop_assert!(margin <= grace);
            prop_assert!(margin <= TEARDOWN_MARGIN);
            prop_assert!(margin >= TEARDOWN_MARGIN.min(grace / 2));
        }
    }

    /// The stop came at `stopped`; a thread that sees it late drains less, so every thread
    /// shuts its runtime down by the one deadline.
    #[test]
    fn a_thread_that_sees_the_stop_late_drains_less_and_ends_by_the_same_deadline() {
        let deadline = ShutdownDeadline::new(Duration::from_secs(5));
        let stopped = Instant::now();
        assert_eq!(drain_timeout(&deadline, stopped), Duration::from_secs(4));
        let late = stopped + Duration::from_millis(1_500);
        assert_eq!(drain_timeout(&deadline, late), Duration::from_millis(2_500));
        assert_eq!(teardown_timeout(&deadline, late), Duration::from_millis(3_500));
        let past = stopped + Duration::from_secs(6);
        assert_eq!(
            (drain_timeout(&deadline, past), teardown_timeout(&deadline, past)),
            (Duration::ZERO, Duration::ZERO)
        );
    }
}
