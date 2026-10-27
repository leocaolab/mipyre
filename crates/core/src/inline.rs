//! Times a handler run on the calling thread and judges its budget once it returns. No watchdog can
//! interrupt the call, so a handler that ran past the budget is not stopped, only not answered.
//!
//! The timer never takes the request or the reply: they stay on the caller's stack, built in place
//! and consumed there, because a large value passed by value through a function that can unwind is
//! copied first.

use std::time::Duration;

use tokio::time::Instant;

use mipyre_http::plan::limits::RequestBudget;

/// The moment a call on the calling thread started, by tokio's clock: the std clock, unless a
/// runtime has paused it, as the counting bench does.
#[derive(Debug)]
#[must_use]
pub struct InlineTimer(Instant);

impl InlineTimer {
    /// Reads the clock as the call starts.
    #[inline]
    pub fn start() -> Self {
        Self(Instant::now())
    }

    /// Returns how long the call took, read off the clock now, when that was past `budget`.
    ///
    /// It borrows the timer rather than consuming it: the call site's code is then the same as
    /// reading `Instant::now()` and `elapsed()` by hand, the reading stored once when it is taken.
    /// Taken by value, the reading was kept in registers across the call and stored after it.
    #[inline]
    #[must_use]
    pub fn overran(&self, budget: RequestBudget) -> Option<Duration> {
        past_budget(self.0.elapsed(), budget)
    }
}

/// Returns `took` when it is past `budget`; taking exactly the budget is in time.
#[inline]
fn past_budget(took: Duration, budget: RequestBudget) -> Option<Duration> {
    (took > budget.get()).then_some(took)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget() -> RequestBudget {
        RequestBudget::from_secs(1.0).unwrap()
    }

    #[test]
    fn a_call_a_nanosecond_under_its_budget_is_in_time() {
        assert_eq!(
            past_budget(Duration::from_secs(1) - Duration::from_nanos(1), budget()),
            None
        );
    }

    #[test]
    fn a_call_that_took_its_budget_is_in_time() {
        assert_eq!(past_budget(Duration::from_secs(1), budget()), None);
    }

    #[test]
    fn a_call_a_nanosecond_past_its_budget_overran_by_its_whole_time() {
        let took = Duration::from_secs(1) + Duration::from_nanos(1);
        assert_eq!(past_budget(took, budget()), Some(took));
    }
}
