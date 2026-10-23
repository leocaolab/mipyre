//! Holds what one server serves, the app's rules and the run's handles, built once by `start()` and
//! read-only after; also keeps the access log's per-thread sampling roll.

use std::cell::Cell;
use std::num::NonZeroU64;
use std::sync::Arc;

use hyper::StatusCode;

use mipyre_http::plan::route::RouteTable;
use mipyre_http::plan::site::{AccessLog, SiteRules};
use pyo3::{Py, PyAny};

use mipyre_http::io::static_files::StaticMount;

pub(crate) type Routes = RouteTable<Py<PyAny>, StaticMount>;

pub(crate) struct Site {
    /// Held by value, so a serving thread reads its rules without chasing a pointer or sharing a
    /// count.
    pub(crate) rules: SiteRules<Py<PyAny>, StaticMount>,
    /// Counts the WebSocket connections this run has open, against `limits.ws.max_connections`.
    pub(crate) ws_connections: crate::serve::websocket::OpenConnections,
    /// Is present when the app has `runtime="process"` routes.
    pub(crate) process: Option<Arc<crate::runtime::process::ServerLanes>>,
    /// Is shared by every serving thread's copy of the site.
    pub(crate) counters: Arc<crate::telemetry::RunCounters>,
    /// The run's Postgres pools (b/335 D17), opened when it sealed; `None` when its main
    /// interpreter connected to none.
    pub(crate) pg: Option<Arc<crate::runtime::RunPools>>,
    /// Times answers. quanta reads the CPU's counter when it ticks at one rate on every core (an
    /// invariant TSC) and the monotonic clock otherwise; the counter is calibrated once per
    /// process, when the first site is built, before any request is served.
    pub(crate) clock: quanta::Clock,
}

impl Site {
    /// Copies the site for one TPC thread, with every byte a response clones (fast responses,
    /// CORS values, the request-id header name) in the thread's own allocations. A shared `Site`
    /// would have every serving thread touch the same `Bytes` and `Arc` counts on every request;
    /// with its own, a thread's hot path writes only memory no other thread touches. The WebSocket
    /// connection count stays shared because it is one limit for the run.
    pub(crate) fn for_thread(&self, py: pyo3::Python<'_>) -> Site {
        Site {
            rules: crate::runtime::copy_rules(&self.rules, py),
            ws_connections: self.ws_connections.clone(),
            process: self.process.clone(),
            counters: Arc::clone(&self.counters),
            pg: self.pg.clone(),
            clock: self.clock.clone(),
        }
    }
}

thread_local! {
    /// Each thread keeps 1 in N of the responses it serves, so the log keeps 1 in N overall
    /// without every thread incrementing one shared cache line per response.
    static SAMPLE_ROLL: Cell<u64> = const { Cell::new(0) };
}

/// Returns whether `log` writes a line for a response of `status`, with `roll` giving this thread's
/// next place in the sample. The status floor is checked first, so a response the floor logs takes no
/// place in the sample.
#[inline]
pub(crate) fn samples(log: &AccessLog, status: StatusCode, roll: impl FnOnce() -> u64) -> bool {
    log.always_status.is_some_and(|floor| status >= floor) || in_sample(log.sample_n, roll)
}

/// Returns whether a 1-in-`n` sample keeps the response at the place `roll` gives; `roll` is asked
/// only when `n` is above 1.
#[inline]
pub(crate) fn in_sample(n: NonZeroU64, roll: impl FnOnce() -> u64) -> bool {
    n.get() == 1 || roll().is_multiple_of(n.get())
}

/// Returns this thread's place in the sample and advances it.
#[inline]
pub(crate) fn next_roll() -> u64 {
    SAMPLE_ROLL.with(|roll| {
        let current = roll.get();
        roll.set(current.wrapping_add(1));
        current
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_in(n: u64) -> AccessLog {
        AccessLog {
            sample_n: NonZeroU64::new(n).unwrap(),
            always_status: None,
        }
    }

    #[test]
    fn each_thread_keeps_one_in_n_of_its_own_responses() {
        let log = one_in(10);
        let threads: Vec<_> = (0..4)
            .map(|_| {
                std::thread::spawn(move || {
                    // A fresh thread's roll starts at its own first response, whatever
                    // the other threads did.
                    let first = samples(&log, StatusCode::OK, next_roll);
                    let rest = (0..99).filter(|_| samples(&log, StatusCode::OK, next_roll)).count();
                    (first, rest)
                })
            })
            .collect();
        for t in threads {
            // Of the thread's 100 responses, 0, 10, ..., 90 are kept.
            assert_eq!(t.join().unwrap(), (true, 9));
        }
    }

    proptest::proptest! {
        /// Of any run of `n` responses on one thread at any 1-in-`every`, exactly ceil(n / every)
        /// are kept, the first among them; the rolls are 0, 1, 2, … in order.
        #[test]
        fn a_thread_keeps_exactly_ceil_n_over_every_of_its_responses(n in 0u64..3000, every in 1u64..64) {
            let log = one_in(every);
            let decisions: Vec<bool> = (0..n).map(|roll| samples(&log, StatusCode::OK, || roll)).collect();
            let kept = decisions.iter().filter(|kept| **kept).count();
            proptest::prop_assert_eq!(kept as u64, n.div_ceil(every));
            proptest::prop_assert!(decisions.first().copied().unwrap_or(true));
        }
    }

    #[test]
    fn the_status_floor_always_logs() {
        let log = AccessLog {
            always_status: Some(StatusCode::INTERNAL_SERVER_ERROR),
            ..one_in(1000)
        };
        assert!(samples(&log, StatusCode::OK, || 0));
        assert!(!samples(&log, StatusCode::OK, || 1));
        assert!(samples(&log, StatusCode::BAD_GATEWAY, || 1));
    }

    /// A response the floor logs takes no place in the sample (its roll is not asked), and a log
    /// of every response never asks one; just under the floor, the sample decides.
    #[test]
    fn the_roll_is_asked_only_when_the_sample_decides() {
        let floored = AccessLog {
            always_status: Some(StatusCode::INTERNAL_SERVER_ERROR),
            ..one_in(10)
        };
        let asked = Cell::new(0);
        let roll = || {
            asked.set(asked.get() + 1);
            1
        };
        assert!(samples(&floored, StatusCode::INTERNAL_SERVER_ERROR, roll));
        assert_eq!(asked.get(), 0);
        assert!(!samples(&floored, StatusCode::from_u16(499).unwrap(), roll));
        assert_eq!(asked.get(), 1);
        assert!(samples(&AccessLog::EVERY_RESPONSE, StatusCode::OK, roll));
        assert!(in_sample(NonZeroU64::MIN, roll));
        assert_eq!(asked.get(), 1);
    }

    #[test]
    fn one_in_n_keeps_the_rolls_that_are_multiples_of_n() {
        let ten = NonZeroU64::new(10).unwrap();
        assert!(in_sample(ten, || 0));
        assert!(!in_sample(ten, || 9));
        assert!(in_sample(ten, || 10));
        assert!(!in_sample(ten, || 11));
    }
}
