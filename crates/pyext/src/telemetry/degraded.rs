//! Counts the async database results the process could not deliver.
//!
//! A component that cannot do its job degrades while the server keeps serving, and says why
//! where it is held: a run's CPU pinning on its `ServerCounters`, a process lane's crash loop on
//! its `ServerLanes`, a static mount's lost watch on the mount, RSS sampling on the sampler
//! (`monitor`), leak detection on its recorder. A run's list is `Server.metrics().degraded`,
//! the process's `get_gil_metrics().degraded`. The socket and static-file degradations are
//! counted in `mipyre_http` and read in `monitor::read_counters`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use mipyre_util::throttle::LogThrottle;

/// Counts async database results that could not reach their asyncio loop because it was closed.
static DB_RESULTS_UNDELIVERABLE: AtomicU64 = AtomicU64::new(0);

/// Limits the warning for a result dropped on a closed loop to one per minute, because at
/// shutdown every query in flight hits it at once.
static CLOSED_LOOP_THROTTLE: LogThrottle = LogThrottle::new(Duration::from_secs(60));

pub(crate) fn count_db_result_undeliverable() {
    DB_RESULTS_UNDELIVERABLE.fetch_add(1, Ordering::Relaxed);
}

/// Admits the warning for a DB result dropped on a closed asyncio loop, returning how many were
/// held back since the last one, or `None` while the warning is held.
pub(crate) fn admit_closed_loop_warning() -> Option<u64> {
    CLOSED_LOOP_THROTTLE.admit(mipyre_util::clock::now())
}

pub(crate) struct Degradations {
    pub(crate) db_results_undeliverable: u64,
}

pub(crate) fn read() -> Degradations {
    Degradations {
        db_results_undeliverable: DB_RESULTS_UNDELIVERABLE.load(Ordering::Relaxed),
    }
}
