//! Counts what the process gave up on and lists the components it now runs without, with why.
//!
//! A component that cannot do its job degrades while the server keeps serving, and the stats
//! show the list, so running degraded is visible and not only a log line. What one server run
//! gives up on is counted in `run_counters`; the socket and static-file degradations are counted
//! in `mipyre_http` and read in `monitor::read_counters`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use mipyre_util::throttle::LogThrottle;

/// Counts async database results that could not reach their asyncio loop because it was closed.
static DB_RESULTS_UNDELIVERABLE: AtomicU64 = AtomicU64::new(0);
static GIL_CONGESTED_WAITS: AtomicU64 = AtomicU64::new(0);
/// Holds why the server's CPU count is a fallback; it stays unset when the OS gave the counts.
static CPU_SIZING_FALLBACK: OnceLock<String> = OnceLock::new();

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

pub(crate) fn count_gil_congested_wait() {
    GIL_CONGESTED_WAITS.fetch_add(1, Ordering::Relaxed);
}

/// Records why the server is sized from a fallback; the first reason stands.
pub(crate) fn record_cpu_sizing_fallback(reason: impl FnOnce() -> String) {
    CPU_SIZING_FALLBACK.get_or_init(reason);
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Component {
    CpuPinning,
    RssSampling,
    #[cfg(feature = "leak_detect")]
    LeakDetect,
    /// The process lane with this index is crash-looping.
    ProcessLane(usize),
    StaticWatch(std::path::PathBuf),
}

impl std::fmt::Display for Component {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Component::CpuPinning => f.write_str("cpu_pinning"),
            Component::RssSampling => f.write_str("rss_sampling"),
            #[cfg(feature = "leak_detect")]
            Component::LeakDetect => f.write_str("leak_detect"),
            Component::ProcessLane(i) => write!(f, "process_lane:{i}"),
            Component::StaticWatch(root) => write!(f, "static_watch:{}", root.display()),
        }
    }
}

static DEGRADED: parking_lot::Mutex<BTreeMap<Component, String>> =
    parking_lot::Mutex::new(BTreeMap::new());

/// Marks `component` degraded for `reason`, replacing any previous reason.
pub(crate) fn set_degraded(component: Component, reason: impl Into<String>) {
    DEGRADED.lock().insert(component, reason.into());
}

/// Clears `component` once it runs as configured again or has stopped.
pub(crate) fn clear_degraded(component: &Component) {
    DEGRADED.lock().remove(component);
}

/// Returns every degraded component as `component: reason` in component order, including the
/// static mounts `mipyre_http` serves uncached.
pub(crate) fn degraded_list() -> Vec<String> {
    let mut all = DEGRADED.lock().clone();
    all.extend(
        mipyre_http::io::static_files::degraded::uncached_mounts()
            .into_iter()
            .map(|(root, reason)| (Component::StaticWatch(root), reason)),
    );
    all.iter()
        .map(|(component, reason)| format!("{component}: {reason}"))
        .collect()
}

pub(crate) struct Degradations {
    pub(crate) db_results_undeliverable: u64,
    pub(crate) gil_congested_waits: u64,
    pub(crate) cpu_sizing_fallback: Option<String>,
}

pub(crate) fn read() -> Degradations {
    Degradations {
        db_results_undeliverable: DB_RESULTS_UNDELIVERABLE.load(Ordering::Relaxed),
        gil_congested_waits: GIL_CONGESTED_WAITS.load(Ordering::Relaxed),
        cpu_sizing_fallback: CPU_SIZING_FALLBACK.get().cloned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_degraded_component_is_listed_with_its_reason_until_it_is_cleared() {
        let component = Component::ProcessLane(usize::MAX);
        set_degraded(component.clone(), "the thing failed: why");
        let line = format!("process_lane:{}: the thing failed: why", usize::MAX);
        assert!(degraded_list().contains(&line));
        clear_degraded(&component);
        assert!(!degraded_list().contains(&line));
    }
}
