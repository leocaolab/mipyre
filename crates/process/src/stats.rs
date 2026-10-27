//! The lanes' counters: what the hosts and the workers counted, and one read of them for `stats()`.

use super::host::LaneRunError;
use super::pid::Pid;
use super::run::{RunPhase, ServerLanes};
use crate::sync::{AtomicU64, Mutex, Ordering::*};
use mipyre_shm::channel::WaitTotals;

/// Caps the respawn latencies kept for `stats()` to the most recent ones, so a server that
/// loses workers for months does not grow by them.
pub(crate) const RESPAWN_SAMPLES: usize = 10_000;

/// Counters a test or a bench reads (`ProcessLanes.stats()`).
#[derive(Default)]
pub(crate) struct Stats {
    pub(crate) deaths: AtomicU64,
    pub(crate) timeouts_killed: AtomicU64,
    /// Counts the workers killed for writing a lane counter the protocol cannot produce.
    pub(crate) peer_faults: AtomicU64,
    pub(crate) respawns: AtomicU64,
    pub(crate) respawn_failures: AtomicU64,
    /// Replacement fork asked → new worker ready, per respawn, in microseconds (the fork is
    /// asked when the death is seen, or once a crash-looping lane's wait is over).
    pub(crate) respawn_us: Mutex<Vec<u64>>,
    pub(crate) host_failures: AtomicU64,
}

/// The lanes' counters as one read of them found them; `respawn_us` is in microseconds.
pub struct StatsSnapshot {
    pub deaths: u64,
    pub timeouts_killed: u64,
    pub peer_faults: u64,
    pub respawns: u64,
    pub respawn_failures: u64,
    pub respawn_us: Vec<u64>,
    pub worker_pids: Vec<Option<Pid>>,
    pub requests_in_lanes: usize,
    pub host_waits: WaitTotals,
    pub worker_waits: WaitTotals,
    pub host_events: HostEvents,
    pub host_failures: u64,
    pub lanes_broken: usize,
    pub fatal: Option<LaneRunError>,
}

impl StatsSnapshot {
    /// Counts the host waits that ended in the spin, without parking. A host may be adding while the counters are
    /// read one by one, so parks can be up to one per host ahead of waits; the subtraction saturates for that.
    pub fn park_spin_hits(&self) -> u64 {
        self.host_waits.waits.saturating_sub(self.host_waits.parks)
    }
}

/// Holds a counter whose one writer adds with a plain load and store instead of a
/// read-modify-write; others only read it.
#[derive(Default)]
pub(crate) struct OwnCounter(AtomicU64);

impl OwnCounter {
    /// Adds `n`; only the counter's one writer may call it.
    #[inline]
    pub(crate) fn add(&self, n: u64) {
        self.0.store(self.0.load(Relaxed).wrapping_add(n), Relaxed);
    }

    fn read(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

/// Counts how a host waited for its lanes' answers: each spin before its thread parks.
#[derive(Default)]
pub(crate) struct HostWaitCounts {
    pub(crate) waits: OwnCounter,
    pub(crate) spin_rounds: OwnCounter,
    pub(crate) parks: OwnCounter,
    /// Counts the times a publish woke the lane's sleeping worker.
    pub(crate) wakes: OwnCounter,
}

impl HostWaitCounts {
    fn read(&self) -> WaitTotals {
        WaitTotals {
            waits: self.waits.read(),
            spin_rounds: self.spin_rounds.read(),
            parks: self.parks.read(),
            wakes: self.wakes.read(),
        }
    }
}

/// What one host counted: how it waited for its lanes' answers, where it read them, and how
/// often its thread's park hook ran. The host's thread is the only writer ([`OwnCounter`]).
#[derive(Default)]
pub(crate) struct HostCounts {
    pub(crate) waits: HostWaitCounts,
    /// Counts answers read by a submit on this thread, the busy path.
    pub(crate) read_by_submit: OwnCounter,
    /// Counts answers read after the spin before the thread parks
    /// ([`LaneHost::spin_for_next_request`](super::host::LaneHost::spin_for_next_request)).
    pub(crate) read_by_spin: OwnCounter,
    /// Counts answers read by the poller task ([`LaneHost::run`](super::host::LaneHost::run)).
    pub(crate) read_by_poller: OwnCounter,
    pub(crate) park_hooks: OwnCounter,
    /// Counts the times the poller went to sleep on the wake pipe, and the times a worker's byte
    /// woke it.
    pub(crate) sleeps: OwnCounter,
    pub(crate) pipe_wakes: OwnCounter,
    /// Counts requests this host's thread handed to a less loaded host while its handlers ran long.
    pub(crate) spread_forwards: OwnCounter,
    /// Counts requests this host's thread forwarded to another host because its own lanes were full.
    pub(crate) overflow_forwards: OwnCounter,
    /// Counts queued or forwarded requests this host dropped unrun because nobody waited for them.
    pub(crate) unawaited_skips: OwnCounter,
}

/// How often each thing [`HostCounts`] counts, besides its waits, happened on a host, or on all of
/// them summed.
#[derive(Clone, Copy, Debug, Default)]
pub struct HostEvents {
    pub by_submit: u64,
    pub by_spin: u64,
    pub by_poller: u64,
    pub park_hooks: u64,
    pub sleeps: u64,
    pub pipe_wakes: u64,
    pub spread_forwards: u64,
    pub overflow_forwards: u64,
    pub unawaited_skips: u64,
}

impl HostCounts {
    pub(crate) fn events(&self) -> HostEvents {
        HostEvents {
            by_submit: self.read_by_submit.read(),
            by_spin: self.read_by_spin.read(),
            by_poller: self.read_by_poller.read(),
            park_hooks: self.park_hooks.read(),
            sleeps: self.sleeps.read(),
            pipe_wakes: self.pipe_wakes.read(),
            spread_forwards: self.spread_forwards.read(),
            overflow_forwards: self.overflow_forwards.read(),
            unawaited_skips: self.unawaited_skips.read(),
        }
    }
}

impl HostEvents {
    fn plus(self, b: HostEvents) -> HostEvents {
        HostEvents {
            by_submit: self.by_submit.wrapping_add(b.by_submit),
            by_spin: self.by_spin.wrapping_add(b.by_spin),
            by_poller: self.by_poller.wrapping_add(b.by_poller),
            park_hooks: self.park_hooks.wrapping_add(b.park_hooks),
            sleeps: self.sleeps.wrapping_add(b.sleeps),
            pipe_wakes: self.pipe_wakes.wrapping_add(b.pipe_wakes),
            spread_forwards: self.spread_forwards.wrapping_add(b.spread_forwards),
            overflow_forwards: self.overflow_forwards.wrapping_add(b.overflow_forwards),
            unawaited_skips: self.unawaited_skips.wrapping_add(b.unawaited_skips),
        }
    }
}

pub(crate) fn sum_waits(counts: impl Iterator<Item = WaitTotals>) -> WaitTotals {
    counts.fold(WaitTotals::default(), |a, b| WaitTotals {
        waits: a.waits.wrapping_add(b.waits),
        spin_rounds: a.spin_rounds.wrapping_add(b.spin_rounds),
        parks: a.parks.wrapping_add(b.parks),
        wakes: a.wakes.wrapping_add(b.wakes),
    })
}

impl ServerLanes {
    /// Returns the lanes that crash-loop now, as `(lane, why)`.
    pub fn crash_looping(&self) -> Vec<(usize, String)> {
        self.crash_loops
            .iter()
            .enumerate()
            .filter_map(|(i, why)| why.lock().clone().map(|why| (i, why)))
            .collect()
    }

    /// Records why lane `i` crash-loops, or with `None` that it no longer does.
    pub(super) fn set_crash_loop(&self, i: usize, why: Option<String>) {
        if let Some(slot) = self.crash_loops.get(i) {
            *slot.lock() = why;
        }
    }

    pub(crate) fn host_waits_total(&self) -> WaitTotals {
        sum_waits(self.host_counts.iter().map(|c| c.0.waits.read()))
    }

    pub(crate) fn host_events_total(&self) -> HostEvents {
        self.host_counts
            .iter()
            .map(|c| c.0.events())
            .fold(HostEvents::default(), HostEvents::plus)
    }

    pub(crate) fn worker_waits_total(&self) -> WaitTotals {
        let ended = *self.ended_peer_waits.lock();
        match &*self.run.read() {
            RunPhase::Serving(run) => {
                sum_waits(std::iter::once(ended).chain(run.channels.iter().map(|c| c.peer_waits())))
            }
            RunPhase::Idle | RunPhase::Starting => ended,
        }
    }

    /// Counts the requests in the current run's lanes and queued behind them, as each host last
    /// published it after a submit or a read; none while no run serves.
    pub(crate) fn requests_in_lanes(&self) -> usize {
        match &*self.run.read() {
            RunPhase::Serving(run) => run.hosts.iter().map(|h| h.load.load(Relaxed)).sum(),
            RunPhase::Idle | RunPhase::Starting => 0,
        }
    }

    /// Returns why the lanes ended the server, if they did.
    pub(crate) fn fatal(&self) -> Option<LaneRunError> {
        if !self.health.lock().stopped() {
            return None;
        }
        self.first_failure.lock().clone()
    }

    /// Counts the current run's lanes that have no worker for good.
    pub(crate) fn broken_lanes(&self) -> usize {
        self.health.lock().broken()
    }

    pub fn worker_pids(&self) -> Vec<Option<Pid>> {
        self.processes
            .iter()
            .map(|p| p.lock().as_ref().map(|w| w.pid()))
            .collect()
    }

    /// Reads the lanes' counters, each once.
    pub fn stats_snapshot(&self) -> StatsSnapshot {
        let s = &self.stats;
        StatsSnapshot {
            deaths: s.deaths.load(Relaxed),
            timeouts_killed: s.timeouts_killed.load(Relaxed),
            peer_faults: s.peer_faults.load(Relaxed),
            respawns: s.respawns.load(Relaxed),
            respawn_failures: s.respawn_failures.load(Relaxed),
            respawn_us: s.respawn_us.lock().clone(),
            worker_pids: self.worker_pids(),
            requests_in_lanes: self.requests_in_lanes(),
            host_waits: self.host_waits_total(),
            worker_waits: self.worker_waits_total(),
            host_events: self.host_events_total(),
            host_failures: s.host_failures.load(Relaxed),
            lanes_broken: self.broken_lanes(),
            fatal: self.fatal(),
        }
    }
}
