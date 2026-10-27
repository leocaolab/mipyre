//! How a host's thread parks and wakes around its lanes: the park hooks, its lanes' wake pipes, and why its poller
//! stopped.

use std::os::fd::OwnedFd;

use super::host::HOST;
use super::lane_failure::LaneIndex;
use super::os::BellQueue;
use super::run::RunBell;
use futures_util::FutureExt as _;
use mipyre_shm::adapter::doorbell::{drain_once, Drain, DrainOutcome, HostBell};
use mipyre_shm::SyscallError;
use tokio::io::unix::AsyncFd;

/// Why a host's poller stopped reading its lanes while the run serves.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PollerError {
    #[error("the wake pipe of lane {lane} cannot be read: {source}")]
    BellUnreadable { lane: LaneIndex, source: SyscallError },
    #[error("its lanes' wake pipes cannot be waited on: {source}")]
    BellsUnwatchable { source: std::io::Error },
    #[error("the worker-exit watch failed: {0}")]
    ExitWatchFailed(std::io::Error),
    /// The poller panicked; this holds the panic's text.
    #[error("{0}")]
    Panicked(String),
}

/// Clears the sleeping flag as tokio's `on_thread_unpark` hook: while the thread is awake its
/// own submits and its next `enter_idle` read the answers, so workers need not write the wake
/// pipe.
pub fn resume_from_idle() {
    let host = HOST.with(|h| h.borrow().clone());
    if let Some(host) = host {
        host.wake();
    }
}

/// Runs [`LaneHost::spin_for_next_request`](super::host::LaneHost::spin_for_next_request) as tokio's `on_thread_park` hook on a thread that
/// may host lanes. Tokio calls it only when the thread has nothing ready, so the spin delays
/// no task.
pub fn enter_idle() {
    let host = HOST.with(|h| h.borrow().clone());
    if let Some(host) = host {
        host.spin_for_next_request();
    }
}

/// A host's lanes' wake pipes, in one queue this thread's reactor watches, and the write ends the
/// host keeps so no pipe reads as end of file ([`RunBell`]).
pub(crate) struct HostBellWatch {
    pub(crate) fd: AsyncFd<BellQueue>,
    _writes: Vec<OwnedFd>,
}

impl HostBellWatch {
    /// Registers each of `bells` in one queue, under its lane's place among the host's lanes, and
    /// the queue in this thread's reactor.
    pub(crate) fn new(bells: Vec<RunBell>) -> std::io::Result<Self> {
        let (reads, writes): (Vec<HostBell>, Vec<OwnedFd>) =
            bells.into_iter().map(|RunBell { bell, write }| (bell, write)).unzip();
        let queue =
            BellQueue::new(reads).map_err(|e| std::io::Error::new(e.kind(), format!("the wake pipes' queue: {e}")))?;
        let fd = queue
            .registered()
            .map_err(|e| std::io::Error::new(e.kind(), format!("registering the wake pipes: {e}")))?;
        Ok(HostBellWatch { fd, _writes: writes })
    }
}

/// How many lanes' pipes one look at a host's queue reports; the rest are reported by the next.
pub(crate) const BELL_EVENTS: usize = 16;

/// How many reads of a pipe the host makes, at most, to clear what a dead worker left in it:
/// 1 MiB, the most an unprivileged process can grow a pipe to on Linux.
const LEFTOVER_READS: usize = 4096;

/// Reads what lane `lane`'s pipe holds, up to [`LEFTOVER_READS`] reads, so its next worker is not
/// held to bytes its predecessor (or anything else holding the write end) wrote. What is written
/// past that counts against the next worker.
pub(crate) fn drain_leftover(lane: LaneIndex, bell: &HostBell) -> Result<(), PollerError> {
    for _ in 0..LEFTOVER_READS {
        if bell_drained(lane, drain_once(bell))?.outcome == DrainOutcome::Empty {
            break;
        }
    }
    Ok(())
}

/// Returns how a host's poller ended: `None` when it stopped because the run did, else the
/// failure that ended it. Its unwind is caught here because a `spawn_local` task that panics
/// only drops its output, and the host's lanes would stay usable with no poller behind them.
/// A panic fails the host like an error does, with its text as the cause.
pub(crate) async fn poller_failure(
    poller: impl std::future::Future<Output = Result<(), PollerError>>,
) -> Option<PollerError> {
    match std::panic::AssertUnwindSafe(poller).catch_unwind().await {
        Ok(Ok(())) => None,
        Ok(Err(failure)) => Some(failure),
        Err(payload) => Some(PollerError::Panicked(mipyre_util::panic_message(&*payload))),
    }
}

/// Turns waiting on the host's wake pipe into its ready guard or the host's failure. A pipe
/// that cannot be waited on ends the poller, because going round again would find it failing
/// at once and spin a core with nobody reading the lanes.
#[inline]
pub(crate) fn woken_pipe<G>(waited: std::io::Result<G>) -> Result<G, PollerError> {
    waited.map_err(|source| PollerError::BellsUnwatchable { source })
}

/// Turns one read of lane `lane`'s wake pipe into what it took, or the host's failure: a pipe
/// that cannot be read would fail the same way at once, with nobody reading the lanes.
#[inline]
pub(crate) fn bell_drained(lane: LaneIndex, drained: Result<Drain, SyscallError>) -> Result<Drain, PollerError> {
    drained.map_err(|source| PollerError::BellUnreadable { lane, source })
}
