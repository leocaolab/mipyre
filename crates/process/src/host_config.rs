//! How the server's hosts run their lanes, checked once when the lanes are made.

use std::num::NonZeroUsize;
use std::time::Duration;

use super::health::{CrashBackoff, FailurePolicy};
use mipyre_shm::plan::geometry::Geometry;

/// How the server's hosts run their lanes. Made only by [`HostConfig::new`], which checks that the queue holds at
/// least one lane's depth.
#[derive(Clone, Copy, Debug)]
pub(crate) struct HostConfig {
    /// A handler running longer than this is killed with its worker (504).
    timeout: Duration,
    /// Requests per lane, in the lane and queued behind it, before 503; at least the lane
    /// depth.
    queue_per_worker: NonZeroUsize,
    /// How long a host spins on its lanes, in nanoseconds, before its thread parks.
    spin_ns: u64,
    on_failure: FailurePolicy,
    crash_backoff: CrashBackoff,
    /// How long a run's start waits for its workers to report ready (they import the app).
    worker_start_timeout: Duration,
    /// How long a host waits for the template to report a dead worker's wait status.
    exit_status_wait: Duration,
}

/// Why [`HostConfig::new`] refused.
#[derive(Debug, thiserror::Error)]
pub enum HostConfigError {
    #[error("queue_per_worker ({queue_per_worker}) must be at least the lane depth ({depth})")]
    QueueShorterThanLane { queue_per_worker: usize, depth: u32 },
}

impl HostConfig {
    pub(crate) fn new(
        geometry: &Geometry,
        timeout: Duration,
        queue_per_worker: usize,
        server_spin: Duration,
        on_failure: FailurePolicy,
    ) -> Result<Self, HostConfigError> {
        let depth = geometry.depth().get();
        let Some(queue_per_worker) = NonZeroUsize::new(queue_per_worker).filter(|q| q.get() >= depth as usize) else {
            return Err(HostConfigError::QueueShorterThanLane {
                queue_per_worker,
                depth,
            });
        };
        Ok(HostConfig {
            timeout,
            queue_per_worker,
            // A spin longer than u64 nanoseconds hold (584 years) saturates and never ends.
            spin_ns: u64::try_from(server_spin.as_nanos()).unwrap_or(u64::MAX),
            on_failure,
            crash_backoff: CrashBackoff::DEFAULT,
            worker_start_timeout: WORKER_START_TIMEOUT,
            exit_status_wait: EXIT_STATUS_WAIT,
        })
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) fn queue_per_worker(&self) -> NonZeroUsize {
        self.queue_per_worker
    }

    pub(crate) fn spin_ns(&self) -> u64 {
        self.spin_ns
    }

    pub(crate) fn on_failure(&self) -> FailurePolicy {
        self.on_failure
    }

    pub(crate) fn crash_backoff(&self) -> &CrashBackoff {
        &self.crash_backoff
    }

    pub(crate) fn worker_start_timeout(&self) -> Duration {
        self.worker_start_timeout
    }

    pub(crate) fn exit_status_wait(&self) -> Duration {
        self.exit_status_wait
    }

    /// Returns how many forwarded requests a host with `lanes` lanes holds in its inbox: as many as
    /// its lanes hold. More would mostly wait only to be refused once the host takes them.
    pub(crate) fn inbox_capacity(&self, lanes: NonZeroUsize) -> NonZeroUsize {
        lanes.saturating_mul(self.queue_per_worker)
    }
}

/// How long a host waits for the template to report a dead worker's wait status, unless its
/// [`HostConfig`] says otherwise.
const EXIT_STATUS_WAIT: Duration = Duration::from_millis(100);

/// How long a run's start waits for its workers to report ready (they import the app), unless
/// its [`HostConfig`] says otherwise.
const WORKER_START_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(test)]
#[cfg(not(sync_model))]
pub(crate) mod tests {
    use super::*;

    /// Returns `config` with lanes that crash-loop backing off as `crash_backoff` says, for a test
    /// that cannot wait out the default.
    pub(crate) fn with_crash_backoff(config: HostConfig, crash_backoff: CrashBackoff) -> HostConfig {
        HostConfig {
            crash_backoff,
            ..config
        }
    }
}
