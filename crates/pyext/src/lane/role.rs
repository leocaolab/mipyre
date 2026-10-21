//! Records whether this process is a process-lane worker, so features a worker cannot offer
//! (`SharedState`, `PgPool`) can refuse.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};

static LANE_WORKER: AtomicBool = AtomicBool::new(false);

/// Marks this process a process-lane worker; the worker calls it once, before it serves.
pub(in crate::lane) fn mark_lane_worker() {
    LANE_WORKER.store(true, SeqCst);
}

pub(crate) fn is_lane_worker() -> bool {
    LANE_WORKER.load(Relaxed)
}

#[derive(Debug, thiserror::Error)]
#[error(
    "{what} is not available in a runtime='process' route (process lanes v1): the worker is a \
     forked process, and its copy of the server's state would silently diverge. Use a \
     sub-interpreter or runtime='main' route for it."
)]
pub(crate) struct NotInLaneWorker {
    what: &'static str,
}

/// Refuses `what` when this process is a process-lane worker.
///
/// # Errors
///
/// Returns [`NotInLaneWorker`] in a process-lane worker.
pub(crate) fn refuse_in_lane_worker(what: &'static str) -> Result<(), NotInLaneWorker> {
    if is_lane_worker() {
        return Err(NotInLaneWorker { what });
    }
    Ok(())
}
