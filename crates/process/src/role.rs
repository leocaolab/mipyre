//! Records whether this process is a process-lane worker, and of which lane, so features a
//! worker cannot offer (`SharedState`, `PgPool`) can refuse naming it.

use std::sync::OnceLock;

use super::pid::Pid;

use super::lane_failure::LaneIndex;

/// This process is the worker of lane `lane`, forked or spawned for the template `template`.
/// Unset, the process serves no lane (a server, or a plain `import mipyre`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneWorkerProcess {
    pub lane: LaneIndex,
    pub template: Pid,
}

/// Written once, by the worker's `serve()`, after the app is imported and before it serves.
static LANE_WORKER: OnceLock<LaneWorkerProcess> = OnceLock::new();

/// This process already serves a lane: `serve()` runs once per worker process.
#[derive(Debug, thiserror::Error)]
#[error("this process already serves process lane {}, so it cannot serve lane {}", .serving.lane, .asked.lane)]
pub struct AlreadyLaneWorkerError {
    serving: LaneWorkerProcess,
    asked: LaneWorkerProcess,
}

/// Records that this process is the worker of `process`; the worker's `serve()` calls it once.
///
/// # Errors
///
/// Returns [`AlreadyLaneWorkerError`] when the process already serves a lane.
pub fn mark_lane_worker(process: LaneWorkerProcess) -> Result<(), AlreadyLaneWorkerError> {
    LANE_WORKER.set(process).map_err(|asked| AlreadyLaneWorkerError {
        serving: *LANE_WORKER.get().unwrap_or(&asked),
        asked,
    })
}

#[derive(Debug, thiserror::Error)]
#[error(
    "{what} is not available in a runtime='process' route: the route runs in a worker process \
     of its own (process lane {lane}), where {what} would be a copy apart from the server's, \
     silently out of step with it. Use it from a runtime='subinterp' or runtime='main' route."
)]
pub struct NotInLaneWorker {
    what: &'static str,
    lane: LaneIndex,
}

/// Refuses `what` when this process is a process-lane worker, naming its lane.
///
/// # Errors
///
/// Returns [`NotInLaneWorker`] in a process-lane worker.
pub fn refuse_in_lane_worker(what: &'static str) -> Result<(), NotInLaneWorker> {
    match refusal(LANE_WORKER.get(), what) {
        Some(refused) => Err(refused),
        None => Ok(()),
    }
}

/// The refusal of `what` in a process that is `worker`, or `None` in one that serves no lane.
fn refusal(worker: Option<&LaneWorkerProcess>, what: &'static str) -> Option<NotInLaneWorker> {
    worker.map(|worker| NotInLaneWorker {
        what,
        lane: worker.lane,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_process_that_serves_no_lane_refuses_nothing() {
        assert!(refusal(None, "PgPool").is_none());
    }

    #[test]
    fn a_lane_worker_refuses_naming_its_lane() {
        let worker = LaneWorkerProcess {
            lane: LaneIndex(3),
            template: Pid::new(1).expect("1 is a pid"),
        };
        let refused = refusal(Some(&worker), "PgPool").expect("a lane worker refuses");
        assert!(refused
            .to_string()
            .starts_with("PgPool is not available in a runtime='process' route"));
        assert!(refused.to_string().contains("(process lane 3)"), "{refused}");
    }
}
