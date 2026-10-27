//! Gives an app's process lanes their Python class, `mipyre.engine._LaneSet`.
//!
//! Python has one class for both roles, the server's lanes and one worker's lane, so the role is checked here, on
//! each call; the lanes themselves are two Rust types that never take the other role's calls.

use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::{PyOSError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use mipyre_process::{
    LaneAttachError, LaneIndex, PeerLane, Pid, ServerLanes, StatsSnapshot, TemplateRunError, TemplateSetupError,
    WorkerLane,
};
use mipyre_runtime::CommandLine;

/// Holds an app's process lanes: the server's, or one worker's lane.
#[pyclass(module = "mipyre.engine", name = "_LaneSet", frozen)]
pub(crate) struct PyLaneSet(LaneRole);

enum LaneRole {
    Server(Arc<ServerLanes>),
    Worker(WorkerLane),
}

impl From<ServerLanes> for PyLaneSet {
    fn from(lanes: ServerLanes) -> Self {
        PyLaneSet(LaneRole::Server(Arc::new(lanes)))
    }
}

impl PyLaneSet {
    /// Returns the server's lanes, or raises `RuntimeError` naming `what` on a worker's lane.
    pub(crate) fn server(&self, what: &str) -> PyResult<&Arc<ServerLanes>> {
        match &self.0 {
            LaneRole::Server(lanes) => Ok(lanes),
            LaneRole::Worker(_) => Err(PyRuntimeError::new_err(format!(
                "{what} is the server's: this is a worker's lane"
            ))),
        }
    }

    /// Returns the worker's lane and takes its host's wake pipe, or raises `RuntimeError` naming `what` on the
    /// server's lanes or once the pipe was taken.
    pub(crate) fn peer(&self, what: &str) -> PyResult<(&PeerLane, OwnedFd)> {
        match &self.0 {
            LaneRole::Worker(lane) => lane
                .take_bell()
                .map_err(|spent| PyRuntimeError::new_err(format!("{what}: {spent}"))),
            LaneRole::Server(_) => Err(PyRuntimeError::new_err(format!(
                "{what} is a worker's: these are the server's lanes"
            ))),
        }
    }
}

#[pymethods]
impl PyLaneSet {
    /// Maps, in a worker, lane `lane`'s channel for this run from the fds the template handed it (the shared memory,
    /// which is closed once mapped, and the write end of its host's wake pipe, which the lane keeps); `attempt` is the
    /// start attempt it reports under. This takes ownership of both fds.
    #[staticmethod]
    fn _attach(
        lane: usize,
        attempt: u32,
        depth: u32,
        slot_bytes: usize,
        worker_spin_us: u64,
        shm_fd: i32,
        bell_fd: i32,
    ) -> PyResult<Self> {
        // SAFETY: The caller hands over fds it received and owns.
        let (shm, bell) = unsafe { (OwnedFd::from_raw_fd(shm_fd), OwnedFd::from_raw_fd(bell_fd)) };
        WorkerLane::attach(
            LaneIndex(lane),
            attempt,
            depth,
            slot_bytes,
            Duration::from_micros(worker_spin_us),
            shm,
            bell,
        )
        .map(|lane| PyLaneSet(LaneRole::Worker(lane)))
        .map_err(|e| match e {
            LaneAttachError::Setup(_) => PyValueError::new_err(e.to_string()),
            LaneAttachError::Map { .. } => PyOSError::new_err(e.to_string()),
        })
    }

    /// Sends the template `message`: what a worker imports and how a lane's channel is laid out. Each run then sends
    /// each lane's channel.
    fn _setup_template(&self, py: Python<'_>, message: &[u8]) -> PyResult<()> {
        let lanes = self.server("_setup_template")?;
        py.detach(|| lanes.setup_template(message)).map_err(|e| match e {
            TemplateSetupError::TooLong { .. } => PyValueError::new_err(e.to_string()),
            TemplateSetupError::NoTemplate | TemplateSetupError::Send(_) => PyOSError::new_err(e.to_string()),
        })
    }

    /// Tells the template `command_line`, the one of the run whose workers it starts next. A
    /// command line longer than a template frame holds is refused before anything is sent.
    fn _template_run(&self, py: Python<'_>, command_line: &CommandLine) -> PyResult<()> {
        let lanes = self.server("_template_run")?;
        let text = command_line.encode_text();
        py.detach(|| lanes.tell_template_run(&text)).map_err(|e| match e {
            TemplateRunError::CommandLineTooLong { .. } => PyValueError::new_err(e.to_string()),
            TemplateRunError::NoTemplate | TemplateRunError::Send(_) => PyOSError::new_err(e.to_string()),
        })
    }

    #[getter]
    fn workers(&self) -> PyResult<usize> {
        Ok(self.server("workers")?.workers())
    }

    #[getter]
    fn template_pid(&self) -> PyResult<Option<i32>> {
        Ok(self.server("template_pid")?.template_pid().map(Pid::as_raw))
    }

    fn worker_pids(&self) -> PyResult<Vec<Option<i32>>> {
        Ok(raw_pids(self.server("worker_pids()")?.worker_pids()))
    }

    /// Records the template's pid and takes ownership of its socket `fd`.
    fn _attach_template(&self, pid: i32, fd: i32) -> PyResult<()> {
        let pid = pid_from_python(pid)?;
        let lanes = self.server("_attach_template")?;
        // SAFETY: The caller hands over an fd it owns, from `socket.detach()`.
        let sock = unsafe { OwnedFd::from_raw_fd(fd) };
        lanes.attach_template(pid, sock).map_err(|refused| {
            PyRuntimeError::new_err(format!("these lanes already have a template: pid {refused} is refused"))
        })
    }

    /// Reports to the server that this worker could not start, with `text` (its traceback) cut to fit; the worker
    /// serves nothing after it.
    fn _report_start_failure(&self, text: &str) -> PyResult<()> {
        let (peer, bell) = self.peer("_report_start_failure")?;
        peer.report_failed(bell, text);
        Ok(())
    }

    /// Returns the lanes' counters; `respawn_us` is in microseconds.
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        stats_dict(py, self.server("stats()")?.stats_snapshot())
    }
}

fn stats_dict(py: Python<'_>, s: StatsSnapshot) -> PyResult<Bound<'_, PyDict>> {
    let park_spin_hits = s.park_spin_hits();
    let d = PyDict::new(py);
    d.set_item("deaths", s.deaths)?;
    d.set_item("timeouts_killed", s.timeouts_killed)?;
    d.set_item("peer_faults", s.peer_faults)?;
    d.set_item("respawns", s.respawns)?;
    d.set_item("respawn_failures", s.respawn_failures)?;
    d.set_item("respawn_us", &s.respawn_us)?;
    d.set_item("worker_pids", raw_pids(s.worker_pids))?;
    d.set_item("requests_in_lanes", s.requests_in_lanes)?;
    d.set_item("park_spin_hits", park_spin_hits)?;
    d.set_item("park_spin_misses", s.host_waits.parks)?;
    d.set_item("host_spin_rounds", s.host_waits.spin_rounds)?;
    d.set_item("worker_wakes", s.host_waits.wakes)?;
    d.set_item("worker_waits", s.worker_waits.waits)?;
    d.set_item("worker_spin_rounds", s.worker_waits.spin_rounds)?;
    d.set_item("worker_parks", s.worker_waits.parks)?;
    d.set_item("host_wakes", s.worker_waits.wakes)?;
    let events = s.host_events;
    d.set_item("host_sleeps", events.sleeps)?;
    d.set_item("pipe_wakes", events.pipe_wakes)?;
    d.set_item("answers_by_submit", events.by_submit)?;
    d.set_item("answers_by_spin", events.by_spin)?;
    d.set_item("answers_by_poller", events.by_poller)?;
    d.set_item("park_hooks", events.park_hooks)?;
    d.set_item("overflow_forwards", events.overflow_forwards)?;
    d.set_item("spread_forwards", events.spread_forwards)?;
    d.set_item("unawaited_skips", events.unawaited_skips)?;
    d.set_item("host_failures", s.host_failures)?;
    d.set_item("lanes_broken", s.lanes_broken)?;
    d.set_item("fatal", s.fatal.map(|cause| cause.to_string()))?;
    Ok(d)
}

fn raw_pids(pids: Vec<Option<Pid>>) -> Vec<Option<i32>> {
    pids.into_iter().map(|p| p.map(Pid::as_raw)).collect()
}

/// Parses a pid from Python.
///
/// # Errors
///
/// Raises `ValueError` for 0 and negative values.
pub(crate) fn pid_from_python(raw: i32) -> PyResult<Pid> {
    Pid::new(raw).ok_or_else(|| PyValueError::new_err(format!("a process id must be above 0, got {raw}")))
}
