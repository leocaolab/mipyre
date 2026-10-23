//! Exposes an app's process lanes to Python as `mipyre.engine.LaneSet`.

use crate::sync::{Mutex, Ordering::*};
use std::num::NonZeroUsize;
use std::os::fd::{AsFd, FromRawFd, OwnedFd};
use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;

use super::driver::StartStrategy;
use pyo3::types::PyDict;

use super::health::FailurePolicy;
use super::pid::Pid;
use super::server::{self, ServerLanes};
use super::worker::PeerLane;
use crate::exchange::LaneIndex;
use mipyre_shm::adapter::mapped::MappedMemory;
use mipyre_shm::channel::{Attempt, Channel};
use mipyre_shm::plan::geometry::{Geometry, GeometryError, SlotBytes};
use mipyre_shm::plan::ring::RingDepth;

/// Holds an app's process lanes: the server's, or one worker's lane.
#[pyclass(module = "mipyre.engine", frozen)]
pub(crate) struct LaneSet(LaneSide);

enum LaneSide {
    Server(Arc<ServerLanes>),
    /// A worker's lane, and the write end of its host's wake pipe until the worker serves (or reports that it could
    /// not start), which takes it.
    Peer {
        peer: PeerLane,
        bell: Mutex<Option<OwnedFd>>,
    },
}

impl LaneSet {
    /// Returns the worker's lane and takes its host's wake pipe, or raises `RuntimeError` naming `what` on the
    /// server's lanes or once the pipe was taken.
    fn peer(&self, what: &str) -> PyResult<(&PeerLane, OwnedFd)> {
        match &self.0 {
            LaneSide::Peer { peer, bell } => match bell.lock().take() {
                Some(bell) => Ok((peer, bell)),
                None => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "{what}: this worker's lane already served, or reported that it could not start"
                ))),
            },
            LaneSide::Server(_) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "{what} is a worker's: these are the server's lanes"
            ))),
        }
    }

    /// Returns the server's lanes, or raises `RuntimeError` naming `what` on a worker's lane.
    pub(crate) fn server(&self, what: &str) -> PyResult<&Arc<ServerLanes>> {
        match &self.0 {
            LaneSide::Server(lanes) => Ok(lanes),
            LaneSide::Peer { .. } => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "{what} is the server's: this is a worker's lane"
            ))),
        }
    }
}

#[pymethods]
impl FailurePolicy {
    /// Parses `"isolate"` or `"exit"`.
    ///
    /// # Errors
    ///
    /// Raises `ValueError` naming any other text.
    #[staticmethod]
    fn parse(text: &str) -> PyResult<Self> {
        text.parse()
            .map_err(|e: super::health::ParseFailurePolicyError| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    #[getter]
    fn name(&self) -> String {
        self.to_string()
    }
}

#[pymethods]
impl LaneSet {
    #[new]
    #[pyo3(signature = (
        workers, slot_bytes=256 * 1024, depth=8, timeout_ms=30_000, queue_per_worker=128,
        server_spin_us=20, on_failure=FailurePolicy::Isolate
    ))]
    fn new(
        workers: usize,
        slot_bytes: usize,
        depth: u32,
        timeout_ms: u64,
        queue_per_worker: usize,
        server_spin_us: u64,
        on_failure: FailurePolicy,
    ) -> PyResult<Self> {
        let bad = |m: String| pyo3::exceptions::PyValueError::new_err(m);
        let lanes = NonZeroUsize::new(workers).ok_or_else(|| bad(LaneSettingsError::NoLanes.to_string()))?;
        let geometry = lane_geometry(depth, slot_bytes).map_err(|e| bad(e.to_string()))?;
        let host = server::HostConfig::new(
            &geometry,
            Duration::from_millis(timeout_ms),
            queue_per_worker,
            Duration::from_micros(server_spin_us),
            on_failure,
        )
        .map_err(|e| bad(e.to_string()))?;
        Ok(LaneSet(LaneSide::Server(Arc::new(ServerLanes::create(
            lanes, geometry, host,
        )))))
    }

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
        // The server already checked these, but they came over a pipe.
        let geometry = lane_geometry(depth, slot_bytes)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("the template setup: {e}")))?;
        let memory = MappedMemory::attach(&geometry, shm.as_fd())
            .map_err(|e| pyo3::exceptions::PyOSError::new_err(format!("mapping lane {lane}'s channel: {e}")))?;
        drop(shm);
        Ok(LaneSet(LaneSide::Peer {
            peer: PeerLane {
                lane: LaneIndex(lane),
                attempt: Attempt::new(attempt),
                channel: Arc::new(Channel::new(memory)),
                worker_spin: Duration::from_micros(worker_spin_us),
            },
            bell: Mutex::new(Some(bell)),
        }))
    }

    /// Sends the template `message`: what a worker imports and how a lane's channel is laid out. Each run then sends
    /// each lane's channel.
    fn _setup_template(&self, py: Python<'_>, message: &[u8]) -> PyResult<()> {
        let lanes = Arc::clone(self.server("_setup_template")?);
        let message = message.to_vec();
        py.detach(move || match lanes.template() {
            Some(template) => template.send_setup(&message).map_err(|e| e.to_string()),
            None => Err("no template process to set up".to_string()),
        })
        .map_err(pyo3::exceptions::PyOSError::new_err)
    }

    /// Tells the template `command_line`, the one of the run whose workers it starts next. A
    /// command line longer than a template frame holds is refused before anything is sent.
    fn _template_run(&self, py: Python<'_>, command_line: &crate::runtime::CommandLine) -> PyResult<()> {
        let lanes = Arc::clone(self.server("_template_run")?);
        let text = command_line.encode_text();
        if text.len() >= super::template::MAX_FRAME_BYTES as usize {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "the run's command line is {} bytes of JSON, and the process-lane template takes at most {}: \
                 pass less on the command line (a file the app reads, the environment)",
                text.len(),
                super::template::MAX_FRAME_BYTES - 1
            )));
        }
        py.detach(move || match lanes.template() {
            Some(template) => template.send_run(text.as_bytes()).map_err(|e| e.to_string()),
            None => Err("no template process to tell the run's command line".to_string()),
        })
        .map_err(pyo3::exceptions::PyOSError::new_err)
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
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "these lanes already have a template: pid {refused} is refused"
            ))
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
        let lanes = self.server("stats()")?;
        let s = &lanes.stats;
        let d = PyDict::new(py);
        d.set_item("deaths", s.deaths.load(Relaxed))?;
        d.set_item("timeouts_killed", s.timeouts_killed.load(Relaxed))?;
        d.set_item("peer_faults", s.peer_faults.load(Relaxed))?;
        d.set_item("respawns", s.respawns.load(Relaxed))?;
        d.set_item("respawn_failures", s.respawn_failures.load(Relaxed))?;
        d.set_item("respawn_us", s.respawn_us.lock().clone())?;
        d.set_item("worker_pids", raw_pids(lanes.worker_pids()))?;
        d.set_item("requests_in_lanes", lanes.requests_in_lanes())?;
        let host = lanes.host_waits_total();
        // A host may be adding while these are read one by one, so parks can be up to one per
        // host ahead of waits; the subtraction saturates for that.
        d.set_item("park_spin_hits", host.waits.saturating_sub(host.parks))?;
        d.set_item("park_spin_misses", host.parks)?;
        d.set_item("host_spin_rounds", host.spin_rounds)?;
        d.set_item("worker_wakes", host.wakes)?;
        let worker = lanes.worker_waits_total();
        d.set_item("worker_waits", worker.waits)?;
        d.set_item("worker_spin_rounds", worker.spin_rounds)?;
        d.set_item("worker_parks", worker.parks)?;
        d.set_item("host_wakes", worker.wakes)?;
        let events = lanes.host_events_total();
        d.set_item("host_sleeps", events.sleeps)?;
        d.set_item("pipe_wakes", events.pipe_wakes)?;
        d.set_item("answers_by_submit", events.by_submit)?;
        d.set_item("answers_by_spin", events.by_spin)?;
        d.set_item("answers_by_poller", events.by_poller)?;
        d.set_item("park_hooks", events.park_hooks)?;
        d.set_item("overflow_forwards", events.overflow_forwards)?;
        d.set_item("spread_forwards", events.spread_forwards)?;
        d.set_item("host_failures", s.host_failures.load(Relaxed))?;
        d.set_item("lanes_broken", lanes.broken_lanes())?;
        d.set_item("fatal", lanes.fatal().map(|cause| cause.to_string()))?;
        Ok(d)
    }
}

impl LaneSet {
    /// Serves this worker's lane with `rules` until the process is killed; it returns only on error.
    pub(crate) fn serve_worker<M>(
        &self,
        py: Python<'_>,
        rules: mipyre_http::plan::site::SiteRules<Py<PyAny>, M>,
        template_pid: Pid,
    ) -> PyResult<()> {
        let (peer, bell) = self.peer("serving a lane")?;
        super::worker::serve(py, peer, bell, rules, template_pid)
    }
}

/// One app's process lanes, their flags settled (b/335 C4): what `LaneSet` and the template's
/// setup are made from. Python reads it; `mipyre._process_lane.server.start` builds the lanes.
#[pyclass(module = "mipyre.engine", frozen, get_all)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LaneRecipe {
    pub(crate) start: StartStrategy,
    pub(crate) workers: usize,
    pub(crate) timeout_ms: u64,
    pub(crate) queue_per_worker: usize,
    pub(crate) slot_bytes: usize,
    pub(crate) worker_spin_us: u64,
    pub(crate) host_failure: FailurePolicy,
}

/// Any failure message fits a slot this large once `wire::encode_failure` cuts it.
pub(crate) const MIN_SLOT_BYTES: usize = 4096;

/// Counts a lane slot's bytes, its header included; it is never below [`MIN_SLOT_BYTES`], because only
/// [`LaneSlotBytes::new`] makes one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LaneSlotBytes(SlotBytes);

impl LaneSlotBytes {
    /// Returns `None` below [`MIN_SLOT_BYTES`].
    pub(crate) fn new(bytes: usize) -> Option<Self> {
        if bytes < MIN_SLOT_BYTES {
            return None;
        }
        SlotBytes::new(bytes).map(LaneSlotBytes)
    }

    pub(crate) fn get(self) -> usize {
        self.0.get()
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum LaneSettingsError {
    #[error("process workers must be at least 1")]
    NoLanes,
    #[error("process lane depth must be a power of two and slot_bytes ≥ {MIN_SLOT_BYTES}, got depth={depth} slot_bytes={slot_bytes}")]
    RingOutOfRange { depth: u32, slot_bytes: usize },
    #[error("process lanes: {0}")]
    Layout(#[from] GeometryError),
}

/// Returns the layout of one lane's channel.
pub(super) fn lane_geometry(depth: u32, slot_bytes: usize) -> Result<Geometry, LaneSettingsError> {
    match (RingDepth::new(depth), LaneSlotBytes::new(slot_bytes)) {
        (Some(depth), Some(LaneSlotBytes(slot))) => Ok(Geometry::new(depth, slot)?),
        _ => Err(LaneSettingsError::RingOutOfRange { depth, slot_bytes }),
    }
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
    Pid::new(raw)
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(format!("a process id must be above 0, got {raw}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A lane slot is at least `MIN_SLOT_BYTES`: any failure message fits one.
    #[test]
    fn a_lane_slot_below_the_minimum_is_refused() {
        assert!(LaneSlotBytes::new(MIN_SLOT_BYTES - 1).is_none());
        assert!(LaneSlotBytes::new(MIN_SLOT_BYTES).is_some());
        assert!(matches!(
            lane_geometry(3, MIN_SLOT_BYTES),
            Err(LaneSettingsError::RingOutOfRange { depth: 3, .. })
        ));
    }

    /// A lane's channel gives its message the slot without the slot's 8-byte header: 4 bytes less than the lanes'
    /// own layout gave before mipyre_shm.
    #[test]
    fn a_lane_message_takes_the_slot_without_its_header() {
        assert_eq!(
            lane_geometry(2, MIN_SLOT_BYTES).unwrap().max_message() as usize,
            MIN_SLOT_BYTES - 8
        );
    }
}
