//! Exposes an app's process lanes to Python as `mipyre.engine.LaneSet`.

use std::num::NonZeroUsize;
use std::os::fd::{FromRawFd, OwnedFd};
use crate::sync::Ordering::*;
use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::health::FailurePolicy;
use super::pid::Pid;
use super::server::{self, LaneMemory, ServerLanes};
use super::shm;
use super::wire;

/// Holds an app's process lanes, either the server's or the template's view of them.
#[pyclass(module = "mipyre.engine", frozen)]
pub(crate) struct LaneSet(LaneSide);

enum LaneSide {
    Server(Arc<ServerLanes>),
    Template {
        memory: LaneMemory,
        worker_spin: Duration,
    },
}

impl LaneSet {
    fn memory(&self) -> &LaneMemory {
        match &self.0 {
            LaneSide::Server(lanes) => &lanes.memory,
            LaneSide::Template { memory, .. } => memory,
        }
    }

    /// Returns the template's view, or raises `RuntimeError` naming `what` on the server's lanes.
    fn template(&self, what: &str) -> PyResult<(&LaneMemory, Duration)> {
        match &self.0 {
            LaneSide::Template {
                memory,
                worker_spin,
            } => Ok((memory, *worker_spin)),
            LaneSide::Server(_) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "{what} is the template's and its workers': these are the server's lanes"
            ))),
        }
    }

    /// Returns the server's lanes, or raises `RuntimeError` naming `what` on the template's view.
    pub(crate) fn server(&self, what: &str) -> PyResult<&Arc<ServerLanes>> {
        match &self.0 {
            LaneSide::Server(lanes) => Ok(lanes),
            LaneSide::Template { .. } => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "{what} is the server's: this is the template's view of the lanes"
            ))),
        }
    }
}

#[pymethods]
impl LaneSet {
    /// Raises `IndexError` when `lane` is not one of these lanes, so a template's or worker's bug
    /// surfaces on its side instead of being dropped.
    fn check_lane(&self, lane: usize) -> PyResult<()> {
        let workers = self.memory().workers();
        if lane < workers {
            Ok(())
        } else {
            Err(pyo3::exceptions::PyIndexError::new_err(format!(
                "lane {lane} does not exist: this set has {workers}"
            )))
        }
    }
    #[new]
    #[pyo3(signature = (
        workers, slot_bytes=256 * 1024, depth=8, timeout_ms=30_000, queue_per_worker=128,
        server_spin_us=20, exit_on_failure=false
    ))]
    fn new(
        workers: usize,
        slot_bytes: usize,
        depth: u32,
        timeout_ms: u64,
        queue_per_worker: usize,
        server_spin_us: u64,
        exit_on_failure: bool,
    ) -> PyResult<Self> {
        let bad = |m: String| pyo3::exceptions::PyValueError::new_err(m);
        let geometry = lane_geometry(workers, depth, slot_bytes).map_err(|e| bad(e.to_string()))?;
        let host = server::HostConfig::new(
            geometry,
            Duration::from_millis(timeout_ms),
            queue_per_worker,
            Duration::from_micros(server_spin_us),
            if exit_on_failure {
                FailurePolicy::Exit
            } else {
                FailurePolicy::Isolate
            },
        )
        .map_err(|e| bad(e.to_string()))?;
        let lanes = ServerLanes::create(geometry, host).map_err(|e| {
            pyo3::exceptions::PyOSError::new_err(format!(
                "process lanes: shared memory of {} bytes and {workers} wake pipes: {e}",
                geometry.total_bytes()
            ))
        })?;
        Ok(LaneSet(LaneSide::Server(Arc::new(lanes))))
    }

    /// Maps the server's lanes in the template; `fds` is the shared memory followed by one
    /// wake-pipe write end per lane, and this takes ownership of them.
    #[staticmethod]
    fn _attach(
        workers: usize,
        depth: u32,
        slot_bytes: usize,
        worker_spin_us: u64,
        fds: Vec<i32>,
    ) -> PyResult<Self> {
        // The server already checked these, but they came over a pipe.
        let geometry = lane_geometry(workers, depth, slot_bytes).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("the template setup: {e}"))
        })?;
        let mut owned = fds
            .into_iter()
            // SAFETY: The caller hands over fds it received and owns.
            .map(|fd| unsafe { OwnedFd::from_raw_fd(fd) })
            .collect::<Vec<_>>();
        if owned.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "the template setup carried no fds",
            ));
        }
        let shm_fd = owned.remove(0);
        let memory = LaneMemory::attach(geometry, shm_fd, owned).map_err(|e| {
            pyo3::exceptions::PyOSError::new_err(format!("mapping the server's lanes: {e}"))
        })?;
        Ok(LaneSet(LaneSide::Template {
            memory,
            worker_spin: Duration::from_micros(worker_spin_us),
        }))
    }

    /// Sends the template `message`, then each lane's shared memory and wake pipe.
    fn _setup_template(&self, py: Python<'_>, message: &[u8]) -> PyResult<()> {
        let lanes = Arc::clone(self.server("_setup_template")?);
        let message = message.to_vec();
        py.detach(move || match lanes.template() {
            Some(template) => template.send_setup(&message, &lanes.memory).map_err(|e| e.to_string()),
            None => Err("no template process to set up".to_string()),
        })
        .map_err(pyo3::exceptions::PyOSError::new_err)
    }

    #[getter]
    fn workers(&self) -> usize {
        self.memory().workers()
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

    /// Reports to the server that this worker could not start, with `text` as the traceback.
    fn _report_start_failure(&self, lane: usize, text: &str) -> PyResult<()> {
        self.check_lane(lane)?;
        let region = self.template("_report_start_failure")?.0.region;
        let slot = region.resp_slot(lane, 0);
        // SAFETY: The worker owns its lane until it is ready, and it never will be.
        let written = unsafe {
            region.write_slot(slot, |w| {
                w.put(wire::cut_to_fit(text, w.room()).as_bytes())
            })
        };
        // The cut text fits any slot of at least `MIN_SLOT_BYTES`, so a refusal is a bug and is
        // raised rather than recorded as an empty failure.
        written.map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "the start failure of lane {lane} does not fit its slot: {e}"
            ))
        })?;
        region.header(lane).start_failed.0.store(1, SeqCst);
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
        d.set_item("host_sleeps", s.host_sleeps.load(Relaxed))?;
        d.set_item("pipe_wakes", s.pipe_wakes.load(Relaxed))?;
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
        let reads = lanes.host_reads_total();
        d.set_item("answers_by_submit", reads.by_submit)?;
        d.set_item("answers_by_spin", reads.by_spin)?;
        d.set_item("answers_by_poller", reads.by_poller)?;
        d.set_item("park_hooks", reads.park_hooks)?;
        d.set_item("overflow_forwards", s.overflow_forwards.load(Relaxed))?;
        d.set_item("spread_forwards", s.spread_forwards.load(Relaxed))?;
        d.set_item("host_failures", s.host_failures.load(Relaxed))?;
        d.set_item("lanes_broken", lanes.broken_lanes())?;
        d.set_item("fatal", lanes.fatal().map(|cause| cause.to_string()))?;
        Ok(d)
    }
}

impl LaneSet {
    /// Serves `lane` with `rules` until the process is killed; it returns only on error.
    pub(crate) fn serve_worker<M>(
        &self,
        py: Python<'_>,
        lane: usize,
        rules: mipyre_http::plan::site::SiteRules<Py<PyAny>, M>,
        template_pid: Pid,
    ) -> PyResult<()> {
        let (memory, worker_spin) = self.template("serving a lane")?;
        super::worker::serve(
            py,
            memory.region,
            lane,
            rules,
            worker_spin,
            template_pid,
            memory.wake_channel_fd(lane),
        )
    }
}

#[derive(Debug, thiserror::Error)]
enum LaneSettingsError {
    #[error("process workers must be at least 1")]
    NoLanes,
    #[error("process lane depth must be a power of two and slot_bytes ≥ {}, got depth={depth} slot_bytes={slot_bytes}", shm::MIN_SLOT_BYTES)]
    RingOutOfRange { depth: u32, slot_bytes: usize },
    #[error(transparent)]
    Layout(#[from] shm::GeometryError),
}

fn lane_geometry(
    workers: usize,
    depth: u32,
    slot_bytes: usize,
) -> Result<shm::Geometry, LaneSettingsError> {
    let lanes = NonZeroUsize::new(workers).ok_or(LaneSettingsError::NoLanes)?;
    match (mipyre_shm::plan::ring::RingDepth::new(depth), shm::SlotBytes::new(slot_bytes)) {
        (Some(depth), Some(slot)) => Ok(shm::Geometry::new(lanes, depth, slot)?),
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
    Pid::new(raw).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!("a process id must be above 0, got {raw}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::exceptions::PyIndexError;

    /// The server's lanes and the template's view of them, the template's built from the setup
    /// frames the server sends: the fds it maps are the ones each lane's frame carried.
    fn server_and_template(workers: usize) -> (LaneSet, LaneSet) {
        use std::io::Read;
        let server = LaneSet::new(workers, shm::MIN_SLOT_BYTES, 2, 5_000, 2, 0, false).unwrap();
        let lanes = Arc::clone(server.server("the test").unwrap());
        let (ours, mut theirs) = std::os::unix::net::UnixStream::pair().unwrap();
        lanes.attach_template(Pid::new(1).unwrap(), OwnedFd::from(ours)).unwrap();
        lanes.template().unwrap().send_setup(b"{}", &lanes.memory).unwrap();
        let mut setup = [0u8; 4 + 1 + 2];
        theirs.read_exact(&mut setup).unwrap();
        assert_eq!(setup, [3, 0, 0, 0, 1, b'{', b'}']);
        let mut shm_fd = None;
        let mut wakes = Vec::new();
        for lane in 0..workers {
            let (frame, [shm, wake]) = receive_lane_frame(&theirs);
            assert_eq!(frame, [5, 0, 0, 0, 2, lane as u8, 0, 0, 0]);
            shm_fd.get_or_insert(shm);
            wakes.push(wake);
        }
        let geometry = lanes.memory.region.geometry();
        let memory = LaneMemory::attach(geometry, shm_fd.unwrap(), wakes).unwrap();
        let template = LaneSet(LaneSide::Template {
            memory,
            worker_spin: Duration::ZERO,
        });
        (server, template)
    }

    /// Reads one lane frame as the template does: its 9 bytes and the 2 fds on them.
    fn receive_lane_frame(sock: &std::os::unix::net::UnixStream) -> ([u8; 9], [OwnedFd; 2]) {
        use rustix::net::{RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags};
        use std::os::fd::AsFd;
        let mut frame = [0u8; 9];
        let mut space = [std::mem::MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(2))];
        let mut control = RecvAncillaryBuffer::new(&mut space);
        let mut iov = [std::io::IoSliceMut::new(&mut frame)];
        let got = rustix::net::recvmsg(sock.as_fd(), &mut iov, &mut control, RecvFlags::empty()).unwrap();
        assert_eq!(got.bytes, 9);
        let mut fds = Vec::new();
        for message in control.drain() {
            if let RecvAncillaryMessage::ScmRights(rights) = message {
                fds.extend(rights);
            }
        }
        let [shm, wake]: [OwnedFd; 2] = fds.try_into().expect("a lane frame carries two fds");
        (frame, [shm, wake])
    }

    fn start_failures(server: &LaneSet) -> Vec<Option<String>> {
        let region = &server.server("the test").unwrap().memory.region;
        (0..region.geometry().lanes()).map(|i| region.start_failure(i)).collect()
    }

    /// A report naming a lane past the set is refused and changes no lane; one naming a lane of
    /// the set reaches that lane. The template's exit reports come as frames
    /// (`decode_template_report`), a worker's start failure through the template's view.
    #[test]
    fn a_report_for_a_lane_past_the_set_raises_index_error_and_changes_no_lane() {
        use super::super::template::{decode_template_report, tests::report_frame, TemplateFrameError, TemplateReport};
        let (server, template) = server_and_template(2);
        let lanes = Arc::clone(server.server("the test").unwrap());
        let exited = |lane| {
            report_frame(&TemplateReport::Exited {
                lane: crate::exchange::LaneIndex(lane),
                pid: Pid::new(4321).unwrap(),
                status: 9,
            })
        };
        let take = |frame: &[u8]| {
            decode_template_report(frame, lanes.workers()).map(|decoded| {
                if let Some((TemplateReport::Exited { lane, pid, status }, _)) = decoded {
                    lanes.exits.record(lane, pid, status);
                }
            })
        };
        let before = start_failures(&server);
        assert_eq!(take(&exited(2)), Err(TemplateFrameError::LaneOutOfRange { lane: 2, lanes: 2 }));
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let e = template
                ._report_start_failure(2, "Traceback: import failed")
                .expect_err("lane 2 of a set of 2 is refused");
            assert!(e.is_instance_of::<PyIndexError>(py), "{e}");
            assert!(e.to_string().contains("lane 2 does not exist: this set has 2"), "{e}");
        });
        assert_eq!(start_failures(&server), before);
        let pid = Pid::new(4321).unwrap();
        assert_eq!((lanes.exits.status_of(0, pid), lanes.exits.status_of(1, pid)), (None, None));

        assert_eq!(take(&exited(1)), Ok(()));
        template
            ._report_start_failure(0, "Traceback: import failed")
            .unwrap();
        assert_eq!(lanes.exits.status_of(1, pid), Some(9));
        assert_eq!(start_failures(&server)[0].as_deref(), Some("Traceback: import failed"));
    }
}
