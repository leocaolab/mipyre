//! Makes an app's process lanes: the server's, from its settings, or one worker's, from the fds its template handed it.

use crate::sync::Mutex;
use std::num::NonZeroUsize;
use std::os::fd::{AsFd, OwnedFd};
use std::sync::Arc;
use std::time::Duration;

use super::driver::StartStrategy;
use super::health::FailurePolicy;
use super::host_config::{HostConfig, HostConfigError};
use super::lane_failure::LaneIndex;
use super::run::ServerLanes;
use super::wire;
use mipyre_shm::adapter::doorbell::PipeDoorbell;
use mipyre_shm::adapter::mapped::{AttachError, MappedMemory};
use mipyre_shm::adapter::parker::FutexParker;
use mipyre_shm::channel::{Attempt, Channel, PeerEnd};
use mipyre_shm::plan::geometry::{Geometry, GeometryError, SlotBytes};
use mipyre_shm::plan::ring::RingDepth;

/// What a worker serves: its lane's channel, the write end of its host's wake pipe and the start attempt it is.
pub struct PeerLane {
    pub lane: LaneIndex,
    pub attempt: Attempt,
    pub channel: Arc<Channel<MappedMemory>>,
    pub worker_spin: Duration,
}

impl PeerLane {
    /// Reports that this worker could not start, with `text` (its traceback) cut to the note's room.
    pub fn report_failed(&self, bell: OwnedFd, text: &str) {
        let note = wire::cut_to_fit(text, mipyre_shm::channel::NOTE_BYTES as usize);
        PeerEnd::new(Arc::clone(&self.channel), PipeDoorbell::new(bell), FutexParker)
            .report_failed(self.attempt, note.as_bytes());
    }
}

/// Makes the server's lanes.
///
/// # Errors
///
/// Refuses no workers, a depth that is no power of two, slots below [`MIN_SLOT_BYTES`] and a queue shorter than a
/// lane's ring.
pub fn create_server_lanes(
    workers: usize,
    slot_bytes: usize,
    depth: u32,
    timeout: Duration,
    queue_per_worker: usize,
    server_spin: Duration,
    on_failure: FailurePolicy,
) -> Result<ServerLanes, LaneSettingsError> {
    let lanes = NonZeroUsize::new(workers).ok_or(LaneSettingsError::NoLanes)?;
    let geometry = lane_geometry(depth, slot_bytes)?;
    let host = HostConfig::new(&geometry, timeout, queue_per_worker, server_spin, on_failure)?;
    Ok(ServerLanes::create(lanes, geometry, host))
}

/// A worker's lane, and the write end of its host's wake pipe until the worker serves or reports that it could not
/// start, which takes it.
pub struct WorkerLane {
    peer: PeerLane,
    bell: Mutex<Option<OwnedFd>>,
}

/// A worker lane whose wake pipe was taken, by serving or by reporting that it could not start.
#[derive(Debug, thiserror::Error)]
#[error("this worker's lane already served, or reported that it could not start")]
pub struct TakeBellError;

/// A worker lane that could not be mapped from what its template handed it.
#[derive(Debug, thiserror::Error)]
pub enum LaneAttachError {
    #[error("the template setup: {0}")]
    Setup(#[from] LaneSettingsError),
    #[error("mapping lane {lane}'s channel: {source}")]
    Map { lane: LaneIndex, source: AttachError },
}

impl WorkerLane {
    /// Maps lane `lane`'s channel for this run from `shm`, which is closed once mapped, and keeps `bell`, the write
    /// end of its host's wake pipe; `attempt` is the start attempt it reports under.
    ///
    /// # Errors
    ///
    /// Refuses a layout the lanes refuse (it came over a pipe, though the server checked it) and memory that cannot
    /// be mapped for it.
    pub fn attach(
        lane: LaneIndex,
        attempt: u32,
        depth: u32,
        slot_bytes: usize,
        worker_spin: Duration,
        shm: OwnedFd,
        bell: OwnedFd,
    ) -> Result<Self, LaneAttachError> {
        let geometry = lane_geometry(depth, slot_bytes)?;
        let memory =
            MappedMemory::attach(&geometry, shm.as_fd()).map_err(|source| LaneAttachError::Map { lane, source })?;
        drop(shm);
        Ok(WorkerLane {
            peer: PeerLane {
                lane,
                attempt: Attempt::new(attempt),
                channel: Arc::new(Channel::new(memory)),
                worker_spin,
            },
            bell: Mutex::new(Some(bell)),
        })
    }

    /// Returns the lane and takes its host's wake pipe; only the first call gets it.
    ///
    /// # Errors
    ///
    /// Refuses every call after the first.
    pub fn take_bell(&self) -> Result<(&PeerLane, OwnedFd), TakeBellError> {
        let bell = self.bell.lock().take().ok_or(TakeBellError)?;
        Ok((&self.peer, bell))
    }
}

/// One app's process lanes, their flags settled: what the server's lanes and the template's setup are made from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneRecipe {
    pub start: StartStrategy,
    pub workers: usize,
    pub timeout_ms: u64,
    pub queue_per_worker: usize,
    pub slot_bytes: usize,
    pub worker_spin_us: u64,
    pub host_failure: FailurePolicy,
}

/// Any failure message fits a slot this large once `wire::encode_failure` cuts it.
pub const MIN_SLOT_BYTES: usize = 4096;

/// Counts a lane slot's bytes, its header included; it is never below [`MIN_SLOT_BYTES`], because only
/// [`LaneSlotBytes::new`] makes one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LaneSlotBytes(SlotBytes);

impl LaneSlotBytes {
    /// Returns `None` below [`MIN_SLOT_BYTES`].
    pub fn new(bytes: usize) -> Option<Self> {
        if bytes < MIN_SLOT_BYTES {
            return None;
        }
        SlotBytes::new(bytes).map(LaneSlotBytes)
    }

    pub fn get(self) -> usize {
        self.0.get()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LaneSettingsError {
    #[error("process workers must be at least 1")]
    NoLanes,
    #[error("process lane depth must be a power of two and slot_bytes ≥ {MIN_SLOT_BYTES}, got depth={depth} slot_bytes={slot_bytes}")]
    RingOutOfRange { depth: u32, slot_bytes: usize },
    #[error("process lanes: {0}")]
    Layout(#[from] GeometryError),
    #[error(transparent)]
    Host(#[from] HostConfigError),
}

/// Returns the layout of one lane's channel.
pub(super) fn lane_geometry(depth: u32, slot_bytes: usize) -> Result<Geometry, LaneSettingsError> {
    match (RingDepth::new(depth), LaneSlotBytes::new(slot_bytes)) {
        (Some(depth), Some(LaneSlotBytes(slot))) => Ok(Geometry::new(depth, slot)?),
        _ => Err(LaneSettingsError::RingOutOfRange { depth, slot_bytes }),
    }
}

#[cfg(test)]
#[cfg(not(sync_model))]
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

    fn server_lanes(workers: usize, queue_per_worker: usize) -> Result<ServerLanes, LaneSettingsError> {
        let second = Duration::from_secs(1);
        create_server_lanes(
            workers,
            MIN_SLOT_BYTES,
            8,
            second,
            queue_per_worker,
            second,
            FailurePolicy::Isolate,
        )
    }

    #[test]
    fn the_server_needs_one_lane() {
        assert!(matches!(server_lanes(0, 8), Err(LaneSettingsError::NoLanes)));
        assert!(server_lanes(1, 8).is_ok());
    }

    #[test]
    fn a_host_queue_shorter_than_the_lane_ring_is_refused() {
        assert!(matches!(server_lanes(1, 7), Err(LaneSettingsError::Host(_))));
        assert!(server_lanes(1, 8).is_ok());
    }

    fn worker_lane() -> WorkerLane {
        let geometry = lane_geometry(8, MIN_SLOT_BYTES).unwrap();
        let (_memory, shm) = MappedMemory::create(&geometry).unwrap();
        let (_read, bell) = rustix::pipe::pipe().unwrap();
        WorkerLane::attach(LaneIndex(0), 1, 8, MIN_SLOT_BYTES, Duration::ZERO, shm, bell).unwrap()
    }

    #[test]
    fn a_worker_lane_hands_out_its_bell_once() {
        let lane = worker_lane();
        assert!(lane.take_bell().is_ok());
        assert!(matches!(lane.take_bell(), Err(TakeBellError)));
    }

    #[test]
    fn a_worker_lane_refuses_a_layout_the_lanes_refuse() {
        let geometry = lane_geometry(8, MIN_SLOT_BYTES).unwrap();
        let (_memory, shm) = MappedMemory::create(&geometry).unwrap();
        let (_read, bell) = rustix::pipe::pipe().unwrap();
        let refused = WorkerLane::attach(LaneIndex(0), 1, 3, MIN_SLOT_BYTES, Duration::ZERO, shm, bell);
        assert!(matches!(
            refused,
            Err(LaneAttachError::Setup(LaneSettingsError::RingOutOfRange {
                depth: 3,
                ..
            }))
        ));
    }

    #[test]
    fn a_command_line_a_template_frame_cannot_hold_is_refused_before_the_template_is_asked() {
        let lanes = server_lanes(1, 8).unwrap();
        let max = super::super::template::MAX_FRAME_BYTES as usize;
        assert!(matches!(
            lanes.tell_template_run(&"x".repeat(max - 1)),
            Err(crate::template_host::TemplateRunError::NoTemplate)
        ));
        assert!(matches!(
            lanes.tell_template_run(&"x".repeat(max)),
            Err(crate::template_host::TemplateRunError::CommandLineTooLong { bytes }) if bytes == max
        ));
    }

    #[test]
    fn a_setup_a_template_frame_cannot_hold_is_refused_before_the_template_is_asked() {
        let lanes = server_lanes(1, 8).unwrap();
        let max = super::super::template::MAX_FRAME_BYTES as usize;
        assert!(matches!(
            lanes.setup_template(&vec![b'x'; max - 1]),
            Err(crate::template_host::TemplateSetupError::NoTemplate)
        ));
        assert!(matches!(
            lanes.setup_template(&vec![b'x'; max]),
            Err(crate::template_host::TemplateSetupError::TooLong { bytes }) if bytes == max
        ));
    }

    #[test]
    fn park_spin_hits_saturate_when_parks_run_ahead_of_waits() {
        let mut stats = server_lanes(1, 8).unwrap().stats_snapshot();
        stats.host_waits.waits = 5;
        stats.host_waits.parks = 2;
        assert_eq!(stats.park_spin_hits(), 3);
        stats.host_waits.parks = 6;
        assert_eq!(stats.park_spin_hits(), 0);
    }
}
