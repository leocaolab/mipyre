//! Runs `runtime="process"` routes in worker processes forked from a template, with requests and
//! answers crossing through a shared-memory channel per lane (`mipyre_shm`).
//!
//! `mipyre._process_lane` forks the template after the app is imported and before any server
//! thread exists, because a fork copies only the calling thread.

mod core;
mod driver;
mod fork_server;
mod health;
mod host;
mod inbox;
mod lane_config;
mod lane_failure;
mod lane_set;
mod monitor;
mod os;
mod pid;
mod poller;
mod role;
mod route;
mod run;
mod stats;
mod sync;
mod template;
mod wire;
mod worker_process;

pub use self::core::START_REFUSED;
pub use self::driver::{spawn_python, start_new_process, ProcessStartError, StartStrategy, DRIVER_NAME};
pub use self::fork_server::{TemplateRunError, TemplateSetupError};
pub use self::health::FailurePolicy;
pub use self::host::{host_tpc_thread, LaneHost, LaneLossError, LaneRunError, LaneRunEvent};
pub use self::lane_failure::{LaneDecodeError, LaneIndex, LaneUnansweredError, TaggedField, WorkerExit, WorkerFailure};
pub use self::lane_set::{
    create_server_lanes, LaneAttachError, LaneRecipe, LaneSlotBytes, PeerLane, WorkerLane, MIN_SLOT_BYTES,
};
pub use self::os::{
    exit_now, monotonic_ns, ExitWatch, ExitWatchStatus, ProcessHandle, StartedProcess, KERNEL_ENDS_ORPHANED_WORKERS,
};
pub use self::pid::Pid;
pub use self::poller::{enter_idle, resume_from_idle};
pub use self::role::{
    mark_lane_worker, refuse_in_lane_worker, LaneWorkerProcess, MarkLaneWorkerError, UnsupportedInLaneWorkerError,
};
pub use self::route::LaneRequestError;
pub use self::run::{HostKind, ServerLanes};
pub use self::stats::StatsSnapshot;
pub use self::wire::{decode_request, decode_response, encode_response, LaneRequest, SlotRequest};
