//! Runs `runtime="process"` routes in worker processes forked from a template, with requests and
//! answers crossing through a shared-memory channel per lane (`mipyre_shm`,
//! `docs/design/shm-channel.md`).
//!
//! `mipyre._process_lane` forks the template after the app is imported and before any server
//! thread exists, because a fork copies only the calling thread.

mod core;
mod driver;
mod health;
mod lane_set;
mod os;
mod pid;
mod role;
mod server;
mod template;
mod wire;
#[cfg(test)]
mod wire_fuzz;
mod worker;
mod worker_process;

pub(crate) use self::driver::{
    _driver_fork_worker, _driver_name, _driver_spawn_template, _driver_start_import_worker, _driver_start_template,
    StartStrategy,
};
pub(crate) use self::health::FailurePolicy;
pub(crate) use self::lane_set::{pid_from_python, LaneRecipe, LaneSet, LaneSlotBytes, MIN_SLOT_BYTES};
pub(crate) use self::role::refuse_in_lane_worker;
pub(crate) use self::server::ServerLanes;
pub(crate) use self::server::{enter_idle, host_tpc_thread, resume_from_idle, HostKind, LaneJob};
pub(crate) use self::wire::LaneRequest;
