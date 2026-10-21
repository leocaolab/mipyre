//! Runs `runtime="process"` routes in worker processes forked from a template, with requests and
//! answers crossing through shared memory (`docs/design/process-lane-shm.md`).
//!
//! `mipyre._process_lane` forks the template after the app is imported and before any server
//! thread exists, because a fork copies only the calling thread.

mod driver;
mod health;
mod lane_set;
mod os;
mod pid;
mod role;
mod server;
mod shm;
mod spin;
mod template;
mod wire;
mod worker;

pub(crate) use self::driver::{
    _driver_fork_worker, _driver_name, _driver_start_import_worker, _driver_start_template,
    StartStrategy,
};
pub(crate) use self::lane_set::{pid_from_python, LaneSet};
pub(crate) use self::role::refuse_in_lane_worker;
pub(crate) use self::server::ServerLanes;
pub(crate) use self::server::{enter_idle, host_tpc_thread, resume_from_idle, HostKind, LaneJob};
pub(crate) use self::wire::LaneRequest;
