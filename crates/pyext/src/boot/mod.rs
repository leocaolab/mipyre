//! Reads the server's configuration once and runs what `Server.serve()` starts.

mod config;
mod cpus;
mod run;
mod topology;

pub(crate) use self::config::{EnvConfig, GivenSize, ServerSizes, SizeOrigin};
pub(crate) use self::cpus::{host_cpus, ServerCpus};
#[cfg(feature = "bench")]
pub(crate) use self::run::{thread_sites, worker_recipe};
pub(crate) use self::run::{AbandonedWorkers, AppSource, ServerRun};
pub(crate) use self::topology::{InterpreterMode, Topology};
