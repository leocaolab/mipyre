//! Reads the server's configuration once and runs what `Server.serve()` starts.

mod config;
mod cpus;
mod exit;
mod run;
mod serve_exception;
mod settings;
mod topology;

pub(crate) use self::config::exception::ConfigError as ConfigErrorException;
pub(crate) use self::config::{
    user_cache_dir, ConfigError, FlagError, FlagSource, FlagValue, ServerRecipe, DEFAULT_HOST, DEFAULT_PORT,
};
pub(crate) use self::cpus::{host_cpus, ServerCpus};
pub(crate) use self::exit::exit_without_finalizing;
#[cfg(feature = "bench")]
pub(crate) use self::run::{thread_sites, worker_loss_listener, worker_recipe};
pub(crate) use self::run::{AbandonedWorkers, AppSource, NoIsolateDirError, ReadinessCallback, ServerRun};
pub(crate) use self::serve_exception::{listener_exception, passed_exception};
pub(crate) use self::settings::{
    cli_option, env_variable, CodeCall, Deployment, EntryPoint, Flag, FlagScope, ServerFlagValues, ServerFlags, FLAGS,
};
pub(crate) use self::topology::{InterpreterMode, ServerLayout, Topology};
