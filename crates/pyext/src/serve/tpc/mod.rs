//! Serves on one pinned thread per core, each with its own current-thread runtime and
//! `SO_REUSEPORT` listeners; this is the default server (docs/tpc-rearch.md).
//!
//! A connection never leaves its thread (`LocalSet::spawn_local`), so its future needs no `Send`
//! and there is no work stealing. In sub-interpreter mode each thread owns one worker and runs
//! `def` handlers inline, while `async def` handlers go to the async worker pool.

mod conn;
mod error;
#[cfg(target_os = "macos")]
mod fanout;
mod gil;
mod inline;
mod startup;
mod threads;

use std::sync::{Arc, OnceLock};

use mipyre_cpu::CoreId;
use mipyre_subinterp::gc::GcPolicy;
use mipyre_tpc::ShutdownDeadline;
use mipyre_transport::driver::TransportDriver;

use crate::runtime::{ServerRunStop, WorkerIndex, WorkerLife, WorkerRecipe};
use crate::serve::site::Site;
use crate::telemetry::RunCounters;

pub(crate) use self::error::ServeError;
pub(crate) use self::gil::run_tpc_gil;
pub(crate) use self::conn::{OffThreadRunners, ThreadWorker, TpcThread};
#[cfg(feature = "bench")]
pub(crate) use self::conn::drive_conn;
#[cfg(feature = "bench")]
pub(crate) use self::inline::tpc_accept_loop_inline;
pub(crate) use self::startup::{report_pinning, thread_cpus, tpc_banner};
#[cfg(feature = "bench")]
pub(crate) use self::threads::serve_on_workers;

/// How the TPC sub-interpreter server's threads take connections (`MIPYRE_TPC_DARWIN`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AcceptMode {
    /// One `SO_REUSEPORT` listener per TPC thread (the default).
    PerThreadListener,
    /// One acceptor thread fans connections out to the TPC threads.
    #[cfg(target_os = "macos")]
    Fanout,
}

fn hybrid_mode(accept: AcceptMode) -> &'static str {
    match accept {
        AcceptMode::PerThreadListener => "hybrid-inline",
        #[cfg(target_os = "macos")]
        AcceptMode::Fanout => "hybrid-inline-fanout",
    }
}

pub(crate) struct TpcServer {
    pub(crate) threads: TpcThreads,
    pub(crate) runners: OffThreadRunners,
    pub(crate) gc: GcPolicy,
    /// The CPUs are in thread order; threads past its end run unpinned.
    pub(crate) cpus: Vec<CoreId>,
    /// The TPC threads count through their sites' copies of this, which share it.
    pub(crate) counters: Arc<RunCounters>,
}

/// Every TPC thread has its own site copy ([`Site::for_thread`]); with workers, thread `i` creates
/// worker `i` on itself from `recipe` and serves with the life beside its site. Either every
/// thread has a worker or none does.
pub(crate) enum TpcThreads {
    WithWorkers {
        threads: Vec<(Arc<Site>, WorkerLife)>,
        recipe: Arc<WorkerRecipe>,
    },
    /// Every route runs on the main interpreter or a process lane.
    NoWorkers(Vec<Arc<Site>>),
}

impl TpcThreads {
    pub(crate) fn sites(&self) -> Vec<Arc<Site>> {
        match self {
            TpcThreads::WithWorkers { threads, .. } => threads.iter().map(|(site, _)| Arc::clone(site)).collect(),
            TpcThreads::NoWorkers(sites) => sites.clone(),
        }
    }

    fn into_setups(self) -> Vec<TpcThreadSetup> {
        match self {
            TpcThreads::WithWorkers { threads, recipe } => threads
                .into_iter()
                .enumerate()
                .map(|(i, (site, life))| TpcThreadSetup {
                    index: WorkerIndex::new(i),
                    site,
                    plan: Some(WorkerPlan {
                        recipe: Arc::clone(&recipe),
                        life,
                        retired: Arc::new(OnceLock::new()),
                    }),
                })
                .collect(),
            TpcThreads::NoWorkers(sites) => sites
                .into_iter()
                .enumerate()
                .map(|(i, site)| TpcThreadSetup {
                    index: WorkerIndex::new(i),
                    site,
                    plan: None,
                })
                .collect(),
        }
    }
}

struct TpcThreadSetup {
    index: WorkerIndex,
    site: Arc<Site>,
    plan: Option<WorkerPlan>,
}

/// The fanout acceptor takes a handle on `retired` before the thread starts, to skip the thread
/// once its worker retires.
struct WorkerPlan {
    recipe: Arc<WorkerRecipe>,
    life: WorkerLife,
    retired: Arc<OnceLock<Arc<str>>>,
}

/// Serves `server` on its TPC threads and prints `banner` once every thread has started.
pub(crate) fn run_tpc_workers<D: TransportDriver>(
    transport: D,
    server: TpcServer,
    accept: AcceptMode,
    banner: &str,
    run: &ServerRunStop,
    deadline: &ShutdownDeadline,
) -> Result<(), ServeError> {
    match accept {
        AcceptMode::PerThreadListener => inline::run_per_thread_listener(transport, server, banner, run, deadline),
        #[cfg(target_os = "macos")]
        AcceptMode::Fanout => fanout::run_fanout(transport, server, banner, run, deadline),
    }
}
