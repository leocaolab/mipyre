//! Serves on one pinned thread per core, each with its own current-thread runtime and
//! `SO_REUSEPORT` listeners; this is the default server (docs/tpc-rearch.md).
//!
//! A connection never leaves its thread (`LocalSet::spawn_local`), so its future needs no `Send`
//! and there is no work stealing. When a route runs on a sub-interpreter each thread owns one
//! worker and runs `def` handlers inline, while `async def` handlers go to the async worker pool;
//! main-interpreter handlers go to the bridge, which is the main interpreter's one line of
//! execution.

mod conn;
mod error;
#[cfg(target_os = "macos")]
mod fanout;
mod inline;
mod startup;
mod threads;

use std::sync::{Arc, OnceLock};

use mipyre_cpu::CoreId;
use mipyre_subinterp::gc::GcPolicy;
use mipyre_tpc::ShutdownDeadline;
use mipyre_transport::driver::TransportDriver;

use crate::runtime::{InterpreterPool, MainInterpBridge, ServerRunStop, WorkerIndex, WorkerLife, WorkerRecipe};
use crate::serve::site::Site;
use crate::telemetry::RunCounters;

#[cfg(feature = "bench")]
pub(crate) use self::conn::drive_conn;
pub(crate) use self::conn::{ThreadWorker, TpcThread};
pub(crate) use self::error::ServeError;
#[cfg(feature = "bench")]
pub(crate) use self::inline::tpc_accept_loop_inline;
pub(crate) use self::startup::{log_startup, report_pinning, thread_cpus, tpc_banner};
#[cfg(feature = "bench")]
pub(crate) use self::threads::serve_on_workers;

/// How the TPC server's threads take connections (`MIPYRE_TPC_DARWIN`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AcceptMode {
    /// One `SO_REUSEPORT` listener per TPC thread (the default).
    PerThreadListener,
    /// One acceptor thread fans connections out to the TPC threads.
    #[cfg(target_os = "macos")]
    Fanout,
}

impl std::fmt::Display for AcceptMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            AcceptMode::PerThreadListener => "listener",
            #[cfg(target_os = "macos")]
            AcceptMode::Fanout => "fanout",
        })
    }
}

/// Text that names no accept mode on this platform.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ParseAcceptModeError {
    #[error("{0:?} is not an accept mode (expected \"listener\" or \"fanout\")")]
    Unknown(String),
    #[cfg(not(target_os = "macos"))]
    #[error("the fanout accept mode exists on macOS only")]
    FanoutOffMacos,
}

impl std::str::FromStr for AcceptMode {
    type Err = ParseAcceptModeError;

    fn from_str(text: &str) -> Result<Self, ParseAcceptModeError> {
        match text {
            "listener" => Ok(AcceptMode::PerThreadListener),
            #[cfg(target_os = "macos")]
            "fanout" => Ok(AcceptMode::Fanout),
            #[cfg(not(target_os = "macos"))]
            "fanout" => Err(ParseAcceptModeError::FanoutOffMacos),
            other => Err(ParseAcceptModeError::Unknown(other.to_string())),
        }
    }
}

/// Names the server in its start banner and log: `main` when no thread has a worker.
fn topology_label(threads: &TpcThreads, accept: AcceptMode) -> &'static str {
    match (threads, accept) {
        (TpcThreads::NoWorkers(_), AcceptMode::PerThreadListener) => "main",
        (TpcThreads::WithWorkers { .. }, AcceptMode::PerThreadListener) => "hybrid-inline",
        #[cfg(target_os = "macos")]
        (TpcThreads::NoWorkers(_), AcceptMode::Fanout) => "main-fanout",
        #[cfg(target_os = "macos")]
        (TpcThreads::WithWorkers { .. }, AcceptMode::Fanout) => "hybrid-inline-fanout",
    }
}

pub(crate) struct TpcServer {
    pub(crate) threads: TpcThreads,
    /// Every thread hands its main-interpreter routes to it; it is `None` when the table has none.
    pub(crate) main_bridge: Option<Arc<MainInterpBridge>>,
    /// Every thread hands its `async def` routes to it; it is `None` when the table has none.
    pub(crate) async_pool: Option<Arc<InterpreterPool>>,
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

    fn into_setups(
        self,
        main_bridge: Option<Arc<MainInterpBridge>>,
        async_pool: Option<Arc<InterpreterPool>>,
    ) -> Vec<TpcThreadSetup> {
        let setup = |i: usize, site: Arc<Site>, plan: Option<WorkerPlan>| TpcThreadSetup {
            index: WorkerIndex::new(i),
            site,
            plan,
            main_bridge: main_bridge.clone(),
            async_pool: async_pool.clone(),
        };
        match self {
            TpcThreads::WithWorkers { threads, recipe } => threads
                .into_iter()
                .enumerate()
                .map(|(i, (site, life))| {
                    let plan = WorkerPlan {
                        recipe: Arc::clone(&recipe),
                        life,
                        retired: Arc::new(OnceLock::new()),
                    };
                    setup(i, site, Some(plan))
                })
                .collect(),
            TpcThreads::NoWorkers(sites) => sites
                .into_iter()
                .enumerate()
                .map(|(i, site)| setup(i, site, None))
                .collect(),
        }
    }
}

struct TpcThreadSetup {
    index: WorkerIndex,
    site: Arc<Site>,
    plan: Option<WorkerPlan>,
    main_bridge: Option<Arc<MainInterpBridge>>,
    async_pool: Option<Arc<InterpreterPool>>,
}

/// The fanout acceptor takes a handle on `retired` before the thread starts, to skip the thread
/// once its worker retires.
struct WorkerPlan {
    recipe: Arc<WorkerRecipe>,
    life: WorkerLife,
    retired: Arc<OnceLock<Arc<str>>>,
}

/// Serves `server` on TPC threads `spawner` starts and tells `announce` the topology and the
/// addresses once every thread has started.
pub(crate) fn run_tpc<D: TransportDriver>(
    transport: D,
    server: TpcServer,
    accept: AcceptMode,
    announce: impl FnOnce(&'static str, &[String]),
    run: &ServerRunStop,
    deadline: &ShutdownDeadline,
    spawner: &mut impl mipyre_tpc::Spawner,
) -> Result<(), ServeError> {
    match accept {
        AcceptMode::PerThreadListener => {
            inline::run_per_thread_listener(transport, server, announce, run, deadline, spawner)
        }
        #[cfg(target_os = "macos")]
        AcceptMode::Fanout => fanout::run_fanout(transport, server, announce, run, deadline, spawner),
    }
}
