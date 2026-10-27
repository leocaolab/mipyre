//! Serves on one pinned thread per core, each with its own current-thread runtime and
//! `SO_REUSEPORT` listeners; this is the default server.
//!
//! A connection never leaves its thread (`LocalSet::spawn_local`), so its future needs no `Send`
//! and there is no work stealing. When a route runs on a sub-interpreter each thread owns one
//! worker and runs `def` handlers inline, while `async def` handlers go to the async worker pool;
//! main-interpreter handlers go to the main interpreter's threads.

mod conn;
mod error;
#[cfg(target_os = "macos")]
mod fanout;
mod inline;
mod startup;
mod threads;

use std::sync::{Arc, OnceLock};

use mipyre_cpu::CoreId;
use mipyre_http::io::clock::Clock;
use mipyre_subinterp::gc::GcPolicy;
use mipyre_tpc::ShutdownDeadline;
use mipyre_transport::driver::TransportDriver;

use crate::site::Site;
use crate::ServerCounters;
use crate::ServerRunStop;
use mipyre_runtime::{InterpreterPool, MainThreads, WorkerIndex, WorkerLife, WorkerRecipe};

#[cfg(feature = "bench")]
pub use self::conn::drive_conn;
pub(crate) use self::conn::ThreadWorker;
pub use self::conn::TpcThread;
pub use self::error::ServeError;
#[cfg(feature = "bench")]
pub use self::inline::tpc_accept_loop_inline;
pub use self::startup::{log_startup, report_pinning, thread_cpus, tpc_banner};
#[cfg(feature = "bench")]
pub use self::threads::serve_on_workers;

/// How the TPC server's threads take connections (`MIPYRE_TPC_ACCEPT`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcceptMode {
    /// One listener per TPC thread (the default): its own `SO_REUSEPORT` socket on Linux, a
    /// descriptor of one shared socket on Darwin.
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
pub enum ParseAcceptModeError {
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
        (TpcThreads::WithWorkers { .. }, AcceptMode::PerThreadListener) => "sub-interp",
        #[cfg(target_os = "macos")]
        (TpcThreads::NoWorkers(_), AcceptMode::Fanout) => "main-fanout",
        #[cfg(target_os = "macos")]
        (TpcThreads::WithWorkers { .. }, AcceptMode::Fanout) => "sub-interp-fanout",
    }
}

pub struct TpcServer {
    pub threads: TpcThreads,
    /// Every thread hands its main-interpreter routes to it; it is `None` when the table has none.
    pub main: Option<Arc<MainThreads>>,
    /// Every thread hands its `async def` routes to it; it is `None` when the table has none.
    pub async_pool: Option<Arc<InterpreterPool>>,
    pub gc: GcPolicy,
    /// The CPUs are in thread order; threads past its end run unpinned.
    pub cpus: Vec<CoreId>,
    /// The TPC threads count through their sites' copies of this, which share it.
    pub counters: Arc<ServerCounters>,
}

/// Every TPC thread has its own site copy ([`Site::for_thread`]); with workers, thread `i` creates
/// worker `i` on itself from `recipe` and serves with the life beside its site. Either every
/// thread has a worker or none does.
pub enum TpcThreads {
    WithWorkers {
        threads: Vec<(Arc<Site>, WorkerLife)>,
        recipe: Arc<WorkerRecipe>,
    },
    /// Every route runs on the main interpreter or a process lane.
    NoWorkers(Vec<Arc<Site>>),
}

impl TpcThreads {
    pub fn sites(&self) -> Vec<Arc<Site>> {
        match self {
            TpcThreads::WithWorkers { threads, .. } => threads.iter().map(|(site, _)| Arc::clone(site)).collect(),
            TpcThreads::NoWorkers(sites) => sites.clone(),
        }
    }

    fn into_setups(
        self,
        main: Option<Arc<MainThreads>>,
        async_pool: Option<Arc<InterpreterPool>>,
    ) -> Vec<TpcThreadSetup> {
        let setup = |i: usize, site: Arc<Site>, plan: Option<WorkerPlan>| TpcThreadSetup {
            index: WorkerIndex::new(i),
            site,
            plan,
            main: main.clone(),
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
    main: Option<Arc<MainThreads>>,
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
pub fn run_tpc<D: TransportDriver>(
    transport: D,
    server: TpcServer,
    accept: AcceptMode,
    announce: impl FnOnce(&'static str, &[String]),
    run: &ServerRunStop,
    deadline: &ShutdownDeadline,
    spawner: &mut impl mipyre_tpc::Spawner,
) -> Result<(), ServeError> {
    Clock::process().start().map_err(ServeError::ClockNotStarted)?;
    match accept {
        AcceptMode::PerThreadListener => {
            inline::run_per_thread_listener(transport, server, announce, run, deadline, spawner)
        }
        #[cfg(target_os = "macos")]
        AcceptMode::Fanout => fanout::run_fanout(transport, server, announce, run, deadline, spawner),
    }
}
