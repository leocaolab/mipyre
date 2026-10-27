//! The ways a TPC or multi-thread server run fails.

use mipyre_tpc::SpawnError;
use mipyre_transport::driver::AcceptError;
use mipyre_transport::tcp::listener::ListenerError;

use mipyre_runtime::StartError;

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("TPC thread {thread} panicked: {message}")]
    ThreadPanicked { thread: String, message: String },
    #[error("TPC thread {thread} could not build its runtime: {source}")]
    RuntimeNotBuilt {
        thread: String,
        #[source]
        source: std::io::Error,
    },
    #[error("tokio runtime error: {0}")]
    IoRuntimeNotBuilt(#[source] std::io::Error),
    /// Without the clock's thread no connection deadline fires, so a client could hold a
    /// connection open for as long as it likes.
    #[error("the thread that ticks the connections' clock could not start: {0}")]
    ClockNotStarted(#[source] std::io::Error),
    #[error(transparent)]
    NotSpawned(#[from] SpawnError),
    #[error(transparent)]
    NotListening(#[from] ListenerError),
    /// Every thread creates its own worker, and one failure aborts them all ([`StartGate`]).
    #[error(transparent)]
    Start(#[from] StartError),
    #[error("accept loop stopped: {0}")]
    AcceptStopped(#[from] AcceptError),
    #[error("process lane host could not start: {0}")]
    LaneHost(#[source] std::io::Error),
    /// The fanout acceptor found TPC worker `worker`'s inbox closed while the run was not
    /// stopping, so it can hand that worker no connection.
    #[cfg(target_os = "macos")]
    #[error("the fanout acceptor found TPC worker {worker}'s inbox closed while the server was serving")]
    WorkerInboxClosed { worker: usize },
    /// The `abandoned` threads still hold their interpreters, so the process must not finalize
    /// (see `boot`); `root` is the failure that stopped the others, if one did.
    #[error("TPC threads, or blocking calls of theirs, still running past the shutdown grace: {abandoned:?}")]
    ThreadsStopped {
        root: Option<Box<ServeError>>,
        abandoned: Vec<String>,
    },
    /// The server stopped as asked, from outside or because every thread ended, and these threads
    /// failed as it did; each failure was logged with its error.
    #[error("TPC threads failed while the server stopped: {threads:?}")]
    FailedWhileStopping { threads: Vec<String> },
}

impl ServeError {
    pub fn abandoned_threads(&self) -> &[String] {
        match self {
            ServeError::ThreadsStopped { abandoned, .. } => abandoned,
            ServeError::ThreadPanicked { .. }
            | ServeError::RuntimeNotBuilt { .. }
            | ServeError::IoRuntimeNotBuilt(_)
            | ServeError::ClockNotStarted(_)
            | ServeError::NotSpawned(_)
            | ServeError::FailedWhileStopping { .. }
            | ServeError::NotListening(_)
            | ServeError::Start(_)
            | ServeError::AcceptStopped(_)
            | ServeError::LaneHost(_) => &[],
            #[cfg(target_os = "macos")]
            ServeError::WorkerInboxClosed { .. } => &[],
        }
    }
}
