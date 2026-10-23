//! The ways a TPC run fails, and the exception each one raises.

use mipyre_tpc::SpawnError;
use mipyre_transport::driver::AcceptError;
use mipyre_transport::tcp::listener::ListenerError;

use crate::runtime::StartError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ServeError {
    #[error("TPC thread {thread} panicked: {message}")]
    ThreadPanicked { thread: String, message: String },
    #[error("TPC thread {thread} could not build its runtime: {source}")]
    RuntimeNotBuilt {
        thread: String,
        #[source]
        source: std::io::Error,
    },
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
    pub(crate) fn abandoned_threads(&self) -> &[String] {
        match self {
            ServeError::ThreadsStopped { abandoned, .. } => abandoned,
            ServeError::ThreadPanicked { .. }
            | ServeError::RuntimeNotBuilt { .. }
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

    /// Converts this error into the exception the multi-thread server (`MIPYRE_TPC=0`) raises for
    /// the same failure, so both servers raise alike.
    pub(crate) fn into_pyerr(self, py: pyo3::Python<'_>) -> pyo3::PyErr {
        match self {
            // The multi-thread server raises `RuntimeError` for a runtime, a thread or a stop
            // signal it could not set up.
            failed @ (ServeError::ThreadPanicked { .. }
            | ServeError::RuntimeNotBuilt { .. }
            | ServeError::NotSpawned(_)
            | ServeError::FailedWhileStopping { .. }) => pyo3::exceptions::PyRuntimeError::new_err(failed.to_string()),
            ServeError::NotListening(e) => crate::serve::listener::listener_error(e),
            ServeError::Start(e) => e.into_pyerr(py),
            ServeError::AcceptStopped(e) => crate::serve::listener::accept_stopped_error(&e),
            failed @ ServeError::LaneHost(_) => pyo3::exceptions::PyRuntimeError::new_err(failed.to_string()),
            #[cfg(target_os = "macos")]
            failed @ ServeError::WorkerInboxClosed { .. } => {
                pyo3::exceptions::PyRuntimeError::new_err(failed.to_string())
            }
            ServeError::ThreadsStopped { root: Some(root), .. } => root.into_pyerr(py),
            failed @ ServeError::ThreadsStopped { root: None, .. } => {
                pyo3::exceptions::PyRuntimeError::new_err(failed.to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks that each way a TPC run fails raises the multi-thread server's exception for the
    /// same failure; `Start` is tested with `StartError::into_pyerr`.
    #[test]
    fn each_serve_error_raises_the_multi_thread_servers_exception() {
        use mipyre_transport::tcp::listener::{ListenerError, SocketCall};
        use pyo3::exceptions::{PyOSError, PyRuntimeError};
        use std::io;

        let io_error = || io::Error::from_raw_os_error(libc::EAGAIN);
        let t = || "mipyre-tpc-0".to_string();
        let runtime_errors = [
            ServeError::NotSpawned(SpawnError {
                thread: t(),
                source: io_error(),
            }),
            ServeError::RuntimeNotBuilt {
                thread: t(),
                source: io_error(),
            },
            ServeError::ThreadPanicked {
                thread: t(),
                message: "boom".into(),
            },
            ServeError::FailedWhileStopping { threads: vec![t()] },
            ServeError::LaneHost(io_error()),
            ServeError::ThreadsStopped {
                root: None,
                abandoned: vec![t()],
            },
        ];
        let os_errors = [
            ServeError::NotListening(ListenerError {
                call: SocketCall::Bind,
                addr: "127.0.0.1:1".parse().unwrap(),
                source: io::Error::from_raw_os_error(libc::EADDRINUSE),
            }),
            ServeError::AcceptStopped(AcceptError::ConnectorsGone),
            ServeError::ThreadsStopped {
                root: Some(Box::new(ServeError::AcceptStopped(AcceptError::ConnectorsGone))),
                abandoned: vec![t()],
            },
        ];
        pyo3::Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            for failed in runtime_errors {
                let text = failed.to_string();
                let raised = failed.into_pyerr(py);
                assert!(raised.is_instance_of::<PyRuntimeError>(py), "{raised}");
                assert_eq!(raised.value(py).to_string(), text);
            }
            for failed in os_errors {
                let raised = failed.into_pyerr(py);
                assert!(raised.is_instance_of::<PyOSError>(py), "{raised}");
            }
        });
    }
}
