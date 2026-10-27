//! Converts a server's bind and run failures into the Python exceptions `start()` and `serve()`
//! raise. The TPC and multi-thread servers raise alike, because both fail with a `ServeError`.

use pyo3::exceptions::{PyOSError, PyRuntimeError};
use pyo3::{PyErr, Python};

use mipyre_transport::tcp::listener::ListenerError;

use mipyre_server::ServeError;

/// Raises a listener failure as `OSError(errno, text)`, so a port in use is `errno.EADDRINUSE`.
pub(crate) fn listener_exception(e: ListenerError) -> PyErr {
    match e.errno() {
        Some(errno) => PyOSError::new_err((errno, e.to_string())),
        None => PyOSError::new_err(e.to_string()),
    }
}

/// Raises a malformed activation environment or an unusable handed socket as `ValueError`, and a
/// failed system call on one as `OSError(errno, text)`.
#[cfg(unix)]
pub(crate) fn passed_exception(e: mipyre_transport::tcp::passed::PassedError) -> PyErr {
    use mipyre_transport::tcp::passed::PassedError;
    match &e {
        PassedError::SystemCallFailed { source, .. } => match source.raw_os_error() {
            Some(errno) => PyOSError::new_err((errno, e.to_string())),
            None => PyOSError::new_err(e.to_string()),
        },
        PassedError::NotListening { .. }
        | PassedError::NotTcp { .. }
        | PassedError::NoSockets
        | PassedError::Environment { .. } => pyo3::exceptions::PyValueError::new_err(e.to_string()),
    }
}

#[cfg(not(unix))]
pub(crate) fn passed_exception(never: std::convert::Infallible) -> PyErr {
    match never {}
}

/// Raises a run's failure: a listener or accept failure as `OSError`, a worker start as its own
/// exception, and a runtime, thread or stop signal that could not be set up as `RuntimeError`.
pub(crate) fn serve_exception(py: Python<'_>, failed: ServeError) -> PyErr {
    match failed {
        failed @ (ServeError::ThreadPanicked { .. }
        | ServeError::RuntimeNotBuilt { .. }
        | ServeError::IoRuntimeNotBuilt(_)
        | ServeError::ClockNotStarted(_)
        | ServeError::NotSpawned(_)
        | ServeError::FailedWhileStopping { .. }
        | ServeError::LaneHost(_)
        | ServeError::ThreadsStopped { root: None, .. }) => PyRuntimeError::new_err(failed.to_string()),
        #[cfg(target_os = "macos")]
        failed @ ServeError::WorkerInboxClosed { .. } => PyRuntimeError::new_err(failed.to_string()),
        ServeError::NotListening(e) => listener_exception(e),
        ServeError::Start(e) => e.into_pyerr(py),
        failed @ ServeError::AcceptStopped(_) => PyOSError::new_err(format!("the server stopped: {failed}")),
        ServeError::ThreadsStopped { root: Some(root), .. } => serve_exception(py, *root),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mipyre_tpc::SpawnError;
    use mipyre_transport::driver::AcceptError;
    use mipyre_transport::tcp::listener::SocketCall;
    use pyo3::types::PyAnyMethods;
    use std::io;

    #[test]
    fn each_serve_error_raises_the_exception_both_servers_raise() {
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
            ServeError::IoRuntimeNotBuilt(io_error()),
            ServeError::ClockNotStarted(io_error()),
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
        let accept_stopped =
            "the server stopped: accept loop stopped: every connector of this in-memory server is gone";
        let os_errors = [
            (
                ServeError::AcceptStopped(AcceptError::ConnectorsGone),
                None,
                accept_stopped.to_string(),
            ),
            (
                ServeError::ThreadsStopped {
                    root: Some(Box::new(ServeError::AcceptStopped(AcceptError::ConnectorsGone))),
                    abandoned: vec![t()],
                },
                None,
                accept_stopped.to_string(),
            ),
            (
                ServeError::NotListening(ListenerError {
                    call: SocketCall::Bind,
                    addr: "127.0.0.1:1".parse().unwrap(),
                    source: io::Error::from_raw_os_error(libc::EADDRINUSE),
                }),
                Some(libc::EADDRINUSE),
                format!(
                    "bind for 127.0.0.1:1 failed: {}",
                    io::Error::from_raw_os_error(libc::EADDRINUSE)
                ),
            ),
        ];
        pyo3::Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            for failed in runtime_errors {
                let text = failed.to_string();
                let raised = serve_exception(py, failed);
                assert!(raised.is_instance_of::<PyRuntimeError>(py), "{raised}");
                assert_eq!(raised.value(py).to_string(), text);
            }
            for (failed, errno, text) in os_errors {
                let raised = serve_exception(py, failed);
                assert!(raised.is_instance_of::<PyOSError>(py), "{raised}");
                let value = raised.value(py);
                let raised_errno: Option<i32> = value.getattr("errno").unwrap().extract().unwrap();
                assert_eq!(raised_errno, errno);
                let raised_text: String = match errno {
                    Some(_) => value.getattr("strerror").unwrap().extract().unwrap(),
                    None => value.to_string(),
                };
                assert_eq!(raised_text, text);
            }
        });
    }
}
