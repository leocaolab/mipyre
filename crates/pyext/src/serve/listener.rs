//! Turns listener and accept failures into the Python exceptions `start()` and `serve()` raise,
//! and prints where a server listens.

use mipyre_transport::tcp::listener::{BindOptions, ListenerError, DEFAULT_BACKLOG};
use mipyre_http::plan::limits::HEADER_READ_TIMEOUT;
use mipyre_transport::driver::AcceptError;
use mipyre_transport::endpoint::Endpoints;

pub(crate) fn print_listening(listening: &[String]) {
    for line in listening {
        println!("  Listening on {line}");
    }
}

/// Raises a listener failure as `OSError(errno, text)`, so a port in use is `errno.EADDRINUSE`.
/// It is a function rather than a `From` impl because both types are foreign to this crate.
pub(crate) fn listener_error(e: ListenerError) -> pyo3::PyErr {
    match e.errno() {
        Some(errno) => pyo3::exceptions::PyOSError::new_err((errno, e.to_string())),
        None => pyo3::exceptions::PyOSError::new_err(e.to_string()),
    }
}

/// Builds the `OSError` that `serve()` raises once an accept loop can take no more connections.
pub(crate) fn accept_stopped_error(e: &AcceptError) -> pyo3::PyErr {
    pyo3::exceptions::PyOSError::new_err(format!("the server stopped: accept loop stopped: {e}"))
}

/// Returns how this engine's listeners listen: a connection is held in the kernel until its first
/// byte for as long as a client may take to send its request head.
pub(crate) fn bind_options() -> BindOptions {
    BindOptions {
        backlog: DEFAULT_BACKLOG,
        defer_accept: HEADER_READ_TIMEOUT,
    }
}

/// Returns the sockets systemd handed this process, each with the run's TLS acceptor if there is
/// one, spread over `copies` accept loops, or `None` when nothing was handed over.
///
/// # Errors
///
/// A malformed activation environment or an unusable handed socket is `ValueError`; a failed
/// system call on one is `OSError`.
#[cfg(unix)]
pub(crate) fn handed_by_systemd(
    tls: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
    copies: std::num::NonZeroUsize,
) -> pyo3::PyResult<Option<Endpoints>> {
    use mipyre_transport::tcp::passed::take_from_systemd;
    let handed = take_from_systemd().map_err(passed_error)?;
    if handed.is_empty() {
        return Ok(None);
    }
    let listeners = mipyre_transport::tcp::listener::BoundListeners::passed(handed, copies).map_err(passed_error)?;
    Ok(Some(Endpoints::sharing_tls(listeners, tls)))
}

/// Returns `None`, because socket activation exists only under systemd.
#[cfg(not(unix))]
pub(crate) fn handed_by_systemd(
    _tls: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
    _copies: std::num::NonZeroUsize,
) -> pyo3::PyResult<Option<Endpoints>> {
    Ok(None)
}

#[cfg(unix)]
fn passed_error(e: mipyre_transport::tcp::passed::PassedError) -> pyo3::PyErr {
    use mipyre_transport::tcp::passed::PassedError;
    match &e {
        PassedError::SystemCallFailed { source, .. } => match source.raw_os_error() {
            Some(errno) => pyo3::exceptions::PyOSError::new_err((errno, e.to_string())),
            None => pyo3::exceptions::PyOSError::new_err(e.to_string()),
        },
        PassedError::NotListening { .. }
        | PassedError::NotTcp { .. }
        | PassedError::NoSockets
        | PassedError::Environment { .. } => pyo3::exceptions::PyValueError::new_err(e.to_string()),
    }
}
