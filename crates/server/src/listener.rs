//! Says how a server's listeners bind, finds the sockets systemd handed over, and prints where a
//! server listens.

use mipyre_http::plan::limits::HEADER_READ_TIMEOUT;
use mipyre_transport::endpoint::Endpoints;
use mipyre_transport::tcp::listener::{BindOptions, DEFAULT_BACKLOG};

pub fn print_listening(listening: &[String]) {
    for line in listening {
        println!("  Listening on {line}");
    }
}

/// Returns how this engine's listeners listen: a connection is held in the kernel until its first
/// byte for as long as a client may take to send its request head.
pub fn bind_options() -> BindOptions {
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
/// Returns why the activation environment or a handed socket is unusable.
#[cfg(unix)]
pub fn handed_by_systemd(
    tls: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
    copies: std::num::NonZeroUsize,
) -> Result<Option<Endpoints>, mipyre_transport::tcp::passed::PassedError> {
    let handed = mipyre_transport::tcp::passed::take_from_systemd()?;
    if handed.is_empty() {
        return Ok(None);
    }
    let listeners = mipyre_transport::tcp::listener::BoundListeners::passed(handed, copies)?;
    Ok(Some(Endpoints::sharing_tls(listeners, tls)))
}

/// Returns `None`, because socket activation exists only under systemd.
#[cfg(not(unix))]
pub fn handed_by_systemd(
    _tls: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
    _copies: std::num::NonZeroUsize,
) -> Result<Option<Endpoints>, std::convert::Infallible> {
    Ok(None)
}
