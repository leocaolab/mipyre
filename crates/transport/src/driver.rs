//! Defines the driver a server's accept loops take connections through.
//!
//! The loops are generic over [`TransportDriver`] rather than matching on an enum, so no call on
//! a connection's path branches on which driver it is.

use std::future::Future;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::tcp::listener::ListenerError;

/// One way a server takes its connections.
///
/// An accepted connection is registered with no runtime yet, so it can move to the thread that
/// serves it before [`TransportDriver::into_stream`] sets it up there.
pub trait TransportDriver: Sized + Send + 'static {
    type AcceptSource: Send + 'static;
    type Connection: Send + 'static;
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    fn accept_loops(&self) -> usize;

    /// Splits into one share per accept loop, in loop order, before the threads start.
    fn into_loops(self) -> Vec<Self>;

    /// Returns one line per listening address, for the banner and log.
    fn listening(&self) -> Vec<String>;

    /// Registers this share with the current runtime, so it must be called on the thread that
    /// accepts from it.
    ///
    /// # Errors
    ///
    /// Returns the [`ListenerError`] of a socket the runtime would not take.
    fn register(self) -> Result<Self::AcceptSource, ListenerError>;

    fn accept(source: &mut Self::AcceptSource) -> impl Future<Output = Result<Self::Connection, AcceptError>> + Send;

    fn peer(connection: &Self::Connection) -> Peer;

    /// Sets `connection` up on the thread that serves it; `None` means it failed and the
    /// failure was already logged and counted.
    fn into_stream(connection: Self::Connection) -> impl Future<Output = Option<Self::Stream>> + Send;
}

/// Why an accept loop's source gives no more connections; retrying cannot help, so the loop
/// and its server end.
#[derive(Debug, thiserror::Error)]
pub enum AcceptError {
    #[error("the listener is unusable: {0}")]
    ListenerBroken(#[source] ListenerError),
    #[error("every connector of this in-memory server is gone")]
    ConnectorsGone,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Peer {
    Ip(std::net::IpAddr),
    /// The connection was made in this process, so there is no peer address.
    InMemory,
}
