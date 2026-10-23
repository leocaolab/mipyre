//! Makes connections to a server in the same process, taking no port.
//!
//! Each connection is a `socketpair` rather than an in-memory pipe, so a blocking client such as
//! `http.client` gets the kernel's timeouts, half-close and concurrent reads and writes. From the
//! accept loop on, it is served exactly as a TCP connection, but it has no peer address, no TLS
//! and no TCP options.

use crate::sync::{AtomicUsize, Ordering};
use std::num::NonZeroUsize;
use std::os::unix::net::UnixStream;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::driver::{AcceptError, Peer, TransportDriver};
use crate::stream::events::{self, StreamEvent};
use crate::tcp::listener::ListenerError;

/// An inbox holds as many connections as a TCP listener's backlog (`tcp::listener::DEFAULT_BACKLOG`).
const INBOX_CAPACITY: usize = 8192;

/// Stands in for one accept loop's listening socket.
pub struct MemoryInbox {
    rx: mpsc::Receiver<UnixStream>,
}

impl MemoryInbox {
    /// Returns the next connection, or `None` once every connector is gone.
    pub async fn accept(&mut self) -> Option<UnixStream> {
        self.rx.recv().await
    }

    /// Polls for the next connection, giving `Ready(None)` once every connector is gone.
    pub fn poll_accept(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<UnixStream>> {
        self.rx.poll_recv(cx)
    }
}

/// Sends each connection to the next accept loop whose inbox is open; a clone is the same
/// connector.
#[derive(Clone)]
pub struct MemoryConnector {
    inboxes: Arc<[mpsc::Sender<UnixStream>]>,
    next: Arc<AtomicUsize>,
}

#[derive(Debug, thiserror::Error)]
pub enum MemoryConnectError {
    #[error("the server takes no more connections")]
    Refused,
    #[error("every accept loop's backlog ({INBOX_CAPACITY} connections) is full")]
    BacklogFull,
    #[error("socketpair() failed: {0}")]
    Socketpair(#[source] std::io::Error),
}

/// Makes one inbox per accept loop and the connector that fills them.
pub fn memory_listeners(copies: NonZeroUsize) -> (MemorySource, MemoryConnector) {
    with_backlog(copies, INBOX_CAPACITY)
}

fn with_backlog(copies: NonZeroUsize, backlog: usize) -> (MemorySource, MemoryConnector) {
    let (txs, inboxes): (Vec<_>, Vec<_>) = (0..copies.get())
        .map(|_| {
            let (tx, rx) = mpsc::channel(backlog);
            (tx, MemoryInbox { rx })
        })
        .unzip();
    (
        MemorySource { inboxes, next: 0 },
        MemoryConnector {
            inboxes: txs.into(),
            next: Arc::new(AtomicUsize::new(0)),
        },
    )
}

/// A memory server's inboxes, or one accept loop's share of them, polled in turn.
pub struct MemorySource {
    inboxes: Vec<MemoryInbox>,
    next: usize,
}

impl MemorySource {
    async fn accept(&mut self) -> Result<UnixStream, AcceptError> {
        loop {
            if self.inboxes.is_empty() {
                return Err(AcceptError::ConnectorsGone);
            }
            let (received, index) = std::future::poll_fn(|cx| {
                let n = self.inboxes.len();
                for k in 0..n {
                    let i = (self.next + k) % n;
                    if let std::task::Poll::Ready(r) = self.inboxes[i].poll_accept(cx) {
                        return std::task::Poll::Ready((r, i));
                    }
                }
                std::task::Poll::Pending
            })
            .await;
            match received {
                Some(stream) => {
                    self.next = (index + 1) % self.inboxes.len();
                    return Ok(stream);
                }
                None => {
                    self.inboxes.remove(index);
                    self.next = 0;
                }
            }
        }
    }
}

impl TransportDriver for MemorySource {
    type AcceptSource = MemorySource;
    type Connection = UnixStream;
    type Stream = tokio::net::UnixStream;

    fn accept_loops(&self) -> usize {
        self.inboxes.len()
    }

    fn into_loops(self) -> Vec<Self> {
        self.inboxes
            .into_iter()
            .map(|inbox| MemorySource {
                inboxes: vec![inbox],
                next: 0,
            })
            .collect()
    }

    fn listening(&self) -> Vec<String> {
        vec!["in-memory connections (no listening socket)".to_string()]
    }

    fn register(self) -> Result<MemorySource, ListenerError> {
        Ok(self)
    }

    #[inline]
    fn accept(source: &mut MemorySource) -> impl std::future::Future<Output = Result<UnixStream, AcceptError>> + Send {
        source.accept()
    }

    #[inline]
    fn peer(_connection: &UnixStream) -> Peer {
        Peer::InMemory
    }

    /// Counts a failed registration as for a TCP connection.
    async fn into_stream(connection: UnixStream) -> Option<tokio::net::UnixStream> {
        match tokio::net::UnixStream::from_std(connection) {
            Ok(stream) => Some(stream),
            Err(e) => {
                if let Some(held) = events::degrade(StreamEvent::RegisterFailed) {
                    tracing::warn!(
                        error = %e,
                        held,
                        "an in-memory connection could not be registered with its thread's runtime; dropped"
                    );
                }
                None
            }
        }
    }
}

impl MemoryConnector {
    /// Queues the server's end at an accept loop and returns the client's end in blocking mode,
    /// as `socket.socket()` makes one.
    ///
    /// An inbox whose loop stopped accepting is passed over, as the kernel passes over a closed
    /// `SO_REUSEPORT` socket.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryConnectError::Refused`] when every loop stopped accepting,
    /// [`MemoryConnectError::BacklogFull`] when every open inbox is full, and
    /// [`MemoryConnectError::Socketpair`] when the OS would not make the pair.
    pub fn connect(&self) -> Result<UnixStream, MemoryConnectError> {
        let n = self.inboxes.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let (client, mut server) = UnixStream::pair().map_err(MemoryConnectError::Socketpair)?;
        // Tokio takes a socket only in non-blocking mode.
        server.set_nonblocking(true).map_err(MemoryConnectError::Socketpair)?;
        let mut full = false;
        for k in 0..n {
            let inbox = &self.inboxes[start.wrapping_add(k) % n];
            match inbox.try_send(server) {
                Ok(()) => return Ok(client),
                Err(mpsc::error::TrySendError::Full(unsent)) => {
                    full = true;
                    server = unsent;
                }
                Err(mpsc::error::TrySendError::Closed(unsent)) => server = unsent,
            }
        }
        if full {
            Err(MemoryConnectError::BacklogFull)
        } else {
            Err(MemoryConnectError::Refused)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn copies(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("n > 0")
    }

    #[tokio::test]
    async fn a_memory_source_accepts_until_its_connectors_are_gone() {
        let (source, connector) = memory_listeners(copies(1));
        let mut source = source.register().expect("nothing to register");
        let _client = connector.connect().expect("open");
        let accepted = MemorySource::accept(&mut source).await.expect("the connection");
        assert_eq!(MemorySource::peer(&accepted), Peer::InMemory);
        drop(connector);
        assert!(matches!(
            MemorySource::accept(&mut source).await,
            Err(AcceptError::ConnectorsGone)
        ));
    }

    #[tokio::test]
    async fn a_connection_is_one_socket_pair_between_the_client_and_an_accept_loop() {
        let (MemorySource { mut inboxes, .. }, connector) = memory_listeners(copies(1));
        let mut client = connector.connect().expect("the inbox is open");
        let accepted = inboxes[0].accept().await.expect("the connection is queued");
        let mut server = MemorySource::into_stream(accepted).await.expect("registered");
        client.write_all(b"ping").expect("write");
        let mut got = [0u8; 4];
        server.read_exact(&mut got).await.expect("read");
        assert_eq!(&got, b"ping");
        server.write_all(b"pong").await.expect("write");
        drop(server);
        let mut rest = Vec::new();
        client.read_to_end(&mut rest).expect("read to the end");
        assert_eq!(rest, b"pong");
    }

    #[tokio::test]
    async fn connections_go_to_the_accept_loops_in_turn() {
        let (MemorySource { mut inboxes, .. }, connector) = memory_listeners(copies(2));
        let _clients: Vec<_> = (0..4).map(|_| connector.connect().expect("open")).collect();
        for inbox in &mut inboxes {
            for _ in 0..2 {
                assert!(inbox.accept().await.is_some());
            }
            inbox.rx.close();
            assert!(inbox.accept().await.is_none(), "two each, no more");
        }
    }

    #[test]
    fn a_closed_inbox_is_passed_over_and_all_closed_is_refused() {
        let (MemorySource { mut inboxes, .. }, connector) = memory_listeners(copies(2));
        inboxes[0].rx.close();
        for _ in 0..3 {
            connector.connect().expect("the second loop still accepts");
        }
        inboxes[1].rx.close();
        assert!(matches!(connector.connect(), Err(MemoryConnectError::Refused)));
    }

    #[test]
    fn a_dropped_server_refuses() {
        let (MemorySource { inboxes, .. }, connector) = memory_listeners(copies(3));
        drop(inboxes);
        assert!(matches!(connector.connect(), Err(MemoryConnectError::Refused)));
    }

    #[test]
    fn a_full_backlog_is_its_own_error() {
        // Each connection is two descriptors, so the backlog stays small.
        const BACKLOG: usize = 4;
        let (_inboxes, connector) = with_backlog(copies(1), BACKLOG);
        let held: Vec<_> = (0..BACKLOG)
            .map(|_| connector.connect().expect("room in the backlog"))
            .collect();
        assert!(matches!(connector.connect(), Err(MemoryConnectError::BacklogFull)));
        drop(held);
    }
}
