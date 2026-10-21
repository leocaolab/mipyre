//! Pairs each TCP listener with its TLS acceptor, if it has one, and accepts from one accept
//! loop's listeners in turn; it is the one module where TCP and TLS meet.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;

use crate::driver::{AcceptError, Peer, TransportDriver};
use crate::stream::events::{self, StreamEvent};
use crate::tcp::listener::{BindOptions, BoundListeners, Listener, ListenerError, TcpAcceptor, TcpConnection};
use crate::tcp::loops::PollTurn;
use crate::tls;


// One IO type lets the accept loop hand the protocol above a connection whichever listener it came on.
pin_project! {
    #[project = ConnectionStreamProj]
    pub enum ConnectionStream {
        Plain { #[pin] inner: TcpStream },
        Tls { #[pin] inner: tokio_rustls::server::TlsStream<TcpStream> },
    }
}

impl AsyncRead for ConnectionStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.project() {
            ConnectionStreamProj::Plain { inner } => inner.poll_read(cx, buf),
            ConnectionStreamProj::Tls { inner } => inner.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ConnectionStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.project() {
            ConnectionStreamProj::Plain { inner } => inner.poll_write(cx, buf),
            ConnectionStreamProj::Tls { inner } => inner.poll_write(cx, buf),
        }
    }
    /// Forwards vectored writes, because the trait default would write only the first buffer
    /// and split hyper's head and body into two syscalls.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        match self.project() {
            ConnectionStreamProj::Plain { inner } => inner.poll_write_vectored(cx, bufs),
            ConnectionStreamProj::Tls { inner } => inner.poll_write_vectored(cx, bufs),
        }
    }
    fn is_write_vectored(&self) -> bool {
        match self {
            ConnectionStream::Plain { inner } => inner.is_write_vectored(),
            ConnectionStream::Tls { inner } => inner.is_write_vectored(),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.project() {
            ConnectionStreamProj::Plain { inner } => inner.poll_flush(cx),
            ConnectionStreamProj::Tls { inner } => inner.poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.project() {
            ConnectionStreamProj::Plain { inner } => inner.poll_shutdown(cx),
            ConnectionStreamProj::Tls { inner } => inner.poll_shutdown(cx),
        }
    }
}


#[derive(Clone)]
pub struct ListenerSpec {
    pub addr: SocketAddr,
    pub tls: Option<Arc<TlsAcceptor>>,
}

impl ListenerSpec {
    /// Builds one server's listeners: `addr`, then the extra TLS ports, then the extra plain
    /// ports, all on `addr`'s host. With extra TLS ports `addr` serves plain; without them it
    /// speaks TLS when an acceptor is given.
    ///
    /// # Errors
    ///
    /// Returns [`ListenerSetError`] for extra TLS ports without an acceptor or a repeated port.
    pub fn set(
        addr: SocketAddr,
        tls: Option<Arc<TlsAcceptor>>,
        extra_tls_ports: &[u16],
        extra_plain_ports: &[u16],
    ) -> Result<ListenerSet, ListenerSetError> {
        let mut specs = match (tls, extra_tls_ports) {
            (tls, []) => vec![ListenerSpec { addr, tls }],
            (Some(acceptor), ports) => std::iter::once(ListenerSpec { addr, tls: None })
                .chain(ports.iter().map(|&port| ListenerSpec {
                    addr: SocketAddr::new(addr.ip(), port),
                    tls: Some(Arc::clone(&acceptor)),
                }))
                .collect(),
            (None, ports) => return Err(ListenerSetError::ExtraTlsWithoutCert(ports.to_vec())),
        };
        specs.extend(extra_plain_ports.iter().map(|&port| ListenerSpec {
            addr: SocketAddr::new(addr.ip(), port),
            tls: None,
        }));
        // Each port 0 is its own kernel pick, so it may repeat.
        let mut seen = std::collections::HashSet::new();
        if let Some(port) = specs
            .iter()
            .map(|spec| spec.addr.port())
            .find(|&port| port != 0 && !seen.insert(port))
        {
            return Err(ListenerSetError::DuplicatePort(port));
        }
        Ok(ListenerSet(specs))
    }
}

/// It holds at least one listener and each port once, because only [`ListenerSpec::set`] and
/// [`ListenerSet::one`] make one.
#[derive(Clone)]
pub struct ListenerSet(Vec<ListenerSpec>);

impl ListenerSet {
    pub fn one(spec: ListenerSpec) -> Self {
        ListenerSet(vec![spec])
    }

    pub fn specs(&self) -> &[ListenerSpec] {
        &self.0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ListenerSetError {
    /// Serving without these ports would leave the operator believing they are protected.
    #[error(
        "extra_tls_ports {0:?} configured but tls_cert/tls_key not set; these ports cannot be \
         opened as TLS. Provide tls_cert+tls_key or remove the port list."
    )]
    ExtraTlsWithoutCert(Vec<u16>),
    #[error(
        "port {0} is configured twice (the port, extra_tls_ports and extra_plain_ports must \
         not repeat one another); a server listens on each port once"
    )]
    DuplicatePort(u16),
}

/// The address holds the kernel's pick in place of a port 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ListenAddress {
    pub addr: SocketAddr,
    pub tls: bool,
}

impl std::fmt::Display for ListenAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let scheme = if self.tls { "https" } else { "http" };
        write!(f, "{scheme}://{}", self.addr)
    }
}

/// One listening socket and the TLS its connections get.
struct Endpoint {
    listener: Listener,
    tls: Option<Arc<TlsAcceptor>>,
}

impl Endpoint {
    fn address(&self) -> ListenAddress {
        ListenAddress {
            addr: self.listener.addr,
            tls: self.tls.is_some(),
        }
    }
}

/// Holds every listener of one server with its TLS, bound before any serving thread exists, or
/// one accept loop's share of them.
pub struct Endpoints {
    /// Holds one group per accept loop, as [`BoundListeners`] does; never empty at either level.
    groups: Vec<Vec<Endpoint>>,
}

impl Endpoints {
    /// Binds `copies` sockets for each of `specs`, one group per accept loop, each socket with its
    /// spec's TLS.
    ///
    /// # Errors
    ///
    /// Returns the first failed socket call, such as `AddrInUse`, before any thread exists.
    pub fn bind(specs: &ListenerSet, copies: NonZeroUsize, options: BindOptions) -> Result<Self, ListenerError> {
        let addrs: Vec<SocketAddr> = specs.specs().iter().map(|spec| spec.addr).collect();
        let groups = BoundListeners::bind(&addrs, copies, options)?
            .into_groups()
            .into_iter()
            .map(|group| {
                group
                    .into_iter()
                    .zip(specs.specs())
                    .map(|(listener, spec)| Endpoint {
                        listener,
                        tls: spec.tls.clone(),
                    })
                    .collect()
            })
            .collect();
        Ok(Endpoints { groups })
    }

    /// Serves every socket of `listeners` with the same `tls`, as for sockets systemd handed in.
    pub fn sharing_tls(listeners: BoundListeners, tls: Option<Arc<TlsAcceptor>>) -> Self {
        let groups = listeners
            .into_groups()
            .into_iter()
            .map(|group| {
                group
                    .into_iter()
                    .map(|listener| Endpoint {
                        listener,
                        tls: tls.clone(),
                    })
                    .collect()
            })
            .collect();
        Endpoints { groups }
    }

    /// Returns where each listener is bound, in the order given, each address once: a group can
    /// hold several handed-in sockets of one address.
    pub fn bound(&self) -> Vec<ListenAddress> {
        let mut bound: Vec<ListenAddress> = Vec::new();
        for address in self.groups[0].iter().map(Endpoint::address) {
            if !bound.contains(&address) {
                bound.push(address);
            }
        }
        bound
    }

    /// Returns the first listener's address, which is the server's own.
    pub fn primary(&self) -> ListenAddress {
        self.groups[0][0].address()
    }
}

/// A connection accepted from an endpoint, with the TLS its listener asks for.
pub struct EndpointConnection {
    pub tcp: TcpConnection,
    pub tls: Option<Arc<TlsAcceptor>>,
}

/// Merges one accept loop's endpoints into one stream of connections.
pub struct EndpointSource {
    /// Never empty, because an [`Endpoints`] group never is.
    slots: Vec<(TcpAcceptor, Option<Arc<TlsAcceptor>>)>,
    turn: PollTurn,
}

impl EndpointSource {
    /// Registers every socket with the current runtime, so it must run on the thread that
    /// accepts from them.
    ///
    /// # Errors
    ///
    /// Returns the error of the first socket the runtime refuses.
    pub fn new(endpoints: Endpoints) -> Result<Self, ListenerError> {
        let slots = endpoints
            .groups
            .into_iter()
            .flatten()
            .map(|endpoint| Ok((TcpAcceptor::register(endpoint.listener)?, endpoint.tls)))
            .collect::<Result<_, ListenerError>>()?;
        Ok(EndpointSource {
            slots,
            turn: PollTurn::default(),
        })
    }

    /// Waits for the next connection from any endpoint. Accept errors are logged and retried, at
    /// once or after a pause by their kind.
    ///
    /// # Errors
    ///
    /// Returns [`AcceptError::ListenerBroken`] only when a listener cannot accept any more.
    pub async fn accept(&mut self) -> Result<EndpointConnection, AcceptError> {
        loop {
            let (accepted, index) = std::future::poll_fn(|cx| {
                for i in self.turn.order(self.slots.len()) {
                    if let Poll::Ready(accepted) = self.slots[i].0.poll_accept(cx) {
                        return Poll::Ready((accepted, i));
                    }
                }
                Poll::Pending
            })
            .await;
            self.turn.served(index, self.slots.len());
            let (tcp_acceptor, tls) = &self.slots[index];
            match accepted {
                Ok(tcp) => {
                    return Ok(EndpointConnection {
                        tcp,
                        tls: tls.clone(),
                    })
                }
                Err(e) => tcp_acceptor.recover(e).await.map_err(AcceptError::ListenerBroken)?,
            }
        }
    }
}

impl TransportDriver for Endpoints {
    type AcceptSource = EndpointSource;
    type Connection = EndpointConnection;
    type Stream = ConnectionStream;

    fn accept_loops(&self) -> usize {
        self.groups.len()
    }

    fn into_loops(self) -> Vec<Self> {
        self.groups
            .into_iter()
            .map(|group| Endpoints { groups: vec![group] })
            .collect()
    }

    fn listening(&self) -> Vec<String> {
        self.bound().iter().map(ToString::to_string).collect()
    }

    fn register(self) -> Result<EndpointSource, ListenerError> {
        EndpointSource::new(self)
    }

    #[inline]
    fn accept(source: &mut EndpointSource) -> impl std::future::Future<Output = Result<EndpointConnection, AcceptError>> + Send {
        source.accept()
    }

    #[inline]
    fn peer(connection: &EndpointConnection) -> Peer {
        Peer::Ip(connection.tcp.remote.ip())
    }

    /// Registers the connection with this thread's runtime, then TLS-handshakes it if its
    /// listener has an acceptor. It returns `None` when either fails.
    async fn into_stream(connection: EndpointConnection) -> Option<ConnectionStream> {
        let remote = connection.tcp.remote;
        let stream = match connection.tcp.register() {
            Ok(stream) => stream,
            Err(e) => {
                if let Some(held) = events::degrade(StreamEvent::RegisterFailed) {
                    tracing::warn!(
                        error = %e,
                        %remote,
                        held,
                        "an accepted connection could not be registered with its thread's runtime; dropped"
                    );
                }
                return None;
            }
        };
        match connection.tls {
            None => Some(ConnectionStream::Plain { inner: stream }),
            Some(acceptor) => tls::secure(&acceptor, stream)
                .await
                .map(|inner| ConnectionStream::Tls { inner }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn extra_plain_ports_are_plain_listeners_on_the_same_host() {
        let specs = ListenerSpec::set(loopback(8080), None, &[], &[8082]).unwrap();
        let shape: Vec<_> = specs.specs().iter().map(|s| (s.addr, s.tls.is_some())).collect();
        assert_eq!(shape, [(loopback(8080), false), (loopback(8082), false)]);
    }

    #[test]
    fn extra_tls_ports_need_a_certificate() {
        let err = ListenerSpec::set(loopback(8000), None, &[8443], &[])
            .err()
            .expect("no certificate");
        let ListenerSetError::ExtraTlsWithoutCert(ports) = err else {
            panic!("expected ExtraTlsWithoutCert, got {err:?}");
        };
        assert_eq!(ports, vec![8443]);
        let plain = ListenerSpec::set(loopback(8000), None, &[], &[]).unwrap();
        assert_eq!(plain.specs().len(), 1);
        assert!(plain.specs()[0].tls.is_none());
    }

    /// A repeated port would otherwise fail at its second bind with an `EADDRINUSE` that names no
    /// configuration.
    #[test]
    fn a_port_configured_twice_is_refused() {
        let cert = None;
        for (extra_tls, extra_plain) in [(&[][..], &[8000u16][..]), (&[], &[8082, 8082])] {
            let err = ListenerSpec::set(loopback(8000), cert.clone(), extra_tls, extra_plain)
                .err()
                .expect("a repeated port");
            assert!(matches!(err, ListenerSetError::DuplicatePort(_)), "{err:?}");
            assert!(err.to_string().contains("configured twice"), "{err}");
        }
        let twice_zero = ListenerSpec::set(loopback(0), None, &[], &[0]);
        assert!(twice_zero.is_ok(), "each port 0 is its own kernel pick");
    }


    #[test]
    fn a_bound_endpoint_names_its_scheme_and_the_port_the_kernel_picked() {
        let specs = ListenerSet::one(ListenerSpec {
            addr: loopback(0),
            tls: None,
        });
        let options = BindOptions {
            backlog: crate::tcp::listener::DEFAULT_BACKLOG,
            defer_accept: std::time::Duration::from_secs(10),
        };
        let bound = Endpoints::bind(&specs, NonZeroUsize::new(2).unwrap(), options).expect("loopback binds");
        let primary = bound.primary();
        assert_ne!(primary.addr.port(), 0);
        assert_eq!(bound.bound(), vec![primary]);
        assert_eq!(primary.to_string(), format!("http://127.0.0.1:{}", primary.addr.port()));
    }

    #[tokio::test]
    async fn a_plain_stream_keeps_the_sockets_vectored_writes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
        let mut client = client.unwrap();
        let mut stream = ConnectionStream::Plain {
            inner: accepted.unwrap().0,
        };
        assert!(stream.is_write_vectored());

        let bufs = [
            std::io::IoSlice::new(b"head "),
            std::io::IoSlice::new(b"body"),
        ];
        let written = stream.write_vectored(&bufs).await.unwrap();
        assert_eq!(written, 9);
        let mut got = [0u8; 9];
        client.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"head body");
    }
}
