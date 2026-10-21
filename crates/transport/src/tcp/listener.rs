//! Binds the server's TCP listeners and accepts connections from them, backing off on accept
//! errors.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::stream::peer_gone;
use crate::tcp::events::{self, TcpEvent};
use crate::tcp::os;

/// Counts `event` when `set` failed and returns the errno when the warning is due, once a minute
/// per kind. Taking the call's own result keeps a later call from overwriting the errno.
#[inline]
fn refused_option(event: TcpEvent, set: std::io::Result<()>) -> Option<(std::io::Error, u64)> {
    let errno = set.err()?;
    events::degrade(event).map(|held| (errno, held))
}

/// Enables TCP_QUICKACK on Linux and does nothing elsewhere. A failure is counted and logged
/// once a minute, because it repeats on every connection.
#[inline]
pub fn setup_tcp_quickack(stream: &std::net::TcpStream) {
    if let Err(errno) = os::quickack(stream) {
        quickack_refused(errno);
    }
}

/// Counts and logs a refused TCP_QUICKACK out of line, so the success path every connection takes
/// saves no registers for the log line.
#[cold]
#[inline(never)]
fn quickack_refused(errno: std::io::Error) {
    if let Some(held) = events::degrade(TcpEvent::QuickackFailed) {
        tracing::warn!(
            ?errno,
            held,
            "setsockopt(TCP_QUICKACK) failed; delayed-ACK latency \
             optimization is disabled on this socket"
        );
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{call} for {addr} failed: {source}")]
pub struct ListenerError {
    pub call: SocketCall,
    pub addr: SocketAddr,
    #[source]
    pub source: std::io::Error,
}

/// It is large to avoid SYN drops at 200k+ QPS; the kernel caps it at `net.core.somaxconn`.
pub const DEFAULT_BACKLOG: i32 = 8192;

/// Says how a bound socket listens.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BindOptions {
    pub backlog: i32,
    /// Holds a new connection in the kernel until its first byte arrives or this time passes
    /// (`TCP_DEFER_ACCEPT`, Linux only), so a cold-connect flood holds no task and no fd. The
    /// protocol above knows how long a client may stay silent, so it chooses.
    pub defer_accept: Duration,
}

/// Creates a nonblocking listener with SO_REUSEPORT, so the kernel spreads connections over
/// every listener on the port.
///
/// # Errors
///
/// Returns the socket call that failed, with the OS error.
pub fn create_reuseport_listener(
    addr: SocketAddr,
    options: BindOptions,
) -> Result<std::net::TcpListener, ListenerError> {
    use socket2::{Domain, Protocol, Socket, Type};

    let failed = |call: SocketCall| move |source| ListenerError { call, addr, source };
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
        .map_err(failed(SocketCall::Create))?;

    socket
        .set_reuse_address(true)
        .map_err(failed(SocketCall::SetReuseAddress))?;

    os::reuse_port(&socket).map_err(failed(SocketCall::SetReusePort))?;

    socket
        .set_nonblocking(true)
        .map_err(failed(SocketCall::SetNonblocking))?;

    socket
        .bind(&addr.into())
        .map_err(failed(SocketCall::Bind))?;

    if let Some((errno, held)) = refused_option(
        TcpEvent::DeferAcceptFailed,
        os::defer_accept(&socket, options.defer_accept.as_secs()),
    ) {
        tracing::warn!(
            ?errno,
            held,
            "setsockopt(TCP_DEFER_ACCEPT) failed; cold-connect DoS \
             mitigation is disabled on this socket"
        );
    }

    socket.listen(options.backlog).map_err(failed(SocketCall::Listen))?;

    Ok(socket.into())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SocketCall {
    Create,
    SetReuseAddress,
    SetReusePort,
    SetNonblocking,
    Bind,
    Listen,
    LocalAddr,
    Accept,
    Register,
    CheckListening,
    TryClone,
    SocketType,
    SetCloexec,
    CheckOpen,
}

impl std::fmt::Display for SocketCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SocketCall::Create => "socket creation",
            SocketCall::SetReuseAddress => "set_reuse_address",
            SocketCall::SetReusePort => "set_reuse_port",
            SocketCall::SetNonblocking => "set_nonblocking",
            SocketCall::Bind => "bind",
            SocketCall::Listen => "listen",
            SocketCall::LocalAddr => "local_addr",
            SocketCall::Accept => "accept",
            SocketCall::Register => "register with the runtime",
            SocketCall::CheckListening => os::LISTENING_QUERY,
            SocketCall::TryClone => "try_clone",
            SocketCall::SocketType => "getsockopt(SO_TYPE)",
            SocketCall::SetCloexec => "fcntl(F_SETFD, FD_CLOEXEC)",
            SocketCall::CheckOpen => "fcntl(F_GETFD)",
        })
    }
}

impl ListenerError {
    pub fn errno(&self) -> Option<i32> {
        self.source.raw_os_error()
    }
}

/// The socket is listening but not yet registered with a runtime.
pub struct Listener {
    pub socket: std::net::TcpListener,
    /// It holds the kernel's pick in place of a port 0.
    pub addr: SocketAddr,
}

/// Holds every socket of one server, bound before any serving thread exists.
pub struct BoundListeners {
    /// Holds one group per accept loop, each with one socket per address in the order given. Both
    /// levels are never empty, because the constructors take a `NonZeroUsize` and refuse no
    /// listeners.
    groups: Vec<Vec<Listener>>,
}

/// Proves `addr` free before the `SO_REUSEPORT` sockets join it, and returns the bound address.
///
/// A socket without `SO_REUSEPORT` cannot bind a port another socket listens on, so a port held
/// by another server is `AddrInUse` here instead of silently shared. `SO_REUSEADDR` lets a port
/// in TIME_WAIT count as free. A port 0 must not be probed: the probe would release the kernel's
/// pick for anyone to take before the copies bind it.
fn probe_free(addr: SocketAddr) -> Result<SocketAddr, ListenerError> {
    use socket2::{Domain, Protocol, Socket, Type};

    let failed = |call: SocketCall| move |source| ListenerError { call, addr, source };
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))
        .map_err(failed(SocketCall::Create))?;
    socket
        .set_reuse_address(true)
        .map_err(failed(SocketCall::SetReuseAddress))?;
    socket
        .bind(&addr.into())
        .map_err(failed(SocketCall::Bind))?;
    socket
        .local_addr()
        .map_err(failed(SocketCall::LocalAddr))?
        .as_socket()
        .ok_or_else(|| ListenerError {
            call: SocketCall::LocalAddr,
            addr,
            source: std::io::Error::other("the probe socket has no IP address"),
        })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PortClaim {
    /// A port 0 is resolved by the first copy's own bind, which is kept; the others join the
    /// port the kernel picked.
    FirstCopyBinds,
    ProbedFree,
}

impl PortClaim {
    fn of(addr: SocketAddr) -> Self {
        if addr.port() == 0 {
            PortClaim::FirstCopyBinds
        } else {
            PortClaim::ProbedFree
        }
    }
}

impl BoundListeners {
    /// Wraps `groups`, which must be one per copy, at least one, each with the same non-empty
    /// listeners.
    pub(crate) fn of_groups(groups: Vec<Vec<Listener>>) -> Self {
        debug_assert!(!groups.is_empty(), "copies is a NonZeroUsize");
        debug_assert!(groups.iter().all(|g| !g.is_empty()), "the constructors refuse no listeners");
        BoundListeners { groups }
    }

    pub fn groups(&self) -> &[Vec<Listener>] {
        &self.groups
    }

    pub fn into_groups(self) -> Vec<Vec<Listener>> {
        self.groups
    }

    /// Binds `copies` `SO_REUSEPORT` sockets for each of `addrs`, one group per accept loop, so
    /// the kernel spreads connections over the loops. A named port is first proven free.
    ///
    /// # Errors
    ///
    /// Returns the first failed socket call, such as `AddrInUse`, before any thread exists.
    pub fn bind(addrs: &[SocketAddr], copies: NonZeroUsize, options: BindOptions) -> Result<Self, ListenerError> {
        let mut groups: Vec<Vec<Listener>> = (0..copies.get()).map(|_| Vec::new()).collect();
        for &asked in addrs {
            // The kernel never picks a port another `SO_REUSEPORT` socket holds, so the first
            // copy's own bind is safe without a probe.
            let (addr, mut first) = match PortClaim::of(asked) {
                PortClaim::FirstCopyBinds => {
                    let socket = create_reuseport_listener(asked, options)?;
                    let addr = socket.local_addr().map_err(|source| ListenerError {
                        call: SocketCall::LocalAddr,
                        addr: asked,
                        source,
                    })?;
                    (addr, Some(socket))
                }
                PortClaim::ProbedFree => (probe_free(asked)?, None),
            };
            for group in &mut groups {
                let socket = match first.take() {
                    Some(socket) => socket,
                    None => create_reuseport_listener(addr, options)?,
                };
                group.push(Listener { socket, addr });
            }
        }
        Ok(BoundListeners::of_groups(groups))
    }
}

/// A connection configured for serving but registered with no runtime yet, so it can move to the
/// thread that serves it.
pub struct TcpConnection {
    pub stream: std::net::TcpStream,
    pub remote: SocketAddr,
}

impl TcpConnection {
    /// Registers the connection with the current runtime, so it must be called on the thread
    /// that serves it.
    ///
    /// # Errors
    ///
    /// Returns the runtime's error when it would not take the socket.
    pub fn register(self) -> std::io::Result<tokio::net::TcpStream> {
        tokio::net::TcpStream::from_std(self.stream)
    }
}

/// One listening socket registered with the runtime of the thread that accepts from it.
pub struct TcpAcceptor {
    socket: os::AcceptingSocket,
    addr: SocketAddr,
}

impl TcpAcceptor {
    /// Registers `listener` with the current runtime, so it must run on the thread that accepts
    /// from it.
    ///
    /// # Errors
    ///
    /// Returns a [`SocketCall::Register`] error when the runtime refuses the socket.
    pub fn register(listener: Listener) -> Result<Self, ListenerError> {
        let socket = os::AcceptingSocket::register(listener.socket).map_err(|source| ListenerError {
            call: SocketCall::Register,
            addr: listener.addr,
            source,
        })?;
        Ok(TcpAcceptor {
            socket,
            addr: listener.addr,
        })
    }

    /// Polls for the next connection and sets its options for serving; a failed accept is
    /// returned for [`TcpAcceptor::recover`].
    #[inline]
    pub fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<std::io::Result<TcpConnection>> {
        self.socket
            .poll_accept(cx)
            .map(|accepted| accepted.map(|(stream, remote)| configure(stream, remote)))
    }

    /// Counts, logs and pauses for a failed accept as its kind asks, so the caller can accept
    /// again.
    ///
    /// # Errors
    ///
    /// Returns the failure when the listener cannot accept any more.
    pub async fn recover(&self, source: std::io::Error) -> Result<(), ListenerError> {
        match handle_accept_error(&source).await {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(()) => Err(ListenerError {
                call: SocketCall::Accept,
                addr: self.addr,
                source,
            }),
        }
    }
}

#[inline]
fn configure(stream: std::net::TcpStream, remote: SocketAddr) -> TcpConnection {
    if let Err(errno) = stream.set_nodelay(true) {
        nodelay_refused(errno, remote);
    }
    setup_tcp_quickack(&stream);
    TcpConnection { stream, remote }
}

/// Counts and logs a refused TCP_NODELAY out of line, as [`quickack_refused`] does.
#[cold]
#[inline(never)]
fn nodelay_refused(errno: std::io::Error, remote: SocketAddr) {
    if let Some(held) = events::degrade(TcpEvent::NodelayFailed) {
        tracing::warn!(
            error = %errno,
            %remote,
            held,
            "set_nodelay failed; this connection keeps Nagle's algorithm"
        );
    }
}

/// The errno sets behind each variant differ per platform, so they live in `os`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcceptFailure {
    /// Out of fds, sockets or kernel buffers, the next accept() fails at once too.
    Exhausted,
    PeerGone,
    /// A signal or a network error took only the new socket.
    NewSocketFailed,
    ListenerBroken,
    Unexpected,
}

pub const EXHAUSTION_BACKOFF: Duration = Duration::from_millis(250);
const UNEXPECTED_BACKOFF: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcceptStep {
    Retry,
    RetryAfter(Duration),
    /// The loop ends and its server with it; the caller says why.
    Stop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcceptLog {
    /// The caller that stops says why.
    Quiet,
    /// Logs at debug, because a client can cause it at will.
    Debug(&'static str),
    ThrottledError(&'static str),
    ThrottledWarn(&'static str),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AcceptConsequence {
    counted: Option<TcpEvent>,
    log: AcceptLog,
    next: AcceptStep,
}

/// Decides what a failed accept() leads to.
///
/// Exhaustion pauses because retrying at once spins at 100% CPU until fds close. A failure that
/// took only its own connection is retried at once, or any client could impose the pause by
/// sending SYN then RST. A broken listener fails every accept() the same way, so the loop stops.
fn consequence(failure: AcceptFailure) -> AcceptConsequence {
    let (counted, log, next) = match failure {
        AcceptFailure::Exhausted => (
            Some(TcpEvent::AcceptExhausted),
            AcceptLog::ThrottledError("accept() resource exhaustion — backing off"),
            AcceptStep::RetryAfter(EXHAUSTION_BACKOFF),
        ),
        AcceptFailure::PeerGone => (
            Some(TcpEvent::PeerGoneInQueue),
            AcceptLog::Debug("accept() of a connection its peer abandoned"),
            AcceptStep::Retry,
        ),
        AcceptFailure::NewSocketFailed => (
            None,
            AcceptLog::Debug("accept() failed for one connection"),
            AcceptStep::Retry,
        ),
        AcceptFailure::ListenerBroken => (
            Some(TcpEvent::AcceptFailed),
            AcceptLog::Quiet,
            AcceptStep::Stop,
        ),
        AcceptFailure::Unexpected => (
            Some(TcpEvent::AcceptFailed),
            AcceptLog::ThrottledWarn("accept() error"),
            AcceptStep::RetryAfter(UNEXPECTED_BACKOFF),
        ),
    };
    AcceptConsequence { counted, log, next }
}

fn classify(e: &std::io::Error) -> AcceptFailure {
    let code = e.raw_os_error();
    if code.is_some_and(os::is_exhaustion_code) {
        AcceptFailure::Exhausted
    } else if peer_gone(e) {
        AcceptFailure::PeerGone
    } else if e.kind() == std::io::ErrorKind::Interrupted
        || code.is_some_and(os::is_one_connection_code)
    {
        AcceptFailure::NewSocketFailed
    } else if code.is_some_and(os::is_listener_broken_code) {
        AcceptFailure::ListenerBroken
    } else {
        AcceptFailure::Unexpected
    }
}

/// Counts, logs and pauses as [`consequence`] decides, and breaks when the listener is unusable.
async fn handle_accept_error(e: &std::io::Error) -> ControlFlow<()> {
    let AcceptConsequence { counted, log, next } = consequence(classify(e));
    if let Some(kind) = counted {
        events::count(kind);
    }
    let held = || counted.and_then(events::admit);
    match log {
        AcceptLog::Quiet => {}
        AcceptLog::Debug(what) => tracing::debug!(error = %e, "{what}"),
        AcceptLog::ThrottledError(what) => {
            if let Some(held) = held() {
                tracing::error!(error = %e, held, ?next, "{what}");
            }
        }
        AcceptLog::ThrottledWarn(what) => {
            if let Some(held) = held() {
                tracing::warn!(error = %e, held, "{what}");
            }
        }
    }
    match next {
        AcceptStep::Retry => ControlFlow::Continue(()),
        AcceptStep::RetryAfter(pause) => {
            tokio::time::sleep(pause).await;
            ControlFlow::Continue(())
        }
        AcceptStep::Stop => ControlFlow::Break(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    const OPTIONS: BindOptions = BindOptions {
        backlog: DEFAULT_BACKLOG,
        defer_accept: Duration::from_secs(10),
    };

    fn bind(addr: SocketAddr, copies: usize) -> Result<BoundListeners, ListenerError> {
        BoundListeners::bind(&[addr], NonZeroUsize::new(copies).unwrap(), OPTIONS)
    }

    #[test]
    fn a_port_zero_is_claimed_by_its_first_copy_and_a_named_port_is_probed() {
        let v6 = |port| SocketAddr::new(std::net::Ipv6Addr::LOCALHOST.into(), port);
        for addr in [loopback(0), v6(0)] {
            assert_eq!(PortClaim::of(addr), PortClaim::FirstCopyBinds, "{addr}");
        }
        for addr in [loopback(1), loopback(8000), v6(8000), loopback(u16::MAX)] {
            assert_eq!(PortClaim::of(addr), PortClaim::ProbedFree, "{addr}");
        }
    }

    #[test]
    fn copies_of_a_port_zero_listener_share_the_port_the_kernel_picked() {
        let bound = bind(loopback(0), 3).expect("loopback binds");
        assert_eq!(bound.groups.len(), 3);
        let port = bound.groups[0][0].addr.port();
        assert_ne!(port, 0);
        for group in &bound.groups {
            assert_eq!(group.len(), 1);
            assert_eq!(group[0].socket.local_addr().unwrap().port(), port);
            assert_eq!(group[0].addr.port(), port);
        }
    }

    #[test]
    fn a_port_in_use_is_a_bind_error_with_its_errno() {
        // No SO_REUSEPORT on the holder, so the server's socket can't join it.
        let holder = std::net::TcpListener::bind(loopback(0)).unwrap();
        let addr = holder.local_addr().unwrap();
        let err = bind(addr, 2)
            .err()
            .expect("the port is taken");
        assert_eq!(err.call, SocketCall::Bind);
        assert_eq!(err.source.kind(), std::io::ErrorKind::AddrInUse);
        assert_eq!(err.errno(), Some(libc::EADDRINUSE));
    }

    #[test]
    fn a_port_another_reuseport_server_listens_on_is_in_use() {
        // Without the probe, ours would join this socket and the kernel would split the traffic
        // between the two servers.
        let other = create_reuseport_listener(loopback(0), OPTIONS).unwrap();
        let addr = other.local_addr().unwrap();
        let err = bind(addr, 2)
            .err()
            .expect("the port is another server's");
        assert_eq!(err.call, SocketCall::Bind);
        assert_eq!(err.errno(), Some(libc::EADDRINUSE));
        assert_eq!(err.addr, addr);
    }

    #[test]
    fn a_released_port_binds_again() {
        let first = bind(loopback(0), 2).unwrap();
        let addr = first.groups[0][0].addr;
        drop(first);
        let again = bind(addr, 2).expect("a port its last server released is free");
        assert_eq!(again.groups[0][0].addr, addr);
    }

    #[test]
    fn a_failed_bind_names_the_step_and_keeps_the_os_error() {
        // No SO_REUSEPORT on the holder, so a reuseport socket can't join its port.
        let holder = std::net::TcpListener::bind(loopback(0)).unwrap();
        let addr = holder.local_addr().unwrap();
        let err = create_reuseport_listener(addr, OPTIONS).expect_err("the port is taken");
        assert_eq!(err.call, SocketCall::Bind);
        assert_eq!(err.addr, addr);
        assert_eq!(err.source.kind(), std::io::ErrorKind::AddrInUse);
        assert!(
            err.to_string()
                .starts_with(&format!("bind for {addr} failed: ")),
            "{err}"
        );
    }

    fn errno(code: i32) -> std::io::Error {
        std::io::Error::from_raw_os_error(code)
    }

    fn kind(kind: std::io::ErrorKind) -> std::io::Error {
        std::io::Error::from(kind)
    }

    #[test]
    fn a_failure_that_took_one_connection_is_classed_so() {
        for k in [std::io::ErrorKind::ConnectionAborted, std::io::ErrorKind::ConnectionReset] {
            assert_eq!(classify(&kind(k)), AcceptFailure::PeerGone, "{k:?}");
        }
        assert_eq!(classify(&kind(std::io::ErrorKind::Interrupted)), AcceptFailure::NewSocketFailed);
        assert_eq!(classify(&errno(libc::EINTR)), AcceptFailure::NewSocketFailed);
    }

    #[test]
    fn an_unusable_listener_is_classed_broken() {
        for code in [libc::EBADF, libc::ENOTSOCK, libc::EINVAL] {
            assert_eq!(classify(&errno(code)), AcceptFailure::ListenerBroken, "errno {code}");
        }
    }

    #[test]
    fn running_out_of_descriptors_is_exhaustion_and_anything_else_unexpected() {
        assert_eq!(classify(&errno(libc::EMFILE)), AcceptFailure::Exhausted);
        assert_eq!(classify(&kind(std::io::ErrorKind::PermissionDenied)), AcceptFailure::Unexpected);
    }

    /// An interrupted accept() is nobody's failure.
    #[test]
    fn an_accept_failure_is_counted_by_whose_doing_it_was() {
        let counted = |f| consequence(f).counted;
        assert_eq!(counted(AcceptFailure::PeerGone), Some(TcpEvent::PeerGoneInQueue));
        assert_eq!(counted(AcceptFailure::NewSocketFailed), None);
        assert_eq!(counted(AcceptFailure::Exhausted), Some(TcpEvent::AcceptExhausted));
        assert_eq!(counted(AcceptFailure::ListenerBroken), Some(TcpEvent::AcceptFailed));
        assert_eq!(counted(AcceptFailure::Unexpected), Some(TcpEvent::AcceptFailed));
    }

    /// A client that sends SYN then RST must not throttle the whole listener.
    #[test]
    fn a_one_connection_failure_is_retried_at_once() {
        assert_eq!(consequence(AcceptFailure::PeerGone).next, AcceptStep::Retry);
        assert_eq!(consequence(AcceptFailure::NewSocketFailed).next, AcceptStep::Retry);
    }

    #[test]
    fn exhaustion_and_the_unexpected_pause_before_the_retry() {
        assert_eq!(
            consequence(AcceptFailure::Exhausted).next,
            AcceptStep::RetryAfter(EXHAUSTION_BACKOFF)
        );
        assert_eq!(
            consequence(AcceptFailure::Unexpected).next,
            AcceptStep::RetryAfter(UNEXPECTED_BACKOFF)
        );
    }

    #[test]
    fn a_broken_listener_stops_the_loop() {
        assert_eq!(consequence(AcceptFailure::ListenerBroken).next, AcceptStep::Stop);
    }
}
