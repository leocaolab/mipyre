//! Serves listening sockets that systemd handed this process instead of binding its own.
//!
//! `os.rs` declares this module only on Unix, so nothing here needs a `#[cfg]`.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::tcp::listener::{BoundListeners, Listener, SocketCall};
use crate::tcp::loops::{socket_loops, SocketLoops};
use crate::tcp::os;

/// Matches systemd's `SD_LISTEN_FDS_START`, since descriptors 0 to 2 are stdio.
const FIRST_HANDED_FD: i32 = 3;

/// Records that the handed descriptors were taken, so a second server in the same process binds
/// its own. It is process-wide because the descriptors are handed to the process once.
static TAKEN: AtomicBool = AtomicBool::new(false);

/// Takes the listening sockets systemd handed this process, reading the environment as
/// `sd_listen_fds` does.
///
/// It returns none when `LISTEN_PID` names another process, `LISTEN_FDS` is unset or 0, or an
/// earlier call took them. The variables are left set, because a child has another pid and does
/// not read them as its own.
///
/// # Errors
///
/// Returns an error if a variable is malformed or a handed descriptor is not open.
pub fn take_from_systemd() -> Result<Vec<std::net::TcpListener>, PassedError> {
    let var = |name| std::env::var_os(name).map(|v| v.to_string_lossy().into_owned());
    let count = handed_count(
        var("LISTEN_PID").as_deref(),
        var("LISTEN_FDS").as_deref(),
        std::process::id(),
    )?;
    if count == 0 || TAKEN.swap(true, Ordering::SeqCst) {
        return Ok(Vec::new());
    }
    (0..count)
        .map(|index| {
            let fd = FIRST_HANDED_FD + i32::try_from(index).map_err(|_| too_many(count))?;
            os::adopt_handed(fd).map_err(|source| PassedError::SystemCallFailed {
                index,
                call: SocketCall::CheckOpen,
                source,
            })
        })
        .collect()
}

fn too_many(count: usize) -> PassedError {
    PassedError::Environment {
        var: "LISTEN_FDS",
        value: count.to_string(),
        expected: "a count of descriptors this process can have",
    }
}

fn handed_count(listen_pid: Option<&str>, listen_fds: Option<&str>, own_pid: u32) -> Result<usize, PassedError> {
    let Some(pid) = listen_pid else {
        return Ok(0);
    };
    let pid: u32 = pid.parse().map_err(|_| PassedError::Environment {
        var: "LISTEN_PID",
        value: pid.to_string(),
        expected: "a process id",
    })?;
    if pid != own_pid {
        return Ok(0);
    }
    let Some(fds) = listen_fds else {
        return Ok(0);
    };
    let count: usize = fds.parse().map_err(|_| PassedError::Environment {
        var: "LISTEN_FDS",
        value: fds.to_string(),
        expected: "a count of descriptors",
    })?;
    if i32::try_from(count).map_or(true, |n| n.checked_add(FIRST_HANDED_FD).is_none()) {
        return Err(too_many(count));
    }
    Ok(count)
}

/// It is separate from [`ListenerError`](crate::tcp::listener::ListenerError) because that one
/// names an address this process chose; `index` is the socket's position in the order handed in.
#[derive(Debug, thiserror::Error)]
pub enum PassedError {
    #[error(
        "passed socket {index} is not listening (by {}): a connection, or a socket never \
         put in listen() — a .socket unit with Accept=yes hands over one connection per \
         instance; the front needs Accept=no",
        os::LISTENING_QUERY
    )]
    NotListening { index: usize },
    #[error("passed socket {index} {kind}: only TCP listening sockets are served")]
    NotTcp { index: usize, kind: NonTcpSocket },
    #[error("no listening socket was passed (LISTEN_FDS unset or 0): there is nothing to serve")]
    NoSockets,
    #[error("{var}={value:?} is not {expected}: the socket-activation environment is malformed")]
    Environment {
        var: &'static str,
        value: String,
        expected: &'static str,
    },
    #[error("{call} on passed socket {index} failed: {source}")]
    SystemCallFailed {
        index: usize,
        call: SocketCall,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NonTcpSocket {
    UnixDomain,
    /// It holds the address family's number.
    OtherFamily(i32),
    NotStream,
}

impl std::fmt::Display for NonTcpSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NonTcpSocket::UnixDomain => f.write_str(
                "is a Unix domain socket (ListenStream= names a path; it must name an IP \
                 address and port)",
            ),
            NonTcpSocket::OtherFamily(family) => write!(f, "is not an IP socket (address family {family})"),
            NonTcpSocket::NotStream => f.write_str(
                "is not a stream socket (ListenDatagram= or ListenSequentialPacket=; it must be \
                 ListenStream=)",
            ),
        }
    }
}

/// Refuses `socket` unless it is an IP stream socket. It runs before any other check, because a
/// Unix socket answers `SO_ACCEPTCONN` like a TCP one and then fails a later call with an errno
/// that does not say why.
fn require_tcp(socket: &std::net::TcpListener, index: usize) -> Result<(), PassedError> {
    let failed = |call| move |source| PassedError::SystemCallFailed { index, call, source };
    let not_tcp = |kind| Err(PassedError::NotTcp { index, kind });
    let sock = socket2::SockRef::from(socket);
    let local = sock.local_addr().map_err(failed(SocketCall::LocalAddr))?;
    if local.is_unix() {
        return not_tcp(NonTcpSocket::UnixDomain);
    }
    if local.as_socket().is_none() {
        return not_tcp(NonTcpSocket::OtherFamily(i32::from(local.domain())));
    }
    if sock.r#type().map_err(failed(SocketCall::SocketType))? != socket2::Type::STREAM {
        return not_tcp(NonTcpSocket::NotStream);
    }
    Ok(())
}

/// Spreads one address's sockets over `copies` accept loops, duplicating as [`socket_loops`] says.
///
/// Duplicates come from `try_clone`, which is `fcntl(F_DUPFD_CLOEXEC)` and needs no address
/// family, so `RestrictAddressFamilies=none` keeps holding. Building a fresh socket here, even
/// one bound to the same address, would break that.
fn spread(
    sockets: Vec<(usize, Listener)>,
    copies: std::num::NonZeroUsize,
) -> Result<Vec<Vec<(usize, Listener)>>, PassedError> {
    let mut loops: Vec<Vec<(usize, Listener)>> = (0..copies.get()).map(|_| Vec::new()).collect();
    let placed = socket_loops(sockets.len(), copies);
    for ((index, handed), SocketLoops { home, duplicates }) in sockets.into_iter().zip(placed) {
        for i in duplicates {
            let socket = handed
                .socket
                .try_clone()
                .map_err(|source| PassedError::SystemCallFailed {
                    index,
                    call: SocketCall::TryClone,
                    source,
                })?;
            loops[i].push((
                index,
                Listener {
                    socket,
                    addr: handed.addr,
                },
            ));
        }
        loops[home].push((index, handed));
    }
    Ok(loops)
}

impl BoundListeners {
    /// Serves sockets this process was handed, so it never calls `socket()` and its unit can keep
    /// `RestrictAddressFamilies=none`.
    ///
    /// One socket for an address is duplicated into every accept loop, so the loops share one
    /// queue and each connection wakes all of them. For the kernel to spread connections as with
    /// [`BoundListeners::bind`], hand over one socket per loop: one `.socket` unit per loop with
    /// the same `ListenStream=` and `ReusePort=yes`. `SO_REUSEPORT` must be set before `bind`,
    /// and the bind is systemd's, so it cannot be done here.
    ///
    /// Unlike `bind`, this sets no `TCP_DEFER_ACCEPT`, which is the unit's `DeferAcceptSec=`.
    ///
    /// # Errors
    ///
    /// Returns an error if no socket was handed, a socket is not a TCP listener, or a system call
    /// on one fails. A handed-over
    /// connection (`Accept=yes`) is refused here, because it would fail every `accept()` while
    /// the accept loop only logs and backs off, and the server would look alive and serve nothing.
    pub fn passed(sockets: Vec<std::net::TcpListener>, copies: std::num::NonZeroUsize) -> Result<Self, PassedError> {
        if sockets.is_empty() {
            return Err(PassedError::NoSockets);
        }
        let mut groups: Vec<Vec<Listener>> = (0..copies.get()).map(|_| Vec::new()).collect();
        let mut by_addr: Vec<(std::net::SocketAddr, Vec<(usize, Listener)>)> = Vec::new();
        for (index, socket) in sockets.into_iter().enumerate() {
            let failed = |call: SocketCall| move |source| PassedError::SystemCallFailed { index, call, source };
            require_tcp(&socket, index)?;
            if !os::is_listening(&socket).map_err(failed(SocketCall::CheckListening))? {
                return Err(PassedError::NotListening { index });
            }
            // systemd hands the socket over blocking, and tokio's `from_std` catches that only
            // in debug builds; in release the first `accept()` with no client waiting would
            // block the runtime's thread. The duplicates `spread` makes inherit the flag.
            socket
                .set_nonblocking(true)
                .map_err(failed(SocketCall::SetNonblocking))?;
            // systemd hands its sockets over without close-on-exec, because they had to survive
            // its exec of this process, and a child this process starts must not inherit them.
            socket2::SockRef::from(&socket)
                .set_cloexec(true)
                .map_err(failed(SocketCall::SetCloexec))?;
            let addr = socket.local_addr().map_err(failed(SocketCall::LocalAddr))?;
            let handed = (index, Listener { socket, addr });
            match by_addr.iter_mut().find(|(a, _)| *a == addr) {
                Some((_, same)) => same.push(handed),
                None => by_addr.push((addr, vec![handed])),
            }
        }
        for (_, sockets) in by_addr {
            for (group, handed) in groups.iter_mut().zip(spread(sockets, copies)?) {
                group.extend(handed.into_iter().map(|(_, listener)| listener));
            }
        }
        Ok(BoundListeners::of_groups(groups))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tcp::listener::{create_reuseport_listener, BindOptions, TcpAcceptor, TcpConnection, DEFAULT_BACKLOG};
    use std::net::SocketAddr;
    use std::time::Duration;

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn addrs(bound: &BoundListeners) -> Vec<SocketAddr> {
        let mut addrs: Vec<SocketAddr> = bound.groups()[0].iter().map(|l| l.addr).collect();
        addrs.dedup();
        addrs
    }

    /// Returns an acceptor for the one socket of each of `bound`'s groups.
    fn acceptors(bound: BoundListeners) -> Vec<TcpAcceptor> {
        bound
            .into_groups()
            .into_iter()
            .map(|mut group| {
                assert_eq!(group.len(), 1);
                TcpAcceptor::register(group.remove(0)).expect("registers with tokio")
            })
            .collect()
    }

    async fn accept(acceptor: &TcpAcceptor) -> std::io::Result<TcpConnection> {
        std::future::poll_fn(|cx| acceptor.poll_accept(cx)).await
    }

    fn two() -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::new(2).unwrap()
    }

    fn is_nonblocking(socket: &std::net::TcpListener) -> bool {
        use std::os::unix::io::AsRawFd;
        // SAFETY: The fd is live and F_GETFL takes no argument.
        let flags = unsafe { libc::fcntl(socket.as_raw_fd(), libc::F_GETFL) };
        assert!(flags >= 0, "{}", std::io::Error::last_os_error());
        flags & libc::O_NONBLOCK != 0
    }

    fn current_thread_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn handed_sockets_are_counted_only_for_this_process() {
        assert_eq!(handed_count(None, Some("2"), 7).unwrap(), 0);
        assert_eq!(handed_count(Some("8"), Some("2"), 7).unwrap(), 0);
        assert_eq!(handed_count(Some("7"), None, 7).unwrap(), 0);
        assert_eq!(handed_count(Some("7"), Some("0"), 7).unwrap(), 0);
        assert_eq!(handed_count(Some("7"), Some("2"), 7).unwrap(), 2);
    }

    #[test]
    fn a_malformed_activation_variable_is_refused_with_its_value() {
        let err = handed_count(Some("seven"), Some("2"), 7).unwrap_err();
        assert!(
            matches!(err, PassedError::Environment { var: "LISTEN_PID", .. }),
            "{err}"
        );
        assert!(err.to_string().contains("\"seven\""), "{err}");
        let err = handed_count(Some("7"), Some("-1"), 7).unwrap_err();
        assert!(
            matches!(err, PassedError::Environment { var: "LISTEN_FDS", .. }),
            "{err}"
        );
        let err = handed_count(Some("7"), Some(&u64::MAX.to_string()), 7).unwrap_err();
        assert!(
            matches!(err, PassedError::Environment { var: "LISTEN_FDS", .. }),
            "{err}"
        );
    }

    #[test]
    fn a_descriptor_that_is_not_open_is_not_adopted() {
        // The descriptor is far above anything a test process has open.
        let err = os::adopt_handed(1_000_000).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EBADF), "{err}");
    }

    #[test]
    fn every_copy_of_a_passed_listener_accepts_from_its_one_queue() {
        let handed = std::net::TcpListener::bind(loopback(0)).unwrap();
        let addr = handed.local_addr().unwrap();
        let fd = {
            use std::os::unix::io::AsRawFd;
            handed.as_raw_fd()
        };
        let bound = BoundListeners::passed(vec![handed], two()).unwrap();
        assert_eq!(addrs(&bound), vec![addr]);
        assert_eq!(bound.groups().len(), 2);
        let fds: Vec<_> = bound
            .groups()
            .iter()
            .map(|group| {
                use std::os::unix::io::AsRawFd;
                assert_eq!(group.len(), 1);
                assert_eq!(group[0].addr, addr);
                group[0].socket.as_raw_fd()
            })
            .collect();
        assert!(fds.contains(&fd), "the handed-in fd is served, {fds:?}");
        assert_ne!(fds[0], fds[1]);

        current_thread_runtime().block_on(async {
            for acceptor in acceptors(bound) {
                let client = tokio::net::TcpStream::connect(addr).await.unwrap();
                let accepted = tokio::time::timeout(Duration::from_secs(5), accept(&acceptor))
                    .await
                    .expect("the passed listener accepts the connection")
                    .expect("the passed listener is usable");
                assert_eq!(accepted.remote, client.local_addr().unwrap());
            }
        });
    }

    #[test]
    fn a_passed_connection_is_refused_as_not_listening() {
        // A `.socket` unit with Accept=yes hands over one end of a connection.
        let server = std::net::TcpListener::bind(loopback(0)).unwrap();
        let client = std::net::TcpStream::connect(server.local_addr().unwrap()).unwrap();
        let (_served, _) = server.accept().unwrap();
        let connection = std::net::TcpListener::from(std::os::fd::OwnedFd::from(client));
        let err = BoundListeners::passed(
            vec![std::net::TcpListener::bind(loopback(0)).unwrap(), connection],
            two(),
        )
        .err()
        .expect("a connection is not a listener");
        assert!(matches!(err, PassedError::NotListening { index: 1 }), "{err:?}");
        assert!(err.to_string().contains("Accept=yes"), "{err}");
    }

    #[test]
    fn a_passed_socket_never_put_in_listen_is_refused_as_not_listening() {
        let bound_only = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
        bound_only.bind(&loopback(0).into()).unwrap();
        let bound_only = std::net::TcpListener::from(std::os::fd::OwnedFd::from(bound_only));
        let err = BoundListeners::passed(vec![bound_only], two())
            .err()
            .expect("a socket never put in listen() is not a listener");
        assert!(matches!(err, PassedError::NotListening { index: 0 }), "{err:?}");
        assert!(err.to_string().contains(os::LISTENING_QUERY), "{err}");
    }

    #[test]
    fn a_blocking_passed_listener_comes_out_nonblocking() {
        let handed = std::net::TcpListener::bind(loopback(0)).unwrap();
        assert!(!is_nonblocking(&handed), "std binds blocking, as systemd hands over");
        let addr = handed.local_addr().unwrap();
        let bound = BoundListeners::passed(vec![handed], two()).unwrap();
        for group in bound.groups() {
            assert!(is_nonblocking(&group[0].socket));
        }
        // A blocking accept would hang this single-threaded runtime so the timeout never fired.
        current_thread_runtime().block_on(async {
            let acceptor = acceptors(bound).pop().unwrap();
            let idle = tokio::time::timeout(Duration::from_millis(50), accept(&acceptor)).await;
            assert!(idle.is_err(), "nothing to accept yet");
            let _client = tokio::net::TcpStream::connect(addr).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), accept(&acceptor))
                .await
                .expect("accepts once a client connects")
                .expect("the passed listener is usable");
        });
    }

    /// A server that serves nothing would look alive, so it does not start.
    #[test]
    fn no_passed_socket_is_refused() {
        let err = BoundListeners::passed(Vec::new(), two())
            .err()
            .expect("nothing to serve");
        assert!(matches!(err, PassedError::NoSockets), "{err:?}");
    }

    /// A `.socket` unit with `ListenStream=/path` hands over a Unix domain socket.
    #[test]
    fn a_passed_unix_domain_socket_is_refused_saying_so() {
        let dir = std::env::temp_dir().join(format!("infra-http-passed-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("listen.sock");
        let unix = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let handed = std::net::TcpListener::from(std::os::fd::OwnedFd::from(unix));
        let err = BoundListeners::passed(vec![handed], two())
            .err()
            .expect("a Unix domain socket is not served");
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(matches!(err, PassedError::NotTcp { index: 0, .. }), "{err:?}");
        assert!(err.to_string().contains("Unix domain socket"), "{err}");
    }

    #[test]
    fn a_passed_datagram_socket_is_refused_as_not_tcp() {
        let udp = std::net::UdpSocket::bind(loopback(0)).unwrap();
        let handed = std::net::TcpListener::from(std::os::fd::OwnedFd::from(udp));
        let err = BoundListeners::passed(vec![handed], two())
            .err()
            .expect("a datagram socket is not served");
        assert!(matches!(err, PassedError::NotTcp { index: 0, .. }), "{err:?}");
        assert!(err.to_string().contains("not a stream socket"), "{err}");
    }

    fn is_cloexec(fd: std::os::fd::RawFd) -> bool {
        // SAFETY: The fd is live and F_GETFD takes no argument.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert!(flags >= 0, "{}", std::io::Error::last_os_error());
        flags & libc::FD_CLOEXEC != 0
    }

    #[test]
    fn a_passed_listener_is_not_inherited_by_children() {
        use std::os::unix::io::AsRawFd;
        let handed = std::net::TcpListener::bind(loopback(0)).unwrap();
        // SAFETY: The fd is live, and F_SETFD with 0 clears close-on-exec as systemd leaves it.
        assert_eq!(unsafe { libc::fcntl(handed.as_raw_fd(), libc::F_SETFD, 0) }, 0);
        assert!(!is_cloexec(handed.as_raw_fd()));
        let bound = BoundListeners::passed(vec![handed], two()).unwrap();
        for group in bound.groups() {
            assert!(is_cloexec(group[0].socket.as_raw_fd()));
        }
    }

    /// Opens `n` `SO_REUSEPORT` listeners on one port, as `n` `.socket` units with `ReusePort=yes` hand them over.
    fn reuseport_set(n: usize) -> (Vec<std::net::TcpListener>, Vec<std::os::fd::RawFd>) {
        use std::os::unix::io::AsRawFd;
        let options = BindOptions {
            backlog: DEFAULT_BACKLOG,
            defer_accept: Duration::from_secs(10),
        };
        let first = create_reuseport_listener(loopback(0), options).unwrap();
        let addr = first.local_addr().unwrap();
        let mut sockets = vec![first];
        for _ in 1..n {
            sockets.push(create_reuseport_listener(addr, options).unwrap());
        }
        let fds = sockets.iter().map(|s| s.as_raw_fd()).collect();
        (sockets, fds)
    }

    fn group_fds(bound: &BoundListeners) -> Vec<Vec<std::os::fd::RawFd>> {
        use std::os::unix::io::AsRawFd;
        bound
            .groups()
            .iter()
            .map(|g| g.iter().map(|l| l.socket.as_raw_fd()).collect())
            .collect()
    }

    #[test]
    fn a_socket_per_accept_loop_is_one_per_group_not_duplicated() {
        let (sockets, fds) = reuseport_set(2);
        let addr = sockets[0].local_addr().unwrap();
        let bound = BoundListeners::passed(sockets, two()).unwrap();
        assert_eq!(group_fds(&bound), vec![vec![fds[0]], vec![fds[1]]]);
        assert_eq!(addrs(&bound), vec![addr]);
    }

    #[test]
    fn more_sockets_than_loops_are_all_served() {
        let (sockets, fds) = reuseport_set(3);
        let bound = BoundListeners::passed(sockets, two()).unwrap();
        assert_eq!(group_fds(&bound), vec![vec![fds[0], fds[2]], vec![fds[1]]]);
        assert_eq!(addrs(&bound).len(), 1, "one address");
    }

    #[test]
    fn fewer_sockets_than_loops_are_shared_in_turn() {
        let (sockets, fds) = reuseport_set(2);
        let four = std::num::NonZeroUsize::new(4).unwrap();
        let bound = BoundListeners::passed(sockets, four).unwrap();
        let groups = group_fds(&bound);
        assert!(groups.iter().all(|g| g.len() == 1), "{groups:?}");
        assert_eq!((groups[0][0], groups[1][0]), (fds[0], fds[1]));
        assert!(
            !fds.contains(&groups[2][0]) && !fds.contains(&groups[3][0]),
            "{groups:?}"
        );
    }
}
