//! Wraps the socket calls that differ between platforms; it is the only file of `tcp/` with a
//! platform `#[cfg]`, and the only file of this crate with `unsafe`.
//!
//! Each function is one socket option or one OS answer, and the caller decides what a failure means.

use std::io;

use crate::tcp::listener::PortLayout;

/// Turns off delayed ACKs on a connection; off Linux there is nothing to set and it succeeds.
#[cfg(target_os = "linux")]
pub(crate) fn quickack(stream: &std::net::TcpStream) -> io::Result<()> {
    Ok(rustix::net::sockopt::set_tcp_quickack(stream, true)?)
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn quickack(_stream: &std::net::TcpStream) -> io::Result<()> {
    Ok(())
}

/// A listening socket registered with the current runtime, whose accepted connections are not.
///
/// Each connection comes back as a nonblocking, close-on-exec std stream, so it can move to the
/// thread that serves it without being deregistered, at the same syscall cost as tokio's
/// `TcpListener::accept`.
#[cfg(unix)]
pub(crate) struct AcceptingSocket(tokio::io::unix::AsyncFd<mio::net::TcpListener>);

#[cfg(unix)]
impl AcceptingSocket {
    /// Registers `socket`, which must be nonblocking, with the current runtime.
    pub(crate) fn register(socket: std::net::TcpListener) -> io::Result<Self> {
        let listener = mio::net::TcpListener::from_std(socket);
        // SAFETY: the listener owns the socket's descriptor, moved into it here, for as long as it
        // lives; nothing replaces it, and `as_raw_fd` returns that one descriptor.
        let registered = unsafe { tokio::io::unix::AsyncFd::register(listener) };
        Ok(AcceptingSocket(registered?))
    }

    pub(crate) fn poll_accept(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<(std::net::TcpStream, std::net::SocketAddr)>> {
        loop {
            let mut ready = std::task::ready!(self.0.poll_read_ready(cx))?;
            // An `Err` is a `WouldBlock` whose readiness was cleared, and polling again registers
            // the task for the next connection.
            if let Ok(accepted) = ready.try_io(|socket| socket.get_ref().accept()) {
                return std::task::Poll::Ready(
                    accepted.map(|(stream, remote)| (std::os::fd::OwnedFd::from(stream).into(), remote)),
                );
            }
        }
    }
}

/// Off Unix, tokio's listener accepts and registers each connection, which is then taken off the
/// runtime again so that it can move threads.
#[cfg(not(unix))]
pub(crate) struct AcceptingSocket(tokio::net::TcpListener);

#[cfg(not(unix))]
impl AcceptingSocket {
    pub(crate) fn register(socket: std::net::TcpListener) -> io::Result<Self> {
        tokio::net::TcpListener::from_std(socket).map(AcceptingSocket)
    }

    pub(crate) fn poll_accept(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<(std::net::TcpStream, std::net::SocketAddr)>> {
        self.0
            .poll_accept(cx)
            .map(|accepted| accepted.and_then(|(stream, remote)| Ok((stream.into_std()?, remote))))
    }
}

/// Linux hashes connections over the `SO_REUSEPORT` sockets of a port; Darwin and the BSDs hand
/// every connection to one of them, so there the accept loops share one socket.
pub(crate) const PORT_LAYOUT: PortLayout = if cfg!(any(target_os = "linux", target_os = "android")) {
    PortLayout::SocketPerLoop
} else {
    PortLayout::SharedSocket
};

/// Sets SO_REUSEPORT; Windows has no such option, so there it succeeds without setting one.
#[cfg(not(windows))]
pub(crate) fn reuse_port(socket: &socket2::Socket) -> io::Result<()> {
    socket.set_reuse_port(true)
}

#[cfg(windows)]
pub(crate) fn reuse_port(_socket: &socket2::Socket) -> io::Result<()> {
    Ok(())
}

/// Sets TCP_DEFER_ACCEPT to `secs`; off Linux it succeeds without setting anything.
#[cfg(target_os = "linux")]
pub(crate) fn defer_accept(socket: &socket2::Socket, secs: u64) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let secs = libc::c_int::try_from(secs).unwrap_or(libc::c_int::MAX);
    setsockopt_int(socket.as_raw_fd(), libc::IPPROTO_TCP, libc::TCP_DEFER_ACCEPT, secs)
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn defer_accept(_socket: &socket2::Socket, _secs: u64) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn setsockopt_int(
    fd: std::os::unix::io::RawFd,
    level: libc::c_int,
    name: libc::c_int,
    val: libc::c_int,
) -> io::Result<()> {
    // SAFETY: `fd` is a live socket, and `val` outlives the call with its size passed.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of_val(&val) as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Names the query [`is_listening`] makes, for error messages.
#[cfg(all(unix, not(target_vendor = "apple")))]
pub(crate) const LISTENING_QUERY: &str = "getsockopt(SO_ACCEPTCONN)";

/// XNU defines `SO_ACCEPTCONN` but `getsockopt` fails with `ENOPROTOOPT` for every socket
/// (measured on macOS 26.6), so Apple platforms ask the TCP state instead.
#[cfg(target_vendor = "apple")]
pub(crate) const LISTENING_QUERY: &str = "getsockopt(TCP_CONNECTION_INFO)";

/// Returns whether `socket` is in the listening state.
#[cfg(all(unix, not(target_vendor = "apple")))]
pub(crate) fn is_listening(socket: &std::net::TcpListener) -> io::Result<bool> {
    Ok(rustix::net::sockopt::socket_acceptconn(socket)?)
}

/// Returns whether `socket` is in the listening state, read from `TCP_CONNECTION_INFO`'s
/// `tcpi_state`, which is `TCPS_LISTEN` exactly when `listen()` was called.
///
/// # Errors
///
/// A socket that is not TCP fails the call (`EINVAL` for UDP, `EOPNOTSUPP` for a Unix socket),
/// and that error is returned rather than read as "not listening".
#[cfg(target_vendor = "apple")]
pub(crate) fn is_listening(socket: &std::net::TcpListener) -> io::Result<bool> {
    use std::os::unix::io::AsRawFd;
    /// It comes from `<netinet/tcp_fsm.h>`, because the libc crate does not export TCP states.
    const TCPS_LISTEN: u8 = 1;
    // SAFETY: `tcp_connection_info` is plain integers, so all zeros is a valid value.
    let mut info: libc::tcp_connection_info = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of_val(&info) as libc::socklen_t;
    // SAFETY: the fd is live, `info` and `len` outlive the call, and `len` is `info`'s size.
    let rc = unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_CONNECTION_INFO,
            &mut info as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(info.tcpi_state == TCPS_LISTEN)
}

/// Takes ownership of a descriptor systemd handed over as a listening socket.
///
/// # Errors
///
/// Refuses a descriptor that is not open (`EBADF`), so that a number nothing owns never becomes
/// a socket this process later closes.
#[cfg(unix)]
pub(crate) fn adopt_handed(fd: i32) -> io::Result<std::net::TcpListener> {
    use std::os::unix::io::FromRawFd;
    // SAFETY: F_GETFD takes no argument and only reads the descriptor table.
    if unsafe { libc::fcntl(fd, libc::F_GETFD) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` is open (checked above), and `passed::take_from_systemd` adopts each handed
    // descriptor once per process, so nothing else owns or closes it.
    Ok(unsafe { std::net::TcpListener::from_raw_fd(fd) })
}

/// Returns whether a `raw_os_error()` code means out of file descriptors, socket handles or
/// kernel buffers.
#[cfg(unix)]
pub(crate) fn is_exhaustion_code(code: i32) -> bool {
    matches!(code, libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM)
}

#[cfg(windows)]
pub(crate) fn is_exhaustion_code(code: i32) -> bool {
    // WinSock has no ENFILE or ENOMEM analogue.
    const WSAEMFILE: i32 = 10024;
    const WSAENOBUFS: i32 = 10055;
    matches!(code, WSAEMFILE | WSAENOBUFS)
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn is_exhaustion_code(_code: i32) -> bool {
    false
}

/// Returns whether an accept errno cost only the connection it was about.
///
/// On Linux, `accept(2)` passes on the new socket's network errors and asks that they be treated
/// like `EAGAIN`.
#[cfg(target_os = "linux")]
pub(crate) fn is_one_connection_code(code: i32) -> bool {
    matches!(
        code,
        libc::EINTR
            | libc::ECONNABORTED
            | libc::ECONNRESET
            | libc::EPROTO
            | libc::EPERM
            | libc::ENETDOWN
            | libc::ENOPROTOOPT
            | libc::EHOSTDOWN
            | libc::ENONET
            | libc::EHOSTUNREACH
            | libc::ENETUNREACH
            | libc::ETIMEDOUT
    )
}

#[cfg(all(unix, not(target_os = "linux")))]
pub(crate) fn is_one_connection_code(code: i32) -> bool {
    matches!(code, libc::EINTR | libc::ECONNABORTED | libc::ECONNRESET)
}

#[cfg(not(unix))]
pub(crate) fn is_one_connection_code(_code: i32) -> bool {
    false
}

/// Returns whether an accept errno says the listening socket itself is unusable, which no retry
/// changes.
#[cfg(unix)]
pub(crate) fn is_listener_broken_code(code: i32) -> bool {
    matches!(code, libc::EBADF | libc::ENOTSOCK | libc::EINVAL | libc::EFAULT)
}

#[cfg(not(unix))]
pub(crate) fn is_listener_broken_code(_code: i32) -> bool {
    false
}

/// Socket activation by systemd exists only on Unix, so elsewhere the module is empty.
#[cfg(unix)]
#[path = "passed.rs"]
pub mod passed;

#[cfg(not(unix))]
pub mod passed {}

#[cfg(all(test, unix))]
mod tests {
    use super::is_listening;
    use std::net::SocketAddr;

    fn loopback() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 0))
    }

    #[test]
    fn only_a_listening_tcp_socket_reads_as_listening() {
        let listening = std::net::TcpListener::bind(loopback()).unwrap();
        assert!(is_listening(&listening).unwrap());

        let bound_only = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
        bound_only.bind(&loopback().into()).unwrap();
        let bound_only = std::net::TcpListener::from(std::os::fd::OwnedFd::from(bound_only));
        assert!(!is_listening(&bound_only).unwrap());

        let client = std::net::TcpStream::connect(listening.local_addr().unwrap()).unwrap();
        let connection = std::net::TcpListener::from(std::os::fd::OwnedFd::from(client));
        assert!(!is_listening(&connection).unwrap());
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn a_non_tcp_socket_cannot_be_asked_on_apple() {
        let udp = std::net::UdpSocket::bind(loopback()).unwrap();
        let udp = std::net::TcpListener::from(std::os::fd::OwnedFd::from(udp));
        let err = is_listening(&udp).expect_err("TCP_CONNECTION_INFO is TCP only");
        assert_eq!(err.raw_os_error(), Some(libc::EINVAL), "{err}");
    }
}
