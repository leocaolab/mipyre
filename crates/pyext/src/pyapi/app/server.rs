//! Defines one bound server of an app and parses `start()`'s transport, address and count
//! arguments.

use std::net::SocketAddr;
use std::num::NonZeroUsize;

use mipyre_http::plan::site::Protocols;
use mipyre_transport::endpoint::{Endpoints, ListenerSet, ListenerSpec};
use mipyre_transport::memory::{MemoryConnectError, MemoryConnector, MemorySource};
use pyo3::prelude::*;
use tokio_util::sync::CancellationToken;

use crate::boot::{GivenSize, ServerRun};
use crate::pyapi::config::PyGivenSize;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 8000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Transport {
    Tcp,
    Memory,
}

/// Parses `start()`'s `transport`, where `None` means TCP; an unknown value is a `ValueError`
/// naming it.
pub(super) fn transport(given: Option<&str>) -> PyResult<Transport> {
    match given {
        None | Some("tcp") => Ok(Transport::Tcp),
        Some("memory") => Ok(Transport::Memory),
        Some(other) => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "transport {other:?} is not one of \"tcp\", \"memory\""
        ))),
    }
}

fn socket_args_given(
    host: Option<&str>,
    port: Option<u16>,
    tls_cert: Option<&str>,
    tls_key: Option<&str>,
    extra_tls_ports: Option<&[u16]>,
    extra_plain_ports: Option<&[u16]>,
) -> Vec<&'static str> {
    [
        ("host", host.is_some()),
        ("port", port.is_some()),
        ("tls_cert", tls_cert.is_some()),
        ("tls_key", tls_key.is_some()),
        ("extra_tls_ports", extra_tls_ports.is_some()),
        ("extra_plain_ports", extra_plain_ports.is_some()),
    ]
    .into_iter()
    .filter_map(|(name, given)| given.then_some(name))
    .collect()
}

/// Refuses socket arguments for a memory server with a `ValueError` naming them, rather than
/// silently dropping them.
pub(super) fn refuse_socket_args(
    host: Option<&str>,
    port: Option<u16>,
    tls_cert: Option<&str>,
    tls_key: Option<&str>,
    extra_tls_ports: Option<&[u16]>,
    extra_plain_ports: Option<&[u16]>,
) -> PyResult<()> {
    let given = socket_args_given(host, port, tls_cert, tls_key, extra_tls_ports, extra_plain_ports);
    if given.is_empty() {
        return Ok(());
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "transport=\"memory\" listens on no socket, so {} cannot be given",
        given.join(", ")
    )))
}

/// Builds the listener set `start()`'s socket arguments name.
///
/// Extra TLS ports without a certificate are refused, because the operator would believe them
/// protected.
pub(super) fn listener_set(
    host: Option<&str>,
    port: Option<u16>,
    tls_cert: Option<&str>,
    tls_key: Option<&str>,
    extra_tls_ports: Option<Vec<u16>>,
    extra_plain_ports: Option<Vec<u16>>,
) -> PyResult<ListenerSet> {
    let addr = socket_addr(host.unwrap_or(DEFAULT_HOST), port.unwrap_or(DEFAULT_PORT))?;
    let tls = match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => Some(
            mipyre_transport::tls::build_acceptor(cert, key, Protocols::Http2AndHttp1.alpn())
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
        ),
        (None, None) => None,
        _ => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "tls_cert and tls_key must be provided together",
            ))
        }
    };
    ListenerSpec::set(
        addr,
        tls,
        &extra_tls_ports.unwrap_or_default(),
        &extra_plain_ports.unwrap_or_default(),
    )
    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

/// Binds every listener of `specs` before any thread or worker exists, so a port in use is
/// this call's `OSError(EADDRINUSE)` rather than a log line from a serving thread.
///
/// Sockets systemd handed over are served instead, each with the server's TLS, and cannot be
/// combined with extra ports.
pub(super) fn bind(specs: &ListenerSet, copies: NonZeroUsize) -> PyResult<Endpoints> {
    let tls = specs.specs().first().and_then(|spec| spec.tls.clone());
    match crate::serve::handed_by_systemd(tls, copies)? {
        None => Endpoints::bind(specs, copies, crate::serve::bind_options()).map_err(crate::serve::listener_error),
        Some(_) if specs.specs().len() > 1 => Err(pyo3::exceptions::PyValueError::new_err(
            "extra TLS / plain ports cannot be combined with sockets systemd handed over \
             (LISTEN_FDS): the .socket unit binds the addresses, so list every one there",
        )),
        Some(handed) => {
            println!("  Note: serving the sockets systemd handed over (LISTEN_FDS); host and port are not used");
            Ok(handed)
        }
    }
}

/// Checks a count argument of `start()`; a value below 1 is a `ValueError` naming the
/// argument, because a server with no worker or no IO thread serves nothing.
///
/// It is the only check of a size, reached from `Mipyre.run()`, its environment variables and
/// the raw engine alike.
pub(super) fn positive(name: &str, given: Option<PyGivenSize>) -> PyResult<Option<GivenSize>> {
    given
        .map(|given| {
            let value = usize::try_from(given.value)
                .ok()
                .and_then(std::num::NonZeroUsize::new)
                .ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "{name} must be >= 1, got {}",
                        given.value
                    ))
                })?;
            Ok(GivenSize {
                value,
                origin: given.origin,
            })
        })
        .transpose()
}

/// Parses `host` as an IPv4 or IPv6 address, bare or in brackets; a name such as `localhost`
/// is refused, not resolved.
fn socket_addr(host: &str, port: u16) -> Result<SocketAddr, BadHost> {
    let ip = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    ip.parse::<std::net::IpAddr>()
        .map(|ip| SocketAddr::new(ip, port))
        .map_err(|source| BadHost {
            host: host.to_string(),
            source,
        })
}

#[derive(Debug, thiserror::Error)]
#[error(
    "host {host:?} is not an IP address ({source}); use e.g. \"127.0.0.1\", \"0.0.0.0\" or \
     \"::1\" (host names such as \"localhost\" are not resolved)"
)]
pub(super) struct BadHost {
    host: String,
    source: std::net::AddrParseError,
}

impl From<BadHost> for PyErr {
    fn from(e: BadHost) -> Self {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    }
}

pub(super) enum BoundServer {
    Tcp {
        /// The first listener's port, with `port=0` resolved to the kernel's pick.
        port: u16,
        /// The run is taken by `serve`, so the server serves once.
        run: crate::sync::Mutex<Option<ServerRun<Endpoints>>>,
    },
    /// The server takes only the connections its connector makes in this process.
    Memory {
        connector: MemoryConnector,
        /// The run is taken by `serve`, so the server serves once.
        run: crate::sync::Mutex<Option<ServerRun<MemorySource>>>,
    },
}

impl BoundServer {
    pub(super) fn tcp(run: ServerRun<Endpoints>) -> Self {
        BoundServer::Tcp {
            port: run.transport.primary().addr.port(),
            run: crate::sync::Mutex::new(Some(run)),
        }
    }

    pub(super) fn memory(run: ServerRun<MemorySource>, connector: MemoryConnector) -> Self {
        BoundServer::Memory {
            connector,
            run: crate::sync::Mutex::new(Some(run)),
        }
    }
}

/// Serves `run` once; Python can ask twice, and the second time is a `RuntimeError`.
fn serve_once<D: mipyre_transport::driver::TransportDriver>(
    run: &crate::sync::Mutex<Option<ServerRun<D>>>,
    py: Python<'_>,
) -> PyResult<()> {
    let run = run.lock().take().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("this server has already served; start() another")
    })?;
    run.serve(py)
}

/// One server of an app, bound by `MipyreApp.start()`; its listeners are released when
/// `serve()` returns.
#[pyclass(module = "mipyre.engine", frozen)]
pub(crate) struct Server {
    /// The token is shared with the run and cancelled by `shutdown()` or SIGINT.
    stop: CancellationToken,
    bound: BoundServer,
}

impl Server {
    pub(super) fn new(stop: CancellationToken, bound: BoundServer) -> Self {
        Server { stop, bound }
    }
}

#[pymethods]
impl Server {
    /// Serve until SIGINT or `shutdown()`, blocking this thread, then drain in-flight
    /// connections and return.
    ///
    /// A server stopped before it served returns at once; serving a second time raises
    /// `RuntimeError`.
    pub(crate) fn serve(&self, py: Python<'_>) -> PyResult<()> {
        match &self.bound {
            BoundServer::Tcp { run, .. } => serve_once(run, py),
            BoundServer::Memory { run, .. } => serve_once(run, py),
        }
    }

    /// Stop this server the way SIGINT does; it may be called from any thread, any number of
    /// times, and other servers of the same app keep serving.
    fn shutdown(&self) {
        self.stop.cancel();
    }

    /// Return the port of this server's first listener, or `None` for a memory server.
    #[getter]
    fn port(&self) -> Option<u16> {
        match &self.bound {
            BoundServer::Tcp { port, .. } => Some(*port),
            BoundServer::Memory { .. } => None,
        }
    }

    /// Return a new in-memory connection to this memory server as a blocking `socket.socket`,
    /// served like a TCP connection from its accept loop on.
    ///
    /// A connection made before the server serves waits in the backlog. It raises
    /// `ConnectionRefusedError` once the server takes no more connections, and `RuntimeError`
    /// on a server that listens on sockets.
    fn connect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        use std::os::fd::{AsRawFd, IntoRawFd, OwnedFd};

        let connector = match &self.bound {
            BoundServer::Memory { connector, .. } => connector,
            BoundServer::Tcp { port, .. } => {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "this server listens on sockets (port {port}), not in memory: connect to its port"
                )))
            }
        };
        let client = OwnedFd::from(connector.connect().map_err(|e| match e {
            MemoryConnectError::Refused => pyo3::exceptions::PyConnectionRefusedError::new_err(e.to_string()),
            MemoryConnectError::BacklogFull => pyo3::exceptions::PyConnectionError::new_err(e.to_string()),
            MemoryConnectError::Socketpair(os) => PyErr::from(os),
        })?);
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item(pyo3::intern!(py, "fileno"), client.as_raw_fd())?;
        // `client` closes the descriptor if the socket is not made; once it is, the socket
        // owns it.
        let socket = py
            .import(pyo3::intern!(py, "socket"))?
            .getattr(pyo3::intern!(py, "socket"))?
            .call((), Some(&kwargs))?;
        let _owned_by_the_socket: std::os::fd::RawFd = client.into_raw_fd();
        Ok(socket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_transport_is_tcp_by_default_and_an_unknown_one_is_refused_by_name() {
        assert_eq!(transport(None).unwrap(), Transport::Tcp);
        assert_eq!(transport(Some("tcp")).unwrap(), Transport::Tcp);
        assert_eq!(transport(Some("memory")).unwrap(), Transport::Memory);
        Python::initialize();
        let err = transport(Some("udp")).unwrap_err();
        assert!(err.to_string().contains("\"udp\""), "{err}");
    }

    #[test]
    fn a_memory_server_refuses_every_socket_argument_given_by_name() {
        assert!(socket_args_given(None, None, None, None, None, None).is_empty());
        assert_eq!(
            socket_args_given(None, Some(0), Some("c.pem"), None, None, Some(&[])),
            vec!["port", "tls_cert", "extra_plain_ports"]
        );
    }

    #[test]
    fn a_host_is_an_ip_address_and_its_error_names_it() {
        assert_eq!(
            socket_addr("127.0.0.1", 80).unwrap(),
            "127.0.0.1:80".parse().unwrap()
        );
        assert_eq!(socket_addr("::1", 80).unwrap(), "[::1]:80".parse().unwrap());
        assert_eq!(
            socket_addr("[::1]", 80).unwrap(),
            "[::1]:80".parse().unwrap()
        );
        for host in ["localhost", "", "1.2.3", "[127.0.0.1"] {
            let text = socket_addr(host, 80).unwrap_err().to_string();
            assert!(text.starts_with(&format!("host {host:?} ")), "{text}");
        }
    }
}
