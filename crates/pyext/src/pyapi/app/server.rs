//! Defines one bound server of an app and parses `start()`'s transport, address and count
//! arguments.

use std::net::SocketAddr;
use std::num::NonZeroUsize;

use mipyre_http::plan::site::Protocols;
use mipyre_transport::endpoint::{Endpoints, ListenerSet, ListenerSpec};
use mipyre_transport::memory::{MemoryConnectError, MemoryConnector, MemorySource};
use pyo3::prelude::*;
use tokio_util::sync::CancellationToken;

use crate::boot::{ReadinessCallback, ServerFlagValues, ServerRun, DEFAULT_HOST, DEFAULT_PORT};

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

/// Names the socket flags a server was given (by its deployment or its code).
fn socket_flags_given(values: &ServerFlagValues) -> Vec<&'static str> {
    [
        ("host", values.host.is_some()),
        ("port", values.port.is_some()),
        ("tls_cert", values.tls_cert.is_some()),
        ("tls_key", values.tls_key.is_some()),
        ("extra_tls_ports", values.extra_tls_ports.is_some()),
        ("extra_plain_ports", values.extra_plain_ports.is_some()),
        ("allowed_hosts", values.allowed_hosts.is_some()),
        ("trusted_proxies", values.trusted_proxies.is_some()),
    ]
    .into_iter()
    .filter_map(|(name, given)| given.then_some(name))
    .collect()
}

/// Refuses socket flags for a memory server with a `ValueError` naming them, rather than
/// silently dropping them.
pub(super) fn refuse_socket_flags(values: &ServerFlagValues) -> PyResult<()> {
    let given = socket_flags_given(values);
    if given.is_empty() {
        return Ok(());
    }
    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "transport=\"memory\" listens on no socket, so {} cannot be given",
        given.join(", ")
    )))
}

/// Builds the listener set a server's socket flags name. The certificate and key come in pairs
/// (the recipe checked that); extra TLS ports without them are refused, because the operator
/// would believe them protected.
pub(super) fn listener_set(values: &ServerFlagValues) -> PyResult<ListenerSet> {
    let host = values.host.as_ref().map_or(DEFAULT_HOST, |host| host.value().as_str());
    let port = values.port.as_ref().map_or(DEFAULT_PORT, |port| *port.value());
    let addr = socket_addr(host, port)?;
    let tls = match (&values.tls_cert, &values.tls_key) {
        (Some(cert), Some(key)) => Some(
            mipyre_transport::tls::build_acceptor(
                &cert.value().to_string_lossy(),
                &key.value().to_string_lossy(),
                Protocols::Http2AndHttp1.alpn(),
            )
            .map_err(|e| tls_exception(&e, cert, key))?,
        ),
        _ => None,
    };
    let ports = |given: &Option<crate::boot::FlagValue<Vec<std::num::NonZeroU16>>>| -> Vec<u16> {
        given
            .as_ref()
            .map_or_else(Vec::new, |ports| ports.value().iter().map(|p| p.get()).collect())
    };
    ListenerSpec::set(
        addr,
        tls,
        &ports(&values.extra_tls_ports),
        &ports(&values.extra_plain_ports),
    )
    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

/// Raises a TLS file that cannot be used as a `ConfigError` naming its flag and where it was given,
/// and a failure of neither file (the system's random source) as `RuntimeError`.
fn tls_exception(
    e: &mipyre_transport::tls::TlsError,
    cert: &crate::boot::FlagValue<std::path::PathBuf>,
    key: &crate::boot::FlagValue<std::path::PathBuf>,
) -> PyErr {
    use mipyre_transport::tls::PemFile;
    let (flag, given) = match e.file() {
        Some(PemFile::Certificate) => ("tls_cert", cert),
        Some(PemFile::PrivateKey) => ("tls_key", key),
        None => return pyo3::exceptions::PyRuntimeError::new_err(e.to_string()),
    };
    crate::boot::ConfigError::from(crate::boot::FlagError::TlsFileUnusable {
        flag,
        from: given.source(),
        reason: e.to_string(),
    })
    .into()
}

/// The flags that name the addresses a server binds: the `.socket` unit names them instead
/// when systemd hands the sockets over.
const ADDRESS_FLAGS: [&str; 4] = ["host", "port", "extra_tls_ports", "extra_plain_ports"];

/// Returns the address flags `values` gives (from the deployment or from code), which a server
/// serving sockets systemd handed over does not use.
pub(super) fn address_flags(values: &ServerFlagValues) -> Vec<(&'static crate::boot::Flag, crate::boot::FlagSource)> {
    values
        .given()
        .into_iter()
        .filter(|(flag, _)| ADDRESS_FLAGS.contains(&flag.name))
        .collect()
}

/// Binds every listener of `specs` before any thread or worker exists, so a port in use is
/// this call's `OSError(EADDRINUSE)` rather than a log line from a serving thread.
///
/// Sockets systemd handed over are served instead, each with the server's TLS; then every
/// address flag that was given (`addresses`) is unused, and the start fails naming each.
pub(super) fn bind(
    specs: &ListenerSet,
    copies: NonZeroUsize,
    addresses: &[(&'static crate::boot::Flag, crate::boot::FlagSource)],
) -> PyResult<Endpoints> {
    let tls = specs.specs().first().and_then(|spec| spec.tls.clone());
    match mipyre_server::handed_by_systemd(tls, copies).map_err(crate::boot::passed_exception)? {
        None => Endpoints::bind(specs, copies, mipyre_server::bind_options()).map_err(crate::boot::listener_exception),
        Some(handed) => match crate::boot::ConfigError::of(handed_socket_errors(addresses)) {
            Some(unused) => Err(unused.into()),
            None => {
                println!("  Serving the sockets systemd handed over (LISTEN_FDS)");
                Ok(handed)
            }
        },
    }
}

fn handed_socket_errors(
    addresses: &[(&'static crate::boot::Flag, crate::boot::FlagSource)],
) -> Vec<crate::boot::FlagError> {
    addresses
        .iter()
        .map(|(flag, given)| crate::boot::FlagError::UnusedByTopology {
            flag: flag.name,
            given: given.clone(),
            user: "a server serving the sockets systemd handed over (LISTEN_FDS)".to_string(),
            needs: "a server that binds its own sockets: the .socket unit names the addresses, so list every one there",
        })
        .collect()
}

/// Parses `host` as an IPv4 or IPv6 address, bare or in brackets; a name such as `localhost`
/// is refused, not resolved.
fn socket_addr(host: &str, port: u16) -> Result<SocketAddr, BadHost> {
    let ip = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')).unwrap_or(host);
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
    readiness: ReadinessCallback,
) -> PyResult<()> {
    let run = run
        .lock()
        .take()
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("this server has already served; start() another"))?;
    run.serve(py, readiness)
}

/// One server of an app, bound by `MipyreApp.start()`; its listeners are released when
/// `serve()` returns.
#[pyclass(module = "mipyre.engine", frozen)]
pub(crate) struct Server {
    /// The token is shared with the run and cancelled by `shutdown()` or SIGINT.
    stop: CancellationToken,
    bound: BoundServer,
    /// What `metrics()` asks, during the run and after.
    parts: RunSources,
}

/// The parts of one run its metrics are read from, which say what it did and what it runs without: the server's counts
/// (which also hold why its threads run unpinned), its workers' counts, its process lanes, and its static mounts.
pub(super) struct RunSources {
    counters: std::sync::Arc<mipyre_server::ServerCounters>,
    workers: std::sync::Arc<mipyre_runtime::WorkerCounters>,
    lanes: Option<std::sync::Arc<mipyre_process::ServerLanes>>,
    mounts: Vec<mipyre_http::io::static_files::StaticMount>,
}

impl RunSources {
    pub(super) fn of(site: &mipyre_server::Site, workers: &std::sync::Arc<mipyre_runtime::WorkerCounters>) -> Self {
        RunSources {
            counters: std::sync::Arc::clone(&site.counters.run),
            workers: std::sync::Arc::clone(workers),
            lanes: site.process.clone(),
            mounts: site.rules.routes.static_mounts().to_vec(),
        }
    }

    fn static_bytes_in_flight(&self) -> u64 {
        self.mounts
            .iter()
            .map(mipyre_http::io::static_files::StaticMount::bytes_in_flight)
            .sum()
    }

    /// Returns each part the run runs without, as `component: reason`, asking each owner.
    fn degraded(&self, counts: &mipyre_server::ServerCounts) -> Vec<String> {
        let pinning = counts.unpinned_because.iter().map(|why| format!("cpu_pinning: {why}"));
        let lanes = self
            .lanes
            .iter()
            .flat_map(|lanes| lanes.crash_looping())
            .map(|(i, why)| format!("process_lane:{i}: {why}"));
        let mounts = self.mounts.iter().filter_map(|mount| {
            mount
                .uncached_because()
                .map(|(root, why)| format!("static_watch:{}: {why}", root.display()))
        });
        pinning.chain(lanes).chain(mounts).collect()
    }
}

impl Server {
    pub(super) fn new(stop: CancellationToken, bound: BoundServer, parts: RunSources) -> Self {
        Server { stop, bound, parts }
    }
}

#[pymethods]
impl Server {
    /// Serve until SIGINT or `shutdown()`, blocking this thread, then drain in-flight
    /// connections and return.
    ///
    /// `on_serving` is called once on this thread when the server serves: its workers are up
    /// and it takes connections next. If it raises, the server stops and `serve()` raises that.
    /// A server stopped before it served returns at once without calling it; serving a second
    /// time raises `RuntimeError`.
    #[pyo3(signature = (on_serving=None))]
    pub(crate) fn serve(&self, py: Python<'_>, on_serving: Option<Py<PyAny>>) -> PyResult<()> {
        let readiness = ReadinessCallback::new(on_serving);
        match &self.bound {
            BoundServer::Tcp { run, .. } => serve_once(run, py, readiness),
            BoundServer::Memory { run, .. } => serve_once(run, py, readiness),
        }
    }

    /// This server's counts so far: its answers, refusals, workers and the rest of what one run
    /// counts, apart from any other server of the process.
    fn metrics(&self) -> crate::pyapi::config::RunMetrics {
        let counts = self.parts.counters.read();
        let degraded = self.parts.degraded(&counts);
        crate::pyapi::config::RunMetrics {
            counts,
            workers: self.parts.workers.read(),
            static_bytes_in_flight: self.parts.static_bytes_in_flight(),
            degraded,
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
        let given = |pairs: &[(&str, &str)]| {
            let mut code = crate::boot::ServerFlags::default();
            for (keyword, raw) in pairs {
                code.give(crate::boot::CodeCall::ServerFlags, keyword, raw).unwrap();
            }
            socket_flags_given(&crate::boot::Deployment::default().over(&code))
        };
        assert!(given(&[("threads", "2")]).is_empty());
        assert_eq!(
            given(&[
                ("port", "0"),
                ("tls_cert", "c.pem"),
                ("extra_plain_ports", "8001"),
                ("allowed_hosts", "a.example"),
                ("trusted_proxies", "10.0.0.1")
            ]),
            vec![
                "port",
                "tls_cert",
                "extra_plain_ports",
                "allowed_hosts",
                "trusted_proxies"
            ]
        );
    }

    #[test]
    fn a_host_is_an_ip_address_and_its_error_names_it() {
        assert_eq!(socket_addr("127.0.0.1", 80).unwrap(), "127.0.0.1:80".parse().unwrap());
        assert_eq!(socket_addr("::1", 80).unwrap(), "[::1]:80".parse().unwrap());
        assert_eq!(socket_addr("[::1]", 80).unwrap(), "[::1]:80".parse().unwrap());
        for host in ["localhost", "", "1.2.3", "[127.0.0.1"] {
            let text = socket_addr(host, 80).unwrap_err().to_string();
            assert!(text.starts_with(&format!("host {host:?} ")), "{text}");
        }
    }
}
