//! Builds the TLS acceptor from PEM files and runs the server's side of the handshake over any
//! byte stream.

pub mod events;

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::pem::{self, PemObject};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

use events::TlsEvent;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PemFile {
    Certificate,
    PrivateKey,
}

impl std::fmt::Display for PemFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            PemFile::Certificate => "cert",
            PemFile::PrivateKey => "key",
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("open {what} {path:?}: {source}")]
    Unreadable {
        what: PemFile,
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("parse {what} {path:?}: {source}")]
    NotPem {
        what: PemFile,
        path: PathBuf,
        source: pem::Error,
    },
    #[error("no certificates found in {0:?}")]
    NoCertificate(PathBuf),
    #[error("no private key found in {0:?}")]
    NoKey(PathBuf),
    #[error("TLS config error: {0}")]
    RejectedByRustls(#[from] rustls::Error),
    /// The system's random source failed.
    #[error("make the session-ticket key: {0}")]
    TicketKeyNotGenerated(rustls::Error),
}

/// Lists every ring suite in the server's preference, SHA-256 suites before SHA-384 ones.
///
/// OpenSSL-based clients list AES-256-GCM-SHA384 first, and ring's SHA-384 has no AVX path on
/// AMD, so following the client costs about 29% more handshake instructions for no security a
/// 128-bit suite lacks. AES-128 comes before ChaCha20 because AES-GCM runs on AES-NI.
const CIPHER_SUITES: &[rustls::SupportedCipherSuite] = &[
    rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256,
    rustls::crypto::ring::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
    rustls::crypto::ring::cipher_suite::TLS13_AES_256_GCM_SHA384,
    rustls::crypto::ring::cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
    rustls::crypto::ring::cipher_suite::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
    rustls::crypto::ring::cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
    rustls::crypto::ring::cipher_suite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
    rustls::crypto::ring::cipher_suite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
    rustls::crypto::ring::cipher_suite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
];

const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

fn open(what: PemFile, path: &Path) -> Result<BufReader<File>, TlsError> {
    File::open(path)
        .map(BufReader::new)
        .map_err(|source| TlsError::Unreadable {
            what,
            path: path.to_path_buf(),
            source,
        })
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let reader = open(PemFile::Certificate, path)?;
    let certs = CertificateDer::pem_reader_iter(reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsError::NotPem {
            what: PemFile::Certificate,
            path: path.to_path_buf(),
            source,
        })?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificate(path.to_path_buf()));
    }
    Ok(certs)
}

/// Reads the first private key in the file, in PKCS#8, PKCS#1 or SEC1 form.
fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let reader = open(PemFile::PrivateKey, path)?;
    PrivateKeyDer::from_pem_reader(reader).map_err(|source| match source {
        pem::Error::NoItemsFound => TlsError::NoKey(path.to_path_buf()),
        source => TlsError::NotPem {
            what: PemFile::PrivateKey,
            path: path.to_path_buf(),
            source,
        },
    })
}

/// Builds a `TlsAcceptor` from PEM certificate and key files, offering `alpn` in ALPN, most
/// preferred first.
///
/// # Errors
///
/// Returns an error if a file cannot be read or parsed, holds no certificate or key, or rustls
/// rejects the pair.
pub fn build_acceptor(cert_path: &str, key_path: &str, alpn: &[&[u8]]) -> Result<Arc<TlsAcceptor>, TlsError> {
    let certs = load_certs(Path::new(cert_path))?;
    let key = load_key(Path::new(key_path))?;
    acceptor_from(certs, key, alpn)
}

/// Builds a `TlsAcceptor` from a certificate chain and key already in memory.
///
/// # Errors
///
/// Returns an error if rustls rejects the pair or the session-ticket key cannot be made.
pub fn acceptor_from(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    alpn: &[&[u8]],
) -> Result<Arc<TlsAcceptor>, TlsError> {
    // The provider is named here rather than taken from the process default, because
    // `install_default` is first-come and another dependency could decide the crypto on this port.
    let provider = Arc::new(rustls::crypto::CryptoProvider {
        cipher_suites: CIPHER_SUITES.to_vec(),
        ..rustls::crypto::ring::default_provider()
    });
    let mut cfg = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    cfg.alpn_protocols = alpn.iter().map(|id| id.to_vec()).collect();
    cfg.ignore_client_order = true;

    // Stateless tickets replace rustls' default cache, which holds 256 sessions while TLS 1.3
    // stores two per handshake, so past about 128 returning clients none resumed. A ticket also
    // resumes on whichever accept loop the reconnect lands on. The key lives only in this
    // process's memory and rotates every 6 h. A TLS 1.2 ticket carries the master secret, so the
    // key opens the TLS 1.2 sessions it sealed until it rotates out; TLS 1.3 tickets do not.
    cfg.ticketer =
        rustls::crypto::ring::Ticketer::new().map_err(TlsError::TicketKeyNotGenerated)?;
    cfg.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});

    Ok(Arc::new(TlsAcceptor::from(Arc::new(cfg))))
}

/// Performs the TLS handshake under a fixed timeout, so a peer dribbling its ClientHello
/// cannot hold a descriptor and a task forever.
///
/// # Errors
///
/// Returns tokio-rustls's error for a refused handshake, or `ErrorKind::TimedOut`.
pub async fn handshake<IO>(acceptor: &TlsAcceptor, stream: IO) -> std::io::Result<TlsStream<IO>>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
        Ok(handshaken) => handshaken,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("TLS handshake timed out after {HANDSHAKE_TIMEOUT:?} (possible Slowloris)"),
        )),
    }
}

/// Wraps `stream` in TLS, or returns `None` when the handshake fails.
///
/// A failed handshake is the peer's doing, so it is counted, each one logs at debug, and a
/// warning once a minute carries the count.
pub async fn secure<IO>(acceptor: &TlsAcceptor, stream: IO) -> Option<TlsStream<IO>>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    match handshake(acceptor, stream).await {
        Ok(stream) => Some(stream),
        Err(e) => {
            let warn = events::degrade(TlsEvent::HandshakeFailed);
            tracing::debug!(error = %e, "TLS handshake failed");
            if let Some(held) = warn {
                tracing::warn!(error = %e, held, "TLS handshake failed");
            }
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;

    const HTTP1: &[&[u8]] = &[b"http/1.1"];
    const H2_AND_HTTP1: &[&[u8]] = &[b"h2", b"http/1.1"];

    #[test]
    fn a_missing_cert_is_a_typed_open_error_naming_the_file() {
        let err = build_acceptor(
            "/nonexistent/missing-cert.pem",
            "/nonexistent/missing-key.pem",
            HTTP1,
        )
        .err()
        .expect("no such file");
        match &err {
            TlsError::Unreadable { what, path, source } => {
                assert_eq!(*what, PemFile::Certificate);
                assert_eq!(path, Path::new("/nonexistent/missing-cert.pem"));
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
            }
            other => panic!("expected TlsError::Unreadable, got {other:?}"),
        }
        assert!(err.to_string().contains("missing-cert.pem"), "{err}");
    }

    #[test]
    fn an_empty_cert_file_says_so() {
        let dir = std::env::temp_dir().join(format!("mipyre-tls-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert = dir.join("empty.pem");
        std::fs::write(&cert, b"").unwrap();
        let err = build_acceptor(
            cert.to_str().unwrap(),
            "/nonexistent/key.pem",
            HTTP1,
        )
        .err()
        .expect("no certificate");
        assert!(
            matches!(err, TlsError::NoCertificate(ref p) if p == &cert),
            "{err:?}"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_key_file_holding_only_a_certificate_has_no_key() {
        let cert = TestCert::new("no-key");
        let pem = cert.dir.join("cert.pem");
        let err = build_acceptor(pem.to_str().unwrap(), pem.to_str().unwrap(), HTTP1)
            .err()
            .expect("no private key");
        assert!(matches!(err, TlsError::NoKey(ref p) if p == &pem), "{err:?}");
    }

    #[test]
    fn a_key_file_with_a_broken_section_is_not_pem() {
        let cert = TestCert::new("broken-key");
        let key = cert.dir.join("broken.pem");
        std::fs::write(&key, b"-----BEGIN PRIVATE KEY-----\nnot base64 at all!\n").unwrap();
        let err = build_acceptor(
            cert.dir.join("cert.pem").to_str().unwrap(),
            key.to_str().unwrap(),
            HTTP1,
        )
        .err()
        .expect("a broken key");
        assert!(
            matches!(err, TlsError::NotPem { what: PemFile::PrivateKey, ref path, .. } if path == &key),
            "{err:?}"
        );
    }

    /// Holds a self-signed `localhost` certificate as PEM files in a directory removed on drop.
    struct TestCert {
        dir: std::path::PathBuf,
        der: CertificateDer<'static>,
    }

    impl TestCert {
        fn new(test: &str) -> Self {
            let rcgen::CertifiedKey { cert, key_pair } =
                rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
                    .expect("generate a certificate");
            let dir = std::env::temp_dir()
                .join(format!("infra-http-tls-{test}-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("cert.pem"), cert.pem()).unwrap();
            std::fs::write(dir.join("key.pem"), key_pair.serialize_pem()).unwrap();
            TestCert {
                dir,
                der: cert.der().clone(),
            }
        }

        fn acceptor(&self, alpn: &[&[u8]]) -> Arc<TlsAcceptor> {
            let path = |name: &str| self.dir.join(name).to_str().unwrap().to_string();
            build_acceptor(&path("cert.pem"), &path("key.pem"), alpn)
                .expect("a valid certificate builds an acceptor")
        }

        fn connector(&self, alpn: &[&[u8]]) -> tokio_rustls::TlsConnector {
            let mut roots = rustls::RootCertStore::empty();
            roots.add(self.der.clone()).unwrap();
            let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
            config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
            tokio_rustls::TlsConnector::from(Arc::new(config))
        }
    }

    impl Drop for TestCert {
        fn drop(&mut self) {
            if let Err(e) = std::fs::remove_dir_all(&self.dir) {
                eprintln!("leaving {}: {e}", self.dir.display());
            }
        }
    }

    /// Returns the protocol negotiated after a round trip of data both ways, or `None` when the
    /// handshake fails.
    async fn negotiate(cert: &TestCert, served: &[&[u8]], alpn: &[&[u8]]) -> Option<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let acceptor = cert.acceptor(served);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut tls = secure(&acceptor, stream).await?;
            let mut got = [0u8; 4];
            tls.read_exact(&mut got).await.unwrap();
            tls.write_all(&got).await.unwrap();
            tls.flush().await.unwrap();
            Some(())
        });
        let tcp = TcpStream::connect(addr).await.unwrap();
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let negotiated = match cert.connector(alpn).connect(name, tcp).await {
            Ok(mut client) => {
                client.write_all(b"ping").await.unwrap();
                let mut echoed = [0u8; 4];
                client.read_exact(&mut echoed).await.unwrap();
                assert_eq!(&echoed, b"ping");
                client.get_ref().1.alpn_protocol().map(<[u8]>::to_vec)
            }
            Err(_) => None,
        };
        let served = server.await.unwrap();
        assert_eq!(served.is_some(), negotiated.is_some());
        negotiated
    }

    #[tokio::test]
    async fn a_server_of_both_protocols_negotiates_h2() {
        let cert = TestCert::new("both");
        let got = negotiate(&cert, H2_AND_HTTP1, &[b"h2", b"http/1.1"]).await;
        assert_eq!(got.as_deref(), Some(&b"h2"[..]));
        let got = negotiate(&cert, H2_AND_HTTP1, &[b"http/1.1"]).await;
        assert_eq!(got.as_deref(), Some(&b"http/1.1"[..]));
    }

    #[tokio::test]
    async fn an_http1_only_server_never_negotiates_h2() {
        let cert = TestCert::new("http1");
        let got = negotiate(&cert, HTTP1, &[b"h2", b"http/1.1"]).await;
        assert_eq!(got.as_deref(), Some(&b"http/1.1"[..]));
        let failed = events::total(TlsEvent::HandshakeFailed);
        let got = negotiate(&cert, HTTP1, &[b"h2"]).await;
        assert_eq!(got, None);
        assert!(events::total(TlsEvent::HandshakeFailed) > failed);
    }

    #[tokio::test]
    async fn plain_http_on_a_tls_port_fails_that_connection_and_the_next_handshake_completes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let cert = TestCert::new("plaintext");
        let acceptor = cert.acceptor(HTTP1);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
        let mut plain = client.unwrap();
        plain
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let failed = events::total(TlsEvent::HandshakeFailed);
        let wrapped = secure(&acceptor, accepted.unwrap().0).await;
        assert!(wrapped.is_none(), "a plaintext request is no TLS handshake");
        assert!(events::total(TlsEvent::HandshakeFailed) > failed);
        // The plaintext client reads an alert or nothing and then EOF, never a response.
        let mut answered = Vec::new();
        #[expect(
            clippy::let_underscore_must_use,
            reason = "the server may reset rather than close; either way the read ends"
        )]
        let _ = plain.read_to_end(&mut answered).await;
        assert!(!answered.starts_with(b"HTTP/"), "{answered:?}");

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut tls = secure(&acceptor, stream).await?;
            let mut got = [0u8; 4];
            tls.read_exact(&mut got).await.unwrap();
            tls.write_all(&got).await.unwrap();
            tls.flush().await.unwrap();
            Some(())
        });
        let tcp = TcpStream::connect(addr).await.unwrap();
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut client = cert
            .connector(&[b"http/1.1"])
            .connect(name, tcp)
            .await
            .expect("the port still completes a TLS handshake");
        client.write_all(b"ping").await.unwrap();
        let mut echoed = [0u8; 4];
        client.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"ping");
        assert!(server.await.unwrap().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_client_is_dropped_at_the_handshake_timeout() {
        let cert = TestCert::new("silent");
        let acceptor = cert.acceptor(HTTP1);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
        let _silent = client.unwrap();
        let failed = events::total(TlsEvent::HandshakeFailed);
        let started = tokio::time::Instant::now();
        let wrapped = secure(&acceptor, accepted.unwrap().0).await;
        assert!(wrapped.is_none());
        assert_eq!(started.elapsed(), HANDSHAKE_TIMEOUT);
        assert!(events::total(TlsEvent::HandshakeFailed) > failed);
    }

    /// Drives both ends through memory past the handshake and the session tickets TLS 1.3 sends
    /// right after it.
    fn handshake_in_memory(
        client: &mut rustls::ClientConnection,
        server: &mut rustls::ServerConnection,
    ) {
        let mut wire = Vec::new();
        while client.is_handshaking() || server.is_handshaking() || server.wants_write() {
            while client.wants_write() {
                client.write_tls(&mut wire).unwrap();
            }
            let mut unread = &wire[..];
            while !unread.is_empty() {
                server.read_tls(&mut unread).unwrap();
                server.process_new_packets().unwrap();
            }
            wire.clear();
            while server.wants_write() {
                server.write_tls(&mut wire).unwrap();
            }
            let mut unread = &wire[..];
            while !unread.is_empty() {
                client.read_tls(&mut unread).unwrap();
                client.process_new_packets().unwrap();
            }
            wire.clear();
        }
    }

    /// Lists the suites OpenSSL 3 offers by default, in its order.
    const OPENSSL_ORDER: &[rustls::SupportedCipherSuite] = &[
        rustls::crypto::ring::cipher_suite::TLS13_AES_256_GCM_SHA384,
        rustls::crypto::ring::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
        rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256,
        rustls::crypto::ring::cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
        rustls::crypto::ring::cipher_suite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
        rustls::crypto::ring::cipher_suite::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
        rustls::crypto::ring::cipher_suite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
        rustls::crypto::ring::cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
        rustls::crypto::ring::cipher_suite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
    ];

    fn negotiated_suite(
        cert: &TestCert,
        version: &'static rustls::SupportedProtocolVersion,
        offered: &[rustls::SupportedCipherSuite],
    ) -> rustls::CipherSuite {
        let server = Arc::clone(cert.acceptor(HTTP1).config());
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.der.clone()).unwrap();
        let provider = Arc::new(rustls::crypto::CryptoProvider {
            cipher_suites: offered.to_vec(),
            ..rustls::crypto::ring::default_provider()
        });
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[version])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut c = rustls::ClientConnection::new(Arc::new(client), name).unwrap();
        let mut s = rustls::ServerConnection::new(server).unwrap();
        handshake_in_memory(&mut c, &mut s);
        s.negotiated_cipher_suite().expect("the handshake completed").suite()
    }

    #[test]
    fn a_client_in_openssls_order_gets_the_sha256_suite() {
        let cert = TestCert::new("suite-order");
        assert_eq!(
            negotiated_suite(&cert, &rustls::version::TLS13, OPENSSL_ORDER),
            rustls::CipherSuite::TLS13_AES_128_GCM_SHA256
        );
        assert_eq!(
            negotiated_suite(&cert, &rustls::version::TLS12, OPENSSL_ORDER),
            rustls::CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
        );
    }

    #[test]
    fn a_client_that_offers_only_sha384_still_connects() {
        let cert = TestCert::new("suite-sha384");
        let only = [rustls::crypto::ring::cipher_suite::TLS13_AES_256_GCM_SHA384];
        assert_eq!(
            negotiated_suite(&cert, &rustls::version::TLS13, &only),
            rustls::CipherSuite::TLS13_AES_256_GCM_SHA384
        );
    }

    #[test]
    fn the_server_offers_every_suite_of_the_provider() {
        let suites = |list: &[rustls::SupportedCipherSuite]| {
            let mut ids: Vec<u16> = list.iter().map(|s| u16::from(s.suite())).collect();
            ids.sort_unstable();
            ids
        };
        assert_eq!(
            suites(CIPHER_SUITES),
            suites(rustls::crypto::ring::ALL_CIPHER_SUITES)
        );
    }

    /// It uses more clients than rustls' default session cache holds sessions.
    #[test]
    fn a_returning_client_resumes_however_many_clients_came_between() {
        let cert = TestCert::new("resume");
        let server = Arc::clone(cert.acceptor(HTTP1).config());
        for version in [&rustls::version::TLS13, &rustls::version::TLS12] {
            let clients: Vec<Arc<rustls::ClientConfig>> = (0..300)
                .map(|_| {
                    let mut roots = rustls::RootCertStore::empty();
                    roots.add(cert.der.clone()).unwrap();
                    let provider = Arc::new(rustls::crypto::ring::default_provider());
                    let mut config = rustls::ClientConfig::builder_with_provider(provider)
                        .with_protocol_versions(&[version])
                        .unwrap()
                        .with_root_certificates(roots)
                        .with_no_client_auth();
                    config.alpn_protocols = vec![b"http/1.1".to_vec()];
                    Arc::new(config)
                })
                .collect();
            let connect = |client: &Arc<rustls::ClientConfig>| {
                let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
                let mut c = rustls::ClientConnection::new(Arc::clone(client), name).unwrap();
                let mut s = rustls::ServerConnection::new(Arc::clone(&server)).unwrap();
                handshake_in_memory(&mut c, &mut s);
                assert_eq!(c.handshake_kind(), s.handshake_kind());
                s.handshake_kind()
            };
            for client in &clients {
                assert_eq!(connect(client), Some(rustls::HandshakeKind::Full), "{version:?}");
            }
            for client in &clients {
                assert_eq!(connect(client), Some(rustls::HandshakeKind::Resumed), "{version:?}");
            }
        }
    }
}
