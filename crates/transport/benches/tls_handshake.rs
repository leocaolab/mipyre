//! Measures what a new TLS connection costs the server and how often it resumes, by where the
//! resumption state lives: `production` is what `tls::acceptor_from` builds, `shared-cache` is one
//! rustls `ServerSessionMemoryCache` of 256 sessions behind one mutex with no tickets,
//! `per-thread-cache` is one such cache per accept loop, `tickets` is rustls' stateless ticketer with
//! the cache kept for TLS 1.2 session ids, and `tickets-no-cache` is the ticketer alone.
//!
//! The two roles run as two processes, so that a profiler attached to the server counts the server
//! alone:
//!
//!   tls_handshake serve <variant> <loops> <secs>
//!       binds 127.0.0.1:<free port> as `SO_REUSEPORT` accept loops (one thread and
//!       current-thread runtime each, as a thread-per-core loop), prints `port <n>`, serves for <secs>, then
//!       prints the handshakes it completed (full / resumed) and the allocations it made.
//!       Each connection: handshake, read one request, write one response, wait for the close.
//!   tls_handshake load <tls13|tls12> <full|resume> <threads> <identities> <secs> <port>
//!       <identities> clients, each its own `ClientConfig` (its own session store: `full` stores
//!       nothing), spread over <threads> threads, each thread connecting its clients in turn,
//!       one request per connection. Every reconnect comes from a new source port, so
//!       `SO_REUSEPORT` may hand it to any loop. The client does not verify the certificate:
//!       its cost is not what is measured, and the cheaper it is the fewer cores it needs.
//!
//! `benchmarks/tls_handshake/run.sh` runs the scaling curve, the hit rates and the per-handshake
//! counts (perf stat / perf trace / perf c2c) with server and client on separate cores.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mipyre_microbench::Must;
use mipyre_transport::driver::TransportDriver;
use mipyre_transport::endpoint::{EndpointSource, Endpoints, ListenerSet, ListenerSpec};
use mipyre_transport::tcp::listener::{BindOptions, DEFAULT_BACKLOG};
use mipyre_transport::tls;
use rustls::server::{NoServerSessionStorage, ProducesTickets, ServerSessionMemoryCache};
use rustls::{HandshakeKind, ServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsAcceptor;

/// Counts each allocation over `System` on the thread that makes it, because a process-wide counter
/// would be one more cache line every loop writes, which is what this bench looks for in rustls.
struct ThreadAllocationCounter;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

// SAFETY: Every call goes to `System` unchanged, so its guarantees hold.
unsafe impl GlobalAlloc for ThreadAllocationCounter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|n| n.set(n.get() + 1));
        // SAFETY: The caller's layout is passed on unchanged.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout the caller gives back are passed on unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: ThreadAllocationCounter = ThreadAllocationCounter;

/// Issues no tickets, as rustls' default ticketer does, which rustls does not export.
#[derive(Debug)]
struct NoTickets;

impl ProducesTickets for NoTickets {
    fn enabled(&self) -> bool {
        false
    }
    fn lifetime(&self) -> u32 {
        0
    }
    fn encrypt(&self, _plain: &[u8]) -> Option<Vec<u8>> {
        None
    }
    fn decrypt(&self, _cipher: &[u8]) -> Option<Vec<u8>> {
        None
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResumptionState {
    Production,
    SharedCache,
    PerThreadCache,
    Tickets,
    TicketsNoCache,
}

impl ResumptionState {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "production" => ResumptionState::Production,
            "shared-cache" => ResumptionState::SharedCache,
            "per-thread-cache" => ResumptionState::PerThreadCache,
            "tickets" => ResumptionState::Tickets,
            "tickets-no-cache" => ResumptionState::TicketsNoCache,
            _ => return None,
        })
    }
}

/// Matches rustls' default cache size.
const CACHE_SESSIONS: usize = 256;

/// Returns the production config with this variant's resumption state; a shared variant gives every
/// loop `shared`, and the per-thread variant makes its own per call.
fn config(variant: ResumptionState, production: &ServerConfig, shared: &ServerConfig) -> Arc<ServerConfig> {
    match variant {
        ResumptionState::PerThreadCache => {
            let mut cfg = production.clone();
            cfg.session_storage = ServerSessionMemoryCache::new(CACHE_SESSIONS);
            cfg.ticketer = Arc::new(NoTickets);
            Arc::new(cfg)
        }
        _ => Arc::new(shared.clone()),
    }
}

fn shared_config(variant: ResumptionState, production: &ServerConfig) -> ServerConfig {
    let mut cfg = production.clone();
    match variant {
        ResumptionState::Production | ResumptionState::PerThreadCache => {}
        ResumptionState::SharedCache => {
            cfg.session_storage = ServerSessionMemoryCache::new(CACHE_SESSIONS);
            cfg.ticketer = Arc::new(NoTickets);
        }
        ResumptionState::Tickets => {
            cfg.session_storage = ServerSessionMemoryCache::new(CACHE_SESSIONS);
            cfg.ticketer = rustls::crypto::ring::Ticketer::new().must("make a ticketer");
        }
        ResumptionState::TicketsNoCache => {
            cfg.session_storage = Arc::new(NoServerSessionStorage {});
            cfg.ticketer = rustls::crypto::ring::Ticketer::new().must("make a ticketer");
        }
    }
    cfg
}

/// Counts what one accept loop did, on a cache line of its own because only its loop writes it.
#[repr(align(128))]
#[derive(Default)]
struct LoopCounts {
    full: AtomicU64,
    resumed: AtomicU64,
    failed: AtomicU64,
    allocations: AtomicU64,
}

const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";

/// Reads one request, writes [`RESPONSE`] and waits for the client to close. The server closes an
/// HTTP/1.0 request itself, because `openssl s_time -www` in the `suites` experiment reads until
/// the server closes.
async fn answer(inner: &mut tokio_rustls::server::TlsStream<tokio::net::TcpStream>) -> std::io::Result<()> {
    let mut buf = [0u8; 1024];
    let mut seen = 0usize;
    loop {
        match inner.read(&mut buf[seen..]).await? {
            0 => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            n => seen += n,
        }
        if buf[..seen].windows(4).any(|w| w == b"\r\n\r\n") || seen == buf.len() {
            break;
        }
    }
    let http10 = buf[..seen]
        .split(|&b| b == b'\n')
        .next()
        .is_some_and(|line| line.trim_ascii_end().ends_with(b" HTTP/1.0"));
    inner.write_all(RESPONSE).await?;
    inner.flush().await?;
    if http10 {
        return inner.shutdown().await;
    }
    // The client closes with a reset, so that no TIME_WAIT piles up on the port.
    let _closed = inner.read(&mut buf).await;
    Ok(())
}

async fn serve_one(stream: std::net::TcpStream, acceptor: &TlsAcceptor, counts: &LoopCounts) {
    let stream = match stream
        .set_nonblocking(true)
        .and_then(|()| tokio::net::TcpStream::from_std(stream))
    {
        Ok(stream) => stream,
        Err(_) => {
            counts.failed.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let Some(mut inner) = tls::secure(acceptor, stream).await else {
        counts.failed.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let kind = inner.get_ref().1.handshake_kind();
    if answer(&mut inner).await.is_err() {
        counts.failed.fetch_add(1, Ordering::Relaxed);
        return;
    }
    match kind {
        Some(HandshakeKind::Resumed) => counts.resumed.fetch_add(1, Ordering::Relaxed),
        _ => counts.full.fetch_add(1, Ordering::Relaxed),
    };
}

fn serve(args: &[String]) {
    let variant = args
        .first()
        .and_then(|v| ResumptionState::parse(v))
        .must("variant: production | shared-cache | per-thread-cache | tickets | tickets-no-cache");
    let loops: usize = args.get(1).and_then(|n| n.parse().ok()).must("loops");
    let secs: u64 = args.get(2).and_then(|n| n.parse().ok()).must("secs");

    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).must("make a certificate");
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(key_pair.serialize_der().into());
    let production =
        tls::acceptor_from(vec![cert.der().clone()], key, &[b"http/1.1"]).must("build the production acceptor");
    let production = production.config().as_ref().clone();
    let shared = shared_config(variant, &production);

    let copies = NonZeroUsize::new(loops).must("loops must be at least 1");
    let options = BindOptions {
        backlog: DEFAULT_BACKLOG,
        defer_accept: Duration::from_secs(10),
    };
    let bound = Endpoints::bind(
        &ListenerSet::one(ListenerSpec {
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            tls: None,
        }),
        copies,
        options,
    )
    .must("bind the reuseport set");
    let port = bound.primary().addr.port();

    let counts: Arc<Vec<LoopCounts>> = Arc::new((0..loops).map(|_| LoopCounts::default()).collect());
    for (index, one_loop) in bound.into_loops().into_iter().enumerate() {
        let counts = Arc::clone(&counts);
        let acceptor = TlsAcceptor::from(config(variant, &production, &shared));
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .must("build the loop's runtime");
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async move {
                let mut source = EndpointSource::new(one_loop).must("register the listeners");
                let acceptor = std::rc::Rc::new(acceptor);
                loop {
                    let connection = source.accept().await.must("accept");
                    let acceptor = std::rc::Rc::clone(&acceptor);
                    let counts = Arc::clone(&counts);
                    tokio::task::spawn_local(async move {
                        let mine = &counts[index];
                        serve_one(connection.tcp.stream, &acceptor, mine).await;
                        mine.allocations.store(ALLOCATIONS.with(Cell::get), Ordering::Relaxed);
                    });
                }
            });
        });
    }
    println!("port {port}");
    std::io::stdout().flush().must("print the port");
    std::thread::sleep(Duration::from_secs(secs));
    let sum = |f: fn(&LoopCounts) -> &AtomicU64| -> u64 { counts.iter().map(|c| f(c).load(Ordering::Relaxed)).sum() };
    let (full, resumed) = (sum(|c| &c.full), sum(|c| &c.resumed));
    let handshakes = full + resumed;
    let allocations = sum(|c| &c.allocations);
    let per_loop: Vec<String> = counts
        .iter()
        .map(|c| (c.full.load(Ordering::Relaxed) + c.resumed.load(Ordering::Relaxed)).to_string())
        .collect();
    println!(
        "serve {variant:?} loops={loops} secs={secs}: handshakes={handshakes} full={full} \
         resumed={resumed} failed={} rate={:.0}/s allocations/handshake={:.1} per_loop=[{}]",
        sum(|c| &c.failed),
        handshakes as f64 / secs as f64,
        if handshakes == 0 {
            0.0
        } else {
            allocations as f64 / handshakes as f64
        },
        per_loop.join(","),
    );
    std::io::stdout().flush().must("print the counts");
    std::process::exit(0);
}

/// Accepts any certificate, because the server's cost is what is measured, not the client's.
#[derive(Debug)]
struct NullVerifier(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NullVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn client_config(tls12: bool, resume: bool) -> Arc<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let versions: &[&rustls::SupportedProtocolVersion] = if tls12 {
        &[&rustls::version::TLS12]
    } else {
        &[&rustls::version::TLS13]
    };
    let mut cfg = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(versions)
        .must("protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NullVerifier(provider)))
        .with_no_client_auth();
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    // The default store holds 256 sessions. A much smaller one holds nothing, because rustls sizes
    // it in servers of 8 sessions each and a cache of one server evicts what it just stored.
    cfg.resumption = if resume {
        rustls::client::Resumption::default()
    } else {
        rustls::client::Resumption::disabled()
    };
    Arc::new(cfg)
}

/// Sends one request on a new connection and returns whether it resumed, or `None` if it failed.
fn one_request(cfg: &Arc<rustls::ClientConfig>, addr: SocketAddr) -> Option<bool> {
    let mut sock = TcpStream::connect(addr).ok()?;
    sock.set_nodelay(true).ok()?;
    let name = rustls::pki_types::ServerName::try_from("localhost").ok()?;
    let mut conn = rustls::ClientConnection::new(Arc::clone(cfg), name).ok()?;
    let mut tls = rustls::Stream::new(&mut conn, &mut sock);
    tls.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n").ok()?;
    let mut buf = [0u8; 256];
    let mut seen = 0usize;
    while seen < RESPONSE.len() {
        match tls.read(&mut buf[seen..]) {
            Ok(0) | Err(_) => return None,
            Ok(n) => seen += n,
        }
    }
    let resumed = conn.handshake_kind() == Some(HandshakeKind::Resumed);
    // Closing with a reset, not a FIN, leaves no TIME_WAIT on either end however many connections
    // a run makes.
    socket2::SockRef::from(&sock).set_linger(Some(Duration::ZERO)).ok()?;
    Some(resumed)
}

fn load(args: &[String]) {
    let tls12 = match args.first().map(String::as_str) {
        Some("tls13") => false,
        Some("tls12") => true,
        _ => {
            eprintln!("version: tls13 | tls12");
            std::process::exit(2)
        }
    };
    let resume = match args.get(1).map(String::as_str) {
        Some("full") => false,
        Some("resume") => true,
        _ => {
            eprintln!("mode: full | resume");
            std::process::exit(2)
        }
    };
    let threads: usize = args.get(2).and_then(|n| n.parse().ok()).must("threads");
    let identities: usize = args.get(3).and_then(|n| n.parse().ok()).must("identities");
    let secs: u64 = args.get(4).and_then(|n| n.parse().ok()).must("secs");
    let port: u16 = args.get(5).and_then(|n| n.parse().ok()).must("port");
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let deadline = Instant::now() + Duration::from_secs(secs);
    let workers: Vec<_> = (0..threads)
        .map(|t| {
            let mine: Vec<_> = (t..identities)
                .step_by(threads)
                .map(|_| client_config(tls12, resume))
                .collect();
            std::thread::spawn(move || {
                let (mut done, mut resumed, mut failed) = (0u64, 0u64, 0u64);
                'run: loop {
                    for cfg in &mine {
                        if Instant::now() >= deadline {
                            break 'run;
                        }
                        match one_request(cfg, addr) {
                            Some(r) => {
                                done += 1;
                                resumed += u64::from(r);
                            }
                            None => failed += 1,
                        }
                    }
                }
                (done, resumed, failed)
            })
        })
        .collect();
    let (mut done, mut resumed, mut failed) = (0u64, 0u64, 0u64);
    for w in workers {
        let (d, r, f) = w.join().must("a client thread");
        done += d;
        resumed += r;
        failed += f;
    }
    println!(
        "load {} {} threads={threads} identities={identities} secs={secs}: requests={done} \
         rate={:.0}/s resumed={resumed} hit={:.1}% failed={failed}",
        if tls12 { "tls12" } else { "tls13" },
        if resume { "resume" } else { "full" },
        done as f64 / secs as f64,
        if done == 0 {
            0.0
        } else {
            100.0 * resumed as f64 / done as f64
        },
    );
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).filter(|a| a != "--bench").collect();
    match args.first().map(String::as_str) {
        Some("serve") => serve(&args[1..]),
        Some("load") => load(&args[1..]),
        _ => {
            eprintln!("tls_handshake serve <variant> <loops> <secs> | load <tls13|tls12> <full|resume> <threads> <identities> <secs> <port>");
            std::process::exit(2)
        }
    }
}
