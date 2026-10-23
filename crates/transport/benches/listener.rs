//! Measures what each accepted connection costs before it is served: setting TCP_QUICKACK, which is
//! a no-op off Linux, and one accept from a TLS endpoint up to the connection handed out (the
//! handshake is not in it).
use std::alloc::System;
use std::hint::black_box;
use std::io::Write as _;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;

use mipyre_microbench::{measure, run, AllocationCounter, Case, Must};
use mipyre_transport::endpoint::{EndpointSource, Endpoints, ListenerSet, ListenerSpec};
use mipyre_transport::tcp::events::{self, TcpEvent};
use mipyre_transport::tcp::listener::{BindOptions, DEFAULT_BACKLOG};
use mipyre_transport::tls;

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

fn quickack(n: u64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .must("build the runtime");
    rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.must("bind");
        let addr = listener.local_addr().must("local_addr");
        let (client, accepted) = tokio::join!(tokio::net::TcpStream::connect(addr), listener.accept());
        let _client = client.must("connect");
        let (stream, _) = accepted.must("accept");
        let stream = stream.into_std().must("into_std");
        let before = events::total(TcpEvent::QuickackFailed);
        measure("quickack", n, |_| {
            mipyre_transport::tcp::listener::setup_tcp_quickack(black_box(&stream));
        });
        assert_eq!(
            events::total(TcpEvent::QuickackFailed),
            before,
            "every setsockopt succeeded"
        );
    });
}

/// Accepts one connection per call from a TLS endpoint, so the count covers polling the socket,
/// setting the connection's options, and handing it out with its acceptor. The client resets its
/// end before the server drops its own, so no socket is left in TIME_WAIT across the calls.
fn accept(n: u64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .must("build the runtime");
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).must("make a certificate");
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(key_pair.serialize_der().into());
    let acceptor = tls::acceptor_from(vec![cert.der().clone()], key, &[b"http/1.1"]).must("build the acceptor");
    let specs = ListenerSet::one(ListenerSpec {
        addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        tls: Some(acceptor),
    });
    let options = BindOptions {
        backlog: DEFAULT_BACKLOG,
        defer_accept: Duration::from_secs(10),
    };
    let bound = Endpoints::bind(&specs, NonZeroUsize::MIN, options).must("bind");
    let addr = bound.primary().addr;
    let mut source = {
        let _registering = rt.enter();
        EndpointSource::new(bound).must("register")
    };
    measure("accept", n, |_| {
        let mut client = std::net::TcpStream::connect(addr).must("connect");
        // Linux's TCP_DEFER_ACCEPT reports a connection once its first byte arrives.
        client.write_all(b"G").must("write");
        let accepted = rt.block_on(source.accept()).must("accept");
        socket2::SockRef::from(&client)
            .set_linger(Some(Duration::ZERO))
            .must("set_linger");
        drop(client);
        drop(black_box(accepted));
    });
}

fn main() {
    let cases: [Case; 2] = [("quickack", quickack), ("accept", accept)];
    run(&cases);
}
