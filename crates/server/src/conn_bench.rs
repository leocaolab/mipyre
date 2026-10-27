//! Measures what one request costs in `drive_connection` on one in-memory connection, sending
//! `CONN_BENCH_N` requests one after another over HTTP/1 or HTTP/2 to a service that answers at
//! once.
//!
//! By default each test sends a few requests and asserts each is answered; to count, run one by
//! name with `CONN_BENCH_N` set:
//!
//!   CONN_BENCH_N=20000 cargo test --release -p mipyre_pyext --lib -- --exact serve::conn_bench::http1_requests
//!
//! On Linux, instructions per request are `perf stat -x, -e instructions:u` on that test with
//! `CONN_BENCH_N=20000`, minus the same with `CONN_BENCH_N=10000`, divided by 10000; the
//! in-process client is counted too. The production cdylib's fat LTO inlines differently from the
//! test binary, so this test only shows where in the driver a cost is.

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

use crate::conn::{drive_connection, ConnectionTimeouts, LocalExec};
use mipyre_http::io::body::ResponseBody;
use mipyre_http::io::clock::Clock;

fn requests() -> u32 {
    std::env::var("CONN_BENCH_N").map_or(16, |n| n.parse().expect("CONN_BENCH_N is a count"))
}

async fn ok(_: hyper::Request<Incoming>) -> Result<hyper::Response<ResponseBody>, hyper::Error> {
    Ok(hyper::Response::new(ResponseBody::Full(Full::new(Bytes::from_static(
        b"ok",
    )))))
}

/// Runs `client` against one connection served by `drive_connection` on a current-thread runtime,
/// as a TPC thread serves it; the clock is real, but nothing waits on it.
fn bench<F, Fut>(client: F)
where
    F: FnOnce(DuplexStream) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a bench runtime builds");
    tokio::task::LocalSet::new().block_on(&rt, async {
        let (client_io, server) = tokio::io::duplex(64 * 1024);
        let done = tokio::task::spawn_local(drive_connection(
            server,
            service_fn(ok),
            LocalExec,
            ConnectionTimeouts::DEFAULT,
            Clock::process(),
            std::future::pending::<()>(),
        ));
        client(client_io).await;
        done.abort();
    });
}

/// Sends `CONN_BENCH_N` HTTP/1 keep-alive requests, reading each response whole before the next.
#[test]
fn http1_requests() {
    bench(|mut c| async move {
        const GET: &[u8] = b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";
        let mut buf = [0u8; 4096];
        for _ in 0..requests() {
            c.write_all(GET).await.expect("the pipe holds it");
            // The whole response ends with its 2-byte body after the blank line.
            let mut got = 0;
            loop {
                let n = c.read(&mut buf[got..]).await.expect("the pipe reads");
                assert!(n > 0, "the server closed before answering");
                got += n;
                if got >= 6 && buf[..got].windows(4).any(|w| w == b"\r\n\r\n") && buf[got - 2..got] == *b"ok" {
                    break;
                }
            }
        }
    });
}

/// Sends `CONN_BENCH_N` HTTP/2 requests on one connection, one stream at a time as `h2c-json`
/// does.
#[test]
fn http2_requests() {
    bench(|mut c| async move {
        let mut hello = Vec::new();
        hello.extend_from_slice(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        // An empty SETTINGS frame, then a WINDOW_UPDATE that opens the connection's receive
        // window wide.
        hello.extend_from_slice(&[0, 0, 0, 0x4, 0, 0, 0, 0, 0]);
        hello.extend_from_slice(&[0, 0, 4, 0x8, 0, 0, 0, 0, 0, 0x7f, 0xff, 0x00, 0x00]);
        c.write_all(&hello).await.expect("the pipe holds it");
        let mut acked = false;
        for i in 0..requests() {
            let stream = 2 * i + 1;
            // HEADERS, END_HEADERS | END_STREAM: `:method GET`, `:scheme http`, `:path /`, and
            // `:authority t` as a literal without indexing (the dynamic table stays empty).
            let mut frame = vec![0, 0, 6, 0x1, 0x5];
            frame.extend_from_slice(&stream.to_be_bytes());
            frame.extend_from_slice(&[0x82, 0x86, 0x84, 0x01, 0x01, b't']);
            c.write_all(&frame).await.expect("the pipe holds it");
            loop {
                let mut head = [0u8; 9];
                c.read_exact(&mut head).await.expect("a frame header");
                let len = usize::from(head[0]) << 16 | usize::from(head[1]) << 8 | usize::from(head[2]);
                let mut payload = vec![0u8; len];
                c.read_exact(&mut payload).await.expect("a frame payload");
                let (kind, flags) = (head[3], head[4]);
                let on = u32::from_be_bytes([head[5] & 0x7f, head[6], head[7], head[8]]);
                if kind == 0x4 && flags & 0x1 == 0 && !acked {
                    c.write_all(&[0, 0, 0, 0x4, 0x1, 0, 0, 0, 0])
                        .await
                        .expect("the pipe holds it");
                    acked = true;
                }
                // A PING is answered, as a live client does.
                if kind == 0x6 && flags & 0x1 == 0 {
                    let mut pong = vec![0, 0, 8, 0x6, 0x1, 0, 0, 0, 0];
                    pong.extend_from_slice(&payload);
                    c.write_all(&pong).await.expect("the pipe holds it");
                }
                if on == stream && (kind == 0 || kind == 1) && flags & 0x1 != 0 {
                    break;
                }
            }
        }
    });
}
