//! Drives a bench's clients: pipelined keep-alive connections that count the responses they read.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::run::BenchPartError;
use crate::sync::{AtomicU64, Ordering};

/// Is the request every client connection sends, pipelined [`PIPELINE_DEPTH`] deep.
pub(crate) const BENCH_REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n";
const PIPELINE_DEPTH: usize = 32;
/// Bounds how long a loopback client retries a refused `connect` while the server threads bind.
const CONNECT_DEADLINE: Duration = Duration::from_secs(5);
const CONNECT_RETRY: Duration = Duration::from_millis(10);
/// Names the thread the loopback bench's clients run on.
pub(crate) const LOOPBACK_CLIENT: &str = "mipyre-lb-client";
/// Keeps the client runtime at two threads, because a few pipelined connections saturate many
/// server workers and more threads would take cores from the servers.
const LOOPBACK_CLIENT_THREADS: usize = 2;
/// Bounds the headers the probe parses; a bench route's response has far fewer.
const MAX_RESPONSE_HEADERS: usize = 64;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ClientError {
    #[error(transparent)]
    ConnectionIo(#[from] io::Error),
    #[error("the server closed the connection")]
    ServerClosed,
    #[error(transparent)]
    Response(#[from] ProbeError),
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ProbeError {
    #[error("unparseable response head: {0}")]
    UnparseableHead(httparse::Error),
    #[error("GET / answered {0}, not 200")]
    Not200(u16),
    #[error("the response has no Content-Length")]
    NoContentLength,
    #[error("invalid Content-Length {0:?}")]
    InvalidContentLength(String),
}

pub(crate) fn run_loopback_clients(
    addr: SocketAddr,
    conns: usize,
    counter: Arc<AtomicU64>,
    stop: CancellationToken,
) -> Vec<BenchPartError> {
    let rt = match RuntimeBuilder::new_multi_thread()
        .worker_threads(LOOPBACK_CLIENT_THREADS)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(source) => {
            return vec![BenchPartError::RuntimeNotBuilt {
                thread: LOOPBACK_CLIENT.to_string(),
                source,
            }]
        }
    };
    rt.block_on(async move {
        let clients = (0..conns)
            .map(|_| {
                let (counter, stop) = (Arc::clone(&counter), stop.clone());
                tokio::spawn(async move {
                    let stream = connect(addr).await?;
                    stream.set_nodelay(true)?;
                    drive_client(stream, &counter, &stop, BENCH_REQUEST).await
                })
            })
            .collect();
        client_failures(clients).await
    })
}

pub(crate) async fn client_failures(clients: Vec<JoinHandle<Result<(), ClientError>>>) -> Vec<BenchPartError> {
    futures_util::future::join_all(clients)
        .await
        .into_iter()
        .enumerate()
        .filter_map(|(conn, joined)| match joined {
            Ok(Ok(())) => None,
            Ok(Err(source)) => Some(BenchPartError::ClientFailed { conn, source }),
            Err(e) => Some(BenchPartError::ClientPanicked {
                conn,
                payload: e.to_string(),
            }),
        })
        .collect()
}

/// Probes one response to learn its size, then pipelines [`PIPELINE_DEPTH`] requests at a
/// time and reads exactly that many responses' bytes, until `stop`. Every response of the
/// bench route is byte-identical, so counting bytes counts responses.
pub(crate) async fn drive_client<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    counter: &AtomicU64,
    stop: &CancellationToken,
    request: &[u8],
) -> Result<(), ClientError> {
    let response_len = probe(&mut stream, request).await?;
    counter.fetch_add(1, Ordering::Relaxed);

    let batch = request.repeat(PIPELINE_DEPTH);
    let mut responses = vec![0u8; response_len * PIPELINE_DEPTH];
    while !stop.is_cancelled() {
        stream.write_all(&batch).await?;
        stream.read_exact(&mut responses).await?;
        counter.fetch_add(PIPELINE_DEPTH as u64, Ordering::Relaxed);
    }
    Ok(())
}

/// Sends one request, reads its whole response and returns the response's length.
async fn probe<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S, request: &[u8]) -> Result<usize, ClientError> {
    stream.write_all(request).await?;
    let mut buf = Vec::with_capacity(4096);
    loop {
        if stream.read_buf(&mut buf).await? == 0 {
            return Err(ClientError::ServerClosed);
        }
        if let Some(len) = response_len(&buf)? {
            let read = buf.len();
            if read < len {
                buf.resize(len, 0);
                stream.read_exact(&mut buf[read..]).await?;
            }
            return Ok(len);
        }
    }
}

/// Returns the length of the `200` response at the start of `buf`, or `None` while its head is
/// incomplete.
fn response_len(buf: &[u8]) -> Result<Option<usize>, ProbeError> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_RESPONSE_HEADERS];
    let mut response = httparse::Response::new(&mut headers);
    let head_len = match response.parse(buf).map_err(ProbeError::UnparseableHead)? {
        httparse::Status::Partial => return Ok(None),
        httparse::Status::Complete(len) => len,
    };
    let status = response
        .code
        .ok_or(ProbeError::UnparseableHead(httparse::Error::Status))?;
    if status != 200 {
        return Err(ProbeError::Not200(status));
    }
    let raw = response
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("content-length"))
        .ok_or(ProbeError::NoContentLength)?
        .value;
    let body_len = std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .ok_or_else(|| ProbeError::InvalidContentLength(String::from_utf8_lossy(raw).into()))?;
    Ok(Some(head_len + body_len))
}

async fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
    let deadline = Instant::now() + CONNECT_DEADLINE;
    loop {
        match TcpStream::connect(addr).await {
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused && Instant::now() < deadline => {
                tokio::time::sleep(CONNECT_RETRY).await
            }
            connected => return connected,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OK_HEAD: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 2\r\n\r\n";

    #[test]
    fn response_len_is_head_plus_content_length() {
        let mut full = OK_HEAD.to_vec();
        full.extend_from_slice(b"ok");
        assert_eq!(response_len(&full), Ok(Some(OK_HEAD.len() + 2)));
        // Only the head is needed.
        assert_eq!(response_len(OK_HEAD), Ok(Some(OK_HEAD.len() + 2)));
    }

    #[test]
    fn response_len_waits_for_the_whole_head() {
        assert_eq!(response_len(&OK_HEAD[..20]), Ok(None));
    }

    #[test]
    fn missing_content_length_is_an_error_not_an_empty_body() {
        let head = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n";
        assert_eq!(response_len(head), Err(ProbeError::NoContentLength));
    }

    #[test]
    fn bad_content_length_is_reported_with_its_value() {
        let head = b"HTTP/1.1 200 OK\r\ncontent-length: two\r\n\r\n";
        assert_eq!(response_len(head), Err(ProbeError::InvalidContentLength("two".into())));
    }

    #[test]
    fn a_non_200_response_is_an_error() {
        let head = b"HTTP/1.1 404 Not Found\r\ncontent-length: 9\r\n\r\nNot Found";
        assert_eq!(response_len(head), Err(ProbeError::Not200(404)));
    }

    #[tokio::test]
    async fn the_client_reports_a_404_instead_of_counting_it() {
        let (client_io, mut server_io) = tokio::io::duplex(4096);
        let server = tokio::spawn(async move {
            let mut request = vec![0u8; BENCH_REQUEST.len()];
            server_io.read_exact(&mut request).await.unwrap();
            server_io
                .write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 9\r\n\r\nNot Found")
                .await
                .unwrap();
            server_io
        });
        let counter = AtomicU64::new(0);
        let result = drive_client(client_io, &counter, &CancellationToken::new(), BENCH_REQUEST).await;
        assert!(
            matches!(result, Err(ClientError::Response(ProbeError::Not200(404)))),
            "{result:?}"
        );
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        drop(server.await.unwrap());
    }
}
