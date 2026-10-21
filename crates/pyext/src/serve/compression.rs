//! Compresses HTTP responses on the serving side, with a per-thread output buffer pool, inline or
//! on the blocking pool; negotiation, the plan, the codecs and the headers are
//! `mipyre_http::plan::compression`'s.
//!
//! Compression is off unless the app calls `app.enable_compression()`, and while it is off the hot
//! path is one `Option` check. A large body, or one for a slow codec setting, is compressed on the
//! blocking pool so it never stalls a tokio worker.

use bytes::Bytes;
use std::cell::RefCell;

use mipyre_http::plan::compression::{self, Codec, CompressionSettings, GzipStream};
use mipyre_http::plan::response::HandlerResponse;


/// Bytes of buffers one thread keeps for reuse; a buffer that would take its pool past this
/// is dropped instead, so the pools' memory is bounded (per serving thread).
const POOL_MAX_BYTES_PER_THREAD: usize = 4 << 20;
const POOL_SLOTS_PER_THREAD: usize = 8;

/// Recycled output buffers, per thread. A TPC thread compresses and sends its own
/// responses, so a buffer goes back to the thread that used it: no lock every core takes
/// per compressed response, and no buffer written by one core and reused by another.
#[derive(Default)]
struct BufferPool {
    bufs: Vec<Vec<u8>>,
}

thread_local! {
    static COMPRESS_POOL: RefCell<BufferPool> = RefCell::new(BufferPool::default());
    /// Kept per thread for the same reason as the buffers: no lock, and no stream one core wrote
    /// and another reads. A blocking-pool thread keeps one too, freed when the thread exits.
    static GZIP_STREAM: RefCell<GzipStream> = RefCell::new(GzipStream::default());
}

impl BufferPool {
    fn take(&mut self) -> Option<Vec<u8>> {
        self.bufs.pop()
    }

    fn bytes(&self) -> usize {
        self.bufs.iter().map(Vec::capacity).sum()
    }

    fn give(&mut self, mut buf: Vec<u8>) {
        let cap = buf.capacity();
        if self.bufs.len() < POOL_SLOTS_PER_THREAD
            && self.bytes() + cap <= POOL_MAX_BYTES_PER_THREAD
        {
            buf.clear();
            self.bufs.push(buf);
        }
    }
}

/// A compression output buffer that goes back to this thread's pool when dropped.
struct PooledBuffer(Vec<u8>);

impl AsRef<[u8]> for PooledBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        let buf = std::mem::take(&mut self.0);
        #[expect(
            clippy::let_underscore_must_use,
            reason = "`try_with` fails only while the thread's locals are destroyed: the buffer is freed instead of pooled"
        )]
        let _ = COMPRESS_POOL.try_with(|pool| pool.borrow_mut().give(buf));
    }
}


/// Compresses `body` with `codec` into a buffer from this thread's pool, returning `None` when
/// encoding failed, which is logged, or did not shrink the body.
fn run(body: &[u8], codec: Codec) -> Option<Bytes> {
    let mut buf = COMPRESS_POOL
        .try_with(|pool| pool.borrow_mut().take())
        .ok()
        .flatten()
        .unwrap_or_else(|| Vec::with_capacity(body.len() / 2 + 64));
    buf.clear();

    let encoded = match codec {
        Codec::Gzip(level) => GZIP_STREAM
            .try_with(|stream| stream.borrow_mut().encode(body, level, &mut buf))
            // The thread is exiting and its stream is already destroyed.
            .unwrap_or_else(|_| GzipStream::default().encode(body, level, &mut buf)),
        Codec::Brotli(quality) => compression::brotli(body, quality, &mut buf),
    };
    let buf = PooledBuffer(buf);
    match encoded {
        Ok(()) => (buf.0.len() < body.len()).then(|| Bytes::from_owner(buf)),
        Err(e) => {
            tracing::warn!(
                target: "mipyre::server",
                error = %e,
                encoding = e.codec.coding().name(),
                "response compression failed; sending the body uncompressed"
            );
            None
        }
    }
}

/// Compresses `data` for a client that sent `accept_encoding` as the app's `settings` allow; a
/// large body is compressed on the blocking pool.
pub(crate) async fn compress(
    mut data: HandlerResponse,
    accept_encoding: &str,
    settings: &CompressionSettings,
) -> HandlerResponse {
    let Some(plan) = compression::plan(&data, accept_encoding, settings) else {
        return data;
    };
    let codec = plan.codec();
    let compressed = if plan.runs_inline() {
        run(&data.body, codec)
    } else {
        // The blocking pool needs its own reference to the body, which costs one refcount.
        let body = data.body.clone();
        off_the_worker(move || run(&body, codec), codec.coding().name()).await
    };
    if let Some(compressed) = compressed {
        compression::apply(&mut data, compressed, codec);
    }
    data
}

/// Runs an encode on the blocking pool so a large body does not hold the worker. A task that gives
/// no answer is logged here once and yields `None`, so the body goes out uncompressed: a panicked
/// encoder is logged as an error with the panic's text, and a task cancelled at shutdown as a
/// warning.
async fn off_the_worker<J>(job: J, encoding: &str) -> Option<Bytes>
where
    J: FnOnce() -> Option<Bytes> + Send + 'static,
{
    match tokio::task::spawn_blocking(job).await {
        Ok(compressed) => compressed,
        Err(failed) => {
            match failed.try_into_panic() {
                Ok(payload) => tracing::error!(
                    target: "mipyre::server",
                    panic = %mipyre_tpc::panic_message(&*payload),
                    encoding,
                    "response compression panicked; sending the body uncompressed"
                ),
                Err(cancelled) => tracing::warn!(
                    target: "mipyre::server",
                    error = %cancelled,
                    encoding,
                    "response compression task did not finish; sending the body uncompressed"
                ),
            }
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mipyre_http::plan::compression::{plan, INLINE_MAX_BYTES};
    use crate::serve::pipeline::AcceptEncoding;
    use hyper::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, CONTENT_ENCODING, VARY};
    use mipyre_http::plan::response::HeaderLines;

    fn settings(min_size: i64, gzip: bool, brotli: bool) -> CompressionSettings {
        CompressionSettings::parse(min_size, gzip, brotli, 6, 4).unwrap()
    }

    fn json(body: impl Into<Bytes>) -> HandlerResponse {
        json_with(body, HeaderLines::new())
    }

    fn json_with(body: impl Into<Bytes>, headers: HeaderLines) -> HandlerResponse {
        HandlerResponse::new(
            hyper::StatusCode::OK,
            body.into(),
            HeaderValue::from_static("application/json"),
            headers,
        )
    }

    /// Runs `data` through the request's `AcceptEncoding` gate and `compress` on a current-thread
    /// runtime, as a TPC thread does; `None` settings means compression is off.
    fn compressed(
        data: HandlerResponse,
        accept: &str,
        settings: Option<&CompressionSettings>,
    ) -> HandlerResponse {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_str(accept).unwrap());
        let accept_encoding = AcceptEncoding::from_headers(&headers, settings);
        let Some((accepted, settings)) = accept_encoding.negotiable() else {
            return data;
        };
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(compress(data, accepted, settings))
    }

    #[test]
    fn no_settings_is_noop() {
        let data = compressed(json(vec![b'x'; 2048]), "gzip, br", None);
        assert_eq!(data.body.len(), 2048);
        assert!(!data.headers().contains_key("content-encoding"));
    }

    #[test]
    fn enabled_compresses_json() {
        let payload = vec![b'x'; 4096];
        let data = compressed(
            json(payload.clone()),
            "br, gzip",
            Some(&settings(100, true, true)),
        );
        assert!(data.body.len() < payload.len());
        assert_eq!(data.headers().get("content-encoding").unwrap(), "br");
        assert_eq!(
            data.headers()
                .get("vary")
                .unwrap()
                .to_str()
                .unwrap()
                .to_ascii_lowercase(),
            "accept-encoding"
        );
    }

    #[test]
    fn large_body_compresses_off_the_worker() {
        let payload = vec![b'y'; INLINE_MAX_BYTES * 4];
        let job = plan(&json(payload.clone()), "gzip", &settings(100, true, false)).unwrap();
        assert!(!job.runs_inline());
        let data = compressed(
            json(payload.clone()),
            "gzip",
            Some(&settings(100, true, false)),
        );
        assert!(data.body.len() < payload.len());
        assert_eq!(data.headers().get("content-encoding").unwrap(), "gzip");
    }

    fn on_a_blocking_pool<T>(f: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    /// An encode that panics on the blocking pool comes back as a panic with its text, logged at
    /// error with the body sent uncompressed, not as a cancelled task.
    #[test]
    fn an_encode_that_panics_off_the_worker_is_a_panic_with_its_text() {
        let mut failed = Some(Bytes::new());
        let log = crate::exchange::captured_log(|| {
            failed = on_a_blocking_pool(off_the_worker(|| panic!("the encoder exploded"), "gzip"));
        });
        assert!(failed.is_none(), "the body goes out uncompressed: {failed:?}");
        let line = log.lines().find(|l| l.contains("panicked")).expect(&log);
        assert!(line.contains("\"level\":\"ERROR\""), "{line}");
        assert!(line.contains("the encoder exploded"), "{line}");
    }

    #[test]
    fn an_encode_off_the_worker_hands_back_its_result() {
        let done = on_a_blocking_pool(off_the_worker(|| Some(Bytes::from_static(b"z")), "gzip"));
        assert!(matches!(done, Some(b) if b.as_ref() == b"z"));
    }

    #[test]
    fn handler_content_encoding_preserved() {
        let mut headers = HeaderLines::new();
        assert!(headers.append(CONTENT_ENCODING, HeaderValue::from_static("identity")).is_none());
        let data = json_with(vec![b'x'; 2048], headers);
        let data = compressed(data, "gzip, br", Some(&settings(100, true, true)));
        assert_eq!(data.headers().get("Content-Encoding").unwrap(), "identity");
    }

    #[test]
    fn vary_merges_with_existing() {
        let mut headers = HeaderLines::new();
        assert!(headers.append(VARY, HeaderValue::from_static("Origin")).is_none());
        let data = json_with(vec![b'a'; 4096], headers);
        let data = compressed(data, "gzip", Some(&settings(100, true, true)));
        let vary = data
            .headers()
            .get("Vary")
            .unwrap()
            .to_str()
            .unwrap()
            .to_ascii_lowercase();
        assert!(vary.contains("origin"));
        assert!(vary.contains("accept-encoding"));
    }
}
