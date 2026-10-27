//! Lets a handler stream a response body, such as Server-Sent Events, through a `Stream`.
//!
//! `close()` ends the response at once, whatever still references the stream, because waiting
//! for Python's GC to drop it would hold the client's connection open.
//!
//! A producer that finds the buffer full waits for room, woken when the client's connection takes
//! a chunk, the client leaves or the stream is closed; nothing here waits on a clock.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use hyper::header::{HeaderMap, HeaderValue};
use hyper::StatusCode;
use pyo3::exceptions::PyConnectionError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::{mpsc, Notify};

use crate::sync::OnceLock;
use mipyre_http::plan::response::HandlerResponse;

use crate::exchange::loop_wait::on_running_loop;
use crate::exchange::response::{
    header_text, header_value, http_status, response_headers_from_py, response_headers_to_py, sent_content_type,
};

/// This many chunks are buffered for a slow client before a fast producer gets `BlockingIOError`
/// and waits for room, instead of growing the buffer without bound.
const STREAM_CHANNEL_CAP: usize = 1024;

type StreamItem = Result<Bytes, std::convert::Infallible>;

/// What ended a stream for its producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamEnd {
    Closed,
    ClientGone,
}

impl StreamEnd {
    fn into_pyerr(self) -> PyErr {
        PyConnectionError::new_err(match self {
            StreamEnd::Closed => "stream was explicitly closed",
            StreamEnd::ClientGone => "client disconnected",
        })
    }
}

/// Ends when a chunk would fit, or with how the stream ended.
type RoomWait = Pin<Box<dyn Future<Output = Result<(), StreamEnd>> + Send>>;

#[pyclass(frozen, name = "Stream", module = "mipyre.engine")]
pub struct ResponseStream {
    /// The sender is `None` once `close()` dropped it, which ends the response.
    tx: crate::sync::poisoning::Mutex<Option<mpsc::Sender<StreamItem>>>,
    rx: crate::sync::poisoning::Mutex<Option<mpsc::Receiver<StreamItem>>>,
    /// Wakes the producers waiting for room when `close()` ends the stream; the first wait makes it.
    closing: OnceLock<Arc<Notify>>,
    pub(crate) head: StreamHead,
}

/// The head is validated when the `Stream` is made.
#[derive(Clone)]
pub struct StreamHead {
    /// A `Content-Type` in `headers` overrides this ([`Self::sent_content_type`]).
    pub content_type: HeaderValue,
    pub status: StatusCode,
    pub headers: HeaderMap,
}

impl StreamHead {
    /// Returns a `Content-Type` from `headers` if there is one, else `content_type`.
    pub(crate) fn sent_content_type(&self) -> &HeaderValue {
        sent_content_type(&self.headers, None).unwrap_or(&self.content_type)
    }
}

/// What a handler answers, whichever backend ran it: a whole response, or a stream whose body
/// follows. Only a main-interpreter handler can stream; every other backend answers `Response`.
pub enum HandlerReply {
    Response(HandlerResponse),
    Stream(StreamReply),
}

impl From<HandlerResponse> for HandlerReply {
    fn from(response: HandlerResponse) -> Self {
        HandlerReply::Response(response)
    }
}

/// A stream taken from its `Stream`, for the connection to send.
pub struct StreamReply {
    pub rx: mpsc::Receiver<StreamItem>,
    pub head: StreamHead,
}

#[pymethods]
impl ResponseStream {
    /// Creates a stream, `text/event-stream` by default, with `headers` as for `Response`.
    #[new]
    #[pyo3(signature = (content_type=None, status_code=200, headers=None))]
    fn new(content_type: Option<&str>, status_code: u16, headers: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let content_type = match content_type {
            Some(ct) => header_value("content_type", ct)?,
            None => HeaderValue::from_static("text/event-stream"),
        };
        let headers = headers.map(response_headers_from_py).transpose()?.unwrap_or_default();
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAP);
        Ok(ResponseStream {
            tx: crate::sync::poisoning::Mutex::new(Some(tx)),
            rx: crate::sync::poisoning::Mutex::new(Some(rx)),
            closing: OnceLock::new(),
            head: StreamHead {
                content_type,
                status: http_status(status_code)?,
                headers,
            },
        })
    }

    #[getter]
    fn status_code(&self) -> u16 {
        self.head.status.as_u16()
    }

    /// Returns a `Content-Type` from `headers` if there is one, else `content_type`.
    #[getter]
    fn content_type(&self) -> PyResult<&str> {
        header_text(self.head.sent_content_type())
    }

    #[getter]
    fn headers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        response_headers_to_py(&self.head.headers, py)
    }

    /// Sends a raw chunk without blocking; raises `BlockingIOError` when a slow client has let
    /// the buffer fill, and the caller waits for room (`wait_writable`) before sending it again.
    fn send(&self, data: &str) -> PyResult<()> {
        self.push(Bytes::copy_from_slice(data.as_bytes()))
    }

    /// Sends one SSE event as [`sse_event`] writes it.
    #[pyo3(signature = (data=None, event=None, id=None, retry=None))]
    fn send_event(
        &self,
        data: Option<&str>,
        event: Option<&str>,
        id: Option<&str>,
        retry: Option<u64>,
    ) -> PyResult<()> {
        // A line break in `id` or `event` would inject arbitrary SSE fields, since every SSE field is one line.
        if let Some(id) = id {
            if id.contains(['\n', '\r']) {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "SSE id must not contain newline or carriage-return characters",
                ));
            }
        }
        if let Some(event) = event {
            if event.contains(['\n', '\r']) {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "SSE event name must not contain newline or carriage-return characters",
                ));
            }
        }
        self.push(Bytes::from(sse_event(data, event, id, retry)))
    }

    /// Waits with the GIL released until a chunk would fit; raises `ConnectionError` once the
    /// stream is closed or the client is gone. It blocks the calling thread, so an `async def`
    /// producer awaits `wait_writable_async()` instead.
    fn wait_writable(&self, py: Python<'_>) -> PyResult<()> {
        let room = self.room();
        py.detach(|| mipyre_tpc::block_on(room)).map_err(StreamEnd::into_pyerr)
    }

    /// Returns a future on the running asyncio loop that is done when a chunk would fit, or fails
    /// with `ConnectionError` once the stream is closed or the client is gone; cancelling it stops
    /// the wait. Each call opens a doorbell fd (`OSError` when none can be had) for the loop to
    /// watch until the future is done.
    fn wait_writable_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let room = self.room();
        on_running_loop(py, async move { room.await.map_err(StreamEnd::into_pyerr) })
    }

    /// Ends the response now; a later `send` raises `ConnectionError`, and a producer waiting for
    /// room wakes with it.
    fn close(&self) {
        drop(self.tx.lock().unwrap_or_else(|e| e.into_inner()).take());
        if let Some(closing) = self.closing.get() {
            closing.notify_waiters();
        }
    }
}

/// Returns an event's text: `id:`, `event:` and `retry:` lines for the fields given, one `data:`
/// line per line of `data`, then a blank line. Without `data` there is no `data:` line, so the
/// client dispatches nothing; `Some("")` is one empty `data:` line, an empty message. `id` and
/// `event` must hold no line break.
fn sse_event(data: Option<&str>, event: Option<&str>, id: Option<&str>, retry: Option<u64>) -> String {
    let mut msg = String::with_capacity(data.map_or(0, str::len) + 64);
    if let Some(id) = id {
        msg.push_str("id: ");
        msg.push_str(id);
        msg.push('\n');
    }
    if let Some(event) = event {
        msg.push_str("event: ");
        msg.push_str(event);
        msg.push('\n');
    }
    if let Some(retry) = retry {
        msg.push_str("retry: ");
        msg.push_str(itoa::Buffer::new().format(retry));
        msg.push('\n');
    }
    if let Some(data) = data {
        for line in sse_lines(data) {
            msg.push_str("data: ");
            msg.push_str(line);
            msg.push('\n');
        }
    }
    msg.push('\n');
    msg
}

/// Splits at a bare `\r` too, because SSE treats it as a line ending and it would otherwise start
/// a field of its own.
fn sse_lines(data: &str) -> impl Iterator<Item = &str> {
    let body = data
        .strip_suffix("\r\n")
        .or_else(|| data.strip_suffix(['\n', '\r']))
        .unwrap_or(data);
    body.split("\r\n").flat_map(|part| part.split(['\r', '\n']))
}

impl ResponseStream {
    /// Raises `BlockingIOError` when the channel is full, and `ConnectionError` once the stream
    /// is closed or the client is gone.
    fn push(&self, chunk: Bytes) -> PyResult<()> {
        let tx_guard = self.tx.lock().unwrap_or_else(|e| e.into_inner());
        let tx = tx_guard.as_ref().ok_or_else(|| StreamEnd::Closed.into_pyerr())?;
        match tx.try_send(Ok(chunk)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(pyo3::exceptions::PyBlockingIOError::new_err(
                "stream buffer full (client is slow); wait_writable() returns when there is room",
            )),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(StreamEnd::ClientGone.into_pyerr()),
        }
    }

    /// Returns a wait that ends when a chunk would fit or the stream ends. It holds a sender, which
    /// keeps the response open, so `close()` wakes it to let go.
    fn room(&self) -> RoomWait {
        let closing = Arc::clone(self.closing.get_or_init(|| Arc::new(Notify::new())));
        let mut closed = Box::pin(closing.notified_owned());
        // Enabled before `tx` is read, so a close after the read still wakes this wait.
        closed.as_mut().enable();
        let tx = self.tx.lock().unwrap_or_else(|e| e.into_inner()).clone();
        Box::pin(async move {
            let Some(tx) = tx else {
                return Err(StreamEnd::Closed);
            };
            tokio::select! {
                biased;
                () = closed => Err(StreamEnd::Closed),
                permit = tx.reserve() => permit.map(drop).map_err(|_| StreamEnd::ClientGone),
            }
        })
    }

    /// Returns `Some` only the first time.
    pub(crate) fn take_rx(&self) -> Option<mpsc::Receiver<StreamItem>> {
        self.rx.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    use super::{sse_event, sse_lines, ResponseStream, RoomWait, StreamEnd, STREAM_CHANNEL_CAP};
    use bytes::Bytes;
    use pyo3::exceptions::{PyBlockingIOError, PyConnectionError};
    use pyo3::prelude::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};
    use tokio::sync::mpsc::error::TryRecvError;

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl WakeCount {
        fn wakes(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn poll(wait: &mut RoomWait, count: &Arc<WakeCount>) -> Poll<Result<(), StreamEnd>> {
        let waker = Waker::from(Arc::clone(count));
        wait.as_mut().poll(&mut Context::from_waker(&waker))
    }

    fn full_stream() -> (ResponseStream, tokio::sync::mpsc::Receiver<super::StreamItem>) {
        let stream = ResponseStream::new(None, 200, None).unwrap();
        let rx = stream.take_rx().unwrap();
        for _ in 0..STREAM_CHANNEL_CAP {
            stream.push(Bytes::from_static(b"x")).unwrap();
        }
        (stream, rx)
    }

    #[test]
    fn a_wait_with_one_free_slot_ends_at_its_first_poll() {
        let stream = ResponseStream::new(None, 200, None).unwrap();
        let _rx = stream.take_rx().unwrap();
        for _ in 0..STREAM_CHANNEL_CAP - 1 {
            stream.push(Bytes::from_static(b"x")).unwrap();
        }
        let count = Arc::new(WakeCount::default());
        assert_eq!(poll(&mut stream.room(), &count), Poll::Ready(Ok(())));
        assert_eq!(count.wakes(), 0);
    }

    /// Pins that a producer facing a slow client is woken by the client taking a chunk, once,
    /// and not before: no spin and no clock.
    #[test]
    fn a_wait_on_a_full_stream_is_woken_once_when_the_client_takes_a_chunk() {
        let (stream, mut rx) = full_stream();
        let count = Arc::new(WakeCount::default());
        let mut wait = stream.room();

        assert_eq!(poll(&mut wait, &count), Poll::Pending);
        assert_eq!(poll(&mut wait, &count), Poll::Pending);
        assert_eq!(count.wakes(), 0);

        rx.try_recv().unwrap();
        assert_eq!(count.wakes(), 1);
        assert_eq!(poll(&mut wait, &count), Poll::Ready(Ok(())));
        stream.push(Bytes::from_static(b"x")).unwrap();
    }

    #[test]
    fn a_wait_on_a_full_stream_wakes_with_the_client_gone_when_the_receiver_is_dropped() {
        let (stream, rx) = full_stream();
        let count = Arc::new(WakeCount::default());
        let mut wait = stream.room();
        assert_eq!(poll(&mut wait, &count), Poll::Pending);

        drop(rx);
        assert_eq!(count.wakes(), 1);
        assert_eq!(poll(&mut wait, &count), Poll::Ready(Err(StreamEnd::ClientGone)));
    }

    /// Pins that a close from another producer wakes a waiting one at once, and that once that
    /// wait is over no sender is left, so the response ends.
    #[test]
    fn a_close_during_a_wait_wakes_it_with_closed_and_ends_the_response() {
        let (stream, mut rx) = full_stream();
        let count = Arc::new(WakeCount::default());
        let mut wait = stream.room();
        assert_eq!(poll(&mut wait, &count), Poll::Pending);

        stream.close();
        assert_eq!(count.wakes(), 1);
        assert_eq!(poll(&mut wait, &count), Poll::Ready(Err(StreamEnd::Closed)));
        drop(wait);

        for _ in 0..STREAM_CHANNEL_CAP {
            rx.try_recv().unwrap();
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn a_wait_after_close_ends_at_its_first_poll_with_closed() {
        let (stream, _rx) = full_stream();
        stream.close();
        let count = Arc::new(WakeCount::default());
        assert_eq!(poll(&mut stream.room(), &count), Poll::Ready(Err(StreamEnd::Closed)));
        assert_eq!(count.wakes(), 0);
    }

    #[test]
    fn a_thread_blocked_on_a_full_stream_returns_once_the_client_takes_a_chunk() {
        let (stream, mut rx) = full_stream();
        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| mipyre_tpc::block_on(stream.room()));
            rx.blocking_recv().unwrap();
            assert_eq!(waiter.join().unwrap(), Ok(()));
        });
    }

    /// Pins the bound, because an unbounded buffer let a producer faster than its client run the
    /// server out of memory.
    #[test]
    fn a_full_stream_refuses_a_send_with_blocking_io_error_until_a_chunk_is_read() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let stream = ResponseStream::new(None, 200, None).unwrap();
            let mut rx = stream.take_rx().unwrap();
            for i in 0..STREAM_CHANNEL_CAP {
                stream
                    .send("x")
                    .unwrap_or_else(|e| panic!("send {i} of {STREAM_CHANNEL_CAP}: {e}"));
            }
            let full = stream.send("x").unwrap_err();
            assert!(full.is_instance_of::<PyBlockingIOError>(py), "{full}");

            assert!(rx.try_recv().is_ok());
            stream.send("x").unwrap();
            assert!(stream.send("x").unwrap_err().is_instance_of::<PyBlockingIOError>(py));
        });
    }

    fn lines(data: &str) -> Vec<&str> {
        sse_lines(data).collect()
    }

    #[test]
    fn every_line_ending_splits() {
        assert_eq!(lines("a\r\nb\nc\rd"), ["a", "b", "c", "d"]);
    }

    #[test]
    fn one_trailing_line_ending_ends_the_last_line() {
        assert_eq!(lines("a\n"), ["a"]);
        assert_eq!(lines("a\r\n"), ["a"]);
        assert_eq!(lines("a\n\n"), ["a", ""]);
        assert_eq!(lines("\n"), [""]);
    }

    #[test]
    fn empty_data_is_one_empty_line() {
        assert_eq!(lines(""), [""]);
    }

    #[test]
    fn an_event_with_empty_data_is_one_empty_data_line_and_without_data_has_none() {
        assert_eq!(sse_event(Some(""), None, None, None), "data: \n\n");
        assert_eq!(sse_event(None, None, None, None), "\n");
    }

    #[test]
    fn multiline_data_is_one_data_line_per_line() {
        assert_eq!(sse_event(Some("a\nb"), None, None, None), "data: a\ndata: b\n\n");
        assert_eq!(sse_event(Some("a"), None, None, None), "data: a\n\n");
    }

    #[test]
    fn crlf_in_data_splits_without_leaving_a_carriage_return() {
        assert_eq!(sse_event(Some("a\r\nb\r\n"), None, None, None), "data: a\ndata: b\n\n");
    }

    #[test]
    fn a_retry_only_event_has_no_data_line() {
        assert_eq!(sse_event(None, None, None, Some(3000)), "retry: 3000\n\n");
        assert_eq!(sse_event(None, None, None, Some(0)), "retry: 0\n\n");
    }

    #[test]
    fn every_field_is_written_id_event_retry_then_data() {
        assert_eq!(
            sse_event(Some("x"), Some("tick"), Some("7"), Some(10)),
            "id: 7\nevent: tick\nretry: 10\ndata: x\n\n"
        );
    }

    /// Hyper drops the body's receiver with a lost connection, and the handler must hear that the
    /// client left, not that the stream was closed.
    #[test]
    fn a_send_after_the_client_disconnected_raises_connection_error_saying_so() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let stream = ResponseStream::new(None, 200, None).unwrap();
            let rx = stream.take_rx().expect("the body's receiver, once");
            stream.send("data: 1\n\n").expect("the client is there");

            drop(rx);
            let err = stream.send("data: 2\n\n").expect_err("the client is gone");
            assert!(err.is_instance_of::<PyConnectionError>(py), "{err}");
            assert_eq!(err.value(py).to_string(), "client disconnected");
            let err = stream
                .send_event(Some("3"), None, None, None)
                .expect_err("and stays gone");
            assert_eq!(err.value(py).to_string(), "client disconnected");

            stream.close();
            let err = stream.send("data: 4\n\n").expect_err("closed");
            assert!(err.is_instance_of::<PyConnectionError>(py), "{err}");
            assert_eq!(err.value(py).to_string(), "stream was explicitly closed");
        });
    }
}
