//! Lets a handler stream a response body, such as Server-Sent Events, through a `Stream`.
//!
//! `close()` ends the response at once, whatever still references the stream, because waiting
//! for Python's GC to drop it would hold the client's connection open.

use bytes::Bytes;
use hyper::header::{HeaderMap, HeaderValue};
use hyper::StatusCode;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::mpsc;

use crate::exchange::response::{
    header_text, header_value, http_status, response_headers_from_py, response_headers_to_py, sent_content_type,
};

/// This many chunks are buffered for a slow client before a fast producer gets `BlockingIOError`
/// and backs off, instead of growing the buffer without bound.
const STREAM_CHANNEL_CAP: usize = 1024;

type StreamItem = Result<Bytes, std::convert::Infallible>;

#[pyclass(frozen, name = "Stream", module = "mipyre.engine")]
pub(crate) struct ResponseStream {
    /// The sender is `None` once `close()` dropped it, which ends the response.
    tx: crate::sync::poisoning::Mutex<Option<mpsc::Sender<StreamItem>>>,
    rx: crate::sync::poisoning::Mutex<Option<mpsc::Receiver<StreamItem>>>,
    pub(crate) head: StreamHead,
}

/// The head is validated when the `Stream` is made.
#[derive(Clone)]
pub(crate) struct StreamHead {
    /// A `Content-Type` in `headers` overrides this ([`Self::sent_content_type`]).
    pub(crate) content_type: HeaderValue,
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
}

impl StreamHead {
    /// Returns a `Content-Type` from `headers` if there is one, else `content_type`.
    pub(crate) fn sent_content_type(&self) -> &HeaderValue {
        sent_content_type(&self.headers, None).unwrap_or(&self.content_type)
    }
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
    /// the buffer fill, and the caller backs off before retrying.
    fn send(&self, data: &str) -> PyResult<()> {
        self.push(Bytes::copy_from_slice(data.as_bytes()))
    }

    /// Sends an SSE event: `id:` and `event:` lines if given, one `data:` line per line of
    /// `data`, then a blank line.
    #[pyo3(signature = (data, event=None, id=None))]
    fn send_event(&self, data: &str, event: Option<&str>, id: Option<&str>) -> PyResult<()> {
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
        let mut msg = String::with_capacity(data.len() + 64);
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
        for line in sse_lines(data) {
            msg.push_str("data: ");
            msg.push_str(line);
            msg.push('\n');
        }
        msg.push('\n');
        self.push(Bytes::from(msg))
    }

    /// Ends the response now; a later `send` raises `ConnectionError`.
    fn close(&self) {
        drop(self.tx.lock().unwrap_or_else(|e| e.into_inner()).take());
    }
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
        let tx = tx_guard
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyConnectionError::new_err("stream was explicitly closed"))?;
        match tx.try_send(Ok(chunk)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(pyo3::exceptions::PyBlockingIOError::new_err(
                "stream buffer full (client is slow); retry after a brief pause",
            )),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(pyo3::exceptions::PyConnectionError::new_err("client disconnected"))
            }
        }
    }

    /// Returns `Some` only the first time.
    pub(crate) fn take_rx(&self) -> Option<mpsc::Receiver<StreamItem>> {
        self.rx.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

#[cfg(test)]
mod tests {
    use super::{sse_lines, ResponseStream, STREAM_CHANNEL_CAP};
    use pyo3::exceptions::{PyBlockingIOError, PyConnectionError};
    use pyo3::prelude::*;

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
            let err = stream.send_event("3", None, None).expect_err("and stays gone");
            assert_eq!(err.value(py).to_string(), "client disconnected");

            stream.close();
            let err = stream.send("data: 4\n\n").expect_err("closed");
            assert!(err.is_instance_of::<PyConnectionError>(py), "{err}");
            assert_eq!(err.value(py).to_string(), "stream was explicitly closed");
        });
    }
}
