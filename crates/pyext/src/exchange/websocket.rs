//! Gives a Python handler its side of a WebSocket connection and the two queues to the connection.
//!
//! Every queued message holds its bytes from the direction's [`ByteBudget`], so each direction
//! is bounded in memory although the channels are unbounded in count.

use std::sync::Arc;

use crate::sync::Mutex;
use pyo3::exceptions::{PyBlockingIOError, PyConnectionError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use mipyre_http::plan::limits::{MessageByteLimit, MESSAGE_OVERHEAD_BYTES};

use crate::exchange::request::Request;

/// The budget bounds the bytes queued in one direction of one connection; a message holds its
/// share until it leaves the queue.
///
/// The budget fits one maximum-size message, so every allowed message can be queued once the
/// queue drains. Nothing closes the private semaphore, so acquiring only waits or finds the
/// budget spent.
#[derive(Clone)]
pub(crate) struct ByteBudget(Arc<Semaphore>);

impl ByteBudget {
    fn new(max_message_bytes: MessageByteLimit) -> Self {
        let permits = max_message_bytes.budget_permits();
        Self(Arc::new(Semaphore::new(permits as usize)))
    }

    /// Cannot overflow, because tungstenite enforces `max_message_bytes` on input and `send`
    /// checks it on output.
    fn cost(len: usize) -> u32 {
        len as u32 + MESSAGE_OVERHEAD_BYTES
    }

    /// Waits until the queue has drained enough to hold `len`.
    #[expect(
        clippy::expect_used,
        reason = "closing is acquire's only error, and nothing closes a ByteBudget's private semaphore (SEMAPHORE_NEVER_CLOSED)"
    )]
    pub(crate) async fn reserve(&self, len: usize) -> OwnedSemaphorePermit {
        self.0
            .clone()
            .acquire_many_owned(Self::cost(len))
            .await
            .expect(SEMAPHORE_NEVER_CLOSED)
    }

    /// Returns `None` while the budget is spent.
    fn try_reserve(&self, len: usize) -> Option<OwnedSemaphorePermit> {
        match self.0.clone().try_acquire_many_owned(Self::cost(len)) {
            Ok(permit) => Some(permit),
            Err(TryAcquireError::NoPermits) => None,
            Err(TryAcquireError::Closed) => unreachable!("{SEMAPHORE_NEVER_CLOSED}"),
        }
    }
}

/// Acquiring cannot fail because no code in this module, the only one that can reach the
/// semaphore, closes it.
const SEMAPHORE_NEVER_CLOSED: &str =
    "a ByteBudget's semaphore is private to it and never closed: closing is acquire's only error";

pub(crate) enum WebSocketMessage {
    Text(String),
    Binary(Vec<u8>),
}

impl WebSocketMessage {
    fn kind(&self) -> &'static str {
        match self {
            Self::Text(_) => "text",
            Self::Binary(_) => "binary",
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Text(s) => s.len(),
            Self::Binary(b) => b.len(),
        }
    }

    fn into_text(self) -> Result<String, Self> {
        match self {
            Self::Text(s) => Ok(s),
            other => Err(other),
        }
    }

    fn into_binary(self) -> Result<Vec<u8>, Self> {
        match self {
            Self::Binary(b) => Ok(b),
            other => Err(other),
        }
    }
}

pub(crate) struct QueueEntry {
    pub(crate) msg: WebSocketMessage,
    pub(crate) _budget: OwnedSemaphorePermit,
}

/// The message of the other kind stays queued, and the error tells the handler how to read it.
struct KindMismatch {
    method: &'static str,
    got: &'static str,
    len: usize,
}

impl From<KindMismatch> for PyErr {
    fn from(m: KindMismatch) -> Self {
        PyTypeError::new_err(format!(
            "{}() found a {} message ({} bytes) next; it is still queued: read it with \
             recv_message(), which returns str or bytes",
            m.method, m.got, m.len
        ))
    }
}

struct Inbox {
    rx: UnboundedReceiver<QueueEntry>,
    /// A message a typed receive refused, returned first by the next receive.
    held: Option<WebSocketMessage>,
}

impl Inbox {
    /// Returns `None` once the peer has closed.
    fn next(&mut self, method: &'static str) -> Result<Option<WebSocketMessage>, KindMismatch> {
        self.next_matching(method, Ok)
    }

    /// Returns `None` once the peer has closed, and leaves a binary message queued.
    fn next_text(&mut self, method: &'static str) -> Result<Option<String>, KindMismatch> {
        self.next_matching(method, WebSocketMessage::into_text)
    }

    /// Returns `None` once the peer has closed, and leaves a text message queued.
    fn next_bytes(&mut self, method: &'static str) -> Result<Option<Vec<u8>>, KindMismatch> {
        self.next_matching(method, WebSocketMessage::into_binary)
    }

    /// Leaves a message that `extract` hands back at the head of the queue.
    fn next_matching<T>(
        &mut self,
        method: &'static str,
        extract: impl FnOnce(WebSocketMessage) -> Result<T, WebSocketMessage>,
    ) -> Result<Option<T>, KindMismatch> {
        let Some(msg) = self
            .held
            .take()
            .or_else(|| self.rx.blocking_recv().map(|q| q.msg))
        else {
            return Ok(None);
        };
        extract(msg).map(Some).map_err(|msg| {
            let mismatch = KindMismatch {
                method,
                got: msg.kind(),
                len: msg.len(),
            };
            self.held = Some(msg);
            mismatch
        })
    }
}

pub(crate) struct QueueSender {
    pub(crate) tx: UnboundedSender<QueueEntry>,
    pub(crate) budget: ByteBudget,
}

struct Outbox {
    queue: QueueSender,
    max_message_bytes: MessageByteLimit,
}

impl Outbox {
    fn push(&self, msg: WebSocketMessage) -> PyResult<()> {
        if self.max_message_bytes.is_exceeded_by(msg.len()) {
            return Err(PyValueError::new_err(format!(
                "{} message of {} bytes exceeds max_websocket_message_size ({} bytes)",
                msg.kind(),
                msg.len(),
                self.max_message_bytes
            )));
        }
        let budget = self.queue.budget.try_reserve(msg.len()).ok_or_else(|| {
            PyBlockingIOError::new_err(
                "WebSocket send buffer full (client is slow); retry after a brief pause",
            )
        })?;
        self.queue
            .tx
            .send(QueueEntry {
                msg,
                _budget: budget,
            })
            .map_err(|_| PyConnectionError::new_err("WebSocket closed"))
    }
}

pub(crate) struct ConnectionEnds {
    pub(crate) incoming: QueueSender,
    pub(crate) outgoing: UnboundedReceiver<QueueEntry>,
}

pub(crate) struct HandlerEnds {
    inbox: Inbox,
    outbox: Outbox,
}

pub(crate) fn connection_channels(
    max_message_bytes: MessageByteLimit,
) -> (ConnectionEnds, HandlerEnds) {
    let (incoming_tx, incoming_rx) = unbounded_channel::<QueueEntry>();
    let (outgoing_tx, outgoing_rx) = unbounded_channel::<QueueEntry>();
    (
        ConnectionEnds {
            incoming: QueueSender {
                tx: incoming_tx,
                budget: ByteBudget::new(max_message_bytes),
            },
            outgoing: outgoing_rx,
        },
        HandlerEnds {
            inbox: Inbox {
                rx: incoming_rx,
                held: None,
            },
            outbox: Outbox {
                queue: QueueSender {
                    tx: outgoing_tx,
                    budget: ByteBudget::new(max_message_bytes),
                },
                max_message_bytes,
            },
        },
    )
}

#[pyclass(name = "WebSocket", module = "mipyre.engine")]
pub(crate) struct WebSocket {
    inbox: Mutex<Inbox>,
    outbox: Mutex<Option<Outbox>>,
    request: Py<Request>,
}

#[pymethods]
impl WebSocket {
    /// Returns the upgrade request, the same object the `before_request` hooks saw.
    #[getter]
    fn request(&self, py: Python<'_>) -> Py<Request> {
        self.request.clone_ref(py)
    }

    /// Receives the next message as `str` or `bytes`, or `None` once the connection is closed,
    /// without holding the GIL while it waits.
    fn recv_message<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let msg = py.detach(|| self.inbox.lock().next("recv_message"))?;
        Ok(msg.map(|msg| match msg {
            WebSocketMessage::Text(s) => PyString::new(py, &s).into_any(),
            WebSocketMessage::Binary(b) => PyBytes::new(py, &b).into_any(),
        }))
    }

    /// Receives the next text message, or `None` once the connection is closed; raises
    /// `TypeError` if the next message is binary, leaving it queued.
    fn recv(&self, py: Python<'_>) -> PyResult<Option<String>> {
        Ok(py.detach(|| self.inbox.lock().next_text("recv"))?)
    }

    /// Receives the next binary message, or `None` once the connection is closed; raises
    /// `TypeError` if the next message is text, leaving it queued.
    fn recv_bytes<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let data = py.detach(|| self.inbox.lock().next_bytes("recv_bytes"))?;
        Ok(data.map(|b| PyBytes::new(py, &b)))
    }

    /// Sends a text message without blocking; raises `ValueError` past
    /// `max_websocket_message_size`, `BlockingIOError` when a slow client has let the buffer
    /// fill, and `ConnectionError` once the connection is closed.
    fn send(&self, msg: &str) -> PyResult<()> {
        self.push(WebSocketMessage::Text(msg.to_string()))
    }

    /// Sends a binary message, raising as `send` does.
    fn send_bytes(&self, data: Vec<u8>) -> PyResult<()> {
        self.push(WebSocketMessage::Binary(data))
    }

    /// Closes the connection.
    fn close(&self) {
        *self.outbox.lock() = None;
    }
}

impl WebSocket {
    pub(crate) fn new(ends: HandlerEnds, request: Py<Request>) -> Self {
        WebSocket {
            inbox: Mutex::new(ends.inbox),
            outbox: Mutex::new(Some(ends.outbox)),
            request,
        }
    }

    fn push(&self, msg: WebSocketMessage) -> PyResult<()> {
        self.outbox
            .lock()
            .as_ref()
            .ok_or_else(|| PyConnectionError::new_err("WebSocket closed"))?
            .push(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn byte_budget_counts_bytes_not_messages() {
        let budget = ByteBudget::new(MessageByteLimit::new(1000).unwrap());
        let big = budget.try_reserve(900).expect("fits");
        // With 964 of 1064 used, a 100-byte message costs 164 and no longer fits, but an empty one does.
        assert!(budget.try_reserve(100).is_none());
        let small = budget.try_reserve(0).expect("fits");
        drop(big);
        assert!(budget.try_reserve(900).is_some());
        drop(small);
    }

    #[test]
    fn a_max_size_message_always_fits_an_empty_budget() {
        let budget = ByteBudget::new(MessageByteLimit::new(1000).unwrap());
        assert!(budget.try_reserve(1000).is_some());
    }

    #[test]
    fn typed_receive_keeps_the_other_kind_queued() {
        let (tx, rx) = unbounded_channel();
        let budget = ByteBudget::new(MessageByteLimit::new(1000).unwrap());
        for msg in [
            WebSocketMessage::Binary(vec![1, 2, 3]),
            WebSocketMessage::Text("hi".into()),
        ] {
            let permit = budget.try_reserve(msg.len()).unwrap();
            tx.send(QueueEntry {
                msg,
                _budget: permit,
            })
            .unwrap();
        }
        let mut inbox = Inbox { rx, held: None };
        let mismatch = inbox.next_text("recv").err().unwrap();
        assert_eq!((mismatch.got, mismatch.len), ("binary", 3));
        assert_eq!(
            inbox.next_bytes("recv_bytes").ok().flatten(),
            Some(vec![1, 2, 3])
        );
        assert_eq!(
            inbox.next_text("recv").ok().flatten(),
            Some("hi".to_string())
        );
        drop(tx);
        assert!(inbox.next_text("recv").ok().unwrap().is_none());
    }
}
