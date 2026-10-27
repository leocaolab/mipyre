//! Gives a Python handler its side of a WebSocket connection and the two queues to the connection.
//!
//! Every queued message holds its bytes from the direction's [`ByteBudget`], so each direction
//! is bounded in memory although the channels are unbounded in count. A handler that finds the
//! budget spent waits for the connection to send what is queued; nothing here waits on a clock.

use std::future::Future;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use crate::sync::Mutex;
use pyo3::exceptions::{PyConnectionError, PyRuntimeError, PyStopAsyncIteration, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use mipyre_http::plan::limits::{MessageByteLimit, MESSAGE_OVERHEAD_BYTES};

use crate::exchange::loop_wait::LoopWaits;
use crate::exchange::request::Request;

/// The budget bounds the bytes queued in one direction of one connection; a message holds its
/// share until it leaves the queue.
///
/// The budget fits one maximum-size message, so every allowed message can be queued once the
/// queue drains. Nothing closes the private semaphore, so acquiring only waits or finds the
/// budget spent.
#[derive(Clone)]
pub struct ByteBudget(Arc<Semaphore>);

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
    pub async fn reserve(&self, len: usize) -> OwnedSemaphorePermit {
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

pub enum WebSocketMessage {
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

    #[expect(
        clippy::len_without_is_empty,
        reason = "a message's length is what it costs the connection's byte budget; no caller asks whether it is empty"
    )]
    pub fn len(&self) -> usize {
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

pub struct QueueEntry {
    pub msg: WebSocketMessage,
    pub _budget: OwnedSemaphorePermit,
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
    /// Returns `None` once the peer has closed, and leaves a message that `extract` hands back at
    /// the head of the queue.
    fn poll_matching<T>(
        &mut self,
        cx: &mut Context<'_>,
        method: &'static str,
        extract: fn(WebSocketMessage) -> Result<T, WebSocketMessage>,
    ) -> Poll<Result<Option<T>, KindMismatch>> {
        let next = match self.held.take() {
            Some(msg) => Some(msg),
            None => ready!(self.rx.poll_recv(cx)).map(|q| q.msg),
        };
        let Some(msg) = next else {
            return Poll::Ready(Ok(None));
        };
        Poll::Ready(extract(msg).map(Some).map_err(|msg| {
            let mismatch = KindMismatch {
                method,
                got: msg.kind(),
                len: msg.len(),
            };
            self.held = Some(msg);
            mismatch
        }))
    }
}

pub struct QueueSender {
    pub tx: UnboundedSender<QueueEntry>,
    pub budget: ByteBudget,
}

struct Outbox {
    queue: QueueSender,
    max_message_bytes: MessageByteLimit,
}

/// What queuing a message without waiting did.
enum PushOutcome {
    Sent,
    /// The budget is spent; the message waits for room.
    Full(WebSocketMessage, ByteBudget),
}

impl Outbox {
    fn try_push(&self, msg: WebSocketMessage) -> PyResult<PushOutcome> {
        if self.max_message_bytes.is_exceeded_by(msg.len()) {
            return Err(PyValueError::new_err(format!(
                "{} message of {} bytes exceeds max_websocket_message_size ({} bytes)",
                msg.kind(),
                msg.len(),
                self.max_message_bytes
            )));
        }
        let Some(budget) = self.queue.budget.try_reserve(msg.len()) else {
            return Ok(PushOutcome::Full(msg, self.queue.budget.clone()));
        };
        self.queue
            .tx
            .send(QueueEntry { msg, _budget: budget })
            .map_err(|_| closed())?;
        Ok(PushOutcome::Sent)
    }
}

/// Waits until `msg` fits the budget, then queues it. It holds the budget and not the queue, so it
/// keeps no connection open; a connection that ends drops what was queued, which ends the wait,
/// and the message is then refused as closed.
async fn push_when_room(outbox: Arc<Mutex<Option<Outbox>>>, budget: ByteBudget, msg: WebSocketMessage) -> PyResult<()> {
    let permit = budget.reserve(msg.len()).await;
    let outbox = outbox.lock();
    let outbox = outbox.as_ref().ok_or_else(closed)?;
    outbox
        .queue
        .tx
        .send(QueueEntry { msg, _budget: permit })
        .map_err(|_| closed())
}

fn closed() -> PyErr {
    PyConnectionError::new_err("WebSocket closed")
}

pub struct ConnectionEnds {
    pub incoming: QueueSender,
    pub outgoing: UnboundedReceiver<QueueEntry>,
}

pub struct HandlerEnds {
    inbox: Inbox,
    outbox: Outbox,
}

pub fn connection_channels(max_message_bytes: MessageByteLimit) -> (ConnectionEnds, HandlerEnds) {
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

/// One connection as its handler sees it. Every receive and send is awaited on the loop of the
/// engine the connection was handed to; one task receives at a time.
#[pyclass(name = "WebSocket", module = "mipyre.engine", frozen)]
pub struct WebSocket {
    inbox: Arc<Mutex<Inbox>>,
    /// Is the receive being awaited, if any, so a second task's receive is refused meanwhile.
    receiving: Mutex<Option<u64>>,
    outbox: Arc<Mutex<Option<Outbox>>>,
    request: Py<Request>,
    waits: Py<LoopWaits>,
}

#[pymethods]
impl WebSocket {
    /// Returns the upgrade request, the same object the `before_request` hooks saw.
    #[getter]
    fn request(&self, py: Python<'_>) -> Py<Request> {
        self.request.clone_ref(py)
    }

    /// Returns an awaitable of the next message as `str` or `bytes`, or `None` once the
    /// connection is closed.
    fn recv_message<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.receive(py, "recv_message", Ok, message_or_none)
    }

    /// Returns an awaitable of the next text message, or `None` once the connection is closed; it
    /// raises `TypeError` if the next message is binary, leaving it queued.
    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.receive(py, "recv", WebSocketMessage::into_text, |py, text: Option<String>| {
            Ok(text.into_pyobject(py)?.unbind())
        })
    }

    /// Returns an awaitable of the next binary message, or `None` once the connection is closed;
    /// it raises `TypeError` if the next message is text, leaving it queued.
    fn recv_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.receive(py, "recv_bytes", WebSocketMessage::into_binary, |py, data: Option<Vec<u8>>| {
            Ok(data.map_or_else(|| py.None(), |b| PyBytes::new(py, &b).into_any().unbind()))
        })
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Returns an awaitable of the next message, which raises `StopAsyncIteration` once the
    /// connection is closed.
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.receive(py, "__anext__", Ok, |py, msg| match msg {
            Some(msg) => message_or_none(py, Some(msg)),
            None => Err(PyStopAsyncIteration::new_err(())),
        })
    }

    /// Returns an awaitable that queues a text message once a slow client has left room for it;
    /// raises `ValueError` past `max_websocket_message_size`, and `ConnectionError` once the
    /// connection is closed.
    fn send<'py>(&self, py: Python<'py>, msg: &str) -> PyResult<Bound<'py, PyAny>> {
        self.push(py, WebSocketMessage::Text(msg.to_string()))
    }

    /// Returns an awaitable that queues a binary message, raising as `send` does.
    fn send_bytes<'py>(&self, py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        self.push(py, WebSocketMessage::Binary(data))
    }

    /// Closes the connection once what is queued has been sent; a send still waiting for room
    /// raises `ConnectionError` when it wakes, and any later send at once.
    fn close(&self) {
        drop(self.outbox.lock().take());
    }
}

impl WebSocket {
    pub(crate) fn new(ends: HandlerEnds, request: Py<Request>, waits: Py<LoopWaits>) -> Self {
        WebSocket {
            inbox: Arc::new(Mutex::new(ends.inbox)),
            receiving: Mutex::new(None),
            outbox: Arc::new(Mutex::new(Some(ends.outbox))),
            request,
            waits,
        }
    }

    fn receive<'py, T: 'static>(
        &self,
        py: Python<'py>,
        method: &'static str,
        extract: fn(WebSocketMessage) -> Result<T, WebSocketMessage>,
        into_py: fn(Python<'_>, Option<T>) -> PyResult<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let waits = self.waits.get();
        let receiving = *self.receiving.lock();
        if let Some(wait) = receiving {
            if waits.is_awaited(py, wait)? {
                return Err(PyRuntimeError::new_err(format!(
                    "{method}() while another task awaits a receive on this WebSocket; receive from one task"
                )));
            }
        }
        let inbox = Arc::clone(&self.inbox);
        let (done, wait) = waits.start(
            py,
            Box::new(move |py, cx| {
                let got = ready!(inbox.lock().poll_matching(cx, method, extract));
                Poll::Ready(got.map_err(PyErr::from).and_then(|msg| into_py(py, msg)))
            }),
        )?;
        *self.receiving.lock() = wait;
        Ok(done)
    }

    fn push<'py>(&self, py: Python<'py>, msg: WebSocketMessage) -> PyResult<Bound<'py, PyAny>> {
        let queued = self.outbox.lock().as_ref().ok_or_else(closed)?.try_push(msg)?;
        let waits = self.waits.get();
        let PushOutcome::Full(msg, budget) = queued else {
            return Ok(waits.sent(py));
        };
        let mut wait = Box::pin(push_when_room(Arc::clone(&self.outbox), budget, msg));
        let (done, _) = waits.start(
            py,
            Box::new(move |py, cx| wait.as_mut().poll(cx).map(|pushed| pushed.map(|()| py.None()))),
        )?;
        Ok(done)
    }
}

fn message_or_none(py: Python<'_>, msg: Option<WebSocketMessage>) -> PyResult<Py<PyAny>> {
    Ok(match msg {
        Some(WebSocketMessage::Text(s)) => PyString::new(py, &s).into_any().unbind(),
        Some(WebSocketMessage::Binary(b)) => PyBytes::new(py, &b).into_any().unbind(),
        None => py.None(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

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

    /// Polls a send, with whether it queued its message or found the connection closed.
    fn poll(wait: std::pin::Pin<&mut impl Future<Output = PyResult<()>>>, count: &Arc<WakeCount>) -> Poll<bool> {
        let waker = Waker::from(Arc::clone(count));
        wait.poll(&mut Context::from_waker(&waker)).map(|sent| sent.is_ok())
    }

    const LIMIT: u32 = 1000;

    fn channels() -> (ConnectionEnds, Arc<Mutex<Option<Outbox>>>) {
        let (connection, handler) = connection_channels(MessageByteLimit::new(i64::from(LIMIT)).unwrap());
        (connection, Arc::new(Mutex::new(Some(handler.outbox))))
    }

    fn text(len: usize) -> WebSocketMessage {
        WebSocketMessage::Text("x".repeat(len))
    }

    fn try_push(outbox: &Arc<Mutex<Option<Outbox>>>, msg: WebSocketMessage) -> PushOutcome {
        outbox.lock().as_ref().unwrap().try_push(msg).unwrap()
    }

    fn queue(outbox: &Arc<Mutex<Option<Outbox>>>, msg: WebSocketMessage) {
        assert!(
            matches!(try_push(outbox, msg), PushOutcome::Sent),
            "the message waits for room"
        );
    }

    fn send_waiting(outbox: &Arc<Mutex<Option<Outbox>>>, msg: WebSocketMessage) -> impl Future<Output = PyResult<()>> {
        match try_push(outbox, msg) {
            PushOutcome::Full(msg, budget) => push_when_room(Arc::clone(outbox), budget, msg),
            PushOutcome::Sent => panic!("the message fit at once"),
        }
    }

    #[test]
    fn a_send_with_room_queues_at_once() {
        let (mut connection, outbox) = channels();
        queue(&outbox, text(LIMIT as usize));
        assert_eq!(connection.outgoing.try_recv().unwrap().msg.len(), LIMIT as usize);
    }

    /// Pins that a handler facing a slow client is woken once, when the last queued message
    /// leaves, and not while one is still queued: no spin and no clock.
    #[test]
    fn a_send_waiting_for_room_is_woken_once_when_the_last_queued_message_leaves_and_not_before() {
        let (mut connection, outbox) = channels();
        queue(&outbox, text(10));
        queue(&outbox, text(10));
        let count = Arc::new(WakeCount::default());
        let mut wait = pin!(send_waiting(&outbox, text(LIMIT as usize)));
        assert_eq!(poll(wait.as_mut(), &count), Poll::Pending);

        drop(connection.outgoing.try_recv().unwrap());
        assert_eq!(count.wakes(), 0);
        assert_eq!(poll(wait.as_mut(), &count), Poll::Pending);

        drop(connection.outgoing.try_recv().unwrap());
        assert_eq!(count.wakes(), 1);
        assert_eq!(poll(wait.as_mut(), &count), Poll::Ready(true));
        assert_eq!(connection.outgoing.try_recv().unwrap().msg.len(), LIMIT as usize);
    }

    #[test]
    fn a_send_waiting_for_room_fails_once_the_connection_drops_the_queue() {
        let (connection, outbox) = channels();
        queue(&outbox, text(LIMIT as usize));
        let count = Arc::new(WakeCount::default());
        let mut wait = pin!(send_waiting(&outbox, text(LIMIT as usize)));
        assert_eq!(poll(wait.as_mut(), &count), Poll::Pending);

        drop(connection);
        assert_eq!(count.wakes(), 1);
        assert_eq!(poll(wait.as_mut(), &count), Poll::Ready(false));
    }

    #[test]
    fn a_send_waiting_for_room_fails_once_the_handler_closed() {
        let (mut connection, outbox) = channels();
        queue(&outbox, text(LIMIT as usize));
        let count = Arc::new(WakeCount::default());
        let mut wait = pin!(send_waiting(&outbox, text(LIMIT as usize)));
        assert_eq!(poll(wait.as_mut(), &count), Poll::Pending);

        drop(outbox.lock().take());
        drop(connection.outgoing.try_recv().unwrap());
        assert_eq!(poll(wait.as_mut(), &count), Poll::Ready(false));
    }

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

    fn next<T>(
        inbox: &mut Inbox,
        method: &'static str,
        extract: fn(WebSocketMessage) -> Result<T, WebSocketMessage>,
    ) -> Result<Option<T>, KindMismatch> {
        match inbox.poll_matching(&mut Context::from_waker(Waker::noop()), method, extract) {
            Poll::Ready(got) => got,
            Poll::Pending => panic!("{method}() found nothing queued"),
        }
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
            tx.send(QueueEntry { msg, _budget: permit }).unwrap();
        }
        let mut inbox = Inbox { rx, held: None };
        let mismatch = next(&mut inbox, "recv", WebSocketMessage::into_text).err().unwrap();
        assert_eq!((mismatch.got, mismatch.len), ("binary", 3));
        assert_eq!(
            next(&mut inbox, "recv_bytes", WebSocketMessage::into_binary)
                .ok()
                .flatten(),
            Some(vec![1, 2, 3])
        );
        assert_eq!(
            next(&mut inbox, "recv", WebSocketMessage::into_text).ok().flatten(),
            Some("hi".to_string())
        );
        drop(tx);
        assert!(next(&mut inbox, "recv", WebSocketMessage::into_text)
            .ok()
            .unwrap()
            .is_none());
    }
}
