//! Carries WebSocket messages between a connection and the Python that serves it: the connection's
//! outgoing queue ([`Outbox`]), the process's topics, and the `ws` object a handler is given.
//!
//! The outbox holds what a handler or a publish queued until the connection takes it; it bounds
//! the bytes queued to one maximum-size message, and a send that does not fit closes the
//! connection (1008) instead of waiting, because a handler runs on the connection's own thread
//! and must not block it. It does not know the socket, when bytes reach it, or which handler ran.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use pyo3::{PyTraverseError, PyVisit};
use tungstenite::Utf8Bytes;

use mipyre_http::plan::limits::{MessageByteLimit, MESSAGE_OVERHEAD_BYTES};

use crate::exchange::request::Request;
use crate::sync::Mutex;

/// One WebSocket message as a handler sends or receives it: a text one is `str` in Python, a
/// binary one `bytes`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WsMessage {
    Text(Utf8Bytes),
    Binary(Bytes),
}

impl WsMessage {
    #[expect(
        clippy::len_without_is_empty,
        reason = "a message's length is what it costs the outbox's budget; no caller asks whether it is empty"
    )]
    pub fn len(&self) -> usize {
        match self {
            WsMessage::Text(text) => text.len(),
            WsMessage::Binary(data) => data.len(),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            WsMessage::Text(_) => "text",
            WsMessage::Binary(_) => "binary",
        }
    }

    /// Copies a `str` or `bytes` out of Python.
    ///
    /// # Errors
    ///
    /// Raises `TypeError` for any other value.
    pub(crate) fn from_py(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(text) = value.cast::<PyString>() {
            return Ok(WsMessage::Text(Utf8Bytes::from(text.to_str()?)));
        }
        if let Ok(data) = value.cast::<PyBytes>() {
            return Ok(WsMessage::Binary(Bytes::copy_from_slice(data.as_bytes())));
        }
        Err(PyTypeError::new_err(format!(
            "a WebSocket message is str or bytes, not {}",
            value.get_type().name()?
        )))
    }

    /// Copies the message into Python as `str` or `bytes`.
    pub(crate) fn to_py<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        match self {
            WsMessage::Text(text) => PyString::new(py, text.as_str()).into_any(),
            WsMessage::Binary(data) => PyBytes::new(py, data).into_any(),
        }
    }
}

/// The status and reason of a WebSocket close (RFC 6455 §7.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WsClose {
    pub code: u16,
    pub reason: String,
}

impl WsClose {
    pub const GOING_AWAY: u16 = 1001;
    pub const NO_STATUS: u16 = 1005;
    pub const ABNORMAL: u16 = 1006;
    pub const POLICY: u16 = 1008;
    pub const TOO_BIG: u16 = 1009;
    pub const INTERNAL_ERROR: u16 = 1011;

    pub fn new(code: u16, reason: impl Into<String>) -> Self {
        WsClose {
            code,
            reason: reason.into(),
        }
    }
}

/// A close frame's reason must leave its 125-byte payload room for the 2-byte code.
const MAX_CLOSE_REASON_BYTES: usize = 123;

/// Returns whether an endpoint may send `code` in a close frame: 1005, 1006 and 1015 only report
/// what happened and never go on the wire, and below 1000 or from 1016 to 2999 nothing is defined.
fn closable(code: u16) -> bool {
    matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999)
}

/// Returns whether a message of `len` bytes fits a budget of `budget` with `used` taken.
fn fits(used: u32, budget: u32, len: usize) -> bool {
    u64::from(used) + len as u64 + u64::from(MESSAGE_OVERHEAD_BYTES) <= u64::from(budget)
}

/// What the connection writes next, besides the messages taken.
#[derive(Debug, PartialEq, Eq)]
pub enum Taken {
    Messages,
    /// The messages go first, then the close frame; the outbox takes nothing more.
    MessagesThenClose(WsClose),
}

#[derive(Debug)]
enum OutboxState {
    Open {
        queued: VecDeque<WsMessage>,
        used: u32,
    },
    Closing {
        queued: VecDeque<WsMessage>,
        close: WsClose,
    },
    Done,
}

impl OutboxState {
    /// Queues `msg` when it fits, and otherwise drops it and everything queued, owing a 1008;
    /// returns whether it was queued.
    fn send(&mut self, budget: u32, msg: WsMessage) -> bool {
        let OutboxState::Open { queued, used } = self else {
            return false;
        };
        if fits(*used, budget, msg.len()) {
            *used += msg.len() as u32 + MESSAGE_OVERHEAD_BYTES;
            queued.push_back(msg);
            true
        } else {
            *self = OutboxState::Closing {
                queued: VecDeque::new(),
                close: WsClose::new(WsClose::POLICY, "the client reads too slowly"),
            };
            false
        }
    }

    /// Owes `close` after what is queued; a close already owed is kept.
    fn close(&mut self, close: WsClose) {
        if let OutboxState::Open { queued, .. } = self {
            *self = OutboxState::Closing {
                queued: std::mem::take(queued),
                close,
            };
        }
    }

    /// Swaps what is queued into the empty `into`, so both queues keep their capacity.
    fn take(&mut self, into: &mut VecDeque<WsMessage>) -> Option<Taken> {
        if let OutboxState::Open { queued, used } = self {
            if queued.is_empty() {
                return None;
            }
            *used = 0;
            std::mem::swap(queued, into);
            return Some(Taken::Messages);
        }
        match std::mem::replace(self, OutboxState::Done) {
            OutboxState::Closing { mut queued, close } => {
                std::mem::swap(&mut queued, into);
                Some(Taken::MessagesThenClose(close))
            }
            OutboxState::Open { .. } | OutboxState::Done => None,
        }
    }
}

/// What the outbox holds behind its lock: the messages and close not yet taken, and the
/// connection to wake when there are some.
#[derive(Debug)]
struct Backlog {
    state: OutboxState,
    /// Is the connection's waker while it waits for something to write.
    waiting: Option<Waker>,
}

/// One connection's outgoing queue, shared by its `ws` object, every publish that reaches it, and
/// the connection, which alone takes from it.
#[derive(Debug)]
pub struct Outbox {
    backlog: Mutex<Backlog>,
    max_message_bytes: MessageByteLimit,
}

impl Outbox {
    pub fn new(max_message_bytes: MessageByteLimit) -> Arc<Self> {
        Arc::new(Outbox {
            backlog: Mutex::new(Backlog {
                state: OutboxState::Open {
                    queued: VecDeque::new(),
                    used: 0,
                },
                waiting: None,
            }),
            max_message_bytes,
        })
    }

    /// Queues `msg` and returns true, or returns false when it does not fit, which closes the
    /// connection with 1008, or when the connection is closing.
    pub fn send(&self, msg: WsMessage) -> bool {
        let budget = self.max_message_bytes.budget_permits();
        let (sent, woken) = {
            let mut backlog = self.backlog.lock();
            let sent = backlog.state.send(budget, msg);
            (sent, backlog.waiting.take())
        };
        if let Some(waker) = woken {
            waker.wake();
        }
        sent
    }

    /// Closes the connection with `close` once what is queued is written.
    pub fn close(&self, close: WsClose) {
        let woken = {
            let mut backlog = self.backlog.lock();
            backlog.state.close(close);
            backlog.waiting.take()
        };
        if let Some(waker) = woken {
            waker.wake();
        }
    }

    /// Moves what is queued into the empty `into`, or returns `None` when there is nothing to
    /// write.
    pub fn take(&self, into: &mut VecDeque<WsMessage>) -> Option<Taken> {
        self.backlog.lock().state.take(into)
    }

    /// Takes as [`Self::take`] does, or registers `cx` to be woken once there is something.
    pub fn poll_take(&self, cx: &mut Context<'_>, into: &mut VecDeque<WsMessage>) -> Poll<Taken> {
        let mut backlog = self.backlog.lock();
        match backlog.state.take(into) {
            Some(take) => Poll::Ready(take),
            None => {
                backlog.waiting = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    /// Ends the outbox with its connection: what is queued is dropped, every later send returns
    /// false, and the connection leaves every topic.
    pub fn end(self: &Arc<Self>) {
        {
            let mut backlog = self.backlog.lock();
            backlog.state = OutboxState::Done;
            backlog.waiting = None;
        }
        TOPICS.forget(self);
    }

    /// Checks a message a handler hands over against `max_websocket_message_size`.
    fn admit(&self, value: &Bound<'_, PyAny>) -> PyResult<WsMessage> {
        let msg = WsMessage::from_py(value)?;
        if self.max_message_bytes.is_exceeded_by(msg.len()) {
            return Err(PyValueError::new_err(format!(
                "{} message of {} bytes exceeds max_websocket_message_size ({} bytes)",
                msg.kind(),
                msg.len(),
                self.max_message_bytes
            )));
        }
        Ok(msg)
    }
}

/// Which connections are subscribed to each topic, for the whole process.
struct Topics(Mutex<HashMap<Box<str>, Vec<Arc<Outbox>>>>);

static TOPICS: LazyLock<Topics> = LazyLock::new(|| Topics(Mutex::new(HashMap::new())));

impl Topics {
    fn subscribe(&self, topic: &str, outbox: &Arc<Outbox>) {
        let mut topics = self.0.lock();
        let subscribers = topics.entry(topic.into()).or_default();
        if !subscribers.iter().any(|s| Arc::ptr_eq(s, outbox)) {
            subscribers.push(Arc::clone(outbox));
        }
    }

    fn unsubscribe(&self, topic: &str, outbox: &Arc<Outbox>) {
        let mut topics = self.0.lock();
        if let Some(subscribers) = topics.get_mut(topic) {
            subscribers.retain(|s| !Arc::ptr_eq(s, outbox));
            if subscribers.is_empty() {
                topics.remove(topic);
            }
        }
    }

    fn forget(&self, outbox: &Arc<Outbox>) {
        self.0.lock().retain(|_, subscribers| {
            subscribers.retain(|s| !Arc::ptr_eq(s, outbox));
            !subscribers.is_empty()
        });
    }

    /// Sends `msg` to every subscriber of `topic` but `except`; a subscriber it does not fit is
    /// closed with 1008, as a send of its own would be.
    fn publish(&self, topic: &str, msg: &WsMessage, except: Option<&Arc<Outbox>>) {
        let topics = self.0.lock();
        let Some(subscribers) = topics.get(topic) else {
            return;
        };
        for subscriber in subscribers {
            if except.is_some_and(|me| Arc::ptr_eq(me, subscriber)) {
                continue;
            }
            subscriber.send(msg.clone());
        }
    }
}

/// One WebSocket connection as its handlers see it.
#[pyclass(name = "WebSocket", module = "mipyre.engine")]
pub struct WebSocket {
    request: Py<Request>,
    /// Is whatever the handlers keep for this connection, `None` until they set it.
    #[pyo3(get, set)]
    data: Py<PyAny>,
    outbox: Arc<Outbox>,
}

#[pymethods]
impl WebSocket {
    /// Returns the upgrade request, the same object the `before_request` hooks saw.
    #[getter]
    fn request(&self, py: Python<'_>) -> Py<Request> {
        self.request.clone_ref(py)
    }

    /// Queues `msg` (`str` or `bytes`) and returns True; returns False when the client reads too
    /// slowly for it to fit, which closes the connection with 1008, or once the connection is
    /// closing. A message longer than `max_websocket_message_size` raises `ValueError` and
    /// leaves the connection open: only one within the limit that does not fit the outgoing
    /// budget closes it. Raises `TypeError` for another type.
    fn send(&self, msg: &Bound<'_, PyAny>) -> PyResult<bool> {
        Ok(self.outbox.send(self.outbox.admit(msg)?))
    }

    /// Closes the connection with `code` and `reason` once what was sent before is written;
    /// later sends return False. Raises `ValueError` for a code an endpoint may not send or a
    /// reason over 123 bytes.
    #[pyo3(signature = (code = 1000, reason = ""))]
    fn close(&self, code: u16, reason: &str) -> PyResult<()> {
        if !closable(code) {
            return Err(PyValueError::new_err(format!(
                "{code} is not a close code an endpoint may send (1000-1003, 1007-1014, 3000-4999)"
            )));
        }
        if reason.len() > MAX_CLOSE_REASON_BYTES {
            return Err(PyValueError::new_err(format!(
                "a close reason is at most {MAX_CLOSE_REASON_BYTES} bytes, got {}",
                reason.len()
            )));
        }
        self.outbox.close(WsClose::new(code, reason));
        Ok(())
    }

    fn subscribe(&self, topic: &str) {
        TOPICS.subscribe(topic, &self.outbox);
    }

    fn unsubscribe(&self, topic: &str) {
        TOPICS.unsubscribe(topic, &self.outbox);
    }

    /// Sends `msg` to every connection subscribed to `topic` but this one.
    fn publish(&self, topic: &str, msg: &Bound<'_, PyAny>) -> PyResult<()> {
        TOPICS.publish(topic, &self.outbox.admit(msg)?, Some(&self.outbox));
        Ok(())
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.data)?;
        visit.call(&self.request)
    }
}

impl WebSocket {
    /// Queues what a handler returned as its reply: nothing for `None`, else the `str` or
    /// `bytes`, after what the handler sent. A reply that does not fit closes the connection
    /// with 1008, as a send would, so nothing is left to report.
    pub(crate) fn reply(&self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        if !value.is_none() {
            self.outbox.send(self.outbox.admit(value)?);
        }
        Ok(())
    }

    pub(crate) fn new(py: Python<'_>, request: Py<Request>, outbox: Arc<Outbox>) -> Self {
        WebSocket {
            request,
            data: py.None(),
            outbox,
        }
    }
}

/// Sends `msg` (`str` or `bytes`) to every WebSocket connection subscribed to `topic`, from any
/// handler on any runtime; `mipyre.publish` is this. Raises `TypeError` for another type.
pub fn publish(topic: &str, msg: &Bound<'_, PyAny>) -> PyResult<()> {
    TOPICS.publish(topic, &WsMessage::from_py(msg)?, None);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    const LIMIT: u32 = 1000;
    const BUDGET: u32 = LIMIT + MESSAGE_OVERHEAD_BYTES;

    fn text(len: usize) -> WsMessage {
        WsMessage::Text(Utf8Bytes::from("x".repeat(len)))
    }

    /// Takes from `o` into a queue of the test's own, with what follows the messages.
    fn take(o: &Outbox) -> Option<(VecDeque<WsMessage>, Taken)> {
        let mut into = VecDeque::new();
        o.take(&mut into).map(|taken| (into, taken))
    }

    fn outbox() -> Arc<Outbox> {
        Outbox::new(MessageByteLimit::new(i64::from(LIMIT)).unwrap())
    }

    #[test]
    fn a_message_fits_up_to_the_last_byte_of_the_budget() {
        let len = (BUDGET - MESSAGE_OVERHEAD_BYTES) as usize;
        assert!(fits(0, BUDGET, len));
        assert!(!fits(0, BUDGET, len + 1));
        assert!(fits(10, BUDGET, len - 10));
        assert!(!fits(10, BUDGET, len - 9));
    }

    #[test]
    fn a_max_size_message_fits_an_empty_budget_and_not_one_beside_it() {
        let o = outbox();
        assert!(o.send(text(LIMIT as usize)));
        assert!(!o.send(text(0)));
    }

    #[test]
    fn a_send_that_does_not_fit_drops_what_was_queued_and_owes_a_1008() {
        let o = outbox();
        assert!(o.send(text(600)));
        assert!(!o.send(text(600)));
        let Some((queued, Taken::MessagesThenClose(close))) = take(&o) else {
            panic!("a close is owed");
        };
        assert!(queued.is_empty());
        assert_eq!(close.code, WsClose::POLICY);
        assert_eq!(take(&o), None, "nothing after the close");
        assert!(!o.send(text(1)), "a send after the close");
    }

    #[test]
    fn taking_frees_the_budget() {
        let o = outbox();
        assert!(o.send(text(600)));
        assert_eq!(take(&o), Some((VecDeque::from([text(600)]), Taken::Messages)));
        assert!(o.send(text(600)));
    }

    #[test]
    fn a_close_goes_after_what_was_queued_and_later_sends_are_refused() {
        let o = outbox();
        assert!(o.send(text(1)));
        o.close(WsClose::new(4000, "bye"));
        assert!(!o.send(text(2)));
        o.close(WsClose::new(1000, "second"));
        assert_eq!(
            take(&o),
            Some((
                VecDeque::from([text(1)]),
                Taken::MessagesThenClose(WsClose::new(4000, "bye"))
            ))
        );
    }

    #[test]
    fn an_ended_outbox_refuses_and_leaves_its_topics() {
        let o = outbox();
        TOPICS.subscribe("ended", &o);
        o.end();
        assert!(!o.send(text(1)));
        assert!(!TOPICS.0.lock().contains_key("ended"));
    }

    #[test]
    fn the_codes_an_endpoint_may_send_stop_at_the_reserved_ones() {
        for code in [1000, 1003, 1007, 1014, 3000, 4999] {
            assert!(closable(code), "{code}");
        }
        for code in [999, 1004, 1005, 1006, 1015, 2999, 5000] {
            assert!(!closable(code), "{code}");
        }
    }

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn a_waiting_connection_is_woken_by_a_send_and_not_by_a_send_while_it_does_not_wait() {
        let o = outbox();
        let count = Arc::new(WakeCount::default());
        let waker = Waker::from(Arc::clone(&count));
        assert!(o
            .poll_take(&mut Context::from_waker(&waker), &mut VecDeque::new())
            .is_pending());
        assert!(o.send(text(1)));
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert!(o.send(text(1)));
        assert_eq!(count.0.load(Ordering::SeqCst), 1, "the connection was not waiting");
    }

    #[test]
    fn a_publish_reaches_every_subscriber_but_the_one_excluded() {
        let (me, other) = (outbox(), outbox());
        TOPICS.subscribe("room", &me);
        TOPICS.subscribe("room", &other);
        TOPICS.subscribe("room", &other);
        TOPICS.publish("room", &text(3), Some(&me));
        assert_eq!(take(&me), None);
        assert_eq!(take(&other), Some((VecDeque::from([text(3)]), Taken::Messages)));
        TOPICS.unsubscribe("room", &other);
        TOPICS.publish("room", &text(4), None);
        assert_eq!(take(&other), None);
        assert_eq!(take(&me), Some((VecDeque::from([text(4)]), Taken::Messages)));
        me.end();
    }
}
