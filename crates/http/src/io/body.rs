//! Reads request bodies, buffered whole or streamed to the handler, and defines the body every
//! response has.

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::Response;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::io::static_files::{FileBody, FileSendError};
use crate::plan::body_limit::{grown_capacity, BodySizeLimit, TooLarge};
use crate::plan::limits::{BodyTimeAllowance, Limits};

/// A whole body is kept unboxed, so the common buffered response costs no allocation of its own.
/// Only a file sent from disk can fail while it is sent; every other body is whole or infallible.
pub enum ResponseBody {
    Full(Full<Bytes>),
    Boxed(http_body_util::combinators::BoxBody<Bytes, Infallible>),
    File(FileBody),
}

impl ResponseBody {
    pub fn boxed<B>(body: B) -> Self
    where
        B: hyper::body::Body<Data = Bytes, Error = Infallible> + Send + Sync + 'static,
    {
        ResponseBody::Boxed(http_body_util::combinators::BoxBody::new(body))
    }

    pub fn empty() -> Self {
        ResponseBody::Full(Full::new(Bytes::new()))
    }
}

impl hyper::body::Body for ResponseBody {
    type Data = Bytes;
    type Error = FileSendError;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, FileSendError>>> {
        match self.get_mut() {
            ResponseBody::Full(body) => std::pin::Pin::new(body)
                .poll_frame(cx)
                .map(|frame| frame.map(|frame| frame.map_err(|e| match e {}))),
            ResponseBody::Boxed(body) => std::pin::Pin::new(body)
                .poll_frame(cx)
                .map(|frame| frame.map(|frame| frame.map_err(|e| match e {}))),
            ResponseBody::File(body) => std::pin::Pin::new(body).poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            ResponseBody::Full(body) => body.is_end_stream(),
            ResponseBody::Boxed(body) => body.is_end_stream(),
            ResponseBody::File(body) => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        match self {
            ResponseBody::Full(body) => body.size_hint(),
            ResponseBody::Boxed(body) => body.size_hint(),
            ResponseBody::File(body) => body.size_hint(),
        }
    }
}

#[inline]
pub fn full_body(resp: Response<Full<Bytes>>) -> Response<ResponseBody> {
    resp.map(ResponseBody::Full)
}

/// Every variant is the client's doing and answers a 4xx. It is `Clone` so a streamed body's
/// rejection can travel through the handler as a Python exception and back.
#[derive(Debug, Clone, thiserror::Error)]
pub enum BodyReadError {
    #[error("request body is larger than max_body_size")]
    TooLarge,
    #[error("request body did not arrive within {0:?}")]
    TimedOut(Duration),
    #[error("request body read failed: {0}")]
    ConnectionLost(#[source] Arc<hyper::Error>),
}

impl From<TooLarge> for BodyReadError {
    fn from(_: TooLarge) -> Self {
        BodyReadError::TooLarge
    }
}

/// Decides, besides the size cap, whether a buffered body may grow.
pub trait BodyGate {
    type Error: From<BodyReadError>;
    /// Admits or refuses the next frame before it is copied. `committed` is the buffer's
    /// allocation once the frame is in, which for a body that announced its size is that size
    /// from the second frame on.
    ///
    /// # Errors
    ///
    /// Returns the gate's refusal; the frame is then left uncopied.
    fn admit(&mut self, committed: usize) -> Result<(), Self::Error>;
}

impl<G: BodyGate> BodyGate for &mut G {
    type Error = G::Error;
    fn admit(&mut self, committed: usize) -> Result<(), G::Error> {
        (**self).admit(committed)
    }
}

struct OpenGate;

impl BodyGate for OpenGate {
    type Error = BodyReadError;
    fn admit(&mut self, _committed: usize) -> Result<(), BodyReadError> {
        Ok(())
    }
}

/// Reads the whole body, at most `limits.max_body` bytes, within [`Limits::body_time`].
///
/// The deadline is exact to ±100 ms because its timer starts at the first wait for a frame;
/// the time settings are read only then, so a body that is all there costs no timer.
///
/// It returns the inner future itself, like [`collect`], because this runs on every request with
/// a buffered body.
///
/// # Errors
///
/// Returns [`BodyReadError`] when the body is too large, too slow, or the connection is lost.
pub fn read_body<'l, B>(body: B, limits: &'l Limits) -> impl Future<Output = Result<Bytes, BodyReadError>> + 'l
where
    B: hyper::body::Body<Data = Bytes, Error = hyper::Error> + Unpin + 'l,
{
    collect_within(body, OpenGate, limits)
}

/// Reads the body like [`read_body`], asking `gate` before each frame is taken.
///
/// It returns the inner future itself rather than wrapping it, because this runs on every
/// request with a buffered body.
///
/// # Errors
///
/// Returns the gate's refusal, or a [`BodyReadError`] converted into the gate's error type.
pub fn collect<'g, G: BodyGate>(
    body: Incoming,
    gate: &'g mut G,
    limits: &'g Limits,
) -> impl std::future::Future<Output = Result<Bytes, G::Error>> + 'g {
    collect_within(body, gate, limits)
}

/// Arms the timer only when the body is not all there at the first poll, as most bodies are
/// because hyper read them with the head, so they cost no clock read and no timer-wheel entry.
/// The time a body earns by arriving is added when the timer fires, not per frame.
async fn collect_within<B, G>(mut body: B, mut gate: G, limits: &Limits) -> Result<Bytes, G::Error>
where
    B: hyper::body::Body<Data = Bytes, Error = hyper::Error> + Unpin,
    G: BodyGate,
{
    let hint = hyper::body::Body::size_hint(&body);
    // The announced size only caps how far the buffer grows and never sizes it ahead of the bytes
    // that came in, or a client could buy a large buffer with a two-byte header.
    let limit =
        BodySizeLimit::start(limits.max_body.as_usize(), hint.lower(), hint.exact()).map_err(BodyReadError::from)?;
    // It is atomic only because the future must be `Send` while holding a reference across an await.
    let received = crate::sync::AtomicU64::new(0);
    let read = async {
        let mut buf = BodyBuf::Empty;
        while let Some(frame) = body.frame().await {
            // HTTP/2 hands over the empty DATA frame a client may end the stream with; pushing it
            // would copy a one-frame body.
            let Ok(data) = frame
                .map_err(|e| BodyReadError::ConnectionLost(Arc::new(e)))?
                .into_data()
            else {
                continue;
            };
            if data.is_empty() {
                continue;
            }
            limit.take(buf.len(), data.len()).map_err(BodyReadError::from)?;
            gate.admit(buf.committed_with(data.len(), limit.expected()))?;
            buf = buf.push(data, limit.expected());
            received.store(buf.len() as u64, crate::sync::Ordering::Relaxed);
        }
        Ok::<_, G::Error>(buf.freeze())
    };
    let mut read = std::pin::pin!(read);
    // This is `tokio::time::timeout` with the `Sleep` made at the first wait instead of up front,
    // and moved on when it fires to the deadline the body has earned since.
    let mut timer = std::pin::pin!(None::<tokio::time::Sleep>);
    let mut started = None::<Instant>;
    std::future::poll_fn(|cx| {
        if let Poll::Ready(done) = read.as_mut().poll(cx) {
            return Poll::Ready(done);
        }
        let started = *started.get_or_insert_with(Instant::now);
        if timer.is_none() {
            timer.set(Some(tokio::time::sleep_until(started + limits.body_time().allowed(0))));
        }
        let Some(mut sleep) = timer.as_mut().as_pin_mut() else {
            return Poll::Pending;
        };
        loop {
            if sleep.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            let allowed = limits
                .body_time()
                .allowed(received.load(crate::sync::Ordering::Relaxed));
            let deadline = started + allowed;
            if Instant::now() >= deadline {
                return Poll::Ready(Err(BodyReadError::TimedOut(allowed).into()));
            }
            sleep.as_mut().reset(deadline);
        }
    })
    .await
}

/// A one-frame body, the common case, is kept as hyper handed it over, with no copy.
enum BodyBuf {
    Empty,
    One(Bytes),
    Many(BytesMut),
}

impl BodyBuf {
    fn len(&self) -> usize {
        match self {
            BodyBuf::Empty => 0,
            BodyBuf::One(b) => b.len(),
            BodyBuf::Many(b) => b.len(),
        }
    }

    /// Returns what [`BodyBuf::push`] will have allocated once a frame of `add` bytes is pushed.
    fn committed_with(&self, add: usize, expected: Option<usize>) -> usize {
        match self {
            BodyBuf::Empty => add,
            BodyBuf::One(first) => grown_capacity(first.len().saturating_add(add), expected),
            BodyBuf::Many(joined) => {
                let needed = joined.len().saturating_add(add);
                if needed <= joined.capacity() {
                    joined.capacity()
                } else {
                    grown_capacity(needed, expected)
                }
            }
        }
    }

    fn push(self, data: Bytes, expected: Option<usize>) -> Self {
        match self {
            BodyBuf::Empty => BodyBuf::One(data),
            BodyBuf::One(first) => {
                let needed = first.len() + data.len();
                let mut joined = BytesMut::with_capacity(grown_capacity(needed, expected));
                joined.extend_from_slice(&first);
                joined.extend_from_slice(&data);
                BodyBuf::Many(joined)
            }
            BodyBuf::Many(mut joined) => {
                let needed = joined.len() + data.len();
                if needed > joined.capacity() {
                    // `reserve` would grow past `expected`.
                    let mut grown = BytesMut::with_capacity(grown_capacity(needed, expected));
                    grown.extend_from_slice(&joined);
                    joined = grown;
                }
                joined.extend_from_slice(&data);
                BodyBuf::Many(joined)
            }
        }
    }

    fn freeze(self) -> Bytes {
        match self {
            BodyBuf::Empty => Bytes::new(),
            BodyBuf::One(b) => b,
            BodyBuf::Many(b) => b.freeze(),
        }
    }
}

/// It is low enough to push back on a slow handler and high enough that a steady 64 KB-frame
/// stream never stalls on scheduling latency.
const CHUNKS_IN_FLIGHT: usize = 8;

/// The extra slot is reserved before the first chunk for the body's last message, so that
/// message reaches the handler however full the channel is.
const CHANNEL_CAPACITY: usize = CHUNKS_IN_FLIGHT + 1;

/// The feeder's last message is `End`, `Rejected` or `Unread`; a channel that closes without one
/// means the feeder itself was dropped.
#[derive(Debug)]
pub enum BodyMessage {
    Chunk(Bytes),
    End,
    Rejected(BodyReadError),
    /// The handler did not take a chunk within this budget, so the feeder stopped reading: the
    /// server's own backpressure, not the client's slowness.
    Unread(Duration),
}

pub type BodySender = mpsc::Sender<BodyMessage>;
pub type BodyReceiver = mpsc::Receiver<BodyMessage>;

/// Makes the channel for one streamed body; its bound carries a slow handler's backpressure back
/// to the client's TCP window.
pub fn body_channel() -> (BodySender, BodyReceiver) {
    mpsc::channel(CHANNEL_CAPACITY)
}

#[derive(Debug, PartialEq, Eq)]
enum HandoffError {
    Deadline,
    ClosedReceiver,
}

/// Feeds a `body=BodyStream` route's body to its handler one frame at a time.
///
/// The body is bounded like a buffered one, except that time spent waiting for the handler to
/// take a chunk is not charged to the client. Each such wait lasts `time.budget` at most, so a
/// handler that stops reading cannot hold the feeder; it is then told [`BodyMessage::Unread`].
pub async fn stream_body_feeder<B>(body: B, tx: BodySender, max_size: usize, time: BodyTimeAllowance)
where
    B: hyper::body::Body<Data = Bytes, Error = hyper::Error> + Unpin,
{
    let Ok(last_slot) = tx.clone().reserve_owned().await else {
        return;
    };
    let last = match feed(body, &tx, max_size, time).await {
        Ok(Ok(())) => BodyMessage::End,
        Ok(Err(reject)) => BodyMessage::Rejected(reject),
        Err(HandoffError::Deadline) => BodyMessage::Unread(time.budget.get()),
        Err(HandoffError::ClosedReceiver) => return,
    };
    last_slot.send(last);
}

/// Hands the body's frames on as chunks under one timer, which holds the client's deadline
/// except while a chunk waits for room, when it holds the handler's limit.
async fn feed<B>(
    mut body: B,
    tx: &BodySender,
    max_size: usize,
    time: BodyTimeAllowance,
) -> Result<Result<(), BodyReadError>, HandoffError>
where
    B: hyper::body::Body<Data = Bytes, Error = hyper::Error> + Unpin,
{
    let hint = body.size_hint();
    let Ok(limit) = BodySizeLimit::start(max_size, hint.lower(), hint.exact()) else {
        return Ok(Err(BodyReadError::TooLarge));
    };
    let started = Instant::now();
    let mut handler_time = Duration::ZERO;
    let mut total: usize = 0;
    let client_deadline = |total: usize, handler_time: Duration| started + handler_time + time.allowed(total as u64);
    let mut timer = std::pin::pin!(tokio::time::sleep_until(client_deadline(0, handler_time)));
    loop {
        let next = tokio::select! {
            biased;
            next = std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)) => next,
            () = timer.as_mut() => {
                let deadline = client_deadline(total, handler_time);
                if Instant::now() >= deadline {
                    return Ok(Err(BodyReadError::TimedOut(time.allowed(total as u64))));
                }
                timer.as_mut().reset(deadline);
                continue;
            }
        };
        let frame = match next {
            Some(Ok(frame)) => frame,
            Some(Err(e)) => return Ok(Err(BodyReadError::ConnectionLost(Arc::new(e)))),
            None => return Ok(Ok(())),
        };
        // HTTP/2 ends a stream with an empty DATA frame, which must not wake the handler.
        let Ok(chunk) = frame.into_data() else {
            continue;
        };
        if chunk.is_empty() {
            continue;
        }
        let Ok(taken) = limit.take(total, chunk.len()) else {
            return Ok(Err(BodyReadError::TooLarge));
        };
        total = taken;
        // A slow handler blocks this send and so the next poll_frame, which closes the client's
        // TCP window.
        if let Some(waited) = hand(tx, BodyMessage::Chunk(chunk), timer.as_mut(), time.budget.get()).await? {
            handler_time += waited;
            timer.as_mut().reset(client_deadline(total, handler_time));
        }
    }
}

/// Hands `msg` to the handler and returns how long it waited for room, or `None` when it fit.
/// The wait lasts `limit` at most and resets `timer`, which the caller must set again after.
async fn hand(
    tx: &BodySender,
    msg: BodyMessage,
    mut timer: std::pin::Pin<&mut tokio::time::Sleep>,
    limit: Duration,
) -> Result<Option<Duration>, HandoffError> {
    let msg = match tx.try_send(msg) {
        Ok(()) => return Ok(None),
        Err(mpsc::error::TrySendError::Closed(_)) => return Err(HandoffError::ClosedReceiver),
        Err(mpsc::error::TrySendError::Full(msg)) => msg,
    };
    let waiting = Instant::now();
    timer.as_mut().reset(waiting + limit);
    tokio::select! {
        biased;
        sent = tx.send(msg) => sent.map_err(|_| HandoffError::ClosedReceiver)?,
        () = timer => return Err(HandoffError::Deadline),
    }
    Ok(Some(waiting.elapsed()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn absolute(budget: Duration) -> BodyTimeAllowance {
        let budget = crate::plan::limits::RequestBudget::from_secs(budget.as_secs_f64());
        BodyTimeAllowance::absolute(budget.expect("a budget above zero"))
    }

    /// The app's limits with `max` bytes and `time` for a body, the rest at their defaults.
    fn limits(max: usize, time: BodyTimeAllowance) -> Limits {
        Limits {
            max_body: crate::plan::limits::ByteLimit(max as u64),
            request_budget: time.budget,
            body_min_rate: time.min_rate,
            body_time_limit: time.cap,
            ..Limits::DEFAULT
        }
    }

    /// The final message waits exactly to the deadline: earlier would give up too soon, and
    /// later would let a handler that stopped reading hold the feeder.
    #[tokio::test(start_paused = true)]
    async fn a_send_to_a_handler_that_stopped_reading_gives_up_at_the_deadline() {
        let (tx, _rx) = body_channel();
        let deadline = Instant::now() + Duration::from_millis(50);
        let mut timer = std::pin::pin!(tokio::time::sleep_until(deadline));
        for _ in 0..CHANNEL_CAPACITY {
            assert!(hand(
                &tx,
                BodyMessage::Chunk(Bytes::new()),
                timer.as_mut(),
                Duration::from_millis(50)
            )
            .await
            .is_ok());
        }
        let gave_up = tokio::time::timeout(
            Duration::from_secs(5),
            hand(&tx, BodyMessage::End, timer.as_mut(), Duration::from_millis(50)),
        );
        assert!(matches!(gave_up.await, Ok(Err(HandoffError::Deadline))));
        assert_eq!(Instant::now(), deadline);
    }

    #[tokio::test(start_paused = true)]
    async fn a_message_that_fits_is_sent_past_the_deadline() {
        let (tx, mut rx) = body_channel();
        let past = Instant::now();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let budget = Duration::from_secs(1);
        let sent = hand(
            &tx,
            BodyMessage::Rejected(BodyReadError::TimedOut(budget)),
            std::pin::pin!(tokio::time::sleep_until(past)),
            Duration::ZERO,
        )
        .await;
        assert!(sent.is_ok());
        assert!(matches!(
            rx.recv().await,
            Some(BodyMessage::Rejected(BodyReadError::TimedOut(b))) if b == budget
        ));
    }

    fn stalled() -> impl hyper::body::Body<Data = Bytes, Error = hyper::Error> + Unpin {
        http_body_util::StreamBody::new(tokio_stream::pending::<Result<hyper::body::Frame<Bytes>, hyper::Error>>())
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_body_is_refused_within_100_ms_of_the_budget() {
        let budget = Duration::from_secs(30);
        let started = Instant::now();
        let read = collect_within(stalled(), &mut OpenGate, &limits(1024, absolute(budget))).await;
        let took = started.elapsed();
        assert!(
            matches!(read, Err(BodyReadError::TimedOut(b)) if b == budget),
            "{read:?}"
        );
        assert!(took >= budget, "{took:?}");
        assert!(took <= budget + Duration::from_millis(100), "{took:?}");
    }

    /// The deadline counts from the call, not from the last frame.
    #[tokio::test(start_paused = true)]
    async fn a_body_that_stalls_after_its_first_frame_is_refused_within_100_ms_of_the_budget() {
        let budget = Duration::from_secs(30);
        let first = tokio_stream::iter([Ok(hyper::body::Frame::data(Bytes::from_static(b"12")))]);
        let body = http_body_util::StreamBody::new(tokio_stream::StreamExt::chain(first, tokio_stream::pending()));
        let started = Instant::now();
        let read = collect_within(body, &mut OpenGate, &limits(1024, absolute(budget))).await;
        let took = started.elapsed();
        assert!(
            matches!(read, Err(BodyReadError::TimedOut(b)) if b == budget),
            "{read:?}"
        );
        assert!(took >= budget, "{took:?}");
        assert!(took <= budget + Duration::from_millis(100), "{took:?}");
    }

    /// A body that is all there arms no timer: it is read outside any runtime, where arming one
    /// would panic.
    #[test]
    fn a_body_that_is_all_there_is_read_whole_without_a_timer() {
        let body = Full::new(Bytes::from_static(b"hello")).map_err(|never| match never {});
        let limits = limits(1024, absolute(Duration::from_secs(30)));
        let mut gate = OpenGate;
        let read = std::pin::pin!(collect_within(body, &mut gate, &limits));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let Poll::Ready(read) = read.poll(&mut cx) else {
            panic!("a body that is all there is read in one poll");
        };
        assert_eq!(read.unwrap(), Bytes::from_static(b"hello"));
    }

    /// Yields its frames, then ends, or waits forever when `stall` is set; `hint` stands for a
    /// `Content-Length` header.
    struct Scripted {
        frames: std::collections::VecDeque<hyper::body::Frame<Bytes>>,
        hint: Option<u64>,
        stall: bool,
        polled: usize,
    }

    impl Scripted {
        fn new(frames: &[&'static [u8]], hint: Option<u64>) -> Self {
            Scripted {
                frames: frames
                    .iter()
                    .map(|f| hyper::body::Frame::data(Bytes::from_static(f)))
                    .collect(),
                hint,
                stall: false,
                polled: 0,
            }
        }

        fn stalled(hint: Option<u64>) -> Self {
            Scripted {
                stall: true,
                ..Scripted::new(&[], hint)
            }
        }
    }

    impl hyper::body::Body for Scripted {
        type Data = Bytes;
        type Error = hyper::Error;
        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, hyper::Error>>> {
            self.polled += 1;
            match self.frames.pop_front() {
                Some(frame) => Poll::Ready(Some(Ok(frame))),
                None if self.stall => Poll::Pending,
                None => Poll::Ready(None),
            }
        }
        fn size_hint(&self) -> hyper::body::SizeHint {
            self.hint
                .map_or_else(hyper::body::SizeHint::default, hyper::body::SizeHint::with_exact)
        }
    }

    struct Recording {
        asked: Vec<usize>,
        limit: usize,
    }

    impl BodyGate for Recording {
        type Error = BodyReadError;
        fn admit(&mut self, committed: usize) -> Result<(), BodyReadError> {
            self.asked.push(committed);
            if committed > self.limit {
                return Err(BodyReadError::TooLarge);
            }
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_body_announced_over_the_cap_is_refused_before_it_is_read() {
        let started = Instant::now();
        let mut body = Scripted::stalled(Some(2048));
        let read = collect_within(
            &mut body,
            &mut OpenGate,
            &limits(1024, absolute(Duration::from_secs(30))),
        )
        .await;
        assert!(matches!(read, Err(BodyReadError::TooLarge)), "{read:?}");
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert_eq!(body.polled, 0, "no frame was read");
    }

    #[tokio::test]
    async fn frames_that_add_up_past_the_cap_are_refused() {
        let body = Scripted::new(&[b"0123456789", b"0123456789", b"0"], None);
        let read = collect_within(body, &mut OpenGate, &limits(20, absolute(Duration::from_secs(30)))).await;
        assert!(matches!(read, Err(BodyReadError::TooLarge)), "{read:?}");
        let body = Scripted::new(&[b"0123456789", b"0123456789"], None);
        let read = collect_within(body, &mut OpenGate, &limits(20, absolute(Duration::from_secs(30)))).await;
        assert_eq!(read.unwrap().len(), 20, "exactly the cap is allowed");
    }

    /// The gate is asked about each buffer before it is made.
    #[tokio::test]
    async fn an_announced_size_does_not_buy_a_buffer_of_that_size() {
        const ANNOUNCED: u64 = 10 << 20;
        let body = Scripted::new(&[b"a", b"b"], Some(ANNOUNCED));
        let mut gate = Recording {
            asked: Vec::new(),
            limit: usize::MAX,
        };
        let read = collect_within(body, &mut gate, &limits(16 << 20, absolute(Duration::from_secs(30)))).await;
        assert_eq!(read.unwrap(), Bytes::from_static(b"ab"));
        assert_eq!(gate.asked, vec![1, 4]);
    }

    #[tokio::test]
    async fn a_gate_refusal_stops_the_read_before_the_frame_is_taken() {
        let body = Scripted::new(&[b"0123", b"4567", b"89"], None);
        let mut gate = Recording {
            asked: Vec::new(),
            limit: 8,
        };
        let read = collect_within(body, &mut gate, &limits(1024, absolute(Duration::from_secs(30)))).await;
        assert!(matches!(read, Err(BodyReadError::TooLarge)), "{read:?}");
        // The second ask is 16, twice the 8 bytes it must hold, so the third frame is never asked about.
        assert_eq!(gate.asked, vec![4, 16]);
    }

    #[tokio::test]
    async fn a_body_in_several_frames_is_joined_in_order() {
        let body = Scripted::new(&[b"he", b"ll", b"o"], Some(5));
        let mut gate = Recording {
            asked: Vec::new(),
            limit: usize::MAX,
        };
        let read = collect_within(body, &mut gate, &limits(1024, absolute(Duration::from_secs(30)))).await;
        assert_eq!(read.unwrap(), Bytes::from_static(b"hello"));
        assert_eq!(gate.asked, vec![2, 5, 5]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_streamed_body_announced_over_the_cap_is_refused_before_a_chunk() {
        let (tx, mut rx) = body_channel();
        let body = Scripted::new(&[b"0123"], Some(2048));
        stream_body_feeder(body, tx, 1024, absolute(Duration::from_secs(30))).await;
        assert!(
            matches!(rx.recv().await, Some(BodyMessage::Rejected(BodyReadError::TooLarge))),
            "the rejection comes first"
        );
        assert!(rx.recv().await.is_none());
    }

    fn chunks_then(n: usize, tail: Option<hyper::body::Frame<Bytes>>, stall: bool) -> Scripted {
        let mut body = Scripted::new(&[], None);
        body.frames = (0..n)
            .map(|_| hyper::body::Frame::data(Bytes::from_static(b"x")))
            .chain(tail)
            .collect();
        body.stall = stall;
        body
    }

    /// Counts the chunks on the channel and returns them with the message that ended it.
    async fn read_all(rx: &mut BodyReceiver) -> (usize, Option<BodyMessage>) {
        let mut chunks = 0;
        loop {
            match rx.recv().await {
                Some(BodyMessage::Chunk(_)) => chunks += 1,
                other => return (chunks, other),
            }
        }
    }

    /// Trailers are not handed to the handler.
    #[tokio::test]
    async fn a_streamed_body_arrives_whole_and_ends() {
        let (tx, mut rx) = body_channel();
        let trailers = hyper::body::Frame::trailers(hyper::HeaderMap::new());
        let feeder = tokio::spawn(stream_body_feeder(
            chunks_then(20, Some(trailers), false),
            tx,
            1024,
            absolute(Duration::from_secs(30)),
        ));
        let (chunks, last) = read_all(&mut rx).await;
        feeder.await.unwrap();
        assert_eq!(chunks, 20);
        assert!(matches!(last, Some(BodyMessage::End)), "{last:?}");
    }

    #[tokio::test]
    async fn a_streamed_body_over_the_cap_is_refused() {
        let (tx, mut rx) = body_channel();
        let feeder = tokio::spawn(stream_body_feeder(
            chunks_then(5, None, false),
            tx,
            3,
            absolute(Duration::from_secs(30)),
        ));
        let (chunks, last) = read_all(&mut rx).await;
        feeder.await.unwrap();
        assert_eq!(chunks, 3);
        assert!(
            matches!(last, Some(BodyMessage::Rejected(BodyReadError::TooLarge))),
            "{last:?}"
        );
    }

    /// The feeder does not wait for room to say it, so the handler never sees a stream that just
    /// stops.
    #[tokio::test(start_paused = true)]
    async fn a_client_too_slow_is_told_to_a_handler_with_a_full_channel() {
        let budget = Duration::from_secs(30);
        let (tx, mut rx) = body_channel();
        let started = Instant::now();
        stream_body_feeder(chunks_then(CHUNKS_IN_FLIGHT, None, true), tx, 1024, absolute(budget)).await;
        assert_eq!(started.elapsed(), budget, "it gave up at the deadline, no later");
        let (chunks, last) = read_all(&mut rx).await;
        assert_eq!(chunks, CHUNKS_IN_FLIGHT);
        assert!(
            matches!(last, Some(BodyMessage::Rejected(BodyReadError::TimedOut(b))) if b == budget),
            "{last:?}"
        );
    }

    /// A 408 here would blame the client for the server's own backpressure.
    #[tokio::test(start_paused = true)]
    async fn a_handler_that_does_not_read_is_told_so_not_blamed_on_the_client() {
        let budget = Duration::from_secs(30);
        let (tx, mut rx) = body_channel();
        let started = Instant::now();
        stream_body_feeder(chunks_then(100, None, false), tx, 1024, absolute(budget)).await;
        assert_eq!(started.elapsed(), budget);
        let (chunks, last) = read_all(&mut rx).await;
        assert_eq!(chunks, CHUNKS_IN_FLIGHT);
        assert!(matches!(last, Some(BodyMessage::Unread(b)) if b == budget), "{last:?}");
    }

    struct ThenIncoming {
        first: std::collections::VecDeque<Bytes>,
        rest: Incoming,
    }

    impl hyper::body::Body for ThenIncoming {
        type Data = Bytes;
        type Error = hyper::Error;
        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, hyper::Error>>> {
            match self.first.pop_front() {
                Some(chunk) => Poll::Ready(Some(Ok(hyper::body::Frame::data(chunk)))),
                None => std::pin::Pin::new(&mut self.rest).poll_frame(cx),
            }
        }
    }

    /// The feeder sends `ConnectionLost` in its reserved slot instead of waiting for room until the
    /// deadline. hyper serves the request over an in-memory pipe so the `hyper::Error` is real.
    #[tokio::test]
    async fn a_client_gone_while_the_channel_is_full_is_told_to_the_handler_at_once() {
        use tokio::io::AsyncWriteExt;

        let budget = Duration::from_secs(600);
        let (tx, mut rx) = body_channel();
        let (fed, fed_rx) = tokio::sync::oneshot::channel::<()>();
        let handed = std::sync::Mutex::new(Some((tx, fed)));
        let service = hyper::service::service_fn(move |req: hyper::Request<Incoming>| {
            let handed = handed.lock().unwrap().take();
            async move {
                if let Some((tx, fed)) = handed {
                    let body = ThenIncoming {
                        first: (0..CHUNKS_IN_FLIGHT).map(|_| Bytes::from_static(b"x")).collect(),
                        rest: req.into_body(),
                    };
                    stream_body_feeder(body, tx, 1 << 20, absolute(budget)).await;
                    fed.send(()).expect("the test waits for the feeder");
                }
                Ok::<_, std::convert::Infallible>(Response::new(http_body_util::Empty::<Bytes>::new()))
            }
        });
        let (mut client, server) = tokio::io::duplex(4096);
        let conn = tokio::spawn(
            hyper::server::conn::http1::Builder::new().serve_connection(hyper_util::rt::TokioIo::new(server), service),
        );
        client
            .write_all(b"POST / HTTP/1.1\r\nhost: t\r\ncontent-length: 100\r\n\r\n")
            .await
            .unwrap();
        drop(client);

        let fed_in_time = tokio::time::timeout(Duration::from_secs(10), fed_rx).await;
        assert!(
            matches!(fed_in_time, Ok(Ok(()))),
            "the feeder held on to tell a handler that is not reading: {fed_in_time:?}"
        );
        let (chunks, last) = read_all(&mut rx).await;
        assert_eq!(chunks, CHUNKS_IN_FLIGHT);
        assert!(
            matches!(last, Some(BodyMessage::Rejected(BodyReadError::ConnectionLost(_)))),
            "{last:?}"
        );
        conn.abort();
    }

    fn trickle(
        frames: usize,
        size: usize,
        every: Duration,
    ) -> impl hyper::body::Body<Data = Bytes, Error = hyper::Error> + Unpin {
        let chunk = Bytes::from(vec![b'x'; size]);
        let frames = (0..frames).map(move |_| Ok(hyper::body::Frame::data(chunk.clone())));
        http_body_util::StreamBody::new(Box::pin(tokio_stream::StreamExt::throttle(
            tokio_stream::iter(frames),
            every,
        )))
    }

    /// Allows `budget_s` seconds plus one for every `min_rate` bytes, at most `cap_s` in all.
    fn allowance(budget_s: u64, min_rate: u64, cap_s: u64) -> BodyTimeAllowance {
        use crate::plan::limits::{BodyMinRate, BodyTimeLimit, RequestBudget};
        BodyTimeAllowance {
            budget: RequestBudget::from_secs(budget_s as f64).expect("a budget above zero"),
            min_rate: BodyMinRate::new(min_rate as i64).expect("a rate above zero"),
            cap: BodyTimeLimit::from_secs(cap_s as f64).expect("a cap above zero"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_body_arriving_at_the_minimum_rate_is_read_past_the_budget() {
        let time = allowance(30, 1024, 600);
        // Frames come at 0 s to 89 s and the stream ends a second after the last, so the read
        // takes exactly 90 s, three times the budget.
        let started = Instant::now();
        let read = collect_within(
            trickle(90, 2048, Duration::from_secs(1)),
            &mut OpenGate,
            &limits(1 << 20, time),
        )
        .await;
        assert_eq!(read.map(|b| b.len()).ok(), Some(90 * 2048));
        assert_eq!(started.elapsed(), Duration::from_secs(90));
    }

    #[tokio::test(start_paused = true)]
    async fn a_body_slower_than_the_minimum_rate_runs_out_of_time() {
        let time = allowance(30, 1024, 600);
        // At 30 s, 3100 bytes have come and earn 3 s; at 33 s, 3400 bytes still earn only 3 s, so
        // the body runs out of time at 33 s.
        let started = Instant::now();
        let read = collect_within(
            trickle(1000, 100, Duration::from_secs(1)),
            &mut OpenGate,
            &limits(1 << 20, time),
        )
        .await;
        let Err(BodyReadError::TimedOut(allowed)) = read else {
            panic!("{read:?}");
        };
        assert_eq!(allowed, Duration::from_secs(33));
        assert_eq!(allowed, time.allowed(34 * 100));
        assert_eq!(started.elapsed(), allowed);
    }

    #[tokio::test(start_paused = true)]
    async fn a_body_does_not_get_past_the_cap() {
        let time = allowance(30, 1024, 60);
        let started = Instant::now();
        let read = collect_within(
            trickle(1000, 8192, Duration::from_secs(1)),
            &mut OpenGate,
            &limits(64 << 20, time),
        )
        .await;
        assert!(
            matches!(read, Err(BodyReadError::TimedOut(a)) if a == Duration::from_secs(60)),
            "{read:?}"
        );
        assert_eq!(started.elapsed(), Duration::from_secs(60));
    }

    /// What the rules say a [`trickle`] body comes to.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum TrickleOutcome {
        Read,
        Refused(Duration),
        /// The stream ends at the very instant the timer fires. tokio does not promise which of two
        /// timers due at one instant a task sees first (it follows the timers' history in the wheel),
        /// and the reader promises nothing for a tie, so both are right: read whole, or refused under
        /// this allowance.
        ReadOrRefused(Duration),
    }

    /// Returns when a [`trickle`] body is done with and what it comes to, computed independently of
    /// the reader. A frame due before the timer is read before it.
    fn trickle_outcome(frames: u64, size: u64, every: Duration, time: BodyTimeAllowance) -> (Duration, TrickleOutcome) {
        let ended = every * u32::try_from(frames).unwrap();
        let mut fires = time.allowed(0);
        loop {
            if ended < fires {
                return (ended, TrickleOutcome::Read);
            }
            let arrived = (fires.as_nanos() / every.as_nanos()) as u64 + 1;
            let allowed = time.allowed(arrived.min(frames) * size);
            if ended == fires {
                return if fires >= allowed {
                    (ended, TrickleOutcome::ReadOrRefused(allowed))
                } else {
                    (ended, TrickleOutcome::Read)
                };
            }
            if fires >= allowed {
                return (fires, TrickleOutcome::Refused(allowed));
            }
            fires = allowed;
        }
    }

    /// Reads a [`trickle`] body on a paused clock and returns how long it took and what came of it.
    fn read_trickle(
        frames: u64,
        size: u64,
        every: Duration,
        time: BodyTimeAllowance,
    ) -> (Duration, Result<Bytes, BodyReadError>) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        rt.block_on(async {
            let started = Instant::now();
            let read = collect_within(
                trickle(frames as usize, size as usize, every),
                &mut OpenGate,
                &limits(1 << 30, time),
            )
            .await;
            (started.elapsed(), read)
        })
    }

    /// Returns why `read` breaks the rules' `outcome`, or `None` when it keeps them.
    fn broken_rule(read: &Result<Bytes, BodyReadError>, outcome: TrickleOutcome, whole: u64) -> Option<String> {
        match (read, outcome) {
            (Ok(body), TrickleOutcome::Read | TrickleOutcome::ReadOrRefused(_)) if body.len() as u64 == whole => None,
            (
                Err(BodyReadError::TimedOut(allowed)),
                TrickleOutcome::Refused(want) | TrickleOutcome::ReadOrRefused(want),
            ) if *allowed == want => None,
            (read, outcome) => Some(format!("read {read:?}, the rules say {outcome:?}")),
        }
    }

    proptest::proptest! {
        /// The instant and the allowance must match exactly, not within a second.
        #[test]
        fn a_body_is_read_or_refused_exactly_when_its_time_says(
            frames in 1u64..120,
            size in 1u64..4096,
            every_ms in 100u64..3000,
            budget_s in 1u64..40,
            min_rate in 1u64..8192,
            over_cap_s in 0u64..200,
        ) {
            let time = allowance(budget_s, min_rate, budget_s + over_cap_s);
            let every = Duration::from_millis(every_ms);
            let (at, outcome) = trickle_outcome(frames, size, every, time);
            let (took, read) = read_trickle(frames, size, every, time);
            proptest::prop_assert_eq!(took, at);
            if let Some(why) = broken_rule(&read, outcome, frames * size) {
                proptest::prop_assert!(false, "{}", why);
            }
        }
    }

    /// The case that found the tie: 32 frames of 1822 bytes every 1.625 s end at 52 s, the instant
    /// the timer reaches the 52 s cap, and tokio showed the timer first.
    #[test]
    fn a_stream_ending_as_the_timer_fires_may_be_read_or_refused() {
        let time = allowance(37, 3645, 52);
        let every = Duration::from_millis(1625);
        let (at, outcome) = trickle_outcome(32, 1822, every, time);
        assert_eq!(
            (at, outcome),
            (
                Duration::from_secs(52),
                TrickleOutcome::ReadOrRefused(Duration::from_secs(52))
            )
        );
        let (took, read) = read_trickle(32, 1822, every, time);
        assert_eq!(took, at);
        assert_eq!(broken_rule(&read, outcome, 32 * 1822), None);
    }

    /// One frame holding the tie case's whole body, ending a millisecond either side of the same
    /// 52 s deadline: before it the body is read, after it refused; only the instant itself is a tie.
    #[test]
    fn a_millisecond_either_side_of_the_tie_is_exact() {
        let time = allowance(37, 3645, 52);
        let size = 32 * 1822;
        for (every_ms, want) in [
            (51_999, (Duration::from_millis(51_999), TrickleOutcome::Read)),
            (
                52_001,
                (
                    Duration::from_secs(52),
                    TrickleOutcome::Refused(Duration::from_secs(52)),
                ),
            ),
        ] {
            let every = Duration::from_millis(every_ms);
            assert_eq!(trickle_outcome(1, size, every, time), want, "ending at {every_ms} ms");
            let (took, read) = read_trickle(1, size, every, time);
            assert_eq!(took, want.0, "ending at {every_ms} ms");
            assert_eq!(broken_rule(&read, want.1, size), None, "ending at {every_ms} ms");
        }
    }

    /// Twenty seconds of frames and a handler that reads nothing for 25 s pass a 30 s budget.
    #[tokio::test(start_paused = true)]
    async fn the_handler_keeping_a_streamed_body_waiting_is_not_the_clients_time() {
        let budget = Duration::from_secs(30);
        let (tx, mut rx) = body_channel();
        let feeder = tokio::spawn(stream_body_feeder(
            trickle(20, 1, Duration::from_secs(1)),
            tx,
            1024,
            absolute(budget),
        ));
        tokio::time::sleep(Duration::from_secs(25)).await;
        let (chunks, last) = read_all(&mut rx).await;
        feeder.await.unwrap();
        assert_eq!(chunks, 20);
        assert!(matches!(last, Some(BodyMessage::End)), "{last:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_streamed_body_at_the_minimum_rate_goes_past_the_budget() {
        let time = allowance(30, 1024, 600);
        let (tx, mut rx) = body_channel();
        let feeder = tokio::spawn(stream_body_feeder(
            trickle(60, 2048, Duration::from_secs(1)),
            tx,
            1 << 20,
            time,
        ));
        let (chunks, last) = read_all(&mut rx).await;
        feeder.await.unwrap();
        assert_eq!(chunks, 60);
        assert!(matches!(last, Some(BodyMessage::End)), "{last:?}");
    }

    /// A per-frame deadline would let this body through, though it would take ten budgets in all.
    #[tokio::test(start_paused = true)]
    async fn a_streamed_body_dripping_under_the_per_frame_time_is_refused_at_the_whole_budget() {
        let budget = Duration::from_secs(4);
        let (tx, mut rx) = body_channel();
        let started = Instant::now();
        let feeder = tokio::spawn(stream_body_feeder(
            trickle(40, 1, budget / 4),
            tx,
            1024,
            absolute(budget),
        ));
        let (chunks, last) = read_all(&mut rx).await;
        feeder.await.unwrap();
        assert!(chunks < 40, "{chunks}");
        assert!(
            matches!(last, Some(BodyMessage::Rejected(BodyReadError::TimedOut(b))) if b == budget),
            "{last:?}"
        );
        assert_eq!(started.elapsed(), budget);
    }

    #[tokio::test]
    async fn empty_frames_leave_a_one_frame_body_uncopied() {
        const BODY: &[u8] = b"abc";
        const NONE: &[u8] = b"";
        async fn read(frames: &[&'static [u8]]) -> Result<Bytes, BodyReadError> {
            collect_within(
                Scripted::new(frames, None),
                &mut OpenGate,
                &limits(1024, absolute(Duration::from_secs(30))),
            )
            .await
        }
        for frames in [&[BODY][..], &[BODY, NONE], &[NONE, BODY], &[NONE, BODY, NONE]] {
            let body = read(frames).await.expect("the body was read");
            assert_eq!(body, BODY, "{frames:?}");
            assert_eq!(body.as_ptr(), BODY.as_ptr(), "copied: {frames:?}");
        }
        assert!(read(&[NONE, NONE]).await.expect("the body was read").is_empty());
        assert_eq!(read(&[BODY, BODY]).await.expect("the body was read"), b"abcabc"[..]);
    }

    #[tokio::test]
    async fn an_empty_frame_is_not_put_to_the_gate() {
        let mut gate = Recording {
            asked: Vec::new(),
            limit: usize::MAX,
        };
        let body = Scripted::new(&[b"abc", b""], None);
        let read = collect_within(body, &mut gate, &limits(1024, absolute(Duration::from_secs(30)))).await;
        assert_eq!(read.expect("the body was read"), b"abc"[..]);
        assert_eq!(gate.asked, [3]);
    }

    #[tokio::test]
    async fn a_streamed_body_hands_on_no_empty_chunk() {
        let (tx, mut rx) = body_channel();
        let body = Scripted::new(&[b"", b"x", b"", b"x", b""], None);
        stream_body_feeder(body, tx, 1024, absolute(Duration::from_secs(30))).await;
        let (chunks, last) = read_all(&mut rx).await;
        assert_eq!(chunks, 2);
        assert!(matches!(last, Some(BodyMessage::End)), "{last:?}");
    }

    /// Builds an HTTP/2 POST whose body `abc` is followed by an empty DATA frame that ends the
    /// stream, the way a client that streams its body ends it.
    fn h2_body_then_empty_end() -> Vec<u8> {
        let frame = |kind: u8, flags: u8, stream: u32, payload: &[u8]| {
            let len = u32::try_from(payload.len())
                .expect("a test frame is small")
                .to_be_bytes();
            let mut out = vec![len[1], len[2], len[3], kind, flags];
            out.extend_from_slice(&stream.to_be_bytes());
            out.extend_from_slice(payload);
            out
        };
        const DATA: u8 = 0;
        const HEADERS: u8 = 1;
        const SETTINGS: u8 = 4;
        const END_STREAM: u8 = 1;
        const END_HEADERS: u8 = 4;
        // HPACK: :method POST, :scheme http, :path / (static table), :authority "a".
        let headers = [0x83, 0x86, 0x84, 0x41, 0x01, b'a'];
        [
            &b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"[..],
            &frame(SETTINGS, 0, 0, &[]),
            &frame(HEADERS, END_HEADERS, 1, &headers),
            &frame(DATA, 0, 1, b"abc"),
            &frame(DATA, END_STREAM, 1, &[]),
        ]
        .concat()
    }

    /// Returns what `read` makes of [`h2_body_then_empty_end`]'s body, served by hyper over an
    /// in-memory pipe.
    async fn read_h2_body<T, F, Fut>(read: F) -> T
    where
        T: Send + 'static,
        F: FnOnce(Incoming) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send,
    {
        use tokio::io::AsyncWriteExt;

        let (done, done_rx) = tokio::sync::oneshot::channel();
        let handed = std::sync::Mutex::new(Some((read, done)));
        let service = hyper::service::service_fn(move |req: hyper::Request<Incoming>| {
            let handed = handed.lock().unwrap().take();
            async move {
                if let Some((read, done)) = handed {
                    let out = read(req.into_body()).await;
                    done.send(out).unwrap_or_else(|_| panic!("the test waits for the read"));
                }
                Ok::<_, std::convert::Infallible>(Response::new(http_body_util::Empty::<Bytes>::new()))
            }
        });
        let (mut client, server) = tokio::io::duplex(1 << 16);
        let conn = tokio::spawn(
            hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection(hyper_util::rt::TokioIo::new(server), service),
        );
        client.write_all(&h2_body_then_empty_end()).await.unwrap();
        let out = done_rx.await.expect("hyper served the request");
        conn.abort();
        out
    }

    /// Keeps a second handle on each data frame it hands over.
    struct FrameTap<B> {
        body: B,
        frames: Vec<Bytes>,
    }

    impl<B: hyper::body::Body<Data = Bytes> + Unpin> hyper::body::Body for FrameTap<B> {
        type Data = Bytes;
        type Error = B::Error;
        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, B::Error>>> {
            let frame = std::pin::Pin::new(&mut self.body).poll_frame(cx);
            if let Poll::Ready(Some(Ok(frame))) = &frame {
                if let Some(data) = frame.data_ref() {
                    self.frames.push(data.clone());
                }
            }
            frame
        }
        fn size_hint(&self) -> hyper::body::SizeHint {
            self.body.size_hint()
        }
    }

    /// hyper hands the closing empty DATA frame over as a data frame of its own.
    #[tokio::test]
    async fn an_h2_body_ended_by_an_empty_frame_is_read_uncopied() {
        let (frames, read) = read_h2_body(|body| async move {
            let mut tap = FrameTap {
                body,
                frames: Vec::new(),
            };
            let read = read_body(&mut tap, &limits(1024, absolute(Duration::from_secs(30)))).await;
            (tap.frames, read)
        })
        .await;
        let lens: Vec<usize> = frames.iter().map(Bytes::len).collect();
        assert_eq!(lens, [3, 0], "the frames hyper handed over");
        let read = read.expect("the body was read");
        assert_eq!(read, Bytes::from_static(b"abc"));
        assert_eq!(read.as_ptr(), frames[0].as_ptr(), "the body was copied");
    }

    #[tokio::test]
    async fn an_h2_body_ended_by_an_empty_frame_streams_one_chunk() {
        let (tx, mut rx) = body_channel();
        read_h2_body(move |body| stream_body_feeder(body, tx, 1024, absolute(Duration::from_secs(30)))).await;
        let mut chunks = Vec::new();
        let last = loop {
            match rx.recv().await {
                Some(BodyMessage::Chunk(chunk)) => chunks.push(chunk),
                other => break other,
            }
        };
        assert_eq!(chunks, [Bytes::from_static(b"abc")]);
        assert!(matches!(last, Some(BodyMessage::End)), "{last:?}");
    }
}
