//! Serves WebSocket connections: a Tokio message pump on one side and the connection's Python half
//! on the other, a task on an async engine (`mipyre_runtime::WsEngines`), joined by two queues
//! (`mipyre_runtime::exchange::websocket`).
//!
//! Every resource a client can make the server hold is bounded: connections by `max_connections`;
//! message and frame size by `max_message_bytes`; the bytes queued in each direction by a
//! per-connection byte budget; and how long a client may go unheard by `keepalive`.

use crate::sync::{AtomicUsize, Ordering};
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::Either;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::HeaderValue;
use hyper::{Request, Response, StatusCode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_tungstenite::WebSocketStream;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tungstenite::Message;

use crate::conn::{report_write_stall, write_stalled};
use crate::pipeline::{
    await_reply, client_of, cors_origin, fail, finish, refused_site, reply_or_lost, AcceptEncoding, AccessTimer,
    Requester, ResponseSource,
};
use crate::respond::http_response;
use crate::site::Site;
use crate::ServerCounters;
use mipyre_http::io::body::{full_body, ResponseBody};
use mipyre_http::io::request_head::{RequestHead, RequestLabel};
use mipyre_http::plan::limits::{WsKeepalive, WsLimits};
use mipyre_http::plan::request_id::RequestId;
use mipyre_http::plan::response::site_miss_response;
use mipyre_http::plan::route::{Params, WsPlace, WsRouteId};
use mipyre_http::plan::ws::WsHandshake;
use mipyre_runtime::exchange;
use mipyre_runtime::exchange::error::WorkerGroup;
use mipyre_runtime::exchange::RequestBody;
use mipyre_runtime::exchange::{
    connection_channels, refuse, Capacity, ConnectionEnds, QueueEntry, QueueSender, ReplyHolder, UnansweredError,
    WebSocketMessage,
};
use mipyre_runtime::{InterpreterPool, MainThreads, NoLiveEngine, WsConnection, WsEngines};

// ---------------------------------------------------------------------------
// The limits come per app from `Limits::ws`.
// ---------------------------------------------------------------------------

/// Returns tungstenite's settings, which refuse any message or frame past `max_message_bytes`.
fn tungstenite_config(limits: WsLimits) -> WebSocketConfig {
    let max = Some(limits.max_message_bytes.as_usize());
    WebSocketConfig::default().max_message_size(max).max_frame_size(max)
}

/// Counts the WebSocket connections one server run has open.
#[derive(Clone, Default)]
pub struct OpenConnections(Arc<AtomicUsize>);

/// It is held until the connection's Python task has let go of the connection, and released on
/// drop.
struct ConnectionSlot(Arc<AtomicUsize>);

impl ConnectionSlot {
    fn try_acquire(open: &OpenConnections, max_connections: usize) -> Option<Self> {
        open.0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < max_connections).then_some(n + 1)
            })
            .ok()
            .map(|_| Self(Arc::clone(&open.0)))
    }
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

// ---------------------------------------------------------------------------
// The upgrade handshake is answered here.
// ---------------------------------------------------------------------------

pub(crate) struct WsRouteMatch {
    route: WsRouteId,
    params: Params,
}

/// Returns the WebSocket route `req` asks for, if any; `Upgrade: websocket` on an ordinary route is
/// routed normally. Whether the handshake is valid is left to `WsHandshake::parse`.
fn wants_websocket(req: &Request<Incoming>, site: &Site) -> Option<WsRouteMatch> {
    if !mipyre_http::plan::ws::asks_for_websocket(req.headers()) {
        return None;
    }
    let (route, params) = site.rules.routes.websocket(req.uri().path())?;
    Some(WsRouteMatch { route, params })
}

/// What a connection answers WebSocket upgrades with: the site's routes and the engines their
/// connections are handed to, by where each route runs.
#[derive(Clone)]
pub struct WsUpgrade {
    site: Arc<Site>,
    workers: Option<WsEngines>,
    main: Option<WsEngines>,
}

/// Returns what a connection answers WebSocket upgrades with, or `None` when the site has no
/// WebSocket route. It is decided once per site: without one a connection's service has no upgrade
/// branch, and a TPC connection is driven without a lock. A route's engines are the async pool's
/// workers' or main's, which start whenever a route runs there.
pub fn upgrade_target(
    site: &Arc<Site>,
    pool: Option<&InterpreterPool>,
    main: Option<&MainThreads>,
) -> Option<WsUpgrade> {
    site.rules.routes.has_websocket_routes().then(|| WsUpgrade {
        site: Arc::clone(site),
        workers: pool.and_then(InterpreterPool::websocket_engines).cloned(),
        main: main.and_then(MainThreads::websocket_engines).cloned(),
    })
}

impl WsUpgrade {
    /// Returns the engines `route` runs on and the group they belong to.
    fn engines(&self, route: WsRouteId) -> (Option<&WsEngines>, WorkerGroup) {
        match self.site.rules.routes.ws_place(route) {
            WsPlace::Worker => (self.workers.as_ref(), WorkerGroup::Subinterp),
            WsPlace::Main => (self.main.as_ref(), WorkerGroup::Main),
        }
    }
}

/// Upgrades `req` when it asks for one of the site's WebSocket routes, and hands it to `handle`
/// otherwise.
pub(crate) fn upgrade_or<H, Fut>(
    req: Request<Incoming>,
    upgrade: &WsUpgrade,
    client: mipyre_transport::driver::Peer,
    handle: &H,
) -> Either<impl Future<Output = Result<Response<ResponseBody>, hyper::Error>>, Fut>
where
    H: Fn(Request<Incoming>, mipyre_transport::driver::Peer) -> Fut,
{
    match wants_websocket(&req, &upgrade.site) {
        Some(ws) => Either::Left(handle_websocket(req, ws, upgrade.clone(), client)),
        None => Either::Right(handle(req, client)),
    }
}

/// Returns the `Sec-WebSocket-Accept` value for a client key (RFC 6455 §4.2.2).
#[expect(
    clippy::expect_used,
    reason = "derive_accept_key returns base64 (A-Z a-z 0-9 + / =), which is visible ASCII, and every such string is a valid header value: the parse cannot fail"
)]
fn accept_header(key: &HeaderValue) -> HeaderValue {
    let accept = tungstenite::handshake::derive_accept_key(key.as_bytes());
    HeaderValue::from_str(&accept).expect("base64 output of derive_accept_key is a valid header value")
}

#[inline]
fn ws_upgrade_response(key: &HeaderValue) -> Response<ResponseBody> {
    let mut resp = Response::new(Full::new(Bytes::new()));
    *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    let headers = resp.headers_mut();
    headers.insert(hyper::header::UPGRADE, HeaderValue::from_static("websocket"));
    headers.insert(hyper::header::CONNECTION, HeaderValue::from_static("Upgrade"));
    headers.insert(hyper::header::SEC_WEBSOCKET_ACCEPT, accept_header(key));
    full_body(resp)
}

/// Answers a WebSocket upgrade request with the 101 or the response that refused it, finished like
/// any other response (CORS, access log).
async fn handle_websocket(
    mut req: Request<Incoming>,
    ws: WsRouteMatch,
    upgrade_to: WsUpgrade,
    peer: mipyre_transport::driver::Peer,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let site = &upgrade_to.site;
    let timer = AccessTimer::start(site);
    let origin = cors_origin(site, req.headers());
    let client = client_of(peer, req.headers(), site);
    if let Some(miss) = refused_site(&req, true, site) {
        let resp = full_body(site_miss_response(miss).map(Full::new));
        return Ok(finish(
            resp,
            site,
            req.method(),
            req.uri().path(),
            Requester::Headers(req.headers(), &client),
            timer,
            origin,
            ResponseSource::WebSocket,
        ));
    }
    let upgrade = hyper::upgrade::on(&mut req);
    let (parts, _body) = req.into_parts();
    let request_id = RequestId::of(&parts.headers, site.rules.settings.request_id_header.as_ref());
    let head = RequestHead {
        parts,
        params: ws.params,
        client,
        request_id,
    };
    let label = head.label();
    let client = head.client;
    let resp = match WsHandshake::parse(&head.parts) {
        Ok(handshake) => {
            let engines = upgrade_to.engines(ws.route);
            answer_upgrade(site, engines, ws.route, handshake, head, upgrade, &label).await
        }
        Err(e) => {
            tracing::debug!(target: "mipyre::server", request_id = %label.id,
                path = label.uri.path(), error = %e, "WebSocket handshake refused");
            full_body(e.response().map(Full::new))
        }
    };
    Ok(finish(
        resp,
        site,
        &label.method,
        label.uri.path(),
        Requester::Given(&label.id, &client),
        timer,
        origin,
        ResponseSource::WebSocket,
    ))
}

/// The slot is released only once the engine has let go of the connection, on every path: the 101,
/// a hook's rejection, or the handshake giving up while a hook still runs.
struct HandlerTask {
    ended: oneshot::Receiver<std::convert::Infallible>,
    slot: ConnectionSlot,
}

impl HandlerTask {
    async fn ended(self) {
        let HandlerTask { ended, slot } = self;
        if let Ok(never) = ended.await {
            match never {}
        }
        drop(slot);
    }
}

async fn answer_upgrade(
    site: &Arc<Site>,
    (engines, group): (Option<&WsEngines>, WorkerGroup),
    route: WsRouteId,
    handshake: WsHandshake,
    head: RequestHead,
    upgrade: hyper::upgrade::OnUpgrade,
    label: &RequestLabel,
) -> Response<ResponseBody> {
    let limits = site.rules.settings.limits.ws;
    let Some(slot) = ConnectionSlot::try_acquire(&site.ws_connections, limits.max_connections.get()) else {
        tracing::debug!(target: "mipyre::server", path = label.uri.path(), max_connections = limits.max_connections.get(),
            "WebSocket connection limit reached; answered 503");
        return fail(
            refuse(UnansweredError::Overloaded(Capacity::WebsocketConnections)),
            label,
            &site.counters,
        );
    };

    // The hooks run in the connection's task before the 101 and the handler after it.
    let (ends, handler_ends) = connection_channels(limits.max_message_bytes);
    let (answer_tx, answer_rx) = oneshot::channel();
    let (ended_tx, ended) = oneshot::channel();
    let accept_encoding = AcceptEncoding::of(&head, site.rules.settings.compression.as_ref());
    let connection = WsConnection {
        route,
        request: exchange::Request::new(head, RequestBody::Buffered(Bytes::new())),
        ends: handler_ends,
        hooks_answer: answer_tx,
        ended: ended_tx,
    };
    let handed = engines.map_or(Err(NoLiveEngine), |engines| engines.hand(connection));
    if let Err(NoLiveEngine) = handed {
        return fail(refuse(UnansweredError::WorkersGone(group)), label, &site.counters);
    }
    let task = HandlerTask { ended, slot };

    let answer = reply_or_lost(answer_rx, ReplyHolder::WebsocketHandler);
    let budget = site.rules.settings.limits.request_budget;
    match await_reply(budget, answer).await.map(Result::transpose) {
        Ok(None) => {}
        Ok(Some(response)) => {
            task.ended().await;
            return http_response(response, &accept_encoding, &site.counters).await;
        }
        Err(e) => {
            // The task sees the verdict go unread and ends without running the handler; the slot
            // is held until then.
            tokio::spawn(task.ended());
            return fail(e, label, &site.counters);
        }
    }

    let counters = Arc::clone(&site.counters.run);
    tokio::spawn(async move {
        match upgrade.await {
            Ok(upgraded) => {
                let ws_stream = WebSocketStream::from_raw_socket(
                    hyper_util::rt::TokioIo::new(upgraded),
                    tungstenite::protocol::Role::Server,
                    Some(tungstenite_config(limits)),
                )
                .await;
                run_ws_connection(ws_stream, ends, limits, &counters).await;
            }
            Err(e) => {
                // The client leaving right after the 101 is its doing; anything else is ours to see.
                if mipyre_http::io::events::peer_went_away(&e) {
                    mipyre_transport::stream::events::count(mipyre_transport::stream::events::StreamEvent::PeerGone);
                    tracing::debug!(target: "mipyre::server", error = %e, "WebSocket client left after the upgrade");
                } else {
                    tracing::error!(target: "mipyre::server", error = %e, "WebSocket upgrade error");
                }
                // Dropping the ends makes the handler's recv() return None.
                drop(ends);
            }
        }
        task.ended().await;
    });

    ws_upgrade_response(handshake.key())
}

// ---------------------------------------------------------------------------
// The message pump runs between the socket and the handler's queues.
// ---------------------------------------------------------------------------

async fn run_ws_connection<S>(
    ws_stream: WebSocketStream<S>,
    ends: ConnectionEnds,
    limits: WsLimits,
    counters: &ServerCounters,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (ws_sink, ws_source) = ws_stream.split();
    let mut io = ConnectionIo {
        sink: ws_sink,
        source: ws_source,
        ends,
    };
    let close_frame = pump_messages(&mut io, limits, counters).await;

    let ConnectionIo { sink, source, ends } = io;
    // Python's pending recv returns None, and what it queued to send is dropped, its budget freed
    // and its next send refused, before the close, which can linger.
    drop(ends);
    close_connection(sink, source, close_frame).await;
}

type WsSink<S> = SplitSink<WebSocketStream<S>, Message>;
type WsSource<S> = SplitStream<WebSocketStream<S>>;

struct ConnectionIo<S> {
    sink: WsSink<S>,
    source: WsSource<S>,
    ends: ConnectionEnds,
}

/// Moves messages between the socket and Python until either side ends, and returns the close
/// frame the server owes the client if the server is the one closing.
///
/// The channels are unbounded in count, but every queued message holds its bytes from a
/// `ByteBudget`, so each direction is bounded in memory.
async fn pump_messages<S>(io: &mut ConnectionIo<S>, limits: WsLimits, counters: &ServerCounters) -> Option<CloseFrame>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut heard = Instant::now();
    let mut pinged = None;
    let tick = tokio::time::sleep_until(heard + limits.keepalive.ping_after.get());
    tokio::pin!(tick);
    loop {
        tokio::select! {
            msg = io.source.next() => {
                heard = Instant::now();
                pinged = None;
                let queued = match msg {
                    Some(Ok(Message::Text(text))) => WebSocketMessage::Text(text.to_string()),
                    Some(Ok(Message::Binary(data))) => WebSocketMessage::Binary(data.into()),
                    // tungstenite queues the Pong for a Ping itself and flushes it with
                    // the next read or write.
                    Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                    Some(Ok(Message::Close(_))) | None => return None,
                    Some(Err(tungstenite::Error::Capacity(e))) => {
                        tracing::warn!(target: "mipyre::server", error = %e,
                            max_message_bytes = limits.max_message_bytes.get(),
                            "WebSocket message over max_websocket_message_size; closing with 1009");
                        return Some(CloseFrame { code: CloseCode::Size, reason: e.to_string().into() });
                    }
                    Some(Err(e)) => {
                        tracing::warn!(target: "mipyre::server", error = %e, "WebSocket read error");
                        return None;
                    }
                };
                if forward(&io.ends.incoming, queued).await.is_break() {
                    return None;
                }
            }
            queued = io.ends.outgoing.recv() => {
                // The queue slot's budget is released once the message is on the wire.
                let QueueEntry { msg, _budget } = queued?;
                if let Err(e) = io.sink.send(into_message(msg)).await {
                    report_send_failure(&e);
                    return None;
                }
            }
            () = &mut tick => {
                let now = Instant::now();
                match heartbeat(limits.keepalive, heard, pinged, now) {
                    Heartbeat::Quiet { until } => tick.as_mut().reset(until),
                    Heartbeat::Ping => {
                        if let Err(e) = io.sink.send(Message::Ping(Bytes::new())).await {
                            report_send_failure(&e);
                            return None;
                        }
                        pinged = Some(now);
                        tick.as_mut().reset(now + limits.keepalive.pong_within.get());
                    }
                    Heartbeat::Gone => {
                        counters.ws_keepalive_timeout();
                        tracing::debug!(target: "mipyre::server", keepalive = ?limits.keepalive,
                            "WebSocket client did not answer a ping in time; closing with 1011");
                        return Some(CloseFrame { code: CloseCode::Error, reason: "keepalive ping timeout".into() });
                    }
                }
            }
        }
    }
}

fn report_send_failure(e: &tungstenite::Error) {
    match write_stalled(e) {
        Some(stalled) => report_write_stall(stalled.limit),
        None => tracing::debug!(target: "mipyre::server", error = %e, "WebSocket send failed"),
    }
}

/// Says what a connection does next about a client last heard from at `heard`; `pinged` is when
/// the server pinged it, if no frame has come since.
#[derive(Debug, PartialEq, Eq)]
enum Heartbeat {
    Quiet { until: Instant },
    Ping,
    Gone,
}

fn heartbeat(keepalive: WsKeepalive, heard: Instant, pinged: Option<Instant>, now: Instant) -> Heartbeat {
    match pinged {
        None if now < heard + keepalive.ping_after.get() => Heartbeat::Quiet {
            until: heard + keepalive.ping_after.get(),
        },
        None => Heartbeat::Ping,
        Some(at) if now < at + keepalive.pong_within.get() => Heartbeat::Quiet {
            until: at + keepalive.pong_within.get(),
        },
        Some(_) => Heartbeat::Gone,
    }
}

fn into_message(msg: WebSocketMessage) -> Message {
    match msg {
        WebSocketMessage::Text(s) => Message::Text(s.into()),
        WebSocketMessage::Binary(b) => Message::Binary(b.into()),
    }
}

/// Queues a client message for Python. While the byte budget is spent this waits, which stops the
/// select arm from reading the socket, so flow control reaches the client through the TCP window.
async fn forward(incoming: &QueueSender, msg: WebSocketMessage) -> ControlFlow<()> {
    let permit = incoming.budget.reserve(msg.len()).await;
    match incoming.tx.send(QueueEntry { msg, _budget: permit }) {
        Ok(()) => ControlFlow::Continue(()),
        // The Python side is gone.
        Err(_) => ControlFlow::Break(()),
    }
}

/// How long a closing connection waits for the peer to close its side.
const CLOSE_LINGER: Duration = Duration::from_secs(2);
const LINGER_CHUNK_BYTES: usize = 8 * 1024;

/// Closes so that the client reliably sees the close frame, then discards input until the peer
/// closes, for at most `CLOSE_LINGER`. Dropping a socket with unread input, such as the rest of a
/// message just refused as too big, makes the kernel send RST, which can destroy the close frame
/// before the client reads it.
///
/// Failures here mean the peer is already gone, the normal end of many connections, so they log
/// at debug.
async fn close_connection<S>(mut sink: WsSink<S>, source: WsSource<S>, frame: Option<CloseFrame>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    if let Some(frame) = frame {
        if let Err(e) = sink.send(Message::Close(Some(frame))).await {
            tracing::debug!(target: "mipyre::server", error = %e, "WebSocket close frame not sent");
        }
    }
    if let Err(e) = sink.close().await {
        tracing::debug!(target: "mipyre::server", error = %e, "WebSocket close failed");
    }

    let mut stream = match sink.reunite(source) {
        Ok(stream) => stream,
        Err(e) => {
            // Both halves come from one split, so they always reunite: this is a broken invariant,
            // and it is logged as one, not as the peer having left.
            tracing::error!(target: "mipyre::server", error = %e, "WebSocket halves did not reunite");
            return;
        }
    };
    let io = stream.get_mut();
    if let Err(e) = io.shutdown().await {
        tracing::debug!(target: "mipyre::server", error = %e, "WebSocket write shutdown failed");
    }
    if tokio::time::timeout(CLOSE_LINGER, discard_until_eof(io)).await.is_err() {
        tracing::debug!(target: "mipyre::server", linger = ?CLOSE_LINGER,
            "WebSocket peer did not close in time; dropping the connection");
    }
}

async fn discard_until_eof<R: tokio::io::AsyncRead + Unpin>(io: &mut R) {
    let mut scratch = [0u8; LINGER_CHUNK_BYTES];
    loop {
        // EOF or a read error both mean the peer has gone, which is what we wait for.
        match io.read(&mut scratch).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mipyre_http::plan::limits::{PingAfter, PongWithin};

    #[test]
    fn connection_slots_are_bounded_and_released() {
        // A fresh run's counter starts at 0.
        let open = OpenConnections::default();
        let first = ConnectionSlot::try_acquire(&open, 2).expect("slot 1");
        let second = ConnectionSlot::try_acquire(&open, 2).expect("slot 2");
        assert!(ConnectionSlot::try_acquire(&open, 2).is_none());
        drop(first);
        let third = ConnectionSlot::try_acquire(&open, 2).expect("released slot");
        drop((second, third));
    }

    fn keepalive() -> WsKeepalive {
        WsKeepalive {
            ping_after: PingAfter::from_secs(20.0).unwrap(),
            pong_within: PongWithin::from_secs(5.0).unwrap(),
        }
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn a_client_heard_from_is_pinged_once_ping_after_has_passed() {
        let heard = Instant::now();
        let due = heard + keepalive().ping_after.get();
        assert_eq!(
            heartbeat(keepalive(), heard, None, due - ms(1)),
            Heartbeat::Quiet { until: due }
        );
        assert_eq!(heartbeat(keepalive(), heard, None, due), Heartbeat::Ping);
        assert_eq!(heartbeat(keepalive(), heard, None, due + ms(1)), Heartbeat::Ping);
    }

    #[test]
    fn a_pinged_client_is_gone_once_pong_within_has_passed() {
        let heard = Instant::now();
        let pinged = heard + keepalive().ping_after.get();
        let deadline = pinged + keepalive().pong_within.get();
        assert_eq!(
            heartbeat(keepalive(), heard, Some(pinged), deadline - ms(1)),
            Heartbeat::Quiet { until: deadline }
        );
        assert_eq!(heartbeat(keepalive(), heard, Some(pinged), deadline), Heartbeat::Gone);
        assert_eq!(
            heartbeat(keepalive(), heard, Some(pinged), deadline + ms(1)),
            Heartbeat::Gone
        );
    }

    /// Tests the message pump under the real connection stack, whose write timeout bounds a send
    /// to a client that stopped reading. The pipe is in memory and the clock paused, so the
    /// timeouts elapse without the test waiting for them.
    #[cfg(not(sync_model))]
    mod pump {
        use super::*;

        use std::cell::Cell;
        use std::time::Duration;

        use hyper::service::service_fn;
        use tokio::io::DuplexStream;
        use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
        use tokio::time::Instant;

        use crate::conn::{drive_connection, ConnectionTimeouts, LocalExec};
        use mipyre_http::plan::limits::MessageByteLimit;
        use mipyre_runtime::exchange::websocket::ByteBudget;
        use mipyre_transport::stream::events::{self, StreamEvent};
        use mipyre_transport::stream::stall::WRITE_TIMEOUT;

        /// Each direction of the pipe holds this much, as a socket's buffer does.
        const PIPE_BYTES: usize = 64 * 1024;
        const MAX_MESSAGE_BYTES: usize = 256 * 1024;

        const UPGRADE: &str = "GET /ws HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
                               Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";

        struct Connection {
            client: DuplexStream,
            /// The handler's end of the outgoing queue.
            outgoing: UnboundedSender<QueueEntry>,
            budget: ByteBudget,
            /// It completes once the pump has ended and the connection is closed.
            pumped: oneshot::Receiver<()>,
            counters: Arc<ServerCounters>,
        }

        fn limits() -> WsLimits {
            WsLimits {
                max_message_bytes: MessageByteLimit::new(MAX_MESSAGE_BYTES as i64).unwrap(),
                keepalive: super::keepalive(),
                ..WsLimits::DEFAULT
            }
        }

        /// Returns limits under which a client that never sends a frame is not pinged within any
        /// test here, for tests of the write side alone.
        fn limits_without_pings() -> WsLimits {
            WsLimits {
                keepalive: WsKeepalive {
                    ping_after: PingAfter::from_secs(24.0 * 60.0 * 60.0).unwrap(),
                    ..limits().keepalive
                },
                ..limits()
            }
        }

        async fn upgraded() -> Connection {
            upgraded_under(limits()).await
        }

        /// Serves one connection with `drive_connection`, upgrades it as `answer_upgrade` does, and
        /// reads off the 101.
        async fn upgraded_under(limits: WsLimits) -> Connection {
            let (mut client, server) = tokio::io::duplex(PIPE_BYTES);
            let (handler_tx, connection_rx) = unbounded_channel();
            let (ends, _handler) = connection_channels(limits.max_message_bytes);
            // The queue the handler sends on is the test's own, with a budget of the same size.
            let budget = connection_channels(limits.max_message_bytes).0.incoming.budget;
            let ends = Cell::new(Some(ConnectionEnds {
                incoming: ends.incoming,
                outgoing: connection_rx,
            }));
            let (pumped_tx, pumped) = oneshot::channel();
            let pumped_tx = Cell::new(Some(pumped_tx));
            let counters = Arc::new(ServerCounters::new(false));
            let pump_counters = Arc::clone(&counters);
            let service = service_fn(move |mut req: Request<Incoming>| {
                let upgrade = hyper::upgrade::on(&mut req);
                let ends = ends.take().unwrap();
                let pumped_tx = pumped_tx.take().unwrap();
                let counters = Arc::clone(&pump_counters);
                tokio::task::spawn_local(async move {
                    let ws = WebSocketStream::from_raw_socket(
                        hyper_util::rt::TokioIo::new(upgrade.await.unwrap()),
                        tungstenite::protocol::Role::Server,
                        Some(tungstenite_config(limits)),
                    )
                    .await;
                    run_ws_connection(ws, ends, limits, &counters).await;
                    pumped_tx.send(()).unwrap();
                });
                let key = HeaderValue::from_static("dGhlIHNhbXBsZSBub25jZQ==");
                std::future::ready(Ok::<_, hyper::Error>(ws_upgrade_response(&key)))
            });
            tokio::task::spawn_local(drive_connection(
                server,
                service,
                LocalExec,
                ConnectionTimeouts::DEFAULT,
                mipyre_http::io::clock::Clock::process(),
                std::future::pending(),
            ));
            client.write_all(UPGRADE.as_bytes()).await.unwrap();
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                head.push(client.read_u8().await.unwrap());
            }
            assert!(head.starts_with(b"HTTP/1.1 101"), "{}", String::from_utf8_lossy(&head));
            Connection {
                client,
                outgoing: handler_tx,
                budget,
                pumped,
                counters,
            }
        }

        impl Connection {
            async fn send(&self, len: usize) {
                let permit = self.budget.reserve(len).await;
                let msg = WebSocketMessage::Binary(vec![7; len]);
                self.outgoing
                    .send(QueueEntry { msg, _budget: permit })
                    .unwrap_or_else(|_| panic!("the connection took no message"));
            }

            /// Waits as a handler waiting to write does: until a message of the largest size
            /// would fit, which is once nothing is queued.
            fn room(&self) -> impl Future<Output = ()> + 'static {
                let budget = self.budget.clone();
                async move { drop(budget.reserve(MAX_MESSAGE_BYTES).await) }
            }
        }

        fn run(test: impl Future<Output = ()>) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .start_paused(true)
                .build()
                .unwrap();
            tokio::task::LocalSet::new().block_on(&rt, test);
        }

        /// Checks the stall is told from other send failures: a stall is warned about (throttled),
        /// another failure is a debug line.
        #[test]
        fn a_client_that_never_reads_is_disconnected_and_the_waiting_handler_wakes() {
            let log = mipyre_runtime::testkit::captured_log_from(tracing::Level::DEBUG, never_read);
            assert!(!log.contains("WebSocket send failed"), "{log}");
        }

        fn never_read() {
            run(async {
                let c = upgraded().await;
                let stalls = events::total(StreamEvent::WriteStalled);
                let half = MAX_MESSAGE_BYTES / 2 - 64;
                c.send(half).await;
                c.send(half).await;
                let started = Instant::now();

                c.room().await;

                let woke = started.elapsed();
                assert!(woke >= WRITE_TIMEOUT, "woke after {woke:?}");
                assert!(
                    woke < WRITE_TIMEOUT + CLOSE_LINGER,
                    "woke after {woke:?}, not before the close lingered"
                );
                c.pumped.await.unwrap();
                assert!(c.outgoing.is_closed(), "a send after the stall is refused");
                assert!(events::total(StreamEvent::WriteStalled) > stalls);
                drop(c.client);
            });
        }

        #[test]
        fn a_send_the_write_timeout_failed_is_a_write_stall_and_another_timeout_is_not() {
            let limit = Duration::from_secs(10);
            let stalled = tungstenite::Error::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                mipyre_transport::stream::stall::WriteStallError { limit },
            ));
            assert_eq!(write_stalled(&stalled).map(|s| s.limit), Some(limit));
            let timed_out = tungstenite::Error::Io(std::io::Error::from(std::io::ErrorKind::TimedOut));
            assert!(write_stalled(&timed_out).is_none());
        }

        #[test]
        fn a_client_that_reads_slowly_but_keeps_reading_is_not_disconnected() {
            run(async {
                let mut c = upgraded_under(limits_without_pings()).await;
                c.send(MAX_MESSAGE_BYTES).await;
                let room = tokio::task::spawn_local(c.room());
                // A server frame of this size has a 10-byte header.
                let mut left = MAX_MESSAGE_BYTES + 10;
                let mut chunk = [0u8; 4 * 1024];
                let started = Instant::now();
                while left > 0 {
                    tokio::time::sleep(WRITE_TIMEOUT - Duration::from_millis(1)).await;
                    let read = c.client.read(&mut chunk).await.unwrap();
                    assert!(read > 0, "the connection closed with {left} bytes unread");
                    left -= read;
                }
                assert!(started.elapsed() > 4 * WRITE_TIMEOUT, "the read was not slow");
                room.await.unwrap();
                assert!(!c.outgoing.is_closed(), "the connection is still open");
                assert!(c.pumped.try_recv().is_err(), "the pump still runs");
            });
        }

        /// A masked Pong with no payload, as a client sends it.
        const CLIENT_PONG: [u8; 6] = [0x8A, 0x80, 1, 2, 3, 4];
        const SERVER_PING: [u8; 2] = [0x89, 0x00];

        fn unheard_for() -> Duration {
            limits().keepalive.ping_after.get() + limits().keepalive.pong_within.get()
        }

        async fn read_frame_head(client: &mut DuplexStream) -> [u8; 2] {
            let mut head = [0u8; 2];
            client.read_exact(&mut head).await.unwrap();
            head
        }

        #[test]
        fn a_client_that_sends_nothing_is_pinged_then_closed_with_1011() {
            run(async {
                let mut c = upgraded().await;

                tokio::time::sleep(unheard_for() - Duration::from_millis(1)).await;
                assert!(!c.outgoing.is_closed(), "closed before the pong was due");
                tokio::time::sleep(Duration::from_millis(2)).await;
                assert!(c.outgoing.is_closed(), "still open after the pong was due");

                assert_eq!(read_frame_head(&mut c.client).await, SERVER_PING);
                let close = read_frame_head(&mut c.client).await;
                assert_eq!(close[0], 0x88, "{close:?}");
                let mut code = [0u8; 2];
                c.client.read_exact(&mut code).await.unwrap();
                assert_eq!(u16::from_be_bytes(code), 1011);
                assert_eq!(c.counters.read().ws_keepalive_timeouts, 1);
            });
        }

        #[test]
        fn a_client_that_answers_the_ping_in_time_stays() {
            run(async {
                let mut c = upgraded().await;
                tokio::time::sleep(unheard_for() - Duration::from_millis(1)).await;
                assert_eq!(read_frame_head(&mut c.client).await, SERVER_PING);
                c.client.write_all(&CLIENT_PONG).await.unwrap();

                tokio::time::sleep(limits().keepalive.ping_after.get() - Duration::from_millis(1)).await;
                assert!(!c.outgoing.is_closed(), "closed although the pong came in time");
                assert!(c.pumped.try_recv().is_err(), "the pump still runs");
                assert_eq!(c.counters.read().ws_keepalive_timeouts, 0);
            });
        }

        #[test]
        fn a_client_whose_frames_come_on_time_is_never_pinged() {
            run(async {
                let mut c = upgraded().await;
                for _ in 0..5 {
                    tokio::time::sleep(limits().keepalive.ping_after.get() - Duration::from_millis(1)).await;
                    c.client.write_all(&CLIENT_PONG).await.unwrap();
                }
                tokio::time::sleep(limits().keepalive.ping_after.get() - Duration::from_millis(1)).await;
                assert!(!c.outgoing.is_closed(), "closed although every frame came on time");
                let mut byte = [0u8; 1];
                let read = tokio::time::timeout(Duration::ZERO, c.client.read(&mut byte)).await;
                assert!(read.is_err(), "the server sent {byte:?}, not nothing");
            });
        }

        #[test]
        fn a_frame_trickled_a_byte_at_a_time_is_cut_off_when_the_pong_is_due() {
            run(async {
                let mut c = upgraded().await;
                let payload_bytes = 64u8;
                c.client
                    .write_all(&[0x82, 0x80 | payload_bytes, 1, 2, 3, 4])
                    .await
                    .unwrap();
                let started = Instant::now();
                let drip = limits().keepalive.pong_within.get() / 2;
                for _ in 0..payload_bytes {
                    tokio::time::sleep(drip).await;
                    if c.outgoing.is_closed() {
                        break;
                    }
                    c.client.write_all(&[0]).await.unwrap();
                }
                let closed_after = started.elapsed();
                assert!(c.outgoing.is_closed(), "the trickle was never cut off");
                assert!(closed_after >= unheard_for(), "closed after {closed_after:?}");
                assert!(closed_after <= unheard_for() + drip, "closed after {closed_after:?}");
            });
        }
    }
}
