//! Serves WebSocket connections. A connection is one task on the thread that accepted it: it reads
//! a frame, calls the route's handler with it where the route runs, and writes what the call
//! queued, one message at a time (`mipyre_runtime::websocket`).
//!
//! It owns the socket, frames, ping and pong, the keepalive and the close handshake; it does not
//! know the handler, the `ws` object, or the interpreter, and holds only the connection's session
//! key. Every resource a client can make the server hold is bounded: connections by
//! `max_connections`; message and frame size by `max_message_bytes`; what is queued to send by
//! one message's budget (`Outbox`); what is read by the TCP window, since the socket is not read
//! during a call; and how long a client may go unheard by `keepalive`.

use crate::sync::{AtomicUsize, Ordering};
use std::collections::VecDeque;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::Either;
use futures_util::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::HeaderValue;
use hyper::{Request, Response, StatusCode};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::Instant;
use tokio_tungstenite::WebSocketStream;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tungstenite::Message;

use crate::conn::{report_write_stall, write_stalled};
use crate::pipeline::{client_of, cors_origin, fail, finish, refused_site, AccessTimer, Requester, ResponseSource};
use crate::respond::http_response;
use crate::site::Site;
use crate::tpc::TpcThread;
use crate::ServerCounters;
use mipyre_http::io::body::{full_body, ResponseBody};
use mipyre_http::io::request_head::{RequestHead, RequestLabel};
use mipyre_http::plan::limits::{WsKeepalive, WsLimits};
use mipyre_http::plan::request_id::RequestId;
use mipyre_http::plan::response::site_miss_response;
use mipyre_http::plan::route::{Params, WsPlace, WsRouteId};
use mipyre_http::plan::ws::WsHandshake;
use mipyre_runtime::exchange::error::WorkerGroup;
use mipyre_runtime::exchange::{
    self, refuse, Capacity, HandlerError, Outbox, RequestBody, Taken, UnansweredError, WsClose, WsMessage,
};
use mipyre_runtime::{MainThreads, WsEvent, WsSession, WsUpgradeAnswer};
use mipyre_tpc::{LocalSpawner, LocalStop, LocalTasks};

/// A connection reads its socket into a buffer of this size. tungstenite zero-fills the buffer's
/// whole spare capacity before every read, so its default of 128 KiB was cleared for each small
/// message and held per connection; a frame longer than this grows the buffer to fit it.
const READ_BUFFER_BYTES: usize = 4 * 1024;

/// Returns tungstenite's settings, which refuse any message or frame past `max_message_bytes`.
fn tungstenite_config(limits: WsLimits) -> WebSocketConfig {
    let max = Some(limits.max_message_bytes.as_usize());
    WebSocketConfig::default()
        .read_buffer_size(READ_BUFFER_BYTES)
        .max_message_size(max)
        .max_frame_size(max)
}

/// Counts the WebSocket connections one server run has open.
#[derive(Clone, Default)]
pub struct OpenConnections(Arc<AtomicUsize>);

/// It is held until the connection's session is released, and released on drop.
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
// Where a connection's calls run.
// ---------------------------------------------------------------------------

/// Why a call into a connection's handlers did not complete.
enum CallFailure {
    Raised(HandlerError),
    /// The thread's worker retired, and takes no more calls.
    WorkerRetired,
}

/// Runs an open connection's calls where its route runs, one at a time; it does not know the
/// socket. `Session` is the connection's key to what the calls run on.
trait Callee {
    type Session;

    /// Calls the handler `event` is for; what it returns is queued on the connection's outbox.
    async fn call(&self, route: WsRouteId, session: &Self::Session, event: WsEvent) -> Result<(), CallFailure>;

    /// Lets go of the session, calling `on_close` first when `closed` says how the connection
    /// ended; `None` is a connection whose `on_connect` never ran.
    fn end(&self, route: WsRouteId, session: Self::Session, closed: Option<&WsClose>, label: &RequestLabel);
}

/// Opens the sessions of an interpreter's connections.
trait Opener: Callee<Session = WsSession> {
    /// Runs the hooks on the upgrade and, when none answers, opens the connection's session. The
    /// connection's slot comes back with the answer; on a failure it is held until the hooks have
    /// stopped running.
    async fn open(&self, upgrade: Upgrade) -> Result<(WsUpgradeAnswer, ConnectionSlot), HandlerError>;
}

/// What opening a connection's session takes.
struct Upgrade {
    route: WsRouteId,
    request: exchange::Request,
    outbox: Arc<Outbox>,
    slot: ConnectionSlot,
    label: RequestLabel,
}

/// Runs the calls inline on this TPC thread's worker, as its `def` routes are.
struct Inline(Rc<TpcThread>);

impl Opener for Inline {
    async fn open(&self, upgrade: Upgrade) -> Result<(WsUpgradeAnswer, ConnectionSlot), HandlerError> {
        let Upgrade {
            route,
            request,
            outbox,
            slot,
            label: _,
        } = upgrade;
        let worker = self
            .0
            .worker
            .as_ref()
            .ok_or_else(|| refuse(UnansweredError::WorkersGone(WorkerGroup::Subinterp)))?;
        let lease = worker
            .lease()
            .map_err(|why| refuse(UnansweredError::WorkerRetired(why)))?;
        lease.ws_open(route, request, outbox).map(|opening| (opening, slot))
    }
}

impl Callee for Inline {
    type Session = WsSession;

    async fn call(&self, route: WsRouteId, session: &WsSession, event: WsEvent) -> Result<(), CallFailure> {
        let Some(Ok(lease)) = self.0.worker.as_ref().map(|w| w.lease()) else {
            return Err(CallFailure::WorkerRetired);
        };
        lease.ws_call(route, session, event).map_err(CallFailure::Raised)
    }

    fn end(&self, route: WsRouteId, session: WsSession, closed: Option<&WsClose>, label: &RequestLabel) {
        let Some(worker) = self.0.worker.as_ref() else {
            return;
        };
        match (closed, worker.lease()) {
            (Some(closed), Ok(lease)) => {
                if let Err(e) = lease.ws_close(route, session, closed) {
                    drop(e.log(label));
                }
            }
            // A retired worker runs no more Python, so `on_close` is not called.
            (_, Err(_)) | (None, Ok(_)) => worker.ws_release(session),
        }
    }
}

/// Runs the calls on main's `def` threads and waits for each.
struct OnMain {
    main: Arc<MainThreads>,
    budget: mipyre_http::plan::limits::RequestBudget,
}

impl Opener for OnMain {
    async fn open(&self, upgrade: Upgrade) -> Result<(WsUpgradeAnswer, ConnectionSlot), HandlerError> {
        let Upgrade {
            route,
            request,
            outbox,
            slot,
            label,
        } = upgrade;
        let mut opened = Box::pin(self.main.ws_open(route, request, outbox).map_err(refuse)?);
        match tokio::time::timeout(self.budget.get(), &mut opened).await {
            Ok(opened) => opened.map(|opening| (opening, slot)),
            Err(_) => {
                // A hook past the budget still holds a main thread, so the slot is held until it
                // ends, and a session it opened then is let go of.
                let main = Arc::clone(&self.main);
                tokio::spawn(async move {
                    if let Ok(WsUpgradeAnswer::Accepted(session)) = opened.await {
                        hand_back(&main, route, session, None, &label);
                    }
                    drop(slot);
                });
                Err(refuse(UnansweredError::TimedOut(self.budget.get())))
            }
        }
    }
}

/// Hands a session back to main, which calls `on_close` when `closed` says how the connection
/// ended.
fn hand_back(main: &MainThreads, route: WsRouteId, session: WsSession, closed: Option<WsClose>, label: &RequestLabel) {
    if let Err(e) = main.ws_close(route, session, closed, label.clone()) {
        tracing::warn!(target: "mipyre::server", request_id = %label.id, error = %e,
            "a WebSocket session could not be handed back to main; it is held until the server stops");
    }
}

impl Callee for OnMain {
    type Session = WsSession;

    async fn call(&self, route: WsRouteId, session: &WsSession, event: WsEvent) -> Result<(), CallFailure> {
        let called = self
            .main
            .ws_call(route, session, event)
            .map_err(|e| CallFailure::Raised(refuse(e)))?;
        called.await.map_err(CallFailure::Raised)
    }

    fn end(&self, route: WsRouteId, session: WsSession, closed: Option<&WsClose>, label: &RequestLabel) {
        hand_back(&self.main, route, session, closed.cloned(), label);
    }
}

// ---------------------------------------------------------------------------
// The upgrade handshake is answered here.
// ---------------------------------------------------------------------------

struct WsRouteMatch {
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

/// What a TPC connection answers WebSocket upgrades with: its thread, whose worker runs the
/// worker routes' calls; the thread's stop, which closes its connections with 1001; and its
/// tasks, among which an upgraded connection runs so the thread's drain waits for it.
#[derive(Clone)]
pub struct WsUpgrade {
    thread: Rc<TpcThread>,
    stop: LocalStop,
    tasks: LocalSpawner,
}

/// Returns what a TPC connection answers WebSocket upgrades with, or `None` when the site has no
/// WebSocket route: without one a connection's service has no upgrade branch, and it is driven
/// without a lock.
pub fn upgrade_target(thread: &Rc<TpcThread>, stop: &LocalStop, tasks: &LocalTasks) -> Option<WsUpgrade> {
    thread.site.rules.routes.has_websocket_routes().then(|| WsUpgrade {
        thread: Rc::clone(thread),
        stop: stop.clone(),
        tasks: tasks.spawner(),
    })
}

/// Upgrades `req` when it asks for one of the site's WebSocket routes, spawning the connection's
/// task on this thread, and hands it to `handle` otherwise.
pub(crate) fn upgrade_or<H, Fut>(
    req: Request<Incoming>,
    upgrade: &WsUpgrade,
    client: mipyre_transport::driver::Peer,
    handle: &H,
) -> Either<impl Future<Output = Result<Response<ResponseBody>, hyper::Error>> + 'static, Fut>
where
    H: Fn(Request<Incoming>, mipyre_transport::driver::Peer) -> Fut,
{
    let Some(ws) = wants_websocket(&req, &upgrade.thread.site) else {
        return Either::Right(handle(req, client));
    };
    let upgrade = upgrade.clone();
    Either::Left(async move {
        let site = &upgrade.thread.site;
        let stop = upgrade.stop.clone();
        let response = match site.rules.routes.ws_place(ws.route) {
            WsPlace::Worker => {
                let callee = Inline(Rc::clone(&upgrade.thread));
                let (response, accepted) = handle_websocket(req, ws, site, Some(callee), client).await;
                if let Some(accepted) = accepted {
                    upgrade
                        .tasks
                        .spawn_local(async move { accepted.run(stop.stopped()).await });
                }
                response
            }
            WsPlace::Main => {
                let callee = upgrade.thread.main.as_ref().map(|main| on_main(main, site));
                let (response, accepted) = handle_websocket(req, ws, site, callee, client).await;
                if let Some(accepted) = accepted {
                    upgrade
                        .tasks
                        .spawn_local(async move { accepted.run(stop.stopped()).await });
                }
                response
            }
        };
        Ok(response)
    })
}

/// What the multi-thread server (`MIPYRE_TPC=0`) answers WebSocket upgrades with. It runs only
/// `runtime="main"` routes: a worker route needs the worker of the thread that holds the
/// connection, and the server refuses to start with one.
#[derive(Clone)]
pub struct MainWsUpgrade {
    site: Arc<Site>,
    main: Option<Arc<MainThreads>>,
}

/// Returns what the multi-thread server answers WebSocket upgrades with, or `None` when the site
/// has no WebSocket route.
pub fn main_upgrade_target(site: &Arc<Site>, main: Option<&Arc<MainThreads>>) -> Option<MainWsUpgrade> {
    site.rules.routes.has_websocket_routes().then(|| MainWsUpgrade {
        site: Arc::clone(site),
        main: main.cloned(),
    })
}

/// Upgrades `req` when it asks for one of the site's WebSocket routes, spawning the connection's
/// task, and hands it to `handle` otherwise.
pub(crate) fn upgrade_on_main_or<H, Fut>(
    req: Request<Incoming>,
    upgrade: &MainWsUpgrade,
    client: mipyre_transport::driver::Peer,
    handle: &H,
) -> Either<impl Future<Output = Result<Response<ResponseBody>, hyper::Error>> + Send + 'static, Fut>
where
    H: Fn(Request<Incoming>, mipyre_transport::driver::Peer) -> Fut,
{
    let Some(ws) = wants_websocket(&req, &upgrade.site) else {
        return Either::Right(handle(req, client));
    };
    let site = Arc::clone(&upgrade.site);
    let callee = match site.rules.routes.ws_place(ws.route) {
        WsPlace::Main => upgrade.main.as_ref().map(|main| on_main(main, &site)),
        WsPlace::Worker => None,
    };
    Either::Left(async move {
        let (response, accepted) = handle_websocket(req, ws, &site, callee, client).await;
        if let Some(accepted) = accepted {
            tokio::spawn(accepted.run(std::future::pending()));
        }
        Ok(response)
    })
}

fn on_main(main: &Arc<MainThreads>, site: &Site) -> OnMain {
    OnMain {
        main: Arc::clone(main),
        budget: site.rules.settings.limits.request_budget,
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
/// any other response (CORS, access log); with the 101 comes the connection to run. `callee` is
/// `None` when nothing runs the route's calls.
async fn handle_websocket<C: Opener>(
    mut req: Request<Incoming>,
    ws: WsRouteMatch,
    site: &Arc<Site>,
    callee: Option<C>,
    peer: mipyre_transport::driver::Peer,
) -> (Response<ResponseBody>, Option<Admission<C>>) {
    let timer = AccessTimer::start(site);
    let origin = cors_origin(site, req.headers());
    let client = client_of(peer, req.headers(), site);
    if let Some(miss) = refused_site(&req, true, site) {
        let resp = full_body(site_miss_response(miss).map(Full::new));
        let resp = finish(
            resp,
            site,
            req.method(),
            req.uri().path(),
            Requester::Headers(req.headers(), &client),
            timer,
            origin,
            ResponseSource::WebSocket,
        );
        return (resp, None);
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
    let (resp, accepted) = match WsHandshake::parse(&head.parts) {
        Ok(handshake) => answer_upgrade(site, callee, ws.route, &handshake, head, upgrade, label.clone()).await,
        Err(e) => {
            tracing::debug!(target: "mipyre::server", request_id = %label.id,
                path = label.uri.path(), error = %e, "WebSocket handshake refused");
            (full_body(e.response().map(Full::new)), None)
        }
    };
    let resp = finish(
        resp,
        site,
        &label.method,
        label.uri.path(),
        Requester::Given(&label.id, &client),
        timer,
        origin,
        ResponseSource::WebSocket,
    );
    (resp, accepted)
}

async fn answer_upgrade<C: Opener>(
    site: &Arc<Site>,
    callee: Option<C>,
    route: WsRouteId,
    handshake: &WsHandshake,
    head: RequestHead,
    upgrade: hyper::upgrade::OnUpgrade,
    label: RequestLabel,
) -> (Response<ResponseBody>, Option<Admission<C>>) {
    let limits = site.rules.settings.limits.ws;
    let Some(callee) = callee else {
        let group = match site.rules.routes.ws_place(route) {
            WsPlace::Worker => WorkerGroup::Subinterp,
            WsPlace::Main => WorkerGroup::Main,
        };
        return (
            fail(refuse(UnansweredError::WorkersGone(group)), &label, &site.counters),
            None,
        );
    };
    let Some(slot) = ConnectionSlot::try_acquire(&site.ws_connections, limits.max_connections.get()) else {
        tracing::debug!(target: "mipyre::server", path = label.uri.path(), max_connections = limits.max_connections.get(),
            "WebSocket connection limit reached; answered 503");
        let refused = refuse(UnansweredError::Overloaded(Capacity::WebsocketConnections));
        return (fail(refused, &label, &site.counters), None);
    };

    let accept_encoding = crate::pipeline::AcceptEncoding::of(&head, site.rules.settings.compression.as_ref());
    let outbox = Outbox::new(limits.max_message_bytes);
    let request = exchange::Request::new(head, RequestBody::Buffered(Bytes::new()));
    let upgrade_request = Upgrade {
        route,
        request,
        outbox: Arc::clone(&outbox),
        slot,
        label: label.clone(),
    };
    let (session, slot) = match callee.open(upgrade_request).await {
        Ok((WsUpgradeAnswer::Accepted(session), slot)) => (session, slot),
        Ok((WsUpgradeAnswer::Refused(response), _slot)) => {
            return (
                http_response(Ok(response), &accept_encoding, &site.counters).await,
                None,
            );
        }
        Err(e) => return (fail(e, &label, &site.counters), None),
    };
    let accepted = Admission {
        callee,
        route,
        session,
        outbox,
        upgrade,
        limits,
        counters: Arc::clone(&site.counters.run),
        label,
        _slot: slot,
    };
    (ws_upgrade_response(handshake.key()), Some(accepted))
}

/// A connection whose upgrade was answered 101, with its session open.
struct Admission<C> {
    callee: C,
    route: WsRouteId,
    session: WsSession,
    outbox: Arc<Outbox>,
    upgrade: hyper::upgrade::OnUpgrade,
    limits: WsLimits,
    counters: Arc<ServerCounters>,
    label: RequestLabel,
    _slot: ConnectionSlot,
}

impl<C: Callee<Session = WsSession>> Admission<C> {
    /// Serves the connection until it ends or `stop` completes, then ends its outbox, lets go of
    /// its session (calling `on_close`) and closes the socket.
    async fn run(self, stop: impl Future<Output = ()>) {
        let Admission {
            callee,
            route,
            session,
            outbox,
            upgrade,
            limits,
            counters,
            label,
            _slot,
        } = self;
        let upgraded = match upgrade.await {
            Ok(upgraded) => upgraded,
            Err(e) => {
                report_upgrade_failure(&e);
                outbox.end();
                callee.end(route, session, None, &label);
                return;
            }
        };
        let stream = WebSocketStream::from_raw_socket(
            hyper_util::rt::TokioIo::new(upgraded),
            tungstenite::protocol::Role::Server,
            Some(tungstenite_config(limits)),
        )
        .await;
        let connection = Connection {
            callee: &callee,
            route,
            session: &session,
            outbox: &outbox,
            limits,
            counters: &counters,
            label: &label,
        };
        let closing = connection.serve(stream, stop).await;
        callee.end(route, session, Some(closing.ended.told()), &label);
        closing.close().await;
    }
}

fn report_upgrade_failure(e: &hyper::Error) {
    // The client leaving right after the 101 is its doing; anything else is ours to see.
    if mipyre_http::io::events::peer_went_away(e) {
        mipyre_transport::stream::events::count(mipyre_transport::stream::events::StreamEvent::PeerGone);
        tracing::debug!(target: "mipyre::server", error = %e, "WebSocket client left after the upgrade");
    } else {
        tracing::error!(target: "mipyre::server", error = %e, "WebSocket upgrade error");
    }
}

// ---------------------------------------------------------------------------
// The connection reads a frame, calls, and writes what the call queued.
// ---------------------------------------------------------------------------

/// How a connection ended: `on_close` is told the code, and the server sends a close frame
/// only when it is the one closing.
#[derive(Debug, PartialEq, Eq)]
enum ConnectionEnd {
    /// The client closed, or went away, which `on_close` sees as 1006.
    ByClient(WsClose),
    ByServer(WsClose),
}

impl ConnectionEnd {
    fn told(&self) -> &WsClose {
        match self {
            ConnectionEnd::ByClient(close) | ConnectionEnd::ByServer(close) => close,
        }
    }

    fn frame(self) -> Option<CloseFrame> {
        match self {
            ConnectionEnd::ByClient(_) => None,
            ConnectionEnd::ByServer(close) => Some(CloseFrame {
                code: CloseCode::from(close.code),
                reason: close.reason.into(),
            }),
        }
    }
}

/// A connection that has ended, still to be closed.
struct CloseHandshake<S> {
    stream: WebSocketStream<S>,
    ended: ConnectionEnd,
}

impl<S: AsyncRead + AsyncWrite + Unpin> CloseHandshake<S> {
    async fn close(self) {
        close_connection(self.stream, self.ended.frame()).await;
    }
}

/// What one read from the socket means for the connection.
#[derive(Debug)]
enum Read {
    Message(WsMessage),
    /// A ping, pong or raw frame, which tungstenite answers itself.
    Control,
    Ended(ConnectionEnd),
}

fn read(frame: Option<Result<Message, tungstenite::Error>>, limits: WsLimits) -> Read {
    match frame {
        Some(Ok(Message::Text(text))) => Read::Message(WsMessage::Text(text)),
        Some(Ok(Message::Binary(data))) => Read::Message(WsMessage::Binary(data)),
        // tungstenite queues the Pong for a Ping itself and flushes it with the next read or
        // write.
        Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => Read::Control,
        Some(Ok(Message::Close(Some(frame)))) => Read::Ended(ConnectionEnd::ByClient(WsClose::new(
            u16::from(frame.code),
            frame.reason.as_str(),
        ))),
        Some(Ok(Message::Close(None))) => Read::Ended(ConnectionEnd::ByClient(WsClose::new(WsClose::NO_STATUS, ""))),
        None => Read::Ended(ConnectionEnd::ByClient(WsClose::new(WsClose::ABNORMAL, ""))),
        Some(Err(tungstenite::Error::Capacity(e))) => {
            tracing::warn!(target: "mipyre::server", error = %e,
                max_message_bytes = limits.max_message_bytes.get(),
                "WebSocket message over max_websocket_message_size; closing with 1009");
            Read::Ended(ConnectionEnd::ByServer(WsClose::new(WsClose::TOO_BIG, e.to_string())))
        }
        Some(Err(e)) => {
            tracing::debug!(target: "mipyre::server", error = %e, "WebSocket read error");
            Read::Ended(ConnectionEnd::ByClient(WsClose::new(WsClose::ABNORMAL, "")))
        }
    }
}

/// What woke the connection while it waited.
enum Wake {
    Stop,
    Frame(Option<Result<Message, tungstenite::Error>>),
    Queued(Taken),
    Tick,
}

/// One open connection's part of the pump: where its calls go and what it writes from.
struct Connection<'a, C: Callee> {
    callee: &'a C,
    route: WsRouteId,
    session: &'a C::Session,
    outbox: &'a Arc<Outbox>,
    limits: WsLimits,
    counters: &'a ServerCounters,
    label: &'a RequestLabel,
}

impl<C: Callee> Connection<'_, C> {
    /// Pumps until the connection ends, ends its outbox, then closes the socket once its
    /// session is let go of; returns how it ended.
    async fn serve<S>(&self, mut stream: WebSocketStream<S>, stop: impl Future<Output = ()>) -> CloseHandshake<S>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let ended = self.pump(&mut stream, stop).await;
        self.outbox.end();
        CloseHandshake { stream, ended }
    }

    /// Calls `on_connect`, then for each message read calls `on_message` and writes what it
    /// queued, until the connection ends; the socket is not read during a call.
    async fn pump<S>(&self, stream: &mut WebSocketStream<S>, stop: impl Future<Output = ()>) -> ConnectionEnd
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut queue = VecDeque::new();
        if let Some(ended) = self.call_and_write(stream, &mut queue, WsEvent::Connect).await {
            return ended;
        }
        let keepalive = self.limits.keepalive;
        let mut heard = Instant::now();
        let mut pinged = None;
        let tick = tokio::time::sleep_until(heard + keepalive.ping_after.get());
        tokio::pin!(tick, stop);
        loop {
            let wake = tokio::select! {
                biased;
                () = &mut stop => Wake::Stop,
                frame = stream.next() => Wake::Frame(frame),
                taken = std::future::poll_fn(|cx| self.outbox.poll_take(cx, &mut queue)) => Wake::Queued(taken),
                () = &mut tick => Wake::Tick,
            };
            match wake {
                Wake::Stop => {
                    return ConnectionEnd::ByServer(WsClose::new(WsClose::GOING_AWAY, "the server is stopping"))
                }
                Wake::Frame(frame) => {
                    heard = Instant::now();
                    pinged = None;
                    match read(frame, self.limits) {
                        Read::Message(msg) => {
                            if let Some(ended) = self.call_and_write(stream, &mut queue, WsEvent::Message(msg)).await {
                                return ended;
                            }
                        }
                        Read::Control => {}
                        Read::Ended(ended) => return ended,
                    }
                }
                Wake::Queued(taken) => {
                    if let Some(ended) = write(stream, &mut queue, taken).await {
                        return ended;
                    }
                }
                Wake::Tick => {
                    let now = Instant::now();
                    match heartbeat(keepalive, heard, pinged, now) {
                        Heartbeat::Quiet { until } => tick.as_mut().reset(until),
                        Heartbeat::Ping => {
                            if let Err(e) = stream.send(Message::Ping(Bytes::new())).await {
                                return sent_failed(&e);
                            }
                            pinged = Some(now);
                            tick.as_mut().reset(now + keepalive.pong_within.get());
                        }
                        Heartbeat::Gone => {
                            self.counters.ws_keepalive_timeout();
                            tracing::debug!(target: "mipyre::server", keepalive = ?keepalive,
                                "WebSocket client did not answer a ping in time; closing with 1011");
                            return ConnectionEnd::ByServer(WsClose::new(
                                WsClose::INTERNAL_ERROR,
                                "keepalive ping timeout",
                            ));
                        }
                    }
                }
            }
        }
    }

    /// Makes one call and writes what it queued, the handler's sends and then its return value,
    /// with one flush.
    async fn call_and_write<S>(
        &self,
        stream: &mut WebSocketStream<S>,
        queue: &mut VecDeque<WsMessage>,
        event: WsEvent,
    ) -> Option<ConnectionEnd>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        match self.callee.call(self.route, self.session, event).await {
            Ok(()) => {}
            Err(CallFailure::Raised(e)) => {
                drop(e.log(self.label));
                self.outbox
                    .close(WsClose::new(WsClose::INTERNAL_ERROR, "the handler raised"));
            }
            Err(CallFailure::WorkerRetired) => {
                return Some(ConnectionEnd::ByServer(WsClose::new(
                    WsClose::GOING_AWAY,
                    "the worker retired",
                )));
            }
        }
        let taken = self.outbox.take(queue)?;
        write(stream, queue, taken).await
    }
}

/// Writes the messages taken, with one flush; returns how the connection ended when it did.
async fn write<S>(
    stream: &mut WebSocketStream<S>,
    queue: &mut VecDeque<WsMessage>,
    taken: Taken,
) -> Option<ConnectionEnd>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let written = async {
        while let Some(msg) = queue.pop_front() {
            stream.feed(into_message(msg)).await?;
        }
        stream.flush().await
    };
    if let Err(e) = written.await {
        queue.clear();
        return Some(sent_failed(&e));
    }
    match taken {
        Taken::Messages => None,
        Taken::MessagesThenClose(close) => Some(ConnectionEnd::ByServer(close)),
    }
}

fn into_message(msg: WsMessage) -> Message {
    match msg {
        WsMessage::Text(text) => Message::Text(text),
        WsMessage::Binary(data) => Message::Binary(data),
    }
}

fn sent_failed(e: &tungstenite::Error) -> ConnectionEnd {
    match write_stalled(e) {
        Some(stalled) => report_write_stall(stalled.limit),
        None => tracing::debug!(target: "mipyre::server", error = %e, "WebSocket send failed"),
    }
    ConnectionEnd::ByClient(WsClose::new(WsClose::ABNORMAL, ""))
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
async fn close_connection<S>(mut stream: WebSocketStream<S>, frame: Option<CloseFrame>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // A close frame the client sent is answered by tungstenite, which a flush writes; sending
    // one more after it is refused.
    let closed = match frame {
        Some(frame) => stream.close(Some(frame)).await,
        None => SinkExt::close(&mut stream).await,
    };
    if let Err(e) = closed {
        tracing::debug!(target: "mipyre::server", error = %e, "WebSocket close failed");
    }
    let io = stream.get_mut();
    if let Err(e) = io.shutdown().await {
        tracing::debug!(target: "mipyre::server", error = %e, "WebSocket write shutdown failed");
    }
    if tokio::time::timeout(CLOSE_LINGER, discard_until_eof(io)).await.is_err() {
        tracing::debug!(target: "mipyre::server", linger = ?CLOSE_LINGER,
            "WebSocket peer did not close in time; dropping the connection");
    }
}

async fn discard_until_eof<R: AsyncRead + Unpin>(io: &mut R) {
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

    #[test]
    fn a_client_close_frame_is_what_on_close_is_told_and_no_frame_is_owed() {
        let ended = ConnectionEnd::ByClient(WsClose::new(4000, "bye"));
        assert_eq!(ended.told(), &WsClose::new(4000, "bye"));
        assert_eq!(ended.frame(), None);
        let ended = ConnectionEnd::ByServer(WsClose::new(WsClose::GOING_AWAY, "stop"));
        assert_eq!(ended.frame().map(|f| u16::from(f.code)), Some(WsClose::GOING_AWAY));
    }

    #[test]
    fn a_read_maps_each_frame_to_what_the_connection_does() {
        let limits = WsLimits::DEFAULT;
        let told = |frame| match read(frame, limits) {
            Read::Ended(ended) => Some(ended),
            Read::Message(_) | Read::Control => None,
        };
        assert!(matches!(read(Some(Ok(Message::text("hi"))), limits), Read::Message(WsMessage::Text(t)) if t == "hi"));
        assert!(matches!(
            read(Some(Ok(Message::Ping(Bytes::new()))), limits),
            Read::Control
        ));
        assert_eq!(
            told(None),
            Some(ConnectionEnd::ByClient(WsClose::new(WsClose::ABNORMAL, "")))
        );
        assert_eq!(
            told(Some(Ok(Message::Close(None)))),
            Some(ConnectionEnd::ByClient(WsClose::new(WsClose::NO_STATUS, "")))
        );
        let too_big =
            tungstenite::Error::Capacity(tungstenite::error::CapacityError::MessageTooLong { size: 2, max_size: 1 });
        assert!(matches!(told(Some(Err(too_big))), Some(ConnectionEnd::ByServer(c)) if c.code == WsClose::TOO_BIG));
    }

    /// Tests the connection under the real connection stack, whose write timeout bounds a write
    /// to a client that stopped reading. The handler is a script, the pipe is in memory and the
    /// clock paused, so the timeouts elapse without the test waiting for them.
    #[cfg(not(sync_model))]
    mod pump {
        use super::*;

        use std::cell::{Cell, RefCell};
        use std::rc::Rc;
        use std::time::Duration;

        use hyper::service::service_fn;
        use tokio::io::DuplexStream;
        use tokio::sync::oneshot;
        use tokio::time::Instant;

        use crate::conn::{drive_connection, ConnectionTimeouts, LocalExec};
        use mipyre_http::plan::limits::MessageByteLimit;
        use mipyre_http::plan::route::{RouteTable, WsHandlers};
        use mipyre_transport::stream::events::{self, StreamEvent};
        use mipyre_transport::stream::stall::WRITE_TIMEOUT;

        /// Each direction of the pipe holds this much, as a socket's buffer does.
        const PIPE_BYTES: usize = 64 * 1024;
        const MAX_MESSAGE_BYTES: usize = 256 * 1024;

        const UPGRADE: &str = "GET /ws HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
                               Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";

        /// What the scripted handler does with each call.
        type Script = fn(&Outbox, &WsEvent) -> Result<(), CallFailure>;

        /// Stands in for the interpreter: it runs `script` on each call and records the calls and
        /// how the connection ended.
        struct ScriptCallee {
            outbox: Arc<Outbox>,
            script: Script,
            calls: RefCell<Vec<WsEvent>>,
            ended: RefCell<Vec<Option<WsClose>>>,
        }

        impl Callee for ScriptCallee {
            type Session = ();

            async fn call(&self, _route: WsRouteId, _session: &(), event: WsEvent) -> Result<(), CallFailure> {
                let called = (self.script)(&self.outbox, &event);
                self.calls.borrow_mut().push(event);
                called
            }

            fn end(&self, _route: WsRouteId, _session: (), closed: Option<&WsClose>, _label: &RequestLabel) {
                self.ended.borrow_mut().push(closed.cloned());
            }
        }

        fn nothing(_: &Outbox, _: &WsEvent) -> Result<(), CallFailure> {
            Ok(())
        }

        fn echo(outbox: &Outbox, event: &WsEvent) -> Result<(), CallFailure> {
            if let WsEvent::Message(msg) = event {
                assert!(outbox.send(WsMessage::Text("first".into())));
                assert!(outbox.send(msg.clone()));
            }
            Ok(())
        }

        struct Conn {
            client: DuplexStream,
            callee: Rc<ScriptCallee>,
            stop: Option<oneshot::Sender<()>>,
            /// It completes once the connection has ended and its socket is closed.
            closed: oneshot::Receiver<()>,
            counters: Arc<ServerCounters>,
        }

        impl Conn {
            fn ended(&self) -> bool {
                !self.callee.ended.borrow().is_empty()
            }

            fn told(&self) -> Vec<Option<WsClose>> {
                self.callee.ended.borrow().clone()
            }

            fn outbox(&self) -> &Outbox {
                &self.callee.outbox
            }
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

        fn route() -> WsRouteId {
            let mut table = RouteTable::<(), ()>::new();
            let handlers = WsHandlers {
                on_message: (),
                on_connect: None,
                on_close: None,
            };
            table.add_websocket("/ws", handlers, WsPlace::Worker).unwrap();
            table.websocket("/ws").unwrap().0
        }

        async fn upgraded(script: Script) -> Conn {
            upgraded_under(limits(), script).await
        }

        /// Serves one connection with `drive_connection`, upgrades it and runs it as `Admission`
        /// does with the scripted handler, and reads off the 101.
        async fn upgraded_under(limits: WsLimits, script: Script) -> Conn {
            let (mut client, server) = tokio::io::duplex(PIPE_BYTES);
            let callee = Rc::new(ScriptCallee {
                outbox: Outbox::new(limits.max_message_bytes),
                script,
                calls: RefCell::default(),
                ended: RefCell::default(),
            });
            let (stop, stopped) = oneshot::channel::<()>();
            let (closed_tx, closed) = oneshot::channel();
            let start = Cell::new(Some((Rc::clone(&callee), stopped, closed_tx)));
            let counters = Arc::new(ServerCounters::new(false));
            let pump_counters = Arc::clone(&counters);
            let service = service_fn(move |mut req: Request<Incoming>| {
                let upgrade = hyper::upgrade::on(&mut req);
                let (callee, stopped, closed_tx) = start.take().unwrap();
                let counters = Arc::clone(&pump_counters);
                tokio::task::spawn_local(async move {
                    let stream = WebSocketStream::from_raw_socket(
                        hyper_util::rt::TokioIo::new(upgrade.await.unwrap()),
                        tungstenite::protocol::Role::Server,
                        Some(tungstenite_config(limits)),
                    )
                    .await;
                    let label = mipyre_runtime::testkit::label(&RequestId::Client(
                        mipyre_http::plan::request_id::ClientRequestId::parse("ws").unwrap(),
                    ));
                    let connection = Connection {
                        callee: &*callee,
                        route: route(),
                        session: &(),
                        outbox: &callee.outbox,
                        limits,
                        counters: &counters,
                        label: &label,
                    };
                    let stop = async move {
                        if stopped.await.is_err() {
                            std::future::pending::<()>().await;
                        }
                    };
                    let closing = connection.serve(stream, stop).await;
                    callee.end(route(), (), Some(closing.ended.told()), &label);
                    closing.close().await;
                    closed_tx.send(()).unwrap();
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
            Conn {
                client,
                callee,
                stop: Some(stop),
                closed,
                counters,
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

        const MASK: [u8; 4] = [1, 2, 3, 4];

        /// Returns a masked frame as a client sends it, with a payload under 64 KiB.
        fn client_frame(first: u8, payload: &[u8]) -> Vec<u8> {
            let mut frame = vec![first];
            match u16::try_from(payload.len()) {
                Ok(len) if len < 126 => frame.push(0x80 | len as u8),
                Ok(len) => {
                    frame.push(0x80 | 126);
                    frame.extend_from_slice(&len.to_be_bytes());
                }
                Err(_) => panic!("a test frame is under 64 KiB"),
            }
            frame.extend_from_slice(&MASK);
            frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ MASK[i % 4]));
            frame
        }

        fn text(payload: &str) -> Vec<u8> {
            client_frame(0x81, payload.as_bytes())
        }

        fn close(code: u16, reason: &str) -> Vec<u8> {
            let mut payload = code.to_be_bytes().to_vec();
            payload.extend_from_slice(reason.as_bytes());
            client_frame(0x88, &payload)
        }

        /// Reads one server frame: its first byte and its payload.
        async fn read_frame(client: &mut DuplexStream) -> (u8, Vec<u8>) {
            let mut head = [0u8; 2];
            client.read_exact(&mut head).await.unwrap();
            let len = match head[1] & 0x7f {
                126 => usize::from(client.read_u16().await.unwrap()),
                127 => usize::try_from(client.read_u64().await.unwrap()).unwrap(),
                len => usize::from(len),
            };
            let mut payload = vec![0u8; len];
            client.read_exact(&mut payload).await.unwrap();
            (head[0], payload)
        }

        async fn read_close_code(client: &mut DuplexStream) -> u16 {
            let (first, payload) = read_frame(client).await;
            assert_eq!(first, 0x88, "{payload:?}");
            u16::from_be_bytes([payload[0], payload[1]])
        }

        #[test]
        fn a_message_is_answered_with_the_sends_and_then_the_return_value() {
            run(async {
                let mut c = upgraded(echo).await;
                c.client.write_all(&text("hi")).await.unwrap();
                assert_eq!(read_frame(&mut c.client).await, (0x81, b"first".to_vec()));
                assert_eq!(read_frame(&mut c.client).await, (0x81, b"hi".to_vec()));
                c.client.write_all(&client_frame(0x82, &[7, 8])).await.unwrap();
                assert_eq!(read_frame(&mut c.client).await, (0x81, b"first".to_vec()));
                assert_eq!(read_frame(&mut c.client).await, (0x82, vec![7, 8]));
                let calls = c.callee.calls.borrow();
                assert!(matches!(calls[0], WsEvent::Connect), "{calls:?}");
                assert_eq!(calls.len(), 3, "{calls:?}");
            });
        }

        #[test]
        fn a_handler_that_raises_closes_with_1011_after_what_it_sent() {
            fn raises(outbox: &Outbox, event: &WsEvent) -> Result<(), CallFailure> {
                match event {
                    WsEvent::Connect => Ok(()),
                    WsEvent::Message(_) => {
                        assert!(outbox.send(WsMessage::Text("before".into())));
                        Err(CallFailure::Raised(refuse(UnansweredError::TimedOut(
                            Duration::from_secs(1),
                        ))))
                    }
                }
            }
            run(async {
                let mut c = upgraded(raises).await;
                c.client.write_all(&text("hi")).await.unwrap();
                assert_eq!(read_frame(&mut c.client).await, (0x81, b"before".to_vec()));
                assert_eq!(read_close_code(&mut c.client).await, WsClose::INTERNAL_ERROR);
                assert_eq!(c.told()[0].as_ref().map(|t| t.code), Some(WsClose::INTERNAL_ERROR));
            });
        }

        #[test]
        fn a_retired_worker_closes_its_connections_with_1001() {
            fn retired(_: &Outbox, event: &WsEvent) -> Result<(), CallFailure> {
                match event {
                    WsEvent::Connect => Ok(()),
                    WsEvent::Message(_) => Err(CallFailure::WorkerRetired),
                }
            }
            run(async {
                let mut c = upgraded(retired).await;
                c.client.write_all(&text("hi")).await.unwrap();
                assert_eq!(read_close_code(&mut c.client).await, WsClose::GOING_AWAY);
            });
        }

        #[test]
        fn on_close_is_told_the_clients_close_frame_once() {
            run(async {
                let mut c = upgraded(nothing).await;
                c.client.write_all(&close(4000, "bye")).await.unwrap();
                assert_eq!(
                    read_close_code(&mut c.client).await,
                    4000,
                    "tungstenite echoes the code"
                );
                c.client.shutdown().await.unwrap();
                (&mut c.closed).await.unwrap();
                assert_eq!(c.told(), vec![Some(WsClose::new(4000, "bye"))]);
            });
        }

        #[test]
        fn a_client_that_goes_without_a_close_frame_is_told_1006() {
            run(async {
                let mut c = upgraded(nothing).await;
                c.client.shutdown().await.unwrap();
                (&mut c.closed).await.unwrap();
                assert_eq!(c.told(), vec![Some(WsClose::new(WsClose::ABNORMAL, ""))]);
            });
        }

        #[test]
        fn the_server_stopping_closes_with_1001() {
            run(async {
                let mut c = upgraded(nothing).await;
                c.stop.take().unwrap().send(()).unwrap();
                assert_eq!(read_close_code(&mut c.client).await, WsClose::GOING_AWAY);
                assert_eq!(c.told()[0].as_ref().map(|t| t.code), Some(WsClose::GOING_AWAY));
            });
        }

        #[test]
        fn a_message_past_the_limit_closes_with_1009() {
            run(async {
                let small = WsLimits {
                    max_message_bytes: MessageByteLimit::new(1024).unwrap(),
                    ..limits()
                };
                let mut c = upgraded_under(small, nothing).await;
                c.client.write_all(&client_frame(0x82, &[0; 2048])).await.unwrap();
                assert_eq!(read_close_code(&mut c.client).await, WsClose::TOO_BIG);
            });
        }

        #[test]
        fn a_send_from_outside_a_call_wakes_the_connection() {
            run(async {
                let mut c = upgraded(nothing).await;
                tokio::task::yield_now().await;
                assert!(c.outbox().send(WsMessage::Binary(Bytes::from_static(b"pub"))));
                assert_eq!(read_frame(&mut c.client).await, (0x82, b"pub".to_vec()));
            });
        }

        #[test]
        fn a_send_that_does_not_fit_closes_with_1008_and_later_sends_are_refused() {
            run(async {
                let mut c = upgraded(nothing).await;
                let half = WsMessage::Binary(Bytes::from(vec![0; MAX_MESSAGE_BYTES / 2 + 1]));
                assert!(c.outbox().send(half.clone()));
                assert!(!c.outbox().send(half.clone()));
                assert_eq!(read_close_code(&mut c.client).await, WsClose::POLICY);
                assert!(!c.outbox().send(half));
            });
        }

        /// Checks the stall is told from other send failures: a stall is warned about (throttled),
        /// another failure is a debug line.
        #[test]
        fn a_client_that_never_reads_is_disconnected() {
            let log = mipyre_runtime::testkit::captured_log_from(tracing::Level::DEBUG, never_read);
            assert!(!log.contains("WebSocket send failed"), "{log}");
        }

        fn sends_two_halves(outbox: &Outbox, event: &WsEvent) -> Result<(), CallFailure> {
            if let WsEvent::Connect = event {
                let half = MAX_MESSAGE_BYTES / 2 - 64;
                assert!(outbox.send(WsMessage::Binary(Bytes::from(vec![7; half]))));
                assert!(outbox.send(WsMessage::Binary(Bytes::from(vec![7; half]))));
            }
            Ok(())
        }

        fn never_read() {
            run(async {
                let stalls = events::total(StreamEvent::WriteStalled);
                let started = Instant::now();
                let mut c = upgraded_under(limits_without_pings(), sends_two_halves).await;

                while !c.ended() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                let ended = started.elapsed();
                assert!(ended >= WRITE_TIMEOUT, "ended after {ended:?}");
                assert!(ended < WRITE_TIMEOUT + CLOSE_LINGER, "ended after {ended:?}");
                assert!(
                    !c.outbox().send(WsMessage::Text("late".into())),
                    "a send after the stall is refused"
                );
                assert!(events::total(StreamEvent::WriteStalled) > stalls);
                (&mut c.closed).await.unwrap();
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

        fn sends_one_max(outbox: &Outbox, event: &WsEvent) -> Result<(), CallFailure> {
            if let WsEvent::Connect = event {
                assert!(outbox.send(WsMessage::Binary(Bytes::from(vec![7; MAX_MESSAGE_BYTES]))));
            }
            Ok(())
        }

        #[test]
        fn a_client_that_reads_slowly_but_keeps_reading_is_not_disconnected() {
            run(async {
                let mut c = upgraded_under(limits_without_pings(), sends_one_max).await;
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
                assert!(!c.ended(), "the connection is still open");
                assert!(c.outbox().send(WsMessage::Text("next".into())));
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
                let mut c = upgraded(nothing).await;

                tokio::time::sleep(unheard_for() - Duration::from_millis(1)).await;
                assert!(!c.ended(), "closed before the pong was due");
                tokio::time::sleep(Duration::from_millis(2)).await;
                assert!(c.ended(), "still open after the pong was due");

                assert_eq!(read_frame_head(&mut c.client).await, SERVER_PING);
                assert_eq!(read_close_code(&mut c.client).await, 1011);
                assert_eq!(c.counters.read().ws_keepalive_timeouts, 1);
            });
        }

        #[test]
        fn a_client_that_answers_the_ping_in_time_stays() {
            run(async {
                let mut c = upgraded(nothing).await;
                tokio::time::sleep(unheard_for() - Duration::from_millis(1)).await;
                assert_eq!(read_frame_head(&mut c.client).await, SERVER_PING);
                c.client.write_all(&CLIENT_PONG).await.unwrap();

                tokio::time::sleep(limits().keepalive.ping_after.get() - Duration::from_millis(1)).await;
                assert!(!c.ended(), "closed although the pong came in time");
                assert_eq!(c.counters.read().ws_keepalive_timeouts, 0);
            });
        }

        #[test]
        fn a_client_whose_frames_come_on_time_is_never_pinged() {
            run(async {
                let mut c = upgraded(nothing).await;
                for _ in 0..5 {
                    tokio::time::sleep(limits().keepalive.ping_after.get() - Duration::from_millis(1)).await;
                    c.client.write_all(&CLIENT_PONG).await.unwrap();
                }
                tokio::time::sleep(limits().keepalive.ping_after.get() - Duration::from_millis(1)).await;
                assert!(!c.ended(), "closed although every frame came on time");
                let mut byte = [0u8; 1];
                let read = tokio::time::timeout(Duration::ZERO, c.client.read(&mut byte)).await;
                assert!(read.is_err(), "the server sent {byte:?}, not nothing");
            });
        }

        #[test]
        fn a_frame_trickled_a_byte_at_a_time_is_cut_off_when_the_pong_is_due() {
            run(async {
                let mut c = upgraded(nothing).await;
                let payload_bytes = 64u8;
                c.client
                    .write_all(&[0x82, 0x80 | payload_bytes, 1, 2, 3, 4])
                    .await
                    .unwrap();
                let started = Instant::now();
                let drip = limits().keepalive.pong_within.get() / 2;
                for _ in 0..payload_bytes {
                    tokio::time::sleep(drip).await;
                    if c.ended() {
                        break;
                    }
                    c.client.write_all(&[0]).await.unwrap();
                }
                let closed_after = started.elapsed();
                assert!(c.ended(), "the trickle was never cut off");
                assert!(closed_after >= unheard_for(), "closed after {closed_after:?}");
                assert!(closed_after <= unheard_for() + drip, "closed after {closed_after:?}");
            });
        }
    }
}
