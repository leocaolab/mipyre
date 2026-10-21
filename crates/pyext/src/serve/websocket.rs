//! Serves WebSocket connections: a Tokio message pump on one side and a synchronous Python handler
//! on its own OS thread on the other, joined by two queues (`crate::exchange::websocket`).
//!
//! Every resource a client can make the server hold is bounded: connections, each holding one OS
//! thread, by `max_connections`; message and frame size by `max_message_bytes`; and the bytes
//! queued in each direction by a per-connection byte budget.
//!
//! A thread per connection is what the handler API costs: `recv()` blocks, so a waiting
//! conversation holds its thread, and every connection's thread shares the main interpreter's GIL.
//! An `async def` handler also gets its own thread under `asyncio.run`, because the `WebSocket`
//! methods it calls block and would stall other connections on a shared loop; sharing one needs an
//! awaitable `WebSocket` API fed from the message pump.

use std::future::Future;
use std::ops::ControlFlow;
use crate::sync::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::future::Either;
use futures_util::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::HeaderValue;
use hyper::{Request, Response, StatusCode};
use pyo3::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio_tungstenite::WebSocketStream;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tungstenite::Message;

use crate::exchange;
use crate::exchange::{
    connection_channels, refuse, Capacity, ConnectionEnds, ErrorRecord, HandlerEnds, HandlerError,
    PyException, QueueEntry, QueueSender, UnansweredError, ReplyHolder, RequestHead, RequestLabel,
    RequestStage, WebSocket, WebSocketMessage,
};
use crate::runtime::{answer_if_awaited, main_chain, RequestContext};
use crate::serve::pipeline::{
    await_reply, cors_origin, fail, finish, reply_or_lost, AcceptEncoding, AccessTimer,
    ResponseSource,
};
use crate::serve::respond::http_response;
use crate::serve::site::Site;
use mipyre_http::plan::limits::WsLimits;
use mipyre_http::plan::response::HandlerResponse;
use mipyre_http::plan::route::{Params, WsRouteId};
use mipyre_http::plan::ws::WsHandshake;
use crate::exchange::RequestBody;
use mipyre_http::io::body::{full_body, ResponseBody};
use mipyre_http::io::request_id::RequestId;
use mipyre_subinterp::context::PyContext;

// ---------------------------------------------------------------------------
// The limits come per app from `Limits::ws`.
// ---------------------------------------------------------------------------

/// Returns tungstenite's settings, which refuse any message or frame past `max_message_bytes`.
fn tungstenite_config(limits: WsLimits) -> WebSocketConfig {
    let max = Some(limits.max_message_bytes.as_usize());
    WebSocketConfig::default()
        .max_message_size(max)
        .max_frame_size(max)
}

/// Counts the WebSocket connections one server run has open.
#[derive(Clone, Default)]
pub(crate) struct OpenConnections(Arc<AtomicUsize>);

/// It is held until the connection's Python handler thread has been joined, and released on drop.
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

/// Says whether a site's connections may upgrade to WebSocket. It is decided once per site: with no
/// WebSocket route a connection's service has no upgrade branch, and a TPC connection is driven
/// without a lock.
#[derive(Clone)]
pub(crate) enum Upgrades {
    WebSocket(Arc<Site>),
    Off,
}

impl Upgrades {
    pub(crate) fn of(site: &Arc<Site>) -> Self {
        if site.rules.routes.has_websocket_routes() {
            Upgrades::WebSocket(Arc::clone(site))
        } else {
            Upgrades::Off
        }
    }
}

/// Upgrades `req` when it asks for one of `site`'s WebSocket routes, and hands it to `handle`
/// otherwise.
pub(crate) fn upgrade_or<H, Fut>(
    req: Request<Incoming>,
    site: &Arc<Site>,
    client: mipyre_transport::driver::Peer,
    handle: &H,
) -> Either<impl Future<Output = Result<Response<ResponseBody>, hyper::Error>>, Fut>
where
    H: Fn(Request<Incoming>, mipyre_transport::driver::Peer) -> Fut,
{
    match wants_websocket(&req, site) {
        Some(ws) => Either::Left(handle_websocket(req, ws, Arc::clone(site), client)),
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
    HeaderValue::from_str(&accept)
        .expect("base64 output of derive_accept_key is a valid header value")
}

fn ws_upgrade_response(key: &HeaderValue) -> Response<ResponseBody> {
    let mut resp = Response::new(Full::new(Bytes::new()));
    *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    let headers = resp.headers_mut();
    headers.insert(
        hyper::header::UPGRADE,
        HeaderValue::from_static("websocket"),
    );
    headers.insert(
        hyper::header::CONNECTION,
        HeaderValue::from_static("Upgrade"),
    );
    headers.insert(hyper::header::SEC_WEBSOCKET_ACCEPT, accept_header(key));
    full_body(resp)
}

/// Answers a WebSocket upgrade request with the 101 or the response that refused it, finished like
/// any other response (CORS, access log).
async fn handle_websocket(
    mut req: Request<Incoming>,
    ws: WsRouteMatch,
    site: Arc<Site>,
    client: mipyre_transport::driver::Peer,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let timer = AccessTimer::start(&site);
    let origin = cors_origin(&site, req.headers());
    let upgrade = hyper::upgrade::on(&mut req);
    let (parts, _body) = req.into_parts();
    let request_id = RequestId::of(
        &parts.headers,
        site.rules.settings.request_id_header.as_ref(),
    );
    let head = RequestHead {
        parts,
        params: ws.params,
        client,
        request_id,
    };
    let label = head.label();
    let resp = match WsHandshake::parse(&head.parts) {
        Ok(handshake) => answer_upgrade(&site, ws.route, handshake, head, upgrade, &label).await,
        Err(e) => {
            tracing::debug!(target: "mipyre::server", request_id = %label.id,
                path = label.uri.path(), error = %e, "WebSocket handshake refused");
            full_body(e.response().map(Full::new))
        }
    };
    Ok(finish(
        resp,
        &site,
        &label.method,
        label.uri.path(),
        timer,
        origin,
        ResponseSource::WebSocket,
    ))
}

/// The slot is released only once the thread has been joined, on every path: the 101, a hook's
/// rejection, or the handshake giving up while a hook still runs.
struct HandlerThread {
    handle: std::thread::JoinHandle<()>,
    slot: ConnectionSlot,
}

impl HandlerThread {
    /// Joins on the blocking pool, so a hung handler does not pin a Tokio worker thread.
    async fn join(self) {
        let HandlerThread { handle, slot } = self;
        match tokio::task::spawn_blocking(move || handle.join()).await {
            Ok(Ok(())) => {}
            Ok(Err(payload)) => {
                tracing::error!(target: "mipyre::server",
                    panic = %mipyre_tpc::panic_message(&*payload),
                    "WebSocket handler thread panicked");
            }
            Err(e) => {
                tracing::error!(target: "mipyre::server", error = %e,
                    "WebSocket handler thread join failed");
            }
        }
        drop(slot);
    }
}

async fn answer_upgrade(
    site: &Arc<Site>,
    route: WsRouteId,
    handshake: WsHandshake,
    head: RequestHead,
    upgrade: hyper::upgrade::OnUpgrade,
    label: &RequestLabel,
) -> Response<ResponseBody> {
    let limits = site.rules.settings.limits.ws;
    let Some(slot) =
        ConnectionSlot::try_acquire(&site.ws_connections, limits.max_connections.get())
    else {
        tracing::debug!(target: "mipyre::server", path = label.uri.path(), max_connections = limits.max_connections.get(),
            "WebSocket connection limit reached; answered 503");
        return fail(
            refuse(UnansweredError::Overloaded(Capacity::WebsocketConnections)),
            label,
            &site.counters,
        );
    };

    // The hooks run on the connection's thread before the 101 and the handler after it, in one
    // request context. They belong to the main interpreter, and in TPC mode this runs on a thread
    // bound to a worker's interpreter, so they cannot run here.
    let (ends, handler_ends) = connection_channels(limits.max_message_bytes);
    let (answer_tx, answer_rx) = oneshot::channel();
    let accept_encoding = AcceptEncoding::of(&head, site.rules.settings.compression.as_ref());
    let request = exchange::Request::new(head, RequestBody::Buffered(Bytes::new()));
    let handle =
        match spawn_handler_thread(Arc::clone(site), route, request, handler_ends, answer_tx) {
            Ok(handle) => handle,
            Err(e) => return fail(HandlerError::ThreadNotSpawned(e), label, &site.counters),
        };
    let thread = HandlerThread { handle, slot };

    let answer = reply_or_lost(answer_rx, ReplyHolder::WebsocketHandler);
    let budget = site.rules.settings.limits.request_budget;
    match await_reply(budget, answer)
        .await
        .map(Result::transpose)
    {
        Ok(None) => {}
        Ok(Some(response)) => {
            thread.join().await;
            return http_response(response, &accept_encoding, &site.counters).await;
        }
        Err(e) => {
            // The thread sees the verdict go unread and exits without running the
            // handler; its slot is held until then.
            tokio::spawn(thread.join());
            return fail(e, label, &site.counters);
        }
    }

    tokio::spawn(async move {
        match upgrade.await {
            Ok(upgraded) => {
                let ws_stream = WebSocketStream::from_raw_socket(
                    hyper_util::rt::TokioIo::new(upgraded),
                    tungstenite::protocol::Role::Server,
                    Some(tungstenite_config(limits)),
                )
                .await;
                run_ws_connection(ws_stream, ends, limits).await;
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
        thread.join().await;
    });

    ws_upgrade_response(handshake.key())
}

// ---------------------------------------------------------------------------
// A connection is a Python handler thread plus a message pump.
// ---------------------------------------------------------------------------

/// Builds the thread a connection's handler runs on, with a Python-sized stack.
fn handler_thread() -> std::thread::Builder {
    std::thread::Builder::new()
        .name("mipyre-ws".to_string())
        .stack_size(mipyre_subinterp::PYTHON_THREAD_STACK)
}

/// Starts the connection's OS thread, which runs the `before_request` hooks and then, if they let
/// the upgrade through, the handler, in one request context.
///
/// Handlers and hooks live in the main interpreter. The thread attaches to it explicitly, with one
/// thread state for the connection's life, rather than through `with_main`, because it can outlive
/// the server run whose context `with_main` reads. The site clone is dropped inside the attach.
fn spawn_handler_thread(
    site: Arc<Site>,
    route: WsRouteId,
    request: exchange::Request,
    ends: HandlerEnds,
    // `Ok(None)` lets the upgrade through; a response a hook returned, or the logged error of one
    // that raised, answers it instead.
    hooks_answer: oneshot::Sender<Result<Option<HandlerResponse>, ErrorRecord>>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    let main = pyo3::sync::InterpreterHandle::main();
    handler_thread()
        .spawn(move || {
            mipyre_subinterp::attach::with_interpreter(main, move |py| {
                let label = request.label();
                match PyContext::new(py) {
                    // The handshake is still waiting and answers with this error as a 500.
                    Err(making) => {
                        let error =
                            HandlerError::python(py, RequestStage::Setup, &making).log(&label);
                        answer_if_awaited(hooks_answer, Err(error));
                    }
                    Ok(ctx) => {
                        let mut rc = RequestContext::enter(ctx);
                        serve_connection(py, &mut rc, &site, route, request, ends, hooks_answer);
                        // The connection ran; nothing waits for an answer any more.
                        if let Err(exit) = rc.exit() {
                            drop(
                                HandlerError::python(py, RequestStage::ContextExit, &exit)
                                    .log(&label),
                            );
                        }
                    }
                }
                drop(site);
            });
        })
}

fn serve_connection<'py>(
    py: Python<'py>,
    rc: &mut RequestContext<'py>,
    site: &Site,
    route: WsRouteId,
    request: exchange::Request,
    ends: HandlerEnds,
    // `Ok(None)` lets the upgrade through; a response a hook returned, or the logged error of one
    // that raised, answers it instead.
    hooks_answer: oneshot::Sender<Result<Option<HandlerResponse>, ErrorRecord>>,
) {
    // The table is frozen, so the route the request was routed by is still there.
    let handler = site.rules.routes.ws_handler(route).clone_ref(py);
    // The label is taken first because `request` moves into its Python object.
    let label = request.label();
    let hooks = Py::new(py, request)
        .map_err(|e| HandlerError::python(py, RequestStage::Setup, &e))
        .and_then(|request| {
            main_chain(py, rc)
                .before(site.rules.routes.before_hooks(), request.bind(py))
                .map(|r| (request, r))
        });
    // An answer goes unread only if the handshake already gave up.
    let request = match hooks {
        Ok((request, None)) => request,
        Ok((_, Some(response))) => {
            answer_if_awaited(hooks_answer, Ok(Some(response)));
            return;
        }
        Err(e) => {
            answer_if_awaited(hooks_answer, Err(e.log(&label)));
            return;
        }
    };
    if hooks_answer.send(Ok(None)).is_err() {
        // The handshake gave up waiting (request budget): no 101 was sent.
        return;
    }
    run_handler(py, &handler, WebSocket::new(ends, request), &label);
}

/// Runs the handler on the upgraded connection. No response is left to render, so an error is
/// logged here with the request.
fn run_handler(py: Python<'_>, handler: &Py<PyAny>, ws: WebSocket, label: &RequestLabel) {
    let log = |what: &str, e: PyErr| {
        let exception = PyException::capture(py, &e);
        tracing::error!(target: "mipyre::handler", request_id = %label.id,
            method = label.method.as_str(), path = label.uri.path(), error = %exception,
            traceback = exception.traceback(), "{what}");
    };
    let could_not_run = |e| log("WebSocket handler could not be run", e);
    let raised = |e| log("WebSocket handler raised", e);
    let ws_obj = match Py::new(py, ws) {
        Ok(ws_obj) => ws_obj,
        Err(e) => return could_not_run(e),
    };
    let result = match handler.call1(py, (ws_obj,)) {
        Ok(result) => result,
        Err(e) => return raised(e),
    };
    // An `async def` handler returns a coroutine that must be driven on a fresh loop.
    let coroutine = py.import("asyncio").and_then(|asyncio| {
        let is_coro = asyncio
            .call_method1("iscoroutine", (&result,))?
            .extract::<bool>()?;
        Ok((asyncio, is_coro))
    });
    match coroutine {
        Ok((asyncio, true)) => {
            if let Err(e) = asyncio.call_method1("run", (&result,)) {
                raised(e);
            }
        }
        Ok((_, false)) => {}
        Err(e) => could_not_run(e),
    }
}

async fn run_ws_connection<S>(ws_stream: WebSocketStream<S>, ends: ConnectionEnds, limits: WsLimits)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (ws_sink, ws_source) = ws_stream.split();
    let mut io = ConnectionIo {
        sink: ws_sink,
        source: ws_source,
        ends,
    };
    let close_frame = pump_messages(&mut io, limits).await;

    let ConnectionIo {
        sink,
        source,
        ends: ConnectionEnds { incoming, .. },
    } = io;
    // Dropping the sender makes Python's pending recv return None.
    drop(incoming);
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
async fn pump_messages<S>(io: &mut ConnectionIo<S>, limits: WsLimits) -> Option<CloseFrame>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            msg = io.source.next() => {
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
                    tracing::debug!(target: "mipyre::server", error = %e, "WebSocket send failed");
                    return None;
                }
            }
        }
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
    match incoming.tx.send(QueueEntry {
        msg,
        _budget: permit,
    }) {
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
    if tokio::time::timeout(CLOSE_LINGER, discard_until_eof(io))
        .await
        .is_err()
    {
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

    /// Checks that the handler thread has a Python-sized stack.
    #[test]
    fn the_handler_thread_has_a_python_stack() {
        crate::thread_stack::with_exact_stacks(
            concat!(module_path!(), "::the_handler_thread_has_a_python_stack"),
            || {
                let python = crate::thread_stack::of_a_thread_with(mipyre_subinterp::PYTHON_THREAD_STACK);
                let handler = handler_thread()
                    .spawn(crate::thread_stack::current)
                    .expect("the handler thread starts")
                    .join()
                    .expect("the handler thread reads its stack");
                assert_eq!(handler, python);
            },
        );
    }

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
}
