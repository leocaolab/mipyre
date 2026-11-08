//! Runs a WebSocket connection's calls in the interpreter that serves its route: the
//! `before_request` hooks that answer the upgrade, `on_connect`, `on_message` once per message and
//! `on_close`. Each interpreter that runs them (a worker, main) keeps its connections' `ws`
//! objects in its own [`Sessions`], so a Python object never leaves its interpreter; the
//! connection holds only a [`WsSession`] key.
//!
//! It does not know the socket, when the next message comes, or which thread the connection is
//! on; a handler's return value goes to the connection's outbox, which the connection writes once
//! the call returns.

use std::sync::Arc;

use mipyre_http::plan::response::HandlerResponse;
use mipyre_http::plan::route::WsHandlers;
use pyo3::prelude::*;

use crate::exchange::error::{HandlerError, RequestStage};
use crate::exchange::request::Request;
use crate::exchange::websocket::{Outbox, WebSocket, WsClose, WsMessage};
use crate::hook_chain::{Chain, EventLoopProvider};

/// One open connection's key into the [`Sessions`] of the interpreter that opened it. Only
/// [`Sessions::insert`] makes one and [`Sessions::remove`] consumes it, so it names a live entry
/// of the table that made it for as long as it exists.
#[derive(Debug)]
pub struct WsSession(usize);

/// A reference to a [`WsSession`] that can cross to the thread that runs the call while the
/// connection keeps the session.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SessionKey(usize);

impl WsSession {
    pub(crate) fn key(&self) -> SessionKey {
        SessionKey(self.0)
    }
}

/// What one call into a connection's handlers is for.
#[derive(Debug)]
pub enum WsEvent {
    Connect,
    Message(WsMessage),
}

/// How the hooks answered an upgrade: let it through, with the connection's session, or with
/// the response one of them returned.
#[derive(Debug)]
pub enum WsUpgradeAnswer {
    Accepted(WsSession),
    Refused(HandlerResponse),
}

pub(crate) type Handlers = WsHandlers<Py<PyAny>>;

/// Runs the hooks on the upgrade request and, when none answers, makes the connection's `ws`;
/// `Ok(Err(response))` is a hook's answer.
pub(crate) fn open<'py, L: EventLoopProvider>(
    chain: &mut Chain<'_, 'py, L>,
    before_hooks: &[Py<PyAny>],
    request: Request,
    outbox: Arc<Outbox>,
) -> Result<Result<Bound<'py, WebSocket>, HandlerResponse>, HandlerError> {
    let py = chain.py;
    let setup = |e: PyErr| HandlerError::python(py, RequestStage::Setup, &e);
    let req = Bound::new(py, request).map_err(setup)?;
    if let Some(response) = chain.before(before_hooks, &req)? {
        return Ok(Err(response));
    }
    Bound::new(py, WebSocket::new(py, req.unbind(), outbox))
        .map(Ok)
        .map_err(setup)
}

/// Calls the handler `event` is for and queues what it returned on the connection.
pub(crate) fn call<'py, L: EventLoopProvider>(
    chain: &mut Chain<'_, 'py, L>,
    handlers: &Handlers,
    ws: &Bound<'py, WebSocket>,
    event: WsEvent,
) -> Result<(), HandlerError> {
    let py = chain.py;
    let returned = match event {
        WsEvent::Connect => match &handlers.on_connect {
            Some(on_connect) => chain.callback(on_connect, (ws,))?,
            None => return Ok(()),
        },
        WsEvent::Message(msg) => chain.callback(&handlers.on_message, (ws, msg.to_py(py)))?,
    };
    ws.borrow()
        .reply(&returned)
        .map_err(|e| HandlerError::python(py, RequestStage::Handler, &e))
}

/// Calls `on_close`, whose return value is not sent.
pub(crate) fn close<'py, L: EventLoopProvider>(
    chain: &mut Chain<'_, 'py, L>,
    handlers: &Handlers,
    ws: &Bound<'py, WebSocket>,
    close: &WsClose,
) -> Result<(), HandlerError> {
    match &handlers.on_close {
        Some(on_close) => chain
            .callback(on_close, (ws, close.code, close.reason.as_str()))
            .map(drop),
        None => Ok(()),
    }
}

/// The `ws` objects of the connections one interpreter serves; it is touched only attached to
/// that interpreter.
#[derive(Default)]
pub(crate) struct Sessions(slab::Slab<Py<WebSocket>>);

impl Sessions {
    pub(crate) fn insert(&mut self, ws: Bound<'_, WebSocket>) -> WsSession {
        WsSession(self.0.insert(ws.unbind()))
    }

    #[expect(
        clippy::expect_used,
        reason = "a SessionKey comes from a WsSession, which is made only by this table's insert and consumed by its remove, and a connection hands it only to the interpreter that opened it (WS_SESSION_LIVE)"
    )]
    pub(crate) fn ws<'py>(&self, py: Python<'py>, session: SessionKey) -> Bound<'py, WebSocket> {
        self.0.get(session.0).expect(WS_SESSION_LIVE).bind(py).clone()
    }

    /// Takes the connection's `ws` out of the table; dropping it needs this interpreter attached.
    #[expect(
        clippy::expect_used,
        reason = "a WsSession names a live entry, as on `ws` (WS_SESSION_LIVE)"
    )]
    pub(crate) fn remove(&mut self, session: WsSession) -> Py<WebSocket> {
        self.0.try_remove(session.0).expect(WS_SESSION_LIVE)
    }

    /// Lets go of every `ws` still held, with this interpreter attached.
    pub(crate) fn release_all(self, py: Python<'_>) {
        for (_, ws) in self.0 {
            ws.drop_ref(py);
        }
    }
}

const WS_SESSION_LIVE: &str =
    "a WsSession names a live entry of the table that made it: only insert makes one and only remove consumes it";
