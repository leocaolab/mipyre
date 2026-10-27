//! Hands a WebSocket connection's Python half to an async engine, which runs its `before_request`
//! hooks and its `async def` handler as one task on its loop (`_worker/async_requests.py`), so a
//! waiting connection holds no thread. It does not know the socket, the message pump or the
//! connection limit; the server holds the connection's slot until the task lets go of the
//! connection.

use std::convert::Infallible;
use std::sync::Arc;

use mipyre_http::plan::response::HandlerResponse;
use mipyre_http::plan::route::WsRouteId;
use tokio::sync::oneshot;

use crate::exchange::loop_wait::LoopMailbox;
use crate::exchange::{ErrorRecord, HandlerEnds, Request};
use crate::sync::{AtomicUsize, Ordering};

/// Carries `Ok(None)` when the hooks let the upgrade through, and otherwise the response a hook
/// returned or the logged error of one that raised, which answers the upgrade instead.
pub type HooksAnswer = oneshot::Sender<Result<Option<HandlerResponse>, ErrorRecord>>;

/// One upgraded connection on its way to an engine.
pub struct WsConnection {
    pub route: WsRouteId,
    pub request: Request,
    pub ends: HandlerEnds,
    pub hooks_answer: HooksAnswer,
    pub ended: ConnectionHeld,
}

/// Is dropped, never sent on, once the engine lets go of the connection: its task ended, or it was
/// never run.
pub type ConnectionHeld = oneshot::Sender<Infallible>;

/// Hands connections to the engines that serve WebSocket routes, in turn.
#[derive(Clone)]
pub struct WsEngines {
    engines: Arc<[Arc<LoopMailbox<WsConnection>>]>,
    next: Arc<AtomicUsize>,
}

impl WsEngines {
    pub(crate) fn new(engines: Vec<Arc<LoopMailbox<WsConnection>>>) -> Self {
        WsEngines {
            engines: engines.into(),
            next: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Hands `connection` to the next engine still taking; when none is, the connection is
    /// dropped, which tells its handshake nothing will answer it.
    pub fn hand(&self, mut connection: WsConnection) -> Result<(), NoLiveEngine> {
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        for i in 0..self.engines.len() {
            let engine = &self.engines[(start + i) % self.engines.len()];
            match engine.push(connection) {
                Ok(()) => return Ok(()),
                Err(refused) => connection = refused,
            }
        }
        Err(NoLiveEngine)
    }
}

/// Every engine serving WebSocket routes has stopped.
#[derive(Debug)]
pub struct NoLiveEngine;
