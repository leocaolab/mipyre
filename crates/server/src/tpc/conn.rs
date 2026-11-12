//! Serves one connection on a TPC thread and holds the thread state its requests share. This is the TPC connection path: nothing here takes a lock or
//! holds a shared `CancellationToken`.

use std::cell::{Cell, RefCell, RefMut};
use std::future::Future;
use std::rc::Rc;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;

use mipyre_http::io::body::ResponseBody;
use mipyre_http::plan::response::HandlerResponse;
use mipyre_http::plan::route::{RouteId, WsRouteId};
use mipyre_tpc::StopWaiter;
use mipyre_transport::driver::{Peer, TransportDriver};

use crate::conn::{drive_connection, drive_local_connection, ConnectionTimeouts, LocalExec};
use crate::site::Site;
use crate::websocket::{upgrade_or, WsUpgrade};
use mipyre_http::io::clock::Clock;
use mipyre_runtime::exchange::{self, refuse, HandlerError, Outbox, UnansweredError, WsClose};
use mipyre_runtime::{
    LiveTasks, LoopBell, LoopCallError, LoopReply, MainThreads, NextWait, PollerReadiness, RequestLoop, Retirement,
    SubInterpreterWorker, ThreadResident, WorkerIndex, WorkerLife, WsEvent, WsSession, WsUpgradeAnswer,
};

use super::admission::{LoopLimits, LoopSeats};
use super::deadline::Deadlines;
use super::ServeError;

/// The thread's connection tasks share it through an `Rc`, one non-atomic clone per request, and
/// it never leaves the thread.
pub struct TpcThread {
    /// It says which process lanes the thread hosts, whether or not it holds a worker.
    pub index: WorkerIndex,
    pub worker: Option<ThreadWorker>,
    pub(crate) site: Arc<Site>,
    /// It is `None` when the table has no main-interpreter route.
    pub(crate) main: Option<Arc<MainThreads>>,
}

/// A worker retires when its `gc.collect()` fails and then takes no more requests: the thread
/// stops taking connections and closes the ones it has, the other TPC threads take its share, and
/// the server stops once none is left (`LiveWorkers`). A retired interpreter is kept only to be
/// ended on its own thread.
///
/// When the table has `async def` routes it also holds the thread's event loop, which they run
/// on, and what the thread's requests to it share. The loop is in a cell of its own, so a `def`
/// request borrows only the worker, as it does without one.
pub struct ThreadWorker {
    worker: RefCell<SubInterpreterWorker>,
    /// It is shared with the fanout acceptor, which stops handing this thread connections once it
    /// is set.
    retired: Arc<OnceLock<Arc<str>>>,
    /// It is taken when the worker retires or ends.
    life: Cell<Option<WorkerLife>>,
    /// Wakes the thread's accept loop and its event loop's driver when the worker retires.
    retiring: tokio::sync::Notify,
    /// It is `None` when the table has no `async def` route, so the thread opens no loop. It is
    /// boxed to keep a `def` request's `ThreadWorker` small: its size moves the allocations of
    /// every request on the thread between mimalloc's size classes.
    event_loop: Option<Box<ThreadEventLoop>>,
}

/// A TPC thread's event loop and what the thread's `async def` requests to it share: how many it
/// holds at once, the seats of those reading their bodies, and their budgets.
pub(crate) struct ThreadEventLoop {
    pub(crate) limits: LoopLimits,
    pub(crate) seats: LoopSeats,
    pub(crate) deadlines: Deadlines,
    /// It is `None` before the loop opens and once it closes.
    open: RefCell<Option<OpenLoop>>,
}

/// An open event loop, and whether it still starts request Tasks.
struct OpenLoop {
    tasks: RequestLoop,
    /// It is set once the loop's live Tasks were cancelled, at the stop or when the worker
    /// retires. No request Task starts after that: the cancel would miss it, and the finalizers
    /// wait for the live Tasks to end.
    cancelled: bool,
}

impl ThreadWorker {
    /// The server makes `retired` before the thread starts, so the fanout acceptor can read it.
    pub(crate) fn new(
        worker: SubInterpreterWorker,
        life: WorkerLife,
        retired: Arc<OnceLock<Arc<str>>>,
        loop_limits: Option<LoopLimits>,
    ) -> Self {
        ThreadWorker {
            worker: RefCell::new(worker),
            retired,
            life: Cell::new(Some(life)),
            retiring: tokio::sync::Notify::new(),
            event_loop: loop_limits.map(|limits| {
                Box::new(ThreadEventLoop {
                    limits,
                    seats: LoopSeats::default(),
                    deadlines: Deadlines::default(),
                    open: RefCell::new(None),
                })
            }),
        }
    }

    /// Returns the worker, its event loop closed first if it is still open.
    pub(crate) fn into_worker(self) -> SubInterpreterWorker {
        let mut worker = self.worker.into_inner();
        if let Some(left) = self.event_loop.and_then(|event_loop| event_loop.open.into_inner()) {
            worker.close_event_loop(left.tasks);
        }
        worker
    }

    /// Returns the worker with its event loop, on a thread whose table has `async def` routes.
    pub(crate) fn on_loop(&self) -> Option<LoopWorker<'_>> {
        self.event_loop.as_deref().map(|event_loop| LoopWorker {
            worker: self,
            event_loop,
        })
    }

    /// Takes the next request while the worker serves, or returns why it retired.
    #[inline(always)]
    pub(crate) fn lease(&self) -> Result<WorkerLease<'_>, Arc<str>> {
        match self.retired.get() {
            Some(why) => Err(Arc::clone(why)),
            None => Ok(WorkerLease {
                worker: self.worker.borrow_mut(),
                owner: self,
            }),
        }
    }

    /// Returns why the worker retired, or `None` while it serves.
    #[cfg(feature = "bench")]
    pub fn retirement_cause(&self) -> Option<Arc<str>> {
        self.retired.get().cloned()
    }

    /// Runs the idle GC tick, which collects if the worker went quiet; a failed collect retires it.
    pub(crate) fn idle_tick(&self) {
        if self.retired.get().is_some() {
            return;
        }
        if let Err(retirement) = self.worker.borrow_mut().on_idle_tick() {
            self.retire(&retirement);
        }
    }

    /// Returns whether the worker has retired.
    pub(crate) fn has_retired(&self) -> bool {
        self.retired.get().is_some()
    }

    /// Retires the worker: it takes no more requests, the live workers learn of it, and the
    /// thread's accept loop and loop driver are woken. The cause was logged where it happened.
    fn retire(&self, retirement: &Retirement) {
        let why: Arc<str> = retirement.to_string().into();
        // Only a serving worker runs a request or a collect, so this is its first
        // retirement; a second one would find the first's cause and change nothing.
        if !Arc::ptr_eq(self.retired.get_or_init(|| Arc::clone(&why)), &why) {
            return;
        }
        // The cause handed back when this was the last worker is for a queue to answer with, and
        // a TPC thread has none; the live workers have already stopped the run and logged why.
        if let Some(life) = self.life.take() {
            life.retired(retirement);
        }
        // Both waiters check `retired` before they wait, on this thread, so none is missed.
        self.retiring.notify_waiters();
    }

    /// Completes once the worker has retired.
    pub(crate) async fn retired(&self) {
        if self.retired.get().is_none() {
            self.retiring.notified().await;
        }
    }

    /// Lets go of a WebSocket session without calling its `on_close`, whether or not the worker
    /// still serves.
    pub(crate) fn ws_release(&self, session: WsSession) {
        self.worker.borrow_mut().ws_release(session);
    }

    /// Ends the worker on its own thread; one still serving stops counting as live first.
    pub(crate) fn end(self) {
        if let Some(life) = self.life.take() {
            life.stopped();
        }
        drop(self.into_worker());
    }
}

/// The worker of a TPC thread that has an event loop, with that loop: what an `async def` request
/// and the loop's driver use. The driver's calls go on after the worker retires, so none of them
/// asks whether it serves.
#[derive(Clone, Copy)]
pub struct LoopWorker<'w> {
    pub(crate) worker: &'w ThreadWorker,
    pub(crate) event_loop: &'w ThreadEventLoop,
}

/// What a TPC thread's loop driver waits on beside its timers: the loop's bell, and its poller's
/// fd registered with the thread's runtime.
pub(crate) struct LoopWakeups {
    pub(crate) bell: Arc<LoopBell>,
    pub(crate) poller: PollerReadiness,
}

impl LoopWorker<'_> {
    /// Opens the event loop in the worker and returns what its driver waits on. It runs on the
    /// thread's runtime.
    ///
    /// # Errors
    ///
    /// The loop could not be opened, or its poller's fd not registered with this thread's runtime;
    /// the loop is closed again then.
    pub(crate) fn open(&self) -> Result<LoopWakeups, ServeError> {
        let opened = self
            .worker
            .worker
            .borrow_mut()
            .open_event_loop()
            .map_err(|e| ServeError::LoopNotOpened(Box::new(e)))?;
        match opened.register_poller() {
            Ok(poller) => {
                let bell = opened.bell();
                *self.event_loop.open.borrow_mut() = Some(OpenLoop {
                    tasks: opened,
                    cancelled: false,
                });
                Ok(LoopWakeups { bell, poller })
            }
            Err(error) => {
                self.worker.worker.borrow_mut().close_event_loop(opened);
                Err(ServeError::LoopPollerNotRegistered(error))
            }
        }
    }

    /// Returns how many request Tasks are live on the loop, or `None` once it is closed.
    pub(crate) fn live(&self) -> Option<LiveTasks> {
        self.event_loop.open.borrow().as_ref().map(|open| open.tasks.live())
    }

    /// Returns how many request Tasks are live on the loop while it starts new ones, or `None`
    /// once it cancelled its Tasks or closed.
    pub(crate) fn taking(&self) -> Option<LiveTasks> {
        self.event_loop
            .open
            .borrow()
            .as_ref()
            .filter(|open| !open.cancelled)
            .map(|open| open.tasks.live())
    }

    /// Turns the loop once and returns how its driver waits next. A loop that failed too many
    /// turns in a row retires the worker, and its driver turns again to end it.
    pub(crate) fn turn(&self) -> NextWait {
        let turned = {
            let mut event_loop = self.event_loop.open.borrow_mut();
            let Some(open) = event_loop.as_mut() else {
                return NextWait::Parked;
            };
            self.worker.worker.borrow_mut().turn(&mut open.tasks)
        };
        match turned {
            Ok(wait) => wait,
            Err(retirement) => {
                self.worker.retire(&retirement);
                NextWait::Now
            }
        }
    }

    /// Cancels the request Tasks live on the loop, and refuses every request Task after; a
    /// closed loop has none.
    ///
    /// # Errors
    ///
    /// What a cancel raised or a panic in it, after all were tried.
    pub(crate) fn cancel_live_tasks(&self) -> Result<(), LoopCallError> {
        match self.event_loop.open.borrow_mut().as_mut() {
            Some(open) => {
                open.cancelled = true;
                self.worker.worker.borrow_mut().cancel_live_tasks(&open.tasks).map(drop)
            }
            None => Ok(()),
        }
    }

    /// Starts the loop's finalizers, which wait at most `timeout` for the default executor; the
    /// receiver hears when they end. A closed loop has none to start.
    ///
    /// # Errors
    ///
    /// What starting them raised or a panic in it.
    pub(crate) fn start_finalizers(&self, timeout: Duration) -> Result<Option<oneshot::Receiver<()>>, LoopCallError> {
        match self.event_loop.open.borrow().as_ref() {
            Some(open) => self
                .worker
                .worker
                .borrow_mut()
                .start_finalizers(&open.tasks, timeout)
                .map(Some),
            None => Ok(None),
        }
    }

    /// Closes the loop and returns the request Tasks it still had live, which are dropped with
    /// it, or `None` when it was closed already.
    pub(crate) fn close(&self) -> Option<LiveTasks> {
        let open = self.event_loop.open.borrow_mut().take()?;
        let left = open.tasks.live();
        self.worker.worker.borrow_mut().close_event_loop(open.tasks);
        Some(left)
    }
}

/// The thread's worker, lent to one request it agreed to take; `serve` uses it up.
///
/// The worker is borrowed when the lease is made, before the request is built: a borrow taken
/// after the request is handed over can panic while holding it, and the compiler then copies the
/// request once more.
pub(crate) struct WorkerLease<'w> {
    worker: RefMut<'w, SubInterpreterWorker>,
    owner: &'w ThreadWorker,
}

impl WorkerLease<'_> {
    /// Runs `route` on the worker and returns the handler's answer. A failed `gc.collect()` after
    /// it retires the worker before the answer comes back, and the answer still goes out.
    #[inline(always)]
    pub(crate) fn serve(self, route: RouteId, request: exchange::Request) -> Result<HandlerResponse, HandlerError> {
        let WorkerLease { mut worker, owner } = self;
        // The answer is read where `serve_inline` wrote it: moving it out before `retire` costs
        // one more copy of it per request.
        let served = worker.serve_inline(route, request);
        drop(worker);
        if let Some(retirement) = &served.retired {
            owner.retire(retirement);
        }
        served.response
    }

    /// Runs the gRPC method `method` on the worker as [`Self::serve`] runs a route.
    #[inline(always)]
    pub(crate) fn serve_unary(
        self,
        method: RouteId,
        request: exchange::Request,
    ) -> Result<HandlerResponse, HandlerError> {
        let WorkerLease { mut worker, owner } = self;
        let served = worker.serve_unary_inline(method, request);
        drop(worker);
        if let Some(retirement) = &served.retired {
            owner.retire(retirement);
        }
        served.response
    }

    /// Opens a WebSocket connection's session on the worker; a failed `gc.collect()` after it
    /// retires the worker as a request's does.
    pub(crate) fn ws_open(
        self,
        route: WsRouteId,
        request: exchange::Request,
        outbox: Arc<Outbox>,
    ) -> Result<WsUpgradeAnswer, HandlerError> {
        let WorkerLease { mut worker, owner } = self;
        let opened = worker.ws_open(route, request, outbox);
        drop(worker);
        if let Some(retirement) = &opened.retired {
            owner.retire(retirement);
        }
        opened.response
    }

    /// Starts `request` for `route` as a Task on `on`, the thread's event loop; `large` marks it
    /// as holding a large body.
    ///
    /// # Errors
    ///
    /// Refuses the request once the loop has cancelled its Tasks or closed, and returns what
    /// starting the Task raised.
    pub(crate) fn start_task(
        self,
        on: &ThreadEventLoop,
        route: RouteId,
        request: exchange::Request,
        large: bool,
    ) -> Result<LoopReply, HandlerError> {
        let WorkerLease { mut worker, .. } = self;
        let mut event_loop = on.open.borrow_mut();
        let Some(open) = event_loop.as_mut().filter(|open| !open.cancelled) else {
            return Err(refuse(UnansweredError::LoopClosed));
        };
        worker.start_task(&mut open.tasks, route, request, large)
    }

    /// Calls a WebSocket connection's handler for `event` on the worker.
    pub(crate) fn ws_call(self, route: WsRouteId, session: &WsSession, event: WsEvent) -> Result<(), HandlerError> {
        let WorkerLease { mut worker, owner } = self;
        let called = worker.ws_call(route, session, event);
        drop(worker);
        if let Some(retirement) = &called.retired {
            owner.retire(retirement);
        }
        called.response
    }

    /// Calls a WebSocket connection's `on_close` on the worker and lets go of its session.
    pub(crate) fn ws_close(self, route: WsRouteId, session: WsSession, close: &WsClose) -> Result<(), HandlerError> {
        let WorkerLease { mut worker, owner } = self;
        let closed = worker.ws_close(route, session, close);
        drop(worker);
        if let Some(retirement) = &closed.retired {
            owner.retire(retirement);
        }
        closed.response
    }
}

impl TpcThread {
    /// Returns the thread's worker with its event loop, when the table has `async def` routes.
    pub(crate) fn on_loop(&self) -> Option<LoopWorker<'_>> {
        self.worker.as_ref().and_then(ThreadWorker::on_loop)
    }

    /// Ends the worker on this thread once no connection task holds `tpc_thread`; one still
    /// shared is leaked with an error rather than pulled from under its task.
    pub(crate) fn end(tpc_thread: Rc<TpcThread>) {
        match Rc::try_unwrap(tpc_thread) {
            Ok(tpc_thread) => {
                if let Some(worker) = tpc_thread.worker {
                    worker.end();
                }
            }
            Err(still_shared) => tracing::error!(
                target: "mipyre::server",
                worker = still_shared.index.get(),
                "a worker is still referenced after its thread's runtime ended; leaking its \
                 interpreter"
            ),
        }
    }
}

impl ThreadResident for ThreadWorker {
    fn end_here(self) {
        drop(self.into_worker());
    }
}

/// Sets up an accepted connection on this thread, registered and TLS-handshaken when the listener
/// has an acceptor, and serves its requests with `handle`; the TLS decision stays at the transport
/// boundary.
pub(super) async fn serve_accepted<D, H, Fut>(
    accepted: D::Connection,
    ws: Option<WsUpgrade>,
    handle: H,
    stop: StopWaiter,
) where
    D: TransportDriver,
    H: Fn(Request<Incoming>, Peer) -> Fut + 'static,
    Fut: Future<Output = Result<Response<ResponseBody>, hyper::Error>> + 'static,
{
    let peer = D::peer(&accepted);
    let Some(stream) = D::into_stream(accepted).await else {
        return;
    };
    drive_conn(stream, peer, ws, handle, stop).await;
}

/// Serves one connection on this thread with `handle`. Without WebSocket routes the service has no
/// upgrade branch and the connection takes the thread-local driver, which shares the connection's
/// write state without a lock.
pub async fn drive_conn<IO, H, Fut>(io: IO, peer: Peer, ws: Option<WsUpgrade>, handle: H, stop: StopWaiter)
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Fn(Request<Incoming>, Peer) -> Fut + 'static,
    Fut: Future<Output = Result<Response<ResponseBody>, hyper::Error>> + 'static,
{
    match ws {
        Some(ws) => {
            let svc = service_fn(move |req: Request<Incoming>| upgrade_or(req, &ws, peer, &handle));
            drive_connection(io, svc, LocalExec, ConnectionTimeouts::DEFAULT, Clock::process(), stop).await;
        }
        None => {
            let svc = service_fn(move |req: Request<Incoming>| handle(req, peer));
            drive_local_connection(io, svc, ConnectionTimeouts::DEFAULT, Clock::process(), stop).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks that only the last reference is ended and that one a task still holds is left to
    /// that task, as `TpcThread::end` relies on.
    #[test]
    fn only_the_last_reference_is_ended_and_a_shared_one_is_left() {
        let sole = Rc::new("worker");
        assert!(matches!(Rc::try_unwrap(sole), Ok("worker")));

        let shared = Rc::new("worker");
        let task = Rc::clone(&shared);
        let Err(left) = Rc::try_unwrap(shared) else {
            panic!("ended a value a task still holds");
        };
        assert!(Rc::ptr_eq(&left, &task));
        assert_eq!(Rc::strong_count(&task), 2, "neither reference was dropped");
    }
}
