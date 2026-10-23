//! Serves one connection on a TPC thread and holds the thread state its requests share. This is the TPC connection path: nothing here takes a lock or
//! holds a shared `CancellationToken`.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::sync::{Arc, OnceLock};

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use tokio::io::{AsyncRead, AsyncWrite};

use mipyre_http::io::body::ResponseBody;
use mipyre_tpc::StopWaiter;
use mipyre_transport::driver::{Peer, TransportDriver};

use crate::runtime::{MainInterpBridge, Retirement, SubInterpreterWorker, ThreadResident, WorkerIndex, WorkerLife};
use crate::serve::conn::{drive_connection, drive_local_connection, ConnectionTimeouts, LocalExec};
use crate::serve::site::Site;
use crate::serve::websocket::upgrade_or;

/// The thread's connection tasks share it through an `Rc`, one non-atomic clone per request, and
/// it never leaves the thread.
pub(crate) struct TpcThread {
    /// It says which process lanes the thread hosts, whether or not it holds a worker.
    pub(crate) index: WorkerIndex,
    pub(crate) worker: Option<ThreadWorker>,
    pub(crate) site: Arc<Site>,
    /// It is `None` when the table has no main-interpreter route.
    pub(crate) main_bridge: Option<Arc<MainInterpBridge>>,
    /// It is `None` when the table has no `async def` route.
    pub(crate) async_pool: Option<Arc<crate::runtime::InterpreterPool>>,
}

/// A worker retires when its `gc.collect()` fails and then takes no more requests: the thread
/// stops taking connections and closes the ones it has, the other TPC threads take its share, and
/// the server stops once none is left (`LiveWorkers`). A retired interpreter is kept only to be
/// ended on its own thread.
pub(crate) struct ThreadWorker {
    worker: RefCell<SubInterpreterWorker>,
    /// It is shared with the fanout acceptor, which stops handing this thread connections once it
    /// is set.
    retired: Arc<OnceLock<Arc<str>>>,
    /// It is taken when the worker retires or ends.
    life: Cell<Option<WorkerLife>>,
    /// Wakes the thread's accept loop when the worker retires.
    retiring: tokio::sync::Notify,
}

impl ThreadWorker {
    /// The server makes `retired` before the thread starts, so the fanout acceptor can read it.
    pub(crate) fn new(worker: SubInterpreterWorker, life: WorkerLife, retired: Arc<OnceLock<Arc<str>>>) -> Self {
        ThreadWorker {
            worker: RefCell::new(worker),
            retired,
            life: Cell::new(Some(life)),
            retiring: tokio::sync::Notify::new(),
        }
    }

    pub(crate) fn into_worker(self) -> SubInterpreterWorker {
        self.worker.into_inner()
    }

    /// Returns the worker while it serves, or why it retired.
    pub(crate) fn serving(&self) -> Result<&RefCell<SubInterpreterWorker>, Arc<str>> {
        match self.retired.get() {
            Some(why) => Err(Arc::clone(why)),
            None => Ok(&self.worker),
        }
    }

    /// Retires the worker: it takes no more requests, the live workers learn of it, and the
    /// thread's accept loop is woken. The cause was logged where the collect failed.
    pub(crate) fn retire(&self, retirement: &Retirement) {
        let why: Arc<str> = retirement.to_string().into();
        // Only a serving worker runs a request or a collect (`serving`), so this is its first
        // retirement; a second one would find the first's cause and change nothing.
        if !Arc::ptr_eq(self.retired.get_or_init(|| Arc::clone(&why)), &why) {
            return;
        }
        // The cause handed back when this was the last worker is for a queue to answer with, and
        // a TPC thread has none; the live workers have already stopped the run and logged why.
        if let Some(life) = self.life.take() {
            life.retired(retirement);
        }
        self.retiring.notify_one();
    }

    /// Completes once the worker has retired.
    pub(crate) async fn retired(&self) {
        if self.retired.get().is_none() {
            self.retiring.notified().await;
        }
    }

    /// Ends the worker on its own thread; one still serving stops counting as live first.
    pub(crate) fn end(self) {
        if let Some(life) = self.life.into_inner() {
            life.stopped();
        }
        self.worker.into_inner().end_on_own_thread();
    }
}

impl TpcThread {
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
        self.into_worker().end_on_own_thread()
    }
}

/// Sets up an accepted connection on this thread, registered and TLS-handshaken when the listener
/// has an acceptor, and serves its requests with `handle`; the TLS decision stays at the transport
/// boundary.
pub(super) async fn serve_accepted<D, H, Fut>(
    accepted: D::Connection,
    ws_site: Option<Arc<Site>>,
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
    drive_conn(stream, peer, ws_site, handle, stop).await;
}

/// Serves one connection on this thread with `handle`. Without WebSocket routes the service has no
/// upgrade branch and the connection takes the thread-local driver, which shares the connection's
/// write state without a lock.
pub(crate) async fn drive_conn<IO, H, Fut>(io: IO, peer: Peer, ws_site: Option<Arc<Site>>, handle: H, stop: StopWaiter)
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Fn(Request<Incoming>, Peer) -> Fut + 'static,
    Fut: Future<Output = Result<Response<ResponseBody>, hyper::Error>> + 'static,
{
    match ws_site {
        Some(site) => {
            let svc = service_fn(move |req: Request<Incoming>| upgrade_or(req, &site, peer, &handle));
            drive_connection(io, svc, LocalExec, ConnectionTimeouts::DEFAULT, stop).await;
        }
        None => {
            let svc = service_fn(move |req: Request<Incoming>| handle(req, peer));
            drive_local_connection(io, svc, ConnectionTimeouts::DEFAULT, stop).await;
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
