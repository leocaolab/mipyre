//! Serves the multi-thread server (`MIPYRE_TPC=0`): Tokio accept loops on a work-stealing runtime,
//! each connection a `Send` task any IO thread may poll.

use std::cell::Cell;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::TokioExecutor;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};
use tokio_util::sync::CancellationToken;

use mipyre_tpc::ShutdownDeadline;
use mipyre_transport::driver::{AcceptError, TransportDriver};

use crate::conn::{drive_connection, ConnectionTimeouts};
use crate::on_main::MultiThreadServer;
use crate::request::handle_request;
use crate::site::Site;
use crate::sync::{AtomicUsize, Ordering};
use crate::websocket::{upgrade_or, WsUpgrade};
use crate::ServeError;
use crate::ServerCounters;
use mipyre_http::io::clock::Clock;

use self::stop::{Stop, StopWaiter};

/// Fixes how many tasks a tokio worker polls from its own queue between looks at the shared one.
/// Left unset, tokio tunes it from measured poll times, which makes a request's instruction count
/// follow how fast the machine ran; 61 is where tokio's tuning starts.
const GLOBAL_QUEUE_INTERVAL: u32 = 61;

/// Builds the multi-thread server's runtime with `PYTHON_THREAD_STACK` on every thread: tokio's
/// default is 2 MiB, and C extensions are written against 8. No handler runs on it; the main
/// interpreter's run on their own threads.
fn io_runtime(io_threads: usize) -> std::io::Result<tokio::runtime::Runtime> {
    RuntimeBuilder::new_multi_thread()
        .worker_threads(io_threads)
        .global_queue_interval(GLOBAL_QUEUE_INTERVAL)
        .thread_stack_size(mipyre_subinterp::PYTHON_THREAD_STACK)
        .enable_all()
        .build()
}

thread_local! {
    /// Is this IO thread's place in its runtime's [`IoThreadServers`], `usize::MAX` until a
    /// connection first starts on the thread.
    static IO_THREAD: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// Holds one server per IO thread. A connection takes the server of the thread it starts on, so the
/// reference count each of its requests adds and the bytes their responses clone sit on that
/// thread's own cache lines; a connection whose task moves to another thread touches them from
/// there.
pub struct IoThreadServers {
    /// Is never empty, because only `build` makes one.
    each: Box<[Arc<MultiThreadServer>]>,
    next: AtomicUsize,
}

impl IoThreadServers {
    /// Makes `io_threads` servers with `make`, one for each IO thread.
    pub fn build(io_threads: NonZeroUsize, mut make: impl FnMut() -> MultiThreadServer) -> Self {
        IoThreadServers {
            each: (0..io_threads.get()).map(|_| Arc::new(make())).collect(),
            next: AtomicUsize::new(0),
        }
    }

    /// Returns the sites of every IO thread's server.
    pub fn sites(&self) -> Vec<Arc<Site>> {
        self.each.iter().map(|server| Arc::clone(&server.site)).collect()
    }

    fn io_threads(&self) -> usize {
        self.each.len()
    }

    /// Returns the server of the thread this is called on.
    #[inline]
    fn here(&self) -> &Arc<MultiThreadServer> {
        let place = IO_THREAD.get();
        match self.each.get(place) {
            Some(server) => server,
            None => self.take_place(),
        }
    }

    /// Gives this thread the next place, the first time a connection starts on it. Only the
    /// runtime's worker threads poll connections, and there are as many of them as servers, so
    /// each gets its own.
    #[cold]
    #[inline(never)]
    fn take_place(&self) -> &Arc<MultiThreadServer> {
        let place = self.next.fetch_add(1, Ordering::Relaxed) % self.each.len();
        IO_THREAD.set(place);
        &self.each[place]
    }
}

/// Runs the multi-thread (`MIPYRE_TPC=0`) server until `stop` (a stop signal or `shutdown()`), then
/// drains in-flight connections for the drain part of the stop's grace, because dropping the
/// runtime would abort them mid-request. The runtime's blocking threads (compression, static
/// files) get until `deadline` to end; one still running then is left running.
///
/// # Errors
///
/// Returns why the runtime or a listener could not be set up, and the accept error that stopped
/// the server.
pub fn serve_multi_thread<D: TransportDriver>(
    transport: D,
    servers: IoThreadServers,
    stop: CancellationToken,
    deadline: &ShutdownDeadline,
    counters: Arc<ServerCounters>,
    ws: Option<WsUpgrade>,
) -> Result<(), ServeError> {
    Clock::process().start().map_err(ServeError::ClockNotStarted)?;
    let rt = io_runtime(servers.io_threads()).map_err(ServeError::IoRuntimeNotBuilt)?;
    let servers = Arc::new(servers);
    let served = rt.block_on(async move {
        let conn_tracker = tokio_util::task::TaskTracker::new();
        let conn_stop = Stop::default();
        // The first accept loop that can take no more connections stops the server; the slot holds
        // its error by value, so the run returns it typed.
        let (stopped_tx, mut stopped_rx) = mpsc::channel::<AcceptError>(1);
        for share in transport.into_loops() {
            let mut source = share.register()?;
            let (token, conn_stop, tracker, ws, servers, stopped_tx) = (
                stop.clone(),
                conn_stop.clone(),
                conn_tracker.clone(),
                ws.clone(),
                Arc::clone(&servers),
                stopped_tx.clone(),
            );
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        accepted = D::accept(&mut source) => match accepted {
                            Ok(accepted) => {
                                tracker.spawn(serve_accepted::<D>(
                                    accepted,
                                    ws.clone(),
                                    Arc::clone(&servers),
                                    conn_stop.waiter(),
                                ));
                            }
                            Err(stopped_accepting) => {
                                // A later loop's error is kept only in its own log line.
                                if let Err(TrySendError::Full(later) | TrySendError::Closed(later)) =
                                    stopped_tx.try_send(stopped_accepting)
                                {
                                    tracing::error!(target: "mipyre::server", error = %later, "accept loop stopped");
                                }
                                token.cancel();
                                break;
                            }
                        },
                        _ = token.cancelled() => break,
                    }
                }
            });
        }

        stop.cancelled().await;
        conn_stop.fire();
        let stopped = Instant::now();
        conn_tracker.close();
        drain_connections(&conn_tracker, crate::drain_timeout(deadline, stopped), &counters).await;
        // Closing first sends a loop that stops from now on to its own log line.
        stopped_rx.close();
        match stopped_rx.try_recv() {
            Ok(stopped_accepting) => Err(ServeError::AcceptStopped(stopped_accepting)),
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => Ok(()),
        }
    });
    // Dropping the runtime would wait for a blocking task with no limit.
    rt.shutdown_timeout(deadline.fix(Instant::now()).saturating_duration_since(Instant::now()));
    served
}

/// Sets up an accepted connection, registered and TLS-handshaken when the listener has an
/// acceptor, and serves its requests with the server of the thread it starts on until it ends or
/// `stop` completes.
async fn serve_accepted<D: TransportDriver>(
    accepted: D::Connection,
    ws: Option<WsUpgrade>,
    servers: Arc<IoThreadServers>,
    stop: StopWaiter,
) {
    let server = Arc::clone(servers.here());
    drop(servers);
    let handle = move |req, peer| handle_request(req, Arc::clone(&server), peer);
    let peer = D::peer(&accepted);
    let Some(stream) = D::into_stream(accepted).await else {
        return;
    };
    match ws {
        Some(ws) => {
            let svc = service_fn(move |req: Request<Incoming>| upgrade_or(req, &ws, peer, &handle));
            drive_connection(
                stream,
                svc,
                TokioExecutor::new(),
                ConnectionTimeouts::DEFAULT,
                Clock::process(),
                stop,
            )
            .await;
        }
        None => {
            let svc = service_fn(move |req: Request<Incoming>| handle(req, peer));
            drive_connection(
                stream,
                svc,
                TokioExecutor::new(),
                ConnectionTimeouts::DEFAULT,
                Clock::process(),
                stop,
            )
            .await;
        }
    }
}

/// Lets the multi-thread server's open connections, on the closed `tracker`, finish for at most
/// `timeout`; the ones still open then are counted and logged, and closed as their tasks drop.
async fn drain_connections(
    tracker: &tokio_util::task::TaskTracker,
    timeout: std::time::Duration,
    counters: &ServerCounters,
) {
    if tokio::time::timeout(timeout, tracker.wait()).await.is_err() {
        crate::report_drain_timeout(tracker.len(), timeout, counters);
    }
}

mod stop {
    //! Gives the multi-thread server's connections a stop whose poll shares nothing writable.
    //!
    //! A connection polls its stop on every wake, and `CancellationToken::cancelled()` locks the
    //! token's tree mutex and its `Notify`'s mutex on each poll, so a token shared by every
    //! connection put two process-wide mutexes on every request. A connection task here moves
    //! between threads, so the TPC's thread-local `LocalStop` does not fit; instead a poll reads the
    //! stop flag, which is written once, and compares the waker against the waiter's own copy. The
    //! shared waker table is locked only when a waiter registers, changes waker or ends.

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    use crate::sync::{AtomicBool, Mutex, Ordering};

    /// The multi-thread server's stop; cloning it is one reference count.
    #[derive(Clone, Default)]
    pub(super) struct Stop(Arc<StopState>);

    #[derive(Default)]
    struct StopState {
        stopped: AtomicBool,
        wakers: Mutex<WakerTable>,
    }

    /// The wakers of the waiters still registered, by key; a key whose waiter ended is reused.
    #[derive(Default)]
    struct WakerTable {
        slots: Vec<Option<Waker>>,
        free: Vec<usize>,
    }

    impl WakerTable {
        fn insert(&mut self, waker: Waker) -> usize {
            match self.free.pop() {
                Some(key) => {
                    self.slots[key] = Some(waker);
                    key
                }
                None => {
                    self.slots.push(Some(waker));
                    self.slots.len() - 1
                }
            }
        }

        fn remove(&mut self, key: usize) -> Option<Waker> {
            self.free.push(key);
            self.slots[key].take()
        }
    }

    impl Stop {
        /// Stops every waiter: each completes at its next poll, and a registered one is woken.
        pub(super) fn fire(&self) {
            self.0.stopped.store(true, Ordering::Release);
            // The wakers are taken out before any is woken or dropped, since either may run code that
            // polls or drops a waiter, which locks the table.
            let wakers: Vec<Waker> = self.0.wakers.lock().slots.iter_mut().filter_map(Option::take).collect();
            for waker in wakers {
                waker.wake();
            }
        }

        /// Returns a future that completes once the server stops, at once if it already has.
        pub(super) fn waiter(&self) -> StopWaiter {
            StopWaiter {
                state: Arc::clone(&self.0),
                registered: None,
            }
        }
    }

    /// The future [`Stop::waiter`] returns.
    pub(super) struct StopWaiter {
        state: Arc<StopState>,
        /// The waiter's key in the table, and its own copy of the waker the table holds for it.
        registered: Option<(usize, Waker)>,
    }

    impl StopWaiter {
        #[cold]
        fn register(&mut self, waker: &Waker) -> Poll<()> {
            // Every waker is cloned and dropped outside the lock, because either may run code that
            // polls or drops another waiter.
            let (own, held) = (waker.clone(), waker.clone());
            let mut table = self.state.wakers.lock();
            // Read under the lock: a stop that fired before it has taken every waker, and one that
            // fires after it finds this one.
            if self.state.stopped.load(Ordering::Relaxed) {
                drop(table);
                drop(held);
                return Poll::Ready(());
            }
            let (key, replaced) = match &self.registered {
                Some((key, _)) => (*key, table.slots[*key].replace(held)),
                None => (table.insert(held), None),
            };
            drop(table);
            drop(replaced);
            self.registered = Some((key, own));
            Poll::Pending
        }
    }

    impl Future for StopWaiter {
        type Output = ();

        #[inline]
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let this = self.get_mut();
            if this.state.stopped.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            match &this.registered {
                Some((_, own)) if own.will_wake(cx.waker()) => Poll::Pending,
                _ => this.register(cx.waker()),
            }
        }
    }

    impl Drop for StopWaiter {
        fn drop(&mut self) {
            if let Some((key, _own)) = self.registered.take() {
                // The table's waker is dropped after the lock, because dropping it may drop a task
                // that holds another waiter.
                let held = self.state.wakers.lock().remove(key);
                drop(held);
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::sync::atomic::AtomicUsize;
        use std::task::Wake;

        #[derive(Default)]
        struct CountingWaker(AtomicUsize);

        impl Wake for CountingWaker {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        fn counting() -> (Arc<CountingWaker>, Waker) {
            let count = Arc::new(CountingWaker::default());
            (Arc::clone(&count), Waker::from(count))
        }

        fn poll(waiter: &mut StopWaiter, waker: &Waker) -> Poll<()> {
            Pin::new(waiter).poll(&mut Context::from_waker(waker))
        }

        #[test]
        fn a_registered_waiter_is_woken_by_the_stop_and_then_ready() {
            let stop = Stop::default();
            let (count, waker) = counting();
            let mut waiter = stop.waiter();
            assert!(poll(&mut waiter, &waker).is_pending());
            assert!(poll(&mut waiter, &waker).is_pending());
            assert_eq!(count.0.load(Ordering::Relaxed), 0);
            stop.fire();
            assert_eq!(count.0.load(Ordering::Relaxed), 1);
            assert!(poll(&mut waiter, &waker).is_ready());
            assert!(poll(&mut waiter, &waker).is_ready());
        }

        #[test]
        fn a_waiter_made_after_the_stop_is_ready_at_its_first_poll() {
            let stop = Stop::default();
            stop.fire();
            let (count, waker) = counting();
            assert!(poll(&mut stop.waiter(), &waker).is_ready());
            assert_eq!(count.0.load(Ordering::Relaxed), 0);
        }

        #[test]
        fn the_stop_wakes_the_waker_of_the_last_poll_only() {
            let stop = Stop::default();
            let ((first, first_waker), (last, last_waker)) = (counting(), counting());
            let mut waiter = stop.waiter();
            assert!(poll(&mut waiter, &first_waker).is_pending());
            assert!(poll(&mut waiter, &last_waker).is_pending());
            stop.fire();
            assert_eq!(
                (first.0.load(Ordering::Relaxed), last.0.load(Ordering::Relaxed)),
                (0, 1)
            );
        }

        #[test]
        fn an_ended_waiter_is_not_woken_and_its_key_is_reused() {
            let stop = Stop::default();
            let ((ended, ended_waker), (kept, kept_waker)) = (counting(), counting());
            let mut kept_waiter = stop.waiter();
            assert!(poll(&mut kept_waiter, &kept_waker).is_pending());
            for _ in 0..3 {
                let mut waiter = stop.waiter();
                assert!(poll(&mut waiter, &ended_waker).is_pending());
            }
            assert_eq!(stop.0.wakers.lock().slots.len(), 2);
            stop.fire();
            assert_eq!(
                (ended.0.load(Ordering::Relaxed), kept.0.load(Ordering::Relaxed)),
                (0, 1)
            );
        }

        #[test]
        fn a_waiter_that_ends_after_the_stop_leaves_the_table_consistent() {
            let stop = Stop::default();
            let (_, waker) = counting();
            let mut waiter = stop.waiter();
            assert!(poll(&mut waiter, &waker).is_pending());
            stop.fire();
            drop(waiter);
            assert!(poll(&mut stop.waiter(), &waker).is_ready());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn the_io_runtime_starts_its_threads_with_python_stacks() {
        mipyre_runtime::testkit::thread_stack::with_exact_stacks(
            concat!(module_path!(), "::the_io_runtime_starts_its_threads_with_python_stacks"),
            || {
                let python =
                    mipyre_runtime::testkit::thread_stack::of_a_thread_with(mipyre_subinterp::PYTHON_THREAD_STACK);
                let rt = io_runtime(1).expect("the IO runtime builds");
                let (worker, blocking) = rt.block_on(async {
                    let worker = tokio::spawn(async { mipyre_runtime::testkit::thread_stack::current() });
                    let blocking = tokio::task::spawn_blocking(mipyre_runtime::testkit::thread_stack::current);
                    (
                        worker.await.expect("on a worker"),
                        blocking.await.expect("on the blocking pool"),
                    )
                });
                assert_eq!((worker, blocking), (python, python));
            },
        );
    }

    /// Stands in for a stop's drain budget; the drain waits exactly as long as it is given.
    const MULTI_THREAD_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

    /// Runs `body` on a paused-clock runtime, where a timer fires at its exact instant once the
    /// runtime has nothing else to do, with no real wait.
    fn on_paused_clock(body: impl std::future::Future<Output = ()>) {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap()
            .block_on(body);
    }

    /// Opens one connection per entry of `lasting`, each open for that long, on a tracker closed
    /// as the server closes it at the stop.
    fn open_connections(lasting: &[std::time::Duration]) -> tokio_util::task::TaskTracker {
        let tracker = tokio_util::task::TaskTracker::new();
        for &d in lasting {
            tracker.spawn(tokio::time::sleep(d));
        }
        tracker.close();
        tracker
    }

    /// Returns the drain timeouts and the connections they closed that a drain of `tracker`
    /// counted.
    async fn drained(tracker: &tokio_util::task::TaskTracker) -> (u64, u64) {
        let counters = ServerCounters::new(false);
        drain_connections(tracker, MULTI_THREAD_DRAIN_TIMEOUT, &counters).await;
        let counts = counters.read();
        (counts.drain_timeouts, counts.drain_closed_connections)
    }

    #[test]
    fn multi_thread_drain_waits_for_a_connection_that_ends_just_before_the_timeout() {
        on_paused_clock(async {
            let tracker = open_connections(&[Duration::from_millis(29_999)]);
            assert_eq!(drained(&tracker).await, (0, 0));
        });
    }

    #[test]
    fn a_multi_thread_drain_that_times_out_counts_the_connections_still_open() {
        on_paused_clock(async {
            let tracker = open_connections(&[
                Duration::from_millis(29_999),
                Duration::from_millis(30_001),
                Duration::MAX,
            ]);
            assert_eq!(drained(&tracker).await, (1, 2));
        });
    }

    /// Proves the drain is woken by the last connection ending, not by the timeout.
    #[test]
    fn multi_thread_drain_completes_when_the_last_connection_ends() {
        on_paused_clock(async {
            let tracker = open_connections(&[Duration::ZERO]);
            let started = tokio::time::Instant::now();
            assert_eq!(drained(&tracker).await, (0, 0));
            assert_eq!(started.elapsed(), Duration::ZERO, "no timer fired");
        });
    }
}
