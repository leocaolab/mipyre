//! Runs the sub-interpreter pool: the queues a server submits requests to, the threads that serve
//! them, and the watchdog that interrupts and replaces stuck sync workers.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use mipyre_core::HandoffBackend;

use mipyre_tpc::spawn;
use pyo3::prelude::*;

use super::engine_queue::{engine_queue, EngineJob, EngineReply, EngineSender};
use super::job_queue::{job_queue, JobReceiver, JobSendError, JobSender};
use super::live_workers::{LiveWorkers, WorkerKind, WorkerKindLoss};
use super::start::{start_here, StartError, StartGate};
use super::worker::async_engine::AsyncEngine;
use super::worker::start::WorkerRecipe;
use super::worker::sync_runner::SyncRunner;
use super::worker::{SubInterpreterWorker, WorkerIndex};
use crate::exchange::error::{refuse, Capacity, ErrorRecord, HandlerError, ReplyHolder, UnansweredError, WorkerGroup};
use crate::exchange::loop_wait::LoopMailbox;
use crate::exchange::request::Request;
use crate::websocket::{WsConnection, WsEngines};
use mipyre_http::plan::response::HandlerResponse;
use mipyre_http::plan::route::{HandlerKind, RouteId, RouteKey, RuntimeChoice, WsPlace};

mod serve;
mod split;
mod threads;
mod watchdog;

pub use self::split::{split_workers_for_routes, RouteKinds, WorkerCounts};
pub use self::threads::{AbandonedWorker, PoolThreads};

use self::serve::{serve_async, serve_sync};
use self::threads::{join_failed_start, python_thread, WorkerThread};
use self::watchdog::{watch, Replacer, Supervision, SyncLane};

/// Carries a reply to a request's caller; an error in it has already been logged.
pub(crate) type ReplySender<T> = tokio::sync::oneshot::Sender<Result<T, ErrorRecord>>;

/// Carries a worker's reply; an error in it has already been logged.
pub(crate) type PoolReplySender = ReplySender<HandlerResponse>;

/// Hands `value` to whoever waits on `tx`. The waiter is gone only when it gave up on the
/// request (it timed out, its client went away, the server stopped): there is nobody left
/// to tell, so the value is dropped — an error in it was already logged where it happened.
pub(crate) fn answer_if_awaited<T>(tx: tokio::sync::oneshot::Sender<T>, value: T) {
    #[expect(
        clippy::let_underscore_must_use,
        reason = "a closed oneshot means the waiter gave up on the request; see the doc above"
    )]
    let _ = tx.send(value);
}

pub struct PoolJob {
    pub(crate) route: mipyre_http::plan::route::RouteId,
    /// It is built on the Tokio thread and moves into the worker's interpreter unchanged.
    pub(crate) request: Request,
    pub(crate) reply: PoolReplySender,
}

impl PoolJob {
    pub(crate) fn into_request(self) -> (mipyre_http::plan::route::RouteId, Request, PoolReplySender) {
        (self.route, self.request, self.reply)
    }
}

/// The sync workers the pools' watchdogs abandoned whose threads still run, and the pools at their
/// limit of them, summed over the process's pools; each watchdog moves its own share.
static WORKERS_ABANDONED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static POOLS_AT_ABANDON_LIMIT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Moves one pool's share of the abandoned workers still running from `was` to `now`.
fn shift_workers_abandoned(was: u64, now: u64) {
    if now >= was {
        WORKERS_ABANDONED.fetch_add(now - was, std::sync::atomic::Ordering::Relaxed);
    } else {
        WORKERS_ABANDONED.fetch_sub(was - now, std::sync::atomic::Ordering::Relaxed);
    }
}

fn pool_reached_abandon_limit() {
    POOLS_AT_ABANDON_LIMIT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

fn pool_left_abandon_limit() {
    POOLS_AT_ABANDON_LIMIT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
}

/// Returns the sync workers the process's pools abandoned whose threads still run.
pub fn workers_abandoned() -> u64 {
    WORKERS_ABANDONED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Returns how many of the process's pools are at their limit of abandoned workers.
pub fn pools_at_abandon_limit() -> u64 {
    POOLS_AT_ABANDON_LIMIT.load(std::sync::atomic::Ordering::Relaxed)
}

// These count created and completed jobs for leak diagnostics. They sit behind `leak_detect`
// because a shared atomic bumped on every request bounces its cache line across every core and
// caps throughput.
#[cfg(feature = "leak_detect")]
static WR_CREATED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
#[cfg(feature = "leak_detect")]
static WR_COMPLETED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl PoolJob {
    #[inline(always)]
    pub(crate) fn inc_created() {
        #[cfg(feature = "leak_detect")]
        WR_CREATED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    /// Counts a request answered, or dropped because its caller gave up; every path calls it
    /// exactly once per request.
    #[inline(always)]
    pub(crate) fn inc_completed() {
        #[cfg(feature = "leak_detect")]
        WR_COMPLETED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    /// Returns `(created, completed)`, or `None` when the engine was built without `leak_detect`.
    pub fn counts() -> Option<(u64, u64)> {
        #[cfg(feature = "leak_detect")]
        {
            Some((
                WR_CREATED.load(std::sync::atomic::Ordering::Relaxed),
                WR_COMPLETED.load(std::sync::atomic::Ordering::Relaxed),
            ))
        }
        #[cfg(not(feature = "leak_detect"))]
        {
            None
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    /// `abandoned` are the started workers' threads still running when the failed start
    /// stopped waiting for them.
    #[error("{source}")]
    Start {
        source: StartError,
        abandoned: Vec<AbandonedWorker>,
    },
    #[error("could not spawn the thread of worker {index}: {source}")]
    ThreadNotSpawned {
        index: WorkerIndex,
        source: std::io::Error,
        abandoned: Vec<AbandonedWorker>,
    },
    #[error("could not spawn the sub-interpreter watchdog: {0}")]
    WatchdogNotSpawned(std::io::Error),
    #[error("could not make the async workers' doorbells: {0}")]
    DoorbellNotMade(std::io::Error),
}

impl PoolError {
    /// Returns the worker threads the failed start left running.
    pub fn abandoned(&self) -> &[AbandonedWorker] {
        match self {
            PoolError::Start { abandoned, .. } | PoolError::ThreadNotSpawned { abandoned, .. } => abandoned,
            PoolError::WatchdogNotSpawned(_) | PoolError::DoorbellNotMade(_) => &[],
        }
    }

    pub fn into_pyerr(self, py: Python<'_>) -> PyErr {
        match self {
            PoolError::Start { source, .. } => source.into_pyerr(py),
            other @ (PoolError::ThreadNotSpawned { .. }
            | PoolError::WatchdogNotSpawned(_)
            | PoolError::DoorbellNotMade(_)) => pyo3::exceptions::PyRuntimeError::new_err(other.to_string()),
        }
    }
}

/// Each worker adds this many requests to its queue's capacity; a request that finds the queue
/// full is answered 503.
const QUEUE_SLOTS_PER_WORKER: usize = 128;

/// Dropping the last reference closes the queues, which tells the workers to finish;
/// [`PoolThreads::join`] then waits for them.
pub struct InterpreterPool {
    sync_queue: Option<WorkerQueue>,
    async_queue: Option<AsyncWorkers>,
    /// Is `None` when no WebSocket route runs on a worker.
    websockets: Option<WsEngines>,
}

/// The queue of the `def` workers; `lives` says why it has no receiver left once its workers are
/// gone.
struct WorkerQueue {
    work_tx: JobSender<PoolJob>,
    lives: LiveWorkers,
}

/// The queue of the `async def` workers, each an engine; `lives` says why it has no engine left once
/// they are gone.
struct AsyncWorkers {
    jobs: EngineSender,
    workers: NonZeroUsize,
    lives: LiveWorkers,
}

impl InterpreterPool {
    /// Starts `split.total()` sub-interpreters, each on the thread that serves it: the first
    /// `split.sync_workers` serve `def` handlers, the rest `async def`. They are created one at a
    /// time in order; if any fails, the ones already created are ended on their own threads and
    /// the failure is returned (see [`StartGate`]). `async_capacity` bounds the `async def`
    /// requests in flight, queued or running, and defaults to the `def` queue's slots per worker.
    /// A kind with no worker left calls `tell_loss`.
    ///
    /// It releases the main GIL while waiting, because the threads create their interpreters
    /// under it.
    pub fn new(
        py: Python<'_>,
        split: WorkerCounts,
        recipe: &Arc<WorkerRecipe>,
        async_capacity: Option<NonZeroUsize>,
        tell_loss: &Arc<dyn Fn(WorkerKindLoss) + Send + Sync>,
        counters: &Arc<super::WorkerCounters>,
    ) -> Result<(Self, PoolThreads), PoolError> {
        #[cfg(feature = "startup_trace")]
        mipyre_subinterp::trace("pool_new_begin");
        let n = split.total();
        let shutdown_timeout = recipe.spec().limits.shutdown_timeout.get();
        let budget = recipe.spec().limits.request_budget.get();
        let sync_capacity = n * QUEUE_SLOTS_PER_WORKER;
        let async_capacity = async_capacity.map_or(sync_capacity, NonZeroUsize::get);
        let (sync_work_tx, sync_work_rx) = job_queue::<PoolJob>(sync_capacity);
        // No async worker: no engine queue and no doorbell.
        let (async_jobs, async_inboxes) = match NonZeroUsize::new(split.async_workers) {
            None => (None, Vec::new()),
            Some(workers) => {
                let (jobs, inboxes) =
                    engine_queue(async_capacity, workers.get()).map_err(PoolError::DoorbellNotMade)?;
                (Some((jobs, workers)), inboxes)
            }
        };
        let sync_count = NonZeroUsize::new(split.sync_workers);
        let serves_websockets = recipe.expected().websockets.iter().any(|w| w.place == WsPlace::Worker);
        let mailboxes: Vec<Option<Arc<LoopMailbox<WsConnection>>>> = (0..split.async_workers)
            .map(|_| {
                serves_websockets
                    .then(LoopMailbox::new)
                    .transpose()
                    .map(|m| m.map(Arc::new))
            })
            .collect::<std::io::Result<_>>()
            .map_err(PoolError::DoorbellNotMade)?;
        let websockets = serves_websockets.then(|| WsEngines::new(mailboxes.iter().flatten().cloned().collect()));

        let (sync_lives, sync_worker_lives) = LiveWorkers::new(
            WorkerKind::PoolSync,
            split.sync_workers,
            Arc::clone(tell_loss),
            counters,
        );
        let (async_lives, async_worker_lives) = LiveWorkers::new(
            WorkerKind::PoolAsync,
            split.async_workers,
            Arc::clone(tell_loss),
            counters,
        );
        let routes: Vec<RouteKey> = recipe.expected().routes.iter().map(|r| r.key.clone()).collect();
        let sup = Supervision::new(routes);
        let (stop_tx, stop_rx) = crossbeam_channel::bounded::<()>(0);
        let replacer = Arc::new(Replacer {
            recipe: Arc::clone(recipe),
            queue: sync_work_rx.clone(),
        });
        if let Some(abandon_limit) = sync_count {
            let (watch_sup, watch_replacer) = (Arc::clone(&sup), Arc::clone(&replacer));
            spawn(python_thread("mipyre-watchdog".into()), move || {
                watch(watch_sup, watch_replacer, stop_rx, abandon_limit)
            })
            .map_err(PoolError::WatchdogNotSpawned)?;
        }
        let mut threads = PoolThreads {
            threads: Vec::with_capacity(n),
            sup: Arc::clone(&sup),
            stop: stop_tx,
        };

        let mut gate = StartGate::<()>::new();
        let mut sync_lanes: Vec<Arc<SyncLane>> = Vec::with_capacity(split.sync_workers);
        for (i, life) in sync_worker_lives.into_iter().enumerate() {
            let index = WorkerIndex::new(i);
            let lane = SyncLane::new(i, Some(life));
            let start_handle = gate.enlist();
            let (rx, for_thread, recipe) = (sync_work_rx.clone(), Arc::clone(&lane), Arc::clone(recipe));
            let spawned = spawn(python_thread(format!("mipyre-worker-{index}")), move || {
                let Some((worker, ())) = start_here(start_handle, index, || {
                    SubInterpreterWorker::<SyncRunner>::create(index, &recipe.spec())
                }) else {
                    return;
                };
                serve_sync(worker, rx, &for_thread, budget)
            });
            match spawned {
                Ok(handle) => {
                    lane.hold_thread(handle);
                    sync_lanes.push(lane);
                }
                Err(source) => {
                    gate.abort();
                    let abandoned = join_failed_start(threads, &sync_lanes, shutdown_timeout);
                    return Err(PoolError::ThreadNotSpawned {
                        index,
                        source,
                        abandoned,
                    });
                }
            }
        }
        let mut async_handles: Vec<WorkerThread> = Vec::with_capacity(split.async_workers);
        for (((i, life), inbox), connections) in (split.sync_workers..n)
            .zip(async_worker_lives)
            .zip(async_inboxes)
            .zip(mailboxes)
        {
            let index = WorkerIndex::new(i);
            let start_handle = gate.enlist();
            let recipe = Arc::clone(recipe);
            let spawned = spawn(python_thread(format!("mipyre-async-worker-{index}")), move || {
                let Some((worker, ())) = start_here(start_handle, index, || {
                    SubInterpreterWorker::<AsyncEngine>::create(index, &recipe.spec())
                }) else {
                    return;
                };
                serve_async(worker, inbox, connections, life)
            });
            match spawned {
                Ok(handle) => async_handles.push(WorkerThread { handle, lane: None }),
                Err(source) => {
                    gate.abort();
                    threads.threads.append(&mut async_handles);
                    let abandoned = join_failed_start(threads, &sync_lanes, shutdown_timeout);
                    return Err(PoolError::ThreadNotSpawned {
                        index,
                        source,
                        abandoned,
                    });
                }
            }
        }

        #[cfg(feature = "startup_trace")]
        mipyre_subinterp::trace("spawn_loops_done");
        let settled = py.detach(|| gate.serve_or_abort(vec![(); n]));
        #[cfg(feature = "startup_trace")]
        mipyre_subinterp::trace("settled");
        if let Err(aborted) = settled {
            threads.threads.append(&mut async_handles);
            let abandoned = join_failed_start(threads, &sync_lanes, shutdown_timeout);
            return Err(PoolError::Start {
                source: aborted,
                abandoned,
            });
        }
        recipe.report_started(n);
        sup.watch_lanes(sync_lanes);
        threads.threads.append(&mut async_handles);

        let pool = InterpreterPool {
            sync_queue: sync_count.is_some().then(|| WorkerQueue {
                work_tx: sync_work_tx,
                lives: sync_lives,
            }),
            async_queue: async_jobs.map(|(jobs, workers)| AsyncWorkers {
                jobs,
                workers,
                lives: async_lives,
            }),
            websockets,
        };
        Ok((pool, threads))
    }

    /// Returns where this pool's WebSocket connections are handed, `None` when no WebSocket route
    /// runs on a worker.
    pub fn websocket_engines(&self) -> Option<&WsEngines> {
        self.websockets.as_ref()
    }

    pub fn async_workers(&self) -> usize {
        self.async_queue.as_ref().map_or(0, |queue| queue.workers.get())
    }

    /// Returns how many jobs the queues in use hold at most together, counting the `async def`
    /// ones running.
    pub fn queue_slots(&self) -> usize {
        let sync = self.sync_queue.as_ref().map_or(0, |queue| queue.work_tx.capacity());
        let async_ = self.async_queue.as_ref().map_or(0, |queue| queue.jobs.capacity());
        sync + async_
    }
}

impl HandoffBackend for InterpreterPool {
    type Ask = (RouteId, HandlerKind, Request);
    type Reply = Result<Result<HandlerResponse, ErrorRecord>, HandlerError>;
    type Refusal = UnansweredError;
    type Run = PoolRun;

    /// Queues the request on the worker kind that runs it; a full queue is refused, and the
    /// caller answers 503.
    fn submit(&self, (route, kind, request): Self::Ask) -> Result<PoolRun, UnansweredError> {
        // A request of a kind the pool has no workers for is a mistake in building the server,
        // not a closed pool, so that request alone is refused and the server keeps serving.
        let queue = match kind {
            HandlerKind::Sync => self.sync_queue.as_ref(),
            HandlerKind::Async => return self.submit_async(route, request),
        };
        let queue = queue.ok_or(UnansweredError::NoWorkers(RuntimeChoice::Worker))?;
        let (reply, answered) = tokio::sync::oneshot::channel();
        queue.submit(PoolJob { route, request, reply })?;
        PoolJob::inc_created();
        Ok(PoolRun(answered))
    }
}

/// One queued job's reply. Dropping it closes the reply: a worker that has not started the job
/// sees that and skips it, and one that has started it runs it to the end.
pub struct PoolRun(tokio::sync::oneshot::Receiver<Result<HandlerResponse, ErrorRecord>>);

impl Future for PoolRun {
    type Output = Result<Result<HandlerResponse, ErrorRecord>, HandlerError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|reply| reply.map_err(|_| refuse(UnansweredError::WorkerLost(ReplyHolder::PoolWorker))))
    }
}

impl WorkerQueue {
    fn submit(&self, req: PoolJob) -> Result<(), UnansweredError> {
        self.work_tx.try_send(req).map_err(|e| refused(e, &self.lives))
    }
}

impl InterpreterPool {
    /// Queues an `async def` request for the async workers; out of line, so the `def` path's code
    /// does not carry it.
    #[inline(never)]
    fn submit_async(&self, route: RouteId, request: Request) -> Result<PoolRun, UnansweredError> {
        let workers = self.async_queue.as_ref();
        let workers = workers.ok_or(UnansweredError::NoWorkers(RuntimeChoice::Worker))?;
        let (reply, answered) = tokio::sync::oneshot::channel();
        let job = EngineJob {
            handler: route.index(),
            request,
            reply: EngineReply::Pool(reply),
        };
        workers.jobs.submit(job).map_err(|e| refused(e, &workers.lives))?;
        PoolJob::inc_created();
        Ok(PoolRun(answered))
    }
}

#[cold]
fn refused(e: JobSendError, lives: &LiveWorkers) -> UnansweredError {
    let group = WorkerGroup::Subinterp;
    match e {
        JobSendError::QueueFull => UnansweredError::Overloaded(Capacity::WorkQueue(group)),
        // The cause is read only here, off the hot path.
        JobSendError::NoReceiverLeft => match lives.first_death() {
            Some(cause) => UnansweredError::WorkersDied { group, cause },
            None => UnansweredError::WorkersGone(group),
        },
    }
}

/// Answers everything still queued with a 503 naming `last` when the dying worker was the last of
/// its kind, rather than leave it to the request budget's 504. A request queued between this
/// drain and the receiver's drop still waits for its 504.
pub(super) fn answer_queue_if_last(last: Option<Arc<str>>, queued: &JobReceiver<PoolJob>) {
    let Some(cause) = last else {
        return;
    };
    while let Some(job) = queued.try_recv() {
        let (_, request, reply) = job.into_request();
        let label = request.label();
        let refused = refuse(UnansweredError::WorkersDied {
            group: WorkerGroup::Subinterp,
            cause: Arc::clone(&cause),
        });
        answer_if_awaited(reply, Err(refused.log(&label)));
        PoolJob::inc_completed();
    }
}

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    use super::*;
    use mipyre_http::plan::route::{HandlerPlace, RouteTable};

    type ReplyReceiver = tokio::sync::oneshot::Receiver<Result<HandlerResponse, ErrorRecord>>;

    fn queue_of(n: usize) -> (JobReceiver<PoolJob>, Vec<ReplyReceiver>) {
        let mut table = RouteTable::<(), ()>::new();
        table
            .insert("GET", "/", (), "index".into(), HandlerPlace::Worker(HandlerKind::Async))
            .expect("one route");
        let route = table.route_id(0);
        let (tx, rx) = job_queue(n);
        let replies = (0..n)
            .map(|_| {
                let (parts, ()) = hyper::Request::get("/").body(()).expect("a valid request").into_parts();
                let head = mipyre_http::io::request_head::RequestHead {
                    parts,
                    params: Default::default(),
                    client: mipyre_transport::driver::Peer::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
                    request_id: mipyre_http::plan::request_id::RequestId::mint(),
                };
                let (reply, answered) = tokio::sync::oneshot::channel();
                tx.try_send(PoolJob {
                    route,
                    request: Request::new(head, crate::exchange::RequestBody::Buffered(Default::default())),
                    reply,
                })
                .expect("the queue is open");
                answered
            })
            .collect();
        (rx, replies)
    }

    fn ask(kind: HandlerKind) -> (RouteId, HandlerKind, Request) {
        let mut table = RouteTable::<(), ()>::new();
        table
            .insert("GET", "/", (), "index".into(), HandlerPlace::Worker(kind))
            .expect("one route");
        let (parts, ()) = hyper::Request::get("/").body(()).expect("a valid request").into_parts();
        let head = mipyre_http::io::request_head::RequestHead {
            parts,
            params: Default::default(),
            client: mipyre_transport::driver::Peer::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
            request_id: mipyre_http::plan::request_id::RequestId::mint(),
        };
        let request = Request::new(head, crate::exchange::RequestBody::Buffered(Default::default()));
        (table.route_id(0), kind, request)
    }

    /// Builds a pool with only `async def` workers, as a TPC server has, without starting an
    /// interpreter; the inbox stands in for its one engine.
    fn async_only_pool() -> (crate::engine_queue::EngineInbox, InterpreterPool) {
        let (jobs, inbox) = crate::engine_queue::one_engine_queue(4).expect("a doorbell");
        let counters = Arc::new(crate::WorkerCounters::new());
        let pool = InterpreterPool {
            sync_queue: None,
            async_queue: Some(AsyncWorkers {
                jobs,
                workers: NonZeroUsize::MIN,
                lives: LiveWorkers::new(WorkerKind::PoolAsync, 1, Arc::new(|_| {}), &counters).0,
            }),
            websockets: None,
        };
        (inbox, pool)
    }

    /// Builds a pool with only `def` workers without starting an interpreter; the receiver stands
    /// in for its one worker.
    fn sync_only_pool() -> (JobReceiver<PoolJob>, InterpreterPool) {
        let (work_tx, rx) = job_queue(4);
        let counters = Arc::new(crate::WorkerCounters::new());
        let pool = InterpreterPool {
            sync_queue: Some(WorkerQueue {
                work_tx,
                lives: LiveWorkers::new(WorkerKind::PoolSync, 1, Arc::new(|_| {}), &counters).0,
            }),
            async_queue: None,
            websockets: None,
        };
        (rx, pool)
    }

    /// Returns the jobs the engine takes, without the places they hold in flight.
    fn taken(inbox: &crate::engine_queue::EngineInbox) -> Vec<EngineJob> {
        let taken = inbox.take().expect("the doorbell reads").expect("the queue is open");
        taken.into_iter().map(|(job, _)| job).collect()
    }

    fn pool_reply(job: EngineJob) -> PoolReplySender {
        match job.reply {
            EngineReply::Pool(reply) => reply,
            EngineReply::Main(_) => panic!("a pool job answers the pool"),
        }
    }

    #[test]
    fn a_request_of_a_kind_the_pool_has_no_workers_for_is_refused_as_no_workers() {
        let (inbox, pool) = async_only_pool();
        assert!(matches!(
            pool.submit(ask(HandlerKind::Sync)),
            Err(UnansweredError::NoWorkers(RuntimeChoice::Worker))
        ));
        assert!(taken(&inbox).is_empty(), "the sync request went onto the async queue");

        let run = pool.submit(ask(HandlerKind::Async));
        assert!(run.is_ok());
        assert_eq!(taken(&inbox).len(), 1);
    }

    #[test]
    fn the_queue_slots_are_those_of_the_queues_in_use() {
        let (_inbox, pool) = async_only_pool();
        assert_eq!(pool.queue_slots(), 4);
    }

    #[tokio::test]
    async fn a_queued_job_replies_through_its_run() {
        let (inbox, pool) = async_only_pool();
        let run = pool.submit(ask(HandlerKind::Async)).expect("the queue has room");
        let job = taken(&inbox).pop().expect("the job was queued");
        let answer = HandlerResponse::new(
            hyper::StatusCode::OK,
            bytes::Bytes::from_static(b"{}"),
            mipyre_http::plan::response::JSON,
            mipyre_http::plan::response::HeaderLines::new(),
        );
        answer_if_awaited(pool_reply(job), Ok(answer));
        assert!(matches!(run.await, Ok(Ok(response)) if response.body.as_ref() == b"{}"));
    }

    #[tokio::test]
    async fn a_job_its_worker_drops_unanswered_is_lost() {
        let (inbox, pool) = async_only_pool();
        let run = pool.submit(ask(HandlerKind::Async)).expect("the queue has room");
        drop(taken(&inbox));
        assert!(matches!(
            run.await,
            Err(HandlerError::Refused(UnansweredError::WorkerLost(
                ReplyHolder::PoolWorker
            )))
        ));
    }

    #[test]
    fn a_job_whose_run_was_dropped_is_not_taken() {
        let (inbox, pool) = async_only_pool();
        drop(pool.submit(ask(HandlerKind::Async)).expect("the queue has room"));
        assert!(
            taken(&inbox).is_empty(),
            "a worker would still run the job nobody waits for"
        );
    }

    #[test]
    fn a_def_job_whose_run_was_dropped_reaches_its_worker_with_its_reply_closed() {
        let (rx, pool) = sync_only_pool();
        drop(pool.submit(ask(HandlerKind::Sync)).expect("the queue has room"));
        let job = rx.try_recv().expect("the job was queued");
        assert!(
            job.reply.is_closed(),
            "a worker would still run the job nobody waits for"
        );
    }

    #[test]
    fn a_def_job_whose_run_is_held_reaches_its_worker_with_its_reply_open() {
        let (rx, pool) = sync_only_pool();
        let _run = pool.submit(ask(HandlerKind::Sync)).expect("the queue has room");
        let job = rx.try_recv().expect("the job was queued");
        assert!(!job.reply.is_closed(), "a worker would skip a job its caller waits for");
    }

    #[test]
    fn running_async_requests_count_against_the_limit_until_their_tasks_end() {
        let (inbox, pool) = async_only_pool();
        let runs: Vec<_> = (0..4)
            .map(|_| pool.submit(ask(HandlerKind::Async)).expect("room for four"))
            .collect();
        let running = inbox.take().expect("the doorbell reads").expect("the queue is open");
        assert!(matches!(
            pool.submit(ask(HandlerKind::Async)),
            Err(UnansweredError::Overloaded(Capacity::WorkQueue(WorkerGroup::Subinterp)))
        ));
        drop(running);
        assert!(pool.submit(ask(HandlerKind::Async)).is_ok());
        drop(runs);
    }

    #[test]
    fn the_last_worker_to_die_answers_the_queue_with_its_cause() {
        let (queued, replies) = queue_of(2);
        answer_queue_if_last(Some("worker 1's engine raised".into()), &queued);
        let left = std::iter::from_fn(|| queued.try_recv()).count();
        assert_eq!(left, 0, "{left} request(s) left queued");
        for mut reply in replies {
            let answer = reply.try_recv().expect("answered at once");
            let Err(record) = answer else {
                panic!("a queued request was answered with a response");
            };
            let (error, _) = record.into_error_and_id();
            match error {
                HandlerError::Refused(UnansweredError::WorkersDied {
                    group: WorkerGroup::Subinterp,
                    cause,
                }) => assert_eq!(&*cause, "worker 1's engine raised"),
                other => panic!("answered with {other}, not WorkersDied"),
            }
        }
    }

    #[test]
    fn a_worker_that_is_not_the_last_leaves_the_queue_as_it_is() {
        let (queued, mut replies) = queue_of(2);
        answer_queue_if_last(None, &queued);
        for reply in &mut replies {
            assert!(
                matches!(reply.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)),
                "a request was answered though workers of its kind still serve"
            );
        }
        assert_eq!(std::iter::from_fn(|| queued.try_recv()).count(), 2);
    }
}
