//! Runs the sub-interpreter pool: the queues a server submits requests to, the threads that serve
//! them, and the watchdog that interrupts and replaces stuck sync workers.

use std::num::NonZeroUsize;
use std::sync::Arc;

use mipyre_tpc::spawn;
use pyo3::prelude::*;

use super::job_queue::{job_queue, JobReceiver, JobSendError, JobSender};
use super::live_workers::{LiveWorkers, ServerRunStop, WorkerKind};
use super::start::{start_here, StartError, StartGate};
use super::worker::async_engine::AsyncEngine;
use super::worker::start::WorkerRecipe;
use super::worker::sync_runner::SyncRunner;
use super::worker::{SubInterpreterWorker, WorkerIndex};
use super::worker_api::AsyncInbox;
use crate::exchange::error::{refuse, Capacity, ErrorRecord, UnansweredError, WorkerGroup};
use crate::exchange::request::Request;
use mipyre_http::plan::response::HandlerResponse;
use mipyre_http::plan::route::RouteKey;

mod serve;
mod split;
mod threads;
mod watchdog;

pub(crate) use self::split::{split_workers_for_routes, RouteKinds, WorkerCounts};
pub(crate) use self::threads::{AbandonedWorker, PoolThreads};

use self::serve::{serve_async, serve_sync};
use self::threads::{join_failed_start, python_thread, RunningSlot, WorkerThread};
use self::watchdog::{watch, Replacer, Supervision, SyncLane};

/// Carries a worker's reply; an error in it has already been logged.
pub(crate) type PoolReplySender =
    tokio::sync::oneshot::Sender<Result<HandlerResponse, ErrorRecord>>;

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

pub(crate) struct PoolJob {
    pub(crate) route: mipyre_http::plan::route::RouteId,
    pub(crate) kind: mipyre_http::plan::route::HandlerKind,
    /// It is built on the Tokio thread and moves into the worker's interpreter unchanged.
    pub(crate) request: Request,
    pub(crate) reply: PoolReplySender,
}

impl PoolJob {
    pub(crate) fn into_request(self) -> (mipyre_http::plan::route::RouteId, Request, PoolReplySender) {
        (self.route, self.request, self.reply)
    }
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
    pub(crate) fn counts() -> Option<(u64, u64)> {
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
pub(crate) enum PoolError {
    #[error(transparent)]
    Start(#[from] StartError),
    #[error("could not spawn the thread of worker {index}: {source}")]
    ThreadNotSpawned {
        index: WorkerIndex,
        source: std::io::Error,
    },
    #[error("could not spawn the sub-interpreter watchdog: {0}")]
    WatchdogNotSpawned(std::io::Error),
}

impl PoolError {
    pub(crate) fn into_pyerr(self, py: Python<'_>) -> PyErr {
        match self {
            PoolError::Start(e) => e.into_pyerr(py),
            other @ (PoolError::ThreadNotSpawned { .. } | PoolError::WatchdogNotSpawned(_)) => {
                pyo3::exceptions::PyRuntimeError::new_err(other.to_string())
            }
        }
    }
}

/// Each worker adds this many requests to its queue's capacity; a request that finds the queue
/// full is answered 503.
const QUEUE_SLOTS_PER_WORKER: usize = 128;

/// Dropping the last reference closes the queues, which tells the workers to finish;
/// [`PoolThreads::join`] then waits for them.
pub(crate) struct InterpreterPool {
    sync_queue: Option<WorkerQueue>,
    async_queue: Option<WorkerQueue>,
    /// A request whose body is past the handler's skip size (`ADMISSION_SKIP_BYTES`) holds a
    /// permit from buffering until its response is ready, so a surge of uploads cannot pile
    /// N × max_body_size up in RAM. Small bodies take none, so a permit does not reserve a
    /// queue slot.
    pub(crate) submit_semaphore: Arc<tokio::sync::Semaphore>,
}

/// The queue of one worker kind; `lives` says why it has no receiver left once its workers are
/// gone.
struct WorkerQueue {
    work_tx: JobSender<PoolJob>,
    workers: NonZeroUsize,
    lives: LiveWorkers,
}

impl InterpreterPool {
    /// Starts `split.total()` sub-interpreters, each on the thread that serves it: the first
    /// `split.sync_workers` serve `def` handlers, the rest `async def`. They are created one at a
    /// time in order; if any fails, the ones already created are ended on their own threads and
    /// the failure is returned (see [`StartGate`]). `async_capacity` sizes the `async def` queue
    /// and defaults to the `def` queue's slots per worker. A kind with no worker left stops
    /// `stop`.
    ///
    /// It releases the main GIL while waiting, because the threads create their interpreters
    /// under it.
    pub(crate) fn new(
        py: Python<'_>,
        split: WorkerCounts,
        recipe: &Arc<WorkerRecipe>,
        async_capacity: Option<NonZeroUsize>,
        stop: &ServerRunStop,
        counters: &Arc<crate::telemetry::RunCounters>,
    ) -> Result<(Self, PoolThreads), PoolError> {
        #[cfg(feature = "startup_trace")]
        mipyre_subinterp::trace("pool_new_begin");
        let n = split.total();
        let shutdown_timeout = recipe.spec().limits.shutdown_timeout.get();
        let budget = recipe.spec().limits.request_budget.get();
        let sync_capacity = n * QUEUE_SLOTS_PER_WORKER;
        let async_capacity = async_capacity.map_or(sync_capacity, NonZeroUsize::get);
        let (sync_work_tx, sync_work_rx) = job_queue::<PoolJob>(sync_capacity);
        let (async_work_tx, async_work_rx) = job_queue::<PoolJob>(async_capacity);
        let sync_count = NonZeroUsize::new(split.sync_workers);
        let async_count = NonZeroUsize::new(split.async_workers);
        // The queues in use bound how many large bodies may be buffered at once.
        let total_permits =
            sync_count.map_or(0, |_| sync_capacity) + async_count.map_or(0, |_| async_capacity);

        let (sync_lives, sync_worker_lives) = LiveWorkers::new(
            WorkerKind::PoolSync,
            split.sync_workers,
            stop.clone(),
            Arc::clone(counters),
        );
        let (async_lives, async_worker_lives) = LiveWorkers::new(
            WorkerKind::PoolAsync,
            split.async_workers,
            stop.clone(),
            Arc::clone(counters),
        );
        let routes: Vec<RouteKey> = recipe
            .expected()
            .routes
            .iter()
            .map(|r| r.key.clone())
            .collect();
        let sup = Supervision::new(routes);
        let (stop_tx, stop_rx) = crossbeam_channel::bounded::<()>(0);
        let replacer = Arc::new(Replacer {
            recipe: Arc::clone(recipe),
            queue: sync_work_rx.clone(),
        });
        if sync_count.is_some() {
            let (watch_sup, watch_replacer) = (Arc::clone(&sup), Arc::clone(&replacer));
            spawn(python_thread("mipyre-watchdog".into()), move || {
                watch(watch_sup, watch_replacer, stop_rx)
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
                    join_failed_start(threads, &sync_lanes, shutdown_timeout);
                    return Err(PoolError::ThreadNotSpawned { index, source });
                }
            }
        }
        let mut async_handles: Vec<WorkerThread> = Vec::with_capacity(split.async_workers);
        for (i, life) in (split.sync_workers..n).zip(async_worker_lives) {
            let index = WorkerIndex::new(i);
            let start_handle = gate.enlist();
            let (queued, recipe) = (async_work_rx.clone(), Arc::clone(recipe));
            let spawned = spawn(python_thread(format!("mipyre-async-worker-{index}")), move || {
                let Some((worker, ())) = start_here(start_handle, index, || {
                    SubInterpreterWorker::<AsyncEngine>::create(index, &recipe.spec())
                }) else {
                    return;
                };
                let inbox = AsyncInbox::new(queued.clone(), recipe.spec().limits.request_budget);
                serve_async(worker, inbox, &queued, life)
            });
            match spawned {
                Ok(handle) => async_handles.push(WorkerThread {
                    handle,
                    running: RunningSlot::idle(),
                }),
                Err(source) => {
                    gate.abort();
                    threads.threads.append(&mut async_handles);
                    join_failed_start(threads, &sync_lanes, shutdown_timeout);
                    return Err(PoolError::ThreadNotSpawned { index, source });
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
            join_failed_start(threads, &sync_lanes, shutdown_timeout);
            return Err(PoolError::Start(aborted));
        }
        recipe.report_started(n);
        sup.watch_lanes(sync_lanes);
        threads.threads.append(&mut async_handles);

        let pool = InterpreterPool {
            sync_queue: sync_count.map(|workers| WorkerQueue {
                work_tx: sync_work_tx,
                workers,
                lives: sync_lives,
            }),
            async_queue: async_count.map(|workers| WorkerQueue {
                work_tx: async_work_tx,
                workers,
                lives: async_lives,
            }),
            submit_semaphore: Arc::new(tokio::sync::Semaphore::new(total_permits)),
        };
        Ok((pool, threads))
    }

    pub(crate) fn async_workers(&self) -> usize {
        self.async_queue
            .as_ref()
            .map_or(0, |queue| queue.workers.get())
    }

    /// Queues a request on the worker kind that runs it without waiting; a full queue is an
    /// error the caller answers with 503.
    pub(crate) fn submit(&self, req: PoolJob) -> Result<(), UnansweredError> {
        // A request of a kind the pool has no workers for is a mistake in building the server,
        // not a closed pool, so that request alone is refused and the server keeps serving.
        let queue = match req.kind {
            mipyre_http::plan::route::HandlerKind::Sync => self.sync_queue.as_ref(),
            mipyre_http::plan::route::HandlerKind::Async => self.async_queue.as_ref(),
        };
        queue
            .ok_or(UnansweredError::NoWorkers(WorkerGroup::Subinterp))?
            .submit(req)
    }
}

impl WorkerQueue {
    fn submit(&self, req: PoolJob) -> Result<(), UnansweredError> {
        let group = WorkerGroup::Subinterp;
        self.work_tx.try_send(req).map_err(|e| match e {
            JobSendError::QueueFull => {
                UnansweredError::Overloaded(Capacity::WorkQueue(group))
            }
            // The cause is read only here, off the hot path.
            JobSendError::NoReceiverLeft => match self.lives.first_death() {
                Some(cause) => UnansweredError::WorkersDied { group, cause },
                None => UnansweredError::WorkersGone(group),
            },
        })
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
mod tests {
    use super::*;
    use crate::exchange::error::HandlerError;
    use mipyre_http::plan::route::{HandlerKind, HandlerPlace, RouteTable};

    type ReplyReceiver = tokio::sync::oneshot::Receiver<Result<HandlerResponse, ErrorRecord>>;

    fn queue_of(
        n: usize,
    ) -> (JobReceiver<PoolJob>, Vec<ReplyReceiver>) {
        let mut table = RouteTable::<(), ()>::new();
        table
            .insert("GET", "/", (), "index".into(), HandlerPlace::Worker(HandlerKind::Async))
            .expect("one route");
        let route = table.route_id(0);
        let (tx, rx) = job_queue(n);
        let replies = (0..n)
            .map(|_| {
                let (parts, ()) = hyper::Request::get("/")
                    .body(())
                    .expect("a valid request")
                    .into_parts();
                let head = crate::exchange::RequestHead {
                    parts,
                    params: Vec::new(),
                    client: mipyre_transport::driver::Peer::Ip(
                        std::net::Ipv4Addr::LOCALHOST.into(),
                    ),
                    request_id: mipyre_http::io::request_id::RequestId::mint(),
                };
                let (reply, answered) = tokio::sync::oneshot::channel();
                tx.try_send(PoolJob {
                    route,
                    kind: HandlerKind::Async,
                    request: Request::new(
                        head,
                        crate::exchange::RequestBody::Buffered(Default::default()),
                    ),
                    reply,
                })
                .expect("the queue is open");
                answered
            })
            .collect();
        (rx, replies)
    }

    fn job(kind: HandlerKind) -> (PoolJob, ReplyReceiver) {
        let mut table = RouteTable::<(), ()>::new();
        table
            .insert("GET", "/", (), "index".into(), HandlerPlace::Worker(kind))
            .expect("one route");
        let (parts, ()) = hyper::Request::get("/")
            .body(())
            .expect("a valid request")
            .into_parts();
        let head = crate::exchange::RequestHead {
            parts,
            params: Vec::new(),
            client: mipyre_transport::driver::Peer::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
            request_id: mipyre_http::io::request_id::RequestId::mint(),
        };
        let (reply, answered) = tokio::sync::oneshot::channel();
        let job = PoolJob {
            route: table.route_id(0),
            kind,
            request: Request::new(
                head,
                crate::exchange::RequestBody::Buffered(Default::default()),
            ),
            reply,
        };
        (job, answered)
    }

    /// Builds a pool with only `async def` workers, as a TPC server has, without starting an
    /// interpreter.
    fn async_only_pool() -> (JobReceiver<PoolJob>, InterpreterPool) {
        let (work_tx, work_rx) = job_queue(4);
        let counters = Arc::new(crate::telemetry::RunCounters::new(false));
        let pool = InterpreterPool {
            sync_queue: None,
            async_queue: Some(WorkerQueue {
                work_tx,
                workers: NonZeroUsize::MIN,
                lives: LiveWorkers::new(WorkerKind::PoolAsync, 1, <ServerRunStop>::default(), counters).0,
            }),
            submit_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
        };
        (work_rx, pool)
    }

    #[test]
    fn a_request_of_a_kind_the_pool_has_no_workers_for_is_refused_as_no_workers() {
        let (queued, pool) = async_only_pool();
        let (sync, _) = job(HandlerKind::Sync);
        assert!(matches!(
            pool.submit(sync),
            Err(UnansweredError::NoWorkers(WorkerGroup::Subinterp))
        ));
        assert!(queued.try_recv().is_none(), "the sync request went onto the async queue");

        let (r#async, _) = job(HandlerKind::Async);
        assert!(pool.submit(r#async).is_ok());
        assert_eq!(std::iter::from_fn(|| queued.try_recv()).count(), 1);
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
