//! Runs `runtime="main"` routes and the fallback on the main interpreter, for C extensions that
//! cannot load in a sub-interpreter, on every kind of server.
//!
//! A `def` handler runs on one of several plain threads because one doing I/O releases the GIL but
//! still blocks its thread; with one thread, GIL work would queue behind that I/O while main sits
//! idle. They grow from the fewest kept to the most allowed when a request finds none asleep, and
//! one idle past a keepalive retires, so a server can keep hundreds of handlers in I/O without
//! starting hundreds of threads. `async def` routes run on one more thread, main's async engine
//! (`_worker/async_engine.py`, as an async worker runs them), started only when the table has one:
//! an awaiting handler gives the loop to the others, so how many await at once is not bounded by
//! the threads.
//!
//! The queues are bounded: a saturated main interpreter answers 503 at once instead of queueing
//! memory behind a path slower than the inbound rate; the engine's bound counts its running
//! requests too. It does not know which server uses it; it is given its sizes.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use mipyre_core::HandoffBackend;
use mipyre_http::io::static_files::StaticMount;
use mipyre_tpc::ShutdownDeadline;
use mipyre_util::throttle::LogThrottle;
use pyo3::prelude::*;
use tokio::sync::oneshot;

use super::engine_queue::{one_engine_queue, EngineInbox, EngineJob, EngineReply, EngineSender};
use super::job_queue::{job_queue, JobReceiver, JobSendError, JobSender};
use super::worker::async_engine::{run_engine_module, EngineFailure, EngineRoutes};
use crate::exchange::error::catch_panic;
use crate::exchange::error::{refuse, Capacity, ErrorRecord, HandlerError, ReplyHolder, UnansweredError, WorkerGroup};
use crate::exchange::request::Request;
use crate::exchange::websocket::{Outbox, WsClose};
use crate::exchange::HandlerReply;
use crate::hook_chain::Chain;
use crate::main_call::{main_chain, ThreadLoop};
use crate::request_context::in_reused_context;
use crate::sync::{Mutex, OnceLock};
use crate::websocket::{self, SessionKey, Sessions, WsEvent, WsSession, WsUpgradeAnswer};
use crate::{answer_if_awaited, call_handler_with_hooks, PoolJob};
use mipyre_http::io::request_head::RequestLabel;
use mipyre_http::plan::route::{HandlerPlace, HandlerTarget, MainHandler, WsRouteId};
use mipyre_http::plan::site::SiteRules;
use mipyre_subinterp::context::SpareContext;
use mipyre_tpc::{OsThread, Spawner, ThreadJoinError};

pub(crate) type MainRules = SiteRules<Py<PyAny>, StaticMount>;

/// Keeps at most this many `def` threads when the most allowed is larger; a server that never
/// grows past them starts them all and never retires one.
const FEWEST_DEF_THREADS: usize = 4;

/// Retires a `def` thread past the fewest after this long idle, as tokio's blocking pool does.
const DEF_THREAD_KEEPALIVE: Duration = Duration::from_secs(10);

/// Holds back repeats of a `def` thread the OS would not start, which a burst would log per request.
const NOT_STARTED_LOG_PERIOD: Duration = Duration::from_secs(60);

/// Sizes the main interpreter's `def` threads and its queues; `queue` is the total before 503, not
/// a per-thread one, for the `def` threads' queue and, queued or running, for the async engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MainSizes {
    pub max_threads: NonZeroUsize,
    pub queue: NonZeroUsize,
}

impl MainSizes {
    /// Returns how many `def` threads start with the server and are never retired.
    fn min_threads(self) -> usize {
        FEWEST_DEF_THREADS.min(self.max_threads.get())
    }
}

/// Carries a request to a `def` thread, and its answer back.
///
/// A streamed answer's receiver is `Send`, so it crosses back to the serving thread, whose body
/// writer drives it.
pub(crate) struct MainJob {
    pub(crate) target: HandlerTarget,
    pub(crate) request: Request,
    pub(crate) reply: MainReplySender,
}

pub(crate) type MainReplySender = oneshot::Sender<Result<HandlerReply, ErrorRecord>>;

/// What the `def` threads take from their queue: a request, or a call of a `runtime="main"`
/// WebSocket connection.
/// A WebSocket call is boxed so that a request, the def path's work, moves no more bytes than
/// before there were two kinds.
#[expect(
    clippy::large_enum_variant,
    reason = "boxing the request instead would allocate once per request on the def path"
)]
pub(crate) enum MainWork {
    Request(MainJob),
    WebSocket(Box<MainWsJob>),
}

/// One call of a WebSocket connection on main, with where its answer goes.
pub(crate) struct MainWsJob {
    route: WsRouteId,
    call: MainWsCall,
}

enum MainWsCall {
    /// The request is boxed, as an upgrade is once per connection and the calls are many.
    Open {
        request: Box<Request>,
        outbox: Arc<Outbox>,
        reply: oneshot::Sender<Result<WsUpgradeAnswer, HandlerError>>,
    },
    Event {
        session: SessionKey,
        event: WsEvent,
        reply: oneshot::Sender<Result<(), HandlerError>>,
    },
    /// `on_close` is called when `close` says how the connection ended; the label names the
    /// connection in the log line of one that raised.
    Close {
        session: WsSession,
        close: Option<WsClose>,
        label: RequestLabel,
    },
}

/// Reports what the OS would not give while the server starts; it fails the start, because
/// serving with fewer threads than the fewest kept would be a silent capacity cut.
#[derive(Debug, thiserror::Error)]
pub enum MainThreadStartError {
    #[error("could not start main-interpreter thread {thread} of {requested}: {source}")]
    NotStarted {
        thread: usize,
        requested: usize,
        #[source]
        source: std::io::Error,
    },
    #[error("could not make the doorbell of main's async engine: {0}")]
    DoorbellNotMade(#[source] std::io::Error),
}

impl From<MainThreadStartError> for PyErr {
    fn from(e: MainThreadStartError) -> Self {
        pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
    }
}

/// Sends work to the main interpreter's threads; they exit once every sender is gone.
pub struct MainThreads {
    tx: JobSender<MainWork>,
    /// Is `None` when the table has no `async def` main route.
    engine: Option<MainEngine>,
    max_threads: usize,
    set: Arc<DefThreadSet>,
}

/// What every `def` thread is started from, and every one started.
struct DefThreadSet {
    /// Is the template a new thread copies once attached, so no reference count is shared between
    /// threads and the thread starting it never touches Python.
    rules: MainRules,
    min_threads: usize,
    /// Holds the `ws` of every `runtime="main"` WebSocket connection; its last reference goes
    /// attached to main.
    sessions: Arc<Mutex<Sessions>>,
    roster: Mutex<ThreadRoster>,
    not_started_log: LogThrottle,
}

struct ThreadRoster {
    threads: Vec<OsThread<()>>,
    /// Counts the threads ever started, which numbers the next one's name.
    numbered: usize,
}

/// Feeds main's async engine, and says why it stopped once it has.
struct MainEngine {
    jobs: EngineSender,
    died: Arc<OnceLock<Arc<str>>>,
}

/// Owns what the server joins when it stops: every `def` thread started and the engine's.
pub struct MainThreadHandles {
    set: Arc<DefThreadSet>,
    engine: Option<OsThread<()>>,
}

impl MainThreads {
    /// Starts the fewest `def` threads `sizes` keeps on one queue, and after them main's async
    /// engine when the table has an `async def` main route.
    ///
    /// # Errors
    ///
    /// Fails when a thread does not start; the threads already started are joined within the
    /// app's `shutdown_timeout` before the error returns.
    pub fn spawn(
        py: Python<'_>,
        rules: &MainRules,
        sizes: MainSizes,
        spawner: &mut impl Spawner,
    ) -> Result<(Self, MainThreadHandles), MainThreadStartError> {
        let min_threads = sizes.min_threads();
        let retires = min_threads < sizes.max_threads.get();
        let shutdown_timeout = rules.settings.limits.shutdown_timeout.get();
        let (tx, rx) = job_queue::<MainWork>(sizes.queue.get());
        let set = Arc::new(DefThreadSet {
            rules: crate::copy_rules(rules, py),
            min_threads,
            sessions: Arc::default(),
            roster: Mutex::new(ThreadRoster {
                threads: Vec::with_capacity(min_threads),
                numbered: 0,
            }),
            not_started_log: LogThrottle::new(NOT_STARTED_LOG_PERIOD),
        });
        let handles = MainThreadHandles {
            set: Arc::clone(&set),
            engine: None,
        };

        for i in 0..min_threads {
            let (thread_rx, thread_set) = (rx.clone(), Arc::clone(&set));
            match spawner.spawn(i, def_thread_builder(i), move || {
                serve_def(i, thread_rx, thread_set, retires)
            }) {
                Ok(handle) => {
                    let mut roster = set.roster.lock();
                    roster.threads.push(handle);
                    roster.numbered += 1;
                }
                Err(source) => {
                    drop((tx, rx));
                    py.detach(|| handles.join(&ShutdownDeadline::new(shutdown_timeout)));
                    return Err(MainThreadStartError::NotStarted {
                        thread: i,
                        requested: min_threads,
                        source,
                    });
                }
            }
        }
        drop(rx);

        let (engine, handles) = match engine_routes(py, rules) {
            None => (None, handles),
            Some(routes) => {
                let (jobs, inbox) = match one_engine_queue(sizes.queue.get()) {
                    Ok(made) => made,
                    Err(source) => {
                        drop(tx);
                        py.detach(|| handles.join(&ShutdownDeadline::new(shutdown_timeout)));
                        return Err(MainThreadStartError::DoorbellNotMade(source));
                    }
                };
                let died = Arc::new(OnceLock::new());
                let builder = std::thread::Builder::new()
                    .name("mipyre-main-async".to_string())
                    .stack_size(mipyre_subinterp::PYTHON_THREAD_STACK);
                let records = Arc::clone(&died);
                match spawner.spawn(min_threads, builder, move || serve_async(inbox, routes, &records)) {
                    Ok(handle) => (
                        Some(MainEngine { jobs, died }),
                        MainThreadHandles {
                            engine: Some(handle),
                            ..handles
                        },
                    ),
                    Err(source) => {
                        drop((tx, jobs));
                        py.detach(|| handles.join(&ShutdownDeadline::new(shutdown_timeout)));
                        return Err(MainThreadStartError::NotStarted {
                            thread: min_threads,
                            requested: min_threads + 1,
                            source,
                        });
                    }
                }
            }
        };

        tracing::info!(
            target: "mipyre::server",
            min_threads,
            max_threads = sizes.max_threads.get(),
            queue = sizes.queue.get(),
            async_engine = engine.is_some(),
            "main-interpreter threads started"
        );
        let threads = MainThreads {
            tx,
            engine,
            max_threads: sizes.max_threads.get(),
            set,
        };
        Ok((threads, handles))
    }

    /// Starts one more `def` thread on a receiver the queue already counted. A thread the OS
    /// will not start drops the receiver, which counts it out; the job stays queued for the
    /// threads already running, of which there are at least the fewest kept.
    #[cold]
    #[inline(never)]
    fn start_def_thread(&self, rx: JobReceiver<MainWork>) {
        let mut roster = self.set.roster.lock();
        reap_finished(&mut roster.threads);
        let i = roster.numbered;
        let set = Arc::clone(&self.set);
        match mipyre_tpc::spawn(def_thread_builder(i), move || serve_def(i, rx, set, true)) {
            Ok(handle) => {
                roster.threads.push(handle);
                roster.numbered += 1;
            }
            Err(error) => {
                drop(roster);
                if let Some(held) = self.set.not_started_log.admit(mipyre_util::clock::now()) {
                    tracing::warn!(
                        target: "mipyre::server",
                        error = %error,
                        held,
                        "could not start another main-interpreter thread; the queued request waits for a running one"
                    );
                }
            }
        }
    }
}

/// Joins the threads that have finished, logging any that panicked, so the handles kept are at
/// most the threads running plus those finished since the last thread started.
fn reap_finished(threads: &mut Vec<OsThread<()>>) {
    let (finished, running): (Vec<_>, Vec<_>) = std::mem::take(threads).into_iter().partition(OsThread::is_finished);
    *threads = running;
    for thread in finished {
        let name = thread.name();
        match thread.join_by(Instant::now()) {
            Ok(()) | Err(ThreadJoinError::StillRunning) => {}
            Err(ThreadJoinError::Panicked(message)) => log_panicked(&name, &message),
        }
    }
}

fn def_thread_builder(i: usize) -> std::thread::Builder {
    // These are plain threads with no tokio runtime, because a handler reading `req.stream` calls
    // `Receiver::blocking_recv`, which panics inside a runtime context.
    std::thread::Builder::new()
        .name(format!("mipyre-main-def-{i}"))
        .stack_size(mipyre_subinterp::PYTHON_THREAD_STACK)
}

/// Queues an ask without blocking: a `def` handler's for the `def` threads, starting one more when
/// none sleeps and there is room, an `async def` one's for main's async engine. It refuses an ask
/// whose queue is full or has no thread taking from it, and an `async def` one when the table has
/// none, so no engine was started (a bug in building the server). A refused ask is dropped here,
/// its request with it.
impl HandoffBackend for MainThreads {
    type Ask = (HandlerTarget, MainHandler, Request);
    type Reply = Result<Result<HandlerReply, ErrorRecord>, HandlerError>;
    type Refusal = UnansweredError;
    type Run = MainRun;

    // The call sites hand over in four places (TPC's and the multi-thread server's, each with a
    // buffered body and a streamed one), and with several callers LLVM keeps this out of line,
    // which copies the 352-byte request into the job.
    #[inline(always)]
    fn submit(&self, (target, handler, request): Self::Ask) -> Result<MainRun, UnansweredError> {
        if handler == MainHandler::Async {
            return self.submit_async(target, request);
        }
        let (reply, answered) = oneshot::channel();
        self.tx
            .try_send_or_grow(
                MainWork::Request(MainJob { target, request, reply }),
                self.max_threads,
                |rx| {
                    self.start_def_thread(rx);
                },
            )
            .map_err(refusal)?;
        Ok(MainRun(answered))
    }
}

/// Says why the `def` threads' queue refused a job.
#[inline]
fn refusal(e: JobSendError) -> UnansweredError {
    let group = WorkerGroup::Main;
    match e {
        JobSendError::QueueFull => UnansweredError::Overloaded(Capacity::WorkQueue(group)),
        JobSendError::NoReceiverLeft => UnansweredError::WorkersGone(group),
    }
}

impl MainThreads {
    /// Hands a `runtime="main"` WebSocket upgrade to the `def` threads, which run its hooks and
    /// open its session.
    ///
    /// # Errors
    ///
    /// Refuses as a request is refused when the queue is full or has no thread.
    pub fn ws_open(
        &self,
        route: WsRouteId,
        request: Request,
        outbox: Arc<Outbox>,
    ) -> Result<impl Future<Output = Result<WsUpgradeAnswer, HandlerError>> + Send + 'static, UnansweredError> {
        let (reply, answered) = oneshot::channel();
        self.submit_ws(MainWsJob {
            route,
            call: MainWsCall::Open {
                request: Box::new(request),
                outbox,
                reply,
            },
        })?;
        Ok(async move { answered.await.unwrap_or_else(|_| Err(lost())) })
    }

    /// Hands one call of a connection to the `def` threads.
    ///
    /// # Errors
    ///
    /// Refuses as a request is refused when the queue is full or has no thread.
    pub fn ws_call(
        &self,
        route: WsRouteId,
        session: &WsSession,
        event: WsEvent,
    ) -> Result<impl Future<Output = Result<(), HandlerError>> + Send + 'static, UnansweredError> {
        let (reply, answered) = oneshot::channel();
        self.submit_ws(MainWsJob {
            route,
            call: MainWsCall::Event {
                session: session.key(),
                event,
                reply,
            },
        })?;
        Ok(async move { answered.await.unwrap_or_else(|_| Err(lost())) })
    }

    /// Hands a connection's end to the `def` threads, which call its `on_close` when `close` says
    /// how it ended, and let go of its session.
    ///
    /// # Errors
    ///
    /// Refuses as a request is refused; the session is then held until the server stops.
    pub fn ws_close(
        &self,
        route: WsRouteId,
        session: WsSession,
        close: Option<WsClose>,
        label: RequestLabel,
    ) -> Result<(), UnansweredError> {
        self.submit_ws(MainWsJob {
            route,
            call: MainWsCall::Close { session, close, label },
        })
    }

    fn submit_ws(&self, job: MainWsJob) -> Result<(), UnansweredError> {
        self.tx
            .try_send_or_grow(MainWork::WebSocket(Box::new(job)), self.max_threads, |rx| {
                self.start_def_thread(rx)
            })
            .map_err(refusal)
    }

    /// Queues an `async def` ask for main's async engine; out of line, so the `def` path's code does
    /// not carry it.
    #[inline(never)]
    fn submit_async(&self, target: HandlerTarget, request: Request) -> Result<MainRun, UnansweredError> {
        let group = WorkerGroup::Main;
        let engine = self.engine.as_ref().ok_or(UnansweredError::WorkersGone(group))?;
        let (reply, answered) = oneshot::channel();
        let job = EngineJob {
            handler: target.index(),
            request,
            reply: EngineReply::Main(reply),
        };
        engine.jobs.submit(job).map_err(|e| match e {
            JobSendError::QueueFull => UnansweredError::Overloaded(Capacity::WorkQueue(group)),
            JobSendError::NoReceiverLeft => match engine.died.get() {
                Some(cause) => UnansweredError::WorkersDied {
                    group,
                    cause: Arc::clone(cause),
                },
                None => UnansweredError::WorkersGone(group),
            },
        })?;
        // The engine's answer counts it completed, as an async worker's does.
        PoolJob::inc_created();
        Ok(MainRun(answered))
    }
}

/// One queued job's reply. Dropping it closes the reply: a `def` thread or main's engine that has
/// not started the job sees that and skips it, and one that has started it runs it to the end.
pub struct MainRun(oneshot::Receiver<Result<HandlerReply, ErrorRecord>>);

impl Future for MainRun {
    type Output = Result<Result<HandlerReply, ErrorRecord>, HandlerError>;

    // Out of line for the same callers, it returns the reply through memory.
    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|reply| reply.map_err(|_| refuse(UnansweredError::WorkerLost(ReplyHolder::Main))))
    }
}

/// Returns what main's async engine serves: the `async def` main routes' handlers, indexed by
/// [`HandlerTarget`] (`None` for any other), and the app's hooks; `None` when there is no such
/// route.
fn engine_routes(py: Python<'_>, rules: &MainRules) -> Option<EngineRoutes> {
    let routes = &rules.routes;
    let targets: Vec<HandlerTarget> = routes
        .routes()
        .iter()
        .filter(|route| route.runtime == HandlerPlace::Main(MainHandler::Async))
        .map(|route| route.handler)
        .collect();
    if targets.is_empty() {
        return None;
    }
    let len = targets.iter().map(|target| target.index() + 1).max().unwrap_or(0);
    let mut handlers: Vec<Option<Py<PyAny>>> = (0..len).map(|_| None).collect();
    for target in targets {
        handlers[target.index()] = Some(routes.handler(target).clone_ref(py));
    }
    let copy = |hooks: &[Py<PyAny>]| hooks.iter().map(|hook| hook.clone_ref(py)).collect();
    Some(EngineRoutes {
        handlers,
        before: copy(routes.before_hooks()),
        after: copy(routes.after_hooks()),
        streams: true,
    })
}

/// Runs main's async engine until its queue closes. An engine that stops on its own is logged, `died`
/// records why, and the jobs queued for it then, and every one submitted after, are refused at once
/// with that, so their callers are not left to time out.
fn serve_async(inbox: EngineInbox, routes: EngineRoutes, died: &OnceLock<Arc<str>>) {
    let ended = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        mipyre_subinterp::attach::with_main(|py| run_engine_module(py, inbox, routes))
    }));
    let (cause, left): (Arc<str>, _) = match ended {
        Ok((Ok(()), _)) => return,
        Ok((Err(EngineFailure::Setup(exception)), left)) => (
            format!(
                "main's async engine could not start: {exception}\n{}",
                exception.traceback()
            )
            .into(),
            left,
        ),
        Ok((Err(EngineFailure::Raised(exception)), left)) => (
            format!("main's async engine stopped: {exception}\n{}", exception.traceback()).into(),
            left,
        ),
        Err(payload) => (
            format!(
                "a Rust panic in main's async engine: {}",
                mipyre_util::panic_message(&*payload)
            )
            .into(),
            Default::default(),
        ),
    };
    tracing::error!(target: "mipyre::server", error = %cause, "main's async engine stopped serving");
    let cause = died.get_or_init(|| cause);
    for job in left {
        job.refuse(UnansweredError::WorkersDied {
            group: WorkerGroup::Main,
            cause: Arc::clone(cause),
        });
    }
}

impl MainThreadHandles {
    /// Waits until `deadline` for every thread to exit and returns the names of the ones still
    /// running.
    ///
    /// Call it with the GIL released, since the threads attach to main on their way out, and after
    /// every [`MainThreads`] is gone; one still held keeps the queue open. A handler can block
    /// forever, so a thread still running at the deadline is abandoned rather than hang the stop.
    pub fn join(self, deadline: &ShutdownDeadline) -> Vec<String> {
        let MainThreadHandles { set, engine } = self;
        let deadline_at = deadline.fix(Instant::now());
        let threads = std::mem::take(&mut set.roster.lock().threads);
        let abandoned = threads
            .into_iter()
            .chain(engine)
            .filter_map(|handle| {
                let thread = handle.name();
                match handle.join_by(deadline_at) {
                    Ok(()) => None,
                    Err(ThreadJoinError::Panicked(message)) => {
                        log_panicked(&thread, &message);
                        None
                    }
                    Err(ThreadJoinError::StillRunning) => {
                        tracing::error!(
                            target: "mipyre::server",
                            "main-interpreter thread {thread} did not exit within {:?}; abandoning it",
                            deadline.grace(),
                        );
                        Some(thread)
                    }
                }
            })
            .collect();
        // The template holds main-interpreter objects, so its last reference goes attached.
        mipyre_subinterp::attach::with_main(move |_| drop(set));
        abandoned
    }
}

fn log_panicked(thread: &str, message: &str) {
    tracing::error!(
        target: "mipyre::server",
        thread = %thread,
        panic = %message,
        "main-interpreter thread panicked"
    );
}

/// Serves `def` jobs until the queue closes or, when `retires`, until it idles past the keepalive
/// with more than the fewest threads running.
fn serve_def(i: usize, rx: JobReceiver<MainWork>, set: Arc<DefThreadSet>, retires: bool) {
    let min_threads = set.min_threads;
    let sessions = Arc::clone(&set.sessions);
    // The set may be the last reference to the template, so it goes attached.
    let rules = mipyre_subinterp::attach::with_main(move |py| crate::copy_rules(&set.rules, py));
    let mut spare = SpareContext::default();
    if retires {
        let mut receiver = rx;
        while let Some((job, next)) = receiver.recv_or_retire(min_threads, DEF_THREAD_KEEPALIVE) {
            dispatch(&rules, &sessions, &mut spare, job);
            receiver = next;
        }
    } else {
        while let Some(job) = rx.recv() {
            dispatch(&rules, &sessions, &mut spare, job);
        }
    }
    // Python objects this thread owns must go while attached, before the thread state does.
    mipyre_subinterp::attach::with_main(move |py| {
        crate::close_thread_event_loop(py);
        spare.release(py);
        drop((rules, sessions));
    });
    tracing::info!(target: "mipyre::server", thread = i, "main-interpreter thread exiting");
}

/// Runs one job. A request's fields are taken straight out of the job the queue handed over, so
/// it is copied no more often than when the queue held requests alone.
fn dispatch(rules: &MainRules, sessions: &Mutex<Sessions>, spare: &mut SpareContext, work: MainWork) {
    match work {
        MainWork::Request(MainJob { target, request, reply }) => {
            // The caller already gave up, so the main GIL is not spent on a reply nobody reads.
            if reply.is_closed() {
                return;
            }
            let result = call_handler_with_hooks(rules, spare, target, request);
            answer_if_awaited(reply, result);
        }
        MainWork::WebSocket(job) => run_ws_job(rules, sessions, spare, *job),
    }
}

/// Runs one WebSocket call on main. The table's lock is held only to find or file a `ws`, never
/// across a call, since a handler that waits on I/O releases the GIL and another thread runs.
#[inline(never)]
fn run_ws_job(rules: &MainRules, sessions: &Mutex<Sessions>, spare: &mut SpareContext, job: MainWsJob) {
    let MainWsJob { route, call } = job;
    let handlers = rules.routes.ws_handler(route);
    mipyre_subinterp::attach::with_main(|py| match call {
        MainWsCall::Open { request, outbox, reply } => {
            if reply.is_closed() {
                return;
            }
            let opened = in_main_context(py, spare, |chain| {
                websocket::open(chain, rules.routes.before_hooks(), *request, outbox)
            });
            let answer = opened.map(|opened| match opened {
                Ok(ws) => WsUpgradeAnswer::Accepted(sessions.lock().insert(ws)),
                Err(response) => WsUpgradeAnswer::Refused(response),
            });
            if let Err(Ok(WsUpgradeAnswer::Accepted(session))) = reply.send(answer) {
                // The upgrade stopped waiting, so no connection will end this session.
                drop(sessions.lock().remove(session));
            }
        }
        MainWsCall::Event { session, event, reply } => {
            let ws = sessions.lock().ws(py, session);
            let called = in_main_context(py, spare, |chain| websocket::call(chain, handlers, &ws, event));
            answer_if_awaited(reply, called);
        }
        MainWsCall::Close { session, close, label } => {
            let ws = sessions.lock().remove(session).into_bound(py);
            let Some(close) = close else {
                return;
            };
            if let Err(e) = in_main_context(py, spare, |chain| websocket::close(chain, handlers, &ws, &close)) {
                drop(e.log(&label));
            }
        }
    });
}

/// Runs `f` with a chain for main in a context of its own, as a request is; a panic is the
/// call's error.
fn in_main_context<'py, T>(
    py: Python<'py>,
    spare: &mut SpareContext,
    f: impl FnOnce(&mut Chain<'_, 'py, ThreadLoop>) -> Result<T, HandlerError>,
) -> Result<T, HandlerError> {
    catch_panic(|| in_reused_context(py, spare, |rc| f(&mut main_chain(py, rc))))
}

fn lost() -> HandlerError {
    refuse(UnansweredError::WorkerLost(ReplyHolder::Main))
}

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    use super::*;
    use mipyre_http::plan::route::{BodyMode, RouteTable};
    use mipyre_http::plan::site::ServeSettings;
    use pyo3::types::{PyDict, PyList};

    const DEF: MainHandler = MainHandler::Sync(BodyMode::Buffered);

    fn ask(handler: MainHandler) -> (HandlerTarget, MainHandler, Request) {
        let mut table = RouteTable::<(), ()>::new();
        table
            .insert("GET", "/", (), "index".into(), HandlerPlace::Main(handler))
            .expect("one route");
        let target = table.route(table.route_id(0)).handler;
        let (parts, ()) = hyper::Request::get("/").body(()).expect("a valid request").into_parts();
        let head = mipyre_http::io::request_head::RequestHead {
            parts,
            params: Default::default(),
            client: mipyre_transport::driver::Peer::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
            request_id: mipyre_http::plan::request_id::RequestId::mint(),
        };
        let request = Request::new(head, crate::exchange::RequestBody::Buffered(Default::default()));
        (target, handler, request)
    }

    /// Returns rules with no route, which hold no Python object.
    fn no_rules() -> MainRules {
        SiteRules {
            routes: RouteTable::new(),
            settings: ServeSettings::defaults(),
            access_log: None,
        }
    }

    /// Returns rules whose one route's handler appends each request it runs for to the returned
    /// list, and that route's handler.
    fn rules_recording_calls() -> (MainRules, HandlerTarget, Py<PyList>) {
        Python::initialize();
        let (handler, calls) = mipyre_subinterp::attach::with_main(|py| {
            let calls = PyList::empty(py);
            let globals = PyDict::new(py);
            globals.set_item("calls", &calls).expect("a str key");
            let handler = py
                .eval(c"lambda req: calls.append(req) or {}", Some(&globals), None)
                .expect("a lambda");
            (handler.unbind(), calls.unbind())
        });
        let mut routes = RouteTable::new();
        routes
            .insert("GET", "/", handler, "index".into(), HandlerPlace::Main(DEF))
            .expect("one route");
        let target = routes.route(routes.route_id(0)).handler;
        let rules = SiteRules {
            routes,
            settings: ServeSettings::defaults(),
            access_log: None,
        };
        (rules, target, calls)
    }

    /// Runs one job on this thread, its caller waiting or not as `waits` says, and returns how
    /// many times the handler ran.
    fn handler_runs(waits: bool) -> usize {
        let (rules, target, calls) = rules_recording_calls();
        let (reply, run) = oneshot::channel();
        if !waits {
            drop(run);
        }
        let (_, _, request) = ask(DEF);
        let mut spare = SpareContext::default();
        let sessions = Mutex::new(Sessions::default());
        dispatch(
            &rules,
            &sessions,
            &mut spare,
            MainWork::Request(MainJob { target, request, reply }),
        );
        mipyre_subinterp::attach::with_main(move |py| {
            spare.release(py);
            let runs = calls.bind(py).len();
            drop(rules);
            runs
        })
    }

    #[test]
    fn a_job_whose_caller_gave_up_does_not_run_its_handler() {
        assert_eq!(handler_runs(false), 0);
    }

    #[test]
    fn a_job_whose_caller_waits_runs_its_handler() {
        assert_eq!(handler_runs(true), 1);
    }

    /// Builds the backend without threads or an engine, allowed no more `def` threads than the
    /// returned receiver, which stands in for its one thread.
    fn threads_of(capacity: usize) -> (JobReceiver<MainWork>, MainThreads) {
        let (tx, rx) = job_queue(capacity);
        let set = Arc::new(DefThreadSet {
            rules: no_rules(),
            min_threads: 1,
            sessions: Arc::default(),
            roster: Mutex::new(ThreadRoster {
                threads: Vec::new(),
                numbered: 0,
            }),
            not_started_log: LogThrottle::new(NOT_STARTED_LOG_PERIOD),
        });
        let threads = MainThreads {
            tx,
            engine: None,
            max_threads: 1,
            set,
        };
        (rx, threads)
    }

    #[test]
    fn the_fewest_def_threads_kept_are_four_or_the_most_allowed_when_fewer() {
        let sizes = |max: usize| MainSizes {
            max_threads: NonZeroUsize::new(max).unwrap(),
            queue: NonZeroUsize::new(16).unwrap(),
        };
        assert_eq!(sizes(512).min_threads(), 4);
        assert_eq!(sizes(5).min_threads(), 4);
        assert_eq!(sizes(4).min_threads(), 4);
        assert_eq!(sizes(3).min_threads(), 3);
    }

    #[test]
    fn a_full_queue_refuses_as_overloaded() {
        let (_threads, main) = threads_of(1);
        assert!(main.submit(ask(DEF)).is_ok());
        assert!(matches!(
            main.submit(ask(DEF)),
            Err(UnansweredError::Overloaded(Capacity::WorkQueue(WorkerGroup::Main)))
        ));
    }

    #[test]
    fn a_queue_whose_threads_are_gone_refuses_as_workers_gone() {
        let (threads, main) = threads_of(1);
        drop(threads);
        assert!(matches!(
            main.submit(ask(DEF)),
            Err(UnansweredError::WorkersGone(WorkerGroup::Main))
        ));
    }

    #[test]
    fn an_async_def_ask_without_an_engine_is_refused_and_not_queued() {
        let (threads, main) = threads_of(1);
        assert!(matches!(
            main.submit(ask(MainHandler::Async)),
            Err(UnansweredError::WorkersGone(WorkerGroup::Main))
        ));
        assert!(
            threads.try_recv().is_none(),
            "the async def ask went to the def threads"
        );
    }

    #[tokio::test]
    async fn a_queued_job_replies_through_its_run() {
        let (threads, main) = threads_of(1);
        let run = main.submit(ask(DEF)).expect("the queue has room");
        let Some(MainWork::Request(MainJob { reply, .. })) = threads.try_recv() else {
            panic!("the request was queued");
        };
        let answer = mipyre_http::plan::response::HandlerResponse::new(
            hyper::StatusCode::OK,
            bytes::Bytes::from_static(b"{}"),
            mipyre_http::plan::response::JSON,
            mipyre_http::plan::response::HeaderLines::new(),
        );
        answer_if_awaited(reply, Ok(HandlerReply::Response(answer)));
        assert!(matches!(
            run.await,
            Ok(Ok(HandlerReply::Response(response))) if response.body.as_ref() == b"{}"
        ));
    }

    #[tokio::test]
    async fn a_job_dropped_unanswered_is_lost() {
        let (threads, main) = threads_of(1);
        let run = main.submit(ask(DEF)).expect("the queue has room");
        drop(threads.try_recv().expect("the job was queued"));
        assert!(matches!(
            run.await,
            Err(HandlerError::Refused(UnansweredError::WorkerLost(ReplyHolder::Main)))
        ));
    }

    #[test]
    fn a_dropped_run_closes_its_jobs_reply() {
        let (threads, main) = threads_of(1);
        drop(main.submit(ask(DEF)).expect("the queue has room"));
        let Some(MainWork::Request(MainJob { reply, .. })) = threads.try_recv() else {
            panic!("the request was queued");
        };
        assert!(
            reply.is_closed(),
            "a def thread would still run the job nobody waits for"
        );
    }

    /// Refuses the thread at `refuse_at`, since the OS cannot be made to refuse one on demand.
    struct RefuseAt {
        refuse_at: usize,
        started: usize,
    }

    impl Spawner for RefuseAt {
        fn spawn<R: Send + 'static>(
            &mut self,
            index: usize,
            builder: std::thread::Builder,
            body: impl FnOnce() -> R + Send + 'static,
        ) -> std::io::Result<OsThread<R>> {
            if index == self.refuse_at {
                return Err(std::io::Error::other("no thread for the main interpreter"));
            }
            self.started += 1;
            mipyre_tpc::spawn(builder, body)
        }
    }

    #[test]
    fn a_thread_that_cannot_start_fails_the_start_with_the_ones_started_joined() {
        Python::initialize();
        let rules = no_rules();
        let sizes = MainSizes {
            max_threads: NonZeroUsize::new(2).unwrap(),
            queue: NonZeroUsize::new(1).unwrap(),
        };
        let mut spawner = RefuseAt {
            refuse_at: 1,
            started: 0,
        };
        mipyre_subinterp::attach::with_main(|py| {
            let spawned = MainThreads::spawn(py, &rules, sizes, &mut spawner);
            let Err(error) = spawned else {
                panic!("the second thread was refused");
            };
            assert_eq!(spawner.started, 1, "thread 0 started, and was joined");
            assert_eq!(
                error.to_string(),
                "could not start main-interpreter thread 1 of 2: no thread for the main interpreter"
            );
            let raised = PyErr::from(error);
            assert!(raised.is_instance_of::<pyo3::exceptions::PyRuntimeError>(py));
        });
    }
}
