//! Runs a caller's future on one pinned OS thread per core, each with its own current-thread
//! runtime and `LocalSet`, and hands back how every thread ended.
//!
//! What a failure means and how it is said is the caller's: this crate logs nothing except for a
//! [`StopWatcher`] dropped without [`StopWatcher::finish`], whose drop has no one to return its
//! result to. What the caller's code did (returned an error, panicked) and what this crate's own
//! steps did (built the runtime, pinned the thread) are kept apart, as values; whether to merge
//! them is the caller's call.
//!
//! A served thread that fails, a panic included, stops its run before it shuts its runtime down,
//! and waits at most the caller's limit for the runtime's blocking pool: a blocking call that
//! never returns must not keep the other threads from hearing of the failure, nor keep the thread
//! from ending.

use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use crate::sync::{AtomicUsize, Mutex, Ordering};
use mipyre_cpu::{elevate_thread_qos_macos, try_pin_current, CoreId};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

mod os_thread;

pub(crate) use os_thread::into_panic_message;
use os_thread::JOIN_TIMEOUT;
pub use os_thread::{panic_message, spawn, OsThread, ThreadJoinError};

/// Why a thread run under this crate failed: the caller's code returned an error or panicked, or
/// this crate could not build the thread's runtime.
#[derive(Debug, thiserror::Error)]
pub enum ThreadError<E> {
    #[error(transparent)]
    Returned(E),
    #[error("panicked: {0}")]
    Panicked(String),
    #[error("could not build its runtime: {0}")]
    RuntimeNotBuilt(#[source] std::io::Error),
}

/// Lets a caller run its code around a served thread's runtime; each method does nothing by
/// default.
pub trait ThreadHooks: Send + Sync + 'static {
    /// Runs on the new thread once its runtime is built, before anything is served. Whenever it
    /// runs, [`ThreadHooks::after_serve`] runs too, even when this or `serve` panics.
    fn before_serve(&self) {}
    /// Runs when the runtime is about to park the thread because no task is ready.
    fn on_park(&self) {}
    fn on_unpark(&self) {}
    /// Runs after the served future and its `LocalSet` are gone, on every path out of
    /// [`serve_on_this_thread`] past [`ThreadHooks::before_serve`]. A panic here is the thread's
    /// failure after a serve that ended well, is added to the text of a serve that panicked, and
    /// is not reported after a serve that returned its own error, which has no room for it.
    fn after_serve(&self) {}
}

/// Where this thread was put: whether it was pinned to its CPU and its QoS class raised. Only
/// [`pin_and_raise_qos`] makes one, on the thread it describes, and it cannot leave that thread,
/// so a runtime served with it is built on the placed thread.
#[derive(Debug)]
pub struct Placement {
    pub pin: Result<(), mipyre_cpu::PinError>,
    /// Raising the QoS class matters on macOS only, where a thread left at the default class may
    /// run on the efficiency cores; elsewhere it is always `Ok`.
    pub qos: std::io::Result<()>,
    on_this_thread: PhantomData<*const ()>,
}

/// Pins this thread to `core` (`None` leaves it where the OS put it) and raises its QoS class,
/// and returns what happened; only the caller knows what an unpinned thread means and how to say
/// so.
pub fn pin_and_raise_qos(core: Option<CoreId>) -> Placement {
    place_with(&HostOs, core)
}

fn place_with(os: &impl ThreadOs, core: Option<CoreId>) -> Placement {
    Placement {
        pin: os.pin(core),
        qos: os.raise_qos(),
        on_this_thread: PhantomData,
    }
}

/// Serves `serve` on this thread, placed by `placement`, on a current-thread runtime and
/// `LocalSet` that are gone when this returns; the runtime's memory is first touched on the
/// placed CPU.
///
/// A failed serve stops `stop`'s run before the runtime shuts down. The shutdown then waits at
/// most what `teardown` returns, asked once the serve is over, and leaves any call still running
/// on the blocking pool; the run's join reads how many pool threads are still alive.
/// `blocking_stack` is the stack size of every blocking-pool thread, since only the caller knows
/// what its `spawn_blocking` work needs.
///
/// # Errors
///
/// Returns [`ThreadError::RuntimeNotBuilt`] when the runtime cannot be built,
/// [`ThreadError::Panicked`] when `serve` or a hook panics, and otherwise `serve`'s own error as
/// [`ThreadError::Returned`].
pub fn serve_on_this_thread<H, Fut, E, C>(
    placement: Placement,
    blocking_stack: usize,
    hooks: H,
    stop: &ThreadStop<C>,
    teardown: impl FnOnce() -> Duration,
    serve: impl FnOnce() -> Fut,
) -> Result<(), ThreadError<E>>
where
    H: ThreadHooks + Clone,
    Fut: Future<Output = Result<(), E>>,
{
    serve_with(&HostOs, placement, blocking_stack, hooks, stop, teardown, serve)
}

/// Serves as [`serve_on_this_thread`] does, building the runtime through `os`.
fn serve_with<O, H, Fut, E, C, S>(
    os: &O,
    placement: Placement,
    blocking_stack: usize,
    hooks: H,
    stop: &ThreadStop<C, S>,
    teardown: impl FnOnce() -> Duration,
    serve: impl FnOnce() -> Fut,
) -> Result<(), ThreadError<E>>
where
    O: ThreadOs,
    H: ThreadHooks + Clone,
    Fut: Future<Output = Result<(), E>>,
    S: Cancellable,
{
    // It is consumed so the runtime is built only on the thread it placed.
    drop(placement);
    // tokio's callbacks must be `'static`, so each owns a copy of the hooks.
    let (park, unpark) = (hooks.clone(), hooks.clone());
    // A current-thread runtime starts threads only for its blocking pool, so the thread's count,
    // read at the join, is what its runtimes left running.
    let (started, stopped) = (Arc::clone(&stop.pool_threads), Arc::clone(&stop.pool_threads));
    let mut builder = RuntimeBuilder::new_current_thread();
    builder
        .enable_all()
        .thread_stack_size(blocking_stack)
        .on_thread_park(move || park.on_park())
        .on_thread_unpark(move || unpark.on_unpark())
        .on_thread_start(move || {
            started.fetch_add(1, Ordering::AcqRel);
        })
        .on_thread_stop(move || {
            stopped.fetch_sub(1, Ordering::AcqRel);
        });
    let rt = os.build_runtime(&mut builder).map_err(ThreadError::RuntimeNotBuilt)?;
    // Every unwind out of the caller's code is caught here, inside the runtime's life, because an
    // unwind that dropped the runtime would wait for its blocking pool with no limit.
    let served = {
        let local = LocalSet::new();
        catch_unwind(AssertUnwindSafe(|| {
            hooks.before_serve();
            serve_loop(&local, &rt, serve())
        }))
        // The `LocalSet`'s tasks hold the thread's `Rc`s, so they go before `after_serve`.
    };
    let after = catch_unwind(AssertUnwindSafe(|| hooks.after_serve()));
    let result = match (served, after) {
        (Ok(Ok(())), Ok(())) => Ok(()),
        (Ok(Err(error)), after) => {
            // The serve's own error is the thread's failure, and it has no room for the hook's
            // panic text; the payload still goes through `into_panic_message`, whose drop of it
            // cannot panic out of here.
            if let Err(payload) = after {
                drop(into_panic_message(payload));
            }
            Err(ThreadError::Returned(error))
        }
        (Err(payload), after) => Err(panicked(payload, after.err())),
        (Ok(Ok(())), Err(payload)) => Err(panicked(payload, None)),
    };
    if result.is_err() {
        stop.fail();
    }
    rt.shutdown_timeout(teardown());
    result
}

/// Returns the failure of a serve that panicked, with the panic of the `after_serve` that
/// followed it, if that panicked too.
fn panicked<E>(payload: Box<dyn std::any::Any + Send>, after: Option<Box<dyn std::any::Any + Send>>) -> ThreadError<E> {
    let message = into_panic_message(payload);
    ThreadError::Panicked(match after {
        Some(after) => format!("{message}; after_serve panicked too: {}", into_panic_message(after)),
        None => message,
    })
}

/// Polls the runtime's scheduler, the `LocalSet`'s tasks and `serve`, which every request on this
/// thread runs through. It is never inlined, so what the compiler inlines into the loop does not
/// depend on how much set-up and tear-down the caller's thread body has around it.
#[inline(never)]
fn serve_loop<F: Future>(local: &LocalSet, rt: &Runtime, serve: F) -> F::Output {
    local.block_on(rt, serve)
}

/// Makes the OS calls that set up a per-core thread, so this crate's tests can make them fail.
trait ThreadOs {
    fn pin(&self, core: Option<CoreId>) -> Result<(), mipyre_cpu::PinError>;
    fn raise_qos(&self) -> std::io::Result<()>;
    fn build_runtime(&self, builder: &mut RuntimeBuilder) -> std::io::Result<Runtime>;
}

/// Makes the host's real calls.
struct HostOs;

impl ThreadOs for HostOs {
    fn pin(&self, core: Option<CoreId>) -> Result<(), mipyre_cpu::PinError> {
        try_pin_current(core)
    }

    fn raise_qos(&self) -> std::io::Result<()> {
        elevate_thread_qos_macos()
    }

    fn build_runtime(&self, builder: &mut RuntimeBuilder) -> std::io::Result<Runtime> {
        builder.build()
    }
}

/// Starts the threads of a set, so a test can refuse one; `index` is the thread's place in the set.
pub trait Spawner {
    fn spawn<R: Send + 'static>(
        &mut self,
        index: usize,
        builder: std::thread::Builder,
        body: impl FnOnce() -> R + Send + 'static,
    ) -> std::io::Result<OsThread<R>>;
}

/// Starts every thread on the OS ([`spawn`]).
#[derive(Clone, Copy, Debug, Default)]
pub struct OsSpawner;

impl Spawner for OsSpawner {
    fn spawn<R: Send + 'static>(
        &mut self,
        _index: usize,
        builder: std::thread::Builder,
        body: impl FnOnce() -> R + Send + 'static,
    ) -> std::io::Result<OsThread<R>> {
        spawn(builder, body)
    }
}

/// The place of one per-core thread in its set: its index, and the CPU paired with its payload.
/// Only [`spawn_per_core`] makes one; the thread's name is `{name}-{index}`.
#[derive(Debug, Clone, Copy)]
pub struct CoreSlot {
    index: usize,
    core: Option<CoreId>,
}

impl CoreSlot {
    pub fn index(&self) -> usize {
        self.index
    }

    /// Returns the CPU the thread is to be pinned to; `None` leaves it unpinned.
    pub fn core(&self) -> Option<CoreId> {
        self.core
    }
}

/// Cancels a run's threads. A server uses a [`CancellationToken`]; the Loom model of the stop
/// (`threads/loom_model.rs`) uses one of its own, since tokio_util's token synchronizes through a
/// lock Loom cannot see.
pub trait Cancellable {
    fn cancel(&self);
    fn is_cancelled(&self) -> bool;
}

impl Cancellable for CancellationToken {
    fn cancel(&self) {
        CancellationToken::cancel(self);
    }

    fn is_cancelled(&self) -> bool {
        CancellationToken::is_cancelled(self)
    }
}

/// Why a run stopped, as the first stop told to its [`RunStop`] said. `C` is the caller's own
/// reasons, such as a failure of something that is not one of the threads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopCause<C> {
    /// A thread of the run failed while the run was still going.
    ThreadFailed {
        thread: String,
    },
    /// The OS refused to start this thread of the run.
    NotSpawned {
        thread: String,
    },
    /// Every thread of the run ended, with nothing stopping it.
    ThreadsEnded,
    Caller(C),
}

/// One run's stop: the token its threads wait on, why the run stopped, and how many of its
/// threads have not ended. Clones share all three, and each run makes its own.
///
/// A stop told here records its cause before it cancels the token, and only while the token is
/// not cancelled yet, so the cause is the first stop's: a thread that fails because it saw the
/// stop never becomes the cause. The token's owner may also cancel it directly, for a reason it
/// keeps itself (a signal, a start it aborted); the run then has no cause, and no later failure
/// becomes one.
pub struct RunStop<C, S = CancellationToken> {
    token: S,
    cause: Arc<Mutex<Option<StopCause<C>>>>,
    running: Arc<AtomicUsize>,
}

impl<C, S: Clone> Clone for RunStop<C, S> {
    fn clone(&self) -> Self {
        RunStop {
            token: self.token.clone(),
            cause: Arc::clone(&self.cause),
            running: Arc::clone(&self.running),
        }
    }
}

impl<C, S: Default> Default for RunStop<C, S> {
    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<C, S> RunStop<C, S> {
    /// Makes the stop of a run whose threads wait on `token`.
    pub fn new(token: S) -> Self {
        RunStop {
            token,
            cause: Arc::new(Mutex::new(None)),
            running: Arc::default(),
        }
    }
}

impl<C, S: Cancellable> RunStop<C, S> {
    /// Stops the run for the caller's `cause`, which is the run's cause unless it already stopped.
    pub fn stop(&self, cause: C) {
        self.stop_for(StopCause::Caller(cause));
    }

    /// Stops the run from outside it, as its token's owner does, for a reason the one stopping
    /// keeps itself (a signal, a start it aborted and returns): no cause is recorded, and none
    /// can be after.
    pub fn cancel(&self) {
        self.token.cancel();
    }

    pub fn is_stopped(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Records `cause` unless the run has already stopped, then cancels the token.
    fn stop_for(&self, cause: StopCause<C>) {
        {
            // A panic while the lock is held can only be one of the caller's `C` drops; the
            // value under it is still one `Option`, so the poison is passed over.
            let mut held = self.cause.lock().unwrap_or_else(PoisonError::into_inner);
            if held.is_none() && !self.token.is_cancelled() {
                *held = Some(cause);
            }
        }
        self.token.cancel();
    }

    /// Counts one more thread of the run as running until the guard drops.
    fn count_thread(&self) -> RunCountGuard<C, S>
    where
        S: Clone,
    {
        self.running.fetch_add(1, Ordering::AcqRel);
        RunCountGuard(self.clone())
    }
}

impl<C: Clone, S> RunStop<C, S> {
    /// Returns why the run stopped: `None` while it has not, and when its token's owner stopped
    /// it directly.
    pub fn cause(&self) -> Option<StopCause<C>> {
        self.cause.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

impl<C> RunStop<C> {
    /// Returns a token cancelled when the run stops, for waiting on it. It is a child of the
    /// run's own, so cancelling it stops nothing: a stop goes through [`RunStop::stop`] with its
    /// cause.
    pub fn token(&self) -> CancellationToken {
        self.token.child_token()
    }
}

/// Counts a thread of its run until it drops, as the thread ends or as a spawn that failed drops
/// the body it was given; the last one stops the run, since a run whose threads have all ended is
/// over even when nothing stopped it.
struct RunCountGuard<C, S: Cancellable>(RunStop<C, S>);

impl<C, S: Cancellable> Drop for RunCountGuard<C, S> {
    fn drop(&mut self) {
        if self.0.running.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.stop_for(StopCause::ThreadsEnded);
        }
    }
}

/// One thread's part in its run's stop. Only this crate makes one, as it starts the thread.
pub struct ThreadStop<C, S = CancellationToken> {
    run: RunStop<C, S>,
    thread: String,
    /// The blocking-pool threads alive of the runtimes served on this thread: each pool thread
    /// counts itself in as it starts and out as it stops, so the run's join reads here what the
    /// thread left running, even when its body panicked after its serve.
    pool_threads: Arc<AtomicUsize>,
}

impl<C, S> ThreadStop<C, S> {
    fn new(run: RunStop<C, S>, thread: String) -> Self {
        ThreadStop {
            run,
            thread,
            pool_threads: Arc::default(),
        }
    }
}

impl<C> ThreadStop<C> {
    /// Returns a token cancelled when the run stops ([`RunStop::token`]).
    pub fn token(&self) -> CancellationToken {
        self.run.token()
    }
}

impl<C, S: Cancellable> ThreadStop<C, S> {
    /// Stops the run for this thread's failure, which is the run's cause unless it already
    /// stopped; telling it again changes nothing.
    fn fail(&self) {
        self.run.stop_for(StopCause::ThreadFailed {
            thread: self.thread.clone(),
        });
    }
}

/// Runs `run`, the body of the thread `own` belongs to. A failure or panic stops the run, so the
/// other threads stop; a panic becomes [`ThreadError::Panicked`].
fn stop_on_failure<T, E, C, S>(
    own: &ThreadStop<C, S>,
    run: impl FnOnce(&ThreadStop<C, S>) -> Result<T, ThreadError<E>>,
) -> Result<T, ThreadError<E>>
where
    S: Cancellable,
{
    let outcome = catch_unwind(AssertUnwindSafe(|| run(own))).unwrap_or_else(|payload| Err(panicked(payload, None)));
    if outcome.is_err() {
        own.fail();
    }
    outcome
}

/// A thread of a run, started by [`spawn_per_core`] or [`spawn_in_run`], whose body's failure or
/// panic stops the run and is already one of its outcomes.
pub struct ThreadHandle<T, E> {
    thread: OsThread<Result<T, ThreadError<E>>>,
    /// The thread's [`ThreadStop::pool_threads`].
    pool_threads: Arc<AtomicUsize>,
}

impl<T, E> ThreadHandle<T, E> {
    pub fn name(&self) -> String {
        self.thread.name()
    }
}

/// Makes the body of the thread `thread` of `stop`'s run: `body` under [`stop_on_failure`],
/// counted among the run's threads until it ends, beside the pool count its handle reads.
fn in_run<T, E, C>(
    stop: &RunStop<C>,
    thread: String,
    body: impl FnOnce(&ThreadStop<C>) -> Result<T, ThreadError<E>> + Send + 'static,
) -> (
    impl FnOnce() -> Result<T, ThreadError<E>> + Send + 'static,
    Arc<AtomicUsize>,
)
where
    C: Send + 'static,
{
    let own = ThreadStop::new(stop.clone(), thread);
    let pool_threads = Arc::clone(&own.pool_threads);
    let counted = stop.count_thread();
    let run = move || {
        let _counted = counted;
        stop_on_failure(&own, body)
    };
    (run, pool_threads)
}

/// A spawn the OS refused.
#[derive(Debug, thiserror::Error)]
#[error("could not spawn thread {thread}: {source}")]
pub struct SpawnError {
    pub thread: String,
    #[source]
    pub source: std::io::Error,
}

/// Starts one thread per payload through `spawner`, named `{name}-{i}` and paired with the CPU
/// given beside its payload, each running `run(slot, payload, stop)`, whose failure or panic
/// stops the run. A payload moves into its thread with the body, so one no thread took is dropped
/// on this thread, the one that made it.
///
/// # Errors
///
/// When a spawn fails, stops the run for it ([`StopCause::NotSpawned`]) and returns the threads
/// already started in [`SpawnFailure::started`], not joined: the caller releases whatever they
/// wait on besides the stop, such as a start handshake, then joins them.
pub fn spawn_per_core<P, F, T, E, C>(
    name: &str,
    payloads: Vec<(P, Option<CoreId>)>,
    stack_size: usize,
    stop: &RunStop<C>,
    spawner: &mut impl Spawner,
    run: F,
) -> Result<Vec<ThreadHandle<T, E>>, SpawnFailure<T, E>>
where
    P: Send + 'static,
    F: Fn(CoreSlot, P, &ThreadStop<C>) -> Result<T, ThreadError<E>> + Clone + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
    C: Send + 'static,
{
    // Held across the spawns, so threads that end before the last one starts do not count the
    // run as over.
    let _spawning = stop.count_thread();
    let mut started = Vec::with_capacity(payloads.len());
    for (index, (payload, core)) in payloads.into_iter().enumerate() {
        let thread = format!("{name}-{index}");
        let slot = CoreSlot { index, core };
        let run = run.clone();
        let (body, pool_threads) = in_run(stop, thread.clone(), move |own| run(slot, payload, own));
        let builder = std::thread::Builder::new().name(thread.clone()).stack_size(stack_size);
        match spawner.spawn(index, builder, body) {
            Ok(handle) => started.push(ThreadHandle {
                thread: handle,
                pool_threads,
            }),
            // The body, with its payload and its count, was dropped by the failed spawn; the
            // payloads after it are dropped with the loop.
            Err(source) => {
                stop.stop_for(StopCause::NotSpawned { thread: thread.clone() });
                return Err(SpawnFailure {
                    error: SpawnError { thread, source },
                    started,
                });
            }
        }
    }
    Ok(started)
}

/// Why [`spawn_per_core`] stopped early. It is `must_use` because the threads it holds have been
/// stopped but not joined.
#[must_use = "the threads already started must be joined"]
pub struct SpawnFailure<T, E> {
    pub error: SpawnError,
    pub started: Vec<ThreadHandle<T, E>>,
}

/// Starts one more thread of `stop`'s run, named `name`, running `body`, whose failure or panic
/// stops the run, for a thread that is not one of a per-core set.
///
/// # Errors
///
/// Returns [`SpawnError`] when the OS refuses the thread, after stopping the run for it
/// ([`StopCause::NotSpawned`]).
pub fn spawn_in_run<T, E, C>(
    name: &str,
    stack_size: usize,
    stop: &RunStop<C>,
    body: impl FnOnce(&ThreadStop<C>) -> Result<T, ThreadError<E>> + Send + 'static,
) -> Result<ThreadHandle<T, E>, SpawnError>
where
    T: Send + 'static,
    E: Send + 'static,
    C: Send + 'static,
{
    let (body, pool_threads) = in_run(stop, name.to_string(), body);
    let builder = std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(stack_size);
    match spawn(builder, body) {
        Ok(handle) => Ok(ThreadHandle {
            thread: handle,
            pool_threads,
        }),
        Err(source) => {
            stop.stop_for(StopCause::NotSpawned {
                thread: name.to_string(),
            });
            Err(SpawnError {
                thread: name.to_string(),
                source,
            })
        }
    }
}

/// Blocks this runtime-less thread until `token` is cancelled, parked in between, with no timer
/// and no wakeups; a server's main thread waits here for the server's whole life.
pub fn block_until_cancelled(token: &CancellationToken) {
    struct Unpark(std::thread::Thread);
    impl std::task::Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = std::task::Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = std::task::Context::from_waker(&waker);
    let mut cancelled = std::pin::pin!(token.cancelled());
    // `park` may return spuriously, so the poll says whether it was the cancel.
    while cancelled.as_mut().poll(&mut cx).is_pending() {
        std::thread::park();
    }
}

/// Joins every thread by `deadline`, one after another, so the wait ends by it however many
/// threads there are. A thread still running then is left running, since waiting for it could
/// hang the process.
pub fn join_until<T, E>(handles: Vec<ThreadHandle<T, E>>, deadline: Instant) -> ThreadResults<T, E> {
    ThreadResults::tally(handles.into_iter().map(|handle| {
        let thread = handle.name();
        let joined = handle.thread.join_by(deadline);
        // Read after the join, so a pool thread that ended by then is not counted.
        let pool_threads = NonZeroUsize::new(handle.pool_threads.load(Ordering::Acquire));
        JoinOutcome {
            thread,
            joined,
            pool_threads,
        }
    }))
}

/// What joining one thread gave, as [`ThreadResults::tally`] sorts it.
struct JoinOutcome<T, E> {
    thread: String,
    joined: Result<Result<T, ThreadError<E>>, ThreadJoinError>,
    /// The blocking-pool threads of its runtimes alive after the join.
    pool_threads: Option<NonZeroUsize>,
}

/// What every thread of a run did when it was joined: why the run stopped is the run's
/// ([`RunStop::cause`]), not the threads'.
pub struct ThreadResults<T, E> {
    /// The `T` of every thread that ended well, in thread order.
    pub ended: Vec<T>,
    /// Every failure, in thread order.
    pub failures: Vec<ThreadFailure<E>>,
    /// The threads, ended or not, that left something running past the join, in thread order.
    pub left_running: Vec<ThreadLeftover>,
}

pub struct ThreadFailure<E> {
    pub thread: String,
    pub error: ThreadError<E>,
}

/// A thread that left something running when its run was joined, which keeps running as the
/// process goes on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadLeftover {
    pub thread: String,
    pub what: Leftover,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Leftover {
    /// The thread itself was still running at the deadline.
    Thread,
    /// The thread ended, but its runtime's shutdown left this many blocking-pool threads running:
    /// each is a call that had not returned or, past a limit near zero, an idle thread that had
    /// not yet seen the shutdown, so it is an upper bound on the calls left.
    BlockingCalls(NonZeroUsize),
}

impl<T, E> ThreadResults<T, E> {
    /// Sorts each thread's join, in thread order, into the results.
    fn tally(joins: impl IntoIterator<Item = JoinOutcome<T, E>>) -> Self {
        let mut results = ThreadResults {
            ended: Vec::new(),
            failures: Vec::new(),
            left_running: Vec::new(),
        };
        for JoinOutcome {
            thread,
            joined,
            pool_threads,
        } in joins
        {
            let outcome = match joined {
                Ok(outcome) => outcome,
                Err(ThreadJoinError::StillRunning) => {
                    results.left_running.push(ThreadLeftover {
                        thread,
                        what: Leftover::Thread,
                    });
                    continue;
                }
                // `stop_on_failure` catches a body's panic, so this one panicked past it.
                Err(ThreadJoinError::Panicked(payload)) => Err(ThreadError::Panicked(payload)),
            };
            if let Some(calls) = pool_threads {
                results.left_running.push(ThreadLeftover {
                    thread: thread.clone(),
                    what: Leftover::BlockingCalls(calls),
                });
            }
            match outcome {
                Ok(value) => results.ended.push(value),
                Err(error) => results.failures.push(ThreadFailure { thread, error }),
            }
        }
        results
    }

    /// Returns whether every thread ended well and left nothing running.
    pub fn is_clean(&self) -> bool {
        self.failures.is_empty() && self.left_running.is_empty()
    }
}

/// The instant a server's stop must be over by: its grace counted from when the stop begins.
/// Every group of threads the stop joins, and every thread that drains its connections, works to
/// this one instant, so the stop takes at most the grace however many groups it joins one after
/// another. Clones share the instant.
#[derive(Clone, Debug)]
pub struct ShutdownDeadline {
    grace: Duration,
    at: Arc<OnceLock<Instant>>,
}

impl ShutdownDeadline {
    pub fn new(grace: Duration) -> Self {
        ShutdownDeadline {
            grace,
            at: Arc::default(),
        }
    }

    pub fn grace(&self) -> Duration {
        self.grace
    }

    /// Returns the deadline, fixing it at `now` plus the grace the first time it is asked for:
    /// the first to ask is the first to see the stop, and every later ask gets the same instant.
    ///
    /// # Panics
    ///
    /// Panics when `now` plus the grace is past what an `Instant` holds; a grace read from the
    /// configuration is bounded long before that.
    pub fn fix(&self, now: Instant) -> Instant {
        *self.at.get_or_init(|| now + self.grace)
    }
}

/// A thread that stops a server's run from outside it ([`RunStop::cancel`]) when `until_stopped`
/// returns, for example on SIGINT, or panics. The caller calls [`StopWatcher::finish`] when its
/// run ends to learn how the thread ended.
pub struct StopWatcher<C> {
    stop: RunStop<C>,
    thread: Option<OsThread<()>>,
}

/// Why [`spawn_stop_watcher`] started no watcher.
#[derive(Debug, thiserror::Error)]
pub enum StopWatcherError {
    #[error("stop watcher {thread} could not build its runtime: {source}")]
    RuntimeNotBuilt {
        thread: String,
        #[source]
        source: std::io::Error,
    },
    #[error("stop watcher {thread} could not arm its stop: {source}")]
    NotArmed {
        thread: String,
        #[source]
        source: std::io::Error,
    },
    #[error("could not spawn stop watcher {thread}: {source}")]
    NotSpawned {
        thread: String,
        #[source]
        source: std::io::Error,
    },
}

/// Starts a [`StopWatcher`] named `name` that runs `until_stopped(token, armed)`, `token` being
/// cancelled when `stop`'s run stops, on its own current-thread runtime, and stops the run when
/// that returns or panics. `arm` runs here inside that runtime before the thread starts and yields
/// what `until_stopped` waits on, such as the SIGINT and SIGTERM streams.
///
/// # Errors
///
/// Returns [`StopWatcherError`] when the runtime, the arm or the thread cannot be had, and then
/// starts nothing.
pub fn spawn_stop_watcher<C, A, W, Fut>(
    name: &str,
    stop: &RunStop<C>,
    arm: impl FnOnce() -> std::io::Result<A>,
    until_stopped: W,
) -> Result<StopWatcher<C>, StopWatcherError>
where
    C: Send + 'static,
    A: Send + 'static,
    W: FnOnce(CancellationToken, A) -> Fut + Send + 'static,
    Fut: Future<Output = ()>,
{
    // The runtime is built before the thread because a watcher without one cannot see the stop,
    // and a server that would ignore its stop must not start.
    let rt = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| StopWatcherError::RuntimeNotBuilt {
            thread: name.into(),
            source,
        })?;
    let armed = {
        let _in_runtime = rt.enter();
        arm()
    }
    .map_err(|source| StopWatcherError::NotArmed {
        thread: name.into(),
        source,
    })?;
    let (on_exit, watched) = (WatcherExit(stop.clone()), stop.token());
    let thread = spawn(std::thread::Builder::new().name(name.into()), move || {
        let _exit = on_exit;
        rt.block_on(until_stopped(watched, armed))
    })
    .map_err(|source| StopWatcherError::NotSpawned {
        thread: name.into(),
        source,
    })?;
    Ok(StopWatcher {
        stop: stop.clone(),
        thread: Some(thread),
    })
}

/// A watcher thread's exit: it stops the run from outside as it drops, so a watcher that returns
/// or panics stops the run.
struct WatcherExit<C>(RunStop<C>);

impl<C> Drop for WatcherExit<C> {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl<C> StopWatcher<C> {
    /// Stops the run from outside and waits for the watcher thread until `deadline`.
    ///
    /// # Errors
    ///
    /// Returns [`ThreadJoinError::StillRunning`] when the thread is still running then, leaving it
    /// behind, and [`ThreadJoinError::Panicked`] when it panicked.
    pub fn finish(mut self, deadline: Instant) -> Result<(), ThreadJoinError> {
        self.stop_and_join(deadline)
    }

    fn stop_and_join(&mut self, deadline: Instant) -> Result<(), ThreadJoinError> {
        self.stop.cancel();
        match self.thread.take() {
            Some(thread) => thread.join_by(deadline),
            None => Ok(()),
        }
    }
}

impl<C> Drop for StopWatcher<C> {
    /// Stops and waits as [`StopWatcher::finish`] does, at most [`JOIN_TIMEOUT`], for a watcher
    /// whose caller never called it. A drop cannot return what happened, so it logs an abandoned
    /// or panicked watcher; that is the one line this crate logs.
    fn drop(&mut self) {
        let Some(name) = self.thread.as_ref().map(OsThread::name) else {
            return;
        };
        if let Err(error) = self.stop_and_join(Instant::now() + JOIN_TIMEOUT) {
            tracing::error!(thread = %name, error = %error, "stop watcher not joined cleanly on drop");
        }
    }
}

#[cfg(test)]
#[cfg(sync_model)]
mod loom_model;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::Mutex as TestMutex;

    /// Tokio's default stack, since these tests run no Python on the blocking pool.
    const BLOCKING_STACK: usize = 2 << 20;

    /// Counts the serve hooks it is called with; copies share the counts, so the test keeps one.
    #[derive(Clone, Default)]
    struct HookRecorder {
        before_serve: Arc<AtomicUsize>,
        after_serve: Arc<AtomicUsize>,
    }

    impl ThreadHooks for HookRecorder {
        fn before_serve(&self) {
            self.before_serve.fetch_add(1, Ordering::SeqCst);
        }
        fn after_serve(&self) {
            self.after_serve.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Which payload was dropped on which thread.
    type Drops = Arc<TestMutex<Vec<(usize, std::thread::ThreadId)>>>;

    struct DropWitness {
        value: usize,
        drops: Drops,
    }

    impl Drop for DropWitness {
        fn drop(&mut self) {
            self.drops
                .lock()
                .unwrap()
                .push((self.value, std::thread::current().id()));
        }
    }

    fn witnesses(values: &[usize]) -> (Vec<(DropWitness, Option<CoreId>)>, Drops) {
        let drops = Arc::new(TestMutex::new(Vec::new()));
        let payloads = values
            .iter()
            .map(|&value| {
                (
                    DropWitness {
                        value,
                        drops: Arc::clone(&drops),
                    },
                    None,
                )
            })
            .collect();
        (payloads, drops)
    }

    #[derive(Debug, thiserror::Error)]
    enum BodyError {
        #[error("serve failed")]
        NotServed,
        #[error("a companion of the failure")]
        AlsoNotServed,
    }

    /// Returns a body's own result as a thread's.
    fn body<T>(result: Result<T, BodyError>) -> Result<T, ThreadError<BodyError>> {
        result.map_err(ThreadError::Returned)
    }

    /// A run whose caller has no causes of its own.
    type TestRun = RunStop<()>;

    /// A one-shot event a test thread waits on, so tests wait on events rather than a clock.
    #[derive(Clone, Default)]
    struct Gate(Arc<(TestMutex<bool>, std::sync::Condvar)>);

    impl Gate {
        fn open(&self) {
            let (open, changed) = &*self.0;
            *open.lock().unwrap() = true;
            changed.notify_all();
        }

        fn wait(&self) {
            let (open, changed) = &*self.0;
            let mut is_open = open.lock().unwrap();
            while !*is_open {
                is_open = changed.wait(is_open).unwrap();
            }
        }
    }

    /// Runs `f` on its own thread and fails the test, rather than hanging the run, if it is not
    /// back within 10 s.
    fn within_10s<R: Send + 'static>(f: impl FnOnce() -> R + Send + 'static) -> R {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || tx.send(f()).unwrap());
        rx.recv_timeout(Duration::from_secs(10))
            .expect("did not finish within 10 s")
    }

    /// Returns `thread`'s part in a run of its own, for a serve called outside
    /// [`stop_on_failure`].
    fn own_stop(thread: &str) -> ThreadStop<()> {
        ThreadStop::new(RunStop::default(), thread.into())
    }

    /// Returns the blocking-pool threads alive of the runtimes served under `stop`, as the join
    /// reads them.
    fn left_on_pool<C, S>(stop: &ThreadStop<C, S>) -> Option<NonZeroUsize> {
        NonZeroUsize::new(stop.pool_threads.load(Ordering::SeqCst))
    }

    /// How a body run by [`run_as`] ended.
    struct BodyOutcome<T> {
        outcome: Result<T, ThreadError<BodyError>>,
        left_on_pool: Option<NonZeroUsize>,
    }

    /// Runs `run` on this thread as the body of thread `thread` of `stop`'s run, as a thread
    /// started in the run runs it.
    fn run_as<T, C>(
        stop: &RunStop<C>,
        thread: &str,
        run: impl FnOnce(&ThreadStop<C>) -> Result<T, ThreadError<BodyError>>,
    ) -> BodyOutcome<T> {
        let own = ThreadStop::new(stop.clone(), thread.into());
        let outcome = stop_on_failure(&own, run);
        BodyOutcome {
            outcome,
            left_on_pool: left_on_pool(&own),
        }
    }

    /// Waits for the run's stop, then joins its threads by the stop plus `grace`, as a server's
    /// main thread does.
    fn join<T, E>(handles: Vec<ThreadHandle<T, E>>, run: &TestRun, grace: Duration) -> ThreadResults<T, E> {
        block_until_cancelled(&run.token());
        join_until(handles, Instant::now() + grace)
    }

    fn failed(thread: &str) -> Option<StopCause<()>> {
        Some(StopCause::ThreadFailed { thread: thread.into() })
    }

    /// Starts every thread on the OS except the one at the given index, whose spawn fails as an
    /// OS refusal does.
    struct RefuseAt(usize);

    impl Spawner for RefuseAt {
        fn spawn<R: Send + 'static>(
            &mut self,
            index: usize,
            builder: std::thread::Builder,
            body: impl FnOnce() -> R + Send + 'static,
        ) -> std::io::Result<OsThread<R>> {
            if index == self.0 {
                return Err(std::io::Error::other(format!("thread {index} refused")));
            }
            spawn(builder, body)
        }
    }

    fn serve_test<H, Fut>(
        os: &impl ThreadOs,
        hooks: H,
        stop: &ThreadStop<()>,
        limit: Duration,
        serve: impl FnOnce() -> Fut,
    ) -> Result<(), ThreadError<BodyError>>
    where
        H: ThreadHooks + Clone,
        Fut: Future<Output = Result<(), BodyError>>,
    {
        serve_with(
            os,
            place_with(os, None),
            BLOCKING_STACK,
            hooks,
            stop,
            move || limit,
            serve,
        )
    }

    /// Serves on this thread, unpinned, with the host's real calls.
    fn serve_here<Fut>(
        stop: &ThreadStop<()>,
        limit: Duration,
        serve: impl FnOnce() -> Fut,
    ) -> Result<(), ThreadError<BodyError>>
    where
        Fut: Future<Output = Result<(), BodyError>>,
    {
        serve_on_this_thread(
            pin_and_raise_qos(None),
            BLOCKING_STACK,
            HookRecorder::default(),
            stop,
            move || limit,
            serve,
        )
    }

    /// The blocking call waits for the test, so a runtime dropped with no limit would never
    /// return here; the shutdown instead gives up at its limit, and the call finishes once
    /// released.
    #[test]
    fn a_serve_returns_at_its_limit_with_its_blocking_call_still_running() {
        within_10s(|| {
            let (release, released) = mpsc::channel::<()>();
            let (ended, call_ended) = mpsc::channel::<()>();
            let (entered, in_pool) = mpsc::channel::<()>();
            let stop = own_stop("t");
            let finished = Arc::new(AtomicUsize::new(0));
            let done = Arc::clone(&finished);
            let served = serve_test(&HostOs, HookRecorder::default(), &stop, Duration::ZERO, || async move {
                drop(tokio::task::spawn_blocking(move || {
                    entered.send(()).unwrap();
                    released.recv().unwrap();
                    done.fetch_add(1, Ordering::SeqCst);
                    ended.send(()).unwrap();
                }));
                in_pool.recv().expect("the call started");
                Ok(())
            });
            assert!(served.is_ok(), "{:?}", served.err());
            assert_eq!(
                left_on_pool(&stop),
                NonZeroUsize::new(1),
                "the one call is left on the pool"
            );
            assert_eq!(finished.load(Ordering::SeqCst), 0, "the call is still running");
            release.send(()).unwrap();
            call_ended.recv().unwrap();
            assert_eq!(finished.load(Ordering::SeqCst), 1, "the call left running finished");
        });
    }

    /// The steps of one thread's setup in order, written by both [`StepOs`] and [`StepLog`].
    type Steps = Arc<TestMutex<Vec<&'static str>>>;

    /// Records each OS call and fails the build when `fail_build` is set, because tokio's own
    /// build cannot be made to fail on demand.
    struct StepOs {
        steps: Steps,
        fail_build: bool,
    }

    impl ThreadOs for StepOs {
        fn pin(&self, _core: Option<CoreId>) -> Result<(), mipyre_cpu::PinError> {
            self.steps.lock().unwrap().push("pin");
            Ok(())
        }
        fn raise_qos(&self) -> std::io::Result<()> {
            self.steps.lock().unwrap().push("qos");
            Ok(())
        }
        fn build_runtime(&self, builder: &mut RuntimeBuilder) -> std::io::Result<Runtime> {
            self.steps.lock().unwrap().push("build");
            if self.fail_build {
                return Err(std::io::Error::other("no runtime for this thread"));
            }
            builder.build()
        }
    }

    #[derive(Clone)]
    struct StepLog(Steps);

    impl ThreadHooks for StepLog {
        fn before_serve(&self) {
            self.0.lock().unwrap().push("before_serve");
        }
        fn after_serve(&self) {
            self.0.lock().unwrap().push("after_serve");
        }
    }

    /// The order is the types': a serve takes the `Placement` only placing the thread makes.
    #[test]
    fn the_thread_is_pinned_before_its_runtime_is_built() {
        let steps = Steps::default();
        let os = StepOs {
            steps: Arc::clone(&steps),
            fail_build: false,
        };
        let served = serve_test(
            &os,
            StepLog(Arc::clone(&steps)),
            &own_stop("t"),
            Duration::ZERO,
            || async { Ok(()) },
        );
        assert!(served.is_ok());
        let steps = steps.lock().unwrap().clone();
        assert_eq!(steps, ["pin", "qos", "build", "before_serve", "after_serve"]);
    }

    #[test]
    fn a_runtime_that_is_not_built_calls_no_serve_hook() {
        let steps = Steps::default();
        let os = StepOs {
            steps: Arc::clone(&steps),
            fail_build: true,
        };
        let served = Arc::new(AtomicUsize::new(0));
        let ran = Arc::clone(&served);
        let result = serve_test(
            &os,
            StepLog(Arc::clone(&steps)),
            &own_stop("t"),
            Duration::ZERO,
            || async move {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        );
        assert!(
            matches!(
                result,
                Err(ThreadError::RuntimeNotBuilt(ref source)) if source.to_string() == "no runtime for this thread"
            ),
            "{:?}",
            result.err()
        );
        let steps = steps.lock().unwrap().clone();
        assert_eq!(steps, ["pin", "qos", "build"], "no serve hook ran");
        assert_eq!(served.load(Ordering::SeqCst), 0, "nothing served");
    }

    /// No CPU number reaches `MAX_CPUS` on any host, and a platform without pinning refuses every
    /// pin, so this pin fails everywhere.
    #[test]
    fn a_pin_the_os_refuses_is_handed_back_and_the_thread_still_serves() {
        within_10s(|| {
            let served = Arc::new(AtomicUsize::new(0));
            let ran = Arc::clone(&served);
            let placement = pin_and_raise_qos(Some(CoreId {
                id: mipyre_cpu::MAX_CPUS,
            }));
            let refused = placement.pin.as_ref().expect_err("no host has this CPU").to_string();
            assert!(refused.contains("CPU 8192"), "{refused}");
            let outcome: Result<(), ThreadError<BodyError>> = serve_on_this_thread(
                placement,
                BLOCKING_STACK,
                HookRecorder::default(),
                &own_stop("t"),
                || Duration::ZERO,
                || async move {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            );
            assert!(outcome.is_ok());
            assert_eq!(served.load(Ordering::SeqCst), 1, "served unpinned");
        });
    }

    #[test]
    fn a_thread_with_no_pin_to_make_is_placed_with_no_failure() {
        let placement = pin_and_raise_qos(None);
        assert!(placement.pin.is_ok(), "{:?}", placement.pin);
    }

    #[test]
    fn a_panicking_serve_still_pairs_before_and_after_serve() {
        within_10s(|| {
            let hooks = HookRecorder::default();
            let stop = own_stop("t");
            let served = serve_test(&HostOs, hooks.clone(), &stop, Duration::ZERO, || async {
                if std::hint::black_box(true) {
                    panic!("injected panic in serve");
                }
                Ok(())
            });
            assert!(
                matches!(&served, Err(ThreadError::Panicked(payload)) if payload == "injected panic in serve"),
                "the panic is the serve's failure"
            );
            assert_eq!(stop.run.cause(), failed("t"), "the panic stopped the run");
            assert_eq!(hooks.before_serve.load(Ordering::SeqCst), 1);
            assert_eq!(hooks.after_serve.load(Ordering::SeqCst), 1);
        });
    }

    #[derive(Clone)]
    struct ParkSignal(Gate);

    impl ThreadHooks for ParkSignal {
        fn on_park(&self) {
            self.0.open();
        }
    }

    /// A server's idle threads end this way: the stop is cancelled on another thread and only the
    /// wake reaches them. `on_park` runs just before the wait, so the test cancels once the thread
    /// is parked or about to park, and never waits on a clock.
    #[test]
    fn a_parked_thread_wakes_for_the_stop_and_ends() {
        let stop = CancellationToken::new();
        let parked = Gate::default();
        let token = stop.clone();
        let hooks = ParkSignal(parked.clone());
        let thread = std::thread::spawn(move || {
            serve_test(&HostOs, hooks, &own_stop("idle"), Duration::ZERO, || async move {
                crate::LocalStop::follow(&token).stopped().await;
                Ok(())
            })
        });
        parked.wait();
        stop.cancel();
        let served = thread.join().expect("the thread did not panic");
        assert!(served.is_ok(), "{:?}", served.err());
    }

    #[test]
    fn a_panicking_thread_stops_the_others() {
        let run = TestRun::default();
        let handles = spawn_per_core(
            "p",
            vec![0usize, 1].into_iter().map(|p| (p, None)).collect(),
            1 << 20,
            &run,
            &mut OsSpawner,
            |_slot, payload, stop| -> Result<bool, ThreadError<BodyError>> {
                if payload == 0 {
                    panic!("injected panic in thread 0");
                }
                block_until_cancelled(&stop.token());
                Ok(stop.token().is_cancelled())
            },
        )
        .ok()
        .unwrap();
        let waited = run.clone();
        let joined = within_10s(move || join(handles, &waited, JOIN_TIMEOUT));
        assert_eq!(joined.ended, vec![true], "the other thread saw the stop");
        assert_eq!(run.cause(), failed("p-0"));
        assert!(matches!(joined.failures[0].error, ThreadError::Panicked(_)));
    }

    /// The join must not be stuck waiting on thread 0, which ends only on the stop.
    #[test]
    fn a_panic_in_the_last_thread_stops_the_others() {
        let run = TestRun::default();
        let handles = spawn_per_core(
            "p",
            vec![0usize, 1].into_iter().map(|p| (p, None)).collect(),
            1 << 20,
            &run,
            &mut OsSpawner,
            |_slot, payload, stop| -> Result<bool, ThreadError<BodyError>> {
                if payload == 1 {
                    panic!("injected panic in thread 1");
                }
                block_until_cancelled(&stop.token());
                Ok(stop.token().is_cancelled())
            },
        )
        .ok()
        .unwrap();
        let waited = run.clone();
        let joined = within_10s(move || join(handles, &waited, JOIN_TIMEOUT));
        assert_eq!(joined.ended, vec![true], "the other thread saw the stop");
        assert!(joined.left_running.is_empty(), "{:?}", joined.left_running);
        assert_eq!(run.cause(), failed("p-1"));
    }

    /// The join gets no time and the thread never ends by itself, so the join returns as soon as
    /// its deadline passes; the test asks only that this is after the stop, which comes 30 ms
    /// after the thread starts. That holds however loaded the machine is, while a deadline
    /// counted from the call would return before the stop.
    #[test]
    fn the_join_waits_for_the_stop_before_its_deadline_starts() {
        let run = TestRun::default();
        let started = Gate::default();
        let announce = started.clone();
        let release = Gate::default();
        let held = release.clone();
        let handles = spawn_per_core(
            "serving",
            vec![(0usize, None)],
            1 << 20,
            &run,
            &mut OsSpawner,
            move |_slot, _payload, _stop| -> Result<bool, ThreadError<BodyError>> {
                announce.open();
                held.wait();
                Ok(true)
            },
        )
        .ok()
        .unwrap();
        let stopper = run.clone();
        std::thread::spawn(move || {
            started.wait();
            std::thread::sleep(Duration::from_millis(30));
            stopper.stop(());
        });
        let waited = run.clone();
        let (joined, stopped_first) = within_10s(move || {
            let joined = join(handles, &waited, Duration::ZERO);
            (joined, waited.is_stopped())
        });
        release.open();
        assert!(stopped_first, "the join returned before the stop");
        assert_eq!(
            joined.left_running,
            [ThreadLeftover {
                thread: "serving-0".into(),
                what: Leftover::Thread
            }]
        );
    }

    #[test]
    fn a_shutdown_deadline_is_fixed_by_its_first_ask() {
        let deadline = ShutdownDeadline::new(Duration::from_secs(5));
        let stopped = Instant::now();
        assert_eq!(deadline.fix(stopped), stopped + Duration::from_secs(5));
        assert_eq!(
            deadline.fix(stopped + Duration::from_secs(60)),
            stopped + Duration::from_secs(5)
        );
        assert_eq!(
            deadline.clone().fix(stopped + Duration::from_secs(60)),
            stopped + Duration::from_secs(5),
            "a clone shares it"
        );
    }

    /// An earlier group of the same stop fixed the deadline and used it all, so a stuck thread of
    /// this group is given up at once: a grace of its own, counted afresh, would outlast the 10 s
    /// guard.
    #[test]
    fn a_join_by_the_deadline_an_earlier_group_fixed_gives_up_at_once() {
        const GRACE: Duration = Duration::from_secs(30);
        let run = TestRun::default();
        let release = Gate::default();
        let held = release.clone();
        let handles = spawn_per_core(
            "late-group",
            vec![(0usize, None)],
            1 << 20,
            &run,
            &mut OsSpawner,
            move |_slot, _payload, _stop| -> Result<bool, ThreadError<BodyError>> {
                held.wait();
                Ok(true)
            },
        )
        .ok()
        .unwrap();
        run.stop(());
        let deadline = ShutdownDeadline::new(GRACE);
        let stopped = Instant::now()
            .checked_sub(GRACE)
            .expect("the monotonic clock has run longer than the grace");
        deadline.fix(stopped);
        let joined = within_10s(move || join_until(handles, deadline.fix(Instant::now())));
        release.open();
        assert_eq!(
            joined.left_running,
            [ThreadLeftover {
                thread: "late-group-0".into(),
                what: Leftover::Thread
            }]
        );
    }

    #[test]
    fn the_join_hands_back_what_ended_well_beside_the_failure() {
        let run = TestRun::default();
        let handles = spawn_per_core(
            "j",
            vec![0usize, 1, 2].into_iter().map(|p| (p, None)).collect(),
            1 << 20,
            &run,
            &mut OsSpawner,
            |slot, payload, _stop| {
                assert_eq!(
                    std::thread::current().name(),
                    Some(format!("j-{}", slot.index()).as_str())
                );
                if payload == 1 {
                    body(Err(BodyError::NotServed))
                } else {
                    Ok(payload * 10)
                }
            },
        )
        .ok()
        .unwrap();
        let joined = join(handles, &run, Duration::from_secs(5));
        assert_eq!(joined.ended, vec![0, 20]);
        assert_eq!(joined.failures.len(), 1);
        assert!(matches!(
            joined.failures[0].error,
            ThreadError::Returned(BodyError::NotServed)
        ));
        assert_eq!(joined.failures[0].thread, "j-1");
        assert_eq!(run.cause(), failed("j-1"), "the failed thread stopped the others");
    }

    /// Thread 0 fails only after the stop, so it is joined first but is not the cause.
    #[test]
    fn the_cause_is_the_failure_that_stopped_the_others() {
        let run = TestRun::default();
        let handles = spawn_per_core(
            "x",
            vec![0usize, 1].into_iter().map(|p| (p, None)).collect(),
            1 << 20,
            &run,
            &mut OsSpawner,
            |_slot, payload, stop| -> Result<(), ThreadError<BodyError>> {
                if payload == 1 {
                    return body(Err(BodyError::NotServed));
                }
                block_until_cancelled(&stop.token());
                body(Err(BodyError::AlsoNotServed))
            },
        )
        .ok()
        .unwrap();
        let joined = join(handles, &run, Duration::from_secs(5));
        let failed_threads: Vec<_> = joined.failures.iter().map(|f| f.thread.as_str()).collect();
        assert_eq!(failed_threads, ["x-0", "x-1"], "every failure, in thread order");
        assert_eq!(run.cause(), failed("x-1"));
    }

    /// The stop came from the caller (a signal, `Server.shutdown()`); the thread fails only as it
    /// ends, and that failure is not why the run stopped.
    #[test]
    fn a_failure_after_the_callers_stop_is_no_cause() {
        let run = RunStop::<&'static str>::default();
        let handles = spawn_per_core(
            "t",
            vec![((), None)],
            1 << 20,
            &run,
            &mut OsSpawner,
            |_slot, (), own| -> Result<(), ThreadError<BodyError>> {
                block_until_cancelled(&own.token());
                body(Err(BodyError::AlsoNotServed))
            },
        )
        .ok()
        .unwrap();
        run.stop("SIGTERM");
        block_until_cancelled(&run.token());
        let joined = join_until(handles, Instant::now() + JOIN_TIMEOUT);
        assert_eq!(joined.failures.len(), 1, "the failure is still there");
        assert_eq!(run.cause(), Some(StopCause::Caller("SIGTERM")));
    }

    /// The token's owner cancels it directly, as `Server.shutdown()` does: the run has no cause,
    /// and a thread that fails as it stops does not become one.
    #[test]
    fn a_failure_after_the_owner_cancelled_the_token_is_no_cause() {
        let token = CancellationToken::new();
        let run = TestRun::new(token.clone());
        token.cancel();
        let failed = run_as(&run, "t-0", |_| body::<()>(Err(BodyError::AlsoNotServed)));
        assert!(failed.outcome.is_err());
        assert!(run.is_stopped());
        assert_eq!(run.cause(), None);
    }

    /// The spawn's failure stopped the run; a started thread that fails as it ends is not the
    /// cause.
    #[test]
    fn a_failure_after_a_failed_spawn_is_no_cause() {
        let run = TestRun::default();
        let failure = match spawn_per_core(
            "t",
            vec![((), None), ((), None)],
            1 << 20,
            &run,
            &mut RefuseAt(1),
            |_slot, (), own| -> Result<(), ThreadError<BodyError>> {
                block_until_cancelled(&own.token());
                body(Err(BodyError::AlsoNotServed))
            },
        ) {
            Ok(_) => panic!("the second spawn was refused"),
            Err(failure) => failure,
        };
        let joined = join_until(failure.started, Instant::now() + JOIN_TIMEOUT);
        assert_eq!(joined.failures.len(), 1);
        assert_eq!(run.cause(), Some(StopCause::NotSpawned { thread: "t-1".into() }));
    }

    /// Nothing stops the run, and every thread ends well; the join must not wait forever.
    #[test]
    fn a_run_whose_threads_all_end_well_is_over_with_no_stop() {
        let run = TestRun::default();
        let handles = spawn_per_core(
            "ok",
            vec![(1usize, None), (2, None)],
            1 << 20,
            &run,
            &mut OsSpawner,
            |_slot, payload, _own| -> Result<usize, ThreadError<BodyError>> { Ok(payload) },
        )
        .ok()
        .unwrap();
        let waited = run.clone();
        let joined = within_10s(move || join(handles, &waited, JOIN_TIMEOUT));
        assert_eq!(joined.ended, vec![1, 2]);
        assert_eq!(run.cause(), Some(StopCause::ThreadsEnded));
    }

    /// One thread still runs, so the run is not over though the other ended well.
    #[test]
    fn a_run_with_a_thread_still_running_is_not_over() {
        let run = TestRun::default();
        let release = Gate::default();
        let held = release.clone();
        let handles = spawn_per_core(
            "half",
            vec![(0usize, None), (1, None)],
            1 << 20,
            &run,
            &mut OsSpawner,
            move |_slot, payload, _own| -> Result<usize, ThreadError<BodyError>> {
                if payload == 1 {
                    held.wait();
                }
                Ok(payload)
            },
        )
        .ok()
        .unwrap();
        while !handles[0].thread.is_finished() {
            std::thread::yield_now();
        }
        assert!(!run.is_stopped(), "one ended, one runs");
        release.open();
        let waited = run.clone();
        let joined = within_10s(move || join(handles, &waited, JOIN_TIMEOUT));
        assert_eq!(joined.ended, vec![0, 1]);
    }

    /// The first stop is the cause; a later one, by a failure or by the caller, changes nothing.
    #[test]
    fn the_first_stop_is_the_runs_cause() {
        let run = RunStop::<&'static str>::default();
        let ok = run_as(&run, "t-0", |_| Ok::<u8, ThreadError<BodyError>>(7));
        assert!(matches!(ok.outcome, Ok(7)));
        assert!(!run.is_stopped(), "a body that ended well stops nothing");
        assert_eq!(run.cause(), None);

        let first = run_as(&run, "t-1", |_| body::<u8>(Err(BodyError::NotServed)));
        assert!(first.outcome.is_err());
        assert!(run.is_stopped(), "a failure stops the run");

        let panicked = run_as(&run, "t-2", |_| -> Result<u8, ThreadError<BodyError>> {
            if std::hint::black_box(true) {
                panic!("a body that panics");
            }
            Ok(0)
        });
        assert!(matches!(&panicked.outcome, Err(ThreadError::Panicked(payload)) if payload == "a body that panics"));
        run.stop("SIGINT");
        assert_eq!(run.cause(), Some(StopCause::ThreadFailed { thread: "t-1".into() }));

        let another_run = RunStop::<&'static str>::default();
        another_run.stop("SIGINT");
        run_as(&another_run, "u-0", |_| body::<u8>(Err(BodyError::NotServed)));
        assert_eq!(
            another_run.cause(),
            Some(StopCause::Caller("SIGINT")),
            "another run has its own cause"
        );
    }

    /// The fold that sorts a run's joins is fed values, with no thread started.
    #[test]
    fn the_tally_sorts_each_join_by_how_it_ended() {
        fn joined(
            thread: &str,
            joined: Result<Result<u8, ThreadError<BodyError>>, ThreadJoinError>,
            pool: usize,
        ) -> JoinOutcome<u8, BodyError> {
            JoinOutcome {
                thread: thread.into(),
                joined,
                pool_threads: NonZeroUsize::new(pool),
            }
        }
        let joins = vec![
            joined("a", Ok(Ok(1)), 0),
            joined("b", Ok(Ok(2)), 3),
            joined("c", Ok(Err(ThreadError::Returned(BodyError::NotServed))), 0),
            joined("d", Err(ThreadJoinError::StillRunning), 2),
            joined("e", Err(ThreadJoinError::Panicked("past the body".into())), 0),
        ];
        let results = ThreadResults::tally(joins);
        assert_eq!(results.ended, [1, 2]);
        assert_eq!(
            results.left_running,
            [
                ThreadLeftover {
                    thread: "b".into(),
                    what: Leftover::BlockingCalls(NonZeroUsize::new(3).unwrap())
                },
                ThreadLeftover {
                    thread: "d".into(),
                    what: Leftover::Thread
                },
            ]
        );
        let failed: Vec<_> = results.failures.iter().map(|f| f.thread.as_str()).collect();
        assert_eq!(failed, ["c", "e"]);
        assert!(matches!(&results.failures[1].error, ThreadError::Panicked(p) if p == "past the body"));
        assert!(!results.is_clean());
    }

    #[test]
    fn a_run_that_ended_well_and_left_nothing_is_clean() {
        let ended = |pool| {
            ThreadResults::<u8, BodyError>::tally([JoinOutcome {
                thread: "a".into(),
                joined: Ok(Ok(1)),
                pool_threads: NonZeroUsize::new(pool),
            }])
        };
        assert!(ended(0).is_clean());
        assert!(!ended(1).is_clean(), "a call left running is not a clean end");
    }

    #[test]
    fn a_failed_spawn_drops_every_payload_on_this_thread() {
        let run = TestRun::default();
        let (payloads, drops) = witnesses(&[1, 2, 3]);
        let failure = match spawn_per_core(
            "s",
            payloads,
            usize::MAX / 2,
            &run,
            &mut OsSpawner,
            |_slot, _payload, _stop| -> Result<(), ThreadError<BodyError>> { Ok(()) },
        ) {
            Ok(_) => panic!("a stack of usize::MAX / 2 can't be had"),
            Err(failure) => failure,
        };
        assert_eq!(
            run.cause(),
            Some(StopCause::NotSpawned { thread: "s-0".into() }),
            "the spawn's failure stopped the run"
        );
        let here = std::thread::current().id();
        let mut dropped = drops.lock().unwrap().clone();
        dropped.sort_unstable_by_key(|(value, _)| *value);
        assert_eq!(dropped, vec![(1, here), (2, here), (3, here)]);
        assert!(failure.started.is_empty());
        assert_eq!(failure.error.thread, "s-0");
    }

    /// Covers both stop-watcher tests of the arm's failure (b/636 ⑱): it names the watcher and the
    /// cause, runs nothing, starts no thread and leaves the stop alone.
    #[test]
    fn a_stop_watcher_whose_arm_fails_starts_no_thread_and_does_not_stop() {
        let run = TestRun::default();
        let (ran, heard) = std::sync::mpsc::channel::<()>();
        let started = spawn_stop_watcher(
            "unarmed-watch",
            &run,
            || -> std::io::Result<()> { Err(std::io::Error::other("could not register the SIGTERM handler")) },
            move |_stop, ()| async move {
                ran.send(()).expect("the test listens until the start returns");
            },
        );
        match started {
            Err(StopWatcherError::NotArmed { thread, source }) => {
                assert_eq!(thread, "unarmed-watch");
                assert_eq!(source.to_string(), "could not register the SIGTERM handler");
            }
            Err(other) => panic!("expected the arm's failure, got {other:?}"),
            Ok(_) => panic!("a watcher whose arm failed started"),
        }
        // `until_stopped` was dropped unrun, so the sender it held is gone.
        assert_eq!(heard.recv(), Err(std::sync::mpsc::RecvError));
        assert!(!run.is_stopped());
    }

    /// The deadline has already passed, so `finish` gives up at once rather than wait for the
    /// thread, which is held until after it returns.
    #[test]
    fn finish_reports_a_stop_watcher_thread_that_ignores_the_stop() {
        let release = Gate::default();
        let held = release.clone();
        let watcher = spawn_stop_watcher(
            "finish-watch",
            &TestRun::default(),
            || Ok(()),
            move |_stop, ()| async move { held.wait() },
        )
        .unwrap();
        assert!(matches!(
            watcher.finish(Instant::now()),
            Err(ThreadJoinError::StillRunning)
        ));
        release.open();
    }

    /// A `println!` to a closed stdout is how a real watcher panics.
    #[test]
    fn a_stop_watcher_that_panics_still_cancels_the_stop() {
        let run = TestRun::default();
        let watcher = spawn_stop_watcher(
            "panicking-watch",
            &run,
            || Ok(()),
            |_stop, ()| async {
                if std::hint::black_box(true) {
                    panic!("failed printing to stdout: Broken pipe");
                }
            },
        )
        .unwrap();
        let watched = run.token();
        within_10s(move || block_until_cancelled(&watched));
        assert_eq!(run.cause(), None, "a watcher stops the run from outside");
        assert!(matches!(
            watcher.finish(Instant::now() + JOIN_TIMEOUT),
            Err(ThreadJoinError::Panicked(payload)) if payload == "failed printing to stdout: Broken pipe"
        ));
    }

    #[test]
    fn a_stop_watcher_that_returns_cancels_the_stop() {
        let run = TestRun::default();
        let watcher = spawn_stop_watcher("returning-watch", &run, || Ok(()), |_stop, ()| async {}).unwrap();
        let watched = run.token();
        within_10s(move || block_until_cancelled(&watched));
        assert!(watcher.finish(Instant::now() + JOIN_TIMEOUT).is_ok());
    }

    #[test]
    fn a_spawn_failing_midway_stops_the_started_threads_and_drops_the_rest_here() {
        let run = TestRun::default();
        let (payloads, drops) = witnesses(&[10, 11, 12, 13]);
        let failure = match spawn_per_core(
            "m",
            payloads,
            1 << 20,
            &run,
            &mut RefuseAt(2),
            |_slot, payload, stop| -> Result<usize, ThreadError<BodyError>> {
                block_until_cancelled(&stop.token());
                Ok(payload.value)
            },
        ) {
            Ok(_) => panic!("the third spawn was refused"),
            Err(failure) => failure,
        };
        assert!(run.is_stopped());
        let here = std::thread::current().id();
        let mut dropped_here: Vec<_> = drops
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, by)| *by == here)
            .map(|(value, _)| *value)
            .collect();
        dropped_here.sort_unstable();
        assert_eq!(dropped_here, vec![12, 13], "the payloads no thread took, dropped here");
        assert_eq!(failure.error.thread, "m-2");
        // The two started threads end on the stop, and the caller joins them.
        let joined = join_until(failure.started, Instant::now() + JOIN_TIMEOUT);
        let mut ended = joined.ended;
        ended.sort_unstable();
        assert_eq!(ended, vec![10, 11]);
        assert!(joined.failures.is_empty());
        assert!(joined.left_running.is_empty());
    }

    /// The started threads wait for the caller, not for the stop; the caller releases them before
    /// it joins, so they end well instead of at the deadline.
    #[test]
    fn the_caller_releases_the_started_threads_before_it_joins_them() {
        let run = TestRun::default();
        let release = Gate::default();
        let held = release.clone();
        let failure = match spawn_per_core(
            "h",
            vec![(0usize, None), (1, None)],
            1 << 20,
            &run,
            &mut RefuseAt(1),
            move |_slot, payload, _stop| -> Result<usize, ThreadError<BodyError>> {
                held.wait();
                Ok(payload)
            },
        ) {
            Ok(_) => panic!("the second spawn was refused"),
            Err(failure) => failure,
        };
        release.open();
        let joined = join_until(failure.started, Instant::now() + JOIN_TIMEOUT);
        assert_eq!(joined.ended, vec![0], "released, it ended well");
        assert!(joined.left_running.is_empty());
    }

    #[test]
    fn a_cause_and_an_abandoned_thread_come_back_together() {
        let run = TestRun::default();
        let release = Gate::default();
        let held = release.clone();
        let handles = spawn_per_core(
            "x",
            vec![0usize, 1].into_iter().map(|p| (p, None)).collect(),
            1 << 20,
            &run,
            &mut OsSpawner,
            move |_slot, payload, _stop| -> Result<(), ThreadError<BodyError>> {
                if payload == 0 {
                    return body(Err(BodyError::NotServed));
                }
                held.wait();
                Ok(())
            },
        )
        .ok()
        .unwrap();
        let joined = join(handles, &run, Duration::from_millis(10));
        release.open();
        assert_eq!(
            joined.left_running,
            [ThreadLeftover {
                thread: "x-1".into(),
                what: Leftover::Thread
            }]
        );
        assert_eq!(joined.failures[0].thread, "x-0");
        assert_eq!(run.cause(), failed("x-0"));
    }

    #[test]
    fn a_thread_still_running_at_the_deadline_is_abandoned() {
        let run = TestRun::default();
        // The deadline runs from the stop, so the stop comes first.
        run.stop(());
        let release = Gate::default();
        let held = release.clone();
        let handles = spawn_per_core(
            "slow",
            vec![0usize].into_iter().map(|p| (p, None)).collect(),
            1 << 20,
            &run,
            &mut OsSpawner,
            move |_slot, _payload, _stop| -> Result<usize, ThreadError<BodyError>> {
                held.wait();
                Ok(1)
            },
        )
        .ok()
        .unwrap();
        let joined = join(handles, &run, Duration::from_millis(10));
        release.open();
        assert_eq!(
            joined.left_running,
            [ThreadLeftover {
                thread: "slow-0".into(),
                what: Leftover::Thread
            }]
        );
        assert!(joined.ended.is_empty());
        assert!(joined.failures.is_empty());
        assert!(!joined.is_clean(), "an abandoned thread is not a clean end");
    }

    /// The blocking call waits for the run's stop, so a runtime shut down before the stop would
    /// wait for it forever; told first, the call returns and the hour-long shutdown ends at once.
    #[test]
    fn a_failed_serve_stops_the_run_before_its_runtime_waits_for_the_blocking_pool() {
        let run = TestRun::default();
        let stopped = run.clone();
        let finished = within_10s(move || {
            run_as(&stopped, "t-0", |own| {
                let token = own.token();
                let (entered, in_pool) = mpsc::channel::<()>();
                serve_here(own, Duration::from_secs(3600), || async move {
                    drop(tokio::task::spawn_blocking(move || {
                        entered.send(()).expect("the serve waits for this");
                        block_until_cancelled(&token);
                    }));
                    in_pool.recv().expect("the call started");
                    Err::<(), BodyError>(BodyError::NotServed)
                })
            })
        });
        assert!(matches!(
            finished.outcome,
            Err(ThreadError::Returned(BodyError::NotServed))
        ));
        assert_eq!(run.cause(), failed("t-0"));
        assert_eq!(finished.left_on_pool, None, "the call returned within the limit");
    }

    /// Runs one thread whose serve leaves a call on the blocking pool, held until the test
    /// releases it, and serves it as `serve` ends.
    fn stuck_serve(
        name: &str,
        run: &TestRun,
        release: &Gate,
        serve: fn() -> Result<(), BodyError>,
    ) -> Vec<ThreadHandle<(), BodyError>> {
        let held = release.clone();
        spawn_per_core(
            name,
            vec![(0usize, None)],
            1 << 20,
            run,
            &mut OsSpawner,
            move |_slot, _payload, own| -> Result<(), ThreadError<BodyError>> {
                let held = held.clone();
                let (entered, in_pool) = mpsc::channel::<()>();
                serve_here(own, Duration::ZERO, || async move {
                    drop(tokio::task::spawn_blocking(move || {
                        entered.send(()).expect("the serve waits for this");
                        held.wait();
                    }));
                    in_pool.recv().expect("the call started");
                    serve()
                })
            },
        )
        .ok()
        .unwrap()
    }

    #[test]
    fn a_failed_serve_with_a_stuck_blocking_call_stops_the_run_and_its_thread_ends_at_the_limit() {
        let run = TestRun::default();
        let release = Gate::default();
        let handles = stuck_serve("stuck", &run, &release, || Err(BodyError::NotServed));
        let waited = run.clone();
        let joined = within_10s(move || join(handles, &waited, JOIN_TIMEOUT));
        release.open();
        assert!(matches!(
            joined.failures[0].error,
            ThreadError::Returned(BodyError::NotServed)
        ));
        assert_eq!(run.cause(), failed("stuck-0"));
        assert_eq!(
            joined.left_running,
            [ThreadLeftover {
                thread: "stuck-0".into(),
                what: Leftover::BlockingCalls(NonZeroUsize::MIN)
            }]
        );
        assert!(
            !joined.is_clean(),
            "a thread that left a call running is not a clean end"
        );
    }

    #[test]
    fn a_thread_that_ends_well_leaving_a_stuck_blocking_call_is_named_by_the_join() {
        let run = TestRun::default();
        let release = Gate::default();
        let handles = stuck_serve("kept", &run, &release, || Ok(()));
        run.stop(());
        let waited = run.clone();
        let joined = within_10s(move || join(handles, &waited, JOIN_TIMEOUT));
        release.open();
        assert_eq!(joined.ended, vec![()]);
        assert!(joined.failures.is_empty());
        assert_eq!(
            joined.left_running,
            [ThreadLeftover {
                thread: "kept-0".into(),
                what: Leftover::BlockingCalls(NonZeroUsize::MIN)
            }]
        );
        assert!(!joined.is_clean(), "not a clean end");
    }

    /// The body panics after its serve left a call running; the unwind takes any return value
    /// with it, and the join must still learn of the call.
    #[test]
    fn a_body_that_panics_after_its_serve_still_says_what_its_runtime_left() {
        let release = Gate::default();
        let held = release.clone();
        let finished = within_10s(move || {
            run_as(
                &TestRun::default(),
                "t-0",
                |own| -> Result<(), ThreadError<BodyError>> {
                    let (entered, in_pool) = mpsc::channel::<()>();
                    let served = serve_here(own, Duration::ZERO, || async move {
                        drop(tokio::task::spawn_blocking(move || {
                            entered.send(()).expect("the serve waits for this");
                            held.wait();
                        }));
                        in_pool.recv().expect("the call started");
                        Ok(())
                    });
                    assert!(served.is_ok());
                    panic!("the body panicked after its serve");
                },
            )
        });
        release.open();
        assert_eq!(finished.left_on_pool, NonZeroUsize::new(1));
        assert!(finished.outcome.is_err());
    }

    struct PanicsOnDrop;

    impl Drop for PanicsOnDrop {
        fn drop(&mut self) {
            panic!("the payload's drop panicked");
        }
    }

    /// A second panic escaping `stop_on_failure` would stop nothing and leave the join waiting
    /// forever; [`within_10s`] catches that hang, since nothing here waits on a clock.
    #[test]
    fn a_panic_whose_payload_panics_on_drop_still_stops_the_run() {
        let run = TestRun::default();
        let handles = spawn_per_core(
            "bomb",
            vec![(0usize, None)],
            1 << 20,
            &run,
            &mut OsSpawner,
            |_slot, _payload, _own| -> Result<(), ThreadError<BodyError>> { std::panic::panic_any(PanicsOnDrop) },
        )
        .ok()
        .unwrap();
        let waited = run.clone();
        let joined = within_10s(move || join(handles, &waited, JOIN_TIMEOUT));
        assert_eq!(run.cause(), failed("bomb-0"));
        let error = &joined.failures[0].error;
        assert!(
            matches!(error, ThreadError::Panicked(payload)
                if payload == "<panic payload is neither &str nor String>; dropping its payload panicked too: the payload's drop panicked"),
            "{error}"
        );
    }

    /// An unwind through the runtime's drop would wait for the stuck call with no limit and never
    /// stop the run; [`within_10s`] catches that hang, since nothing here waits on a clock.
    #[test]
    fn a_serve_that_panics_with_a_stuck_blocking_call_stops_the_run_and_leaves_the_call() {
        let run = TestRun::default();
        let release = Gate::default();
        let handles = stuck_serve("panics", &run, &release, || {
            if std::hint::black_box(true) {
                panic!("serve panicked");
            }
            Ok(())
        });
        let waited = run.clone();
        let joined = within_10s(move || join(handles, &waited, JOIN_TIMEOUT));
        release.open();
        assert_eq!(run.cause(), failed("panics-0"));
        assert!(matches!(&joined.failures[0].error, ThreadError::Panicked(payload) if payload == "serve panicked"));
        assert_eq!(
            joined.left_running,
            [ThreadLeftover {
                thread: "panics-0".into(),
                what: Leftover::BlockingCalls(NonZeroUsize::MIN)
            }]
        );
    }

    #[derive(Clone, Default)]
    struct PanicsBeforeServe {
        before_serve: Arc<AtomicUsize>,
        after_serve: Arc<AtomicUsize>,
    }

    impl ThreadHooks for PanicsBeforeServe {
        fn before_serve(&self) {
            self.before_serve.fetch_add(1, Ordering::SeqCst);
            panic!("before_serve panicked");
        }
        fn after_serve(&self) {
            self.after_serve.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn a_panicking_before_serve_is_still_paired_with_after_serve() {
        let hooks = PanicsBeforeServe::default();
        let served = Arc::new(AtomicUsize::new(0));
        let ran = Arc::clone(&served);
        let outcome = serve_test(&HostOs, hooks.clone(), &own_stop("t"), Duration::ZERO, || async move {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        assert_eq!(
            (
                hooks.before_serve.load(Ordering::SeqCst),
                hooks.after_serve.load(Ordering::SeqCst)
            ),
            (1, 1)
        );
        assert_eq!(served.load(Ordering::SeqCst), 0, "nothing served");
        assert!(matches!(&outcome, Err(ThreadError::Panicked(payload)) if payload == "before_serve panicked"));
    }

    #[derive(Clone)]
    struct AfterServePanic;

    impl ThreadHooks for AfterServePanic {
        fn after_serve(&self) {
            panic!("after_serve panicked");
        }
    }

    /// The hook's panic must not unwind through the runtime, whose drop would wait for the stuck
    /// call with no limit and never stop the run; [`within_10s`] catches that hang.
    #[test]
    fn an_after_serve_panic_with_a_stuck_blocking_call_stops_the_run_and_ends_at_the_limit() {
        let release = Gate::default();
        let held = release.clone();
        let run = TestRun::default();
        let stopped = run.clone();
        let finished = within_10s(move || {
            run_as(&stopped, "t-0", |own| {
                let (entered, in_pool) = mpsc::channel::<()>();
                serve_on_this_thread(
                    pin_and_raise_qos(None),
                    BLOCKING_STACK,
                    AfterServePanic,
                    own,
                    || Duration::ZERO,
                    || async move {
                        drop(tokio::task::spawn_blocking(move || {
                            entered.send(()).expect("the serve waits for this");
                            held.wait();
                        }));
                        in_pool.recv().expect("the call started");
                        Ok::<(), BodyError>(())
                    },
                )
            })
        });
        release.open();
        assert_eq!(run.cause(), failed("t-0"), "the failure stopped the run");
        assert!(matches!(&finished.outcome, Err(ThreadError::Panicked(payload)) if payload == "after_serve panicked"));
        assert_eq!(finished.left_on_pool, NonZeroUsize::new(1));
    }

    #[test]
    fn an_after_serve_panic_after_a_panicking_serve_is_said_beside_it() {
        let served = serve_test(&HostOs, AfterServePanic, &own_stop("t"), Duration::ZERO, || async {
            if std::hint::black_box(true) {
                panic!("serve panicked");
            }
            Ok(())
        });
        assert!(matches!(&served, Err(ThreadError::Panicked(payload))
            if payload == "serve panicked; after_serve panicked too: after_serve panicked"));
    }

    #[test]
    fn a_failure_told_early_and_again_is_still_one_cause() {
        let run = TestRun::default();
        let told = run_as(&run, "t-0", |own| {
            own.fail();
            own.fail();
            assert!(run.is_stopped(), "telling the failure stops the run");
            body::<()>(Err(BodyError::NotServed))
        });
        let companion = run_as(&run, "t-1", |_| body::<()>(Err(BodyError::AlsoNotServed)));
        assert!(told.outcome.is_err() && companion.outcome.is_err());
        assert_eq!(run.cause(), failed("t-0"));
    }

    #[test]
    fn a_serve_that_ends_well_stops_nothing() {
        let run = TestRun::default();
        let finished = run_as(&run, "t-0", |own| serve_here(own, Duration::ZERO, || async { Ok(()) }));
        assert!(matches!(finished.outcome, Ok(())));
        assert!(!run.is_stopped());
    }

    #[test]
    fn a_thread_started_in_a_run_is_joined_with_it() {
        let run = TestRun::default();
        let handle = spawn_in_run(
            "acceptor",
            1 << 20,
            &run,
            |_own| -> Result<String, ThreadError<BodyError>> {
                Ok(std::thread::current().name().unwrap_or_default().to_string())
            },
        )
        .unwrap();
        assert_eq!(handle.name(), "acceptor");
        let waited = run.clone();
        let joined = within_10s(move || join(vec![handle], &waited, JOIN_TIMEOUT));
        assert_eq!(joined.ended, ["acceptor"], "it ran under its name");
        assert_eq!(run.cause(), Some(StopCause::ThreadsEnded), "and its end ended the run");
    }
}
