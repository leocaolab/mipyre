//! Starts, stops, drains and joins the TPC threads, and says how each one ended.

use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use mipyre_cpu::CoreId;
use mipyre_tpc::{
    CoreSlot, Leftover, LocalTasks, ShutdownDeadline, Spawner, StopCause, ThreadError, ThreadFailure, ThreadHandle,
    ThreadHooks, ThreadResults, ThreadStop,
};

use super::conn::{ThreadWorker, TpcThread};
use super::startup::{report_affinity, with_cores};
use super::{ServeError, TpcServer, TpcThreadSetup};
use crate::ServerCounters;
use crate::{drain_timeout, report_drain_timeout, teardown_timeout};
use crate::{ServerRunStop, ServerStop};
use mipyre_process::LaneHost;
use mipyre_runtime::{start_here, StartGate, StartHandle, SubInterpreterWorker, SyncRunner};

/// One TPC thread's part in the server's stop.
pub(super) type TpcThreadStop = ThreadStop<ServerStop>;

pub(super) type ServeThread = ThreadHandle<(), ServeError>;

/// A TPC thread whose accept loop has ended: it takes no more connections, and holds the ones still
/// open and the process lanes it hosts until they finish.
pub(super) struct ThreadDrain {
    pub(super) connections: LocalTasks,
    pub(super) lane_host: Option<Rc<LaneHost>>,
}

impl ThreadDrain {
    /// Lets the connections finish until the run's stop `deadline` less the teardown margin, then
    /// shuts the lane host down; the connections still open are closed, counted and logged.
    pub(super) async fn finish(self, deadline: &ShutdownDeadline, counters: &ServerCounters) {
        drain_connections(self.connections, deadline, counters).await;
        if let Some(host) = self.lane_host {
            host.shutdown().await;
        }
    }
}

/// Lets a stopped accept loop's connections finish until the run's stop `deadline` less the
/// teardown margin; the ones still open then are closed, counted and logged.
async fn drain_connections(tracker: LocalTasks, deadline: &ShutdownDeadline, counters: &ServerCounters) {
    drain_within(tracker, drain_timeout(deadline, Instant::now()), counters).await;
}

/// Lets `tracker`'s connections finish for at most `timeout`; the ones still open then are
/// counted and logged, and closed as their tasks drop.
async fn drain_within(mut tracker: LocalTasks, timeout: Duration, counters: &ServerCounters) {
    if tokio::time::timeout(timeout, tracker.idle()).await.is_err() {
        report_drain_timeout(tracker.len(), timeout, counters);
    }
}

/// Returns the run's error for a thread's failure: the error its code returned as it is, a panic
/// or a runtime it could not build as the thread's.
fn into_serve_error(failure: ThreadFailure<ServeError>) -> ServeError {
    let thread = failure.thread;
    match failure.error {
        ThreadError::Returned(error) => error,
        ThreadError::Panicked(message) => ServeError::ThreadPanicked { thread, message },
        ThreadError::RuntimeNotBuilt(source) => ServeError::RuntimeNotBuilt { thread, source },
    }
}

pub(super) fn stopped_error(root: Option<ServeError>, abandoned: Vec<String>) -> ServeError {
    match root {
        Some(root) if abandoned.is_empty() => root,
        root => ServeError::ThreadsStopped {
            root: root.map(Box::new),
            abandoned,
        },
    }
}

/// Waits until the run stops, by a failure, from outside, or because every thread ended, then
/// joins every TPC thread by the stop's `deadline`; every failure and every thread that left
/// something running is logged.
pub(super) fn join_run(
    handles: Vec<ServeThread>,
    run: &ServerRunStop,
    deadline: &ShutdownDeadline,
    counters: &ServerCounters,
) -> ThreadResults<(), ServeError> {
    mipyre_tpc::block_until_cancelled(&run.token());
    let joined = mipyre_tpc::join_until(handles, deadline.fix(Instant::now()));
    report(&joined, deadline.grace(), counters);
    joined
}

/// Returns the error a run that was joined ends with: the failure of the thread that stopped the
/// run, if `cause` (the run's) says one did. The other failures were logged at the join; when no thread's failure stopped
/// the run they are named in [`ServeError::FailedWhileStopping`]. A thread that left something
/// running is abandoned: what it runs may hold Python, so the process must not finalize.
pub(super) fn settle(
    joined: ThreadResults<(), ServeError>,
    cause: Option<StopCause<ServerStop>>,
) -> Result<(), ServeError> {
    if joined.is_clean() {
        return Ok(());
    }
    let left = left_running(&joined);
    let ThreadResults { failures, .. } = joined;
    let root = match cause {
        Some(StopCause::ThreadFailed { thread }) => failures.iter().position(|failure| failure.thread == thread),
        Some(StopCause::NotSpawned { .. } | StopCause::ThreadsEnded | StopCause::Caller(_)) | None => None,
    };
    match root {
        Some(at) => {
            let root = failures.into_iter().nth(at).map(into_serve_error);
            Err(stopped_error(root, left))
        }
        None if left.is_empty() => Err(ServeError::FailedWhileStopping {
            threads: failures.into_iter().map(|failure| failure.thread).collect(),
        }),
        None => Err(stopped_error(None, left)),
    }
}

/// Returns the threads of `joined` that left something running, which the process must not
/// finalize under.
pub(super) fn left_running(joined: &ThreadResults<(), ServeError>) -> Vec<String> {
    joined.left_running.iter().map(|left| left.thread.clone()).collect()
}

/// Logs every failed thread of `joined`, and logs and counts every thread that left something
/// running, since `mipyre_tpc` logs nothing.
fn report(joined: &ThreadResults<(), ServeError>, grace: Duration, counters: &ServerCounters) {
    for failure in &joined.failures {
        report_thread_failure(failure);
    }
    for left in &joined.left_running {
        match left.what {
            Leftover::Thread => tracing::error!(
                target: "mipyre::server",
                thread = %left.thread,
                "TPC thread still running at the join deadline ({grace:?}); left behind"
            ),
            Leftover::BlockingCalls(calls) => report_blocking_calls_left(&left.thread, calls.get(), counters),
        }
    }
}

fn report_thread_failure(failure: &ThreadFailure<ServeError>) {
    tracing::error!(
        target: "mipyre::server",
        thread = %failure.thread,
        error = %failure.error,
        "TPC thread failed"
    );
}

#[derive(Clone)]
struct TpcThreadHooks;

impl ThreadHooks for TpcThreadHooks {
    /// Keeps a worker thread's interpreter attached between requests until the thread is about to
    /// park.
    fn before_serve(&self) {
        mipyre_subinterp::keep_attached_until_idle();
    }

    /// Spins on this thread's process lanes before detaching its interpreter, since an answer
    /// arriving then keeps the thread from parking.
    fn on_park(&self) {
        mipyre_process::enter_idle();
        mipyre_subinterp::detach_for_idle();
    }

    fn on_unpark(&self) {
        mipyre_process::resume_from_idle();
    }

    /// Runs once the thread's tasks and the `Rc`s they hold are gone; the caller ends the worker
    /// next.
    fn after_serve(&self) {
        mipyre_subinterp::detach_for_idle();
    }
}

/// Runs `serve` on this new TPC thread, the one `stop` belongs to, pinned to `core` with its QoS
/// raised, on a current-thread runtime and `LocalSet` that are dropped before this returns, so
/// before the caller ends the thread's worker; a thread that could not be placed is counted in
/// `counters` and serves where it is. A failed serve is told to the run before the runtime waits
/// for its blocking pool, where main-interpreter handlers run; that wait lasts until the run's
/// stop `deadline`. A handler still running then is left running, and the join logs, counts and
/// abandons the thread.
pub(super) fn serve_on_this_thread<Fut>(
    core: Option<core_affinity::CoreId>,
    counters: &ServerCounters,
    stop: &TpcThreadStop,
    deadline: &ShutdownDeadline,
    serve: impl FnOnce() -> Fut,
) -> Result<(), ThreadError<ServeError>>
where
    Fut: Future<Output = Result<(), ServeError>>,
{
    let affinity = mipyre_tpc::pin_and_raise_qos(core);
    report_affinity(&affinity, counters);
    mipyre_tpc::serve_on_this_thread(
        affinity,
        // The blocking pool runs main-interpreter handlers, which need a Python-sized stack.
        mipyre_subinterp::PYTHON_THREAD_STACK,
        TpcThreadHooks,
        stop,
        || teardown_timeout(deadline, Instant::now()),
        serve,
    )
}

/// Logs and counts the `calls` main-interpreter handlers that TPC thread `thread` left running
/// past its runtime's shutdown at the stop deadline.
fn report_blocking_calls_left(thread: &str, calls: usize, counters: &ServerCounters) {
    counters.blocking_calls_left(calls);
    tracing::error!(
        target: "mipyre::server",
        thread = %thread,
        blocking_calls = calls,
        "TPC thread's runtime shut down at the stop deadline with blocking calls still running; left behind"
    );
}

/// Serves `serve` on `server`'s threads, thread `i` handed `payloads[i]`, and runs `meanwhile` on
/// this thread once every thread has started; then waits until the run stops and joins the threads
/// by its stop `deadline`. `spawner` starts the threads. The bench serves through this too, so its
/// threads are the server's.
pub fn serve_on_workers<T, F, Fut, R>(
    server: TpcServer,
    payloads: Vec<T>,
    run: &ServerRunStop,
    deadline: &ShutdownDeadline,
    spawner: &mut impl Spawner,
    serve: F,
    meanwhile: impl FnOnce() -> R,
) -> Result<R, ServeError>
where
    T: Send + 'static,
    F: Fn(Rc<TpcThread>, T, CancellationToken, ShutdownDeadline) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), ServeError>> + 'static,
{
    let TpcServer {
        threads,
        main,
        async_pool,
        cpus,
        counters,
        ..
    } = server;
    let threads = threads.into_setups(main, async_pool);
    // Both are sized by `Topology::accept_loops`; a zip would drop the extra ones unserved.
    assert_eq!(payloads.len(), threads.len(), "one payload per TPC thread");
    let threads = threads.into_iter().zip(payloads).collect();
    let handles = spawn_worker_threads(spawner, threads, &cpus, run, deadline, &counters, serve)?;
    let meant = meanwhile();
    settle(join_run(handles, run, deadline, &counters), run.cause())?;
    Ok(meant)
}

/// Starts one thread per entry of `threads` with `spawner`, thread `i` pinned to `cpus[i]` or unpinned past its
/// end and handed its payload. The threads create their workers one at a time in thread order and
/// serve only once every thread has started ([`StartGate`]). A failed start aborts the rest: the
/// workers already created end on their own threads and the error is returned. A failed spawn
/// stops the running threads before any of them creates a worker.
///
/// # Panics
///
/// Panics unless `payloads` has one entry per thread.
pub(super) fn spawn_worker_threads<T, F, Fut>(
    spawner: &mut impl Spawner,
    threads: Vec<(TpcThreadSetup, T)>,
    cpus: &[CoreId],
    run: &ServerRunStop,
    deadline: &ShutdownDeadline,
    counters: &ServerCounters,
    serve: F,
) -> Result<Vec<ServeThread>, ServeError>
where
    T: Send + 'static,
    F: Fn(Rc<TpcThread>, T, CancellationToken, ShutdownDeadline) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), ServeError>> + 'static,
{
    let recipe = threads
        .first()
        .and_then(|(setup, _)| setup.plan.as_ref())
        .map(|plan| Arc::clone(&plan.recipe));
    let started = threads.len();
    let mut gate = StartGate::<T>::new();
    let mut payloads = Vec::with_capacity(threads.len());
    let mut enlisted = Vec::with_capacity(threads.len());
    for ((setup, payload), core) in with_cores(threads, cpus) {
        // Each thread is enlisted before the spawn, so it carries its start handle in with its setup.
        enlisted.push(((setup, gate.enlist()), core));
        payloads.push(payload);
    }
    let thread_deadline = deadline.clone();
    let handles = start_tpc_threads(
        spawner,
        enlisted,
        run,
        deadline,
        counters,
        move |slot, (setup, start_handle), stop| {
            run_tpc_thread(slot, setup, start_handle, stop, &thread_deadline, &serve)
        },
        // The spawned threads wait for their turn, not for the run's stop, so they must be told to end
        // before they are joined; no turn was handed out yet, so none of them made a worker.
        || gate.abort(),
    )?;
    match gate.serve_or_abort(payloads) {
        Ok(()) => {
            if let Some(recipe) = recipe {
                recipe.report_started(started);
            }
            Ok(handles)
        }
        // The start's failure is returned here, so the run is stopped from outside.
        Err(aborted) => {
            run.cancel();
            let joined = join_run(handles, run, deadline, counters);
            Err(stopped_error(Some(aborted.into()), left_running(&joined)))
        }
    }
}

/// Creates this thread's worker here, serves once every thread has started, then ends the worker
/// on this thread.
fn run_tpc_thread<T, F, Fut>(
    slot: CoreSlot,
    setup: TpcThreadSetup,
    start_handle: StartHandle<T>,
    stop: &TpcThreadStop,
    deadline: &ShutdownDeadline,
    serve: &F,
) -> Result<(), ThreadError<ServeError>>
where
    F: Fn(Rc<TpcThread>, T, CancellationToken, ShutdownDeadline) -> Fut,
    Fut: Future<Output = Result<(), ServeError>> + 'static,
{
    let TpcThreadSetup {
        index,
        site,
        plan,
        main,
        async_pool,
    } = setup;
    let started = start_here(start_handle, index, || match plan {
        Some(plan) => {
            let created = SubInterpreterWorker::<SyncRunner>::create(index, &plan.recipe.spec())?;
            Ok(Some(ThreadWorker::new(created, plan.life, plan.retired)))
        }
        None => Ok(None),
    });
    let Some((worker, payload)) = started else {
        return Ok(());
    };
    let tpc_thread = Rc::new(TpcThread {
        index,
        worker,
        site,
        main,
        async_pool,
    });
    let counters = Arc::clone(&tpc_thread.site.counters.run);
    let served = serve_on_this_thread(slot.core(), &counters, stop, deadline, || {
        serve(Rc::clone(&tpc_thread), payload, stop.token(), deadline.clone())
    });
    TpcThread::end(tpc_thread);
    served
}

/// Starts pyre's TPC threads, named `mipyre-tpc-{i}`. When a spawn fails, the run is stopped;
/// this runs `on_spawn_failure` to release threads that wait for something other than the run's
/// stop, joins the threads already running by the stop's `deadline`, and returns the spawn's
/// error with the threads that are left running.
pub(super) fn start_tpc_threads<P, F>(
    spawner: &mut impl Spawner,
    payloads: Vec<(P, Option<CoreId>)>,
    run: &ServerRunStop,
    deadline: &ShutdownDeadline,
    counters: &ServerCounters,
    body: F,
    on_spawn_failure: impl FnOnce(),
) -> Result<Vec<ServeThread>, ServeError>
where
    P: Send + 'static,
    F: Fn(CoreSlot, P, &TpcThreadStop) -> Result<(), ThreadError<ServeError>> + Clone + Send + 'static,
{
    mipyre_tpc::spawn_per_core(
        "mipyre-tpc",
        payloads,
        mipyre_subinterp::PYTHON_THREAD_STACK,
        run,
        spawner,
        body,
    )
    .map_err(|failure| {
        let mipyre_tpc::SpawnFailure { error, started } = failure;
        on_spawn_failure();
        let joined = mipyre_tpc::join_until(started, deadline.fix(Instant::now()));
        report(&joined, deadline.grace(), counters);
        stopped_error(Some(error.into()), left_running(&joined))
    })
}

#[cfg(test)]
#[cfg(not(sync_model))]
mod stop_tests {
    use super::*;
    use mipyre_tpc::OsSpawner;
    use std::sync::mpsc;
    use std::sync::Mutex;

    /// Returns a stop deadline with the default shutdown grace of 5 s.
    fn grace() -> ShutdownDeadline {
        ShutdownDeadline::new(Duration::from_secs(5))
    }

    /// Runs `body` on a thread of a run of its own, as a TPC thread runs, and returns how it
    /// ended.
    fn on_tpc_thread<T: Send + 'static>(
        name: &str,
        body: impl FnOnce(&TpcThreadStop) -> Result<T, ThreadError<ServeError>> + Send + 'static,
    ) -> ThreadResults<T, ServeError> {
        let run = ServerRunStop::default();
        let handle = mipyre_tpc::spawn_in_run(name, mipyre_subinterp::PYTHON_THREAD_STACK, &run, body)
            .expect("the test thread starts");
        mipyre_tpc::join_until(vec![handle], Instant::now() + Duration::from_secs(10))
    }

    fn failed(thread: &str) -> mipyre_tpc::ThreadFailure<ServeError> {
        mipyre_tpc::ThreadFailure {
            thread: thread.into(),
            error: ThreadError::Panicked("injected".into()),
        }
    }

    fn failure_of(thread: &str) -> Option<StopCause<ServerStop>> {
        Some(StopCause::ThreadFailed { thread: thread.into() })
    }

    fn left(thread: &str) -> mipyre_tpc::ThreadLeftover {
        mipyre_tpc::ThreadLeftover {
            thread: thread.into(),
            what: Leftover::Thread,
        }
    }

    /// Checks that the blocking pool, where main-interpreter handlers run, starts threads with
    /// Python-sized stacks.
    #[test]
    fn a_tpc_threads_blocking_pool_has_python_stacks() {
        mipyre_runtime::testkit::thread_stack::with_exact_stacks(
            concat!(module_path!(), "::a_tpc_threads_blocking_pool_has_python_stacks"),
            || {
                let python =
                    mipyre_runtime::testkit::thread_stack::of_a_thread_with(mipyre_subinterp::PYTHON_THREAD_STACK);
                let joined = on_tpc_thread("tpc-stack-test", |stop| {
                    let read = std::cell::Cell::new(0);
                    let counters = ServerCounters::new(false);
                    serve_on_this_thread(None, &counters, stop, &grace(), || async {
                        let stack = tokio::task::spawn_blocking(mipyre_runtime::testkit::thread_stack::current).await;
                        read.set(stack.expect("on the blocking pool"));
                        Ok(())
                    })?;
                    Ok(read.get())
                });
                assert!(joined.failures.is_empty(), "the TPC thread failed");
                assert_eq!(joined.ended, [python]);
            },
        );
    }

    struct Held;

    impl mipyre_subinterp::Releasable for Held {
        fn release(self, _py: pyo3::Python<'_>) {}
    }

    /// Checks through CPython's own thread state that a TPC thread keeps its interpreter attached
    /// while it serves and that a panicking serve still detaches it, since `after_serve` runs on
    /// the unwind.
    #[test]
    fn a_panicking_serve_leaves_no_thread_state_attached() {
        pyo3::Python::initialize();
        let joined = on_tpc_thread("tpc-panic-test", |stop| {
            let mut interp = match mipyre_subinterp::SubInterpreter::<Held>::create_on_this_thread::<
                mipyre_subinterp::NewInterpreterError,
            >("tpc-panic-test".to_string(), std::convert::identity, |_py| Ok(Held))
            {
                Ok(interp) => interp,
                Err(e) => panic!("no sub-interpreter for the test: {e}"),
            };
            let attached_while_served = std::cell::Cell::new(false);
            let counters = ServerCounters::new(false);
            let served = serve_on_this_thread(None, &counters, stop, &grace(), || async {
                interp.with_gil(|_, _py| ()).expect("no panic in the call");
                // SAFETY: this only reads the calling thread's current thread state.
                let current = unsafe { pyo3::ffi::PyThreadState_GetUnchecked() };
                attached_while_served.set(!current.is_null());
                if std::hint::black_box(true) {
                    panic!("injected panic in serve");
                }
                Ok(())
            });
            // SAFETY: this only reads the calling thread's current thread state.
            let detached_after = unsafe { pyo3::ffi::PyThreadState_GetUnchecked() }.is_null();
            interp.end_on_own_thread();
            Ok((served, attached_while_served.get(), detached_after))
        });
        let [(served, attached_while_served, detached_after)] =
            <[_; 1]>::try_from(joined.ended).unwrap_or_else(|_| panic!("the test thread failed"));
        assert!(
            matches!(&served, Err(ThreadError::Panicked(payload)) if payload == "injected panic in serve"),
            "the panic is the serve's failure"
        );
        assert!(attached_while_served, "the interpreter was attached while served");
        assert!(detached_after, "no thread state is current after the panic");
    }

    /// Checks that a blocking call that never returns is given up on at the limit, logged and
    /// counted, and that its thread is abandoned so the process does not finalize.
    #[test]
    fn a_thread_that_leaves_a_stuck_blocking_call_is_counted_and_abandoned() {
        let run = ServerRunStop::default();
        let counters = Arc::new(ServerCounters::new(false));
        let thread_counters = Arc::clone(&counters);
        let (release, held) = mpsc::channel::<()>();
        let held = Arc::new(Mutex::new(held));
        let handles = mipyre_tpc::spawn_per_core(
            "tpc-stuck",
            vec![(0usize, None)],
            1 << 20,
            &run,
            &mut OsSpawner,
            move |_slot, _payload, own| -> Result<(), ThreadError<ServeError>> {
                let held = Arc::clone(&held);
                let (entered, in_pool) = mpsc::channel::<()>();
                // A zero grace leaves no time to the teardown, so the shutdown gives up at once.
                serve_on_this_thread(
                    None,
                    &thread_counters,
                    own,
                    &ShutdownDeadline::new(Duration::ZERO),
                    || async move {
                        drop(tokio::task::spawn_blocking(move || {
                            entered.send(()).expect("the serve waits for this");
                            held.lock().unwrap().recv().expect("released by the test");
                        }));
                        in_pool.recv().expect("the call started");
                        Ok(())
                    },
                )
            },
        )
        .ok()
        .unwrap();
        run.cancel();
        let outcome = settle(join_run(handles, &run, &grace(), &counters), run.cause());
        release.send(()).unwrap();
        assert_eq!(counters.read().blocking_calls_left, 1);
        match outcome {
            Err(ServeError::ThreadsStopped { root, abandoned }) => {
                assert!(root.is_none(), "no thread failed");
                assert_eq!(abandoned, vec!["tpc-stuck-0".to_string()]);
            }
            other => panic!(
                "expected the thread abandoned, got {:?}",
                other.err().map(|e| e.to_string())
            ),
        }
    }

    /// Checks that blocking calls left running are logged with the thread and how many.
    #[test]
    fn blocking_calls_left_running_are_said_with_the_thread_and_how_many() {
        let counters = ServerCounters::new(false);
        let log = mipyre_runtime::testkit::captured_log(|| report_blocking_calls_left("mipyre-tpc-3", 2, &counters));
        assert_eq!(counters.read().blocking_calls_left, 2);
        let line = log
            .lines()
            .find(|l| l.contains("blocking calls still running; left behind"))
            .unwrap_or_else(|| panic!("not said:\n{log}"));
        assert!(line.contains("\"thread\":\"mipyre-tpc-3\""), "{line}");
        assert!(line.contains("\"blocking_calls\":2"), "{line}");
        assert!(line.contains("\"target\":\"mipyre::server\""), "{line}");
    }

    /// Checks that an abandoned thread with nothing failed stops the run with no root cause and
    /// names the thread.
    #[test]
    fn an_abandoned_thread_alone_is_a_stopped_run_with_no_root_cause() {
        let run = ServerRunStop::default();
        // The stop comes before the join, and the thread ignores it until the test releases it.
        run.cancel();
        let (release, held) = mpsc::channel::<()>();
        let held = Arc::new(Mutex::new(held));
        let handles = mipyre_tpc::spawn_per_core(
            "tpc-solo",
            vec![(0usize, None)],
            1 << 20,
            &run,
            &mut OsSpawner,
            move |_slot, _payload, _stop| -> Result<(), ThreadError<ServeError>> {
                held.lock().unwrap().recv().expect("released by the test");
                Ok(())
            },
        )
        .ok()
        .unwrap();
        let counters = ServerCounters::new(false);
        let deadline = ShutdownDeadline::new(Duration::from_millis(10));
        let outcome = settle(join_run(handles, &run, &deadline, &counters), run.cause());
        release.send(()).unwrap();
        match outcome {
            Err(ServeError::ThreadsStopped { root, abandoned }) => {
                assert!(root.is_none(), "no thread failed");
                assert_eq!(abandoned, vec!["tpc-solo-0".to_string()]);
            }
            other => panic!(
                "expected an abandoned thread, got {:?}",
                other.err().map(|e| e.to_string())
            ),
        }
    }

    /// The root cause and the abandoned threads come back in one error: an abandoned thread may
    /// hold its interpreter, and the caller decides how to exit from that.
    #[test]
    fn settle_carries_the_root_cause_and_the_abandoned_threads_together() {
        let joined = mipyre_tpc::ThreadResults {
            ended: vec![],
            failures: vec![failed("mipyre-tpc-0")],
            left_running: vec![left("mipyre-tpc-1")],
        };
        match settle(joined, failure_of("mipyre-tpc-0")) {
            Err(ServeError::ThreadsStopped { root, abandoned }) => {
                assert!(matches!(
                    root.as_deref(),
                    Some(ServeError::ThreadPanicked { thread, .. }) if thread == "mipyre-tpc-0"
                ));
                assert_eq!(abandoned, vec!["mipyre-tpc-1".to_string()]);
            }
            other => panic!("expected both, got {:?}", other.err().map(|e| e.to_string())),
        }
    }

    #[test]
    fn settle_of_a_failure_with_nothing_abandoned_is_the_failure_itself() {
        let joined = mipyre_tpc::ThreadResults {
            ended: vec![],
            failures: vec![failed("mipyre-tpc-0")],
            left_running: vec![],
        };
        let err = settle(joined, failure_of("mipyre-tpc-0")).expect_err("a failure");
        assert!(err.abandoned_threads().is_empty());
        assert!(matches!(err, ServeError::ThreadPanicked { .. }));
    }

    /// The server was stopped from outside and a thread failed as it ended: that failure is no
    /// root cause, and the run still says that threads failed.
    #[test]
    fn settle_of_failures_after_an_outside_stop_names_the_threads() {
        let joined = mipyre_tpc::ThreadResults {
            ended: vec![()],
            failures: vec![failed("mipyre-tpc-0")],
            left_running: vec![],
        };
        match settle(joined, None) {
            Err(ServeError::FailedWhileStopping { threads }) => assert_eq!(threads, ["mipyre-tpc-0"]),
            other => panic!(
                "expected the threads named, got {:?}",
                other.err().map(|e| e.to_string())
            ),
        }
    }

    /// A drain that gives up is counted with the connections it closes; a drain that completes
    /// counts nothing.
    #[test]
    fn drain_counters_move_on_a_timeout_and_not_on_a_clean_drain() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let run = ServerCounters::new(false);
        // A grace of 20 ms drains for the 10 ms the teardown margin leaves of it.
        let deadline = || ShutdownDeadline::new(Duration::from_millis(20));
        tokio::task::LocalSet::new().block_on(&rt, async {
            let tracker = LocalTasks::new();
            tracker.spawn_local(std::future::pending::<()>());
            tracker.spawn_local(std::future::pending::<()>());
            drain_connections(tracker, &deadline(), &run).await;
        });
        let timed_out = run.read();
        assert_eq!((timed_out.drain_timeouts, timed_out.drain_closed_connections), (1, 2));
        let run = ServerCounters::new(false);
        tokio::task::LocalSet::new().block_on(&rt, async {
            let tracker = LocalTasks::new();
            tracker.spawn_local(async {});
            tracker.spawn_local(async {});
            drain_connections(tracker, &deadline(), &run).await;
        });
        let clean = run.read();
        assert_eq!((clean.drain_timeouts, clean.drain_closed_connections), (0, 0));
    }

    /// Returns the drain timeouts and the connections they closed when connections lasting
    /// `lasting` are drained for 5 s on a paused clock, where a timer fires at its exact instant
    /// once nothing else is ready, with no real wait.
    fn drained_for_5s(lasting: &[Duration]) -> (u64, u64) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        let run = ServerCounters::new(false);
        tokio::task::LocalSet::new().block_on(&rt, async {
            let tracker = LocalTasks::new();
            for &d in lasting {
                tracker.spawn_local(tokio::time::sleep(d));
            }
            drain_within(tracker, Duration::from_secs(5), &run).await;
        });
        let counts = run.read();
        (counts.drain_timeouts, counts.drain_closed_connections)
    }

    #[test]
    fn a_drain_waits_for_a_connection_that_ends_just_before_the_timeout() {
        assert_eq!(drained_for_5s(&[Duration::from_millis(4_999)]), (0, 0));
    }

    #[test]
    fn a_drain_gives_up_at_the_timeout_and_counts_the_connections_still_open() {
        let lasting = [
            Duration::from_millis(4_999),
            Duration::from_millis(5_001),
            Duration::MAX,
        ];
        assert_eq!(drained_for_5s(&lasting), (1, 2));
    }
}
