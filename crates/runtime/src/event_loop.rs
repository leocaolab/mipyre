//! An asyncio event loop that a TPC thread's tokio runtime does the waiting for: the loop holds
//! the ready callbacks, the timers, the fds its callers watch and the request Tasks it counts,
//! runs one turn when asked, and says after each turn whether its driver may wait for the bell
//! and the loop's poller fd.
//!
//! It does not know routes, budgets, status codes or limits: it knows callbacks, instants, fds, its
//! live request Tasks and, for each, whether its caller marked it as holding a large body. It
//! never blocks and does not know what drives it; it knows only the thread it belongs to. It
//! watches fds, and sets its timer, through its own poller, not tokio's reactor, and does not own
//! the fds.

mod fd_table;
mod future;
mod handle;
mod poller;
mod report;
mod task;
mod timers;

#[cfg(test)]
#[cfg(all(sync_model, sync_shuttle))]
mod shuttle_model;

use std::collections::VecDeque;
use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use mipyre_subinterp::context::ContextSuspension;
use mipyre_util::throttle::LogThrottle;
use pyo3::exceptions::{PyAttributeError, PyKeyError, PyOSError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::panic::PanicException;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyInt, PyTuple};

use crate::sync::{AtomicBool, Mutex, Ordering};

use self::fd_table::{FdTable, ReadyFds, Side, WatchRefusal};
use self::future::{DoneCallback, Future};
use self::handle::{call_in_context, Handle, TimerHandle};
pub use self::poller::PollerReadiness;
use self::poller::ScanEnd;
use self::report::{report, Subject};
use self::task::Task;
pub(crate) use self::task::TaskSink;
use self::timers::{change, sooner, TimerChange, Timers};

/// Holds back repeats of a callback's error, so a callback that fails every turn cannot fill the
/// log.
const CALLBACK_ERROR_LOG_PERIOD: Duration = Duration::from_secs(10);

/// Wakes the driver of one loop when a callback is scheduled outside a turn. A timer coming due
/// does not ring it: the kernel makes the loop's poller readable at the timer's deadline.
#[derive(Debug, Default)]
pub struct LoopBell(tokio::sync::Notify);

impl LoopBell {
    /// Waits until the bell rings; a ring while nobody waits is kept for the next wait.
    pub async fn rung(&self) {
        self.0.notified().await;
    }

    fn ring(&self) {
        self.0.notify_one();
    }
}

/// Says how the driver waits after a turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NextWait {
    /// Callbacks are ready: turn again once the thread's other tasks had their go.
    Now,
    /// Nothing is ready: wait for the bell.
    Parked,
}

/// Locates a live request Task in the loop's table; its generation tells a reused slot apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LiveKey {
    slot: usize,
    generation: u64,
}

/// Says who runs the loop's Python right now, which decides whether a schedule rings the bell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LoopPhase {
    Idle,
    /// The driver is turning the loop, and looks at the ready queue again when the turn ends.
    InTurn,
    /// A connection task is taking a request Task's first step, outside any turn.
    Starting,
}

/// Returns how the driver waits, from how many callbacks are ready after a turn, whether the turn
/// queued or cleared a callback of an fd its scan found, and whether the scan saw every ready fd.
/// Such an fd may still be ready, and the poller is level-triggered while the driver's reactor
/// wakes only on a new event, so the driver turns again until a scan that saw everything acts on
/// none.
fn next_wait(ready: usize, acted_on_fd: bool, scan: ScanEnd) -> NextWait {
    if ready == 0 && !acted_on_fd && scan == ScanEnd::Drained {
        NextWait::Parked
    } else {
        NextWait::Now
    }
}

/// Returns whether a schedule made in `phase` rings the driver: inside a turn the driver looks
/// again when the turn ends, and one ring since its last look is enough.
fn wakes(phase: LoopPhase, rung: bool) -> bool {
    phase != LoopPhase::InTurn && !rung
}

/// Says what a timer does when it comes due: run a `call_later` handle, or resolve one of the
/// loop's Futures with the second argument of the handle `asyncio.sleep` made, without running
/// Python.
enum Due {
    Call(Py<TimerHandle>),
    Resolve {
        handle: Py<TimerHandle>,
        future: Py<Future>,
        value: Py<PyAny>,
    },
}

/// Holds one callback ready to run.
enum Ready {
    Handle(Py<Handle>),
    /// It wakes one of the loop's Tasks because `from` ended, or to step again (`None`).
    Wake {
        task: Py<Task>,
        from: Option<Py<PyAny>>,
    },
    /// It is a done callback a Future queued, called with the Future.
    Call {
        done: DoneCallback,
        arg: Py<PyAny>,
    },
}

/// Counts a loop's live request Tasks, and how many of them hold a large body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiveTasks {
    pub count: usize,
    pub large: usize,
}

struct LiveTask {
    task: Py<Task>,
    large: bool,
    generation: u64,
}

struct LoopCore {
    phase: LoopPhase,
    rung: bool,
    closed: bool,
    ready: VecDeque<Ready>,
    timers: Timers,
    fds: FdTable,
    live: slab::Slab<LiveTask>,
    /// It counts the live Tasks marked large, so reading it takes no walk over `live`.
    large: usize,
    generation: u64,
}

/// Holds what the loop calls in asyncio and `inspect`, looked up once when the loop is made.
struct Asyncio {
    set_running_loop: Py<PyAny>,
    cancelled_error: Py<PyAny>,
    invalid_state_error: Py<PyAny>,
    enter_task: Py<PyAny>,
    leave_task: Py<PyAny>,
    register_task: Py<PyAny>,
    unregister_task: Py<PyAny>,
    set_result_unless_cancelled: Py<PyAny>,
    get_loop: Py<PyAny>,
    task_name_counter: Py<PyAny>,
    isgenerator: Py<PyAny>,
    copy_context: Py<PyAny>,
}

impl Asyncio {
    fn look_up(py: Python<'_>) -> PyResult<Self> {
        let asyncio = py.import(intern!(py, "asyncio"))?;
        let events = py.import(intern!(py, "asyncio.events"))?;
        let tasks = py.import(intern!(py, "asyncio.tasks"))?;
        let futures = py.import(intern!(py, "asyncio.futures"))?;
        Ok(Asyncio {
            set_running_loop: events.getattr(intern!(py, "_set_running_loop"))?.unbind(),
            cancelled_error: asyncio.getattr(intern!(py, "CancelledError"))?.unbind(),
            invalid_state_error: asyncio.getattr(intern!(py, "InvalidStateError"))?.unbind(),
            enter_task: tasks.getattr(intern!(py, "_enter_task"))?.unbind(),
            leave_task: tasks.getattr(intern!(py, "_leave_task"))?.unbind(),
            register_task: tasks.getattr(intern!(py, "_register_task"))?.unbind(),
            unregister_task: tasks.getattr(intern!(py, "_unregister_task"))?.unbind(),
            set_result_unless_cancelled: futures.getattr(intern!(py, "_set_result_unless_cancelled"))?.unbind(),
            get_loop: futures.getattr(intern!(py, "_get_loop"))?.unbind(),
            task_name_counter: tasks.getattr(intern!(py, "_task_name_counter"))?.unbind(),
            isgenerator: py
                .import(intern!(py, "inspect"))?
                .getattr(intern!(py, "isgenerator"))?
                .unbind(),
            copy_context: py
                .import(intern!(py, "contextvars"))?
                .getattr(intern!(py, "copy_context"))?
                .unbind(),
        })
    }
}

/// Runs asyncio's event loop for a thread whose tokio runtime does its waiting
/// (`mipyre.engine._EventLoop`).
#[pyclass(module = "mipyre.engine", name = "_EventLoop", frozen, subclass)]
pub(crate) struct EventLoop {
    core: Mutex<LoopCore>,
    origin: Instant,
    /// It is the thread the loop was made on, the only one that runs its callbacks.
    thread: ThreadId,
    bell: Arc<LoopBell>,
    asyncio: Asyncio,
    debug: AtomicBool,
    exception_handler: Mutex<Option<Py<PyAny>>>,
    task_factory: Mutex<Option<Py<PyAny>>>,
    callback_errors: LogThrottle,
}

#[pymethods]
impl EventLoop {
    /// Makes a loop that belongs to the calling thread, which must be in a tokio runtime.
    #[new]
    fn new(py: Python<'_>) -> PyResult<Self> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(PyRuntimeError::new_err(
                "this event loop runs only on a thread of the server's tokio runtime, and this thread has none",
            ));
        }
        let debug = py
            .import(intern!(py, "asyncio.coroutines"))?
            .call_method0(intern!(py, "_is_debug_mode"))?
            .is_truthy()?;
        let bell = Arc::new(LoopBell::default());
        Ok(EventLoop {
            core: Mutex::new(LoopCore {
                phase: LoopPhase::Idle,
                rung: false,
                closed: false,
                ready: VecDeque::new(),
                timers: Timers::default(),
                fds: FdTable::new()?,
                live: slab::Slab::new(),
                large: 0,
                generation: 0,
            }),
            origin: Instant::now(),
            thread: std::thread::current().id(),
            bell,
            asyncio: Asyncio::look_up(py)?,
            debug: AtomicBool::new(debug),
            exception_handler: Mutex::new(None),
            task_factory: Mutex::new(None),
            callback_errors: LogThrottle::new(CALLBACK_ERROR_LOG_PERIOD),
        })
    }

    fn time(&self) -> f64 {
        self.now()
    }

    #[pyo3(signature = (callback, *args, context = None))]
    fn call_soon(
        &self,
        py: Python<'_>,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<Py<Handle>> {
        self.check_thread()?;
        let context = self.given_or_current(py, context)?;
        let handle = Py::new(py, Handle::new(callback, args, context))?;
        self.schedule(Ready::Handle(handle.clone_ref(py)), Scheduler::LoopThread)?;
        Ok(handle)
    }

    /// Queues the callback for the next turn and always rings the driver: the caller may be on
    /// another thread, which the phase does not see.
    #[pyo3(signature = (callback, *args, context = None))]
    fn call_soon_threadsafe(
        &self,
        py: Python<'_>,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<Py<Handle>> {
        let context = self.given_or_current(py, context)?;
        let handle = Py::new(py, Handle::new(callback, args, context))?;
        self.schedule(Ready::Handle(handle.clone_ref(py)), Scheduler::AnyThread)?;
        Ok(handle)
    }

    #[pyo3(signature = (delay, callback, *args, context = None))]
    fn call_later(
        slf: &Bound<'_, Self>,
        delay: f64,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<Py<TimerHandle>> {
        let when = slf.get().now() + delay;
        Self::call_at(slf, when, callback, args, context)
    }

    #[pyo3(signature = (when, callback, *args, context = None))]
    fn call_at(
        slf: &Bound<'_, Self>,
        when: f64,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<Py<TimerHandle>> {
        let py = slf.py();
        let this = slf.get();
        this.check_thread()?;
        let resolves = this.resolved_future(slf, &callback, &args);
        let context = this.given_or_current(py, context)?;
        let handle = TimerHandle::new(py, Handle::new(callback, args, context), when, slf.clone().unbind())?;
        let due = match resolves {
            Some((future, value)) => Due::Resolve {
                handle: handle.clone_ref(py),
                future,
                value,
            },
            None => Due::Call(handle.clone_ref(py)),
        };
        let at = this.instant(when);
        let pushed = {
            let mut core = this.core.lock();
            if core.closed {
                return Err(closed());
            }
            let armed = if sooner(core.fds.timer(), at) {
                core.fds.arm_timer(Some(at))
            } else {
                Ok(())
            };
            armed.map(|()| {
                let key = core.timers.push(at, due);
                this.ring(&mut core);
                key
            })
        };
        let key = pushed.map_err(|failed| os_error(py, failed))?;
        *handle.get().key.lock() = Some(key);
        Ok(handle)
    }

    fn create_future(slf: &Bound<'_, Self>) -> PyResult<Py<Future>> {
        Py::new(slf.py(), Future::new(slf.clone().unbind()))
    }

    /// Wraps `coro` in a Task scheduled to take its first step on the next turn, in a copy of the
    /// current context unless `context` is given; a task factory, when set, makes it instead.
    #[pyo3(signature = (coro, *, name = None, context = None))]
    fn create_task<'py>(
        slf: &Bound<'py, Self>,
        coro: Bound<'py, PyAny>,
        name: Option<Bound<'py, PyAny>>,
        context: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let this = slf.get();
        this.check_thread()?;
        if this.is_closed() {
            return Err(closed());
        }
        let factory = this.task_factory.lock().as_ref().map(|f| f.clone_ref(py));
        if let Some(factory) = factory {
            let kwargs = PyDict::new(py);
            if let Some(name) = name {
                kwargs.set_item(intern!(py, "name"), name)?;
            }
            if let Some(context) = context {
                kwargs.set_item(intern!(py, "context"), context)?;
            }
            return factory.bind(py).call((slf, coro), Some(&kwargs));
        }
        let context = match context {
            Some(context) => ContextSuspension::new(context),
            None => ContextSuspension::new(this.copied_context(py)?),
        };
        let task = Task::new(slf, coro, context, None, name)?;
        this.queue_wake(py, task.clone().unbind(), None);
        Ok(task.into_any())
    }

    /// Runs `callback(*args)` on every turn that finds `fd` readable, in place of its reader, and
    /// returns the handle that cancels it (asyncio's selector loop's hook, which its transports
    /// call).
    #[pyo3(signature = (fd, callback, *args))]
    fn _add_reader(
        &self,
        py: Python<'_>,
        fd: Bound<'_, PyAny>,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
    ) -> PyResult<Py<Handle>> {
        self.add_fd_callback(py, &fd, Side::Reader, callback, args)
    }

    /// Stops watching `fd` for reading, and returns whether it had a reader.
    fn _remove_reader(&self, py: Python<'_>, fd: Bound<'_, PyAny>) -> PyResult<bool> {
        self.remove_fd_callback(py, &fd, Side::Reader)
    }

    /// Runs `callback(*args)` on every turn that finds `fd` writable, in place of its writer, and
    /// returns the handle that cancels it.
    #[pyo3(signature = (fd, callback, *args))]
    fn _add_writer(
        &self,
        py: Python<'_>,
        fd: Bound<'_, PyAny>,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
    ) -> PyResult<Py<Handle>> {
        self.add_fd_callback(py, &fd, Side::Writer, callback, args)
    }

    /// Stops watching `fd` for writing, and returns whether it had a writer.
    fn _remove_writer(&self, py: Python<'_>, fd: Bound<'_, PyAny>) -> PyResult<bool> {
        self.remove_fd_callback(py, &fd, Side::Writer)
    }

    /// Raises that the loop is closed, when it is, in the words of asyncio's own check, which its
    /// I/O code calls.
    fn _check_closed(&self) -> PyResult<()> {
        if self.core.lock().closed {
            return Err(closed());
        }
        Ok(())
    }

    /// Says whether the loop is in debug mode, for asyncio's I/O code, which reads `_debug`.
    #[getter(_debug)]
    fn debug_mode(&self) -> bool {
        self.get_debug()
    }

    /// Returns a read-only view of the fds the loop watches: asyncio's transports read it to say
    /// in their repr whether they are reading or writing.
    #[getter(_selector)]
    fn selector(slf: &Bound<'_, Self>) -> PyResult<Py<SelectorView>> {
        Py::new(
            slf.py(),
            SelectorView {
                owner: slf.clone().unbind(),
            },
        )
    }

    fn get_debug(&self) -> bool {
        self.debug.load(Ordering::Relaxed)
    }

    fn set_debug(&self, enabled: bool) {
        self.debug.store(enabled, Ordering::Relaxed);
    }

    fn is_running(&self) -> bool {
        self.core.lock().phase != LoopPhase::Idle
    }

    fn is_closed(&self) -> bool {
        self.core.lock().closed
    }

    fn set_exception_handler(&self, handler: Option<Py<PyAny>>) {
        let replaced = std::mem::replace(&mut *self.exception_handler.lock(), handler);
        drop(replaced);
    }

    fn get_exception_handler(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.exception_handler.lock().as_ref().map(|h| h.clone_ref(py))
    }

    fn set_task_factory(&self, factory: Option<Py<PyAny>>) {
        let replaced = std::mem::replace(&mut *self.task_factory.lock(), factory);
        drop(replaced);
    }

    fn get_task_factory(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.task_factory.lock().as_ref().map(|f| f.clone_ref(py))
    }

    fn call_exception_handler(slf: &Bound<'_, Self>, context: Bound<'_, PyDict>) {
        report::call_handler(slf, context);
    }

    fn default_exception_handler(&self, context: Bound<'_, PyDict>) {
        report::log_context(&self.callback_errors, self.elapsed(), &context);
    }

    fn run_forever(&self) -> PyResult<()> {
        Err(owned_by_server("run_forever"))
    }

    #[pyo3(signature = (_future))]
    fn run_until_complete(&self, _future: Bound<'_, PyAny>) -> PyResult<()> {
        Err(owned_by_server("run_until_complete"))
    }

    fn stop(&self) -> PyResult<()> {
        Err(owned_by_server("stop"))
    }

    fn close(&self) -> PyResult<()> {
        Err(owned_by_server("close"))
    }
}

impl EventLoop {
    pub(crate) fn bell(&self) -> Arc<LoopBell> {
        Arc::clone(&self.bell)
    }

    /// Registers a duplicate of the loop's poller fd with this thread's tokio reactor, for its driver
    /// to wait on; see [`poller::Poller::register_with_reactor`].
    pub(crate) fn register_poller(&self) -> io::Result<PollerReadiness> {
        self.core.lock().fds.register_poller()
    }

    /// Returns `context`, or a copy of the current context when none is given, as asyncio's `Handle`
    /// takes: the callback then sees the context it was scheduled in, and what it sets stays in
    /// that copy. The thread's own context is shared by every request on the thread.
    fn given_or_current(&self, py: Python<'_>, context: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        match context {
            Some(context) => Ok(context),
            None => Ok(self.copied_context(py)?.unbind()),
        }
    }

    /// Returns a copy of the current context, through the `contextvars.copy_context` looked up when
    /// the loop was made.
    pub(super) fn copied_context<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.asyncio.copy_context.bind(py).call0()
    }

    fn add_fd_callback(
        &self,
        py: Python<'_>,
        fd: &Bound<'_, PyAny>,
        side: Side,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
    ) -> PyResult<Py<Handle>> {
        self.check_thread()?;
        let fd = fd_of(fd)?;
        let context = self.given_or_current(py, None)?;
        let handle = Py::new(py, Handle::new(callback, args, context))?;
        match self.watch(fd, side, Some(handle.clone_ref(py))) {
            Ok(Some(replaced)) => replaced.get().cancel(),
            Ok(None) => {}
            Err(WatchFdError::Closed) => return Err(closed()),
            Err(WatchFdError::Refused(refusal)) => return Err(os_error(py, refusal.error)),
        }
        Ok(handle)
    }

    fn remove_fd_callback(&self, py: Python<'_>, fd: &Bound<'_, PyAny>, side: Side) -> PyResult<bool> {
        self.check_thread()?;
        let fd = fd_of(fd)?;
        match self.watch(fd, side, None) {
            Ok(Some(removed)) => {
                removed.get().cancel();
                removed.drop_ref(py);
                Ok(true)
            }
            Ok(None) | Err(WatchFdError::Closed) => Ok(false),
            Err(WatchFdError::Refused(refusal)) => Err(os_error(py, refusal.error)),
        }
    }

    /// Puts `callback` in `side`'s slot of `fd` and returns the callback it replaced; what it hands
    /// back is let go of after the loop's lock.
    ///
    /// # Errors
    ///
    /// Returns that the loop is closed, or the poller's refusal, a [`WatchRefusal`].
    fn watch(&self, fd: RawFd, side: Side, callback: Option<Py<Handle>>) -> Result<Option<Py<Handle>>, WatchFdError> {
        let mut core = self.core.lock();
        if core.closed {
            return Err(WatchFdError::Closed);
        }
        core.fds.set(fd, side, callback).map_err(WatchFdError::Refused)
    }

    /// Returns how long the loop has been, on the monotonic clock its timers are set by.
    fn elapsed(&self) -> Duration {
        Instant::now().duration_since(self.origin)
    }

    fn now(&self) -> f64 {
        self.elapsed().as_secs_f64()
    }

    /// Returns the instant of the loop time `when`; a time before the loop began is its start, and
    /// one too far ahead is [`timers::FAR_FUTURE`] after it.
    fn instant(&self, when: f64) -> Instant {
        let since_origin =
            Duration::try_from_secs_f64(when.max(0.0)).map_or(timers::FAR_FUTURE, |d| d.min(timers::FAR_FUTURE));
        self.origin + since_origin
    }

    fn on_its_thread(&self) -> bool {
        std::thread::current().id() == self.thread
    }

    /// Refuses a call from a thread other than the loop's. asyncio checks this only in debug mode;
    /// this loop always does, because its Futures are resolved only on its thread.
    fn check_thread(&self) -> PyResult<()> {
        if self.on_its_thread() {
            return Ok(());
        }
        Err(PyRuntimeError::new_err(
            "Non-thread-safe operation invoked on an event loop other than the current one",
        ))
    }

    /// Returns the Future a timer resolves, and with what, when the timer is the one
    /// `asyncio.sleep` sets: `_set_result_unless_cancelled(future, result)` with one of this
    /// loop's Futures.
    fn resolved_future(
        &self,
        slf: &Bound<'_, Self>,
        callback: &Py<PyAny>,
        args: &Bound<'_, PyTuple>,
    ) -> Option<(Py<Future>, Py<PyAny>)> {
        if !callback.is(&self.asyncio.set_result_unless_cancelled) {
            return None;
        }
        let [future, value] = args.as_slice() else { return None };
        let future = future.cast::<Future>().ok()?;
        future
            .get()
            .core()
            .owner()
            .is(slf)
            .then(|| (future.clone().unbind(), value.clone().unbind()))
    }

    fn ring(&self, core: &mut LoopCore) {
        if wakes(core.phase, core.rung) {
            core.rung = true;
            self.bell.ring();
        }
    }

    /// Queues `ready` for a later turn and rings the driver as `scheduler` needs; a closed loop
    /// refuses it with that error, and lets go of `ready` after its lock.
    fn schedule(&self, ready: Ready, scheduler: Scheduler) -> PyResult<()> {
        self.enqueue(ready, scheduler).map_err(|refused| {
            drop(refused);
            closed()
        })
    }

    /// Queues `ready` for a later turn, or hands it back when the loop is closed.
    fn enqueue(&self, ready: Ready, scheduler: Scheduler) -> Result<(), Ready> {
        let ring = {
            let mut core = self.core.lock();
            if core.closed {
                return Err(ready);
            }
            core.ready.push_back(ready);
            let ring = scheduler == Scheduler::AnyThread || wakes(core.phase, core.rung);
            core.rung |= ring;
            ring
        };
        if ring {
            self.bell.ring();
        }
        Ok(())
    }

    /// Queues a step of `task` for a later turn; a closed loop drops it.
    pub(super) fn queue_wake(&self, py: Python<'_>, task: Py<Task>, from: Option<Py<PyAny>>) {
        let ready = Ready::Wake { task, from };
        if let Err(dropped) = self.enqueue(ready, Scheduler::LoopThread) {
            drop_ready(py, dropped);
        }
    }

    /// Queues the done callback `done` to run with `arg` on a later turn; a closed loop drops it.
    fn queue_call(&self, py: Python<'_>, done: DoneCallback, arg: Py<PyAny>) {
        let ready = Ready::Call { done, arg };
        if let Err(dropped) = self.enqueue(ready, Scheduler::LoopThread) {
            drop_ready(py, dropped);
        }
    }

    /// Takes a request Task that ended out of the loop's count.
    pub(super) fn forget_live(&self, py: Python<'_>, key: LiveKey) {
        let ended = {
            let mut core = self.core.lock();
            match core.live.get(key.slot) {
                Some(live) if live.generation == key.generation => {
                    let ended = core.live.remove(key.slot);
                    core.large -= usize::from(ended.large);
                    Some(ended)
                }
                _ => None,
            }
        };
        if let Some(ended) = ended {
            ended.task.drop_ref(py);
        }
    }

    pub(super) fn cancelled_error_type<'py>(&self, py: Python<'py>) -> &Bound<'py, PyAny> {
        self.asyncio.cancelled_error.bind(py)
    }

    /// Returns asyncio's `CancelledError`, carrying `message` when there is one.
    pub(super) fn cancelled_error(&self, py: Python<'_>, message: Option<&Py<PyAny>>) -> PyErr {
        let error = self.asyncio.cancelled_error.bind(py);
        let made = match message {
            Some(m) => error.call1((m.clone_ref(py),)),
            None => error.call0(),
        };
        match made {
            Ok(raised) => PyErr::from_value(raised),
            Err(e) => e,
        }
    }

    pub(super) fn invalid_state(&self, py: Python<'_>, text: String) -> PyErr {
        match self.asyncio.invalid_state_error.bind(py).call1((text,)) {
            Ok(raised) => PyErr::from_value(raised),
            Err(e) => e,
        }
    }

    pub(super) fn enter_task(slf: &Bound<'_, Self>, task: &Bound<'_, Task>) -> PyResult<()> {
        slf.get().asyncio.enter_task.bind(slf.py()).call1((slf, task)).map(drop)
    }

    pub(super) fn leave_task(slf: &Bound<'_, Self>, task: &Bound<'_, Task>) -> PyResult<()> {
        slf.get().asyncio.leave_task.bind(slf.py()).call1((slf, task)).map(drop)
    }

    pub(super) fn register_task(&self, task: &Bound<'_, Task>) -> PyResult<()> {
        self.asyncio.register_task.bind(task.py()).call1((task,)).map(drop)
    }

    pub(super) fn unregister_task(&self, task: &Bound<'_, Task>) -> PyResult<()> {
        self.asyncio.unregister_task.bind(task.py()).call1((task,)).map(drop)
    }

    /// Returns the loop a future that is not this loop's own belongs to, as asyncio's Task asks it.
    pub(super) fn get_loop_of<'py>(&self, future: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        self.asyncio.get_loop.bind(future.py()).call1((future,))
    }

    pub(super) fn is_generator(&self, obj: &Bound<'_, PyAny>) -> PyResult<bool> {
        self.asyncio.isgenerator.bind(obj.py()).call1((obj,))?.is_truthy()
    }

    /// Returns the next number asyncio gives a Task without a name.
    pub(super) fn next_task_number<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.asyncio.task_name_counter.bind(py).call0()
    }

    /// Takes the timer `key` names out of the loop's timers, and lets go of it after the lock:
    /// dropping it may run Python that calls the loop again.
    fn remove_timer(&self, key: &timers::Key) {
        let removed = self.core.lock().timers.remove(key);
        drop(removed);
    }

    /// Marks the loop running for the Python that runs in a turn or a start, or not, through
    /// `asyncio.events._set_running_loop`.
    fn set_running(slf: &Bound<'_, Self>, running: bool) -> PyResult<()> {
        let py = slf.py();
        let set = slf.get().asyncio.set_running_loop.bind(py);
        if running {
            set.call1((slf,)).map(drop)
        } else {
            set.call1((py.None(),)).map(drop)
        }
    }

    fn set_phase(&self, phase: LoopPhase) {
        self.core.lock().phase = phase;
    }

    /// Runs one turn of the loop and returns how its driver waits next. A callback that raises goes
    /// to the exception handler and the turn goes on; one queued during the turn runs on the next,
    /// as in asyncio. A change the poller refuses while the turn takes a cancelled callback out
    /// goes to the exception handler too.
    ///
    /// # Errors
    ///
    /// Fails when the poller's scan fails or it refuses to set the loop's timer, after the
    /// callbacks the turn found have run. Fails when asyncio cannot mark the loop running, and then
    /// runs no callback, which waits for a later turn; or when it cannot mark the loop not running,
    /// after the callbacks ran.
    pub(crate) fn turn(slf: &Bound<'_, Self>) -> Result<NextWait, TurnFailure> {
        let py = slf.py();
        let this = slf.get();
        let (scanned, rearmed, due) = {
            let mut core = this.core.lock();
            if core.closed {
                return Ok(NextWait::Parked);
            }
            core.phase = LoopPhase::InTurn;
            core.rung = false;
            let LoopCore { fds, ready, timers, .. } = &mut *core;
            let scanned = fds.take_ready(py, ready);
            let fired = scanned.scan.as_ref().is_ok_and(|s| s.timer_fired);
            let now = Instant::now();
            let due = timers.take_due(now);
            let rearmed = match change(fds.timer(), timers.earliest(), now, fired) {
                TimerChange::Keep => Ok(()),
                TimerChange::SetFor(at) => fds.arm_timer(Some(at)),
                TimerChange::Disarm => fds.arm_timer(None),
            };
            (scanned, rearmed, due)
        };
        let ReadyFds {
            acted,
            scan,
            cleared,
            refused,
        } = scanned;
        drop(cleared);
        let scanned = scan.and_then(|scanned| rearmed.map(|()| (acted, scanned.end)));
        let mut calls = Vec::new();
        let mut resolved = Vec::new();
        for timer in due {
            match timer {
                Due::Call(handle) => {
                    let handle = handle.into_bound(py);
                    handle.get().came_due();
                    if !handle.as_super().get().is_cancelled() {
                        calls.push(Ready::Handle(handle.into_super().unbind()));
                    }
                }
                Due::Resolve { handle, future, value } => {
                    let handle = handle.bind(py);
                    handle.get().came_due();
                    if !handle.as_super().get().is_cancelled() {
                        resolved.push((future, value));
                    }
                }
            }
        }
        // A resolved Future wakes its Task for the next turn, as asyncio's timer callback queues
        // the Task's wakeup behind the callbacks already ready.
        let count = {
            let mut core = this.core.lock();
            core.ready.extend(calls);
            core.ready.len()
        };
        for (future, value) in resolved {
            Future::resolve(future.bind(py), value);
        }

        let running = Self::set_running(slf, true);
        for refusal in refused {
            report_refused(slf, refusal);
        }
        if running.is_ok() {
            for _ in 0..count {
                let Some(ready) = this.core.lock().ready.pop_front() else {
                    break;
                };
                run_ready(slf, ready);
            }
        }
        let unset = Self::set_running(slf, false);
        let ready = {
            let mut core = this.core.lock();
            core.phase = LoopPhase::Idle;
            core.ready.len()
        };
        let (acted, scan) = turn_ended(py, scanned, running, unset)?;
        Ok(next_wait(ready, acted, scan))
    }

    /// Starts a request Task that steps `coro` in `context`, its first step taken now, its end
    /// handed to `sink`. One that suspends counts as live, marked `large` or not, until it ends;
    /// one that ends in its first step never does.
    ///
    /// # Errors
    ///
    /// Fails when `coro` cannot be awaited, the Task cannot be made, or the loop cannot be marked
    /// running for the step.
    pub(crate) fn start_task<'py>(
        slf: &Bound<'py, Self>,
        coro: Bound<'py, PyAny>,
        context: ContextSuspension<'py>,
        sink: Option<TaskSink>,
        large: bool,
    ) -> PyResult<()> {
        let this = slf.get();
        let task = Task::new(slf, coro, context, sink, None)?;
        this.set_phase(LoopPhase::Starting);
        let running = Self::set_running(slf, true);
        if running.is_ok() {
            Task::wake(&task, None);
        }
        let unset = Self::set_running(slf, false);
        this.set_phase(LoopPhase::Idle);
        chained(slf.py(), running, unset)?;
        if !task.get().is_pending() {
            return Ok(());
        }
        let key = {
            let mut core = this.core.lock();
            core.generation += 1;
            let generation = core.generation;
            let slot = core.live.insert(LiveTask {
                task: task.clone().unbind(),
                large,
                generation,
            });
            core.large += usize::from(large);
            LiveKey { slot, generation }
        };
        task.get().set_live(key);
        Ok(())
    }

    /// Schedules a Task of `coro` for the next turn, in a copy of the current context, its end
    /// handed to `sink`. It is not a request: it does not count as live, and cancelling the live
    /// Tasks leaves it alone.
    ///
    /// # Errors
    ///
    /// Fails when `coro` cannot be awaited or the Task cannot be made.
    pub(crate) fn schedule_task<'py>(slf: &Bound<'py, Self>, coro: Bound<'py, PyAny>, sink: TaskSink) -> PyResult<()> {
        let py = slf.py();
        let context = ContextSuspension::new(slf.get().copied_context(py)?);
        let task = Task::new(slf, coro, context, Some(sink), None)?;
        slf.get().queue_wake(py, task.unbind(), None);
        Ok(())
    }

    /// Cancels every live request Task, as the loop's life ends at the stop or the worker's
    /// retirement, and returns how many were live.
    ///
    /// # Errors
    ///
    /// Returns the first error a cancel raised, after trying them all; each later one goes to the
    /// exception handler.
    pub(crate) fn cancel_all(&self, py: Python<'_>) -> PyResult<usize> {
        let tasks: Vec<Py<Task>> = self
            .core
            .lock()
            .live
            .iter()
            .map(|(_, l)| l.task.clone_ref(py))
            .collect();
        let live = tasks.len();
        let mut first_error = None;
        for task in tasks {
            let task = task.bind(py);
            let Err(raised) = Task::cancel_now(task, None) else {
                continue;
            };
            if first_error.is_none() {
                first_error = Some(raised);
            } else {
                Task::report(task, "a live task could not be cancelled as the loop ends", &raised);
            }
        }
        first_error.map_or(Ok(live), Err)
    }

    pub(crate) fn live(&self) -> LiveTasks {
        let core = self.core.lock();
        LiveTasks {
            count: core.live.len(),
            large: core.large,
        }
    }

    /// Closes the loop and lets go of everything it holds, in its interpreter: a callback a
    /// Future queues afterwards is dropped, and scheduling from Python raises that the loop is
    /// closed.
    pub(crate) fn release(&self, py: Python<'_>) {
        let (ready, timers, fds, live) = {
            let mut core = self.core.lock();
            core.closed = true;
            core.large = 0;
            (
                std::mem::take(&mut core.ready),
                std::mem::take(&mut core.timers),
                core.fds.take_all(),
                std::mem::take(&mut core.live),
            )
        };
        for ready in ready {
            drop_ready(py, ready);
        }
        drop(timers);
        for handle in fds {
            handle.drop_ref(py);
        }
        for (_, live) in live {
            live.task.drop_ref(py);
        }
        let handler = self.exception_handler.lock().take();
        let factory = self.task_factory.lock().take();
        for held in [handler, factory].into_iter().flatten() {
            held.drop_ref(py);
        }
    }
}

/// Runs one ready callback inside a turn; what it raises goes to the exception handler.
fn run_ready(slf: &Bound<'_, EventLoop>, ready: Ready) {
    let py = slf.py();
    match ready {
        Ready::Handle(handle) => {
            let handle = handle.into_bound(py);
            if handle.get().is_cancelled() {
                return;
            }
            if let Err(raised) = handle.get().run(py) {
                report(slf, "Exception in callback", &raised, Subject::Handle(handle.as_any()));
            }
        }
        Ready::Wake { task, from } => {
            let from = from.map(|f| f.into_bound(py));
            Task::wake(task.bind(py), from.as_ref());
        }
        Ready::Call { done, arg } => {
            let callback = done.callback.bind(py);
            if let Err(raised) = call_in_context(callback, (arg,), done.context.bind(py)) {
                report(slf, "Exception in callback", &raised, Subject::Handle(callback));
            }
        }
    }
}

/// Lets go of a callback that will not run, with its interpreter attached.
fn drop_ready(py: Python<'_>, ready: Ready) {
    match ready {
        Ready::Handle(handle) => handle.drop_ref(py),
        Ready::Wake { task, from } => {
            task.drop_ref(py);
            if let Some(from) = from {
                from.drop_ref(py);
            }
        }
        Ready::Call { done, arg } => {
            done.callback.drop_ref(py);
            done.context.drop_ref(py);
            arg.drop_ref(py);
        }
    }
}

/// Says why a turn failed.
#[derive(Debug)]
pub(crate) enum TurnFailure {
    /// asyncio could not mark the loop running, or not running; a scan that failed as well is the
    /// first cause in its chain.
    Raised(PyErr),
    /// The poller's scan failed, so the turn ran without the fds that were ready, or the poller
    /// refused to set the loop's timer, which then wakes no later turn.
    Poller(io::Error),
}

/// Returns what a turn's scan found, or why the turn failed, from the scan and the two markings
/// of the loop running; when several failed, each later error has the earlier as its cause.
fn turn_ended(
    py: Python<'_>,
    scanned: io::Result<(bool, ScanEnd)>,
    running: PyResult<()>,
    unset: PyResult<()>,
) -> Result<(bool, ScanEnd), TurnFailure> {
    match (scanned, running, unset) {
        (Ok(scanned), running, unset) => chained(py, running, unset)
            .map(|()| scanned)
            .map_err(TurnFailure::Raised),
        (Err(failed), Ok(()), Ok(())) => Err(TurnFailure::Poller(failed)),
        (Err(failed), running, unset) => Err(TurnFailure::Raised(after(
            py,
            after(py, os_error(py, failed), running),
            unset,
        ))),
    }
}

/// Hands the exception handler a change the poller refused while a turn took a cancelled callback
/// out; [`WatchRefusal`] says what the refusal did to the fd.
fn report_refused(slf: &Bound<'_, EventLoop>, refusal: WatchRefusal) {
    let fd = refusal.fd;
    let message =
        format!("the poller refused to stop watching fd {fd} for a cancelled callback, so the loop dropped the fd");
    report(slf, &message, &os_error(slf.py(), refusal.error), Subject::Fd(fd));
}

/// Says why the loop did not change an fd's watch.
enum WatchFdError {
    Closed,
    Refused(WatchRefusal),
}

/// Says who schedules a callback, which decides whether the driver is rung.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scheduler {
    /// The loop's own thread: it rings only as [`wakes`] says.
    LoopThread,
    /// Possibly another thread, whose schedule the loop's phase does not see: it always rings.
    AnyThread,
}

/// Calls `call`, which runs Python, and returns a Rust panic in it as PyO3's `PanicException`.
/// PyO3 resumes a panic when it fetches a `PanicException` that Python code raised, so without
/// this a panic in a handler would unwind out of the loop past the step's bookkeeping: the Task
/// left as the running task, its context entered and its request never answered through it.
fn caught<T>(call: impl FnOnce() -> PyResult<T>) -> PyResult<T> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(call))
        .unwrap_or_else(|payload| Err(PanicException::new_err(mipyre_util::panic_message(&*payload))))
}

/// Returns `first`'s result when only it or nothing failed, and when both failed returns `then`'s
/// error with `first`'s as its cause, so neither is lost.
fn chained(py: Python<'_>, first: PyResult<()>, then: PyResult<()>) -> PyResult<()> {
    match first {
        Ok(()) => then,
        Err(first) => Err(after(py, first, then)),
    }
}

/// Returns `then`'s error with `first` as its cause when `then` failed too, and `first` when it did
/// not.
fn after(py: Python<'_>, first: PyErr, then: PyResult<()>) -> PyErr {
    match then {
        Ok(()) => first,
        Err(then) => {
            then.set_cause(py, Some(first));
            then
        }
    }
}

/// Returns `error` as the `OSError` CPython raises for it: one the kernel gave carries its `errno`
/// and the subclass that goes with it, as `selectors` raises, which pyo3's conversion leaves out.
fn os_error(py: Python<'_>, error: io::Error) -> PyErr {
    let Some(errno) = error.raw_os_error() else {
        return error.into();
    };
    match py
        .import(intern!(py, "os"))
        .and_then(|os| os.call_method1(intern!(py, "strerror"), (errno,)))
    {
        Ok(strerror) => PyOSError::new_err((errno, strerror.unbind())),
        Err(failed) => after(py, error.into(), Err(failed)),
    }
}

fn closed() -> PyErr {
    PyRuntimeError::new_err("Event loop is closed")
}

/// Is `selectors.EVENT_READ`, the bit of a `SelectorKey`'s `events` for reading.
const EVENT_READ: u8 = 1;

/// Is `selectors.EVENT_WRITE`, the bit of a `SelectorKey`'s `events` for writing.
const EVENT_WRITE: u8 = 2;

/// Shows the fds a loop watches as asyncio's selector does to a transport's repr, and nothing
/// else: it cannot change them.
#[pyclass(module = "mipyre.engine", name = "_SelectorView", frozen)]
pub(crate) struct SelectorView {
    owner: Py<EventLoop>,
}

#[pymethods]
impl SelectorView {
    /// Returns the `selectors.SelectorKey` of `fileobj`, whose `events` say what it is watched for.
    ///
    /// # Errors
    ///
    /// `KeyError` for an fd the loop does not watch, as a selector raises.
    fn get_key<'py>(&self, fileobj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let py = fileobj.py();
        let fd = fd_of(fileobj)?;
        let watched = self.owner.get().core.lock().fds.interest(fd);
        let Some(watched) = watched else {
            return Err(PyKeyError::new_err(format!("{fileobj:?} is not registered")));
        };
        let events = (if watched.reads() { EVENT_READ } else { 0 }) | (if watched.writes() { EVENT_WRITE } else { 0 });
        py.import(intern!(py, "selectors"))?
            .getattr(intern!(py, "SelectorKey"))?
            .call1((fileobj, fd, events, py.None()))
    }
}

/// Returns the fd number an fd argument names: an `int`, or what the object's `fileno()` returns,
/// as Python's `selectors` reads it.
fn fd_of(fd: &Bound<'_, PyAny>) -> PyResult<RawFd> {
    let number = if fd.is_instance_of::<PyInt>() {
        fd.extract::<RawFd>()?
    } else {
        fd.call_method0(intern!(fd.py(), "fileno"))
            .and_then(|n| n.extract::<RawFd>())
            .map_err(|raised| invalid_file_object(fd, raised))?
    };
    if number < 0 {
        return Err(PyValueError::new_err(format!("Invalid file descriptor: {number}")));
    }
    Ok(number)
}

/// Turns what reading an object's fd raised into `ValueError` when it says the object has no
/// usable `fileno()`, as `selectors` does for `AttributeError`, `TypeError` and `ValueError`; any
/// other error passes through.
fn invalid_file_object(fd: &Bound<'_, PyAny>, raised: PyErr) -> PyErr {
    let py = fd.py();
    let unusable = raised.is_instance_of::<PyAttributeError>(py)
        || raised.is_instance_of::<PyTypeError>(py)
        || raised.is_instance_of::<PyValueError>(py);
    if unusable {
        PyValueError::new_err(format!("Invalid file object: {fd:?}"))
    } else {
        raised
    }
}

fn owned_by_server(method: &str) -> PyErr {
    PyRuntimeError::new_err(format!(
        "{method}() is not available: this event loop belongs to the server's TPC thread, which runs and stops it"
    ))
}

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests;
