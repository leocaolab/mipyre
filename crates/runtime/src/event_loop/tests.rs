use std::ffi::CStr;
use std::time::Duration;

use mipyre_subinterp::context::PyContext;
use pyo3::types::iterator::PySendResult;
use pyo3::types::{PyDict, PyTuple};

use super::*;

#[test]
fn a_turn_with_ready_callbacks_does_not_wait() {
    assert_eq!(next_wait(1, false, ScanEnd::Drained), NextWait::Now);
    assert_eq!(next_wait(0, false, ScanEnd::Drained), NextWait::Parked);
}

#[test]
fn a_turn_that_queued_or_cleared_a_callback_of_a_found_fd_does_not_wait() {
    assert_eq!(next_wait(0, true, ScanEnd::Drained), NextWait::Now);
}

#[test]
fn a_turn_whose_scan_may_have_left_fds_ready_does_not_wait() {
    assert_eq!(next_wait(0, false, ScanEnd::Partial), NextWait::Now);
}

#[test]
fn only_a_schedule_outside_a_turn_rings_and_only_once() {
    assert!(wakes(LoopPhase::Idle, false));
    assert!(wakes(LoopPhase::Starting, false));
    assert!(!wakes(LoopPhase::InTurn, false));
    assert!(!wakes(LoopPhase::Idle, true));
}

/// Runs the tests on the main interpreter one at a time: two threads importing asyncio there at
/// once, each giving up the GIL while it waits for its loop's bell, can trip the import system's
/// deadlock detection.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    Python::initialize();
    SERIAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .expect("a current-thread runtime")
}

fn new_loop(py: Python<'_>) -> Bound<'_, EventLoop> {
    py.get_type::<EventLoop>()
        .call0()
        .and_then(|made| Ok(made.cast_into::<EventLoop>()?))
        .expect("a loop on a thread of a tokio runtime")
}

/// Turns `event_loop` until `done` says so, waiting for its bell or its poller when it parks.
fn drive(rt: &tokio::runtime::Runtime, event_loop: &Bound<'_, EventLoop>, done: impl Fn() -> bool) {
    let bell = event_loop.get().bell();
    let poller = event_loop
        .get()
        .register_poller()
        .expect("the poller's fd on the reactor");
    for _ in 0..100_000 {
        if done() {
            return;
        }
        if EventLoop::turn(event_loop).expect("a turn") == NextWait::Parked && !done() {
            let woken = event_loop.py().detach(|| {
                rt.block_on(tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::select! {
                        () = bell.rung() => {}
                        waited = poller.wait() => waited.expect("the poller waits"),
                    }
                }))
            });
            woken.expect("the bell rings or the poller turns readable within five seconds");
        }
    }
    panic!("the loop did not get there in 100000 turns");
}

/// Runs `async def main()` from `source` as a Task of a new loop on a current-thread runtime and
/// returns what `check` makes of its result.
fn run_main<T>(source: &CStr, check: impl for<'py> FnOnce(PyResult<Bound<'py, PyAny>>) -> T) -> T {
    let _serial = serial();
    let rt = runtime();
    let _entered = rt.enter();
    mipyre_subinterp::attach::with_main(|py| {
        let names = PyDict::new(py);
        py.run(source, Some(&names), None).expect("the source runs");
        let event_loop = new_loop(py);
        let main = names
            .get_item("main")
            .expect("names answers")
            .expect("the source defines main")
            .call0()
            .expect("main() makes a coroutine");
        let task = event_loop
            .call_method1("create_task", (main,))
            .expect("create_task takes a coroutine");
        drive(&rt, &event_loop, || {
            task.call_method0("done")
                .and_then(|d| d.is_truthy())
                .expect("done() answers")
        });
        let checked = check(task.call_method0("result"));
        event_loop.get().release(py);
        checked
    })
}

fn result_of<T: for<'a, 'py> FromPyObject<'a, 'py>>(source: &CStr) -> T {
    run_main(source, |result| {
        let result = result.unwrap_or_else(|raised| panic!("main raised {raised}"));
        result
            .extract()
            .unwrap_or_else(|_| panic!("main returned {result}, not the type asked"))
    })
}

#[test]
fn asyncio_sleep_resolves_on_the_loops_timer() {
    let slept: f64 = result_of(
        c"
import asyncio
async def main():
    loop = asyncio.get_running_loop()
    start = loop.time()
    await asyncio.sleep(0.02)
    await asyncio.sleep(0)
    return loop.time() - start
",
    );
    assert!(slept >= 0.02, "slept {slept}s");
}

#[test]
fn gather_runs_child_tasks_and_collects_their_results() {
    let gathered: Vec<i64> = result_of(
        c"
import asyncio
async def child(n):
    await asyncio.sleep(0.01 * (3 - n))
    return n * 10
async def main():
    return list(await asyncio.gather(child(0), child(1), child(2)))
",
    );
    assert_eq!(gathered, vec![0, 10, 20]);
}

#[test]
fn a_timeout_that_expires_raises_timeout_error_and_one_that_does_not_returns() {
    let ended: (String, i64) = result_of(
        c"
import asyncio
async def main():
    try:
        async with asyncio.timeout(0.01):
            await asyncio.sleep(10)
        expired = 'returned'
    except TimeoutError:
        expired = 'TimeoutError'
    async with asyncio.timeout(1):
        await asyncio.sleep(0.001)
    return expired, asyncio.current_task().cancelling()
",
    );
    assert_eq!(ended, ("TimeoutError".to_owned(), 0));
}

#[test]
fn create_task_runs_a_child_and_contextvars_pass_down_but_not_back() {
    let seen: (String, String) = result_of(
        c"
import asyncio, contextvars
var = contextvars.ContextVar('var', default='unset')
async def child():
    seen = var.get()
    var.set('child')
    return seen
async def main():
    var.set('parent')
    seen = await asyncio.get_running_loop().create_task(child())
    return seen, var.get()
",
    );
    assert_eq!(seen, ("parent".to_owned(), "parent".to_owned()));
}

#[test]
fn current_task_and_all_tasks_see_the_loops_task() {
    let seen: (String, bool, bool) = result_of(
        c"
import asyncio
async def main():
    me = asyncio.current_task()
    return type(me).__name__, me in asyncio.all_tasks(), me.get_loop() is asyncio.get_running_loop()
",
    );
    assert_eq!(seen, ("_Task".to_owned(), true, true));
}

#[test]
fn a_cancelled_child_raises_cancelled_error_to_its_awaiter_and_runs_its_finally() {
    let seen: (bool, bool, bool) = result_of(
        c"
import asyncio
cleaned = []
async def child():
    try:
        await asyncio.sleep(10)
    finally:
        cleaned.append(True)
async def main():
    task = asyncio.get_running_loop().create_task(child())
    await asyncio.sleep(0)
    task.cancel('stop')
    try:
        await task
        raised = False
    except asyncio.CancelledError:
        raised = True
    return raised, task.cancelled(), cleaned == [True]
",
    );
    assert_eq!(seen, (true, true, true));
}

#[test]
fn a_task_awaits_a_future_of_the_standard_library_and_a_bare_yield() {
    let seen: (i64, i64) = result_of(
        c"
import asyncio, types
@types.coroutine
def bare_yield():
    yield
    return 2
async def main():
    loop = asyncio.get_running_loop()
    fut = asyncio.Future(loop=loop)
    loop.call_later(0.001, fut.set_result, 1)
    return await fut, await bare_yield()
",
    );
    assert_eq!(seen, (1, 2));
}

#[test]
fn a_yield_asyncio_refuses_is_thrown_into_the_coroutine() {
    let said: String = result_of(
        c"
import types
@types.coroutine
def bad():
    yield 'not a future'
async def main():
    try:
        await bad()
    except RuntimeError as e:
        return str(e)
",
    );
    assert_eq!(said, "Task got bad yield: 'not a future'");
}

#[test]
fn a_callback_that_raises_goes_to_the_exception_handler_and_the_turn_goes_on() {
    let seen: (String, bool) = result_of(
        c"
import asyncio
async def main():
    loop = asyncio.get_running_loop()
    seen = []
    loop.set_exception_handler(lambda loop, context: seen.append(type(context['exception']).__name__))
    ran = []
    loop.call_soon(lambda: 1 / 0)
    loop.call_soon(ran.append, True)
    await asyncio.sleep(0.001)
    return seen[0], ran == [True]
",
    );
    assert_eq!(seen, ("ZeroDivisionError".to_owned(), true));
}

#[test]
fn the_servers_own_methods_are_refused_with_why() {
    let said: String = result_of(
        c"
import asyncio
async def main():
    loop = asyncio.get_running_loop()
    try:
        loop.run_until_complete(None)
    except RuntimeError as e:
        return str(e)
",
    );
    assert!(said.starts_with("run_until_complete() is not available"), "{said}");
}

/// Runs `body` with a new loop on a current-thread runtime, the loop bound to `loop` in `names`,
/// and closes the loop after.
fn with_loop<T>(body: impl for<'py> FnOnce(&Bound<'py, EventLoop>, &Bound<'py, PyDict>) -> T) -> T {
    let _serial = serial();
    let rt = runtime();
    let _entered = rt.enter();
    mipyre_subinterp::attach::with_main(|py| {
        let event_loop = new_loop(py);
        let names = PyDict::new(py);
        names.set_item("loop", &event_loop).expect("a dict takes the loop");
        let out = body(&event_loop, &names);
        event_loop.get().release(py);
        out
    })
}

fn run(names: &Bound<'_, PyDict>, source: &CStr) {
    names
        .py()
        .run(source, Some(names), None)
        .unwrap_or_else(|raised| panic!("the source raised {raised}"));
}

fn get<'py, T: for<'a> FromPyObject<'a, 'py>>(names: &Bound<'py, PyDict>, name: &str) -> T {
    let value = names
        .get_item(name)
        .expect("names answers")
        .unwrap_or_else(|| panic!("the source set no {name}"));
    value
        .extract()
        .unwrap_or_else(|_| panic!("{name} is {value}, not the type asked"))
}

fn turn(event_loop: &Bound<'_, EventLoop>) {
    EventLoop::turn(event_loop).expect("a turn");
}

#[test]
fn a_reader_runs_every_turn_while_its_fd_stays_readable() {
    with_loop(|event_loop, names| {
        run(
            names,
            c"
import socket
a, b = socket.socketpair()
calls = []
handle = loop._add_reader(a.fileno(), calls.append, 'read')
b.send(b'x')
",
        );
        for _ in 0..3 {
            turn(event_loop);
        }
        assert_eq!(get::<Vec<String>>(names, "calls").len(), 3);
        run(
            names,
            c"removed = loop._remove_reader(a); again = loop._remove_reader(a)",
        );
        turn(event_loop);
        assert_eq!(get::<Vec<String>>(names, "calls").len(), 3);
        assert!(get::<bool>(names, "removed"));
        assert!(!get::<bool>(names, "again"));
        run(names, c"cancelled = handle.cancelled(); a.close(); b.close()");
        assert!(get::<bool>(names, "cancelled"));
    });
}

#[test]
fn a_timer_comes_due_while_a_watched_fd_stays_readable() {
    with_loop(|event_loop, names| {
        let set = std::time::Instant::now();
        run(
            names,
            c"
import socket
a, b = socket.socketpair()
reads = []
fired = []
loop._add_reader(a.fileno(), reads.append, 'read')
b.send(b'x')
loop.call_later(0.02, fired.append, 'timer')
",
        );
        let mut turns = 0;
        while get::<Vec<String>>(names, "fired").is_empty() {
            assert_eq!(EventLoop::turn(event_loop).expect("a turn"), NextWait::Now);
            turns += 1;
            assert!(
                set.elapsed() < Duration::from_secs(5),
                "the timer did not come due in {turns} turns"
            );
        }
        assert!(set.elapsed() >= Duration::from_millis(20), "it came due early");
        assert!(
            get::<Vec<String>>(names, "reads").len() >= turns,
            "the reader ran every turn"
        );
        run(names, c"loop._remove_reader(a); a.close(); b.close()");
    });
}

/// Returns the sides that ran so far, sorted: kqueue reports an fd's reading and writing as two
/// events in no set order, so only how often each side ran is fixed.
fn runs(names: &Bound<'_, PyDict>) -> Vec<String> {
    let mut calls = get::<Vec<String>>(names, "calls");
    calls.sort();
    calls
}

#[test]
fn a_reader_and_a_writer_on_one_fd_each_run_for_what_it_is_ready_for() {
    with_loop(|event_loop, names| {
        run(
            names,
            c"
import socket
a, b = socket.socketpair()
calls = []
loop._add_writer(a, calls.append, 'write')
loop._add_reader(a, calls.append, 'read')
",
        );
        turn(event_loop);
        assert_eq!(runs(names), ["write"]);
        run(names, c"b.send(b'x')");
        turn(event_loop);
        assert_eq!(runs(names), ["read", "write", "write"]);
        run(names, c"loop._remove_writer(a)");
        turn(event_loop);
        assert_eq!(runs(names), ["read", "read", "write", "write"]);
        run(names, c"loop._remove_reader(a); a.close(); b.close()");
    });
}

#[test]
fn a_closed_fd_without_remove_reader_does_not_reach_a_reused_number() {
    with_loop(|event_loop, names| {
        run(
            names,
            c"
import os, socket
a, b = socket.socketpair()
calls = []
number = a.fileno()
loop._add_reader(number, calls.append, 'old')
a.close()
c, d = socket.socketpair()
reused = c.fileno() == number
if not reused:
    os.dup2(c.fileno(), number)
d.send(b'x')
",
        );
        turn(event_loop);
        assert!(get::<Vec<String>>(names, "calls").is_empty());
        run(
            names,
            c"
removed = loop._remove_reader(number)
if not reused:
    os.close(number)
for s in (b, c, d):
    s.close()
",
        );
        assert!(get::<bool>(names, "removed"));
    });
}

#[test]
fn a_loop_turns_again_while_a_watched_fd_stays_ready_and_parks_once_a_scan_finds_none() {
    with_loop(|event_loop, names| {
        run(
            names,
            c"
import socket
a, b = socket.socketpair()
loop._add_reader(a, print)
",
        );
        assert_eq!(EventLoop::turn(event_loop).expect("a turn"), NextWait::Parked);
        run(names, c"b.send(b'x')");
        assert_eq!(EventLoop::turn(event_loop).expect("a turn"), NextWait::Now);
        assert_eq!(
            EventLoop::turn(event_loop).expect("a turn"),
            NextWait::Now,
            "the byte is still unread"
        );
        run(names, c"a.recv(1)");
        assert_eq!(EventLoop::turn(event_loop).expect("a turn"), NextWait::Parked);
        run(names, c"loop._remove_reader(a); a.close(); b.close()");
    });
}

#[test]
fn a_cancelled_reader_of_a_ready_fd_is_unwatched_not_run_and_the_loop_then_parks() {
    with_loop(|event_loop, names| {
        run(
            names,
            c"
import socket
a, b = socket.socketpair()
calls = []
handle = loop._add_reader(a, calls.append, 'read')
b.send(b'x')
handle.cancel()
",
        );
        assert_eq!(EventLoop::turn(event_loop).expect("a turn"), NextWait::Now);
        run(
            names,
            c"
try:
    loop._selector.get_key(a)
    watched = True
except KeyError:
    watched = False
",
        );
        assert!(get::<Vec<String>>(names, "calls").is_empty());
        assert!(!get::<bool>(names, "watched"));
        assert_eq!(EventLoop::turn(event_loop).expect("a turn"), NextWait::Parked);
        run(names, c"a.close(); b.close()");
    });
}

#[test]
fn a_change_the_kernel_refuses_takes_the_fd_out_and_a_later_add_of_another_file_starts_over() {
    with_loop(|event_loop, names| {
        run(
            names,
            c"
import os, socket
a, b = socket.socketpair()
kept = a.dup()
number = a.fileno()
loop._add_reader(number, print)
a.close()
try:
    loop._add_writer(number, print)
    refused = 'added'
except OSError as e:
    refused = type(e).__name__
try:
    loop._selector.get_key(number)
    watched = True
except KeyError:
    watched = False
c, d = socket.socketpair()
reused = c.fileno() == number
if not reused:
    os.dup2(c.fileno(), number)
calls = []
loop._add_reader(number, calls.append, 'read')
d.send(b'x')
",
        );
        assert_eq!(get::<String>(names, "refused"), "OSError");
        assert!(!get::<bool>(names, "watched"));
        turn(event_loop);
        assert_eq!(get::<Vec<String>>(names, "calls"), ["read"]);
        run(
            names,
            c"
loop._remove_reader(number)
if not reused:
    os.close(number)
for s in (b, c, d, kept):
    s.close()
",
        );
    });
}

#[test]
fn a_registration_left_behind_by_a_closed_fd_does_not_keep_the_loop_turning() {
    with_loop(|event_loop, names| {
        run(
            names,
            c"
import socket
a, b = socket.socketpair()
c, d = socket.socketpair()
kept = a.dup()
number = a.fileno()
loop._add_reader(number, print)
loop._add_reader(c, print)
a.close()
removed = loop._remove_reader(number)
b.send(b'x')
",
        );
        assert!(get::<bool>(names, "removed"));
        assert_eq!(EventLoop::turn(event_loop).expect("a turn"), NextWait::Parked);
        run(
            names,
            c"
loop._remove_reader(c)
for s in (b, c, d, kept):
    s.close()
",
        );
    });
}

#[test]
fn a_stale_registration_under_a_number_the_table_reuses_runs_nothing_and_lets_the_loop_park() {
    with_loop(|event_loop, names| {
        run(
            names,
            c"
import os, socket
a, b = socket.socketpair()
kept = a.dup()
number = a.fileno()
r, w = os.pipe()
os.set_blocking(w, False)
try:
    while True:
        os.write(w, b'x' * 65536)
except BlockingIOError:
    pass
loop._add_reader(number, print)
a.close()
removed = loop._remove_reader(number)
os.dup2(w, number)
os.close(w)
calls = []
loop._add_writer(number, calls.append, 'write')
b.send(b'x')
",
        );
        assert!(get::<bool>(names, "removed"));
        let waits: Vec<NextWait> = (0..3).map(|_| EventLoop::turn(event_loop).expect("a turn")).collect();
        let calls: Vec<String> = get(names, "calls");
        run(
            names,
            c"
loop._remove_writer(number)
os.close(number)
os.close(r)
for s in (b, kept):
    s.close()
",
        );
        assert!(calls.is_empty(), "{calls:?}");
        assert_eq!(waits, [NextWait::Parked; 3]);
    });
}

#[cfg(target_os = "linux")]
#[test]
fn a_stale_registration_under_a_number_the_table_reuses_runs_that_numbers_reader_as_the_selector_loop_does() {
    with_loop(|event_loop, names| {
        run(
            names,
            c"
import os, socket
a, b = socket.socketpair()
kept = a.dup()
number = a.fileno()
r, w = os.pipe()
loop._add_reader(number, print)
a.close()
removed = loop._remove_reader(number)
os.dup2(r, number)
os.close(r)
calls = []
loop._add_reader(number, calls.append, 'read')
b.send(b'x')
",
        );
        assert!(get::<bool>(names, "removed"));
        let wait = EventLoop::turn(event_loop).expect("a turn");
        let calls: Vec<String> = get(names, "calls");
        run(
            names,
            c"
loop._remove_reader(number)
os.close(number)
os.close(w)
for s in (b, kept):
    s.close()
",
        );
        assert_eq!(calls, ["read"]);
        assert_eq!(wait, NextWait::Now);
    });
}

#[cfg(target_os = "linux")]
#[test]
fn re_adding_a_refused_fd_on_the_same_file_is_refused_as_already_watched_as_selectors_does() {
    with_loop(|event_loop, names| {
        run(
            names,
            c"
import errno, os, socket
seen = []
loop.set_exception_handler(lambda l, c: seen.append((c.get('fd'), type(c['exception']).__name__)))
y, yp = socket.socketpair()
calls = []
def on_y():
    y.recv(1)
    calls.append('y')
loop._add_reader(y, on_y)
yp.send(b'y')
x, xp = socket.socketpair()
kept = x.dup()
xn = x.fileno()
xp.send(b'x')
rh = loop._add_reader(xn, print)
loop._add_writer(xn, print)
rh.cancel()
x.close()
",
        );
        let first = EventLoop::turn(event_loop).expect("the turn survives the refusal");
        let second = EventLoop::turn(event_loop).expect("a turn");
        run(
            names,
            c"
try:
    loop._selector.get_key(xn)
    x_watched = True
except KeyError:
    x_watched = False
os.dup2(kept.fileno(), xn)
try:
    loop._add_reader(xn, print)
    readd = ('added', False)
    loop._remove_reader(xn)
except OSError as e:
    readd = (type(e).__name__, e.errno == errno.EEXIST)
os.close(xn)
loop._remove_reader(y)
for s in (y, yp, xp, kept):
    s.close()
",
        );
        let seen: Vec<(i64, String)> = get(names, "seen");
        assert_eq!(get::<Vec<String>>(names, "calls"), ["y"]);
        assert_eq!(seen, [(i64::from(get::<i32>(names, "xn")), "OSError".to_owned())]);
        assert!(!get::<bool>(names, "x_watched"));
        assert_eq!((first, second), (NextWait::Now, NextWait::Parked));
        assert_eq!(
            get::<(String, bool)>(names, "readd"),
            ("FileExistsError".to_owned(), true)
        );
    });
}

#[cfg(target_os = "linux")]
#[test]
fn a_refused_watch_raises_the_kernels_errno_as_selectors_does() {
    with_loop(|_, names| {
        run(
            names,
            c"
import selectors, tempfile
f = tempfile.TemporaryFile()
try:
    loop._add_reader(f, print)
except OSError as e:
    ours = (type(e).__name__, e.errno, e.strerror)
s = selectors.EpollSelector()
try:
    s.register(f, selectors.EVENT_READ)
except OSError as e:
    stdlib = (type(e).__name__, e.errno, e.strerror)
s.close()
f.close()
",
        );
        let ours: (String, Option<i64>, String) = get(names, "ours");
        let stdlib: (String, Option<i64>, String) = get(names, "stdlib");
        assert_eq!(ours, stdlib);
    });
}

#[test]
fn more_ready_fds_than_one_scan_takes_are_all_served_on_the_next_turn() {
    with_loop(|event_loop, names| {
        run(
            names,
            c"
import socket
pairs = [socket.socketpair() for _ in range(65)]
calls = []
def reader(sock):
    def read():
        sock.recv(1)
        loop._remove_reader(sock)
        calls.append(sock.fileno())
    return read
for a, b in pairs:
    loop._add_reader(a, reader(a))
    b.send(b'x')
",
        );
        assert_eq!(EventLoop::turn(event_loop).expect("a turn"), NextWait::Now);
        assert_eq!(get::<Vec<i64>>(names, "calls").len(), 64);
        assert_eq!(EventLoop::turn(event_loop).expect("a turn"), NextWait::Now);
        assert_eq!(get::<Vec<i64>>(names, "calls").len(), 65);
        assert_eq!(EventLoop::turn(event_loop).expect("a turn"), NextWait::Parked);
        run(
            names,
            c"
for a, b in pairs:
    a.close(); b.close()
",
        );
    });
}

#[test]
fn watching_an_fd_on_a_closed_loop_raises_and_unwatching_one_says_nothing_was_there() {
    with_loop(|event_loop, names| {
        event_loop.get().release(names.py());
        run(
            names,
            c"
import socket
a, b = socket.socketpair()
try:
    loop._add_reader(a, print)
    said = 'added'
except RuntimeError as e:
    said = str(e)
removed = loop._remove_reader(a)
a.close(); b.close()
",
        );
        assert_eq!(get::<String>(names, "said"), "Event loop is closed");
        assert!(!get::<bool>(names, "removed"));
    });
}

#[test]
fn an_fd_argument_is_a_number_or_has_fileno_and_is_not_negative() {
    with_loop(|_, names| {
        run(
            names,
            c"
said = []
for bad in (object(), -1):
    try:
        loop._add_reader(bad, print)
    except ValueError as e:
        said.append(str(e).split(':')[0])
",
        );
        assert_eq!(
            get::<Vec<String>>(names, "said"),
            ["Invalid file object", "Invalid file descriptor"]
        );
    });
}

#[test]
fn a_fileno_that_raises_other_than_selectors_reads_as_unusable_passes_its_error_through() {
    with_loop(|_, names| {
        run(
            names,
            c"
class Raises:
    def __init__(self, error):
        self.error = error
    def fileno(self):
        raise self.error
said = []
for error in (AttributeError('no'), TypeError('no'), ValueError('no'), OSError(9, 'detached')):
    try:
        loop._add_reader(Raises(error), print)
    except Exception as e:
        said.append(type(e).__name__)
",
        );
        assert_eq!(
            get::<Vec<String>>(names, "said"),
            ["ValueError", "ValueError", "ValueError", "OSError"]
        );
    });
}

#[test]
fn the_selector_view_says_what_an_fd_is_watched_for_and_raises_for_one_not_watched() {
    with_loop(|_, names| {
        run(
            names,
            c"import socket
a, b = socket.socketpair()
loop._add_reader(a, print)
read = loop._selector.get_key(a).events
loop._add_writer(a.fileno(), print)
both = loop._selector.get_key(a.fileno()).events
loop._remove_reader(a)
write = loop._selector.get_key(a).events
loop._remove_writer(a)
try:
    loop._selector.get_key(a)
    unwatched = 'found'
except KeyError:
    unwatched = 'KeyError'
a.close(); b.close()",
        );
        assert_eq!(get::<u8>(names, "read"), 1);
        assert_eq!(get::<u8>(names, "both"), 3);
        assert_eq!(get::<u8>(names, "write"), 2);
        assert_eq!(get::<String>(names, "unwatched"), "KeyError");
    });
}

#[test]
fn asyncios_closed_check_raises_only_once_the_loop_is_closed_and_debug_follows_set_debug() {
    with_loop(|event_loop, names| {
        run(
            names,
            c"loop._check_closed()
loop.set_debug(True)
debug_on = loop._debug
loop.set_debug(False)
debug_off = loop._debug",
        );
        assert!(get::<bool>(names, "debug_on"));
        assert!(!get::<bool>(names, "debug_off"));
        event_loop.get().release(event_loop.py());
        let raised = event_loop.call_method0("_check_closed").expect_err("a closed loop");
        assert!(raised.is_instance_of::<PyRuntimeError>(event_loop.py()), "{raised}");
        assert!(raised.to_string().contains("Event loop is closed"), "{raised}");
    });
}

/// Loads `source`, a Python file of the repo, as a module, because a test binary does not load
/// the `mipyre` package or the repo's tests.
fn module_from<'py>(py: Python<'py>, source: &str, file: &CStr, name: &CStr) -> Bound<'py, PyModule> {
    let source = std::ffi::CString::new(source).expect("no NUL in the source");
    PyModule::from_code(py, &source, file, name).expect("the module compiles")
}

#[test]
fn asyncio_reads_nothing_on_the_tpc_loop_that_it_does_not_give_and_never_reaches_its_scheduler() {
    let _serial = serial();
    mipyre_subinterp::attach::with_main(|py| {
        let tpc_loop = module_from(
            py,
            include_str!("../../../../python/mipyre/_worker/tpc_loop.py"),
            c"tpc_loop.py",
            c"tpc_loop",
        );
        let census = module_from(
            py,
            include_str!("../../../../tests/test_tpc_loop_inherits.py"),
            c"test_tpc_loop_inherits.py",
            c"test_tpc_loop_inherits",
        );
        let native = py.get_type::<EventLoop>();
        let class = tpc_loop
            .getattr("make_loop_class")
            .and_then(|make| make.call1((&native,)))
            .expect("the loop class");
        let ungiven: std::collections::BTreeSet<String> = census
            .getattr("ungiven")
            .and_then(|ungiven| ungiven.call1((&class, &native)))
            .and_then(|found| found.extract())
            .expect("the census runs");
        assert!(ungiven.is_empty(), "read by asyncio and given by nobody: {ungiven:?}");
        let methods: std::collections::BTreeSet<String> = census
            .getattr("tpc_loop_census")
            .and_then(|count| count.call1((&class, &native)))
            .and_then(|found| found.getattr("methods"))
            .and_then(|methods| methods.extract())
            .expect("the census runs");
        let scheduler: std::collections::BTreeSet<String> = census
            .getattr("SCHEDULER")
            .and_then(|scheduler| scheduler.extract())
            .expect("the scheduler's names");
        assert!(
            methods.is_disjoint(&scheduler),
            "the standard library's scheduler is reached: {:?}",
            methods.intersection(&scheduler).collect::<Vec<_>>()
        );
        assert!(
            methods.contains("sock_recv") && methods.contains("_ensure_fd_no_transport"),
            "{methods:?}"
        );
    });
}

#[test]
fn timers_due_at_one_instant_run_in_the_order_set() {
    let order: Vec<i64> = result_of(
        c"
import asyncio
async def main():
    loop = asyncio.get_running_loop()
    order = []
    when = loop.time() + 0.01
    for i in range(8):
        loop.call_at(when, order.append, i)
    await asyncio.sleep(0.05)
    return order
",
    );
    assert_eq!(order, (0..8).collect::<Vec<_>>());
}

#[test]
fn a_cancelled_timer_is_taken_out_of_the_loops_timers() {
    let _serial = serial();
    let rt = runtime();
    let _entered = rt.enter();
    mipyre_subinterp::attach::with_main(|py| {
        let event_loop = new_loop(py);
        let print = py.import("builtins").and_then(|b| b.getattr("print")).expect("print");
        let handle = EventLoop::call_later(&event_loop, 10.0, print.unbind(), PyTuple::empty(py), None)
            .expect("a timer on the loop's thread");
        let timer = handle.bind(py);
        let key = (*timer.get().key.lock()).expect("a queued timer has its key");
        timer.call_method0("cancel").expect("cancel");
        assert!(timer.as_super().get().is_cancelled());
        assert!(timer.get().key.lock().is_none(), "the handle let go of its key");
        let removed = event_loop.get().core.lock().timers.remove(&key);
        assert!(removed.is_none(), "the timer is no longer in the loop's timers");
        event_loop.get().release(py);
    });
}

#[test]
fn call_later_from_another_thread_raises_instead_of_reaching_tokio() {
    let _serial = serial();
    let rt = runtime();
    let _entered = rt.enter();
    let event_loop = mipyre_subinterp::attach::with_main(|py| new_loop(py).unbind());
    let said = std::thread::spawn({
        let event_loop = mipyre_subinterp::attach::with_main(|py| event_loop.clone_ref(py));
        move || {
            mipyre_subinterp::attach::with_main(|py| {
                let raised = event_loop
                    .bind(py)
                    .call_method1("call_later", (1.0, py.None()))
                    .expect_err("another thread's call_later raises");
                assert!(raised.is_instance_of::<PyRuntimeError>(py), "{raised}");
                event_loop.drop_ref(py);
                raised.to_string()
            })
        }
    })
    .join()
    .expect("the other thread returns");
    assert!(said.contains("Non-thread-safe operation"), "{said}");
    mipyre_subinterp::attach::with_main(|py| {
        let handle = event_loop
            .bind(py)
            .call_method1("call_later", (1.0, py.None()))
            .expect("the loop's own thread gets a handle");
        assert!(handle.cast::<Handle>().is_ok());
        event_loop.get().release(py);
        event_loop.drop_ref(py);
    });
}

#[test]
fn a_loop_built_off_a_tokio_runtime_raises() {
    let _serial = serial();
    mipyre_subinterp::attach::with_main(|py| {
        let raised = py
            .get_type::<EventLoop>()
            .call0()
            .expect_err("no tokio runtime on this thread");
        assert!(raised.is_instance_of::<PyRuntimeError>(py), "{raised}");
        assert!(raised.to_string().contains("tokio runtime"), "{raised}");
    });
}

#[test]
fn a_threadsafe_call_from_another_thread_rings_a_parked_loop() {
    let _serial = serial();
    let rt = runtime();
    let _entered = rt.enter();
    mipyre_subinterp::attach::with_main(|py| {
        let names = PyDict::new(py);
        py.run(c"ran = []", Some(&names), None).expect("the source runs");
        let ran = names.get_item("ran").expect("names answers").expect("ran is set");
        let event_loop = new_loop(py);
        assert_eq!(EventLoop::turn(&event_loop).expect("a turn"), NextWait::Parked);
        let (target, append) = (
            event_loop.clone().unbind(),
            ran.getattr("append").expect("append").unbind(),
        );
        py.detach(|| {
            std::thread::spawn(move || {
                mipyre_subinterp::attach::with_main(|py| {
                    target
                        .bind(py)
                        .call_method1("call_soon_threadsafe", (append, 1))
                        .expect("a threadsafe schedule");
                });
            })
            .join()
            .expect("the other thread returns");
        });
        drive(&rt, &event_loop, || ran.len().expect("len") == 1);
        event_loop.get().release(py);
    });
}

/// A request Task: started with a sink, it takes its first step at once, counts as live while it
/// waits, and leaves the count when it ends.
#[test]
fn a_started_task_counts_as_live_until_it_ends_and_hands_its_end_to_its_sink() {
    let _serial = serial();
    let rt = runtime();
    let _entered = rt.enter();
    mipyre_subinterp::attach::with_main(|py| {
        let names = PyDict::new(py);
        py.run(
            c"
import asyncio
class Later:
    def __await__(self):
        yield from asyncio.sleep(0.001).__await__()
        return 'later'
",
            Some(&names),
            None,
        )
        .expect("the source runs");
        let event_loop = new_loop(py);
        let ended = std::sync::Arc::new(std::sync::Mutex::new(None));
        let sink: TaskSink = Box::new({
            let ended = std::sync::Arc::clone(&ended);
            move |_, result| {
                *ended.lock().expect("the test's lock") =
                    Some(result.map(|v| v.to_string()).map_err(|e| e.to_string()));
            }
        });
        let awaitable = names
            .get_item("Later")
            .expect("names answers")
            .expect("Later is set")
            .call0()
            .expect("Later()");
        let context = ContextSuspension::from(PyContext::new(py).expect("a context"));
        EventLoop::start_task(&event_loop, awaitable, context, Some(sink), true).expect("the task starts");
        assert_eq!(event_loop.get().live(), LiveTasks { count: 1, large: 1 });
        drive(&rt, &event_loop, || ended.lock().expect("the test's lock").is_some());
        assert_eq!(event_loop.get().live(), LiveTasks { count: 0, large: 0 });
        assert_eq!(
            ended.lock().expect("the test's lock").take(),
            Some(Ok("later".to_owned()))
        );
        event_loop.get().release(py);
    });
}

/// Returns `BENCH_N`, or 16 for the ordinary test run.
fn bench_n() -> u64 {
    std::env::var("BENCH_N").map_or(16, |n| n.parse().expect("BENCH_N: a number of calls"))
}

/// The instructions a request Task's `_register_task` / `_unregister_task` pair adds: run under
/// `perf stat` and subtract the `BENCH_N=0` run.
///
///   BENCH_N=200000 perf stat -x, -e instructions:u -- target/release/deps/mipyre_runtime-* \
///       --exact event_loop::tests::registering_with_asyncio_and_leaving
#[test]
fn registering_with_asyncio_and_leaving() {
    let _serial = serial();
    mipyre_subinterp::attach::with_main(|py| {
        let tasks = py.import("asyncio.tasks").expect("asyncio.tasks");
        let register = tasks.getattr("_register_task").expect("_register_task");
        let unregister = tasks.getattr("_unregister_task").expect("_unregister_task");
        let names = PyDict::new(py);
        py.run(c"class Weak:\n    pass\nweak = Weak()\n", Some(&names), None)
            .expect("the source runs");
        let task = names.get_item("weak").expect("names answers").expect("weak is set");
        for _ in 0..bench_n() {
            register.call1((&task,)).expect("registered");
            unregister.call1((&task,)).expect("unregistered");
        }
    });
}

/// A Task's first step as it is taken: a new coroutine, its `__await__()` iterator, one `send`.
/// The loop's instructions less those of [`making_coroutines`] are the step's.
#[test]
fn stepping_a_coroutine_through_its_await_iterator() {
    step_coroutines(|coroutine| {
        let py = coroutine.py();
        let steps = task::steps_of(&coroutine).expect("a coroutine is awaitable");
        assert!(matches!(
            steps.send(&py.None().into_bound(py)),
            Ok(PySendResult::Return(_))
        ));
    });
}

#[test]
fn making_coroutines() {
    step_coroutines(|coroutine| coroutine.call_method0("close").map(drop).expect("closed"));
}

fn step_coroutines(step: impl for<'py> Fn(Bound<'py, PyAny>)) {
    let _serial = serial();
    mipyre_subinterp::attach::with_main(|py| {
        let names = PyDict::new(py);
        py.run(c"async def f():\n    return 1\n", Some(&names), None)
            .expect("the source runs");
        let f = names.get_item("f").expect("names answers").expect("f is set");
        for _ in 0..bench_n() {
            step(f.call0().expect("a coroutine"));
        }
    });
}

/// `BENCH_N` `call_soon` calls from Python, each run on a turn of its own: run under `perf stat` and
/// subtract the `BENCH_N=0` run.
///
///   BENCH_N=200000 perf stat -x, -e instructions:u -- target/release/deps/mipyre_runtime-* \
///       --exact event_loop::tests::call_soon_callbacks_scheduled_and_run
#[test]
fn call_soon_callbacks_scheduled_and_run() {
    with_loop(|event_loop, names| {
        run(names, c"def noop():\n    pass\nschedule = loop.call_soon\n");
        let schedule = names
            .get_item("schedule")
            .expect("names answers")
            .expect("schedule is set");
        let noop = names.get_item("noop").expect("names answers").expect("noop is set");
        for _ in 0..bench_n() {
            schedule.call1((&noop,)).expect("call_soon schedules");
            EventLoop::turn(event_loop).expect("a turn");
        }
    });
}

/// Panics as a Rust function a handler calls would, for the tests of what the loop does with it.
#[pyfunction]
fn rust_panic(text: &str) {
    panic!("{text}");
}

#[test]
fn a_rust_panic_in_a_task_ends_that_task_and_the_next_task_and_callback_run() {
    with_loop(|event_loop, names| {
        let py = names.py();
        names
            .set_item("rust_panic", wrap_pyfunction!(rust_panic, py).expect("a pyfunction"))
            .expect("a dict takes it");
        run(
            names,
            c"
import asyncio
seen = []
loop.set_exception_handler(lambda l, c: seen.append((type(c['exception']).__name__, str(c['exception']))))
async def panics():
    rust_panic('in a task')
async def fine():
    return 'fine'
panicked = loop.create_task(panics())
loop.call_soon(rust_panic, 'in a callback')
ran = []
loop.call_soon(ran.append, True)
after = loop.create_task(fine())
",
        );
        for _ in 0..4 {
            turn(event_loop);
        }
        run(
            names,
            c"
ended = (panicked.done(), type(panicked.exception()).__name__, str(panicked.exception()))
after_result = after.result()
",
        );
        let ended: (bool, String, String) = get(names, "ended");
        assert_eq!(ended, (true, "PanicException".to_owned(), "in a task".to_owned()));
        assert_eq!(get::<String>(names, "after_result"), "fine");
        assert_eq!(get::<Vec<bool>>(names, "ran"), vec![true]);
        let seen: Vec<(String, String)> = get(names, "seen");
        assert!(
            seen.contains(&("PanicException".to_owned(), "in a callback".to_owned())),
            "{seen:?}"
        );
    });
}
