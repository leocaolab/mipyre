use std::ffi::CStr;
use std::time::Duration;

use tokio::runtime::Runtime;

use super::*;
use crate::worker::sync_runner::tests::{get_root, sync_worker};

#[test]
fn a_worker_retires_after_three_failed_turns_in_a_row() {
    assert!(!retires(2));
    assert!(retires(3));
}

const ASYNC_REQUESTS: &str = include_str!("../../../../../python/mipyre/_worker/async_requests.py");
const TPC_LOOP: &str = include_str!("../../../../../python/mipyre/_worker/tpc_loop.py");

/// Registers the runtime's Python modules in the worker's interpreter from their sources, because
/// a test binary does not load the `mipyre` package.
fn register_worker_modules(worker: &mut SubInterpreterWorker<SyncRunner>) {
    register_module(worker, "mipyre._worker.async_requests", ASYNC_REQUESTS);
    register_module(worker, "mipyre._worker.tpc_loop", TPC_LOOP);
}

/// Registers `source` as the module `name` of the `mipyre._worker` package in the worker's
/// interpreter.
fn register_module(worker: &mut SubInterpreterWorker<SyncRunner>, name: &'static str, source: &'static str) {
    worker
        .with_gil(|_, py| {
            let source = std::ffi::CString::new(source).expect("no NUL in the source");
            let module_name = std::ffi::CString::new(name).expect("no NUL in the name");
            let module = PyModule::from_code(py, &source, &module_name, &module_name).expect("the module compiles");
            let modules = py
                .import("sys")
                .and_then(|sys| sys.getattr("modules"))
                .expect("sys.modules");
            for package in ["mipyre", "mipyre._worker"] {
                if !modules.contains(package).expect("sys.modules answers") {
                    let made = PyModule::new(py, package).expect("an empty package");
                    modules.set_item(package, made).expect("the package registered");
                }
            }
            modules.set_item(name, module).expect("the module registered");
        })
        .expect("no panic registering a module");
}

/// Runs `test` on a thread of its own, in a current-thread tokio runtime with timers and I/O,
/// with a worker serving `app`'s one route `GET /`.
fn on_a_thread(
    app: &'static CStr,
    test: impl FnOnce(&Runtime, &mut SubInterpreterWorker<SyncRunner>, RouteId) + Send + 'static,
) {
    Python::initialize();
    std::thread::Builder::new()
        .name("request-loop".into())
        .stack_size(16 << 20)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .enable_io()
                .build()
                .expect("a current-thread runtime");
            let _entered = rt.enter();
            let (mut worker, route) = sync_worker(app);
            test(&rt, &mut worker, route);
        })
        .expect("a thread")
        .join()
        .expect("the test's thread returns");
}

/// Runs `test` as [`on_a_thread`] does, with the worker's loop open; the loop is closed after.
fn on_a_loop(
    app: &'static CStr,
    test: impl FnOnce(&Runtime, &mut SubInterpreterWorker<SyncRunner>, &mut RequestLoop, RouteId) + Send + 'static,
) {
    on_a_thread(app, |rt, worker, route| {
        register_worker_modules(worker);
        let mut on = worker.open_event_loop().expect("the loop opens");
        test(rt, worker, &mut on, route);
        worker.close_event_loop(on);
    });
}

/// Turns the loop until `done` says so; when it parks, waits for its bell or for an fd it watches
/// to be ready, as a TPC thread's driver does.
fn drive(
    rt: &Runtime,
    worker: &mut SubInterpreterWorker<SyncRunner>,
    on: &mut RequestLoop,
    mut done: impl FnMut(&RequestLoop) -> bool,
) {
    let bell = on.bell();
    let poller = on.register_poller().expect("the poller's fd registers with tokio");
    for _ in 0..100_000 {
        if done(on) {
            return;
        }
        let wait = worker.turn(on).unwrap_or_else(|retired| panic!("{retired}"));
        if wait == NextWait::Parked && !done(on) {
            rt.block_on(tokio::time::timeout(Duration::from_secs(5), async {
                tokio::select! {
                    () = bell.rung() => {}
                    ready = poller.wait() => ready.expect("the poller's readiness"),
                }
            }))
            .expect("the bell rings or an fd is ready within five seconds");
        }
    }
    panic!("the loop did not get there in 100000 turns");
}

/// Returns the body of the answer `reply` holds, or the error it holds as text.
fn answered(reply: &mut LoopReply) -> Result<String, String> {
    match reply.0.try_recv() {
        Ok(Ok(LoopAnswer(response))) => Ok(String::from_utf8_lossy(&response.body).into_owned()),
        Ok(Err(record)) => Err(format!("{:?}", record.into_error_and_id().0)),
        Err(e) => Err(format!("no answer: {e}")),
    }
}

#[test]
fn an_async_handler_that_sleeps_is_answered_through_its_loop_reply() {
    on_a_loop(
        c"import asyncio\nasync def handler(req):\n    await asyncio.sleep(0.001)\n    return 'slept'\nbefore = []\nafter = []\n",
        |rt, worker, on, route| {
            let mut reply = worker.start_task(on, route, get_root(), false).expect("the request starts");
            assert_eq!(on.live(), LiveTasks { count: 1, large: 0 }, "the Task waits on its sleep");
            drive(rt, worker, on, |on| on.live() == LiveTasks { count: 0, large: 0 });
            let body = answered(&mut reply).expect("the handler's value");
            assert!(body.contains("slept"), "{body}");
        },
    );
}

#[test]
fn a_handler_that_returns_a_plain_value_is_answered_without_a_task() {
    on_a_loop(
        c"handler = lambda req: 'plain'\nbefore = []\nafter = []\n",
        |_, worker, on, route| {
            let mut reply = worker
                .start_task(on, route, get_root(), true)
                .expect("the request starts");
            assert_eq!(on.live(), LiveTasks { count: 0, large: 0 });
            let body = answered(&mut reply).expect("the handler's value");
            assert!(body.contains("plain"), "{body}");
        },
    );
}

#[test]
fn a_handler_that_raises_is_answered_with_its_exception() {
    on_a_loop(
        c"async def handler(req):\n    raise ValueError('no')\nbefore = []\nafter = []\n",
        |_, worker, on, route| {
            let mut reply = worker
                .start_task(on, route, get_root(), false)
                .expect("the request starts");
            assert_eq!(
                on.live(),
                LiveTasks { count: 0, large: 0 },
                "the Task ended in its first step"
            );
            let error = answered(&mut reply).expect_err("the handler raised");
            assert!(error.contains("ValueError") && error.contains("no"), "{error}");
        },
    );
}

#[test]
fn a_task_whose_reply_was_dropped_runs_to_its_end_and_leaves_the_count() {
    on_a_loop(
        c"import asyncio\nended = []\nasync def handler(req):\n    await asyncio.sleep(0.001)\n    ended.append(True)\n    return 'late'\nbefore = []\nafter = []\n",
        |rt, worker, on, route| {
            let reply = worker.start_task(on, route, get_root(), true).expect("the request starts");
            drop(reply);
            assert_eq!(on.live(), LiveTasks { count: 1, large: 1 });
            drive(rt, worker, on, |on| on.live() == LiveTasks { count: 0, large: 0 });
        },
    );
}

#[test]
fn cancelling_the_live_tasks_ends_them_and_answers_their_requests() {
    on_a_loop(
        c"import asyncio\nasync def handler(req):\n    await asyncio.sleep(10)\nbefore = []\nafter = []\n",
        |rt, worker, on, route| {
            let mut reply = worker
                .start_task(on, route, get_root(), false)
                .expect("the request starts");
            assert_eq!(worker.cancel_live_tasks(on).expect("the cancel"), 1);
            drive(rt, worker, on, |on| on.live() == LiveTasks { count: 0, large: 0 });
            let error = answered(&mut reply).expect_err("a cancelled request has no value");
            assert!(error.contains("CancelledError"), "{error}");
        },
    );
}

#[test]
fn an_app_with_hooks_runs_them_around_its_async_handler() {
    on_a_loop(
        c"import asyncio\nseen = []\nasync def handler(req):\n    await asyncio.sleep(0.001)\n    return 'hooked'\ndef note(req):\n    seen.append('before')\nbefore = [note]\nafter = []\n",
        |rt, worker, on, route| {
            let mut reply = worker.start_task(on, route, get_root(), false).expect("the request starts");
            drive(rt, worker, on, |on| on.live() == LiveTasks { count: 0, large: 0 });
            let body = answered(&mut reply).expect("the handler's value");
            assert!(body.contains("hooked"), "{body}");
        },
    );
}

#[test]
fn a_request_loop_dropped_without_being_closed_leaks_and_says_so() {
    on_a_thread(
        c"handler = lambda req: 'ok'\nbefore = []\nafter = []\n",
        |_, worker, _| {
            register_worker_modules(worker);
            let on = worker.open_event_loop().expect("the loop opens");
            let logged = crate::testkit::captured_log(|| drop(on));
            assert!(logged.contains("dropped without being closed"), "{logged}");
        },
    );
}

/// Answers one request of the route `route` on the loop `on`, turning the loop until it is.
fn ask(
    rt: &Runtime,
    worker: &mut SubInterpreterWorker<SyncRunner>,
    on: &mut RequestLoop,
    route: RouteId,
) -> Result<String, String> {
    let mut reply = worker
        .start_task(on, route, get_root(), false)
        .expect("the request starts");
    drive(rt, worker, on, |on| on.live() == LiveTasks { count: 0, large: 0 });
    answered(&mut reply)
}

#[test]
fn a_handler_serves_and_connects_to_a_socket_server_on_its_loop() {
    on_a_loop(
        c"import asyncio
async def echo(reader, writer):
    writer.write(await reader.readline())
    await writer.drain()
    writer.close()
    await writer.wait_closed()
async def handler(req):
    server = await asyncio.start_server(echo, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    shown = repr(writer.transport)
    writer.write(b'hello\\n')
    await writer.drain()
    line = await reader.readline()
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    return f'{line.decode().strip()} {shown}'
before = []
after = []
",
        |rt, worker, on, route| {
            let body = ask(rt, worker, on, route).expect("the handler's value");
            assert!(body.contains("hello"), "{body}");
            assert!(
                body.contains("read=polling"),
                "a transport's repr reads the loop's fds: {body}"
            );
        },
    );
}

#[test]
fn a_handler_waits_on_its_loop_for_a_socket_to_become_readable() {
    on_a_loop(
        c"import asyncio, socket
async def handler(req):
    loop = asyncio.get_running_loop()
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    received = asyncio.ensure_future(loop.sock_recv(b, 4))
    await asyncio.sleep(0.01)
    await loop.sock_sendall(a, b'ping')
    got = await received
    a.close()
    b.close()
    return got.decode()
before = []
after = []
",
        |rt, worker, on, route| {
            let body = ask(rt, worker, on, route).expect("the handler's value");
            assert!(body.contains("ping"), "{body}");
        },
    );
}

#[test]
fn a_handler_runs_blocking_calls_and_name_lookups_on_its_loops_executor() {
    on_a_loop(
        c"import asyncio, time
async def handler(req):
    loop = asyncio.get_running_loop()
    slept = await asyncio.to_thread(time.sleep, 0.01)
    total = await loop.run_in_executor(None, sum, [1, 2, 3])
    found = await loop.getaddrinfo('localhost', 80)
    return f'{slept} {total} {len(found) > 0}'
before = []
after = []
",
        |rt, worker, on, route| {
            let body = ask(rt, worker, on, route).expect("the handler's value");
            assert!(body.contains("None 6 True"), "{body}");
            finish(rt, worker, on);
        },
    );
}

#[test]
fn a_handler_runs_a_subprocess_on_linux_and_is_refused_where_child_watching_needs_a_daemon_thread() {
    on_a_loop(
        c"import asyncio
async def handler(req):
    try:
        child = await asyncio.create_subprocess_exec('echo', 'hi', stdout=asyncio.subprocess.PIPE)
    except RuntimeError as refused:
        return f'refused: {refused}'
    out, _ = await child.communicate()
    return f'{out.decode().strip()} {child.returncode}'
before = []
after = []
",
        |rt, worker, on, route| {
            let body = ask(rt, worker, on, route).expect("the handler's value");
            if cfg!(target_os = "linux") {
                assert!(body.contains("hi 0"), "{body}");
            } else {
                assert!(body.contains("daemon threads"), "{body}");
            }
        },
    );
}

/// Runs the loop's finalizers to their end, which counts no live Task meanwhile.
fn finish(rt: &Runtime, worker: &mut SubInterpreterWorker<SyncRunner>, on: &mut RequestLoop) {
    let mut finished = worker
        .start_finalizers(on, Duration::from_secs(5))
        .expect("the finalizers start");
    assert_eq!(
        on.live(),
        LiveTasks { count: 0, large: 0 },
        "the finalizers are no request"
    );
    let mut ended = false;
    drive(rt, worker, on, |_| {
        ended = ended || finished.try_recv().is_ok();
        ended
    });
}

#[test]
fn the_finalizers_close_an_open_async_generator_and_shut_the_executor_down_outside_the_live_count() {
    on_a_loop(
        c"import asyncio
closed = []
held = []
async def numbers():
    try:
        yield 1
        yield 2
    finally:
        closed.append(True)
async def handler(req):
    loop = asyncio.get_running_loop()
    if not held:
        held.append(numbers())
        await held[0].__anext__()
        await asyncio.to_thread(int)
        return 'started'
    return f'closed={closed} shut={loop._executor_shutdown_called}'
before = []
after = []
",
        |rt, worker, on, route| {
            assert_eq!(ask(rt, worker, on, route).as_deref(), Ok("started"));
            finish(rt, worker, on);
            let body = ask(rt, worker, on, route).expect("the handler's value");
            assert!(body.contains("closed=[True] shut=True"), "{body}");
        },
    );
}

/// Returns the thread's async generator hooks, as text.
fn asyncgen_hooks(worker: &mut SubInterpreterWorker<SyncRunner>) -> String {
    worker
        .with_gil(|_, py| {
            py.import("sys")
                .and_then(|sys| sys.call_method0("get_asyncgen_hooks"))
                .map(|hooks| hooks.to_string())
                .expect("sys.get_asyncgen_hooks")
        })
        .expect("no panic reading the hooks")
}

#[test]
fn closing_the_loop_restores_the_asyncgen_hooks() {
    on_a_thread(
        c"handler = lambda req: 'ok'\nbefore = []\nafter = []\n",
        |_, worker, _| {
            register_worker_modules(worker);
            worker
                .with_gil(|_, py| {
                    py.run(
                        c"import sys\ndef first(agen): pass\nsys.set_asyncgen_hooks(firstiter=first)",
                        None,
                        None,
                    )
                })
                .expect("no panic")
                .expect("the hook is set");
            let before = asyncgen_hooks(worker);
            let on = worker.open_event_loop().expect("the loop opens");
            let open = asyncgen_hooks(worker);
            assert!(open.contains("_asyncgen_firstiter_hook"), "{open}");
            worker.close_event_loop(on);
            assert_eq!(asyncgen_hooks(worker), before);
        },
    );
}

#[test]
fn an_unclosed_loop_dropped_after_a_failed_open_prints_nothing() {
    on_a_thread(
        c"handler = lambda req: 'ok'\ndef note(req): pass\nbefore = [note]\nafter = []\n",
        |_, worker, _| {
            register_module(worker, "mipyre._worker.tpc_loop", TPC_LOOP);
            register_module(
                worker,
                "mipyre._worker.async_requests",
                "def job_runner(*args):\n    raise RuntimeError('no job runner here')\n",
            );
            worker
                .with_gil(|_, py| py.run(c"import io, sys\nsys.stderr = io.StringIO()", None, None))
                .expect("no panic")
                .expect("stderr is captured");
            let refused = worker
                .open_event_loop()
                .err()
                .expect("the open fails after the loop is made");
            assert!(refused.to_string().contains("no job runner here"), "{refused}");
            let printed: String = worker
                .with_gil(|_, py| {
                    let names = pyo3::types::PyDict::new(py);
                    py.run(
                        c"import gc, sys\ngc.collect()\nprinted = sys.stderr.getvalue()",
                        None,
                        Some(&names),
                    )?;
                    names.get_item("printed")?.expect("the source sets printed").extract()
                })
                .expect("no panic")
                .expect("stderr's text");
            assert_eq!(printed, "");
        },
    );
}

#[test]
fn a_loop_whose_init_raised_is_released_and_prints_nothing() {
    on_a_thread(
        c"handler = lambda req: 'ok'\nbefore = []\nafter = []\n",
        |_, worker, _| {
            register_worker_modules(worker);
            register_module(
            worker,
            "mipyre._worker.tpc_loop",
            "from asyncio import unix_events\n\
             made = []\n\
             def make_loop_class(native):\n\
             \x20   def __init__(self):\n\
             \x20       made.append(self)\n\
             \x20       raise RuntimeError('no init here')\n\
             \x20   return type('TpcEventLoop', (native, unix_events._UnixSelectorEventLoop), {'__init__': __init__})\n",
        );
            worker
                .with_gil(|_, py| py.run(c"import io, sys\nsys.stderr = io.StringIO()", None, None))
                .expect("no panic")
                .expect("stderr is captured");
            let refused = worker.open_event_loop().err().expect("the open fails in __init__");
            assert!(refused.to_string().contains("no init here"), "{refused}");
            let (closed, printed): (bool, String) = worker
                .with_gil(|_, py| {
                    let names = pyo3::types::PyDict::new(py);
                    py.run(
                        c"import gc, sys\n\
                      made = sys.modules['mipyre._worker.tpc_loop'].made\n\
                      closed = made.pop().is_closed()\n\
                      gc.collect()\n\
                      printed = sys.stderr.getvalue()",
                        None,
                        Some(&names),
                    )?;
                    let closed = names.get_item("closed")?.expect("the source sets closed").extract()?;
                    let printed = names.get_item("printed")?.expect("the source sets printed").extract()?;
                    Ok::<_, PyErr>((closed, printed))
                })
                .expect("no panic")
                .expect("the loop and stderr's text");
            assert!(closed);
            assert_eq!(printed, "");
        },
    );
}

/// Returns `BENCH_N`, or 16 for the ordinary test run.
fn bench_n() -> u64 {
    std::env::var("BENCH_N").map_or(16, |n| n.parse().expect("BENCH_N: a number of requests"))
}

/// Answers `BENCH_N` requests of `app` one after another through the loop. Run under `perf stat`
/// and subtract the `BENCH_N=0` run, which is Python's start-up and the harness:
///
///   BENCH_N=20000 perf stat -x, -e instructions:u -- target/release/deps/mipyre_runtime-* \
///       --exact worker::request_loop::tests::async_requests_answered_in_one_step
fn answer_requests(app: &'static CStr) {
    on_a_loop(app, |rt, worker, on, route| {
        for _ in 0..bench_n() {
            let mut reply = worker
                .start_task(on, route, get_root(), false)
                .expect("the request starts");
            drive(rt, worker, on, |on| on.live() == LiveTasks { count: 0, large: 0 });
            let body = answered(&mut reply).expect("the handler's value");
            std::hint::black_box(body);
        }
    });
}

#[test]
fn async_requests_answered_in_one_step() {
    answer_requests(c"async def handler(req):\n    return 'ok'\nbefore = []\nafter = []\n");
}

/// The loop of [`async_requests_answered_in_one_step`] with one `asyncio.sleep` on the loop's
/// timer: the difference is a sleep's cost (a loop `Future`, its `Due::Resolve`, one more step).
#[test]
fn async_requests_answered_after_a_sleep() {
    answer_requests(
        c"import asyncio\nasync def handler(req):\n    await asyncio.sleep(1e-6)\n    return 'ok'\nbefore = []\nafter = []\n",
    );
}

#[test]
fn fd_callbacks_run_in_the_context_they_were_added_in_and_what_they_set_stays_there() {
    on_a_loop(
        c"import asyncio, socket, contextvars
rid = contextvars.ContextVar('rid', default='unset')
leak = contextvars.ContextVar('leak', default='clean')
round = [0]
async def handler(req):
    loop = asyncio.get_running_loop()
    round[0] += 1
    rid.set(f'request-{round[0]}')
    a, b = socket.socketpair()
    seen = loop.create_future()
    def cb():
        b.recv(1)
        loop.remove_reader(b)
        before = leak.get()
        leak.set(f'set-by-request-{round[0]}')
        seen.set_result((rid.get(), before))
    loop.add_reader(b, cb)
    a.send(b'x')
    got, before = await asyncio.wait_for(seen, 2)
    a.close(); b.close()
    return f'rid-in-callback={got} leak-seen-on-entry={before}'
before = []
after = []
",
        |rt, worker, on, route| {
            let first = ask(rt, worker, on, route).expect("the first request's value");
            let second = ask(rt, worker, on, route).expect("the second request's value");
            assert_eq!(first, "rid-in-callback=request-1 leak-seen-on-entry=clean");
            assert_eq!(second, "rid-in-callback=request-2 leak-seen-on-entry=clean");
        },
    );
}

#[test]
fn a_server_started_in_one_request_hands_its_connections_that_requests_context() {
    on_a_loop(
        c"import asyncio, socket, contextvars
rid = contextvars.ContextVar('rid', default='unset')
leak = contextvars.ContextVar('leak', default='clean')
n = [0]
async def handler(req):
    loop = asyncio.get_running_loop()
    n[0] += 1
    rid.set(f'request-{n[0]}')
    if n[0] == 1:
        a, b = socket.socketpair()
        seen = loop.create_future()
        def cb():
            b.recv(1); loop.remove_reader(b)
            leak.set('user-of-request-1')
            seen.set_result(None)
        loop.add_reader(b, cb)
        a.send(b'x')
        await seen
        a.close(); b.close()
        return 'set'
    got = loop.create_future()
    async def on_conn(reader, writer):
        got.set_result((rid.get(), leak.get()))
        writer.close()
    server = await asyncio.start_server(on_conn, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    r, w = await asyncio.open_connection('127.0.0.1', port)
    seen_rid, seen_leak = await asyncio.wait_for(got, 2)
    w.close(); server.close(); await server.wait_closed()
    return f'connection-task rid={seen_rid} leak={seen_leak}'
before = []
after = []
",
        |rt, worker, on, route| {
            assert_eq!(ask(rt, worker, on, route).as_deref(), Ok("set"));
            let body = ask(rt, worker, on, route).expect("the second request's value");
            assert_eq!(body, "connection-task rid=request-2 leak=clean");
        },
    );
}

#[test]
fn call_soon_callbacks_run_in_the_context_they_were_scheduled_in() {
    on_a_loop(
        c"import asyncio, contextvars
rid = contextvars.ContextVar('rid', default='unset')
async def handler(req):
    loop = asyncio.get_running_loop()
    rid.set('request-1')
    seen = loop.create_future()
    loop.call_soon(lambda: seen.set_result(rid.get()))
    return f'rid-in-call_soon={await seen}'
before = []
after = []
",
        |rt, worker, on, route| {
            let body = ask(rt, worker, on, route).expect("the handler's value");
            assert_eq!(body, "rid-in-call_soon=request-1");
        },
    );
}

/// A handler cancels the reader of an fd that also has a writer and closes the fd while a `dup`
/// keeps its file open: the kernel then refuses to change that registration, once in each of three
/// turns. On macOS the cancel is a kqueue delete on a closed fd, which counts as already gone, so
/// nothing is refused.
#[cfg(target_os = "linux")]
#[test]
fn a_refused_change_to_one_fd_neither_fails_the_turn_nor_retires_the_worker_and_is_reported() {
    on_a_loop(
        c"import asyncio, socket
keep = []
async def handler(req):
    loop = asyncio.get_running_loop()
    for _ in range(3):
        a, b = socket.socketpair()
        keep.extend([a, b.dup()])
        a.send(b'x')
        h = loop._add_reader(b.fileno(), lambda: None)
        loop._add_writer(b.fileno(), lambda: None)
        h.cancel()
        b.close()
        await asyncio.sleep(0)
    await asyncio.sleep(0.05)
    return 'survived'
before = []
after = []
",
        |rt, worker, on, route| {
            let logged = crate::testkit::captured_log(|| {
                assert_eq!(ask(rt, worker, on, route).as_deref(), Ok("survived"));
            });
            assert!(logged.contains("the poller refused to stop watching fd"), "{logged}");
            assert!(
                !logged.contains("a turn of the TPC thread's event loop failed"),
                "{logged}"
            );
        },
    );
}
