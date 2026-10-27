//! Counts the Python objects one request makes on a worker, by type, for each path.
//!
//! CPython's reference tracer (`PyRefTracer_SetTracer`) is called for every object as it is made,
//! a freelist's reused one included, so the count is of objects made, not of allocator calls; a
//! `bytes` or `str` CPython resizes in place is counted again. Only the thread with an open count
//! counts, each case runs in a run of the test binary of its own ([`alone`]), and every case
//! asserts that the requests it counts all made the same objects, and that those are its
//! baseline: one object more fails, as one instruction more per request does.
//!
//! The sync paths go through `serve_inline`, the TPC thread's call. The async path runs the
//! engine's request coroutine on its own loop, fed from a real inbox; it leaves out the fetcher
//! thread's wake-up of the loop, which depends on how many requests one wake-up finds.
//!
//! `cargo test -p mipyre_pyext --lib objects_per_request -- --nocapture --test-threads 1`
//! prints each path's objects. The cases' JSON responses need `isojson`, from the venv
//! `PYO3_PYTHON` names or this checkout's `.venv-gate` / `.venv` ([`import_isojson`]).

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::ffi::{c_int, c_void, CStr};
use std::sync::Once;

use pyo3::ffi;
use pyo3::prelude::*;

use super::sync_runner::tests::sync_worker;
use crate::exchange::json::tests::import_isojson;
use crate::exchange::request::Request;
use crate::exchange::RequestBody;
use crate::request_context::tests::register_awaitable_module;
use crate::worker_api::AsyncInbox;

// pyo3-ffi has no binding for `PyRefTracer_SetTracer` (`cpython/object.h`).
unsafe extern "C" {
    fn PyRefTracer_SetTracer(
        tracer: Option<unsafe extern "C" fn(*mut ffi::PyObject, c_int, *mut c_void) -> c_int>,
        data: *mut c_void,
    ) -> c_int;
}

/// It mirrors CPython's `PyRefTracer_CREATE`.
const CREATE: c_int = 0;

thread_local! {
    /// It is `Some` while a count is open on this thread, keyed by type address.
    static MADE: RefCell<Option<HashMap<usize, (String, u64)>>> = const { RefCell::new(None) };
}

/// Counts `obj` by its type when a count is open on this thread; it makes no Python object and
/// never unwinds.
unsafe extern "C" fn count_made(obj: *mut ffi::PyObject, event: c_int, _: *mut c_void) -> c_int {
    if event != CREATE {
        return 0;
    }
    let counted = MADE.try_with(|made| {
        let Ok(mut made) = made.try_borrow_mut() else {
            return;
        };
        let Some(made) = made.as_mut() else {
            return;
        };
        // SAFETY: `obj` is being made, so its type is set and alive, and `tp_name` is a
        // NUL-terminated string the type owns.
        let ty = unsafe { ffi::Py_TYPE(obj) };
        made.entry(ty as usize)
            .or_insert_with(|| {
                // SAFETY: The same holds as above.
                let name = unsafe { CStr::from_ptr((*ty).tp_name) };
                (name.to_string_lossy().into_owned(), 0)
            })
            .1 += 1;
    });
    // A thread whose locals are gone has no count open.
    if counted.is_err() {
        return 0;
    }
    0
}

static TRACER: Once = Once::new();

/// Installs the tracer once for the process; it stays, and with no count open it only reads a
/// thread local.
fn install_tracer(_py: Python<'_>) {
    TRACER.call_once(|| {
        // SAFETY: The thread is attached (`_py`), and `count_made` matches `PyRefTracer` and
        // takes no data.
        let set = unsafe { PyRefTracer_SetTracer(Some(count_made), std::ptr::null_mut()) };
        assert_eq!(set, 0, "PyRefTracer_SetTracer");
    });
}

/// Returns the Python objects made on this thread while `f` runs, by type name.
fn objects_made<T>(f: impl FnOnce() -> T) -> (T, BTreeMap<String, u64>) {
    MADE.with(|made| *made.borrow_mut() = Some(HashMap::new()));
    let ran = f();
    let made = MADE
        .with(|made| made.borrow_mut().take())
        .expect("the count opened above");
    let mut by_name = BTreeMap::new();
    for (name, n) in made.into_values() {
        *by_name.entry(name).or_insert(0) += n;
    }
    (ran, by_name)
}

/// Builds `GET /<value>/<value>…`, one segment per path param, with the headers a typical client sends.
fn request(params: &[(&str, &str)]) -> Request {
    let mut path = String::new();
    let mut located = mipyre_http::plan::route::Params::new();
    for (name, value) in params {
        path.push('/');
        located.push(name, path.len()..path.len() + value.len());
        path.push_str(value);
    }
    if path.is_empty() {
        path.push('/');
    }
    let (parts, ()) = hyper::Request::get(path)
        .header("host", "localhost:8000")
        .header("user-agent", "wrk/4.2.0")
        .header("accept", "*/*")
        .header("x-request-id", "abc123")
        .body(())
        .expect("a valid request")
        .into_parts();
    let head = mipyre_http::io::request_head::RequestHead {
        parts,
        params: located,
        client: mipyre_transport::driver::Peer::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
        request_id: mipyre_http::plan::request_id::RequestId::mint(),
    };
    Request::new(head, RequestBody::Buffered(Default::default()))
}

/// The first requests make what a worker keeps (the reused request context, interned names, the
/// loop's first timer), so they are not counted.
const WARM_UP: usize = 3;
const COUNTED: usize = 4;

/// Returns the objects one request on `app` makes through `serve_inline`; each of `COUNTED`
/// requests must make the same objects and be answered with `body`.
fn sync_path(
    app: &'static CStr,
    params: &'static [(&'static str, &'static str)],
    body: &'static [u8],
) -> BTreeMap<String, u64> {
    Python::initialize();
    std::thread::Builder::new()
        .name("objects-per-request".into())
        .stack_size(16 << 20)
        .spawn(move || {
            let (mut worker, route) = sync_worker(app);
            worker
                .with_gil(|_, py| {
                    install_tracer(py);
                    import_isojson(py);
                    register_awaitable_module(py);
                })
                .expect("the tracer installed, isojson and mipyre._worker.awaitable importable");
            let mut serve = || {
                let served = worker.serve_inline(route, request(params));
                let answer = served.response.map(|r| r.body).map_err(|e| e.to_string());
                assert_eq!(answer.as_deref(), Ok(body), "the route answered");
            };
            for _ in 0..WARM_UP {
                serve();
            }
            let counts: Vec<_> = (0..COUNTED).map(|_| objects_made(&mut serve).1).collect();
            worker.end_on_own_thread();
            assert!(
                counts.windows(2).all(|w| w[0] == w[1]),
                "every request makes the same objects: {counts:#?}"
            );
            counts.into_iter().next().expect("COUNTED > 0")
        })
        .expect("spawn the worker thread")
        .join()
        .expect("the worker thread's assertions hold")
}

/// It is the response body of `PAYLOAD`, each app's first line.
const PAYLOAD_JSON: &[u8] = br#"{"id":1,"name":"mipyre","tags":["a","b","c"],"price":12.5,"ok":true}"#;

/// It is set in the run of the test binary that runs one test alone.
const ALONE: &str = "OBJECTS_PER_REQUEST_ALONE";

/// Runs `body` in a run of this test binary for `test` alone. pyo3 caches per thread where its
/// per-interpreter values are until any interpreter is added or dropped, and the lookup after that
/// makes a `str`; tests beside this one start interpreters at any time, so in a shared process a
/// request now and then makes one object more. A failure there fails this test with that run's
/// output.
fn alone(test: &str, body: impl FnOnce()) {
    if std::env::var_os(ALONE).is_some() {
        return body();
    }
    // libtest names a test by its path inside the crate.
    let name = test.split_once("::").map_or(test, |(_, path)| path);
    let binary = std::env::current_exe().expect("the test binary's path");
    let run = std::process::Command::new(&binary)
        .args([name, "--exact", "--test-threads=1", "--nocapture"])
        .env(ALONE, "1")
        .output()
        .expect("the test binary runs again");
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    // Requiring "1 passed" makes a misspelt name fail instead of pass.
    assert!(
        run.status.success() && stdout.contains("test result: ok. 1 passed"),
        "{name} alone ({}): {}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        binary.display(),
        run.status,
    );
    eprint!("{stderr}");
}

/// Prints the objects one request on `path` makes and asserts they are `baseline`, by type.
fn assert_made(path: &str, made: &BTreeMap<String, u64>, baseline: &[(&str, u64)]) {
    let total: u64 = made.values().sum();
    eprintln!("{path}: {total} objects per request");
    for (ty, n) in made {
        eprintln!("    {n:>3}  {ty}");
    }
    let baseline: BTreeMap<String, u64> = baseline.iter().map(|&(ty, n)| (ty.to_owned(), n)).collect();
    assert_eq!(
        made, &baseline,
        "{path}: the objects one request makes, against its baseline"
    );
}

/// Builds an app's source for `sync_worker`: `PAYLOAD`, the JSON object the bench's json paths
/// answer, made once so a handler returning it makes nothing, then `lines`.
macro_rules! app {
    ($($line:literal),* $(,)?) => {
        match CStr::from_bytes_with_nul(concat!(
            "PAYLOAD = {'id': 1, 'name': 'mipyre', 'tags': ['a', 'b', 'c'], 'price': 12.5, 'ok': True}\n",
            $($line, "\n",)* "\0"
        ).as_bytes()) {
            Ok(app) => app,
            Err(_) => panic!("an app's source has no NUL inside"),
        }
    };
}

#[test]
fn json() {
    alone(concat!(module_path!(), "::json"), || {
        let made = sync_path(
            app!("def handler(req): return PAYLOAD", "before = []", "after = []"),
            &[],
            PAYLOAD_JSON,
        );
        assert_made(
            "json (def handler returns a dict)",
            &made,
            &[("bytes", 3), ("mipyre.engine.Request", 1)],
        );
    });
}

#[test]
fn plaintext() {
    alone(concat!(module_path!(), "::plaintext"), || {
        let made = sync_path(
            app!("def handler(req): return 'Hello, World!'", "before = []", "after = []"),
            &[],
            b"Hello, World!",
        );
        assert_made(
            "plaintext (def handler returns a str)",
            &made,
            &[("mipyre.engine.Request", 1)],
        );
    });
}

#[test]
fn path_param() {
    alone(concat!(module_path!(), "::path_param"), || {
        let made = sync_path(
            app!("def handler(req): return req.params['id']", "before = []", "after = []"),
            &[("id", "42")],
            b"42",
        );
        assert_made(
            "path param (req.params['id'])",
            &made,
            &[("dict", 1), ("mipyre.engine.Request", 1), ("str", 2)],
        );
    });
}

#[test]
fn one_header() {
    alone(concat!(module_path!(), "::one_header"), || {
        let made = sync_path(
            app!(
                "def handler(req): return req.headers.get('user-agent')",
                "before = []",
                "after = []"
            ),
            &[],
            b"wrk/4.2.0",
        );
        assert_made(
            "one header (req.headers.get once)",
            &made,
            &[("mipyre.engine.Headers", 1), ("mipyre.engine.Request", 1), ("str", 1)],
        );
    });
}

#[test]
fn three_headers() {
    alone(concat!(module_path!(), "::three_headers"), || {
        let made = sync_path(
            app!(
                "def handler(req):",
                "    return req.headers.get('user-agent') + req.headers.get('accept') + req.headers['x-request-id']",
                "before = []",
                "after = []",
            ),
            &[],
            b"wrk/4.2.0*/*abc123",
        );
        assert_made(
            "three headers (req.headers three times)",
            &made,
            &[("mipyre.engine.Headers", 3), ("mipyre.engine.Request", 1), ("str", 5)],
        );
    });
}

#[test]
fn sync_hooks() {
    alone(concat!(module_path!(), "::sync_hooks"), || {
        let made = sync_path(
            app!(
                "def handler(req): return PAYLOAD",
                "def look(req): return None",
                "def look_after(req, resp): return None",
                "before = [look]",
                "after = [look_after, look_after]",
            ),
            &[],
            PAYLOAD_JSON,
        );
        assert_made(
            "hooks (json; one before, two after, all return None)",
            &made,
            &[
                ("bytes", 3),
                ("mipyre.engine.Request", 1),
                ("mipyre.engine.Response", 1),
                ("str", 1),
            ],
        );
    });
}

#[test]
fn async_before_hook() {
    alone(concat!(module_path!(), "::async_before_hook"), || {
        let made = sync_path(
            app!(
                "def handler(req): return PAYLOAD",
                "async def check(req): return None",
                "before = [check]",
                "after = []",
            ),
            &[],
            PAYLOAD_JSON,
        );
        assert_made(
            "async before hook (json; async def before hook returns None)",
            &made,
            &[
                ("_asyncio.Task", 1),
                ("asyncgen_hooks", 1),
                ("bytes", 3),
                ("coroutine", 1),
                ("dict", 2),
                ("int", 1),
                ("method", 2),
                ("mipyre.engine.Request", 1),
                ("str", 2),
                ("tuple", 2),
                ("tuple_iterator", 1),
            ],
        );
    });
}

/// It sets up the engine's request coroutine and loop as `_worker/async_engine.py` does, minus the doorbell:
/// `serve(inbox)` takes every queued job as the doorbell's reader does and starts each as an eager Task, inside
/// one run of the loop.
const ASYNC_ENGINE: &CStr = c"
import asyncio

def engine(requests, handlers, before, after, to_response, send, fail, take):
    loop = asyncio.new_event_loop()
    run = requests.job_runner(handlers, before, after, to_response, send, fail)

    async def window(inbox):
        for job in take(inbox):
            asyncio.Task(run(job), loop=loop, eager_start=True)

    def serve(inbox):
        loop.run_until_complete(window(inbox))

    return serve
";

/// The loop's fixed cost per run is taken out by difference: windows of `n`, `2n` and `3n`
/// requests give `(2n − n) / n` per request, which must equal `(3n − 2n) / n`.
#[test]
fn async_engine() {
    alone(concat!(module_path!(), "::async_engine"), || {
        const N: usize = 8;
        Python::initialize();
        let made = std::thread::Builder::new()
            .name("objects-per-request-async".into())
            .stack_size(16 << 20)
            .spawn(|| {
                let (mut worker, route) = sync_worker(app!(
                    "async def handler(req): return PAYLOAD",
                    "before = []",
                    "after = []"
                ));
                let windows = {
                    worker.with_gil(|runner, py| {
                        install_tracer(py);
                        import_isojson(py);
                        let handler = runner.routes[route.index()]
                            .handler
                            .as_ref()
                            .expect("the route's handler")
                            .bind(py)
                            .clone();
                        let requests = pyo3::types::PyModule::from_code(
                            py,
                            &std::ffi::CString::new(include_str!(
                                "../../../../python/mipyre/_worker/async_requests.py"
                            ))
                            .expect("no NUL"),
                            c"async_requests.py",
                            c"async_requests",
                        )
                        .expect("async_requests runs");
                        let harness = pyo3::types::PyModule::from_code(
                            py,
                            ASYNC_ENGINE,
                            c"async_engine_harness.py",
                            c"async_engine_harness",
                        )
                        .expect("the harness runs");
                        let serve = harness
                            .getattr("engine")
                            .and_then(|engine| {
                                engine.call1((
                                    requests,
                                    vec![handler],
                                    Vec::<Py<PyAny>>::new(),
                                    Vec::<Py<PyAny>>::new(),
                                    wrap_pyfunction!(crate::_worker_to_response, py)?,
                                    wrap_pyfunction!(crate::_worker_send, py)?,
                                    wrap_pyfunction!(crate::_worker_fail, py)?,
                                    wrap_pyfunction!(crate::_worker_take, py)?,
                                ))
                            })
                            .expect("the engine is set up");
                        let window = |n: usize| {
                            let (queue, inbox) = crate::engine_queue::one_engine_queue(n).expect("a doorbell");
                            let mut answers = Vec::new();
                            for _ in 0..n {
                                let (reply, answer) = tokio::sync::oneshot::channel();
                                queue
                                    .submit(crate::engine_queue::EngineJob {
                                        handler: route.index(),
                                        request: request(&[]),
                                        reply: crate::engine_queue::EngineReply::Pool(reply),
                                    })
                                    .expect("the queue holds the whole window");
                                answers.push(answer);
                            }
                            let inbox = Bound::new(py, AsyncInbox(inbox)).expect("the inbox");
                            let (served, made) = objects_made(|| serve.call1((&inbox,)).map(drop));
                            served.expect("the engine served the window");
                            drop(queue);
                            for mut answer in answers {
                                let body = answer
                                    .try_recv()
                                    .expect("answered")
                                    .map(|r| r.body)
                                    .map_err(|e| format!("{e:?}"));
                                assert_eq!(body.as_deref(), Ok(PAYLOAD_JSON), "the request was answered");
                            }
                            made
                        };
                        // The first window makes what the loop keeps, so it is not counted.
                        window(N);
                        [window(N), window(2 * N), window(3 * N)]
                    })
                }
                .expect("the worker ran the windows");
                worker.end_on_own_thread();
                windows
            })
            .expect("spawn the worker thread")
            .join()
            .expect("the worker thread's assertions hold");
        let [one, two, three] = made;
        let per_request = |from: &BTreeMap<String, u64>, to: &BTreeMap<String, u64>| -> BTreeMap<String, u64> {
            to.iter()
                .filter_map(|(ty, &n)| {
                    let extra = n - from.get(ty).copied().unwrap_or(0);
                    assert_eq!(extra % N as u64, 0, "{ty}: {extra} more over {N} requests");
                    (extra > 0).then(|| (ty.clone(), extra / N as u64))
                })
                .collect()
        };
        let first = per_request(&one, &two);
        assert_eq!(first, per_request(&two, &three), "every request makes the same objects");
        assert_made(
            "async engine (async def handler returns a dict, no hooks)",
            &first,
            &[
                ("_asyncio.Task", 1),
                ("_contextvars.Context", 1),
                ("bytes", 3),
                ("coroutine", 2),
                ("dict", 1),
                ("list_iterator", 1),
                ("mipyre.engine.Request", 1),
                ("mipyre.engine._AsyncJob", 1),
                ("tuple", 1),
            ],
        );
    });
}
