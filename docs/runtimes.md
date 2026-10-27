# Runtimes: where a route runs

Every route runs in one of four runtimes. Pick one per route, per `Router`, or for the whole app.

| `runtime=` | Where the handler runs | Parallel? | Use it for |
|---|---|---|---|
| `"subinterp"` (default) | an own-GIL sub-interpreter per core, inside the server process | yes, one GIL per core | pure Python, the standard library, extensions that declare sub-interpreter support. The fastest |
| `"process"` | worker processes beside the server, reached through shared memory | yes, one process per worker | C extensions that don't load in a sub-interpreter: numpy, pandas, polars, pyarrow, scipy, scikit-learn, pydantic |
| `"main"` | the server's main interpreter | no, one GIL | a few stateful routes: streaming responses (`Stream`), streaming request bodies (`body=BodyStream`), state that must live in one place |
| `"isolated"` | the same sub-interpreters as `subinterp`, each with a private copy of every extension that can't load in one | yes | a compatibility option when `process` doesn't fit yet (see [below](#isolated-a-compatibility-option)) |

`runtime=` takes the name or the member of `mipyre.Runtime` (`Runtime.PROCESS`); a name that is neither is a `RuntimeDeclarationError` when the route is declared, listing the four. `app.routes` lists each route with the runtime it runs on.

Which libraries load in a sub-interpreter: [compatibility.md](compatibility.md#which-libraries-load-in-a-sub-interpreter).

## Choosing

1. Start with `subinterp`, the default. If every library a route imports loads in a sub-interpreter, keep it there.
2. A route that needs numpy, pandas, polars, pydantic or another extension that doesn't load in a sub-interpreter: use `process`.
3. A route that `process` can't serve (an `async def` handler, a streaming request or response, `app.state`, the PostgreSQL pool; see [the limits of `process`](#process)): use `main`, or keep the library out of that route.
4. Use `isolated` only when a library must be used in a sub-interpreter route and moving the route to `process` isn't possible yet.

## Declaring a runtime

```python
# app.py
from mipyre import Mipyre, ProcessLanes

app = Mipyre(
    runtime="subinterp",                  # the app's default
    process=ProcessLanes(workers=2, timeout=10.0),
)
app.include_router("analytics:router")   # a router by import string; prefix="/x" puts its paths under /x


@app.get("/health", runtime="main")       # one route
def health(req):
    return {"ok": True}
```

```python
# analytics.py: imported by the process workers and the server, never by a sub-interpreter
import numpy as np
from mipyre import Router

router = Router(runtime="process")


@router.get("/report")
def report(req):
    return {"mean": float(np.mean([1.0, 2.0, 3.0]))}
```

```bash
mipyre run app:app
```

A route's runtime is, most specific first: its decorator's `runtime=`, its `Router`'s, `Mipyre(runtime=...)`; else `"subinterp"`. It is the code's alone: no `mipyre run` flag, environment variable or `pyproject.toml` key changes it. An app that one deployment serves on another runtime reads a setting of its own and passes it in:

```python
# app.py
import os

from mipyre import Mipyre

app = Mipyre(runtime=os.environ.get("APP_RUNTIME", "subinterp"))


@app.get("/")
def index(req):
    return {"ok": True}
```

```bash
APP_RUNTIME=main mipyre run app:app
```

`MIPYRE_RUNTIME` and `mipyre run --runtime` are not settings: a deployment that still sets `MIPYRE_RUNTIME` stops at startup, naming it.

### What each runtime imports

The server imports the app module and every router. A sub-interpreter imports the app module and the routers that have sub-interpreter routes. A process worker imports the app module and the routers that have process routes. So a module with sub-interpreter routes must not import, at its top, a library a sub-interpreter can't load: put those routes in a `Router(runtime="process")` module, as above, or import the library inside the process route's function. An app with no route on a sub-interpreter starts no sub-interpreter at all.

## `subinterp`

One own-GIL sub-interpreter per server thread, and one thread per physical core (per performance core on Apple silicon), set with `threads=` / `--threads` / `MIPYRE_THREADS`. A request is accepted, parsed and answered on the same core. With `MIPYRE_TPC=0` the server is instead a multi-thread one: I/O threads (`threads=`) hand requests to a pool of sub-interpreters (`subinterpreters=` / `--subinterpreters` / `MIPYRE_SUBINTERPRETERS`, by default one per logical CPU).

Limits:

- Only extensions that declare sub-interpreter support load. Others raise CPython's `ImportError` (`module X does not support loading in subinterpreters`), and the server's start error names the library and the line that imported it.
- Module-level state is per sub-interpreter: each worker has its own copy of every global. State shared by all workers goes in `app.state`.
- A readiness check can't run here (`HealthProbes(app, runtime=...)` takes `"main"` or `"process"`).

## `process`

Worker processes beside the server, one per physical core by default. The server forwards each request to a worker through shared memory and writes the worker's answer back. Every response feature (compression, CORS, the access log) applies as on any route.

The workers start from a template process. `python app.py`, `mipyre dev`, `TestServer` and `TestClient` start it after they import the app, as a new interpreter on every platform, so the threads the app's imports started (numpy's thread pool, a database pool) don't reach it. `mipyre run` starts it before it imports the app, from a process that runs no other thread yet: on Linux it forks the template from itself. Each way serves process routes; tests do too ([testing.md](testing.md)).

If code that runs before the app, such as a `sitecustomize` or a `.pth` file, started a thread, `mipyre run` on Linux stops with `ThreadedTemplateError`, which names the threads and says what to do.

### Settings

`ProcessLanes(start=..., workers=..., timeout=..., queue_per_worker=..., slot_bytes=...)` in code. A deployment value overrides it: a `mipyre run --process-*` flag, else a `MIPYRE_PROCESS_*` variable, else `[tool.mipyre.process]` in `pyproject.toml`; a setting nobody gives takes its default. The start prints each value the deployment gave. The full table is in [configuration.md](configuration.md#process-workers).

`start` says how a worker gets the app:

- `"import"` (default): the worker starts without the app and imports it itself. Safe whatever the app's libraries do at import.
- `"fork"`: a template imports the app once and forks each worker from it. Replacing a worker is faster and uses less memory, for apps that replace workers often. The template must be single-threaded when it forks: if the app's imports leave threads running, the start fails and names them.

Before a worker (or a `"fork"` template) imports the app, mipyre sets `OPENBLAS_NUM_THREADS`, `OMP_NUM_THREADS`, `MKL_NUM_THREADS`, `VECLIB_MAXIMUM_THREADS` and `POLARS_MAX_THREADS` to `1` unless you set them: N workers, each with a thread pool sized to every core, would oversubscribe the machine. The start line says so:

```text
  Process lanes: 2 workers (template pid 32986, timeout 10000 ms, queue 128/worker; BLAS (numpy, scipy): 1 thread per worker; polars: 1 thread per worker)
```

### What the client gets

| Case | Answer |
|---|---|
| the handler returns | its response |
| the handler raises | `500`; the exception and traceback go to the server's log with the request id |
| the worker dies (crash, `os._exit`, `SIGKILL`) | `502` for the request it was running and for the requests handed to it that it hadn't taken; none of them runs on its replacement. The death is logged once and counted (`mipyre_workers_died_total{runtime="process"}`) |
| the worker writes what a correct worker never writes into its lane (a counter or a length out of range, more wake-ups than answers) | it is killed and replaced under the lane's crash backoff; `502` for the requests in its lane. Other lanes are not touched |
| the handler runs past the timeout | the worker is killed and replaced; `504` |
| every worker and queue is full | `503`, never a wait |
| the request or the response is larger than `slot_bytes` | `413` / `500`, naming the sizes |
| a worker can't import the app | the server doesn't start; the error carries the worker's traceback |

### Limits

These are refused with an error, not quietly run somewhere else:

- `async def` handlers: refused when the route is registered.
- `body=BodyStream` (streaming request bodies): refused when the route is registered.
- returning a `Stream` (Server-Sent Events): the request is answered `500` and the log says to use `runtime='main'`.
- `app.state` and `PgPool`: using either in a process handler raises `RuntimeError`, answered `500`. A worker is a separate process, so its copy of the server's state would silently diverge.

WebSocket handlers always run on the main interpreter, whatever the app's runtime.

Requests and responses are limited to `slot_bytes`, 256 KiB by default.

## `main`

The server's main interpreter: one GIL for every `main` route. Use it for the few routes that need one place: `Stream` responses, `body=BodyStream` bodies, objects that must not be copied per worker, libraries that can't load in a sub-interpreter when `process` doesn't fit.

On the default, thread-per-core server, the `def` handlers of `main` routes run on 4 bridge threads that share the one GIL (an I/O-bound handler releases it for the others), with a queue of 64 requests in front of them; past that the server answers `503` at once. On the multi-thread server (`MIPYRE_TPC=0`) they run on threads of the same kind, up to 512, started as requests find none free and retired after 10 s idle, with 16 queued requests per thread. `MIPYRE_BRIDGE_WORKERS` and `MIPYRE_BRIDGE_CAPACITY` change both ([configuration.md](configuration.md#engine-tuning)). `async def` handlers of `main` routes run on one event loop of the main interpreter: one that awaits holds no thread, so any number of them can wait at once.

`Mipyre(runtime="main")` serves every route on the main interpreter and starts no sub-interpreter; so does any app none of whose routes runs on a sub-interpreter. CPU-bound Python on `main` doesn't scale past one core.

## `isolated`: a compatibility option

Not the recommended way. Use it when a library you need in sub-interpreter routes can't load there and moving those routes to `process` isn't an option yet. Libraries like numpy, pandas or pyarrow belong on `process`.

When any route of the app is `isolated`, every sub-interpreter of the app finds packages through mipyre's finder. The first time a worker imports a package, the finder reads the package's extension files, before any of them loads; a package with a file that can't load in a sub-interpreter is copied (the whole package, copy-on-write where the file system allows) into a directory of that worker's own, and loaded from there with CPython's multi-interpreter check switched off for that load. A copy that fails raises `IsolationError` with the reason, on that worker's imports of the package. The libraries are found as they are imported; at startup the server prints a line naming them and the size of each copy:

```text
  runtime='isolated' (a compatibility option, not recommended): per-worker copies of numpy (21.5 MiB: .../numpy/_core/_operand_flag_tests...so is single-phase (...)), in each of 2 sub-interpreters, loaded with CPython's multi-interpreter check off; for numpy, runtime='process' is the better fit (worker processes, ...
```

Risks and costs:

- **Memory**: one copy of each such library per worker, on disk and in memory.
- **Process-wide state**: each copy believes it is alone in the process (signal handlers, `atexit`, environment variables, thread pools, file locks); copies can step on each other.
- **Untested by the libraries' authors**: CPython refuses these loads for a reason, and a library may keep state that breaks in ways no test covers. numpy warns about it when it is imported this way.
- **BLAS threads**: with isolated numpy or scipy, every worker calls the same BLAS library, so the server limits it to one thread per worker unless you set `OPENBLAS_NUM_THREADS` and the like.

Copies live in the user's cache directory (`~/Library/Caches/mipyre` on macOS, `$XDG_CACHE_HOME/mipyre`, by default `~/.cache/mipyre`, elsewhere), or in `MIPYRE_ISOLATE_DIR`. They are reused by later starts and remade when the installed library changes. A worker makes its copy the first time it imports the library, at start for a module-level import or on the first request for an import inside a handler, so that first request is slower; import the library at module level to pay it at start.
