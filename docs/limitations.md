# Limitations and known issues

What mipyre doesn't do yet, and the issues we know about.

## Platforms

- **Python**: CPython 3.14, the regular build with the GIL. The free-threaded build (3.14t) is not supported. 3.13 is not supported either: its own `_datetime` module crashes under concurrent own-GIL sub-interpreters, with no mipyre code involved.
- **Wheels**: Linux x86_64 and macOS arm64. Other platforms have no wheel on PyPI.

## Libraries in sub-interpreters

An extension module loads in a sub-interpreter only if it declares support for it. Today numpy, pandas, scipy, pyarrow, orjson, pydantic and the PyPI build of polars don't; their routes run on `process` or `main`. PyO3-based extensions need to be built against the PyO3 fork mipyre uses. The measured list, a script to check a library yourself, and the fork: [compatibility.md](compatibility.md).

## Per runtime

### `process`

- Not supported in process workers, and refused with an error rather than run somewhere else:
  - `async def` handlers (refused when the route is registered);
  - `body=BodyStream` streaming request bodies (refused when the route is registered);
  - returning a `Stream` (the request is answered `500`, and the log says to use `runtime='main'`);
  - `app.state` and `PgPool` (using either raises `RuntimeError`, answered `500`).
- WebSocket handlers always run on the main interpreter, whatever the app's runtime.
- A request body and a response are each limited to `slot_bytes`, 256 KiB by default: a larger request is answered `413`, a larger response `500`, naming the sizes. Raise it with `ProcessLanes(slot_bytes=...)` or `--process-slot-bytes`.
- Every request to a process route crosses to another process, so it costs the server more CPU than the same route on `subinterp`.

### `isolated`

- Each worker loads its own copy of every library that can't load in a sub-interpreter: one copy per worker, on disk and in memory. The start line names each library and the size of its copy.
- Each copy believes it is alone in the process. Process-wide state (signal handlers, `atexit`, environment variables, thread pools, file locks) is shared by the copies, and they can step on each other. The libraries' authors don't test this way of loading.

### `main`

- One GIL for every `main` route: CPU-bound Python there doesn't scale past one core.
- On the thread-per-core server, the `def` handlers of `main` routes go through 4 bridge threads with a queue of 64; past it the server answers `503`.

### `subinterp`

- Module-level state is per sub-interpreter: each worker has its own copy of every global. Share state through `app.state`.
- Readiness checks can't run in a sub-interpreter (they run on `main` or `process`).

Choosing between them: [runtimes.md](runtimes.md).

## Command-line flags

`mipyre run` and `mipyre dev` have a flag for every server setting, each also a `MIPYRE_*` variable and a `[tool.mipyre]` key; `mipyre run --help` lists them ([every server setting](configuration.md#every-server-setting)). The app's declared flags join them, and what follows `--` is the app's ([your own flags](#your-own-flags)).

What belongs to the app is set in code only: the routes and their runtimes, every limit on the app (`max_body_size`, `request_timeout`, ...), the log configuration and the features it turns on. [configuration.md](configuration.md) lists where each one is set.

### Your own flags

Declare them with `mipyre.flags` (`mipyre run app:app --db-dsn x`, listed by `--help`, the same in every interpreter), or parse them yourself after `--` (`mipyre run app:app -- --db-dsn x`, in every interpreter's `sys.argv`): [configuration.md](configuration.md#declaring-flags). Flags come from the command line only: no environment variable, flag file or `[tool.mipyre]` entry sets one, and a flag the code no longer declares is an error, not ignored.

## Other frameworks' apps

FastAPI, Starlette, Flask, Django and other ASGI or WSGI apps can't be mounted in mipyre yet. This is planned.

## Known issues

- **A handler parameter named like a path parameter, in the request's place.** A handler's first parameter always receives the request, whatever it is called. With `@app.get("/hello/{name}")` and `def hello(name):`, registration succeeds, but `name` receives the `Request` object. Write `def hello(req, name):`.
