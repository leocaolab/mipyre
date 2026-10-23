# Changelog

All notable changes to mipyre are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/): within 1.x, the public API does not break.

## [2.0.0] — 2026-10-22

A large rework: settings are declared once and checked at startup, the features an app turns on are objects of their own, the engine answers more by itself, and `runtime="process"` routes run on a shared-memory channel of their own (the `mipyre_shm` crate). It breaks the 1.0 API in the places listed under Breaking. An old spelling fails where it is written, not silently; most of the errors name the new spelling.

### Breaking

Routes and app features:

- `model=` and `stream=` on a route are one argument, `body=`: write `body=Model` for `model=Model` and `body=BodyStream` (`from mipyre import BodyStream`) for `stream=True`. The old spellings raise `TypeError` where the route is declared, naming the new one.
- `app.routes` lists `mipyre.Route` (was `RouteInfo`): `handler` is the function, `body` and `query` are the classes, `runtime` is a `mipyre.Runtime`, and `gil`, `model` and `stream` are gone. `FastRouteInfo` is `FastRoute`.
- `app.rpc` and the module `mipyre.rpc` are gone: write `@rpc(app)` (or `@rpc(app, "name")`) with `from mipyre import rpc`, and `from mipyre import RPCClient`.
- RPC answers in FastAPI's shapes: a reply's body is the handler's result, with no `{"ok": ..., "result": ...}` envelope, and a refused call is its status with `{"detail": ...}`. `RPCClient` raises `RPCRequestError` (4xx), `RPCServerError` (5xx) or `RPCReplyError` (a reply it cannot read), all `RPCError`, instead of `RuntimeError`. An RPC handler takes the request when it requires two positional parameters (`fn(req, data)`) and not when it requires one; an empty body is decoded by its codec instead of becoming `{}`.
- `app.enable_openapi`, `app.openapi()` and `MissingOpenApiDocumentError` are gone: write `docs = OpenApi(app, title=..., version=...)` (`from mipyre import OpenApi`) and `docs.document()` for the dict.
- `app.enable_health_probes()` and `@app.readiness_check(...)` are gone: write `probes = HealthProbes(app)` (`from mipyre import HealthProbes`) and `@probes.check("db")`. The probes are refused on `runtime="subinterp"` and `"isolated"` (`RuntimeDeclarationError`).
- `app.mcp` is gone: `mcp = MCPServer(app)` (`from mipyre.mcp import MCPServer`) registers `POST /mcp`, then `@mcp.tool()` and the rest as before. `MCPServer.handle_request(body) -> str` is `MCPServer.answer(message) -> dict | None`.
- `app.isolate(...)` is gone: declare the routes `runtime="isolated"` (or, better, `runtime="process"`).
- `app.enable_request_id()` is `app.enable_request_id_header()`, and `ctx.request_id()` / `ctx.set_request_id()` are gone: read `req.request_id`.
- `set_cookie` and `delete_cookie` are gone: put a `Cookie("sid", "abc", httponly=True)` under the `set-cookie` header of a `Response` (a list sends several), and `Cookie.expired("sid")` to drop one. A `Cookie` checks its name, value, path and domain when it is made (`ValueError`); `samesite` is always sent, `Lax` by default.

Requests and errors:

- Every error body the engine writes is `{"detail": ...}` (was `{"error": ...}`); a 5xx also carries `request_id`.
- `req.json()` and `req.text()` raise `HTTPException(400)` for a body that is not JSON / not UTF-8, instead of `ValueError`. `MultipartError` is an `HTTPException` (400, or 413 past `max_parts` / `max_part_header_bytes`), no longer a `ValueError`.
- Errors are grouped and imported from `mipyre`: `DeclarationError` is the base of `LateRegistrationError` (was `RegistrationSealed`), `MissingAppFileError` (was `NoWorkerScript` / `NoReloadScript`), `RuntimeDeclarationError` and `QueryDeclarationError`, which are no longer `ValueError` / `TypeError`. `BodyRejected` is `BodyReadError`, `WorkersAbandoned` is `AbandonedWorkersError` (its `workers` a tuple of `AbandonedWorker`), and the template's single-thread error is `ThreadedTemplateError`.
- A request method is case-sensitive (RFC 9110): `get /x` gets `405`, naming the uppercase methods in `Allow`. Registration still uppercases what you write; clients send standard methods in uppercase.
- A request that acts (any method but `GET`, `HEAD`, `OPTIONS`, and a WebSocket upgrade) from another site's page gets `403` before any handler runs, unless that site is in `enable_cors(allow_origins=...)`. Requests without `Origin` / `Sec-Fetch-Site` (curl, SDKs, other servers) are not affected. `app.trusted_origins` is gone; `enable_cors()` requires `allow_origins` (`"*"` or a list of `scheme://host[:port]`, checked when set, and a string is no longer split on commas).
- A request whose `Host` is not one of the server's names gets `421`. IP addresses and `localhost` are always served; list DNS names with `allowed_hosts` (`run(allowed_hosts=[...])`, `MIPYRE_ALLOWED_HOSTS`, `--allowed-host`), or `["*"]` behind a proxy that checks `Host`. `TestClient(app)` checks no host.

Settings and running:

- `workers` / `io_workers` are `threads` and `subinterpreters`: `run(threads=)`, `--threads`, `MIPYRE_THREADS` for the server's threads, and `run(subinterpreters=)`, `--subinterpreters`, `MIPYRE_SUBINTERPRETERS` for the `MIPYRE_TPC=0` server's pool. `run(workers=)` is a `TypeError`; `MIPYRE_WORKERS` and `MIPYRE_IO_WORKERS` are refused (below). `ProcessLanes(workers=)` and `--process-workers` are unchanged.
- A route's runtime is the code's only: `MIPYRE_RUNTIME`, `mipyre run --runtime` and `runtime` in `[tool.mipyre]` are gone. An app a deployment should switch reads a setting of its own and passes it in, e.g. `Mipyre(runtime=os.environ.get("APP_RUNTIME", "subinterp"))`. A deployment that still sets `MIPYRE_RUNTIME` stops at startup, naming it.
- A deployment value (the `mipyre run` flag, then the `MIPYRE_*` variable, then `pyproject.toml`) overrides the value the code gives, for every setting; the start prints each code value it replaced, e.g. `port=18125 (MIPYRE_PORT) overrides 18123 (run(port=))`. In 1.0, an argument in code won over the environment.
- Every configuration mistake stops the start, all of them at once in one `ConfigError` (a `ValueError`), each naming where it came from: a value that doesn't parse, a `MIPYRE_*` variable or `[tool.mipyre]` key that is no setting (names starting `MIPYRE_TEST_` / `MIPYRE_BENCH_` are left alone), and a value given to a server that doesn't use it (`subinterpreters` on the thread-per-core server, `host` / `port` / extra ports when systemd hands over the sockets), which 1.0 only noted.
- `MIPYRE_PROCESS_TIMEOUT` (seconds, as everywhere else) replaces `MIPYRE_PROCESS_TIMEOUT_MS`.
- `TestClient` no longer takes `host`, `port` or `transport`: a real TCP port is `TestServer(app)`, and `TestClient(server)` is a client of it. `client.transport` and `mipyre.testing.Transport` are gone. A start that does not serve in time raises `TimeoutError`.
- `get_gil_metrics()` holds only the process's own counters (GIL, RSS, connections, static files); one server's counts (`total_requests`, `dropped_requests`, `workers_*`, `cpu_*`, `degraded`, ...) are its `RunMetrics`, read from `/metrics` or, in tests, `TestServer.metrics()`.
- `/metrics` is rendered by the engine and counts every response, the engine's own included (404, 405, 503, 504, static files, fast routes, a raised exception's 500). `mipyre_http_requests_total` is labelled `method` and `status_class`; `mipyre_http_requests_by_class_total` and `mipyre_http_requests_by_method_total` are gone; `mipyre_http_request_duration_seconds` is a histogram. `mipyre.observability`, `install_metrics`, `install_request_id`, `CorruptMetric` and the `_m:*` keys in `app.state` are gone.
- One process has one log configuration: a server that asks for a different one than the process already has (another level, format or access log) fails with `RuntimeError` naming both, where 1.0 replaced it.
- `PgPool.connect()` connects nothing: it returns a handle, and the server opens one pool per DSN when it starts, binds the handles to it and closes it when it stops. Two calls for one DSN with different pool settings stop the start with `ValueError`, naming them all. Outside a server, a handle opens its own pool on its first query.
- A shutdown hook that raises is raised from `app.run()` (several as an `ExceptionGroup`), and `mipyre run` exits with status 1, instead of only logging it.
- Under `mipyre run`, `dev` and `routes`, `sys.argv` is the app's (its file and what follows `--`), not mipyre's command line, and options can no longer be abbreviated.

### Added

- `mipyre.flags`: command-line flags declared where they are used (`flags.string`, `integer`, `float`, `boolean`, `enum`, `strings`), parsed once by `mipyre run` with its own options (`--help` lists both) and read with `.value` in every interpreter; `flags.parse()` in a script, `flags.override(...)` in tests. Arguments after `--` are the app's own, in every interpreter's `sys.argv`.
- Every server setting has a `mipyre run` / `mipyre dev` flag, a `MIPYRE_*` variable and a `[tool.mipyre]` key (the process ones under `[tool.mipyre.process]`): new flags include `--tpc`, `--tpc-accept`, `--gc-*`, `--bridge-workers`, `--bridge-capacity`, `--async-queue-capacity`, `--metrics`, `--extra-tls-ports`, `--extra-plain-ports`, `--isolate-dir`, `--process-spin-us`, `--process-host-failure`, `--reload` and `--log`. `mipyre run --help` lists them with their variables and defaults.
- `mipyre.testing.TestServer(app, host="127.0.0.1", port=0)`: one real run of the app in the test process, every runtime and process routes included, on a port the kernel picks (`server.url`); `close()` raises how the run ended. `TestClient` and `TestServer` take `config=ServerFlags(...)` (`from mipyre.engine import ServerFlags`).
- `mipyre.HTTPException(status_code, detail=None, headers=None)`, FastAPI's signature: raised from a handler, a hook or a process worker, it answers `status_code` with `{"detail": detail}`.
- `trusted_proxies` (`run(trusted_proxies=[...])`, `MIPYRE_TRUSTED_PROXIES`, `--trusted-proxy`): behind the listed proxies, `req.client_ip` comes from `X-Forwarded-For`, read from the right past every trusted proxy.
- `mipyre.Runtime`, a `StrEnum` of the four runtimes, taken by every `runtime=`. `in_openapi=False` on a route leaves it out of the OpenAPI document.
- `mipyre.mcp.ToolError(message)`: a tool's failure reported to the model (`isError: true`) without a log line.
- `mipyre.engine.encode_json(value) -> bytes`, the encoder response bodies use. `uuid.UUID` and `decimal.Decimal` (what `PgPool` returns for `uuid` and `numeric`) encode as strings, and integers wider than 64 bits are written in full.
- The access log line carries `request_id` and `client_ip`.
- New metrics: `mipyre_http_requests_refused_total`, `mipyre_validation_failures_total`, `mipyre_workers_died_total{runtime="process"}`, `mipyre_forwarded_for_unreadable_total`, `mipyre_static_bytes_in_flight` and `mipyre_static_answers_cut_total`.
- The `X-Request-ID` header (`enable_request_id_header()`) is on every response, the engine's own included.

### Changed

- Process lanes (`runtime="process"`) run on `mipyre_shm`, a shared-memory channel crate: each server run gives every lane new shared memory and its own wake pipe, which no other process inherits, and no worker of an earlier run can write into a later run's lanes.
- A process worker is untrusted. One that writes what no correct worker writes into its lane (a counter or length out of range, more wake-ups than it has answers) is killed, the requests in its lane are answered `502`, and the lane restarts under its crash backoff; the other lanes are not touched. A worker that cannot map its lane, or imports another build of mipyre, exits with status 78 and its lane stops instead of restarting in a loop.
- When a process worker dies, the requests handed to it that it had not taken are answered `502`, never run on the next worker. Each death is logged once (lane, pid, how it ended, how long it served) and counted.
- A process worker's Python `logging` goes through the server's log (JSON or text, with the level set), each line carrying `lane`.
- `python app.py`, `mipyre dev`, `TestClient` and `TestServer` start the process-lane template themselves, as a new interpreter on every platform, so process routes work from every way in, including apps whose imports start threads; only `mipyre run` forks it on Linux. `ProcessLanesNeedLauncher` is gone.
- On the thread-per-core server every `runtime="main"` route runs through the bridge, also in an app with no sub-interpreter route; a full bridge answers `503` at once. `async def` main routes run on one event loop of the main interpreter and hold no bridge thread while they wait.
- How the server is built follows the routes: it starts sub-interpreters only when a route runs on one, and such an app needs no source file for workers. The access log field is `source` (was `mode`), with `main` (was `gil`) and `tpc-main-bridge`; the start line's field is `topology`.
- A `body=` or `query=` route on `runtime="subinterp"` is checked against the installed pydantic-core when it is declared: it registers when that build supports sub-interpreters, and otherwise raises `RuntimeDeclarationError` naming the way out, instead of failing when the workers start.
- `runtime="isolated"` decides which packages need a worker's own copy by reading their extension files before they load, so a library's own `except ImportError` can no longer hide a failed load (polars 2.0); a failed copy raises `IsolationError` with its reason.
- The `422` of `body=` / `query=` lists the first 20 errors with `error_count`; a validator that raises is a 422, not a 500; the warning for it is throttled to one line a minute.
- `@cached_json` keys on the path and the query string, serves requests with `Authorization` or `Cookie` uncached, caches only `dict` / `list` results, and holds at most 64 MiB per handler.
- `/livez` is answered by the engine without Python; `/readyz` runs its checks at most once a second and throttles its failure log.
- MCP tools run on the request's own thread under `request_timeout`; registering a name twice raises `ValueError`.
- BLAS and polars thread pools in process workers and isolated workers are limited to one thread unless you set any variable of that pool, in which case the whole pool is left as you set it.
- A static file the cache doesn't keep is sent from disk 64 KiB at a time as the client reads, so a slow client holds at most a chunk and the write buffer, not the whole file. A file rewritten while it is sent ends the connection short instead of sending other bytes. Static mounts start watching their files when the server starts, and each server has its own 128 MiB cache budget.
- A stop ends a lingering close at once, and an answer the server gives before reading the body (a `413`) reaches the client instead of a reset.
- Exiting with abandoned workers is decided by whether one is still running Python, with no 1 s timer.
- The text log writes colour codes only to a terminal.
- `mipyre run` makes its cache directory only for an app with process routes, so a read-only or homeless container can run other apps.

### Fixed

- A static request for a FIFO (or a file whose `.br` / `.gz` copy is one) is answered instead of hanging a thread; on macOS a FIFO under a static root no longer hangs the file watcher.
- A static root that is a symlink follows its switch to another directory; a root swapped for a new directory is watched again; a change to any link of a symlink chain is followed; a dropped mount gives its cache bytes back; a file reached by several paths is cached once.
- A worker answer claiming more header lines than its bytes hold, or a handler returning more headers than a header map takes, is an error instead of a panic.
- A replacement worker still forking when its server stops is killed when its fork answers; workers are signalled through a pidfd on Linux, so a reused pid is never hit.
- A `SIGINT` or `SIGTERM` while the workers start stops the run; `SIGTERM` to `mipyre dev` stops its server, and the reloader exits with the server's status.
- A server with process workers no longer prints a `ConnectionResetError` traceback from the template when it stops (Linux).
- A failed `mipyre run` of an app with process routes no longer prints a second, misleading error about the template's imports.
- A failed pool start no longer aborts in `Py_Finalize` under a live worker, and a thread that ends while the process finalizes no longer restores a thread state `Py_Finalize` has freed.
- `POST /mcp` answers bodies a client controls (deep nesting, huge or non-finite ids) with a JSON-RPC parse error instead of `500`; a notification gets `202`.
- An `async def` class-based view taking path parameters or a model runs on the async workers.
- A MsgPack RPC body nested too deep is a `400` that says so.
- `TestClient.websocket_connect` no longer triggers websockets 17's `DeprecationWarning`.
- Error messages say what to do themselves instead of pointing at a document, and no longer name internals.

## [1.0.0] — 2026-10-21

The first public release.

### Added

- An HTTP/1.1 and HTTP/2 server written in Rust (tokio, hyper, rustls), one thread per physical core by default, with a multi-thread server as the alternative (`MIPYRE_TPC=0`).
- Python handlers running in parallel in sub-interpreters that each own their GIL, inside one process; `def` and `async def` handlers.
- Per-route runtimes: `subinterp` (default), `process` (worker processes over shared memory, for C extensions that can't load in a sub-interpreter), `main` (the main interpreter) and `isolated`; declared on a route, a `Router`, the app or the deployment.
- Routing with path and query parameters, `Router` modules, a fallback route and prebuilt constant responses.
- Body and query validation with pydantic (`model=`, `query=`), answered with `422` and the reasons; an OpenAPI 3.1 document.
- Hooks (`before_request`, `after_request`, `on_startup`, `on_shutdown`), CORS, cookies, CSRF checks, redirects.
- gzip and brotli compression; static files with ETag and precompressed variants.
- Multipart uploads, streaming request bodies, Server-Sent Events, WebSocket.
- RPC over JSON, MsgPack or Protobuf; an MCP endpoint.
- A PostgreSQL pool, a per-worker JSON cache, and state shared by every worker (`app.state`).
- Health probes, Prometheus metrics, JSON or text logs, an access log with sampling.
- Limits on body size, timeouts, slow uploads and clients that stop reading; bounded queues answered with `503`.
- TLS, systemd socket activation, graceful shutdown, hot reload, the `mipyre` command (`run`, `dev`, `routes`) and an in-memory `TestClient`.

### Requirements

- Python 3.14 (the build with the GIL). Wheels for Linux x86_64 and macOS arm64.
