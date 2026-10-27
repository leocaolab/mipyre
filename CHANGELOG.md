# Changelog

All notable changes to mipyre are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/): within 1.x, the public API does not break.

## [3.0.1] — 2026-10-27

### Fixed

- The thread-per-core server takes `async_queue_capacity` (`MIPYRE_ASYNC_QUEUE_CAPACITY`, `--async-queue-capacity`, `[tool.mipyre] async_queue_capacity`). It hands `async def` routes to an async pool that this setting sizes, but setting it refused the start as a setting the server does not use, so the pool stayed at 128 per worker: with a `def` route beside them, 2048 `async def` requests on a 32-thread server, and each one past that answered `503`.

## [3.0.0] — 2026-10-27

WebSocket handlers are `async def` and run on the sub-interpreters by default, the server is given its settings as one `ServerFlags`, every setting has one spelling, and the request-body defaults are tighter. It breaks the 2.0 API in the places listed under Breaking; an old spelling fails where it is written, never silently.

### Breaking

WebSocket:

- A WebSocket handler is an `async def`: `recv()`, `recv_bytes()`, `recv_message()`, `send()` and `send_bytes()` return awaitables, and `async for msg in ws` yields each message until the connection ends. A `def` handler raises `TypeError` when the route is declared. A send that finds the buffer full waits for room instead of raising `BlockingIOError`; a receive while another task awaits one raises `RuntimeError`.
- `app.websocket(path, handler=None, *, runtime=None)` runs the handler on the route's runtime, the app's unless given, like an HTTP route: by default each connection is a task on a sub-interpreter worker's event loop, so handlers on different workers run in parallel and module globals are per worker (share through `app.state`). `runtime="main"` runs them on the main interpreter's event loop; `runtime="process"` raises `ValueError`. A connection waiting for a message holds no thread. The `before_request` hooks of a WebSocket route run on its event loop: write them as `async def`, since a blocking `def` hook stalls every connection there.

Settings and running:

- `app.run(config=None, *, reload=False)` takes the server's settings as one `ServerFlags` (`from mipyre import ServerFlags`), the value `TestClient(config=)` and `TestServer(config=)` take: `app.run(port=8001)` is `app.run(ServerFlags(port=8001))`. The start's summary and errors name a code value as `ServerFlags(port=)`.
- `Mipyre(routers=[...])` is gone: write `app.include_router("svc.admin:router")`, one line per router, with `prefix="/admin"` to put its paths under `/admin` (it starts with `/` and does not end with one, else `ValueError`). A router is still named by its import string, so each runtime imports only the routers it serves; a `Router` object raises `TypeError` saying how to name it. A target that names no module or attribute raises the error the app's own target does, which `mipyre run` prints as one line.
- One spelling per setting: its variable is `MIPYRE_` and its name in capitals, its option `--` and its name with dashes. Renamed: `MIPYRE_TPC_DARWIN` → `MIPYRE_TPC_ACCEPT`, `MIPYRE_GIL_BRIDGE_WORKERS` / `MIPYRE_GIL_BRIDGE_CAPACITY` → `MIPYRE_BRIDGE_WORKERS` / `MIPYRE_BRIDGE_CAPACITY`, `MIPYRE_TLS_PORTS` / `MIPYRE_PLAIN_PORTS` → `MIPYRE_EXTRA_TLS_PORTS` / `MIPYRE_EXTRA_PLAIN_PORTS`, `MIPYRE_PROCESS_QUEUE` → `MIPYRE_PROCESS_QUEUE_PER_WORKER`, `MIPYRE_LANE_HOST_FAILURE` → `MIPYRE_PROCESS_HOST_FAILURE`, `--allowed-host` → `--allowed-hosts`, `--trusted-proxy` → `--trusted-proxies`, `--process-queue` → `--process-queue-per-worker`, `MIPYRE_WORKERS` → `MIPYRE_THREADS`. An old name is not read: a variable stops the start with a `ConfigError` as an unknown variable, an option as an unrecognized argument; the new name is the one in this list.
- `app.process_lanes` is gone: `app.lane_stats()` returns the counters it was read for (worker pids, deaths, respawns, ...), a new snapshot per call, or `None` before a server prepared the app or without process routes. `mipyre.engine.LaneSet` is private.
- A 2.x keyword on a route, `app.run()` or `TestClient` (`gil=`, `model=`, `stream=`, `host=`, ...) is Python's own `TypeError` for an unexpected keyword, no longer a message naming the new spelling.
- `get_gil_metrics().static_bytes_in_flight` is gone: each server counts its own static answers, as `static_bytes_in_flight` in its `RunMetrics` (`TestServer.metrics()`) and `mipyre_static_bytes_in_flight` on its `/metrics`.

Request bodies:

- `max_body_size` defaults to 2.5 MiB (2,621,440 bytes, as Django's), was 10 MiB; `body_time_limit` defaults to 30 s, was 600 s.
- A body has `app.body_grace` (default 5 s) to arrive however slowly it comes, no longer `request_timeout`; past it, it goes on only while its average rate since it started is at least `body_min_rate`, and gets `408` as soon as it falls below. A body sent far slower than `body_min_rate` used to earn a second for every `body_min_rate` bytes and hold its connection up to `body_time_limit`. `request_timeout` is now the handler's time only (`504`), and for `body=BodyStream` how long the handler may leave a chunk untaken.

Handlers and routes:

- A handler that returns something that is not a response (a tuple, a set, an int, another object), from the handler, an `after_request` hook or as a `Response`'s body, answers the generic `500` and logs the type and its `repr`, e.g. `the returned tuple is not a response: ('x', 201)`. It used to go out as `200 text/plain` with its `str()`. A response is a `dict`, `list`, `str`, `bytes`, `bytearray`, `None`, `Response` or `Stream`.
- A route path that does not start with `/` raises `ValueError` where it is declared, on the app and on a `Router`: `GET 'nopath': a route path starts with "/"`.
- A path param annotated as anything but `str` raises `TypeError` at registration, naming the handler, the parameter and the annotation; a path param is always the `str` the URL holds.
- A handler whose first parameter is named after a path param raises `TypeError` at registration with the signature to write: `def hello(name)` on `/hello/{name}` used to get the request in `name`; write `def hello(req, name)`.
- `MCPServer`: a tool whose `input_schema` does not list as required an argument its handler requires raises `ValueError` when it is registered. A call is checked against the whole input schema once, so `"additionalProperties": false` is enforced, and the messages name the path: `invalid params: arguments.expression: expected string, got integer`, `invalid params: arguments: missing required key(s) ['b']`.

Logs:

- The production log level is `WARN` (was `ERROR`), so a handler's `logger.warning(...)` is logged. Turning the access log on (`enable_access_log()`, `MIPYRE_LOG=1`, `mipyre dev`) raises it to `INFO`, as it raised `ERROR`.

### Added

- `EventSourceResponse(source)` and `ServerSentEvent(data=None, event=None, id=None, retry=None)`: a handler on `runtime="main"` returns a generator or async generator of events, and mipyre writes them as `text/event-stream` with `Cache-Control: no-cache`, waits out a slow client, and closes the generator when the client leaves. At most 256 sync generators are read at once; one more is answered `503`.
- `Stream.wait_writable()` (blocks with the GIL released) and `await Stream.wait_writable_async()` wait for room after a `send` raised `BlockingIOError`, and raise `ConnectionError` once the client is gone or the stream closed, `close()` from another thread included. `Stream.send_event` takes `retry=` (milliseconds), and its `data` is optional.
- `app.stop()` stops the app's running servers from any thread, as `SIGTERM` does: each run drains, runs its shutdown hooks and returns from `app.run()`. It does not wait; with no run of the app running it does nothing.
- WebSocket keepalive: a client unheard for `app.websocket_ping_interval` (default 20 s) is pinged, and one that sends no frame within `app.websocket_ping_timeout` (default 20 s) after that is closed with `1011`, counted in `mipyre_websocket_keepalive_timeouts_total` and `RunMetrics.ws_keepalive_timeouts`.
- The log counts the lines it drops: `get_gil_metrics().log_lines_dropped` (and `log_lines_write_failed`, the ones stderr refused) and `mipyre_log_lines_dropped_total{reason="queue_full"|"write_failed"}`, with a warning at most once a minute.
- `get_gil_metrics().workers_abandoned` and `pools_at_abandon_limit`, also `mipyre_workers_abandoned` and `mipyre_pools_at_abandon_limit` on `/metrics`, and an `abandoned_workers` check in every `/readyz` of `HealthProbes` on `runtime="main"` (the default), which fails while a pool is at its limit (see Changed).
- `app.lane_stats()` counts `unawaited_skips`, the queued process-route requests skipped because their client had gone.

### Changed

- WebSocket connections no longer hold an OS thread each: in 2.0, 2,048 connections were 2,048 threads on one GIL. On one Mac, an echo server on the sub-interpreters answered about 58,000–64,000 echoes/s from 512 to 4,096 connections, where 2.0 answered 42,000 at 512 and 1,600 at 2,048.
- A WebSocket client that stops reading is cut off after 10 s without progress and warned of, as an HTTP client is, and a handler waiting for room wakes at once.
- One way to run `runtime="main"` handlers on every server: with `MIPYRE_TPC=0` they no longer go to tokio's blocking pool. A `def` handler runs on up to `bridge_workers` threads (default 4 thread-per-core, 512 with `MIPYRE_TPC=0`; more start as needed and retire after 10 s idle), with `bridge_capacity` requests waiting (16 per thread) and the next one answered `503`. `bridge_workers` and `bridge_capacity` now apply to every server with main-interpreter routes. The access log's `source` is `main` on every server, and the threads are named `mipyre-main-def-N`.
- `async def` handlers run on one stock asyncio event loop per interpreter. At `request_timeout` the client gets `504` and the handler's task is not cancelled: it runs to its end and its late answer is dropped; stopping the server cancels it.
- A request whose client left before its handler started is not run, on `runtime="main"` and on process routes.
- A request handed to another thread's process lanes waits in a queue of fixed size (the lanes' `queue_per_worker` each); past it the request is answered `503`, where it used to wait without limit.
- A sub-interpreter pool abandons a sync worker that an interruption did not end, and starts a replacement, only while it has abandoned fewer still-running workers than it has sync workers; at that limit the stuck worker keeps its place until its request ends, so a handler that never returns can no longer grow the process's threads without bound. Reaching the limit logs one `ERROR` and leaving it one `INFO`, and `/readyz` answers `503` so the orchestrator restarts the process. The interruption warning is held to one a minute.
- macOS: the thread-per-core server's threads accept from one shared socket (Darwin's `SO_REUSEPORT` handed every connection to one of them), and the multi-thread server runs an accept loop per I/O thread instead of one.
- The request-head read timeout and a new connection's deadline are kept by one clock thread, `mipyre-clock`, ticking every 100 ms, instead of a timer per request: they fire up to 100 ms late.
- The log writer writes the lines waiting in its queue in one write, up to 64 KiB, instead of one write per line.
- What a newcomer's mistakes print: `app.run()` reached while the app's module is imported (no `if __name__ == "__main__":` guard, under `mipyre run` or in a process worker) raises `FlagError` saying to add the guard, and a process worker whose app fails to import no longer prints its own traceback. A failed start is raised once, without a second JSON `ERROR` line, and a port in use adds the ways to give another. A TLS file that cannot be used is a `ConfigError` naming the setting and where it was given, and a key that is not the certificate's says so with both file names.
- The start banner reads `Mipyre v3.0.0` and counts `N sub-interp + N main + N process + N async` (was `+ N GIL +`); the start log's `topology` says `sub-interp` / `pool` (was `hybrid-inline` / `hybrid`). `mipyre dev` logs text unless `log_config` gives a format.
- A sub-interpreter `def` handler that imports an extension which can't load in a sub-interpreter logs its `500` with the advice the worker's start gives (`runtime="process"`, `"main"` or `"isolated"`).
- An unknown `MIPYRE_*` variable's error names it without its value, which may be a secret.
- A buffered request body that arrives in several frames is copied once, and `req.params` is decoded once per request; each access returns a new `dict`.
- Building from source uses the Rust toolchain pinned in `rust-toolchain.toml` (1.97.1).

### Fixed

- Request smuggling: a request with both `Content-Length` and `Transfer-Encoding: chunked`, in either order, is read by the chunks alone, its `Content-Length` is dropped before the handler or a process worker sees it, and the connection closes after the answer (RFC 9112, section 6.3). With the `Content-Length` first, the handler used to see a length that disagreed with its body and bytes after the body were read as the next request. hyper 1.11.1.
- A WebSocket client that sent nothing after the upgrade, lost its network without a FIN, or trickled a frame byte by byte held its connection forever; it is now closed by the keepalive (see Added).
- A `SIGINT` or `SIGTERM` received between two runs in one process no longer stops the next run as it starts. After a run, `SIGTERM` ends the process again instead of being caught and dropped, and a run takes the stop signals back from a handler installed with `signal.signal` between runs, giving them back when it ends.
- `Accept-Encoding` weights outside 0–1, and `NaN`, disable that coding (RFC 9110): `br;q=-1, *` no longer picks brotli.
- `runtime="isolated"`: copying a package while another process writes its bytecode no longer fails the worker's start.
- A process worker whose fork answer came after its deadline no longer sits idle until something else reads the template's socket.
- A `sys.path` too long for the process lanes' setup frame raises `ValueError` when the lanes start, instead of the template closing the link without a reason.
- tokio 1.53.3 (RUSTSEC-2026-0386) and ctutils 0.4.4 (RUSTSEC-2026-0385).

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
