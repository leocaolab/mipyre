# Configuration reference

Every setting mipyre reads, where it is set, and its default.

## Where settings come from

- **Code**: `Mipyre(...)`, `ServerFlags(...)` (passed to `app.run()`), `ProcessLanes(...)`, attributes and `enable_*` methods on the app. What belongs to the app wherever it runs: its routes and their runtimes, limits, logging, probes, metrics, compression.
- **The deployment**: every server setting (address, sizes, TLS, the server model, process workers, tuning) is also a `mipyre run` / `mipyre dev` flag, a `MIPYRE_*` environment variable and a key in `[tool.mipyre]` of the `pyproject.toml` in the directory the server starts in. [Every server setting](#every-server-setting) lists the three names of each.

A deployment value overrides the code's: the `mipyre run` flag first, then the environment, then `pyproject.toml`, then what the code gives, then the default. The start prints each code value it replaced:

```text
  port=18125 (MIPYRE_PORT) overrides 18123 (ServerFlags(port=))
```

Every value is checked once, at startup, and every mistake stops the start, all of them at once, each naming where it came from: a value that doesn't parse, a `MIPYRE_*` variable or `[tool.mipyre]` key that is not a setting (a misspelling, or one mipyre no longer reads), and a value given to a server that doesn't use it. For example:

```text
$ MIPYRE_TPC=maybe MIPYRE_THREDS=2 mipyre run app:app
mipyre: MIPYRE_THREDS is set, but it is not one of mipyre's settings (`mipyre run --help` lists them); unset it, or rename it if it is your own (MIPYRE_TEST_* and MIPYRE_BENCH_* are left to test and benchmark tools)
mipyre: MIPYRE_TPC: "maybe" is invalid for tpc: expected 1/true/yes/on or 0/false/no/off
```

From `app.run()` or a test, the same reasons are one `mipyre.engine.ConfigError`, a `ValueError` whose `errors` lists them. Your own variables need a prefix other than `MIPYRE_`.

## Server

| Setting | In code | Environment | `mipyre run` flag | Default |
|---|---|---|---|---|
| address | `ServerFlags(host=)` | `MIPYRE_HOST` | `--host` | `127.0.0.1` |
| port | `ServerFlags(port=)` | `MIPYRE_PORT` | `--port` | `8000` |
| server threads: thread-per-core threads (one sub-interpreter each), or with `MIPYRE_TPC=0` the I/O threads and accept loops | `ServerFlags(threads=)` | `MIPYRE_THREADS` | `--threads` | thread-per-core: physical cores (performance cores on Apple silicon); `MIPYRE_TPC=0`: logical CPUs |
| the sub-interpreter pool's size (`MIPYRE_TPC=0` only; thread-per-core runs one per thread) | `ServerFlags(subinterpreters=)` | `MIPYRE_SUBINTERPRETERS` | `--subinterpreters` | logical CPUs |
| server model | | `MIPYRE_TPC` | `--tpc` | `1`: thread-per-core. `0` (or `false`, `no`, `off`): multi-thread server, I/O threads in front of a pool of sub-interpreters |
| TLS certificate chain and key (PEM) | `ServerFlags(tls_cert=, tls_key=)` | `MIPYRE_TLS_CERT`, `MIPYRE_TLS_KEY` | `--tls-cert`, `--tls-key` | unset: plain HTTP. Set both or neither |
| more TLS ports; `port` then serves plain HTTP | `ServerFlags(extra_tls_ports=[...])` | `MIPYRE_EXTRA_TLS_PORTS` (comma-separated) | `--extra-tls-ports` (comma-separated) | none |
| more plain ports (HTTP/1.1 and HTTP/2 with prior knowledge) | `ServerFlags(extra_plain_ports=[...])` | `MIPYRE_EXTRA_PLAIN_PORTS` (comma-separated) | `--extra-plain-ports` (comma-separated) | none |
| names this server is reached by; a request whose `Host` is another name gets `421` (stops DNS rebinding). `localhost` and IP addresses are always served; `["*"]` serves any name, for a reverse proxy that checks it | `ServerFlags(allowed_hosts=[...])` | `MIPYRE_ALLOWED_HOSTS` (comma-separated) | `--allowed-hosts` (repeatable) | none: `localhost` and IP addresses only |
| reverse proxies whose `X-Forwarded-For` names the client (`req.client_ip`): IP addresses and CIDR blocks. A request from one of them takes its client from the header, read from the right past every listed proxy; anyone else's header is ignored. See [Behind nginx](integrations.md#behind-nginx) | `ServerFlags(trusted_proxies=[...])` | `MIPYRE_TRUSTED_PROXIES` (comma-separated) | `--trusted-proxies` (repeatable) | none: the TCP peer is the client |
| hot reload | `run(reload=True)` | `MIPYRE_RELOAD=1` | `--reload`, or `mipyre dev` | off |

The address must be an IP address (`127.0.0.1`, `0.0.0.0`, `::1`); host names such as `localhost` are not resolved and stop the start.

`mipyre dev` takes the same settings as `mipyre run`, except `--reload`: it always reloads. It turns logging on (the access log and INFO lines) unless `MIPYRE_LOG` is set to something other than `1`.

## Runtime

Where a route runs ([runtimes.md](runtimes.md)) is the code's alone: the route's `runtime=`, else its `Router(runtime=)`, else `Mipyre(runtime=)`, else `"subinterp"`. No deployment setting changes it. An app that a deployment should switch reads a setting of its own and passes it in, for example `Mipyre(runtime=os.environ.get("APP_RUNTIME", "subinterp"))`.

| Setting | Environment | `mipyre run` flag | `[tool.mipyre]` | Default |
|---|---|---|---|---|
| where `runtime="isolated"` keeps its per-worker library copies | `MIPYRE_ISOLATE_DIR` | `--isolate-dir` | `isolate_dir` | `~/Library/Caches/mipyre` on macOS; `$XDG_CACHE_HOME/mipyre`, by default `~/.cache/mipyre`, elsewhere |
| where `runtime="process"` keeps the server's copy of mipyre, which its workers import (made only when the app has process routes; the directory must be writable by the server's user) | `XDG_CACHE_HOME` (`HOME` on macOS) | | | `~/Library/Caches/mipyre/run` on macOS; `$XDG_CACHE_HOME/mipyre/run`, by default `~/.cache/mipyre/run`, elsewhere |

## Process workers

The worker processes `runtime="process"` routes run on. Each setting comes from the first place that sets it: a `mipyre run` flag, the environment, `[tool.mipyre.process]`, `Mipyre(process=ProcessLanes(...))` in code, the default.

| Setting | `ProcessLanes(...)` | `mipyre run` flag | Environment | `[tool.mipyre.process]` | Default |
|---|---|---|---|---|---|
| worker processes | `workers=` | `--process-workers` | `MIPYRE_PROCESS_WORKERS` | `workers` | one per physical core (performance core on Apple silicon) |
| a handler's time limit, in seconds; past it the worker is killed and replaced, `504` | `timeout=` | `--process-timeout` | `MIPYRE_PROCESS_TIMEOUT` | `timeout` | `30` |
| requests queued per worker before `503` | `queue_per_worker=` | `--process-queue-per-worker` | `MIPYRE_PROCESS_QUEUE_PER_WORKER` | `queue_per_worker` | `128` |
| largest request body, and largest response, in bytes | `slot_bytes=` | `--process-slot-bytes` | `MIPYRE_PROCESS_SLOT_BYTES` | `slot_bytes` | `262144` (256 KiB) |
| how a worker gets the app: `"import"` or `"fork"` | `start=` | `--process-start` | `MIPYRE_PROCESS_START` | `start` | `"import"` |
| what a failed worker host does: `"isolate"` takes it out of service and keeps serving with the rest; `"exit"` stops the server at the first failure | | `--process-host-failure` | `MIPYRE_PROCESS_HOST_FAILURE` | `host_failure` | `"isolate"` |
| microseconds a worker spins after an answer, waiting for the next request, before it sleeps | | `--process-spin-us` | `MIPYRE_PROCESS_SPIN_US` | `spin_us` | `50` |

```toml
[tool.mipyre.process]
workers = 2
timeout = 5
```

Either `host_failure` way, a server that ends because of a failure exits non-zero, so a supervisor restarts it.

## Limits

Set on the app, in code. Each app keeps its own.

| Attribute | Default | Past it |
|---|---|---|
| `max_body_size` | 2.5 MiB (2,621,440 bytes, as Django's) | `413` |
| `request_timeout` | 30 s for the handler to answer | `504` |
| `body_grace` | 5 s for the body to arrive however slowly it comes | `408` |
| `body_min_rate` | 1024 bytes/s: past `body_grace` a body goes on while its average rate stays at least this, so a steady upload isn't cut off and a trickle is | `408` |
| `body_time_limit` | 30 s, the most a body may take however fast it arrives | `408` |
| `interrupt_grace` | 2 s for a sync handler interrupted at `request_timeout` to end before its worker is replaced | |
| `shutdown_timeout` | 5 s for each of the app's threads to finish at shutdown | the thread is abandoned and reported |
| `max_websocket_message_size` | 1 MiB, either direction | a client message closes the connection with `1009`; a larger `ws.send` raises `ValueError` |
| `max_websocket_connections` | 1024 | `503` for the upgrade |
| `websocket_ping_interval` | 20 s a client may go unheard before it is pinged | |
| `websocket_ping_timeout` | 20 s for a pinged client to send a frame | the connection is closed with `1011` and counted in `mipyre_websocket_keepalive_timeouts_total` |

```python
app.max_body_size = 64 * 1024 * 1024
app.request_timeout = 60
```

Fixed limits, not configurable:

| Limit | Value |
|---|---|
| a new connection's first request head, and on HTTP/1 each next one | 10 s, then the connection is closed |
| a client that stops reading a response | 10 s, then the connection is closed |
| a file served by `app.static` | 16 MiB |
| a client's `X-Request-ID` that is echoed back | visible ASCII, at most 128 bytes |
| one readiness check | 10 s, then it counts as failed |

## Logging

| Setting | In code | Environment | Default |
|---|---|---|---|
| level, format, access log | `Mipyre(log_config={"level": ..., "format": ..., "access_log": ...})` | | `WARN`, `"json"`, no access log |
| development defaults | `Mipyre(debug=True)` | | text at `DEBUG` with the access log on |
| the access log on, and the level raised to `INFO` when it is `WARN`, `ERROR` or `OFF` | `app.enable_access_log(level=None, sample=1, always_log_status=None)` | `MIPYRE_LOG=1` (`--log`) | off |

`log_config` keys: `level` (`"OFF"`, `"ERROR"`, `"WARN"`, `"INFO"`, `"DEBUG"`, `"TRACE"`, or a `LogLevel`), `format` (`"json"` or `"text"`), `access_log` (`True` or `False`). An unknown level raises `ValueError` in `Mipyre()`.

One process has one log configuration, set by its first server. A later server in the same process (a second app in a test session) that asks for another level, format or access log fails to start with `RuntimeError`, naming both.

`enable_access_log(sample=100, always_log_status=400)` logs about 1 response in 100, and every response with a status of 400 or more.

What the logs and metrics contain, and how to ship them: [observability.md](observability.md).

## Features turned on in code

| Call | Parameters and defaults |
|---|---|
| `app.enable_compression()` | `min_size=512`, `gzip=True`, `brotli=True`, `gzip_level=6` (1–9), `brotli_quality=4` (0–11). Off until called; `app.disable_compression()` turns it off |
| `app.enable_cors(allow_origins=...)` | `allow_origins` (required): `"*"` or a list of `scheme://host[:port]`; the only sites whose pages may read responses and send requests that act (`POST`, `PUT`, `PATCH`, `DELETE`, WebSocket upgrades), from any other site's page those get `403`. `allow_methods="GET, POST, PUT, DELETE, PATCH, OPTIONS"`, `allow_headers="*"`, `expose_headers=""`, `allow_credentials=False`, `max_age=86400`. `"*"` with credentials raises `ValueError` |
| `app.enable_request_id_header()` | `header="X-Request-ID"` |
| `app.enable_metrics()` | `path="/metrics"` (Prometheus) |
| `HealthProbes(app)` | `livez_path="/livez"`, `readyz_path="/readyz"`, `runtime="main"` (or `"process"`) |
| `OpenApi(app, title=, version=)` | `path="/openapi.json"`. Off until made |
| `app.static(prefix, directory)` | `precompressed=False` |

`PgPool.connect(dsn, ...)` takes `max_connections=10`, `acquire_timeout_secs=30`, `max_lifetime_secs=1800`, `idle_timeout_secs=300` (0 is never), `ping_on_acquire=False`, and `runtime_threads` (the threads every pool's I/O runs on: one per 4 CPUs, at least 2). It connects nothing: the server opens one pool per DSN when it starts, from every `connect()` its app's modules made, and closes it when it stops. Two calls for one DSN that name different settings stop the start with `ValueError`. Outside a server (a script, a migration), a pool opens on the first query.

## Configuring your own app

The settings above are mipyre's. Your app's own settings (a database DSN, an API key, a page size) need a way in that every interpreter sees the same:

- **Declare flags** with `mipyre.flags` (below): `mipyre run app:app --db-dsn x` parses them once, lists them in `--help` and gives every interpreter the same values.
- **Or parse your own arguments**, after `--`: `mipyre run app:app --port 8000 -- --db-dsn postgres://db/app`. mipyre doesn't read what follows the first `--`. Every interpreter of the server (the main interpreter, each sub-interpreter, each process worker) has `sys.argv == ["/path/to/app.py", "--db-dsn", "postgres://db/app"]` before your module is imported, so an app that parses `sys.argv` with `argparse` (or click) at import reads the same values everywhere, and rejects an argument it doesn't know with its own usage message. Under `python app.py --db-dsn x`, every interpreter has that process's `sys.argv`.
- **Or use environment variables.** Every interpreter of the server sees the same environment: the sub-interpreters, the main interpreter and the process workers.
- **Or read your own config file**, by a path that doesn't depend on how the server was started: an absolute path, one taken from an environment variable, or one next to your module (`Path(__file__)`).
- **Don't put your settings in `[tool.mipyre]`**: that table is mipyre's.

Command lines are visible to other users of the machine (`ps`): pass secrets through the environment or a file, not a flag or an argument.

### Declaring flags

Declare a flag at the top of the module that uses it, and read its `.value`:

```python
# app.py
from mipyre import Mipyre, flags

DSN = flags.string("db_dsn", None, "Postgres DSN")
PAGE_SIZE = flags.integer("page_size", 20, "rows per page")

app = Mipyre()


@app.get("/")
def index(req):
    return {"dsn": DSN.value, "page_size": PAGE_SIZE.value}
```

```bash
mipyre run app:app --threads 4 --db-dsn postgres://db/app
```

| Declaration | On the command line | `.value` |
|---|---|---|
| `flags.string(name, default, help)` | `--name text` | `str` or `None` |
| `flags.integer(name, default, help)` | `--name 7` (64-bit) | `int` or `None` |
| `flags.float(name, default, help)` | `--name 0.5` (finite) | `float` or `None` |
| `flags.boolean(name, default, help)` | `--name` / `--no-name` | `bool` |
| `flags.enum(name, default, choices, help)` | `--name one-of-choices` | `str` or `None` |
| `flags.strings(name, default, help)` | `--name a,b` | `list[str]` or `None` |

A flag's name is written with dashes on the command line (`db_dsn` is `--db-dsn`). `mipyre run` imports the app, then parses its command line once with mipyre's options and every flag the app declared: `mipyre run app:app --help` lists both, and an argument that is neither stops the server before it binds, with exit status 2:

```text
mipyre run: error: unrecognized arguments: --db-dns x
They are neither options of this command nor flags the app declares; arguments the app parses itself go after `--`.
```

Every interpreter then reads the same values (and the same `sys.argv`): the main interpreter, each sub-interpreter, each process worker. None of them parses the command line again.

What is an error, and when:

- **Two declarations of one name** (in two modules, or one of yours named like a `mipyre run` option such as `workers`): `DuplicateFlagError` at the second declaration, naming both lines. The same line executed twice (a script imported both as `__main__` and by its name) declares one flag.
- **Reading `.value` before anything parsed the command line**: `UnparsedFlagError`, naming the flag. While a test's server runs (`TestClient`, `TestServer`), a flag nothing parsed reads as its default instead.
- **Declaring a flag only where a handler imports it**, or one that differs between interpreters (another default): `FlagMismatchError` in that worker or at that import, naming the flag and the line. Declare flags at the top of modules the app imports.

All of them are `mipyre.flags.FlagError`, a `mipyre.DeclarationError`.

**A script** started with `python app.py --db-dsn x` parses its command line itself, before it reads a flag. Sub-interpreters that execute the script again don't run the `__main__` block and get the main interpreter's values:

```python
DSN = flags.string("db_dsn", None, "Postgres DSN")

if __name__ == "__main__":
    flags.parse()   # what follows `--` stays in sys.argv for the script

app = Mipyre()
...

if __name__ == "__main__":
    app.run()
```

**A test** doesn't parse: the server it starts (`TestClient` or `TestServer`) gives every flag its declared default, in every runtime, and never reads the test runner's `sys.argv`. It sets other values with `flags.override`, as a context manager or a decorator of a (sync) test function; the server gives them to every runtime, and leaving the override puts back what was there before ([testing.md](testing.md#flags-in-tests) has the pytest fixtures):

```python
from mipyre import flags
from mipyre.testing import TestClient

import app


def test_index_reads_the_dsn():
    with flags.override(db_dsn="postgres://test/app"), TestClient(app.app) as client:
        assert client.get("/").json()["dsn"] == "postgres://test/app"


@flags.override(page_size=5)
def test_page_size():
    with TestClient(app.app) as client:
        assert client.get("/").json()["page_size"] == 5
```

Process workers started with `start="fork"` come from a template that imports the app once, at its first server: a later server of the same app with other flag values fails to start with `FlagMismatchError`, naming the flags, rather than serve workers whose module-level code saw the old values. Give every server of one app the same values, or use `start="import"` (the default).

A server holds the values it started with until it stops, so the override is entered before the `TestClient` starts and left after it closes; entering or leaving one while a server runs is a `FlagOverrideError`, as is overriding a name nobody declared or giving a value of the wrong kind. A flag read while the module is imported (`Mipyre(runtime=RUNTIME.value)` at the top of `app.py`) can't be tested this way: `import app` reads it before any server or override exists and raises `UnparsedFlagError`. Read flags in handlers and startup hooks.

An example that reads the environment first, then an optional `app.toml` beside the module:

```python
# settings.py
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    db_dsn: str
    page_size: int
    greeting: str


def load() -> Settings:
    """Read the app's own settings: the environment first, then app.toml beside this file."""
    path = Path(os.environ.get("APP_CONFIG", Path(__file__).with_name("app.toml")))
    file = tomllib.loads(path.read_text()) if path.exists() else {}
    return Settings(
        db_dsn=os.environ.get("APP_DB_DSN", file.get("db_dsn", "postgresql://localhost/app")),
        page_size=int(os.environ.get("APP_PAGE_SIZE", file.get("page_size", 20))),
        greeting=os.environ.get("APP_GREETING", file.get("greeting", "hello")),
    )
```

```python
# app.py
from mipyre import Mipyre

import settings

config = settings.load()
app = Mipyre()


@app.get("/sub")
def on_subinterp(req):
    return {"page_size": config.page_size, "greeting": config.greeting}


@app.get("/main", runtime="main")
def on_main(req):
    return {"page_size": config.page_size, "greeting": config.greeting}


@app.get("/process", runtime="process")
def on_process(req):
    return {"page_size": config.page_size, "greeting": config.greeting}
```

```toml
# app.toml
page_size = 50
greeting = "from the file"
```

```bash
APP_PAGE_SIZE=100 mipyre run app:app
curl -s 127.0.0.1:8000/process
```

```text
{"page_size":100,"greeting":"from the file"}
```

`/sub` and `/main` answer the same: `config` is built when each interpreter imports `app.py`, from the same environment and the same file.

## Engine tuning

Each is also a `mipyre run` flag and a `[tool.mipyre]` key ([Every server setting](#every-server-setting)). The defaults are what the server was measured with; change them when you have measured a reason.

| Environment | Meaning | Default |
|---|---|---|
| `MIPYRE_GC_MODE` | when each worker runs `gc.collect()`. Automatic collection is off in the workers (`gc.disable()`); reference counting still frees non-cyclic garbage at once. `count`: every `MIPYRE_GC_THRESHOLD` requests. `idle`: in a lull, checked every `MIPYRE_GC_IDLE_MS`, and at the latest after `MIPYRE_GC_OOM_FAILSAFE` requests. `off`: only your own `gc.collect()` calls | `count` |
| `MIPYRE_GC_THRESHOLD` | requests between collections in `count` mode; `0` never collects | `100000` |
| `MIPYRE_GC_IDLE_MS` | how often `idle` mode checks for a lull, in milliseconds | `100` |
| `MIPYRE_GC_OOM_FAILSAFE` | requests after which `idle` mode collects even without a lull | `50000` |
| `MIPYRE_BRIDGE_WORKERS` | the most threads that run the `def` handlers of `runtime="main"` routes, on every server (`async def` ones wait on the main interpreter's event loop, holding none). Four start with the server; past them a thread starts when a request finds none free, and retires after 10 s idle | `4` (thread-per-core), `512` (`MIPYRE_TPC=0`) |
| `MIPYRE_BRIDGE_CAPACITY` | main-interpreter requests queued (`async def`: queued or running) before `503` | 16 per thread (`64` thread-per-core, `8192` with `MIPYRE_TPC=0`) |
| `MIPYRE_ASYNC_QUEUE_CAPACITY` | requests queued for the sub-interpreters that run `async def` handlers before `503` | 128 per worker of the pool that runs them |
| `MIPYRE_METRICS` | `1` turns on the engine's own counters and a memory (RSS) sampler | `0` |
| `MIPYRE_TPC_ACCEPT` | macOS only: how the thread-per-core server accepts connections. `listener`: each thread runs its own accept loop on the port's one listening socket, so whichever thread is free takes the next connection (macOS's `SO_REUSEPORT` would hand every connection to one socket). `fanout`: one acceptor that hands connections to the threads | `listener` |

`idle` GC mode needs the default thread-per-core server with a listener per thread; the multi-thread server (`MIPYRE_TPC=0`) and `MIPYRE_TPC_ACCEPT=fanout` take `count` or `off`, and `idle` there stops the start.

## Every server setting

Each server setting, by its three names. A `[tool.mipyre]` key is written in `pyproject.toml` under `[tool.mipyre]`, the `process.*` ones under `[tool.mipyre.process]`. `mipyre run --help` prints the same list.

| Setting | `mipyre run` / `mipyre dev` | Environment | `pyproject.toml` | Default | What it is |
|---|---|---|---|---|---|
| `threads` | `--threads` | `MIPYRE_THREADS` | `[tool.mipyre] threads` | one per physical core (thread-per-core); with tpc off, one per logical CPU | the server's threads: thread-per-core threads, or with tpc off the I/O threads |
| `subinterpreters` | `--subinterpreters` | `MIPYRE_SUBINTERPRETERS` | `[tool.mipyre] subinterpreters` | one per logical CPU, at least 2 | the sub-interpreter pool's size, with tpc off; thread-per-core runs one per thread |
| `tpc` | `--tpc` | `MIPYRE_TPC` | `[tool.mipyre] tpc` | on | thread-per-core; off runs the multi-thread server |
| `tpc_accept` | `--tpc-accept` | `MIPYRE_TPC_ACCEPT` | `[tool.mipyre] tpc_accept` | listener | how thread-per-core threads take connections on macOS: an accept loop each on one shared socket, or one fanout acceptor |
| `gc_mode` | `--gc-mode` | `MIPYRE_GC_MODE` | `[tool.mipyre] gc_mode` | count | when a sub-interpreter worker runs gc.collect() |
| `gc_threshold` | `--gc-threshold` | `MIPYRE_GC_THRESHOLD` | `[tool.mipyre] gc_threshold` | 100000 | requests between collections in count mode; 0 never collects |
| `gc_idle_ms` | `--gc-idle-ms` | `MIPYRE_GC_IDLE_MS` | `[tool.mipyre] gc_idle_ms` | 100 | the lull, in milliseconds, after which idle mode collects |
| `gc_oom_failsafe` | `--gc-oom-failsafe` | `MIPYRE_GC_OOM_FAILSAFE` | `[tool.mipyre] gc_oom_failsafe` | 50000 | requests after which idle mode collects even without a lull; 0 collects only in lulls |
| `bridge_workers` | `--bridge-workers` | `MIPYRE_BRIDGE_WORKERS` | `[tool.mipyre] bridge_workers` | 4 (thread-per-core) / 512 (tpc off) | most threads that run main-interpreter def routes |
| `bridge_capacity` | `--bridge-capacity` | `MIPYRE_BRIDGE_CAPACITY` | `[tool.mipyre] bridge_capacity` | 16 per main-interpreter thread | main-interpreter requests queued (async def: queued or running) before 503 |
| `async_queue_capacity` | `--async-queue-capacity` | `MIPYRE_ASYNC_QUEUE_CAPACITY` | `[tool.mipyre] async_queue_capacity` | 128 per pool worker | async def requests queued before 503 |
| `metrics` | `--metrics` | `MIPYRE_METRICS` | `[tool.mipyre] metrics` | off | count every answer and sample RSS |
| `host` | `--host` | `MIPYRE_HOST` | `[tool.mipyre] host` | 127.0.0.1 | the address to bind |
| `port` | `--port` | `MIPYRE_PORT` | `[tool.mipyre] port` | 8000 | the port to bind |
| `tls_cert` | `--tls-cert` | `MIPYRE_TLS_CERT` | `[tool.mipyre] tls_cert` | none (plain HTTP) | the TLS certificate chain (PEM); given with tls_key |
| `tls_key` | `--tls-key` | `MIPYRE_TLS_KEY` | `[tool.mipyre] tls_key` | none (plain HTTP) | the TLS private key (PEM); given with tls_cert |
| `extra_tls_ports` | `--extra-tls-ports` | `MIPYRE_EXTRA_TLS_PORTS` | `[tool.mipyre] extra_tls_ports` | none | more ports served over TLS |
| `extra_plain_ports` | `--extra-plain-ports` | `MIPYRE_EXTRA_PLAIN_PORTS` | `[tool.mipyre] extra_plain_ports` | none | more ports served over plain HTTP |
| `allowed_hosts` | `--allowed-hosts` | `MIPYRE_ALLOWED_HOSTS` | `[tool.mipyre] allowed_hosts` | localhost and IP addresses | DNS names this server is reached by, besides localhost and IP addresses ('*' for any) |
| `trusted_proxies` | `--trusted-proxies` | `MIPYRE_TRUSTED_PROXIES` | `[tool.mipyre] trusted_proxies` | none: the TCP peer is the client | reverse proxies (IP addresses and CIDR blocks) whose X-Forwarded-For names the client |
| `isolate_dir` | `--isolate-dir` | `MIPYRE_ISOLATE_DIR` | `[tool.mipyre] isolate_dir` | mipyre in the user's cache directory | where each sub-interpreter worker keeps its copy of an isolated extension |
| `process.workers` | `--process-workers` | `MIPYRE_PROCESS_WORKERS` | `[tool.mipyre.process] workers` | one per physical core | worker processes for runtime="process" routes |
| `process.queue_per_worker` | `--process-queue-per-worker` | `MIPYRE_PROCESS_QUEUE_PER_WORKER` | `[tool.mipyre.process] queue_per_worker` | 128 | requests each process worker holds before 503 |
| `process.slot_bytes` | `--process-slot-bytes` | `MIPYRE_PROCESS_SLOT_BYTES` | `[tool.mipyre.process] slot_bytes` | 262144 | the largest request and answer a process worker takes, in bytes |
| `process.start` | `--process-start` | `MIPYRE_PROCESS_START` | `[tool.mipyre.process] start` | import | how a process worker gets the app: import it, or fork from a template that did |
| `process.timeout` | `--process-timeout` | `MIPYRE_PROCESS_TIMEOUT` | `[tool.mipyre.process] timeout` | 30 | a process handler's time, in seconds; past it the worker is killed and the request answered 504 |
| `process.spin_us` | `--process-spin-us` | `MIPYRE_PROCESS_SPIN_US` | `[tool.mipyre.process] spin_us` | 50 | how long a process worker spins for the next request, in microseconds, before it sleeps |
| `process.host_failure` | `--process-host-failure` | `MIPYRE_PROCESS_HOST_FAILURE` | `[tool.mipyre.process] host_failure` | isolate | what a failed process lane or host does: take it out of service, or end the server |
| `reload` | `--reload` | `MIPYRE_RELOAD` | `[tool.mipyre] reload` | off | restart the app when its files change |
| `log` | `--log` | `MIPYRE_LOG` | `[tool.mipyre] log` | off; on under mipyre dev | turn Python logging on |
