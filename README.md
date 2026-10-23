# mipyre

A Python web framework whose HTTP server is written in Rust and whose Python handlers run in parallel on every core, inside one process.

```bash
pip install mipyre
```

Python 3.14 (the regular build with the GIL). Wheels for Linux x86_64 and macOS arm64.

## Why mipyre

**It is fast.** The server is Rust; Python runs only your handler. On 4 physical cores, a handler that returns a short string or a small `dict` is served at about 470,000 requests per second, with sub-millisecond latency:

| Route | Requests/s | p50 | p99 |
|---|---:|---:|---:|
| `/plaintext` (returns `"Hello, World!"`) | 470,727 | 0.26 ms | 0.43 ms |
| `/json` (returns `{"message": "Hello, World!"}`) | 466,825 | 0.27 ms | 0.39 ms |

**It runs Python in parallel.** Each core runs its own sub-interpreter with its own GIL, so CPU-bound Python handlers don't wait for each other. The same 4 cores, serving a pure-Python handler that computes for a few milliseconds per request:

| Where the handler runs | Requests/s | Server cores used |
|---|---:|---:|
| sub-interpreters, one per core (the default) | 1,702 | 4.0 |
| a pool of 8 sub-interpreters on the multi-thread server (`MIPYRE_TPC=0`), which also uses the cores' SMT siblings | 1,978 | 8.0 |
| the main interpreter, one GIL (`runtime="main"`) | 414 | 1.1 |

With one GIL the server tops out at one core's worth of Python; with a GIL per core it uses all four, about 4 times as many requests.

**Libraries that can't run in a sub-interpreter still run in parallel.** A route declared `runtime="process"` runs in worker processes beside the server. A handler that takes the mean of a 10,000-element numpy array:

| Where the handler runs | Requests/s |
|---|---:|
| worker processes (`runtime="process"`) | 325,601 |
| the main interpreter, one GIL (`runtime="main"`) | 77,185 |

**One process.** By default there is no `fork` and no copy of your app per core: every core's sub-interpreter lives in the same server process, and they share state through `app.state`. Only `runtime="process"` routes use extra processes.

How these numbers were measured: Linux on an AMD Ryzen 7 7840HS (8 cores, 16 threads); the server pinned to 4 physical cores and their SMT siblings, `wrk` on the other 4 cores and their siblings; `wrk -t8 -c128 -d10s --latency` without pipelining, after a 2 s warm-up, against a fresh server per run; default settings otherwise; Python 3.14.7, numpy 2.5.4; each number the median of 3 runs. This measures mipyre only; it is not a comparison with other frameworks. Every mode, the raw table with min–max ranges, and the script to rerun it are in [`benchmarks/public/`](benchmarks/public/).

## How it works

The Rust engine ([tokio](https://tokio.rs) and [hyper](https://hyper.rs)) owns the sockets, parses HTTP/1.1 and HTTP/2, enforces limits and writes responses. Python only runs your handlers.

By default the server runs one thread per physical core (per performance core on Apple silicon). Each thread owns a Python **sub-interpreter with its own GIL** ([PEP 684](https://peps.python.org/pep-0684/)), so handlers on different cores never wait for each other: one process, many GILs, no `fork` and no copy of your app per core. A request is accepted, parsed and answered on the same core.

Not every library can load in a sub-interpreter yet, so each route declares where it runs:

| `runtime=` | Where the handler runs | Use it for |
|---|---|---|
| `"subinterp"` (default) | the sub-interpreter of the core that took the request | pure Python, the standard library, extensions that support sub-interpreters |
| `"process"` | worker processes beside the server, reached through shared memory | C extensions that don't support sub-interpreters: numpy, pandas, polars, pydantic. `app.state` and the PostgreSQL pool are not available there |
| `"main"` | the server's main interpreter (one GIL) | a few stateful routes: streaming responses (`Stream`), async database calls, objects that must live in one place |
| `"isolated"` | sub-interpreters that each load their own copy of an extension | a compatibility option when `process` doesn't fit; uses more memory per worker |

Workers share state through `app.state`, a concurrent map that lives in Rust and is visible to every interpreter and core. Queues are bounded: when the server is full it answers `503` instead of growing without limit.

How to choose, and what each runtime can't do: [docs/runtimes.md](docs/runtimes.md).

## Limitations

- **Libraries in sub-interpreters.** An extension module loads in a sub-interpreter only if it declares support for it. The standard library and pure-Python packages do; numpy, pandas, pyarrow, orjson and pydantic don't yet, so their routes run on `process` or `main`. [What loads where](docs/compatibility.md#which-libraries-load-in-a-sub-interpreter).
- **PyO3 extensions.** mipyre is built against an experimental fork of PyO3 that keeps PyO3's state per interpreter; upstream PyO3 refuses to load in a second interpreter with its own GIL. Your own PyO3 extension must be built against the fork to load in mipyre's sub-interpreters. [Details](docs/compatibility.md#the-pyo3-fork).
- **Per runtime.** `process` routes can't be `async def`, take `body=BodyStream`, return a `Stream`, or use `app.state` or the PostgreSQL pool; requests and responses there are limited to 256 KiB by default. `isolated` costs a copy of each library per worker. `main` is one GIL. [Details](docs/limitations.md#per-runtime).
- **Platforms.** CPython 3.14 with the GIL only: not the free-threaded 3.14t, not 3.13. Wheels for Linux x86_64 and macOS arm64.
- **Code-only settings.** A route's runtime, the app's limits, its logging and its features are set in code; no flag or variable changes them.
- **No ASGI / WSGI.** FastAPI, Starlette, Flask and Django apps can't be mounted yet; this is planned.

All of them, and the known issues: [docs/limitations.md](docs/limitations.md).

## Documentation

[docs/README.md](docs/README.md) lists the step-by-step codelabs and the reference pages: runtimes, configuration, testing, deployment, compatibility, integrations, observability and limitations.

## Examples

[examples/README.md](examples/README.md) lists the runnable apps, what each shows, the runtimes it uses and the codelab that builds it: hello world, Server-Sent Events, WebSocket, an AI agent server with MCP tools, a trading API with numpy and pandas on worker processes, and a full-stack API with validation, sessions and uploads.

## Benchmarks

[benchmarks/public/](benchmarks/public/) has the runtime-mode benchmark above: the app, the harness, how a run is made, and every result.

## Quick start

```python
# app.py
from mipyre import Mipyre

app = Mipyre()


@app.get("/")
def index(req):
    return {"message": "Hello from mipyre"}


@app.get("/hello/{name}")
def hello(req, name):
    return {"message": f"Hello, {name}!"}


@app.post("/echo")
async def echo(req):
    return req.json()


if __name__ == "__main__":
    app.run()
```

```bash
python app.py                 # http://127.0.0.1:8000
mipyre run app:app --port 8080
mipyre dev app:app            # reloads when a .py file changes
```

For a step-by-step tutorial (a PostgreSQL-backed fortune app, choosing runtimes, configuration, testing, production), see the [fortune codelab](docs/codelabs/fortune.md); the other codelabs are listed in [docs/README.md](docs/README.md).

A handler takes the request first, then any path parameters by name. It returns a `dict` or `list` (JSON), `str` (text), `bytes`, or a `Response(body, status_code=..., content_type=..., headers=...)`.

### Routes that need a C extension

A sub-interpreter imports your module too, so a library that can't load there must not be imported at the top of a module the sub-interpreters import. Put those routes in a module of their own with a `Router`, and list it in the app:

```python
# models.py
from pydantic import BaseModel
from mipyre import Router

router = Router(runtime="main")   # or "process"


class Order(BaseModel):
    ticker: str
    quantity: int


@router.post("/order", body=Order)
def order(body):
    return {"ticker": body.ticker, "quantity": body.quantity}
```

```python
# app.py
from mipyre import Mipyre

app = Mipyre(routers=["models:router"])
```

Each runtime imports only the routers it serves. Tests serve every runtime, process routes included: [docs/testing.md](docs/testing.md).

## Configuration

What belongs to the app is set in code: routes and their runtimes, limits, logging, features (`Mipyre(...)`, attributes and `enable_*` methods on the app). Each server setting can be given in code (`app.run(...)`, `ProcessLanes(...)`) and by the deployment, as a `mipyre run` / `mipyre dev` flag, a `MIPYRE_*` environment variable or a key in `[tool.mipyre]` of `pyproject.toml`; a deployment value overrides the code's, and the start prints what it replaced. The most used variables:

| Variable | Meaning | Default |
|---|---|---|
| `MIPYRE_HOST` / `MIPYRE_PORT` | address and port | `127.0.0.1` / `8000` |
| `MIPYRE_THREADS` | server threads (one sub-interpreter each) | physical cores (performance cores on Apple silicon) |
| `MIPYRE_TPC` | `0` switches from thread-per-core to a multi-thread server with a sub-interpreter pool | on |
| `MIPYRE_PROCESS_WORKERS` | worker processes for `runtime="process"` | physical cores |
| `MIPYRE_PROCESS_TIMEOUT` | a process handler's time limit, in seconds; the worker is replaced and the client gets `504` | `30` |
| `MIPYRE_TLS_CERT` / `MIPYRE_TLS_KEY` | PEM certificate and key; turns on TLS with HTTP/2 over ALPN | unset |
| `MIPYRE_LOG` | `1` turns on the access log and INFO-level logs; errors are always logged | off |
| `MIPYRE_RELOAD` | `1` turns hot reload on | off |

Every mistake stops the server at startup, all of them at once, each naming where it came from: a value that doesn't parse, a `MIPYRE_*` variable or `[tool.mipyre]` key that is not a setting, and a value given to a server that doesn't use it.

Limits on the request path have safe defaults: body size (10 MB, `413`), request timeout (30 s), minimum upload rate and total upload time can be set on the app; reading the request head and writing to a client that stops reading each time out after 10 s.

Every setting, where it is set and its default: [docs/configuration.md](docs/configuration.md).

## Features

- **Routing**: all HTTP methods, path parameters (`{id}`, `{*rest}`) injected by name, query parameters, `Router` modules, a fallback route, prebuilt constant responses served without Python.
- **Handlers**: `def` and `async def`; `HTTPException` for an error answer (`{"detail": ...}`, as every error the engine writes); request context (`mipyre.context.ctx`); request ids.
- **Validation**: `body=` for JSON bodies and `query=` for query strings with pydantic; `422` with the reasons.
- **OpenAPI 3.1** document from the routes and models: `OpenApi(app, title=..., version=...)`.
- **Middleware**: `before_request` / `after_request` hooks, `on_startup` / `on_shutdown`.
- **Security**: cross-site requests that act refused on every route unless the site is in `enable_cors(allow_origins=...)`; a `Host` allowlist (`allowed_hosts`) against DNS rebinding; `Cookie` values checked when made; redirects that refuse CR/LF; `trusted_proxies` for the client's address behind a reverse proxy.
- **Responses**: gzip and brotli compression, static files with ETag / `304` and precompressed `.br` / `.gz`.
- **Bodies and streams**: multipart uploads, streaming request bodies (`body=BodyStream`), Server-Sent Events (`Stream`), WebSocket.
- **Services**: RPC with JSON, MsgPack or Protobuf (`@rpc(app)`, `RPCClient`); an MCP endpoint for AI agents (`mcp = MCPServer(app)`, then `@mcp.tool`, `@mcp.resource`, `@mcp.prompt`).
- **Data**: a PostgreSQL pool (`mipyre.db.PgPool`), a per-worker JSON cache (`@cached_json`), shared state (`app.state`).
- **Operations**: `/livez` and `/readyz` probes, Prometheus metrics, JSON or text logs, an access log with sampling (`app.enable_access_log()`), graceful shutdown, systemd socket activation, TLS, HTTP/2.
- **Development**: hot reload, `mipyre routes`; command-line flags declared where they are used (`mipyre.flags`); `TestClient` and `TestServer` for integration tests of every runtime, with WebSocket support and `flags.override` ([docs/testing.md](docs/testing.md)).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues: report them privately through the repository's Security tab.

## License

[Apache License 2.0](LICENSE).
