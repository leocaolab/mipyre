# mipyre

A Python web framework whose HTTP server is written in Rust and whose Python handlers run in parallel on every core, inside one process.

```bash
pip install mipyre
```

Python 3.14 (the regular build with the GIL). Wheels for Linux x86_64 and macOS arm64.

## How it works

The Rust engine ([tokio](https://tokio.rs) and [hyper](https://hyper.rs)) owns the sockets, parses HTTP/1.1 and HTTP/2, enforces limits and writes responses. Python only runs your handlers.

By default the server runs one thread per physical core. Each thread owns a Python **sub-interpreter with its own GIL** ([PEP 684](https://peps.python.org/pep-0684/)), so handlers on different cores never wait for each other: one process, many GILs, no `fork` and no copy of your app per core. A request is accepted, parsed and answered on the same core.

Not every library can load in a sub-interpreter yet, so each route declares where it runs:

| `runtime=` | Where the handler runs | Use it for |
|---|---|---|
| `"subinterp"` (default) | the sub-interpreter of the core that took the request | pure Python, the standard library, extensions that support sub-interpreters |
| `"process"` | worker processes beside the server, reached through shared memory | C extensions that don't support sub-interpreters: numpy, pandas, polars, pydantic |
| `"main"` | the server's main interpreter (one GIL) | a few stateful routes: streaming responses (`Stream`), async database calls, objects that must live in one place |
| `"isolated"` | sub-interpreters that each load their own copy of an extension | a compatibility option when `process` doesn't fit; uses more memory per worker |

Workers share state through `app.state`, a concurrent map that lives in Rust and is visible to every interpreter and core. Queues are bounded: when the server is full it answers `503` instead of growing without limit.

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


@router.post("/order", model=Order)
def order(body):
    return {"ticker": body.ticker, "quantity": body.quantity}
```

```python
# app.py
from mipyre import Mipyre

app = Mipyre(routers=["models:router"])
```

Each runtime imports only the routers it serves. Apps with `runtime="process"` routes start with `mipyre run`.

## Configuration

Everything can be set in code (`app.run(...)`, `Mipyre(...)`), on the `mipyre run` command line, or through the environment. The most used variables:

| Variable | Meaning | Default |
|---|---|---|
| `MIPYRE_HOST` / `MIPYRE_PORT` | address and port | `127.0.0.1` / `8000` |
| `MIPYRE_WORKERS` | worker threads (one sub-interpreter each) | physical cores |
| `MIPYRE_RUNTIME` | the deployment's default runtime | `subinterp` |
| `MIPYRE_TPC` | `0` switches from thread-per-core to a multi-thread server with a sub-interpreter pool | on |
| `MIPYRE_PROCESS_WORKERS` | worker processes for `runtime="process"` | physical cores |
| `MIPYRE_PROCESS_TIMEOUT_MS` | a process handler's time limit; the worker is replaced and the client gets `504` | `30000` |
| `MIPYRE_TLS_CERT` / `MIPYRE_TLS_KEY` | PEM certificate and key; turns on TLS with HTTP/2 over ALPN | unset |
| `MIPYRE_LOG` | `1` turns logging on | off |
| `MIPYRE_RELOAD` | `1` turns hot reload on | off |

An invalid value in a `MIPYRE_*` variable, or a runtime declared one way in code and another in the deployment, stops the server at startup with the reason.

Limits on the request path have safe defaults and setters on the app: body size (10 MB, `413`), request timeout (30 s), minimum upload rate, header read timeout, and a write-stall timeout for clients that stop reading.

## Features

- **Routing**: all HTTP methods, path parameters (`{id}`, `{*rest}`) injected by name, query parameters, `Router` modules, a fallback route, prebuilt constant responses served without Python.
- **Handlers**: `def` and `async def`; request context (`mipyre.context.ctx`); request ids.
- **Validation**: `model=` for JSON bodies and `query=` for query strings with pydantic; `422` with the reasons.
- **OpenAPI 3.1** document from the routes and models: `app.enable_openapi()`.
- **Middleware**: `before_request` / `after_request` hooks, `on_startup` / `on_shutdown`.
- **Security**: CORS, cookies with header-injection checks, CSRF checks on RPC and MCP, redirects that refuse CR/LF.
- **Responses**: gzip and brotli compression, static files with ETag / `304` and precompressed `.br` / `.gz`.
- **Bodies and streams**: multipart uploads, streaming request bodies (`stream=True`), Server-Sent Events (`Stream`), WebSocket.
- **Services**: RPC with JSON, MsgPack or Protobuf (`@app.rpc`, `RPCClient`); an MCP endpoint for AI agents (`@app.mcp.tool`, `resource`, `prompt`).
- **Data**: a PostgreSQL pool (`mipyre.db.PgPool`), a per-worker JSON cache (`@cached_json`), shared state (`app.state`).
- **Operations**: `/livez` and `/readyz` probes, Prometheus metrics, JSON or text logs, an access log with sampling (`app.enable_access_log()`), graceful shutdown, systemd socket activation, TLS, HTTP/2.
- **Development**: hot reload, `mipyre routes`, an in-memory `TestClient` with WebSocket support.

## Examples

[`examples/`](examples) has runnable apps: a minimal API, sub-interpreters, SSE, WebSocket, an AI agent server with MCP tools, a full-stack REST API with auth and uploads, and a trading API with numpy and pandas.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues: report them privately through the repository's Security tab.

## License

[Apache License 2.0](LICENSE).
