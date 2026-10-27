# mipyre examples

Runnable apps, from a one-file hello world to APIs that run numpy, pandas and pydantic beside the sub-interpreters. Each starts on `http://127.0.0.1:8000`; run one at a time.

| Example | What it shows | Runtimes | Codelab |
|---|---|---|---|
| [`hello.py`](hello.py) | routes, path and query parameters, an `after_request` hook, a custom 404 | `subinterp` | |
| [`hello_subinterp.py`](hello_subinterp.py) | the default runtime, named explicitly | `subinterp` | |
| [`sse_demo.py`](sse_demo.py) | Server-Sent Events with `Stream`, stopping when the client leaves | `subinterp`, `main` | [realtime](../docs/codelabs/realtime.md) |
| [`websocket_demo.py`](websocket_demo.py) | a WebSocket echo for text and binary messages, refusing other sites | `subinterp` | [realtime](../docs/codelabs/realtime.md) |
| [`ai_agent_server.py`](ai_agent_server.py) | MCP tools and a resource, a pydantic chat route, an SSE answer stream, session memory in `app.state` | `subinterp`, `main` | [AI agent](../docs/codelabs/ai-agent.md) |
| [`trading_api.py`](trading_api.py) | numpy and pandas in worker processes, a pydantic order route, MsgPack RPC, a WebSocket price feed | `subinterp`, `main`, `process` | [trading](../docs/codelabs/trading.md) |
| [`fullstack_api.py`](fullstack_api.py) | CRUD, pydantic validation, cookie sessions, scrypt password hashes, file upload | `subinterp`, `main` | |

The [fortune codelab](../docs/codelabs/fortune.md) builds a PostgreSQL-backed app from scratch and covers configuration, tests and production.

## Set up

The examples need Python 3.14 (the regular build, not the free-threaded one) on macOS on Apple silicon or Linux x86_64. Get them from the repository and install what they use:

```bash
git clone https://github.com/leocaolab/mipyre
cd mipyre/examples
python3.14 -m venv .venv
source .venv/bin/activate
pip install mipyre pydantic numpy pandas msgpack httpx websockets
```

Each example below lists what it needs, if you'd rather install less: `hello.py`, `hello_subinterp.py` and `sse_demo.py` need only `mipyre`. Run the commands from the `examples` directory.

## Which runtime a route uses

Each route runs in one of mipyre's runtimes, and every example puts each route on the one it needs:

- **`subinterp`** (the default): the sub-interpreter of the server thread that took the request, one GIL per thread, in parallel on every core. For pure Python, the standard library and `app.state`.
- **`main`**: the server's main interpreter, one GIL for every `main` route. For pydantic (which can't load in a sub-interpreter) on routes that also use `app.state`, for `Stream` responses, and for anything that must exist once. `/mcp` and `@rpc(app)` methods always run here; WebSocket routes run on the runtime they are given, like any route.
- **`process`**: worker processes beside the server. For numpy, pandas and other extensions that can't load in a sub-interpreter, on routes that need only the request: no `app.state`, no database pool. An app with process routes starts with `python app.py` or `mipyre run`; `mipyre run` takes the `--process-*` flags.

A module that the sub-interpreters import must not import a library they can't load, so the examples put such routes in a `Router` module of their own and include it with `app.include_router("module:router")`.

## hello.py

Routes, parameters, an `after_request` hook that adds a header, and a custom 404. Needs `mipyre`.

```bash
python hello.py
```

```bash
curl -s localhost:8000/hello/ada; echo
curl -s -X POST localhost:8000/echo -d 'ping'; echo
curl -s 'localhost:8000/search?q=rust'; echo
curl -s localhost:8000/nope; echo
```

```text
{"message":"Hello, ada!"}
ping
{"query":"rust","user_agent":"curl/8.7.1"}
{"error":"not found","path":"/nope"}
```

## hello_subinterp.py

The same kind of app with `Mipyre(runtime="subinterp")` spelled out: the default, named to show where the app's runtime is chosen. Needs `mipyre`.

```bash
python hello_subinterp.py
```

```bash
curl -s localhost:8000/hello/ada; echo
```

```text
{"message":"Hello, ada!"}
```

## sse_demo.py

Tokens sent one by one as Server-Sent Events, the way a chat model's answer arrives. `/stream` runs on `main`, because a `Stream` works only on the main interpreter; `/` and `/fast` stay on the sub-interpreters. Needs `mipyre`.

```bash
python sse_demo.py
```

Open `http://127.0.0.1:8000/` in a browser, or:

```bash
curl -s -N localhost:8000/stream | head -6
curl -s localhost:8000/fast; echo
```

```text
id: 0
data: Hello!

id: 1
data: I

fast route still works
```

## websocket_demo.py

A WebSocket that echoes text and binary messages, with a page that uses it. mipyre refuses an upgrade whose `Origin` is another site's page with `403`, which browsers otherwise allow. Needs `mipyre`, and `websockets` for the client below.

```bash
python websocket_demo.py
```

Open `http://127.0.0.1:8000/` in a browser, or:

```bash
python -c "
from websockets.sync.client import connect
with connect('ws://127.0.0.1:8000/ws') as ws:
    ws.send('hello')
    print(ws.recv())
"
```

```text
echo: hello
```

## ai_agent_server.py

An AI agent's server, with a simulated model: MCP tools (a document search, a safe calculator, a clock) and a resource at `/mcp`, `POST /chat` validated with pydantic, `GET /stream` sending the answer as Server-Sent Events, and `GET /memory/{session_id}` reading the history the chat routes keep in `app.state`. The chat routes run on `main`, because they use pydantic and `app.state`; the memory route runs on the sub-interpreters. Needs `mipyre` and `pydantic`. The code is in the [`ai_agent/`](ai_agent) package.

```bash
python ai_agent_server.py
```

```bash
curl -s localhost:8000/chat -H 'content-type: application/json' -d '{"prompt": "What is Python?", "session_id": "ada", "max_tokens": 8}'; echo
curl -s -N 'localhost:8000/stream?prompt=Rust&session_id=ada' | head -3
curl -s localhost:8000/memory/ada; echo
curl -s localhost:8000/mcp -H 'content-type: application/json' -d '{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "calculate", "arguments": {"expression": "(2 + 3) * 4"}}}'; echo
```

```text
{"response":"I received your question about 'What is Python?'. ","session_id":"ada","tokens":8}
event: token
data: I 

{"session_id":"ada","messages":[{"role":"user","content":"What is Python?"},{"role":"assistant","content":"I received your question about 'What is Python?'. "}],"count":2}
{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"20"}],"isError":false}}
```

The stream above was cut off by `head`, so it isn't in the memory: a stream that its client leaves is not remembered.

## trading_api.py

A trading API on simulated market data: quotes on the sub-interpreters; `POST /order` validated with pydantic on `main`, because it records the order in `app.state`; portfolio analytics and a correlation matrix with numpy and pandas on `process`, because they need only the request; MsgPack RPC at `/rpc/get_signals`; a WebSocket price feed at `/ws/prices`. Needs `mipyre`, `pydantic`, `numpy`, `pandas` and `msgpack`, plus `httpx` for `RPCClient`. The code is in the [`trading/`](trading) package.

Start it with `python trading_api.py`, or with `mipyre run trading_api:app`, which also takes the `--process-*` flags:

```bash
python trading_api.py
```

```bash
curl -s localhost:8000/market/AAPL; echo
curl -s localhost:8000/order -H 'content-type: application/json' -d '{"ticker": "AAPL", "side": "buy", "quantity": 100, "price": 230.5}'; echo
curl -s localhost:8000/orders/1; echo
curl -s localhost:8000/analytics/portfolio; echo
python -c "
from mipyre import RPCClient
with RPCClient('http://127.0.0.1:8000') as client:
    print(client.get_signals(tickers=['AAPL']))
"
```

```text
{"ticker":"AAPL","price":229.72,"bid":229.49,"ask":229.95,"volume":238201,"timestamp":1792586097.530881}
{"order_id":1,"status":"filled","ticker":"AAPL","side":"buy","quantity":100,"requested_price":230.5,"fill_price":230.7,"market_price":230.23,"slippage_bps":8.7,"timestamp":1792586097.550865}
{"order_id": 1, "status": "filled", "ticker": "AAPL", "side": "buy", "quantity": 100, "requested_price": 230.5, "fill_price": 230.7, "market_price": 230.23, "slippage_bps": 8.7, "timestamp": 1792586097.550865}
{"positions":[{"ticker":"AAPL","shares":500,"avg_cost":145.0,"price":229.76,"market_value":114880.0,"pnl":42380.0,"pnl_pct":58.46}, ...],"summary":{"total_value":303420.0,"total_pnl":27920.0,"max_position_weight_pct":41.7,"num_positions":4}}
{'signals': [{'ticker': 'AAPL', 'signal': 'hold', 'confidence': 0.74, 'price': 228.89}], 'timestamp': 1792586097.687344}
```

The prices are random, so yours differ.

## fullstack_api.py

A REST API: registration and login with scrypt password hashes and a random session token in an `HttpOnly` cookie, items you create, list (paged), update and delete, and an avatar upload. The routes with pydantic bodies run on `main`, the rest on the sub-interpreters; users, sessions and items live in `app.state`, so they are gone when the server stops. Needs `mipyre` and `pydantic`. The code is in the [`fullstack/`](fullstack) package.

```bash
python fullstack_api.py
```

```bash
curl -s localhost:8000/auth/register -H 'content-type: application/json' -d '{"username": "alice", "email": "alice@example.com", "password": "secret123"}'; echo
curl -s -c cookies.txt localhost:8000/auth/login -H 'content-type: application/json' -d '{"username": "alice", "password": "secret123"}'; echo
curl -s -b cookies.txt localhost:8000/items -H 'content-type: application/json' -d '{"name": "Widget", "price": 9.99, "tags": ["new"]}'; echo
curl -s 'localhost:8000/items?page=1&per_page=10'; echo
curl -s -b cookies.txt -F 'avatar=@hello.py' localhost:8000/auth/avatar; echo
```

```text
{"status":"registered","username":"alice"}
{"status": "logged in", "username": "alice"}
{"id": 1, "name": "Widget", "price": 9.99, "tags": ["new"], "created_by": "alice", "created_at": 1792586191.4103749}
{"items":[{"id":1,"name":"Widget","price":9.99,"tags":["new"],"created_by":"alice","created_at":1792586191.4103749}],"total":1,"page":1,"per_page":10}
{"status":"uploaded","filename":"hello.py","size":1864,"content_type":"application/octet-stream"}
```
