# Build a trading API with mipyre

A step-by-step guide to running numpy, pandas and pydantic in a mipyre app without giving up parallelism: each route goes to the runtime its libraries and its data allow, and other services call the API over MsgPack RPC.

## Before you begin

### What you'll build

A trading API on simulated market data:

- `GET /market/{ticker}` and `GET /market`: quotes, on the sub-interpreters.
- `POST /order`: an order validated with pydantic and recorded in shared state; `GET /orders/{order_id}` reads it back.
- `GET /analytics/portfolio` and `GET /analytics/correlation`: portfolio analytics with numpy and pandas, in worker processes.
- `POST /rpc/get_signals`: trading signals for other services, over MsgPack RPC.
- `/ws/prices`: quotes over a WebSocket.

The finished app is [`examples/trading_api.py`](../../examples/trading_api.py) and the [`examples/trading/`](../../examples/trading) package.

### What you'll learn

- Why numpy and pandas routes run on `runtime="process"`, and what a process route can and can't use.
- Why the pydantic route runs on `runtime="main"` here, and the quote routes on the default `subinterp`.
- How to split an app into routers so each interpreter imports only what it can load.
- How to start an app with process routes, and size its worker processes.
- How to serve and call MsgPack RPC with `@rpc(app)` and `RPCClient`.

### What you'll need

- macOS on Apple silicon or Linux x86_64.
- Python 3.14: the regular build, not the free-threaded one.
- `curl`.

Estimated time: 30 minutes.

## 1. Set up the project

**Goal:** a virtual environment with mipyre and the libraries this guide uses.

```bash
mkdir trading-api && cd trading-api
python3.14 -m venv .venv
source .venv/bin/activate
pip install mipyre pydantic numpy pandas msgpack httpx websockets
mipyre --version
```

**Checkpoint:**

```text
mipyre 2.0.0
```

The server needs `pydantic`, `numpy`, `pandas` and `msgpack`. `httpx` is what `RPCClient` uses to call the server (step 5), and `websockets` is the test client for step 6.

## 2. Serve quotes on the sub-interpreters

**Goal:** the quote routes, running in parallel on every core.

The app is a package, `trading`, with each group of routes in a module of its own; steps 3 and 4 show why. Create it:

```bash
mkdir trading
touch trading/__init__.py
```

Create `trading/market.py`, the simulated market data every route uses:

```python
import random
import time

BASE_PRICES = {
    "AAPL": 230.0, "TSLA": 250.0, "NVDA": 120.0, "MSFT": 420.0, "AMZN": 185.0,
    "GOOG": 165.0, "META": 560.0, "JPM": 215.0, "V": 280.0, "WMT": 80.0,
}
TICKERS = list(BASE_PRICES)


def get_live_price(ticker: str) -> dict:
    """Return a quote for one of TICKERS; KeyError for any other ticker."""
    price = BASE_PRICES[ticker] * (1 + random.gauss(0, 0.002))
    return {
        "ticker": ticker,
        "price": round(price, 2),
        "bid": round(price * 0.999, 2),
        "ask": round(price * 1.001, 2),
        "volume": random.randint(10_000, 1_000_000),
        "timestamp": time.time(),
    }
```

The base prices are constants on purpose. Each sub-interpreter and each worker process imports this module separately; prices drawn at random at import would differ from one interpreter to the next, so two requests could see two markets.

Create `trading/main.py`:

```python
from mipyre import Mipyre, Response

from trading.market import TICKERS, get_live_price

app = Mipyre()
app.enable_cors(allow_origins="*")
app.enable_access_log()


@app.get("/")
def index(req):
    return {"service": "Mipyre Trading API", "tickers": TICKERS}


@app.get("/market/{ticker}")
def market_quote(req, ticker):
    ticker = ticker.upper()
    if ticker not in TICKERS:
        return Response({"error": f"unknown ticker {ticker!r}"}, status_code=404)
    return get_live_price(ticker)


@app.get("/market")
def market_all(req):
    return {"quotes": {t: get_live_price(t) for t in TICKERS}, "count": len(TICKERS)}
```

and `trading_api.py`, the module you start:

```python
from trading.main import app

if __name__ == "__main__":
    app.run()
```

The quote routes are pure Python, so they stay on the default runtime, `subinterp`: each server thread runs them in its own sub-interpreter with its own GIL, in parallel on every core. A handler takes the request first, then the path parameters by name.

Start the server:

```bash
python trading_api.py
```

In a second terminal:

```bash
curl -s localhost:8000/market/aapl; echo
curl -s localhost:8000/market/xyz; echo
```

**Checkpoint:** a quote, then a `404` for a ticker that isn't listed. Your prices differ, since the data is random:

```text
{"ticker":"AAPL","price":229.72,"bid":229.49,"ask":229.95,"volume":238201,"timestamp":1792586097.530881}
{"error":"unknown ticker 'XYZ'"}
```

The server's terminal shows a JSON access line for each request, with `"source":"tpc-inline"`: answered on the sub-interpreter of the thread that accepted the connection. Stop the server with Ctrl-C.

## 3. Validate orders with pydantic on `main`

**Goal:** an order route validated with pydantic that records each order where every route can read it.

The order route needs two things, and each rules out a runtime:

- **pydantic**, whose core can't load in a sub-interpreter. That rules out `subinterp`, and it means pydantic must not be imported by any module the sub-interpreters import, such as `trading/main.py`.
- **`app.state`**, the map every interpreter shares, to record the order. A `process` route can't use it: a worker process has its own copy of the server's state, and `app.state` raises `RuntimeError` there.

That leaves `runtime="main"`, the server's main interpreter. It has one GIL for every `main` route, which is fine for one low-traffic write. Create `trading/orders.py`:

```python
import json
import random
import time
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from mipyre import Response, Router, SharedState

from trading.market import BASE_PRICES, get_live_price

router = Router(runtime="main")

MAX_SLIPPAGE = 0.001


class OrderRequest(BaseModel):
    ticker: str
    side: Literal["buy", "sell"]
    quantity: int = Field(gt=0, le=100_000)
    price: float = Field(gt=0)

    @field_validator("ticker")
    @classmethod
    def known_ticker(cls, ticker: str) -> str:
        if ticker not in BASE_PRICES:
            raise ValueError(f"unknown ticker {ticker!r}")
        return ticker


def _state() -> SharedState:
    # trading.main imports this module while it builds the app, so the app is looked up
    # when a request comes, not at import.
    from trading.main import app

    return app.state


@router.post("/order", body=OrderRequest)
def submit_order(order: OrderRequest):
    state = _state()
    slippage = random.uniform(0, MAX_SLIPPAGE)
    fill_price = order.price * (1 + slippage if order.side == "buy" else 1 - slippage)
    filled = {
        "order_id": state.incr("orders:last_id", 1),
        "status": "filled",
        "ticker": order.ticker,
        "side": order.side,
        "quantity": order.quantity,
        "requested_price": order.price,
        "fill_price": round(fill_price, 2),
        "market_price": get_live_price(order.ticker)["price"],
        "slippage_bps": round(slippage * 10_000, 1),
        "timestamp": time.time(),
    }
    state[f"order:{filled['order_id']}"] = json.dumps(filled)
    return Response(filled, status_code=201)
```

With `body=OrderRequest`, the handler receives the validated order instead of the request; a body that fails validation is answered `422` with pydantic's errors, and the handler doesn't run. `state.incr` adds to a counter atomically, so two orders at the same moment never get the same id.

In `trading/main.py`, list the router. Change `app = Mipyre()` to:

```python
app = Mipyre(routers=["trading.orders:router"])
```

The app names its routers by import string, not by object, so each runtime imports only the routers it serves: the main interpreter imports `trading.orders`, and the sub-interpreters never do.

Reading an order back needs only `app.state`, so that route can run on the sub-interpreters. Add it to the end of `trading/main.py`:

```python
@app.get("/orders/{order_id}")
def get_order(req, order_id):
    order = app.state.get(f"order:{order_id}")
    if order is None:
        return Response({"error": f"no order {order_id}"}, status_code=404)
    return Response(order, content_type="application/json")
```

The order is already JSON text in `app.state`, so the route sends it as it is, naming the content type.

Run `python trading_api.py`, then:

```bash
curl -s localhost:8000/order -H 'content-type: application/json' -d '{"ticker": "AAPL", "side": "buy", "quantity": 100, "price": 230.5}'; echo
curl -s localhost:8000/order -H 'content-type: application/json' -d '{"ticker": "XYZ", "side": "hold", "quantity": 0, "price": 230.5}'; echo
curl -s localhost:8000/orders/1; echo
```

**Checkpoint:** a filled order, three validation errors, and the order read back from a sub-interpreter:

```text
{"order_id":1,"status":"filled","ticker":"AAPL","side":"buy","quantity":100,"requested_price":230.5,"fill_price":230.7,"market_price":230.23,"slippage_bps":8.7,"timestamp":1792586097.550865}
{"detail":[{"type":"value_error","loc":["ticker"],"msg":"Value error, unknown ticker 'XYZ'","ctx":{"error":"unknown ticker 'XYZ'"}},{"type":"literal_error","loc":["side"],"msg":"Input should be 'buy' or 'sell'","ctx":{"expected":"'buy' or 'sell'"}},{"type":"greater_than","loc":["quantity"],"msg":"Input should be greater than 0","ctx":{"gt":0}}],"error_count":3}
{"order_id": 1, "status": "filled", "ticker": "AAPL", "side": "buy", "quantity": 100, "requested_price": 230.5, "fill_price": 230.7, "market_price": 230.23, "slippage_bps": 8.7, "timestamp": 1792586097.550865}
```

The banner counts the order route on the main interpreter as `GIL`:

```text
  Routes: 4 sub-interp + 1 GIL + 0 process + 0 async
```

Stop the server.

## 4. Run numpy and pandas analytics on `process`

**Goal:** analytics with numpy and pandas that run in parallel, in worker processes.

numpy and pandas can't load in a sub-interpreter either. The analytics routes differ from the order route in what they need: only the request. They don't touch `app.state` or a database, so they can run on `runtime="process"`: in worker processes beside the server, which load the libraries normally and run in parallel, one request per worker at a time. The request and the response travel between the server and the worker through shared memory.

Create `trading/analytics.py`:

```python
import numpy as np
import pandas as pd

from mipyre import Router

from trading.market import get_live_price

router = Router(runtime="process")

PORTFOLIO = pd.DataFrame(
    {
        "ticker": ["AAPL", "TSLA", "NVDA", "MSFT"],
        "shares": [500, 200, 100, 300],
        "avg_cost": [145.0, 220.0, 450.0, 380.0],
    }
)
CORRELATION_DAYS = 60
DAILY_VOLATILITY = 0.02


@router.get("/analytics/portfolio")
def portfolio(req):
    positions = PORTFOLIO.assign(price=[get_live_price(t)["price"] for t in PORTFOLIO["ticker"]])
    positions["market_value"] = (positions["shares"] * positions["price"]).round(2)
    positions["pnl"] = (positions["market_value"] - positions["shares"] * positions["avg_cost"]).round(2)
    positions["pnl_pct"] = ((positions["price"] / positions["avg_cost"] - 1) * 100).round(2)
    weights = positions["market_value"] / positions["market_value"].sum()
    return {
        "positions": positions.to_dict(orient="records"),
        "summary": {
            "total_value": round(float(positions["market_value"].sum()), 2),
            "total_pnl": round(float(positions["pnl"].sum()), 2),
            "max_position_weight_pct": round(float(weights.max()) * 100, 1),
            "num_positions": len(positions),
        },
    }


@router.get("/analytics/correlation")
def correlation(req):
    tickers = list(PORTFOLIO["ticker"])
    rng = np.random.default_rng()
    returns = pd.DataFrame(rng.normal(0, DAILY_VOLATILITY, (CORRELATION_DAYS, len(tickers))), columns=tickers)
    return {
        "tickers": tickers,
        "correlation_matrix": returns.corr().round(3).values.tolist(),
        "days": CORRELATION_DAYS,
    }
```

The portfolio is a constant in the module, which each worker builds once when it imports the module. The sub-interpreters never import `trading/analytics.py`, so they never load numpy or pandas.

List the router in `trading/main.py`:

```python
app = Mipyre(routers=["trading.orders:router", "trading.analytics:router"])
```

The worker processes start from a template process. `python trading_api.py` starts it after it imports the app, as a new interpreter, so the thread pool numpy starts when it is imported doesn't reach it. Restart the server:

```bash
python trading_api.py
```

**Checkpoint:** a line about the worker processes above the banner. The number of workers defaults to one per physical core:

```text
  Process lanes: 6 workers (template pid ..., timeout 30000 ms, queue 128/worker; BLAS (numpy, scipy): 1 thread per worker; polars: 1 thread per worker)
```

Each worker gets one BLAS thread, so six workers doing linear algebra don't start six thread pools that fight over the same cores. In the second terminal:

```bash
curl -s localhost:8000/analytics/portfolio; echo
curl -s localhost:8000/analytics/correlation; echo
```

```text
{"positions":[{"ticker":"AAPL","shares":500,"avg_cost":145.0,"price":229.76,"market_value":114880.0,"pnl":42380.0,"pnl_pct":58.46},{"ticker":"TSLA","shares":200,"avg_cost":220.0,"price":250.67,"market_value":50134.0,"pnl":6134.0,"pnl_pct":13.94},{"ticker":"NVDA","shares":100,"avg_cost":450.0,"price":119.86,"market_value":11986.0,"pnl":-33014.0,"pnl_pct":-73.36},{"ticker":"MSFT","shares":300,"avg_cost":380.0,"price":421.4,"market_value":126420.0,"pnl":12420.0,"pnl_pct":10.89}],"summary":{"total_value":303420.0,"total_pnl":27920.0,"max_position_weight_pct":41.7,"num_positions":4}}
{"tickers":["AAPL","TSLA","NVDA","MSFT"],"correlation_matrix":[[1.0,0.122,0.016,-0.128],[0.122,1.0,0.144,0.015],[0.016,0.144,1.0,-0.033],[-0.128,0.015,-0.033,1.0]],"days":60}
```

The access lines in the server's terminal show `"source":"process"` for these two routes. What to know about process routes:

- A worker that runs past the handler timeout (30 s by default) is killed and replaced, and the client gets `504`. A worker's queue holds 128 requests by default; past it, the client gets `503`.
- A request and a response each travel through a 256 KiB slot by default; raise it with `--process-slot-bytes` for bigger ones.
- Tests serve process routes too: `TestServer` and `TestClient` start the worker processes on every platform ([testing.md](../testing.md)).

Size the workers for the machine with `--process-workers`. Stop the server and start it with two:

```bash
mipyre run trading_api:app --process-workers 2
```

```text
  Process lanes: 2 workers (template pid ..., timeout 30000 ms, queue 128/worker; BLAS (numpy, scipy): 1 thread per worker; polars: 1 thread per worker)
```

The same settings can go in `[tool.mipyre.process]` of `pyproject.toml` or in `MIPYRE_PROCESS_*` environment variables; the [fortune codelab](fortune.md) lists them all. Stop the server.

## 5. Serve signals over MsgPack RPC

**Goal:** a method other services call like a local function, with a compact binary encoding.

`@rpc(app)` serves a function at `POST /rpc/<its name>` (`@rpc(app, "name")` names it). The body is decoded by its `Content-Type`, MsgPack (`application/msgpack`, when `msgpack` is installed) or JSON, the function gets the decoded value, and its return value is the reply's body, in the encoding the client asked for; a failure is its HTTP status with `{"detail": ...}`.

In `trading/main.py`, add these imports at the top:

```python
import random
import time

from mipyre import rpc
```

these constants below the `app` lines:

```python
SIGNALS = ("buy", "sell", "hold")
DEFAULT_SIGNAL_TICKERS = TICKERS[:5]
```

and this method at the end:

```python
@rpc(app)
def get_signals(data):
    tickers = [t for t in data.get("tickers", DEFAULT_SIGNAL_TICKERS) if t in TICKERS]
    signals = [
        {
            "ticker": t,
            "signal": random.choice(SIGNALS),
            "confidence": round(random.uniform(0.5, 0.99), 2),
            "price": get_live_price(t)["price"],
        }
        for t in tickers
    ]
    return {"signals": signals, "timestamp": time.time()}
```

RPC methods run on the main interpreter, so a method may use any library. Like `/mcp`, an RPC endpoint refuses a body type it can't decode with `415`. A `POST` from another site's page is refused with `403` by the engine unless that site is in `enable_cors(allow_origins=...)`; this app's `"*"` trusts every site.

Start `mipyre run trading_api:app` and call the method with `RPCClient`, which sends MsgPack when `msgpack` is installed:

```bash
python -c "
from mipyre import RPCClient
with RPCClient('http://127.0.0.1:8000') as client:
    print(client.get_signals(tickers=['AAPL', 'TSLA']))
"
```

**Checkpoint:** your signals and prices differ:

```text
{'signals': [{'ticker': 'AAPL', 'signal': 'hold', 'confidence': 0.74, 'price': 228.89}, {'ticker': 'TSLA', 'signal': 'sell', 'confidence': 0.51, 'price': 250.35}], 'timestamp': 1792586097.687344}
```

`client.get_signals(**kwargs)` posts the keyword arguments as one object to `/rpc/get_signals` and returns the reply's body; a failed call raises an `RPCError` (`RPCRequestError` for a 4xx, `RPCServerError` for a 5xx) with the server's `detail`. A service without `RPCClient` can send JSON:

```bash
curl -s localhost:8000/rpc/get_signals -H 'content-type: application/json' -d '{"tickers": ["NVDA"]}'; echo
```

```text
{"signals":[{"ticker":"NVDA","signal":"buy","confidence":0.8,"price":120.19}],"timestamp":1792586891.7300742}
```

Stop the server.

## 6. Stream quotes over a WebSocket

**Goal:** a client asks for tickers and gets their quotes back on one connection.

Add `import json` at the top of `trading/main.py`, and this handler at the end:

```python
@app.websocket("/ws/prices")
def price_stream(ws):
    """Answer each text message, a comma-separated list of tickers or "all", with their
    quotes."""
    while (msg := ws.recv_message()) is not None:
        if isinstance(msg, bytes):
            continue
        wanted = TICKERS if msg.strip().lower() == "all" else [t.strip().upper() for t in msg.split(",")]
        try:
            ws.send(json.dumps({t: get_live_price(t) for t in wanted if t in TICKERS}))
        except (ConnectionError, BlockingIOError):
            ws.close()  # the client left, or stopped reading what we send
            return
```

WebSocket handlers always run on the main interpreter, each connection on a thread of its own. `recv_message()` returns `None` when the connection ends; `send()` raises `ConnectionError` when it has closed and `BlockingIOError` when the client stops reading. Like a `POST`, an upgrade from another site's page is refused with `403` unless that site is in `enable_cors(allow_origins=...)`; this app's `"*"` lets every site's pages open the feed. The [realtime codelab](realtime.md) covers WebSockets in depth.

Start `mipyre run trading_api:app`, then:

```bash
python -c "
from websockets.sync.client import connect
with connect('ws://127.0.0.1:8000/ws/prices') as ws:
    ws.send('AAPL,NVDA')
    print(ws.recv())
"
```

**Checkpoint:**

```text
{"AAPL": {"ticker": "AAPL", "price": 229.95, "bid": 229.72, "ask": 230.18, "volume": 543740, "timestamp": 1792586097.756675}, "NVDA": {"ticker": "NVDA", "price": 120.04, "bid": 119.92, "ask": 120.16, "volume": 647842, "timestamp": 1792586097.7566788}}
```

Stop the server, and print the route table, which `mipyre routes` builds without starting a server:

```bash
mipyre routes trading_api:app
```

```text
METHOD  PATH                    HANDLER       FLAGS
------  ----------------------  ------------  -------------------------------
POST    /order                  submit_order  runtime=main,body=OrderRequest
GET     /analytics/portfolio    portfolio     runtime=process
GET     /analytics/correlation  correlation   runtime=process
GET     /                       index         runtime=subinterp
GET     /market/{ticker}        market_quote  runtime=subinterp
GET     /market                 market_all    runtime=subinterp
GET     /orders/{order_id}      get_order     runtime=subinterp
POST    /rpc/get_signals        get_signals   runtime=main

8 route(s)
```

## What you've built

- A trading API where each route runs on the runtime its libraries and its data allow:

  | Route | Runtime | Why |
  |---|---|---|
  | `GET /`, `GET /market/{ticker}`, `GET /market`, `GET /orders/{order_id}` | `subinterp` | pure Python and `app.state`: parallel on every core |
  | `POST /order` | `main` | pydantic can't load in a sub-interpreter, and the route writes `app.state`, which a worker process can't reach; one GIL is enough for a low-traffic write |
  | `GET /analytics/portfolio`, `GET /analytics/correlation` | `process` | numpy and pandas can't load in a sub-interpreter, and the routes need only the request, so they run in parallel in worker processes |
  | `POST /rpc/get_signals` | `main` | always: RPC methods run on the main interpreter |
  | `/ws/prices` | main interpreter | always: WebSocket handlers run there, one thread per connection |

- An app split into routers, so the sub-interpreters import neither pydantic nor numpy nor pandas, and the worker processes import only the analytics.
- A MsgPack RPC method that other services call with `RPCClient`.

If a route needed both numpy and `app.state` at high traffic, `runtime="isolated"` is the last resort: each sub-interpreter loads its own copy of the extension, at a cost in memory and only for extensions that keep no process-wide state. The [fortune codelab](fortune.md) explains it, along with databases, configuration, tests and production. The finished app in [`examples/trading_api.py`](../../examples/trading_api.py) is the same code, with an index that lists the endpoints.
