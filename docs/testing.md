# Testing your app

`mipyre.testing` is for integration tests of your handlers: each request a test sends goes through the same server as in production (HTTP parsing, routing, hooks, CORS, compression), and each route runs on the runtime it declares, worker processes included. A test sees what a client would see.

Two classes, used three ways:

| | What it is | Use it for |
|---|---|---|
| `TestClient(app)` | an HTTP and WebSocket client that serves the app itself, in a background thread, on a server that binds no socket | a test, or a module of tests, that owns its server |
| `TestServer(app)` | the app served in a background thread on a real TCP port, as `mipyre run` serves it | one server for the whole test session; a test that needs a client address or a port; any other client (httpx, requests, a browser, a subprocess) |
| `TestClient(server)` | a client of a running `TestServer`, with its own cookies | a fresh client per test against the session's server |

Every example on this page ran with pytest 9.1, pytest-xdist 3.8, pytest-timeout 2.4 and httpx on macOS, and each output block is what that run printed; the same tests pass on Linux x86_64, under `pytest -n 2` and `--timeout-method=thread` too.

## A first test

An app with routes on three runtimes. `app.py`:

```python
from mipyre import Cookie, Mipyre, Response
from mipyre.cookies import get_cookie

app = Mipyre(routers=["reports:router"])

ORDERS = {"1": "filled", "2": "open"}


@app.get("/orders/{order_id}")
def order(req, order_id):
    return {"id": order_id, "status": ORDERS[order_id]}


@app.post("/login", runtime="main")
def login(req):
    user = req.json()["user"]
    return Response({"user": user}, headers={"set-cookie": Cookie("user", user)})


@app.get("/me", runtime="main")
def me(req):
    return {"user": get_cookie(req, "user")}


if __name__ == "__main__":
    app.run()
```

`reports.py`, a numpy route on worker processes:

```python
import os

import numpy as np

from mipyre import Router

router = Router(runtime="process")


@router.get("/report")
def report(req):
    fills = np.array([101.5, 102.0, 100.5])
    return {"average": float(fills.mean()), "pid": os.getpid()}
```

`pyproject.toml`, so the tests can import the app:

```toml
[tool.pytest.ini_options]
pythonpath = ["."]
```

`tests/test_orders.py`:

```python
import os

from mipyre.testing import TestClient

from app import app


def test_every_runtime():
    with TestClient(app) as client:
        assert client.get("/orders/1").json() == {"id": "1", "status": "filled"}

        client.post("/login", body={"user": "ada"})
        assert client.get("/me").json() == {"user": "ada"}

        report = client.get("/report").json()
        assert report["average"] == 101.33333333333333
        assert report["pid"] != os.getpid()
```

```bash
pytest -q
```

```text
.                                                                        [100%]
1 passed in 0.56s
```

`/orders/1` ran in a sub-interpreter, `/login` and `/me` on the main interpreter, and `/report` in a worker process, which is why its `pid` isn't the test's. Leaving the `with` block stops the server: the shutdown hooks run and the worker processes exit.

Define the app at module level, as here and as in production: the sub-interpreters and the worker processes rebuild it by executing the module that created it. An app created inside a test function can only be served with `Mipyre(runtime="main")`.

## The client

`TestClient` has `get`, `post`, `put`, `delete`, `patch`, `options` and `head`, each taking:

- `body=`: a `dict` is sent as JSON with `Content-Type: application/json`; `str` and `bytes` are sent as given.
- `headers=`, `params=` (added to the query string), `timeout=` (seconds; the client's default is 10, set with `TestClient(app, timeout=...)`).

A response has `status_code`, `headers` (lower-case names; a header sent more than once, such as `set-cookie`, is a list), `get_header_list(name)`, `body` (bytes), `text`, `json()`, `ok` (status below 400) and `raise_for_status()`. Redirects are followed unless the client is made with `follow_redirects=False`.

Each client has its own cookie jar: cookies the server sets are sent back on that client's later requests, as `/login` and `/me` show above.

`client.websocket_connect(path)` opens a WebSocket (it needs the `websockets` package). For an echo route:

```python
@app.websocket("/ws")
def echo(ws):
    while (msg := ws.recv_message()) is not None:
        ws.send(f"echo: {msg}")
```

a test reads:

```python
with client.websocket_connect("/ws") as ws:
    ws.send("ping")
    assert ws.recv() == "echo: ping"
```

`TestClient(app)` talks to its server over in-memory connections: real sockets inside the test process, with no address and no port. The difference shows only where a route looks for them:

```python
@app.get("/whoami")
def whoami(req):
    return {"client_ip": req.client_ip, "host": req.headers.get("host")}
```

```python
from mipyre.testing import TestClient

from app import app

with TestClient(app) as client:
    print(client.base_url, client.get("/whoami").json())
    client.port
```

Besides the banner the server prints as it starts and stops, this prints:

```text
http://testserver {'client_ip': None, 'host': 'testserver'}
Traceback (most recent call last):
  ...
AttributeError: an in-memory server listens on no port; serve the app with TestServer(app) for a real TCP listener
```

A test that needs a client address, a port, or a client other than `TestClient` uses `TestServer`.

## `TestServer`: a real port

`TestServer(app, host="127.0.0.1", port=0)` serves the app the way `mipyre run` does and returns once every worker is serving. With `port=0` (the default) the kernel picks a free port, so test runs never collide; `server.port` is the one it bound and `server.url` is `http://127.0.0.1:<port>`. Any client reaches it:

```python
import httpx

from mipyre.testing import TestServer

from app import app

with TestServer(app) as server:
    print(server.url, server.port)
    print(httpx.get(f"{server.url}/orders/2").json())
```

prints, after the server's banner:

```text
http://127.0.0.1:56042 56042
{'id': '2', 'status': 'open'}
```

`server.close()`, or leaving the `with` block, stops it: the shutdown hooks run and the port is free again. `TestClient(server)` is a client of the server; closing the client leaves the server running.

## One server for the session, a fresh client per test

Starting a server takes most of a test's time: it builds one sub-interpreter per worker thread, and the first server of an app with process routes also starts their worker processes. Start one for the session and give each test its own client. `tests/conftest.py`:

```python
import pytest

from mipyre.testing import TestClient, TestServer

from app import app


@pytest.fixture(scope="session")
def server():
    with TestServer(app) as server:
        yield server


@pytest.fixture
def client(server):
    with TestClient(server) as client:
        yield client
```

`tests/test_orders.py`:

```python
import os

import httpx


def test_order(client):
    assert client.get("/orders/1").json() == {"id": "1", "status": "filled"}


def test_login(client):
    client.post("/login", body={"user": "ada"})
    assert client.get("/me").json() == {"user": "ada"}


def test_each_test_has_its_own_cookies(client):
    assert client.get("/me").json() == {"user": None}


def test_report_runs_in_a_worker_process(server):
    report = httpx.get(f"{server.url}/report").json()
    assert report["average"] == 101.33333333333333
    assert report["pid"] != os.getpid()
```

```bash
pytest -q --durations=3
```

```text
....                                                                     [100%]
============================= slowest 3 durations ==============================
0.51s setup    tests/test_orders.py::test_order
0.01s call     tests/test_orders.py::test_report_runs_in_a_worker_process
0.01s teardown tests/test_orders.py::test_report_runs_in_a_worker_process
4 passed in 0.55s
```

The server started once, in the first test's setup; the cookie `test_login` got didn't reach the next test.

The server reads its settings from the environment and from `[tool.mipyre]` in `pyproject.toml`, as `mipyre run` does. Fewer server threads make each server start faster; for tests that start a server each, set `MIPYRE_THREADS=1`. Two tests that each start their own server (here, the ones in [Flags in tests](#flags-in-tests)) took, on an 18-CPU Mac:

| | first server (starts the worker processes) | second server |
|---|---|---|
| default threads (6) | 0.52 s | 0.22 s |
| `MIPYRE_THREADS=1` | 0.40 s | 0.10 s |

**An app with process routes is served by one server at a time.** While the session's server runs, `TestClient(app)` or a second `TestServer(app)` of the same app fails to start:

```text
RuntimeError: process lanes could not start: the process lanes are already serving another server of this app; a second server with runtime='process' routes needs its own app
```

Use the session's server in every test of that run, or start a server per test and no session server. An app without process routes can have several servers at once.

## Each runtime

The test server runs every runtime, as production does:

- `subinterp` and `isolated` routes run in the server's sub-interpreters, inside the test process.
- `main` routes and WebSocket handlers run on the test process's main interpreter.
- `process` routes run in worker processes, sized and configured from `[tool.mipyre.process]`, `MIPYRE_PROCESS_*` variables or `Mipyre(process=ProcessLanes(...))`, as in production. The worker processes start from a template that a test server always starts as a new interpreter, on every platform, so they don't depend on what else runs in the test process: pytest-xdist's and pytest-timeout's threads don't stop them from starting (see [`pytest -n`](#running-tests-in-parallel-pytest--n)).

The limits of each runtime ([runtimes.md](runtimes.md)) hold in tests too: a `process` route that uses `app.state` gets a `500`, as it would in production.

## When a handler fails

An exception a handler doesn't catch isn't raised in the test. The test gets what a client gets: a `500` whose body carries the request id, and the traceback goes to the server's log, which pytest shows with the failing test. `/orders/3` has no order:

```python
def test_unknown_order_is_404(client):
    r = client.get("/orders/3")
    assert r.status_code == 404
```

```text
F                                                                        [100%]
=================================== FAILURES ===================================
__________________________ test_unknown_order_is_404 ___________________________

client = <mipyre.testing.TestClient object at 0x106430830>

    def test_unknown_order_is_404(client):
        r = client.get("/orders/3")
>       assert r.status_code == 404
E       assert 500 == 404
E        +  where 500 = TestResponse(status_code=500, body=b'{"detail":"Internal Server Error","request_id":"538ef38e745e71680000000000000000"...n', 'server': 'Mipyre/2.0.0', 'connection': 'close', 'content-length': '82', 'date': 'Wed, 21 Oct 2026 20:08:26 GMT'}).status_code

tests/test_missing.py:3: AssertionError
---------------------------- Captured stdout setup -----------------------------
  Process lanes: 6 workers (template pid 55556, timeout 30000 ms, queue 128/worker; BLAS (numpy, scipy): 1 thread per worker; polars: 1 thread per worker)

  Mipyre v2.0.0 [TPC mode, hybrid-inline]
  Listening on http://127.0.0.1:56059
  TPC threads: 6 (CPUs: 18, not pinned: this platform has no CPU pinning)
  Routes: 1 sub-interp + 2 GIL + 1 process + 0 async

----------------------------- Captured stderr call -----------------------------
{"timestamp":"2026-10-21T20:08:26.372415Z","level":"ERROR","fields":{"message":"request failed","request_id":"538ef38e745e71680000000000000000","method":"GET","path":"/orders/3","error":"the handler raised KeyError: '3'","traceback":"Traceback (most recent call last):\n  File \".../.venv/lib/python3.14/site-packages/mipyre/_bind.py\", line 46, in bound\n    return fn(req, **req.params)\n  File \".../v/app.py\", line 11, in order\n    return {\"id\": order_id, \"status\": ORDERS[order_id]}\n                                      ~~~~~~^^^^^^^^^^\nKeyError: '3'\n"},"target":"mipyre::handler"}
--------------------------- Captured stdout teardown ---------------------------

  Shutting down gracefully...
=========================== short test summary info ============================
FAILED tests/test_missing.py::test_unknown_order_is_404 - assert 500 == 404
1 failed in 0.54s
```

The `request_id` in the body and in the log line is the same, so a test can find the line that belongs to its request. To answer `404`, the handler raises `HTTPException(404)`; a test then sees that status, as a client would.

## When the server fails

The server's own failures are raised in the test:

- **It can't start.** A startup hook raises, the port is taken, or a worker can't import the app: `TestServer(app)` or `TestClient(app)` raises that error, and leaves no thread and no bound port behind. With `@app.on_startup` raising `ConnectionError("broker at 10.0.0.5:7497 refused the connection")`:

  ```text
  E       ConnectionError: broker at 10.0.0.5:7497 refused the connection
  FAILED t/test_d.py::test_index - ConnectionError: broker at 10.0.0.5:7497 ref...
  1 failed in 0.03s
  ```

- **It fails while stopping.** A shutdown hook raises, or a worker thread outlives the shutdown grace period (`AbandonedWorkersError`): `close()` raises it once. For the session's server, pytest reports it as an error at teardown, so the run isn't green. With `@app.on_shutdown` raising `OSError("could not write orders.log")`:

  ```text
  E       OSError: could not write orders.log
  ERROR t/test_d.py::test_index - OSError: could not write orders.log
  1 passed, 1 error in 0.39s
  ```

- **It stops on its own mid-test.** The next request raises `RuntimeError("TestClient: request failed: ...")` whose `__cause__` is the reason the server stopped, and `close()` raises that reason.

## Flags in tests

Flags declared with `mipyre.flags` ([configuration.md](configuration.md#declaring-flags)) are parsed by `mipyre run`. A test doesn't parse a command line: a server it starts (`TestClient` or `TestServer`) gives every flag its declared default, and `flags.override`, entered around the server, gives it another value. Move the desk name into a flag, `settings.py`:

```python
from mipyre import flags

DESK = flags.string("desk", "main", "the trading desk this server books for")
```

and read it in both routes: `from settings import DESK`, then `"desk": DESK.value` in the answer of `/orders/{order_id}` in `app.py` and of `/report` in `reports.py`. Without an override, every route answers with the default, `"desk": "main"`, in every runtime. The test runner's own command line (`pytest -q tests`) is never read as the app's flags. Outside a running server, a test that reads `DESK.value` with no override in effect gets `UnparsedFlagError`, as a script does before it calls `flags.parse()`.

For the session's server, enter the override in the session fixture, before the server starts. `tests/conftest.py`:

```python
import pytest

from mipyre import flags
from mipyre.testing import TestClient, TestServer

from app import app


@pytest.fixture(scope="session")
def server():
    with flags.override(desk="test"), TestServer(app) as server:
        yield server


@pytest.fixture
def client(server):
    with TestClient(server) as client:
        yield client
```

Every runtime reads the override: the sub-interpreters, the main interpreter and the worker processes.

```python
def test_order(client):
    assert client.get("/orders/1").json() == {"id": "1", "status": "filled", "desk": "test"}


def test_report_runs_in_a_worker_process(server):
    report = httpx.get(f"{server.url}/report").json()
    assert report["desk"] == "test"
    assert report["pid"] != os.getpid()
```

A server keeps the values it started with until it stops, so an override can't be entered or left while one runs. A test that tries it while the session's server runs fails:

```text
mipyre.flags.FlagOverrideError: flags.override(desk) entered while 1 server(s) hold the flag values: enter it before the server (or TestClient) starts
```

Tests that need other values each start their own server inside the override, as a decorator of the test function or a `with` block. They can't run beside the session's server, so keep them in a directory of their own and run it separately (`pytest tests_flags`). `tests_flags/test_desk.py`:

```python
from mipyre import flags
from mipyre.testing import TestClient

from app import app


@flags.override(desk="night")
def test_night_desk():
    with TestClient(app) as client:
        assert client.get("/orders/2").json()["desk"] == "night"
        assert client.get("/report").json()["desk"] == "night"


def test_london_desk():
    with flags.override(desk="london"), TestClient(app) as client:
        assert client.get("/report").json()["desk"] == "london"
```

```bash
pytest -q tests_flags
```

```text
..                                                                       [100%]
2 passed in 0.79s
```

Leaving an override puts back what was there before. The decorator works on a sync test function; in an `async def` test, use the `with` form.

## Running tests in parallel: `pytest -n`

With pytest-xdist, each worker is a process of its own with its own session server on its own port:

```bash
pytest -n 2 tests
```

```text
============================= test session starts ==============================
platform darwin -- Python 3.14.7, pytest-9.1.1, pluggy-1.7.0
rootdir: .../shop
configfile: pyproject.toml
plugins: xdist-3.8.0, timeout-2.4.0, anyio-4.15.1
created: 2/2 workers
2 workers [4 items]

....                                                                     [100%]
============================== 4 passed in 1.15s ===============================
```

Process routes work under `pytest -n` and under `pytest --timeout-method=thread`, on Linux and macOS: a test server starts its worker processes' template as a new interpreter, whatever threads the test process runs, as `python app.py` and `mipyre dev` do ([runtimes.md](runtimes.md#process)).
