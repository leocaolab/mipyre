# Build a fortune app with mipyre

A step-by-step guide to mipyre 2.0.0: where your code runs, how to configure the server, and how to test and deploy an app.

## Before you begin

### What you'll build

A small web service backed by PostgreSQL:

- `GET /fortunes`: reads every row of a `fortune` table, adds one more message at request time, sorts them by message, and renders an HTML table with every message escaped.
- `GET /fortunes.json`: the same rows as JSON.
- `POST /fortunes`: adds a fortune, validated with pydantic.
- `GET /fortunes/latest`: the three newest fortunes, read with an `async` query.
- `GET /lucky/{name}`: six lucky numbers for a name, drawn with numpy.
- Health probes, Prometheus metrics, an access log, and a test suite.

### What you'll learn

- How to start a server with `python app.py`, `mipyre run` and `mipyre dev`.
- How to use the PostgreSQL pool (`mipyre.db.PgPool`), and which calls work where.
- When to run a route on `subinterp`, `main`, `process` or `isolated`, and how to declare it.
- How to configure workers, process workers, limits, TLS, logging, metrics and health probes.
- How to test an app with `TestServer` and `TestClient`.
- How to run it in production.

### What you'll need

- macOS on Apple silicon or Linux x86_64.
- Python 3.14: the regular build, not the free-threaded one.
- A PostgreSQL server you can create a database on, plus its `createdb` and `psql` commands.
- `curl` and `openssl`.

Estimated time: 45 minutes.

## 1. Set up the project

**Goal:** a virtual environment with mipyre and the libraries this guide uses.

```bash
mkdir fortune && cd fortune
python3.14 -m venv .venv
source .venv/bin/activate
pip install mipyre pydantic numpy pytest watchfiles
mipyre --version
```

**Checkpoint:**

```text
mipyre 2.0.0
```

`pydantic`, `numpy` and `pytest` are only needed for the steps that use them. `watchfiles` lets `mipyre dev` watch files efficiently; without it, the reloader polls.

## 2. Serve your first route

**Goal:** start a server three ways, and know which one to use.

Create `app.py`:

```python
from mipyre import Mipyre

app = Mipyre()


@app.get("/")
def index(req):
    return {"service": "fortune", "status": "ok"}


if __name__ == "__main__":
    app.run()
```

A handler takes the request first, then any path parameters by name. A returned `dict` or `list` is sent as JSON, a `str` as text, and `bytes` as `application/octet-stream`. Return `Response(body, status_code=..., content_type=..., headers=...)` to choose the status code, content type or headers.

Start it:

```bash
python app.py
```

**Checkpoint:** the server prints its address and how it serves. The thread and CPU counts depend on your machine; the `not pinned` note appears on macOS, which has no CPU pinning:

```text
  Mipyre v2.0.0 [TPC mode, hybrid-inline]
  Listening on http://127.0.0.1:8000
  TPC threads: 6 (CPUs: 18, not pinned: this platform has no CPU pinning)
  Routes: 1 sub-interp + 0 GIL + 0 process + 0 async
```

In a second terminal:

```bash
curl -s localhost:8000/
```

```text
{"service":"fortune","status":"ok"}
```

Press Ctrl-C in the first terminal. The server stops taking new connections, finishes the requests in flight, and prints `Shutting down gracefully...`.

`TPC threads: 6` means *thread per core*: one thread per performance core on Apple silicon, or per physical core the process may use on Linux. Each thread accepts connections, parses HTTP and runs your handlers in its own Python sub-interpreter, which has its own GIL. `app.py` is executed once by the main interpreter and once by each sub-interpreter, so keep module-level code to imports, setup and route definitions.

There are three ways to start the same app:

| Command | Use it for |
|---|---|
| `python app.py` | quick runs; calls `app.run()`. |
| `mipyre run app:app` | production. It takes the flags in step 12. |
| `mipyre dev app:app` | development: restarts the server when a `.py` file in the app's directory changes, and turns on the access log. |

Try the reloader. Start it:

```bash
mipyre dev app:app
```

Change `"ok"` to `"ready"` in `app.py` and save. The server restarts, and in the second terminal:

```bash
curl -s localhost:8000/
```

```text
{"service":"fortune","status":"ready"}
```

Stop it with Ctrl-C. `mipyre routes` prints the route table without starting a server:

```bash
mipyre routes app:app
```

```text
METHOD  PATH  HANDLER  FLAGS
------  ----  -------  -----------------
GET     /     index    runtime=subinterp

1 route(s)
```

## 3. Create the fortune database

**Goal:** a `fortune` table with a few rows to serve.

Tell the PostgreSQL tools where your server is. Change these to match your server; if it needs a password, also set `PGPASSWORD`, and put the password in `DATABASE_URL` as `postgresql://user:password@host:port/fortune`.

```bash
export PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres
createdb fortune
export DATABASE_URL="postgresql://$PGUSER@$PGHOST:$PGPORT/fortune"
```

Create `schema.sql`:

```sql
CREATE TABLE fortune (
    id serial PRIMARY KEY,
    message text NOT NULL
);

INSERT INTO fortune (message) VALUES
    ('fortune: No such file or directory'),
    ('A computer scientist is someone who fixes things that aren''t broken.'),
    ('After enough decimal places, nobody gives a damn.'),
    ('<script>alert("This should not be displayed in a browser alert box.");</script>'),
    ('Any program that runs right is obsolete.'),
    ('フレームワークのベンチマーク');
```

Load it:

```bash
psql -d fortune -q -f schema.sql
psql -d fortune -c "SELECT count(*) FROM fortune"
```

**Checkpoint:**

```text
 count
-------
     6
(1 row)
```

One of the messages is a `<script>` tag. The HTML page in step 5 must show it as text, not run it.

## 4. Connect the pool and serve JSON

**Goal:** read the table through `PgPool` from a route that runs on sub-interpreters.

Create `db.py`:

```python
import os

from mipyre.db import PgPool

pool = PgPool.connect(os.environ["DATABASE_URL"], max_connections=20)
```

`PgPool.connect()` connects nothing yet: it records the DSN and the settings. When the server starts, it opens one pool per DSN and hands it to every interpreter that imports `db.py` (the main interpreter and each sub-interpreter), so `max_connections=20` is the limit for the whole server, not for each sub-interpreter; the pool closes when the server stops. Two `connect()` calls to the same DSN with different settings stop the start with `ValueError`, naming the settings that differ. The other settings and their defaults:

| Setting | Default | Meaning |
|---|---|---|
| `max_connections` | 10 | connections in the pool |
| `acquire_timeout_secs` | 30 | how long a query waits for a free connection |
| `max_lifetime_secs` | 1800 | a connection is closed after this long (0: never) |
| `idle_timeout_secs` | 300 | an idle connection is closed after this long (0: never) |
| `ping_on_acquire` | `False` | test each connection with a round trip before using it |
| `runtime_threads` | one per 4 CPUs, at least 2 | threads that run the server's queries |

Without `ping_on_acquire`, the first query on a connection that the server or network closed while it sat idle raises `DatabaseError` carrying the real error (`.sqlstate` is the server's code, or `None` when the socket simply closed). mipyre doesn't retry it, because the statement may have run. The pool then replaces that connection.

Create `pages.py`. The routes go in a `Router`, a group of routes in a module of its own:

```python
import html

from mipyre import Response, Router

from db import pool

router = Router()

EXTRA = "Additional fortune added at request time."


def fortunes():
    rows = pool.fetch_all("SELECT id, message FROM fortune")
    rows.append({"id": 0, "message": EXTRA})
    rows.sort(key=lambda row: row["message"])
    return rows


@router.get("/fortunes.json")
def fortunes_json(req):
    return fortunes()
```

`fetch_all` returns a list of dicts, one per row. Its siblings are `fetch_one` (a dict, or `None` when there is no row), `fetch_scalar` (the first column of the first row), `execute` (the number of rows affected) and `fetch_iter` (a cursor that yields rows one at a time, for large results). Parameters are written `$1`, `$2`, … and sent as the types the statement declares.

Replace `app.py` with an app that lists the router:

```python
from mipyre import Mipyre

app = Mipyre(routers=["pages:router"])

if __name__ == "__main__":
    app.run()
```

The app lists routers by import string, not by object. Each runtime imports only the routers it serves, which matters from step 7 on.

Run it:

```bash
python app.py
```

In the second terminal:

```bash
curl -s localhost:8000/fortunes.json
```

**Checkpoint:** seven fortunes, sorted by message, including the one with `id` 0 added at request time:

```text
[{"id":4,"message":"<script>alert(\"This should not be displayed in a browser alert box.\");</script>"},{"id":2,"message":"A computer scientist is someone who fixes things that aren't broken."},{"id":0,"message":"Additional fortune added at request time."},{"id":3,"message":"After enough decimal places, nobody gives a damn."},{"id":5,"message":"Any program that runs right is obsolete."},{"id":1,"message":"fortune: No such file or directory"},{"id":6,"message":"フレームワークのベンチマーク"}]
```

The sort is by code point, so `<` sorts before letters and upper case before lower case.

A sync query such as `fetch_all` releases the worker's GIL while it waits for PostgreSQL, so a slow query doesn't stop the other workers. It works on the sub-interpreters and on the main interpreter. Step 6 covers where it doesn't.

Stop the server with Ctrl-C.

## 5. Render the HTML table

**Goal:** the same rows as an HTML page, with every message escaped.

Add this route to the end of `pages.py`:

```python
@router.get("/fortunes")
def fortunes_html(req):
    cells = "".join(
        f"<tr><td>{row['id']}</td><td>{html.escape(row['message'])}</td></tr>"
        for row in fortunes()
    )
    page = (
        "<!DOCTYPE html><html><head><title>Fortunes</title></head><body>"
        f"<table><tr><th>id</th><th>message</th></tr>{cells}</table></body></html>"
    )
    return Response(page, content_type="text/html; charset=utf-8")
```

A `str` body is sent as `text/plain` unless you name the content type, so the route passes `content_type`. `html.escape` turns `<`, `>`, `&`, `"` and `'` into entities; mipyre has no template engine and doesn't escape for you.

Run `python app.py` again and fetch the page:

```bash
curl -s -i localhost:8000/fortunes
```

**Checkpoint:**

```text
HTTP/1.1 200 OK
content-type: text/html; charset=utf-8
server: Mipyre/2.0.0
content-length: 704
date: ...

<!DOCTYPE html><html><head><title>Fortunes</title></head><body><table><tr><th>id</th><th>message</th></tr><tr><td>4</td><td>&lt;script&gt;alert(&quot;This should not be displayed in a browser alert box.&quot;);&lt;/script&gt;</td></tr><tr><td>2</td><td>A computer scientist is someone who fixes things that aren&#x27;t broken.</td></tr><tr><td>0</td><td>Additional fortune added at request time.</td></tr><tr><td>3</td><td>After enough decimal places, nobody gives a damn.</td></tr><tr><td>5</td><td>Any program that runs right is obsolete.</td></tr><tr><td>1</td><td>fortune: No such file or directory</td></tr><tr><td>6</td><td>フレームワークのベンチマーク</td></tr></table></body></html>
```

The `<script>` row arrives as `&lt;script&gt;`, so a browser shows it as text. Stop the server.

## 6. Choose where each route runs

**Goal:** know which runtime each kind of route needs before you write it.

Every route runs in one of four runtimes:

| `runtime=` | Where the handler runs | Parallel? |
|---|---|---|
| `"subinterp"` (default) | the sub-interpreter of the thread that took the request | yes: one GIL per thread |
| `"main"` | the server's main interpreter | no: one GIL shared by every `main` route |
| `"process"` | worker processes beside the server, reached through shared memory | yes: one process per worker |
| `"isolated"` | the sub-interpreters, each loading its own copy of extensions that don't support sub-interpreters | yes, at the cost of memory and risk |

Start from `subinterp` and move a route only when it needs something the sub-interpreters can't give it:

| The route needs | Use | Why |
|---|---|---|
| pure Python, the standard library, sync `PgPool` calls, `app.state` | `subinterp` | it runs in parallel on every core |
| a C extension that can't load in a sub-interpreter (pydantic, numpy, pandas, polars) for computation on the request alone | `process` | the extension loads normally in a worker process, and the work still runs in parallel |
| the `*_async` `PgPool` calls, a streaming response (`Stream`), or an object that must exist exactly once | `main` | these only work on the main interpreter |
| such an extension on a low-traffic route that also needs the database or `app.state` (a validated write, an admin page) | `main` | process routes can't use either; one GIL is enough for a few requests |
| such an extension at high traffic, together with the database or `app.state` | `isolated` | a last resort: each worker loads its own copy of the extension with CPython's sub-interpreter check turned off, which uses more memory per worker and is safe only if the extension keeps no process-wide state |

What mipyre enforces, so you find out at startup or on the first request, not in production:

- **An extension that can't load in a sub-interpreter** stops the server at startup, naming the import and the choices. For example, with `from pydantic import BaseModel` at the top of `pages.py`:

  ```text
  ImportError: worker 0: .../app.py raised while the worker started: ImportError: module pydantic_core._pydantic_core does not support loading in subinterpreters
  pydantic can't load in a sub-interpreter (it loads pydantic_core._pydantic_core); imported at .../pages.py:1  from pydantic import BaseModel
  Routes that use it need one of:
    1. runtime='process': ...
    2. runtime='main': ...
    3. runtime='isolated': ...
  ```

  A sub-interpreter imports every module that has routes it serves, so such a library must not be imported at the top of those modules. Put the routes that need it in their own module with a `Router` of the right runtime (steps 7 and 8).
- **`body=` and `query=` validation** use pydantic. With pydantic-core from PyPI, which doesn't load in a sub-interpreter, a `subinterp` route that declares either raises `RuntimeDeclarationError` when the route is declared, naming the ways out; an `isolated` route is not refused.
- **The `*_async` `PgPool` methods** raise `NotImplementedError` outside the main interpreter: `fetch_all_async is not available in sub-interpreter workers; use fetch_all (sync) or route with runtime='main'`.
- **A `process` route** can't use `PgPool` or `app.state`: both raise `RuntimeError`, because a worker process has its own copy of the server's state. It gets the request and returns a response.

Other things that always run on the main interpreter: WebSocket handlers (one thread per connection), the health probes (unless you pass `runtime=`), the MCP endpoint and the OpenAPI document.

## 7. Add a validated write on `main`

**Goal:** a pydantic route and an `async` query, both on the main interpreter, without the sub-interpreters ever importing pydantic.

Create `submit.py`:

```python
from pydantic import BaseModel, Field

from mipyre import Response, Router

from db import pool

router = Router(runtime="main")


class NewFortune(BaseModel):
    message: str = Field(min_length=1, max_length=500)


@router.post("/fortunes", body=NewFortune)
def add_fortune(body):
    new_id = pool.fetch_scalar(
        "INSERT INTO fortune (message) VALUES ($1) RETURNING id", body.message
    )
    return Response({"id": new_id, "message": body.message}, status_code=201)


@router.get("/fortunes/latest")
async def latest(req):
    return await pool.fetch_all_async(
        "SELECT id, message FROM fortune ORDER BY id DESC LIMIT 3"
    )
```

With `body=`, the handler receives the validated model instead of the request; a body that fails validation is answered `422` with pydantic's errors and the handler doesn't run. The `async def` route awaits the query on the main interpreter's event loop.

List the router in `app.py`:

```python
from mipyre import Mipyre

app = Mipyre(routers=["pages:router", "submit:router"])

if __name__ == "__main__":
    app.run()
```

Run `python app.py`, then:

```bash
curl -s -i -X POST localhost:8000/fortunes -H 'content-type: application/json' -d '{"message": "Mipyre was here."}'
curl -s -X POST localhost:8000/fortunes -H 'content-type: application/json' -d '{"message": ""}'
echo
curl -s localhost:8000/fortunes/latest
```

**Checkpoint:**

```text
HTTP/1.1 201 Created
content-type: application/json
server: Mipyre/2.0.0
content-length: 37
date: ...

{"id":7,"message":"Mipyre was here."}{"detail":[{"type":"string_too_short","loc":["message"],"msg":"String should have at least 1 character","ctx":{"min_length":1}}],"error_count":1}
[{"id":7,"message":"Mipyre was here."},{"id":6,"message":"フレームワークのベンチマーク"},{"id":5,"message":"Any program that runs right is obsolete."}]
```

The banner now counts the routes on the main interpreter as `GIL`:

```text
  Routes: 2 sub-interp + 2 GIL + 0 process + 0 async
```

The sub-interpreters import `pages.py` only; `submit.py`, and pydantic with it, is imported by the main interpreter alone. Stop the server.

## 8. Add a numpy route on `process`

**Goal:** a numpy computation that runs in parallel in worker processes.

Create `lucky.py`:

```python
import zlib

import numpy as np

from mipyre import Router

router = Router(runtime="process")


@router.get("/lucky/{name}")
def lucky(req, name):
    rng = np.random.default_rng(zlib.crc32(name.encode()))
    numbers = rng.choice(np.arange(1, 50), size=6, replace=False)
    return {"name": name, "numbers": sorted(int(n) for n in numbers)}
```

The route needs only the request, which is what a process route can have. It seeds the generator from the name, so a name always gets the same numbers.

List it in `app.py`:

```python
from mipyre import Mipyre

app = Mipyre(routers=["pages:router", "submit:router", "lucky:router"])

if __name__ == "__main__":
    app.run()
```

The worker processes start from a template process. `python app.py` starts it after it imports the app, as a new interpreter, so the threads this app's imports start (numpy's and the pool's) don't reach it. Run it again:

```bash
python app.py
```

```text
  Process lanes: 6 workers (template pid 54106, timeout 30000 ms, queue 128/worker; BLAS (numpy, scipy): 1 thread per worker; polars: 1 thread per worker)

  Mipyre v2.0.0 [TPC mode, hybrid-inline]
  Listening on http://127.0.0.1:8000
  TPC threads: 6 (CPUs: 18, not pinned: this platform has no CPU pinning)
  Routes: 2 sub-interp + 2 GIL + 1 process + 0 async
```

In the second terminal:

```bash
curl -s localhost:8000/lucky/ada
```

**Checkpoint:**

```text
{"name":"ada","numbers":[10,13,24,27,33,39]}
```

What to know about process routes:

- A worker that runs past the handler timeout (30 s by default) is killed and replaced, and the client gets `504`. A worker's queue holds 128 requests by default; past it, the client gets `503`.
- A request body and a response each travel through a 256 KiB slot by default. Raise `slot_bytes` (step 10) for bigger ones.
- Tests serve process routes too (step 11).

Stop the server with Ctrl-C.

## 9. Declare the runtime

**Goal:** know where a runtime can be set, and what wins.

A route's runtime comes from the first of these that names one:

1. the route: `@app.get("/x", runtime="main")` or `@router.get("/x", runtime="main")`;
2. its router: `Router(runtime="process")`;
3. the app: `Mipyre(runtime="subinterp")`;
4. `"subinterp"`.

It is the code's alone: no `mipyre run` flag, environment variable or `pyproject.toml` key changes it. List the routes:

```bash
mipyre routes app:app
```

**Checkpoint:**

```text
METHOD  PATH              HANDLER        FLAGS
------  ----------------  -------------  -----------------------------
GET     /fortunes.json    fortunes_json  runtime=subinterp
GET     /fortunes         fortunes_html  runtime=subinterp
POST    /fortunes         add_fortune    runtime=main,body=NewFortune
GET     /fortunes/latest  latest         runtime=main,async
GET     /lucky/{name}     lucky          runtime=process

5 route(s)
```

`pages.py` names no runtime, so its routes take the app's. An app that a deployment should be able to move reads a setting of its own and passes it in. In `app.py`, change the `Mipyre(...)` line to:

```python
import os

from mipyre import Mipyre

app = Mipyre(
    runtime=os.environ.get("FORTUNE_RUNTIME", "subinterp"),
    routers=["pages:router", "submit:router", "lucky:router"],
)
```

and compare:

```bash
FORTUNE_RUNTIME=main mipyre routes app:app
```

**Checkpoint:** the `pages` routes have moved to `main`, while the routers that name a runtime keep theirs:

```text
METHOD  PATH              HANDLER        FLAGS
------  ----------------  -------------  -----------------------------
GET     /fortunes.json    fortunes_json  runtime=main
GET     /fortunes         fortunes_html  runtime=main
POST    /fortunes         add_fortune    runtime=main,body=NewFortune
GET     /fortunes/latest  latest         runtime=main,async
GET     /lucky/{name}     lucky          runtime=process

5 route(s)
```

This guide keeps its routes where step 6 put them, so pin the app's runtime in code:

```python
app = Mipyre(runtime="subinterp", routers=["pages:router", "submit:router", "lucky:router"])
```

`MIPYRE_RUNTIME` is not one of mipyre's settings, and a deployment that sets it doesn't start:

```bash
MIPYRE_RUNTIME=main mipyre run app:app
```

```text
mipyre: MIPYRE_RUNTIME is set, but it is not one of mipyre's settings (`mipyre run --help` lists them); unset it, or rename it if it is your own (MIPYRE_TEST_* and MIPYRE_BENCH_* are left to test and benchmark tools)
```

An unknown runtime name anywhere is an error too.

`Mipyre(runtime="main")` serves every route on the main interpreter, with no sub-interpreters, unless a route asks for `subinterp` or `isolated`.

## 10. Configure the server

**Goal:** set threads, process workers, limits, logging, metrics and health probes, each in the right place.

Settings live in two places:

- **Code** (`app.py`): what belongs to the app wherever it runs: runtimes, limits, logging format, probes, metrics.
- **The deployment**: the server's settings (address, port, sizes, TLS, process workers). Each one is a `mipyre run` flag, a `MIPYRE_*` environment variable and a key in `[tool.mipyre]` of the `pyproject.toml` in the directory you start the server in. They can be given in code too (`app.run(port=...)`, `ProcessLanes(...)`); a deployment value overrides the code's, the flag first, then the environment, then `pyproject.toml`, and the start prints each value the deployment gave.

Every mistake stops the server at startup, all at once, each naming where it came from: a value that doesn't parse, a `MIPYRE_*` variable or `[tool.mipyre]` key that is no setting, and a value the server doesn't use.

### Threads and the server model

| Setting | Flag / argument | Environment | Default |
|---|---|---|---|
| address | `--host` / `host=` | `MIPYRE_HOST` | `127.0.0.1` |
| port | `--port` / `port=` | `MIPYRE_PORT` | `8000` |
| server threads (one sub-interpreter each) | `--threads` / `threads=` | `MIPYRE_THREADS` | performance cores (Apple silicon) or physical cores the process may use (Linux) |
| server model | `--tpc` | `MIPYRE_TPC` | `1` |
| sub-interpreter pool (`MIPYRE_TPC=0` only) | `--subinterpreters` / `subinterpreters=` | `MIPYRE_SUBINTERPRETERS` | logical CPUs |

The default, thread per core, answers each request on the thread that accepted it; keep it unless you have measured a reason not to. `MIPYRE_TPC=0` switches to a different server: `threads` threads do the I/O and hand requests to a pool of `subinterpreters` sub-interpreters (both default: logical CPUs):

```bash
MIPYRE_TPC=0 mipyre run app:app --subinterpreters 4
```

```text
  Mipyre v2.0.0 [hybrid mode]
  Listening on http://127.0.0.1:8000
  Sub-interpreters: 4 | IO threads: 18 (CPUs: 18)
```

A size the chosen server doesn't use stops the start. `mipyre run app:app --subinterpreters 4`, on the default server:

```text
mipyre.engine.ConfigError: subinterpreters is given (mipyre run --subinterpreters), but the thread-per-core server does not use it: it is used only by the sub-interpreter pool (tpc off, with sub-interpreter routes); the thread-per-core server runs one sub-interpreter per thread, sized by threads
```

### Process workers

Create `pyproject.toml` in the project directory:

```toml
[tool.mipyre.process]
workers = 2
timeout = 5
```

Start the server with `mipyre run app:app`. **Checkpoint:**

```text
  Process lanes: 2 workers (template pid ..., timeout 5000 ms, queue 128/worker; BLAS (numpy, scipy): 1 thread per worker; polars: 1 thread per worker)
  process.workers=2 (.../pyproject.toml:2: [tool.mipyre.process] workers)
  process.timeout=5s (.../pyproject.toml:3: [tool.mipyre.process] timeout)
```

Each setting comes from the first place that sets it: a `mipyre run` flag, the environment, `[tool.mipyre.process]`, `Mipyre(process=ProcessLanes(...))` in code, the default.

| Setting | Flag | Environment | `[tool.mipyre.process]` | Default |
|---|---|---|---|---|
| worker processes | `--process-workers` | `MIPYRE_PROCESS_WORKERS` | `workers` | one per physical core |
| handler timeout | `--process-timeout` (seconds) | `MIPYRE_PROCESS_TIMEOUT` (seconds) | `timeout` (seconds) | 30 s |
| queue per worker before `503` | `--process-queue` | `MIPYRE_PROCESS_QUEUE` | `queue_per_worker` | 128 |
| largest request or response | `--process-slot-bytes` | `MIPYRE_PROCESS_SLOT_BYTES` | `slot_bytes` | 262144 (256 KiB) |
| how a worker gets the app | `--process-start` | `MIPYRE_PROCESS_START` | `start` | `import` |
| what a failed worker host does | `--process-host-failure` | `MIPYRE_LANE_HOST_FAILURE` | `host_failure` | `isolate` |

`start = "import"` starts each worker without the app and lets it import the app itself, which is safe whatever the app does at import. `"fork"` imports the app once in a template and forks the workers from it: faster to replace and less memory, for apps whose workers are replaced often; the template must be single-threaded when it forks, or the start fails naming the threads. `host_failure = "isolate"` takes a failed part out of service and keeps serving with the rest; `"exit"` stops the server at the first failure. Either way the process exits non-zero when it ends because of one, so a supervisor restarts it.

### Limits

Limits are set on the app. Add these lines to `app.py`, after `app = Mipyre(...)`:

```python
app.max_body_size = 64 * 1024
app.request_timeout = 10
```

| Attribute | Default | Past it |
|---|---|---|
| `max_body_size` | 10 MB | `413` |
| `request_timeout` | 30 s, for the body to arrive and again for the handler to answer | `408` for the body, `504` for the handler |
| `body_min_rate` | 1024 bytes/s: an upload earns one more second per this many bytes, so a steady upload isn't cut off | `408` |
| `body_time_limit` | 600 s, the most an upload may take | `408` |
| `interrupt_grace` | 2 s for a sync handler interrupted at `request_timeout` to end before its worker is replaced | |
| `shutdown_timeout` | 5 s for each thread to finish at shutdown | |
| `max_websocket_message_size` | 1 MiB | the connection is closed with `1009` |
| `max_websocket_connections` | 1024 | `503` |

The server also closes a connection that sends no request head within 10 s, and one that stops reading a response for 10 s. These two are fixed.

### Logging and the access log

By default the server logs errors only, as JSON lines, without an access log. Change that in code with `Mipyre(log_config={...})`, for example `log_config={"level": "INFO", "format": "text"}` for colored text lines in a terminal. `log_config` takes `level` (`"OFF"`, `"ERROR"`, `"WARN"`, `"INFO"`, `"DEBUG"`, `"TRACE"`), `format` (`"json"` or `"text"`) and `access_log` (`True` or `False`). `Mipyre(debug=True)` switches the defaults to text at DEBUG with the access log on. `MIPYRE_LOG=1` in the environment, and `mipyre dev`, turn the access log on and raise the level to INFO.

The access log writes one line per response, including `404`s and responses that never reach Python. At high traffic, sample it, and keep every error. Add to `app.py`:

```python
app.enable_access_log(sample=100, always_log_status=400)
```

This logs about 1 in 100 responses, plus every response with status 400 or higher. Turning the access log on also raises the log level to INFO if it was ERROR or OFF, so the server's own INFO lines (startup, shutdown) appear too.

### Metrics and health probes

Add to `app.py`:

```python
app.enable_metrics()
probes = HealthProbes(app)


@probes.check("db")
def database_ready():
    pool.fetch_scalar("SELECT 1")
```

and import `HealthProbes` and the pool at the top of `app.py`:

```python
from mipyre import HealthProbes, Mipyre

from db import pool
```

`enable_metrics()` serves Prometheus metrics at `/metrics`: every response by method and status class, a latency histogram, and the server's own counters (workers, accept failures, timeouts). The engine counts each response as it leaves, on every worker and every runtime. `HealthProbes(app)` serves `/livez`, which always answers `200`, and `/readyz`, which runs every readiness check and answers `503` if one raises, returns `False`, or takes more than 10 s. The reason a check failed goes to the log, not to the unauthenticated response.

`app.py` now reads:

```python
from mipyre import HealthProbes, Mipyre

from db import pool

app = Mipyre(runtime="subinterp", routers=["pages:router", "submit:router", "lucky:router"])

app.max_body_size = 64 * 1024
app.request_timeout = 10
app.enable_access_log(sample=100, always_log_status=400)
app.enable_metrics()
probes = HealthProbes(app)


@probes.check("db")
def database_ready():
    pool.fetch_scalar("SELECT 1")


if __name__ == "__main__":
    app.run()
```

Start `mipyre run app:app` and check it:

```bash
curl -s localhost:8000/livez; echo
curl -s localhost:8000/readyz; echo
curl -s localhost:8000/metrics | grep '^mipyre_http_requests_total'
python -c 'print("x" * 70000)' | curl -s -i -X POST localhost:8000/fortunes -H 'content-type: application/json' --data-binary @- | head -1
```

**Checkpoint:**

```text
{"status": "alive"}
{"status": "ready", "checks": {"db": {"ok": true}}}
mipyre_http_requests_total{method="GET",status_class="2xx"} 2
HTTP/1.1 413 Payload Too Large
```

The 70 001-byte body is over the 64 KiB limit, so it's refused with `413` before any handler runs. The server's terminal shows its access line:

```text
{"timestamp":"...","level":"INFO","fields":{"message":"Request handled","method":"POST","path":"/fortunes","status":413,"latency_us":8,"source":"tpc-main-bridge","request_id":"...","client_ip":"127.0.0.1"},"target":"mipyre::access"}
```

Stop the server.

## 11. Test the app

**Goal:** integration tests of every route, on the runtime it runs on in production.

`TestServer(app)` serves the app in a background thread of the test process, the way `mipyre run` does: on a real port the kernel picks, with the routes on their runtimes, worker processes included, and its settings from the environment and `pyproject.toml`. `TestClient(server)` is a client of it. Starting a server builds its workers, so the tests share one for the whole session and give each test a fresh client with its own cookies.

Add to `pyproject.toml`, so the tests can import `app`:

```toml
[tool.pytest.ini_options]
pythonpath = ["."]
```

Create `tests/test_fortunes.py`:

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


def test_html_escapes_every_message(client):
    r = client.get("/fortunes")
    assert r.status_code == 200
    assert r.headers["content-type"] == "text/html; charset=utf-8"
    assert "&lt;script&gt;" in r.text
    assert "<script>" not in r.text


def test_json_is_sorted_and_has_the_extra_row(client):
    messages = [row["message"] for row in client.get("/fortunes.json").json()]
    assert messages == sorted(messages)
    assert "Additional fortune added at request time." in messages


def test_empty_message_is_rejected(client):
    r = client.post("/fortunes", body={"message": ""})
    assert r.status_code == 422


def test_lucky_numbers_are_the_same_for_a_name(client):
    first = client.get("/lucky/ada").json()
    assert first == client.get("/lucky/ada").json()
    assert len(first["numbers"]) == 6


def test_ready(client):
    assert client.get("/readyz").json() == {"status": "ready", "checks": {"db": {"ok": True}}}
```

The tests import the app from `app.py`, defined at module level as the sub-interpreters and the worker processes need, and reach the database through `DATABASE_URL` from step 3. `client.post(body=dict)` sends JSON; `params=` adds a query string; responses have `status_code`, `headers`, `body`, `text`, `json()`.

Run the tests:

```bash
pytest -q
```

**Checkpoint:**

```text
.....                                                                    [100%]
5 passed in 0.64s
```

`/lucky/ada` ran in one of the two worker processes from step 10, `/fortunes` in a sub-interpreter, the validated write on the main interpreter. A handler that raises shows up in a test as the `500` a client would get, with the traceback in the server's log. `TestClient(app)` serves an app on its own, over in-memory connections; [Testing your app](../testing.md) covers it, flags in tests, and `pytest -n`.

## 12. Run it in production

**Goal:** start the server the way a service manager would, and serve TLS.

Use `mipyre run` in production, bound to the interface your load balancer reaches:

```bash
mipyre run app:app --host 0.0.0.0 --port 8000
```

`mipyre run` takes a flag for every server setting: `--host`, `--port`, `--threads`, `--tls-cert`, `--tls-key`, the `--process-*` flags from step 10 and the rest. `mipyre run --help` lists them, each with its variable and default.

**Shutdown.** `SIGTERM` (what systemd and Kubernetes send) and `SIGINT` stop the server the same way: it stops accepting, lets the requests in flight finish, runs the `@app.on_shutdown` hooks and exits. A thread still busy after `shutdown_timeout` (5 s) is reported, and the process exits without waiting for it.

**TLS.** Give the server a PEM certificate and key; it then serves HTTPS, with HTTP/2 negotiated over ALPN. For a local test, make a self-signed certificate:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=localhost" -keyout key.pem -out cert.pem
mipyre run app:app --tls-cert cert.pem --tls-key key.pem --port 8443
```

In the second terminal:

```bash
curl -s -k --http2 -I https://localhost:8443/fortunes | head -1
```

**Checkpoint:** the response came over HTTP/2:

```text
HTTP/2 200
```

The same can be set with `MIPYRE_TLS_CERT` and `MIPYRE_TLS_KEY`. Set both or neither: one without the other is an error.

**Socket activation.** When systemd starts the server with sockets from a `.socket` unit (`LISTEN_FDS` and `LISTEN_PID` set for this process), mipyre serves those sockets instead of binding its own, prints `Serving the sockets systemd handed over (LISTEN_FDS)`, and applies its TLS settings to them. List every address in the `.socket` unit: a host, a port or extra ports given beside handed-over sockets stop the start.

## What you've built

- A fortune service with an escaped HTML page, a JSON API, a validated write and an async read, all on one PostgreSQL pool.
- Routes on three runtimes, each where it has to be: page rendering in parallel on the sub-interpreters, pydantic and async queries on the main interpreter, numpy in worker processes.
- Configuration in its right place: runtimes, limits, logging, metrics and probes in code; sizes and TLS on the command line; process workers in `pyproject.toml`.
- Integration tests of every route, process ones included, and a production start with graceful shutdown and TLS.

The [`examples/`](../../examples) directory has more apps: Server-Sent Events, WebSocket, an MCP server, a REST API with auth and uploads, and a trading API with numpy and pandas.
