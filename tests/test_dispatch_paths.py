"""Behaviour on every dispatch path: panics, context variables, route templates, worker imports,
streamed bodies, MCP arguments, logging, Postgres and gRPC.

Every test here runs a real server in a subprocess on the path it names, so it exercises
the dispatch code that path runs:

  - "gil"   MIPYRE_TEST_MODE=main, MIPYRE_TPC=0  → handlers on main (Tokio blocking pool)
  - "tpc"   default runtime                        → TPC inline sub-interpreters; runtime="main"
                                                     routes go through the main-interp bridge
  - "pool"  default runtime, MIPYRE_TPC=0        → sub-interp pool; runtime="main" routes on main

The panic tests need a `--features fault_injection` build (they are skipped otherwise):

    maturin develop --release --features fault_injection
    pytest tests/test_dispatch_paths.py -k panic
"""

from __future__ import annotations

import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import textwrap
import time
import uuid
from typing import Literal, Optional

import pytest

from tests._helpers import bound_port, metric_count, set_subinterpreters, settle

PYTHON = sys.executable
HOST = "127.0.0.1"

PATHS: dict[str, dict[str, str | None]] = {
    "gil": {"runtime": "main", "tpc": "0"},
    "tpc": {"runtime": None, "tpc": None},
    "pool": {"runtime": None, "tpc": "0"},
}

GENERIC_500 = "Internal Server Error"

# The request budget (`app.request_timeout`, default 30 s) and body grace (`app.body_grace`,
# default 5 s) the scripts whose tests wait for them set.
REQUEST_BUDGET_S = 1.5


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


class Reply:
    def __init__(self, status: int, body: bytes, headers: list[tuple[str, str]]):
        self.status = status
        self.body = body
        self.headers = {k.lower(): v for k, v in headers}

    def json(self):
        return json.loads(self.body)


class Server:
    """`script` served on `path`. The script's `app.run` reads its port from `APP_PORT` (see
    `RUN`) and its size from `MIPYRE_THREADS` / `MIPYRE_SUBINTERPRETERS`; the "gil" path runs
    with `MIPYRE_TEST_MODE=main`.
    The port is 0, and `port` is the one the server bound, from its startup line."""

    def __init__(
        self,
        script: str,
        path: str,
        workers: int | None = 2,
        env: dict[str, str] | None = None,
        wait: bool = True,
    ):
        self.path = path
        self._port: int | None = None
        self._final_output: str | None = None
        fd, self.script_path = tempfile.mkstemp(prefix="mipyre_dispatch_", suffix=".py")
        with os.fdopen(fd, "w") as f:
            f.write(textwrap.dedent(script))
        self.log_path = self.script_path + ".log"
        full_env = dict(os.environ)
        full_env.pop("MIPYRE_TEST_MODE", None)
        if (runtime := PATHS[path]["runtime"]) is not None:
            full_env["MIPYRE_TEST_MODE"] = runtime
        full_env["APP_PORT"] = "0"
        full_env.pop("MIPYRE_TPC", None)
        full_env.pop("MIPYRE_LOG", None)
        if (tpc := PATHS[path]["tpc"]) is not None:
            full_env["MIPYRE_TPC"] = tpc
        if workers is not None:
            set_subinterpreters(full_env, str(workers))
        full_env.update(env or {})
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                [PYTHON, self.script_path],
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=full_env,
            )
        if not wait:
            return
        # The Listening line comes once the sockets are bound: waits for it or the exit.
        try:
            self._port = bound_port(self.log, self.proc)
        except RuntimeError as e:
            raise RuntimeError(f"server ({path}) did not start:\n{self.stop()}") from e

    @property
    def port(self) -> int:
        if self._port is None:
            self._port = bound_port(self.log, self.proc)
        return self._port

    def request(
        self,
        method: str,
        target: str,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 15,
    ) -> Reply:
        conn = http.client.HTTPConnection(HOST, self.port, timeout=timeout)
        try:
            conn.request(method, target, body=body, headers=headers or {})
            r = conn.getresponse()
            return Reply(r.status, r.read(), r.getheaders())
        finally:
            conn.close()

    def get(self, target: str, **kw) -> Reply:
        return self.request("GET", target, **kw)

    def post(self, target: str, body: bytes, **kw) -> Reply:
        return self.request("POST", target, body=body, **kw)

    def log(self) -> str:
        with open(self.log_path, errors="replace") as f:
            return f.read()

    def records(self) -> list[dict]:
        """The JSON log records written so far (the non-JSON lines are stderr)."""
        return _records(self.log())

    def wait_for_records(self, pred, count: int = 1, timeout: float = 5.0) -> list[dict]:
        """The log records matching `pred`, once at least `count` of them are written (the
        log writer is non-blocking)."""
        return settle(
            lambda: [r for r in self.records() if pred(r)],
            lambda found: len(found) >= count,
            timeout=timeout,
        )

    def records_after_marker(self, pred) -> list[dict]:
        """The log records matching `pred` once everything logged before now has been
        written: a marker request (`GET /mark`, which the script logs) goes out after the
        caller's requests, and the writer keeps order, so once its line is in, so is every
        earlier one. For counting ("logged once"), where waiting for the first match would
        miss a late duplicate."""
        token = uuid.uuid4().hex
        assert self.get(f"/mark?id={token}").status == 200
        marked = self.wait_for_records(lambda r: f"log-mark {token}" in _record_text(r))
        assert marked, self.log()[-3000:]
        return [r for r in self.records() if pred(r)]

    def records_at_exit(self, pred) -> list[dict]:
        """Every record matching `pred`: the server is stopped first, and its exit is the log's fence (`run()` waits
        for every line logged before it, `flush_logs`). For a record that must not be there: no wait for
        it can say it will not come, and a line logged after a marker's would not be ruled out by one."""
        out = self.stop()
        assert self.proc.returncode >= 0, f"killed by signal {-self.proc.returncode}, the log is cut:\n{out[-4000:]}"
        return [r for r in _records(out) if pred(r)]

    def stop(self) -> str:
        if self._final_output is not None:
            return self._final_output
        if self.proc.poll() is None:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGINT)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                self.proc.wait(timeout=5)
        out = self.log()
        for p in (self.script_path, self.log_path):
            try:
                os.unlink(p)
            except FileNotFoundError:
                pass
        self._final_output = out
        return out


def _records(text: str) -> list[dict]:
    """The JSON log records in `text`."""
    out = []
    for line in text.splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if isinstance(rec, dict):
            out.append(rec)
    return out


def serve(script: str, path: str, **kw):
    """A started `Server` as a context manager; the log is shown if the body fails."""

    class _Ctx:
        def __enter__(self):
            self.srv = Server(script, path, **kw)
            return self.srv

        def __exit__(self, exc_type, exc, tb):
            out = self.srv.stop()
            if exc is not None:
                print(out[-6000:])
            return False

    return _Ctx()


RUN = """
import os
from mipyre import ServerFlags
app.run(ServerFlags(host="127.0.0.1", port=int(os.environ["APP_PORT"])))
"""


def _fields(rec: dict) -> dict:
    return rec.get("fields", rec)


def _record_text(rec: dict) -> str:
    return json.dumps(rec)


# ---------------------------------------------------------------------------
# A Rust panic on any dispatch path is logged once with the request id and
# answered 500; the thread that ran it keeps serving.
# ---------------------------------------------------------------------------

PANIC_SCRIPT = """
import os
from mipyre import Mipyre
from mipyre.engine import _fault_panic
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.get("/panic")
def panic(req):
    _fault_panic("injected-panic " + req.query_params.get("n", ""))

@app.get("/ok")
def ok(req):
    return "fine"

@app.get("/gil-panic", runtime="main")
def gil_panic(req):
    _fault_panic("injected-panic " + req.query_params.get("n", ""))

@app.get("/gil-ok", runtime="main")
def gil_ok(req):
    return "fine"

@app.get("/async-panic")
async def async_panic(req):
    _fault_panic("injected-panic " + req.query_params.get("n", ""))

@app.get("/async-ok")
async def async_ok(req):
    return "fine"

@app.get("/mark", runtime="main")
def mark(req):
    import logging
    logging.getLogger("dispatch").error("log-mark " + req.query_params["id"])
    return "marked"
""" + RUN

BRIDGE_WORKERS = 2

# (serving path, panicking route, healthy route after it)
PANIC_CASES = [
    ("tpc", "/gil-panic", "/gil-ok"),  # the main-interp bridge
    ("tpc", "/panic", "/ok"),  # TPC inline
    ("pool", "/panic", "/ok"),  # pool sync worker
    ("pool", "/gil-panic", "/gil-ok"),  # pool's main-interpreter dispatch
    ("gil", "/panic", "/ok"),  # GIL mode
    ("pool", "/async-panic", "/async-ok"),  # the async engine
    ("tpc", "/async-panic", "/async-ok"),  # TPC's async pool
]

# A Python exception answers the same generic 500; only this text proves the panic went
# through Rust's catch and was logged as one. In the async engine the panic surfaces in
# the handler's task as PyO3's PanicException, which the engine logs as the task's error.
PANIC_TEXT = "a Rust panic while running the request: "
ASYNC_PANIC_TEXT = "the request's async task raised PanicException: "


def _assert_panic_logged_once(srv: Server, text: str, request_id: str) -> None:
    payload = text.rsplit(": ", 1)[1]
    found = srv.records_after_marker(lambda r: payload in _record_text(r))
    assert len(found) == 1, f"{payload!r} logged {len(found)} times: {found}"
    assert text in _record_text(found[0]), found[0]
    fields = _fields(found[0])
    assert fields.get("request_id") == request_id, found[0]
    assert found[0].get("level") == "ERROR", found[0]


@pytest.mark.needs_feature("fault_injection")
@pytest.mark.parametrize("path,route,healthy", PANIC_CASES)
def test_panic_is_a_logged_500_and_the_thread_survives(path, route, healthy):
    # One thread of each kind (the pool has one sync and one async worker; BRIDGE_WORKERS
    # bridge threads): the N+1 panics all land on the threads that must survive them.
    # The multi-thread GIL server runs main's routes on its own threads, with no bridge to size.
    env = {} if path == "gil" else {"MIPYRE_BRIDGE_WORKERS": str(BRIDGE_WORKERS)}
    workers = 2 if path == "pool" else 1
    with serve(PANIC_SCRIPT, path, workers=workers, env=env) as srv:
        for n in range(BRIDGE_WORKERS + 1):
            r = srv.get(f"{route}?n={n}")
            assert r.status == 500, (n, r.status, r.body)
            body = r.json()
            assert body["detail"] == GENERIC_500
            prefix = ASYNC_PANIC_TEXT if route == "/async-panic" else PANIC_TEXT
            _assert_panic_logged_once(srv, f"{prefix}injected-panic {n}", body["request_id"])
        r = srv.get(healthy)
        assert (r.status, r.body) == (200, b"fine")


# ---------------------------------------------------------------------------
# A ContextVar written by an `async def` hook or handler is seen by the rest of the
# request on every path (the awaitable used to run in a copy of the request's context).
# ---------------------------------------------------------------------------

CTX_SCRIPT = """
import os
from mipyre import Mipyre, Response
from mipyre.context import ctx
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.before_request
async def tag(req):
    ctx.set("user", "u" + req.path)

@app.after_request
def expose(req, resp):
    headers = dict(resp.headers)
    headers["x-handler"] = str(ctx.get("handler"))
    headers["x-user"] = str(ctx.get("user"))
    return Response(resp.body, status_code=resp.status_code,
                    content_type=resp.content_type, headers=headers)

def sync_probe(req):
    return {"user": ctx.get("user")}

async def async_probe(req):
    ctx.set("handler", "h" + req.path)
    return {"user": ctx.get("user")}

app.get("/sync")(sync_probe)
app.get("/sync-gil", runtime="main")(sync_probe)
app.get("/async")(async_probe)
app.get("/async-gil", runtime="main")(async_probe)
""" + RUN

# TPC: inline sub-interpreter (def), the async pool (async def) and the bridge
# (runtime="main"); pool: sync worker, async engine and main-interpreter dispatch; GIL mode:
# every route on main.
CTX_ROUTES = ["/sync", "/async", "/sync-gil", "/async-gil"]


# One thread per kind of dispatch, so every request after the first of its kind reuses a
# thread: TPC one inline thread; pool one sync and one async worker; one bridge thread.
CTX_WORKERS = {"tpc": 1, "pool": 2, "gil": 1}


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_async_hook_and_handler_ctx_writes_reach_the_rest_of_the_request(path):
    # The multi-thread GIL server runs main's routes on its own threads, with no bridge to size.
    env = {} if path == "gil" else {"MIPYRE_BRIDGE_WORKERS": "1"}
    with serve(CTX_SCRIPT, path, workers=CTX_WORKERS[path], env=env) as srv:
        # Twice over: the second round runs every route on a thread that already ran the
        # async routes, whose writes must not leak into the sync ones.
        for route in CTX_ROUTES * 2:
            for _ in range(2):
                r = srv.get(route)
                assert r.status == 200, (route, r.status, r.body)
                # async before_request → sync or async handler
                assert r.json() == {"user": "u" + route}, (path, route, r.body)
                # before-hook → after-hook
                assert r.headers["x-user"] == "u" + route, (path, route, r.headers)
                # async handler → after-hook
                expected = "h" + route if route.startswith("/async") else "None"
                assert r.headers["x-handler"] == expected, (path, route, r.headers)


# ---------------------------------------------------------------------------
# Route templates take only `{name}` / `{*name}`
# ---------------------------------------------------------------------------


def test_colon_param_is_refused_whatever_the_handler_takes():
    from mipyre import Mipyre
    from mipyre.engine import MipyreApp, RuntimeChoice

    app = Mipyre()
    # A handler that takes only the request gets no injection, but the route would still
    # never match: refused all the same.
    with pytest.raises(ValueError, match=r"write it as `\{id\}`"):
        app.get("/users/:id", lambda req: "x")
    # The engine's own registration refuses it too.
    with pytest.raises(ValueError, match=r"GET /users/:id: .*write it as `\{id\}`"):
        MipyreApp().route("GET", "/users/:id", lambda req: "x", RuntimeChoice.Worker)
    # A colon inside a segment is literal text.
    app.get("/v1/items:batchGet", lambda req: "x")


def test_route_params_is_the_one_template_parser():
    from mipyre.engine import _route_params

    assert _route_params("/a/{x}/img{y}.png/{*rest}") == ["x", "y", "rest"]
    assert _route_params("/{{literal}}/{z}") == ["z"]


TEMPLATE_SCRIPT = """
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.get("/items/{item_id}")
def item(req, item_id):
    return {"item_id": item_id}

@app.get("/files/{*rest}")
def files(req, rest):
    return {"rest": rest}

@app.get("/gil-files/{*rest}", runtime="main")
async def gil_files(req, rest):
    return {"rest": rest}
""" + RUN


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_brace_params_and_catch_all_are_injected(path):
    with serve(TEMPLATE_SCRIPT, path) as srv:
        r = srv.get("/items/42")
        assert (r.status, r.json()) == (200, {"item_id": "42"})
        r = srv.get("/files/a/b%20c.txt")
        assert (r.status, r.json()) == (200, {"rest": "a/b c.txt"})
        r = srv.get("/gil-files/x/y")
        assert (r.status, r.json()) == (200, {"rest": "x/y"})


# ---------------------------------------------------------------------------
# `isojson` is required in every worker; a relative import failing in a worker
# says why
# ---------------------------------------------------------------------------


def _start_failure(script: str, path: str) -> str:
    """Runs a script whose `app.run` must fail; returns its output."""
    s = Server(textwrap.dedent(script) + RUN, path, wait=False)
    try:
        s.proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        pass
    out = s.stop()
    assert s.proc.returncode not in (None, 0), out[-3000:]
    return out


@pytest.mark.parametrize("path", ["pool", "tpc"])
def test_isojson_refused_in_a_worker_stops_the_start(path):
    # isojson imports fine on main; the script hides it only inside the workers, so the
    # start fails at the worker's own JSON check, not main's.
    script = """
    import sys
    import os
    from mipyre import Mipyre
    app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

    @app.get("/")
    def index(req):
        return {"a": 1}

    if __name__ == "__mipyre_worker__":
        sys.modules["isojson"] = None
    """
    out = _start_failure(script, path)
    assert "loading the JSON serializer failed" in out, out[-3000:]
    assert "import of isojson halted" in out, out[-3000:]


def test_relative_import_failing_in_a_worker_says_why(tmp_path):
    # A worker executes the app's file as a module of its own, outside its package: a
    # relative import there fails, and the start error now says so.
    pkg = tmp_path / "relpkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "models.py").write_text("GREETING = 'hi'\n")
    (pkg / "app.py").write_text(textwrap.dedent("""
        import os
        from .models import GREETING
        from mipyre import Mipyre, ServerFlags
        app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

        @app.get("/")
        def index(req):
            return GREETING

        if __name__ == "__main__":
            app.run(ServerFlags(
                host="127.0.0.1",
                port=int(os.environ["APP_PORT"]),
                subinterpreters=1,
            ))
    """))
    env = dict(os.environ, APP_PORT="0", MIPYRE_TPC="0")
    proc = subprocess.run(
        [PYTHON, "-m", "relpkg.app"], cwd=tmp_path, env=env,
        capture_output=True, text=True, timeout=60,
    )
    out = proc.stdout + proc.stderr
    assert proc.returncode != 0, out[-3000:]
    assert "attempted relative import" in out, out[-3000:]
    assert "outside any package, so a relative import" in out, out[-3000:]


# ---------------------------------------------------------------------------
# A streamed body's rejection is typed, and answers what a buffered body's does
# ---------------------------------------------------------------------------

STREAM_SCRIPT = """
import os
from mipyre import BodyStream, Mipyre
from mipyre.engine import BodyReadError
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.max_body_size = 1024
app.request_timeout = REQUEST_BUDGET
app.body_grace = REQUEST_BUDGET

@app.post("/buffered", runtime="main")
def buffered(req):
    return {"bytes": len(req.body)}

@app.post("/iterate", runtime="main", body=BodyStream)
def iterate(req):
    total = 0
    for chunk in req.stream:
        total += len(chunk)
    return {"bytes": total}

@app.post("/drain", runtime="main", body=BodyStream)
def drain(req):
    return {"bytes": req.stream.drain_count()}

@app.post("/catch", runtime="main", body=BodyStream)
def catch(req):
    try:
        req.stream.read()
    except OSError as e:
        return {"caught": type(e).__name__, "is_body_rejected": isinstance(e, BodyReadError),
                "text": str(e)}
    return "read it all"
""".replace("REQUEST_BUDGET", str(REQUEST_BUDGET_S)) + RUN

TOO_LARGE = {"detail": "payload too large"}


def _no_error_records(srv: Server) -> None:
    """No ERROR record, all of them read: the server is stopped first (its exit is the log's fence)."""
    errors = srv.records_at_exit(lambda r: r.get("level") == "ERROR")
    assert errors == [], errors


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_oversized_streamed_body_is_413_like_a_buffered_one(path):
    # Every route runs on the main interpreter, so no path has sub-interpreters to size.
    with serve(STREAM_SCRIPT, path, workers=None) as srv:
        body = b"x" * 4096
        buffered = srv.post("/buffered", body)
        assert (buffered.status, buffered.json()) == (413, TOO_LARGE)
        for route in ("/iterate", "/drain"):
            r = srv.post(route, body)
            assert (r.status, r.json()) == (buffered.status, buffered.json()), route
        # A handler can still catch it, as the OSError it always was.
        r = srv.post("/catch", body)
        assert r.status == 200
        assert r.json() == {
            "caught": "BodyReadError",
            "is_body_rejected": True,
            "text": "request body is larger than max_body_size",
        }
        # Within the cap, streaming is unchanged.
        assert srv.post("/iterate", b"y" * 100).json() == {"bytes": 100}
        _no_error_records(srv)


def _send_partial_body(port: int, target: str) -> tuple[int, bytes]:
    """Declares a 100-byte body, sends 10 bytes, then waits for the answer."""
    with socket.create_connection((HOST, port), timeout=60) as s:
        s.sendall(
            f"POST {target} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 100\r\n\r\n".encode()
            + b"z" * 10
        )
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
        head, _, rest = data.partition(b"\r\n\r\n")
        status = int(head.split(b" ")[1])
        return status, rest


def test_slow_streamed_body_is_408_like_a_buffered_one():
    # The body budget runs once per path, all paths at the same time.
    import concurrent.futures

    servers = {path: Server(STREAM_SCRIPT, path) for path in ("tpc", "gil")}
    try:
        with concurrent.futures.ThreadPoolExecutor(len(servers) * 2) as pool:
            futures = {
                (path, route): pool.submit(_send_partial_body, srv.port, route)
                for path, srv in servers.items()
                for route in ("/buffered", "/iterate")
            }
            results = {key: f.result() for key, f in futures.items()}
        for (path, route), (status, _) in results.items():
            assert status == 408, (path, route, status)
        for srv in servers.values():
            _no_error_records(srv)
    finally:
        for srv in servers.values():
            srv.stop()


# ---------------------------------------------------------------------------
# MCP: argument values are checked against the tool schema; hints map to real schemas
# or fail registration; non-str results are JSON
# ---------------------------------------------------------------------------


def _mcp_call(server, name: str, arguments: dict) -> dict:
    body = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    })
    return server.answer(json.loads(body))


def test_mcp_wrong_argument_type_is_32602_with_the_reason():
    from mipyre import Mipyre
    from mipyre.mcp import MCPServer

    mcp = MCPServer(Mipyre())

    @mcp.tool()
    def add(a: int, b: int) -> int:
        return a + b

    @mcp.tool()
    def pick(tags: list[str], mode: Literal["fast", "slow"], limit: int | None = None) -> str:
        return f"{tags} {mode} {limit}"

    resp = _mcp_call(mcp, "add", {"a": 1, "b": "2"})
    assert resp["error"]["code"] == -32602, resp
    assert resp["error"]["message"] == "invalid params: arguments.b: expected integer, got string"

    # bool is not an integer in JSON Schema terms
    resp = _mcp_call(mcp, "add", {"a": True, "b": 2})
    assert resp["error"]["code"] == -32602, resp
    assert "arguments.a: expected integer, got boolean" in resp["error"]["message"]

    resp = _mcp_call(mcp, "pick", {"tags": ["x", 3], "mode": "fast"})
    assert resp["error"]["code"] == -32602, resp
    assert "arguments.tags[1]: expected string, got integer" in resp["error"]["message"]

    resp = _mcp_call(mcp, "pick", {"tags": [], "mode": "medium"})
    assert resp["error"]["code"] == -32602, resp
    assert "'medium' is not one of ['fast', 'slow']" in resp["error"]["message"]

    resp = _mcp_call(mcp, "pick", {"tags": ["x"], "mode": "slow", "limit": None})
    assert resp["result"]["content"][0]["text"] == "['x'] slow None"
    assert _mcp_call(mcp, "add", {"a": 2, "b": 3})["result"]["content"][0]["text"] == "5"


def test_mcp_schema_maps_common_hints():
    from mipyre.mcp import _extract_schema

    def tool(
        a: list[int],
        b: dict[str, float],
        c: int | None,
        d: Optional[str],
        e: Literal["x", "y"],
        f: bool,
        g,
        h: list,
    ):
        pass

    props = _extract_schema(tool)["properties"]
    assert props["a"] == {"type": "array", "items": {"type": "integer"}}
    assert props["b"] == {"type": "object", "additionalProperties": {"type": "number"}}
    assert props["c"] == {"anyOf": [{"type": "integer"}, {"type": "null"}]}
    assert props["d"] == {"anyOf": [{"type": "string"}, {"type": "null"}]}
    assert props["e"] == {"enum": ["x", "y"]}
    assert props["f"] == {"type": "boolean"}
    assert props["g"] == {}  # no hint: any JSON value, not "string"
    assert props["h"] == {"type": "array"}


class Point:
    """A class with no JSON form (module level: this file's annotations are strings,
    resolved against its globals)."""


def test_mcp_unmappable_hint_is_a_registration_error_naming_the_parameter():
    from mipyre import Mipyre
    from mipyre.mcp import MCPServer

    mcp = MCPServer(Mipyre())
    with pytest.raises(TypeError, match=r"parameter 'where' is annotated .*Point.*input_schema="):
        @mcp.tool()
        def locate(name: str, where: Point) -> str:
            return name

    with pytest.raises(TypeError, match=r"parameter 'ids'"):
        @mcp.tool()
        def lookup(ids: set[int]) -> str:
            return ""

    # An explicit schema is the way out, and is what gets checked.
    @mcp.tool(input_schema={"type": "object", "properties": {"where": {"type": "object"}}, "required": ["where"]})
    def locate2(where: Point) -> str:
        return "ok"

    assert _mcp_call(mcp, "locate2", {"where": [1]})["error"]["code"] == -32602


def test_mcp_non_str_results_are_json():
    from mipyre import Mipyre
    from mipyre.mcp import MCPServer

    mcp = MCPServer(Mipyre())

    @mcp.tool()
    def items() -> list:
        return [{"id": 1, "ok": True}, None]

    @mcp.tool()
    def ratio() -> float:
        return 0.5

    @mcp.tool()
    def nothing() -> None:
        return None

    def text(name):
        return _mcp_call(mcp, name, {})["result"]["content"][0]["text"]

    assert json.loads(text("items")) == [{"id": 1, "ok": True}, None]
    assert text("items") != str([{"id": 1, "ok": True}, None])
    assert text("ratio") == "0.5"
    assert text("nothing") == "null"


# ---------------------------------------------------------------------------
# Logging: an explicit enable_access_log(level=...) is the level
# ---------------------------------------------------------------------------

LEVEL_SCRIPT = """
import logging, os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"), debug=os.environ.get("APP_DEBUG") == "1")
app.enable_access_log(level=os.environ["ACCESS_LOG_LEVEL"])

@app.get("/log", runtime="main")
def log(req):
    logging.getLogger("dispatch.app").info("level-info-line")
    logging.getLogger("dispatch.app").warning("level-warning-line")
    logging.getLogger("dispatch.app").error("level-error-line")
    return "ok"
""" + RUN


@pytest.mark.parametrize(
    "env,shown,hidden",
    [
        # debug=True's DEBUG used to win over the explicit level
        ({"APP_DEBUG": "1", "ACCESS_LOG_LEVEL": "warn"}, "level-warning-line", "level-info-line"),
        # the implicit enable_access_log() at run() used to raise an explicit ERROR to INFO
        ({"MIPYRE_LOG": "1", "ACCESS_LOG_LEVEL": "error"}, "level-error-line", "level-warning-line"),
    ],
    ids=["over-debug", "over-MIPYRE_LOG"],
)
def test_explicit_logging_level_wins(env, shown, hidden):
    with serve(LEVEL_SCRIPT, "gil", env=env) as srv:
        assert srv.get("/log").status == 200
        # The default format is text, not JSON: wait on the text the assertions read.
        text = settle(srv.log, lambda t: shown in t)
        assert shown in text, text[-3000:]
        assert hidden not in text, text[-3000:]


KEPT_LEVEL_SCRIPT = """
import logging, os
from mipyre import Mipyre
case = os.environ["LEVEL_CASE"]
if case == "configured-debug":
    app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"), log_config={"level": "DEBUG"})
elif case == "pinned-error":
    app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
    app.enable_access_log(level="error")
else:
    app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))  # ERROR would hide the INFO access lines
app.enable_access_log()  # as MIPYRE_LOG=1 / debug=True at run()

@app.get("/log", runtime="main")
def log(req):
    logger = logging.getLogger("dispatch.app")
    logger.debug("level-debug-line")
    logger.info("level-info-line")
    logger.warning("level-warning-line")
    logger.error("level-error-line")
    return "ok"
""" + RUN


@pytest.mark.parametrize(
    "case,lowest_shown,highest_hidden",
    [
        ("configured-debug", "level-debug-line", None),
        ("default", "level-info-line", "level-debug-line"),
        ("pinned-error", "level-error-line", "level-warning-line"),
    ],
)
def test_enable_access_log_without_a_level_keeps_the_configured_one(case, lowest_shown, highest_hidden):
    with serve(KEPT_LEVEL_SCRIPT, "gil", env={"LEVEL_CASE": case}) as srv:
        assert srv.get("/log").status == 200
        # level-error-line is written last: once it is in, the others would be too.
        srv.wait_for_records(lambda r: "level-error-line" in _record_text(r))
        text = srv.log()
        assert lowest_shown in text, text[-3000:]
        if highest_hidden is not None:
            assert highest_hidden not in text, text[-3000:]


# ---------------------------------------------------------------------------
# Python: body= rule, metrics counters, readiness timeouts, uploads
# ---------------------------------------------------------------------------


def test_model_rule_is_checked_at_registration():
    from pydantic import BaseModel

    from mipyre import Mipyre

    class Item(BaseModel):
        name: str

    # On the main interpreter: PyPI's pydantic-core can't load in a sub-interpreter, which a
    # runtime="subinterp" body= route is refused for first.
    app = Mipyre(runtime="main")

    # The body annotated as the model, but standing where the request goes: `sku` was
    # meant as a path param the template lacks. Used to bind `item` to the request.
    def misplaced(item: Item, sku):
        return sku

    with pytest.raises(TypeError, match=r"'item' is annotated Item but stands where the request goes"):
        app.post("/items", misplaced, body=Item)

    # No parameter for the body at all: used to fail on every request instead.
    def bodiless():
        return "x"

    with pytest.raises(TypeError, match=r"no parameter for the validated Item body"):
        app.post("/things", bodiless, body=Item)

    # The two shapes the rule allows.
    app.post("/a/{item_id}", lambda req, body, item_id: None, body=Item)
    app.post("/b/{item_id}", lambda body, item_id: None, body=Item)


def test_sync_readiness_check_is_bounded_like_an_async_one(monkeypatch):
    import mipyre._health as health

    monkeypatch.setattr(health, "_CHECK_TIMEOUT_S", 0.5)
    # The check hangs until the test releases it, after the run: the run returning at all is the proof that it
    # was cut at the timeout (one that waited for the check would never return; pytest-timeout ends it).
    release = threading.Event()
    checks = [
        health.ReadinessCheck.of("hangs", release.wait),
        health.ReadinessCheck.of("fine", lambda: True),
    ]
    try:
        check_results = health._run_checks(checks)
        ok, results = check_results.ok, check_results.checks
        assert not release.is_set()
    finally:
        release.set()
    assert ok is False
    assert results == {"hangs": {"ok": False}, "fine": {"ok": True}}


class _MultipartRequest:
    def __init__(self, content_type: str, body: bytes):
        self.headers = {"content-type": content_type}
        self.body = body


def _form(boundary: str, disposition: str, data: bytes = b"x") -> bytes:
    return (
        f"--{boundary}\r\nContent-Disposition: {disposition}\r\n\r\n".encode()
        + data
        + f"\r\n--{boundary}--\r\n".encode()
    )


def test_upload_filename_star_is_decoded_and_wins():
    from mipyre.uploads import parse_multipart

    body = _form(
        "b1",
        "form-data; name=\"f\"; filename=\"rates.txt\"; filename*=UTF-8''%E2%82%AC%20rates.txt",
    )
    f = parse_multipart(_MultipartRequest("multipart/form-data; boundary=b1", body))["f"]
    assert f.filename == "€ rates.txt"

    body = _form("b1", "form-data; name=\"f\"; filename*=iso-8859-1'en'%A3.txt")
    f = parse_multipart(_MultipartRequest("multipart/form-data; boundary=b1", body))["f"]
    assert f.filename == "£.txt"


def test_upload_bad_filename_star_is_a_multipart_error():
    from mipyre.uploads import MultipartError, parse_multipart

    body = _form("b1", "form-data; name=\"f\"; filename*=KOI8-R''%C1")
    with pytest.raises(MultipartError, match="RFC 8187"):
        parse_multipart(_MultipartRequest("multipart/form-data; boundary=b1", body))


def test_upload_media_type_is_case_insensitive_and_boundary_may_quote_a_semicolon():
    from mipyre.uploads import MultipartError, parse_multipart

    body = _form("a;b", 'form-data; name="t"', b"hello")
    form = parse_multipart(_MultipartRequest('Multipart/Form-Data; Boundary="a;b"', body))
    assert form["t"].data == b"hello"
    with pytest.raises(MultipartError, match="Expected multipart/form-data"):
        parse_multipart(_MultipartRequest('multipart/mixed; boundary="a;b"', body))


# ---------------------------------------------------------------------------
# DB: typed parameter errors, NUMERIC limits, big ints, float4 range (Postgres)
# ---------------------------------------------------------------------------

PG_DSN = os.environ.get("MIPYRE_TEST_PG_DSN")
needs_pg = pytest.mark.skipif(PG_DSN is None, reason="MIPYRE_TEST_PG_DSN not set")


@pytest.fixture(scope="module")
def values_pool():
    from mipyre.db import PgPool

    pool = PgPool.connect(PG_DSN)
    pool.execute("DROP TABLE IF EXISTS param_values")
    pool.execute("CREATE TABLE param_values (n NUMERIC, b BIGINT, r REAL, d DOUBLE PRECISION)")
    yield pool
    pool.execute("DROP TABLE IF EXISTS param_values")


@needs_pg
def test_big_int_reaches_a_numeric_column(values_pool):
    import decimal

    big = 10**30 + 7
    values_pool.execute("INSERT INTO param_values (n) VALUES ($1)", big)
    assert values_pool.fetch_scalar("SELECT n FROM param_values WHERE n = $1", big) == decimal.Decimal(big)
    assert values_pool.fetch_scalar("SELECT $1::numeric * 2", -big) == decimal.Decimal(-2 * big)


@needs_pg
def test_big_int_for_an_integer_column_is_a_param_error(values_pool):
    from mipyre.db import ParamError

    with pytest.raises(ParamError, match=r"\$1: 9223372036854775808 is out of range for int8") as e:
        values_pool.execute("INSERT INTO param_values (b) VALUES ($1)", 2**63)
    # Both bases, so `except TypeError` / `except ValueError` still catch it.
    assert isinstance(e.value, TypeError) and isinstance(e.value, ValueError)


@needs_pg
def test_float_past_real_is_out_of_range_not_infinity(values_pool):
    from mipyre.db import ParamError

    with pytest.raises(ParamError, match="out of range for float4"):
        values_pool.execute("INSERT INTO param_values (r) VALUES ($1)", 1e300)
    values_pool.execute("INSERT INTO param_values (r, d) VALUES ($1, $2)", 1.5, 1e300)
    assert values_pool.fetch_one("SELECT r, d FROM param_values WHERE r = 1.5") == {"r": 1.5, "d": 1e300}


@needs_pg
def test_numeric_scale_past_0x3fff_is_refused_before_sending(values_pool):
    import decimal

    from mipyre.db import ParamError

    at_max = decimal.Decimal("0." + "1" * 0x3FFF)
    assert values_pool.fetch_scalar("SELECT $1::numeric", at_max) == at_max
    past = decimal.Decimal("0." + "1" * (0x3FFF + 1))
    with pytest.raises(ParamError, match="decimal places"):
        values_pool.fetch_scalar("SELECT $1::numeric", past)


@needs_pg
def test_type_mismatch_is_a_param_error(values_pool):
    from mipyre.db import ParamError

    with pytest.raises(ParamError, match=r"parameter \$1 is int8"):
        values_pool.execute("INSERT INTO param_values (b) VALUES ($1)", "not a number")


# ---------------------------------------------------------------------------
# gRPC: the body read has the body grace and is counted; trailing bytes refused
# ---------------------------------------------------------------------------

GRPC_SCRIPT = """
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.max_body_size = 64
app.request_timeout = REQUEST_BUDGET
app.body_grace = REQUEST_BUDGET
app.enable_grpc_benchmark()
app.enable_metrics()
""".replace("REQUEST_BUDGET", str(REQUEST_BUDGET_S)) + RUN

GET_SUM = "/benchmark.BenchmarkService/GetSum"


def _grpc_frame(message: bytes, declared: int | None = None) -> bytes:
    n = len(message) if declared is None else declared
    return b"\x00" + n.to_bytes(4, "big") + message


def _grpc_call(port: int, body: bytes, end: bool = True) -> dict[str, str]:
    """One h2c call to GetSum; returns its trailers. `end=False` leaves the request body
    open, as a stalled client does."""
    import h2.config
    import h2.connection
    import h2.events

    with socket.create_connection((HOST, port), timeout=60) as s:
        conn = h2.connection.H2Connection(h2.config.H2Configuration(client_side=True))
        conn.initiate_connection()
        stream = conn.get_next_available_stream_id()
        conn.send_headers(stream, [
            (":method", "POST"), (":path", GET_SUM), (":scheme", "http"),
            (":authority", f"{HOST}:{port}"), ("content-type", "application/grpc"),
            ("te", "trailers"),
        ])
        conn.send_data(stream, body, end_stream=end)
        s.sendall(conn.data_to_send())
        trailers: dict[str, str] = {}
        while not trailers:
            data = s.recv(65536)
            if not data:
                break
            for event in conn.receive_data(data):
                if isinstance(event, h2.events.TrailersReceived):
                    trailers = {k.decode(): v.decode() for k, v in event.headers}
            s.sendall(conn.data_to_send())
        return trailers


def test_grpc_trailing_bytes_after_the_message_are_refused():
    with serve(GRPC_SCRIPT, "tpc") as srv:
        ok = _grpc_call(srv.port, _grpc_frame(bytes([0x08, 2, 0x10, 3])))
        assert ok["grpc-status"] == "0", ok
        trailing = _grpc_call(srv.port, _grpc_frame(bytes([0x08, 2, 0x10, 3]), declared=2))
        assert trailing["grpc-status"] == "13", trailing
        assert "nothing after it" in trailing["grpc-message"], trailing
        # The size cap (max_body_size) still applies.
        too_large = _grpc_call(srv.port, _grpc_frame(b"\x08\x01" * 64))
        assert too_large["grpc-status"] == "8", too_large


def test_grpc_stalled_body_is_deadline_exceeded():
    # The body read used to have no deadline at all.
    with serve(GRPC_SCRIPT, "tpc") as srv:
        started = time.monotonic()
        stalled = _grpc_call(srv.port, b"\x00\x00\x00", end=False)
        assert stalled["grpc-status"] == "4", stalled
        took = time.monotonic() - started
        # Not before the budget (a monotonic clock cannot run short). How long after is the machine's; the deadline's
        # exact length is `serve/grpc.rs::a_stalled_body_is_deadline_exceeded`'s, on a paused clock.
        assert took > REQUEST_BUDGET_S - 1, took


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_grpc_calls_are_counted(path):
    with serve(GRPC_SCRIPT, path, env={"MIPYRE_METRICS": "1"}) as srv:
        before = metric_count(srv.get("/metrics").body.decode(), "mipyre_http_requests_total")
        for _ in range(3):
            assert _grpc_call(srv.port, _grpc_frame(bytes([0x08, 1])))["grpc-status"] == "0"
        after = metric_count(srv.get("/metrics").body.decode(), "mipyre_http_requests_total")
        # This server's own count: what the readiness check sent, then the first page read and
        # the three calls.
        assert after - before == 4, (before, after)
