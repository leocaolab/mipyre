"""Final review, fix wave A, W2 (request path): docs/design/code-review-final-rubric.md.

Every server test runs a real server in a subprocess on the path it names, bound to port
0 (the port is read from its "Listening on" line), so it exercises the dispatch code that
path runs:

  - "gil"     MIPYRE_TEST_MODE=main, MIPYRE_TPC=0  → handlers on main (Tokio blocking pool)
  - "tpc-gil" MIPYRE_TEST_MODE=main                  → TPC threads, handlers on main
  - "tpc"     default runtime                        → TPC inline sub-interpreters; runtime="main"
                                                       routes go through the main-interp bridge
  - "pool"    default runtime, MIPYRE_TPC=0        → sub-interp pool; runtime="main" routes on main

The request budget is 30 s by default; the scripts that wait for it set a short one
(`app.request_timeout = REQUEST_BUDGET_S`), and the tests that must outlast it run their
paths concurrently. The bridge spawn-failure test needs a
`maturin develop --release --features fault_injection` build (skipped otherwise).
"""

from __future__ import annotations

import concurrent.futures
import http.client
import json
import os
import pathlib
import re
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import time

import pytest

from tests._helpers import bound_port, metric_count, poll_until, text_appears, set_subinterpreters


PYTHON = sys.executable
HOST = "127.0.0.1"
# `app.request_timeout` in the scripts whose tests wait for it.
REQUEST_BUDGET_S = 1.5

PATHS: dict[str, dict[str, str | None]] = {
    "gil": {"runtime": "main", "tpc": "0"},
    "tpc-gil": {"runtime": "main", "tpc": None},
    "tpc": {"runtime": None, "tpc": None},
    "pool": {"runtime": None, "tpc": "0"},
}

RUN = """
import os
app.run(host="127.0.0.1", port=0)
"""

LISTENING = re.compile(r"Listening on http://127\.0\.0\.1:(\d+)")


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
    """`script` served on `path`, on a port the kernel picked."""

    def __init__(
        self,
        script: str,
        path: str,
        workers: int = 2,
        env: dict[str, str] | None = None,
        wait: bool = True,
    ):
        self.path = path
        self.port: int | None = None
        self._final_output: str | None = None
        fd, self.script_path = tempfile.mkstemp(prefix="mipyre_w2_", suffix=".py")
        with os.fdopen(fd, "w") as f:
            f.write(textwrap.dedent(script))
        self.log_path = self.script_path + ".log"
        full_env = dict(os.environ)
        full_env.pop("MIPYRE_TEST_MODE", None)
        if (runtime := PATHS[path]["runtime"]) is not None:
            full_env["MIPYRE_TEST_MODE"] = runtime
        full_env.pop("MIPYRE_TPC", None)
        full_env.pop("MIPYRE_LOG", None)
        if (tpc := PATHS[path]["tpc"]) is not None:
            full_env["MIPYRE_TPC"] = tpc
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
        if wait:
            self.port = self._bound_port()

    def _bound_port(self) -> int:
        try:
            return bound_port(self.log, self.proc)
        except RuntimeError as e:
            raise RuntimeError(f"server ({self.path}) did not start:\n{self.stop()}") from e

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
        """The JSON log records written so far."""
        return _records(self.log())

    def records_at_exit(self, pred) -> list[dict]:
        """Every record matching `pred`: the server is stopped first, and its exit is the log's fence (`run()` waits
        for every line logged before it, `flush_logs`, b/441). A server killed instead of stopped wrote no fence."""
        out = self.stop()
        assert self.proc.returncode >= 0, f"killed by signal {-self.proc.returncode}, the log is cut:\n{out[-4000:]}"
        return [r for r in _records(out) if pred(r)]

    def wait_for_output(self, text: str) -> None:
        text_appears(self.log, text, self.proc)

    def wait_exit(self, timeout: float = 60) -> int:
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.stop()
            raise AssertionError(f"server did not exit in {timeout}s:\n{self.log()[-4000:]}")

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


def _fields(rec: dict) -> dict:
    return rec.get("fields", rec)


def _text(rec: dict) -> str:
    return json.dumps(rec)


def raw_exchange(port: int, request: bytes, timeout: float = 15) -> tuple[int, dict, bytes]:
    """Sends `request` as is and reads one response: its head, and its body when it
    declares a `content-length`."""
    with socket.create_connection((HOST, port), timeout=timeout) as s:
        s.sendall(request)
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
        head, _, rest = data.partition(b"\r\n\r\n")
        lines = head.decode("latin-1").split("\r\n")
        status = int(lines[0].split(" ")[1])
        headers = {}
        for line in lines[1:]:
            name, _, value = line.partition(":")
            headers[name.strip().lower()] = value.strip()
        length = int(headers.get("content-length", "0"))
        while len(rest) < length:
            chunk = s.recv(4096)
            if not chunk:
                break
            rest += chunk
    return status, headers, rest


def ws_request(path: str, **overrides: str | None) -> bytes:
    """A WebSocket opening handshake for `path`; an override of `None` drops the header,
    `request_line` replaces the request line."""
    fields = {
        "Host": "127.0.0.1",
        "Upgrade": "websocket",
        "Connection": "Upgrade",
        "Sec-WebSocket-Version": "13",
        "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==",
    }
    request_line = overrides.pop("request_line", None) or f"GET {path} HTTP/1.1"
    for name, value in overrides.items():
        name = name.replace("_", "-")
        if value is None:
            fields.pop(name, None)
        else:
            fields[name] = value
    head = request_line + "\r\n" + "".join(f"{k}: {v}\r\n" for k, v in fields.items())
    return (head + "\r\n").encode()


def ws_connect(port: int, target: str, headers: dict | None = None):
    from websockets.sync.client import connect

    return connect(
        f"ws://{HOST}:{port}{target}", additional_headers=headers or {}, open_timeout=60
    )


# ---------------------------------------------------------------------------
# 1. One Request constructor: every path builds the same Request
# ---------------------------------------------------------------------------

REQUEST_SCRIPT = """
import json
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

def echo(req):
    return {
        "method": req.method, "path": req.path, "query": req.query,
        "q": req.query_params.get("q"), "params": req.params,
        "h": req.headers.get("x-h"), "ip": req.client_ip, "body": req.body.decode(),
        "has_id": bool(req.request_id), "stream": req.stream is None,
    }

app.post("/echo/{id}")(echo)
app.post("/echo-gil/{id}", runtime="main")(echo)

@app.get("/plain")
def plain(req):
    return "plain body"

@app.post("/only-post")
def only_post(req):
    return "posted"

@app.websocket("/ws")
def ws(ws):
    r = ws.request
    ws.send(json.dumps({"method": r.method, "path": r.path, "query": r.query,
                        "body": r.body.decode(), "ip": r.client_ip}))
    ws.close()
""" + RUN

REQUEST_CASES = [
    ("tpc", "/echo/7"),  # TPC inline
    ("tpc", "/echo-gil/7"),  # TPC bridge
    ("pool", "/echo/7"),  # pool worker
    ("pool", "/echo-gil/7"),  # pool's main dispatch
    ("gil", "/echo/7"),  # GIL mode
    ("tpc-gil", "/echo/7"),  # TPC threads, handler on main
]


@pytest.mark.parametrize("path", sorted({p for p, _ in REQUEST_CASES}))
def test_every_path_builds_the_same_request(path):
    with serve(REQUEST_SCRIPT, path) as srv:
        for _, route in (c for c in REQUEST_CASES if c[0] == path):
            r = srv.post(route + "?q=a%20b&x=1", b"payload", headers={"x-h": "hv"})
            assert r.status == 200, (route, r.status, r.body)
            assert r.json() == {
                "method": "POST",
                "path": route,
                "query": "q=a%20b&x=1",
                "q": "a b",
                "params": {"id": "7"},
                "h": "hv",
                "ip": "127.0.0.1",
                "body": "payload",
                "has_id": True,
                "stream": True,
            }, route


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_ws_request_comes_from_the_same_constructor(path):
    with serve(REQUEST_SCRIPT, path) as srv:
        with ws_connect(srv.port, "/ws?q=1") as ws:
            got = json.loads(ws.recv(timeout=10))
        assert got == {"method": "GET", "path": "/ws", "query": "q=1", "body": "",
                       "ip": "127.0.0.1"}


def test_python_request_constructor_goes_through_the_same_constructor():
    from mipyre import Request

    req = Request("GET", "/p", {"id": "1"}, "a=1&a=2", b"x", {"x-h": "v"}, "10.0.0.1")
    assert (req.method, req.path, req.query) == ("GET", "/p", "a=1&a=2")
    assert req.query_params == {"a": "1"}
    assert req.params == {"id": "1"}
    assert req.headers["x-h"] == "v"
    assert req.body == b"x"
    assert req.stream is None
    with pytest.raises(ValueError, match="not a valid request line"):
        Request("GET", "/a b", {}, "", b"", {}, "10.0.0.1")
    with pytest.raises(ValueError, match="not a valid request line"):
        Request("BAD METHOD", "/", {}, "", b"", {}, "10.0.0.1")


# ---------------------------------------------------------------------------
# 2. Streamed body: StreamState (the bounded feeder (F4) and the total deadline (G2) are
#    io::body's tests, on a paused clock)
# ---------------------------------------------------------------------------

STREAM_SCRIPT = """
import os
from mipyre import BodyStream, Mipyre
from mipyre.engine import BodyReadError
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.max_body_size = 1024
app.request_timeout = REQUEST_BUDGET

@app.post("/reread", runtime="main", body=BodyStream)
def reread(req):
    stream = req.stream
    outcomes = []
    for _ in range(2):
        try:
            stream.read()
            outcomes.append("read")
        except BodyReadError:
            outcomes.append("rejected")
    try:
        next(stream)
        outcomes.append("chunk")
    except BodyReadError:
        outcomes.append("rejected")
    except StopIteration:
        outcomes.append("stop")
    return outcomes

@app.post("/drain-twice", runtime="main", body=BodyStream)
def drain_twice(req):
    stream = req.stream
    first = len(stream.read())
    try:
        second = stream.drain_count()
    except RuntimeError as e:
        second = "RuntimeError: " + str(e)
    return {"first": first, "second": second}

@app.post("/take-twice", runtime="main", body=BodyStream)
def take_twice(req):
    first = req.stream
    try:
        req.stream
        second = "returned"
    except RuntimeError as e:
        second = "RuntimeError: " + str(e)
    return {"first": type(first).__name__, "second": second, "rest": len(first.read())}
""".replace("REQUEST_BUDGET", str(REQUEST_BUDGET_S)) + RUN


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_a_rejected_stream_keeps_raising_the_rejection(path):
    with serve(STREAM_SCRIPT, path) as srv:
        r = srv.post("/reread", b"x" * 4096)
        # The handler caught every read, so it answers normally with what each read did.
        assert r.status == 200, r.body
        assert r.json() == ["rejected", "rejected", "rejected"]


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_drain_count_on_a_finished_stream_is_an_error_not_zero(path):
    with serve(STREAM_SCRIPT, path) as srv:
        r = srv.post("/drain-twice", b"y" * 100)
        assert r.status == 200, r.body
        body = r.json()
        assert body["first"] == 100
        assert body["second"].startswith("RuntimeError: "), body


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_taking_req_stream_twice_is_an_error_not_none(path):
    with serve(STREAM_SCRIPT, path) as srv:
        r = srv.post("/take-twice", b"z" * 10)
        assert r.status == 200, r.body
        body = r.json()
        assert body["first"] == "BodyStream"
        assert body["second"].startswith("RuntimeError: "), body
        assert "already taken" in body["second"]
        assert body["rest"] == 10


# ---------------------------------------------------------------------------
# 3. R4 residual: a non-coroutine awaitable runs in the request's context
# ---------------------------------------------------------------------------

AWAITABLE_SCRIPT = """
import os
from mipyre import Mipyre, Response
from mipyre.context import ctx
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

class Setting:
    # An awaitable that is not a coroutine, like a Cython or mypyc coroutine or a custom
    # awaitable: what it sets while awaited must reach the rest of the request.
    def __init__(self, key, value, result=None):
        self.key, self.value, self.result = key, value, result

    def __await__(self):
        ctx.set(self.key, self.value)
        return self.result
        yield  # a generator: __await__ returns an iterator

@app.before_request
def tag(req):
    return Setting("user", "u" + req.path)

@app.after_request
def expose(req, resp):
    headers = dict(resp.headers)
    headers["x-handler"] = str(ctx.get("handler"))
    return Response(resp.body, status_code=resp.status_code,
                    content_type=resp.content_type, headers=headers)

def probe(req):
    return Setting("handler", "h" + req.path, {"user": ctx.get("user")})

app.get("/probe")(probe)
app.get("/probe-gil", runtime="main")(probe)
""" + RUN


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_a_non_coroutine_awaitable_writes_the_request_context(path):
    # One worker / TPC thread: the second request runs on the thread that ran the first.
    with serve(AWAITABLE_SCRIPT, path, workers=1) as srv:
        for route in ("/probe", "/probe-gil"):
            for _ in range(2):
                r = srv.get(route)
                assert r.status == 200, (route, r.status, r.body)
                assert r.json() == {"user": "u" + route}, route
                assert r.headers["x-handler"] == "h" + route, route


# ---------------------------------------------------------------------------
# 4. Errors
# ---------------------------------------------------------------------------

BRIDGE_FULL_SCRIPT = """
import os, time
from mipyre import Mipyre
app = Mipyre(log_config={"level": "WARN"}, runtime=os.environ["W2_APP_RUNTIME"])
app.enable_metrics()

@app.get("/hold", runtime="main")
def hold(req):
    # Held until the test creates the gate file (a bound, not a fixed sleep: the test
    # releases it once the refusal is in).
    print("w2 hold started", flush=True)
    deadline = time.time() + 60
    while not os.path.exists(os.environ["W2_GATE"]) and time.time() < deadline:
        time.sleep(0.01)
    return "held"

# With the app's default runtime "subinterp" this is a route on a sub-interpreter, so the TPC
# server has workers; with "main" no route needs one and its threads have none. Either way the
# main routes run on the bridge this test fills.
@app.get("/ok")
def ok(req):
    return "ok"
""" + RUN


@pytest.mark.parametrize("app_runtime", ["subinterp", "main"])
def test_a_full_bridge_queue_is_one_counted_logged_refusal(tmp_path, app_runtime):
    # One bridge thread, one queue slot: with one request running, of two more exactly one
    # is queued and one refused (503) — counted once, where it is refused, and logged once.
    gate = tmp_path / "gate"
    env = {
        "MIPYRE_GIL_BRIDGE_WORKERS": "1",
        "MIPYRE_GIL_BRIDGE_CAPACITY": "1",
        "W2_GATE": str(gate),
        "W2_APP_RUNTIME": app_runtime,
    }
    with serve(BRIDGE_FULL_SCRIPT, "tpc", env=env) as srv:
        with concurrent.futures.ThreadPoolExecutor(3) as pool:
            running = pool.submit(srv.get, "/hold", timeout=30)
            srv.wait_for_output("w2 hold started")
            contenders = [pool.submit(srv.get, "/hold", timeout=30) for _ in range(2)]
            # The refused one answers at once; the queued one waits for the running request,
            # which stays held until the refusal is in.
            concurrent.futures.wait(contenders, return_when=concurrent.futures.FIRST_COMPLETED)
            gate.touch()
            statuses = sorted(f.result().status for f in contenders)
            assert running.result().status == 200
        assert statuses == [200, 503], statuses
        assert metric_count(srv.get("/metrics").body.decode(), "mipyre_http_requests_refused_total") == 1
        refused = srv.records_at_exit(lambda rec: "request refused" in _text(rec))
        assert len(refused) == 1, refused
        assert "runtime='main' bridge queue" in _text(refused[0])


ASYNC_MAIN_SCRIPT = """
import asyncio, os
from mipyre import Mipyre, Response, Stream
app = Mipyre(runtime="main")

@app.after_request
def tag(req, resp):
    headers = dict(resp.headers)
    headers["x-after"] = "ran"
    return Response(resp.body, status_code=resp.status_code,
                    content_type=resp.content_type, headers=headers)

# A `def` route keeps a TPC thread without workers; the `async def` ones run on main's async engine.
@app.get("/sync")
def sync(req):
    return "sync"

@app.get("/json")
async def as_json(req):
    await asyncio.sleep(0)
    return {"ok": True}

@app.get("/stream")
async def stream(req):
    await asyncio.sleep(0)
    out = Stream(content_type="text/plain")
    out.send("a")
    out.send("b")
    out.close()
    return out
""" + RUN


def test_an_async_main_route_is_answered_by_mains_async_engine_with_its_hooks_and_streams():
    # Main's async engine answers as main's other paths do: after hooks see a response, and a
    # returned Stream is streamed with the after hooks unrun.
    with serve(ASYNC_MAIN_SCRIPT, "tpc") as srv:
        r = srv.get("/json")
        assert (r.status, r.json(), r.headers.get("x-after")) == (200, {"ok": True}, "ran")
        r = srv.get("/stream")
        assert (r.status, r.body, r.headers.get("x-after")) == (200, b"ab", None)
        assert srv.get("/sync").headers.get("x-after") == "ran"

WS_ERROR_SCRIPT = """
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.websocket("/ws-raise")
def ws_raise(ws):
    raise ValueError("w2-ws-boom")
""" + RUN


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_a_websocket_handler_error_is_logged_with_its_request(path):
    from websockets.exceptions import ConnectionClosed

    with serve(WS_ERROR_SCRIPT, path) as srv:
        with ws_connect(srv.port, "/ws-raise") as ws:
            try:
                ws.recv(timeout=10)
            except (TimeoutError, ConnectionClosed):
                pass
        found = srv.records_at_exit(lambda rec: "w2-ws-boom" in _text(rec))
        assert len(found) == 1, found
        fields = _fields(found[0])
        assert fields.get("request_id"), found[0]
        assert fields.get("method") == "GET", found[0]
        assert fields.get("path") == "/ws-raise", found[0]
        assert "Traceback" in fields.get("traceback", ""), found[0]
        assert found[0].get("level") == "ERROR"


# ---------------------------------------------------------------------------
# 5. WebSocket: typed handshake, routing, connection slot (R2), one Pong
# ---------------------------------------------------------------------------

WS_SCRIPT = """
import os
from mipyre import Mipyre, Response
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.max_websocket_connections = 1
app.request_timeout = REQUEST_BUDGET

@app.before_request
def guard(req):
    print("w2 hook ran", req.method, req.path, req.query, flush=True)
    if req.query_params.get("slow"):
        # Held past the budget on a FIFO until the test opens and closes its other end: the
        # hook's thread lives exactly as long as the test says. Blocked in C (open, read), so
        # the budget's interrupt cannot end it, as it could a Python loop.
        with open(os.environ["W2_GATE"], "rb") as gate:
            gate.read()
    if req.query_params.get("deny"):
        return Response("denied", status_code=403)

@app.websocket("/ws")
def hold(ws):
    while ws.recv() is not None:
        pass

@app.get("/plain")
def plain(req):
    return "plain body"
""".replace("REQUEST_BUDGET", str(REQUEST_BUDGET_S)) + RUN

BAD_HANDSHAKES = [
    ({"request_line": "POST /ws HTTP/1.1"}, 400),
    ({"request_line": "GET /ws HTTP/1.0"}, 400),
    ({"Connection": "keep-alive"}, 400),
    ({"Sec-WebSocket-Key": None}, 400),
    ({"Sec-WebSocket-Version": "8"}, 426),
]


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_a_bad_handshake_is_refused_before_any_hook_slot_or_thread(path):
    with serve(WS_SCRIPT, path) as srv:
        for overrides, status in BAD_HANDSHAKES:
            got, headers, _ = raw_exchange(srv.port, ws_request("/ws", **dict(overrides)))
            assert got == status, (overrides, got)
            if status == 426:
                assert headers.get("sec-websocket-version") == "13"
        # With one connection slot, a valid upgrade still gets it: none was taken.
        with ws_connect(srv.port, "/ws") as ws:
            ws.send("hi")
        # Only the valid upgrade ran the hook.
        hooks = srv.log().count("w2 hook ran")
        assert hooks == 1, srv.log()[-3000:]


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_upgrade_on_an_ordinary_route_is_routed_normally(path):
    with serve(WS_SCRIPT, path) as srv:
        status, _, body = raw_exchange(
            srv.port,
            b"GET /plain HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            b"Content-Length: 0\r\nConnection: close\r\n\r\n",
        )
        assert status == 200
        assert body.startswith(b"plain body")


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_a_rejected_upgrade_frees_its_slot(path):
    from websockets.exceptions import InvalidStatus

    with serve(WS_SCRIPT, path) as srv:
        with pytest.raises(InvalidStatus) as exc:
            ws_connect(srv.port, "/ws?deny=1")
        assert exc.value.response.status_code == 403
        # The slot is released once the hook's thread is joined: until then a connection is 503.
        poll_until(lambda: _ws_status(srv.port, "/ws") == 101, timeout=None, what="the slot freed")


def _ws_status(port: int, target: str) -> int:
    """The status a WebSocket upgrade of `target` gets: 101, or the refusal's (503 while the one slot is taken)."""
    from websockets.exceptions import InvalidStatus

    try:
        with ws_connect(port, target) as ws:
            ws.send("hi")
        return 101
    except InvalidStatus as e:
        assert e.response.status_code == 503, e.response.status_code
        return 503


def _slot_held_through_a_timed_out_hook(port: int, gate: pathlib.Path) -> tuple[int, int, int, int]:
    """A hook held past the budget on a FIFO (`gate`): the handshake answers 504 at the budget while the hook keeps
    its thread, and so the only slot, until the gate opens. Returns the 504's status, the status of a connection
    tried right after it, and of one tried just before the gate opens, then waits for the slot (a connection that
    gets 101) once the gate is open, for no length of time: when the hook ends is the test's to say, not the
    machine's (b/441 D1)."""
    from websockets.exceptions import InvalidStatus

    try:
        ws_connect(port, "/ws?slow=1").close()
        slow = 101
    except InvalidStatus as e:
        slow = e.response.status_code
    right_after = _ws_status(port, "/ws")
    before_release = _ws_status(port, "/ws")
    # Non-blocking: with no hook holding the read end this fails at once (ENXIO) instead of waiting.
    os.close(os.open(gate, os.O_WRONLY | os.O_NONBLOCK))
    poll_until(lambda: _ws_status(port, "/ws") == 101, timeout=None, what="the slot freed with the hook")
    return slow, right_after, before_release, 101


def test_the_connection_slot_lives_as_long_as_the_hook_thread(tmp_path):
    # R2: the slot used to drop with the 504 while the hook thread kept running, so the
    # next connection got a second thread past the cap.
    gates = {path: tmp_path / f"gate-{path}" for path in ("tpc", "gil")}
    for gate in gates.values():
        os.mkfifo(gate)
    servers = {path: Server(WS_SCRIPT, path, env={"W2_GATE": str(gates[path])}) for path in gates}
    try:
        with concurrent.futures.ThreadPoolExecutor(len(servers)) as pool:
            futures = {
                path: pool.submit(_slot_held_through_a_timed_out_hook, srv.port, gates[path])
                for path, srv in servers.items()
            }
            for path, f in futures.items():
                assert f.result() == (504, 503, 503, 101), path
    finally:
        for srv in servers.values():
            srv.stop()


def _ws_frame(opcode: int, payload: bytes) -> bytes:
    """A masked client frame (payload under 126 bytes)."""
    mask = b"\x01\x02\x03\x04"
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return bytes([0x80 | opcode, 0x80 | len(payload)]) + mask + masked


def _server_frames(data: bytes) -> list[tuple[int, bytes]]:
    """The unmasked server frames in `data` (payloads under 126 bytes)."""
    frames = []
    while len(data) >= 2:
        opcode, length = data[0] & 0x0F, data[1] & 0x7F
        if len(data) < 2 + length:
            break
        frames.append((opcode, data[2 : 2 + length]))
        data = data[2 + length :]
    return frames


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_a_ping_gets_exactly_one_pong(path):
    with serve(WS_SCRIPT, path) as srv:
        with socket.create_connection((HOST, srv.port), timeout=10) as s:
            s.sendall(ws_request("/ws"))
            data = b""
            while b"\r\n\r\n" not in data:
                data += s.recv(4096)
            head, _, rest = data.partition(b"\r\n\r\n")
            assert b" 101 " in head.split(b"\r\n")[0], head
            s.sendall(_ws_frame(0x9, b"w2-ping"))
            s.settimeout(0.3)
            deadline = time.time() + 2
            while time.time() < deadline:
                try:
                    chunk = s.recv(4096)
                except socket.timeout:
                    continue
                if not chunk:
                    break
                rest += chunk
        pongs = [p for op, p in _server_frames(rest) if op == 0xA]
        assert pongs == [b"w2-ping"], _server_frames(rest)


# ---------------------------------------------------------------------------
# 6. Layering: shared error/body types in the bottom layer, one panic_message
# ---------------------------------------------------------------------------

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = ROOT / "crates" / "pyext" / "src"
# The domain packages (`crates/README.md`): every package but the engine. Their sources are
# the bottom layer by construction — they do not depend on the engine at all.
CRATES = sorted(p for p in ROOT.glob("crates/*/src") if p != SRC)


def test_lower_layers_do_not_import_the_handlers_layer():
    # b/51 E8 moved these files into their layers' directories and dissolved `handlers` into
    # `serve` (design §2, §7.2): the same files at their new paths, and the handlers layer
    # under both its names. `grpc.rs` is `serve`'s own now, so for it only the old name is
    # foreign; the rest are `runtime`, `exchange` (b/229 split `interp` into them) and `db`,
    # which sit below `serve`.
    runtime, exchange = SRC / "runtime", SRC / "exchange"
    lower = [
        *(runtime / f for f in ("hook_chain.rs", "isolation.rs",
                                "request_context.rs", "rules.rs", "worker.rs", "worker_api.rs",
                                "worker_app.rs", "worker/async_engine.rs", "worker/exec.rs",
                                "worker/program.rs", "worker/start.rs", "worker/sync_runner.rs")),
        *sorted((runtime / "pool").glob("*.rs")),
        *(exchange / f for f in ("body_stream.rs", "stream.rs", "websocket.rs")),
        exchange / "to_response.rs",
        SRC / "db.rs",
        *sorted((SRC / "db").glob("*.rs")),
        exchange / "error.rs",
        exchange / "request_head.rs",
        exchange / "request.rs",
        exchange / "response.rs",
    ]
    offenders = [
        p.name for p in lower
        if "crate::handlers" in p.read_text() or "crate::serve" in p.read_text()
    ]
    offenders += [
        p.name for p in [SRC / "serve" / "grpc.rs"] if "crate::handlers" in p.read_text()
    ]
    assert offenders == [], offenders
    # body.rs moved to crates/infra/http (b/47 S2a-3, q/5 a), then crates/http/src/io (b/230), and with it every other
    # file of the shared floor. Inside a separate crate `crate::handlers` cannot name pyre's
    # handlers at all, so the rule there is the one that can be broken: a shared crate must
    # not name the engine, by path or by crate name.
    assert (ROOT / "crates" / "http" / "src" / "io" / "body.rs").exists()
    named = [
        f"{p.parent.parent.name}/{p.name}"
        for root in CRATES
        for p in sorted(root.rglob("*.rs"))
        if "mipyre_engine" in p.read_text() or "crate::handlers" in p.read_text()
    ]
    assert named == [], named


def test_there_is_one_panic_message():
    # One panic-payload-to-message conversion in the whole workspace: mipyre_tpc's, beside the
    # threads whose panics it reads. No crate defines another, and no other file downcasts a
    # panic payload itself (maintainer ruling on b/227).
    crates = ROOT / "crates"
    home = "crates/tpc/src/threads/os_thread.rs"
    defs = [
        str(p.relative_to(ROOT))
        for p in sorted(crates.rglob("*.rs"))
        if re.search(r"\bfn panic_message\b", p.read_text())
    ]
    assert defs == [home], defs
    downcasts = [
        str(p.relative_to(ROOT))
        for p in sorted(crates.rglob("*.rs"))
        if re.search(r"downcast_ref::<(&str|String)>", p.read_text())
    ]
    assert downcasts == [home], downcasts


# ---------------------------------------------------------------------------
# 7. Small items: HEAD, 405, sampling, Accept-Encoding
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_head_on_a_get_route_is_served_without_a_body(path):
    with serve(REQUEST_SCRIPT, path) as srv:
        get = srv.get("/plain")
        head = srv.request("HEAD", "/plain")
        assert head.status == 200, head.status
        assert head.body == b""
        assert head.headers["content-length"] == str(len(get.body))
        assert head.headers["content-type"] == get.headers["content-type"]


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_a_wrong_method_is_405_with_allow(path):
    with serve(REQUEST_SCRIPT, path) as srv:
        r = srv.get("/only-post")
        assert r.status == 405, r.body
        assert r.headers["allow"] == "POST"
        assert r.json() == {"detail": "method not allowed"}
        r = srv.request("DELETE", "/plain")
        assert (r.status, r.headers["allow"]) == (405, "GET, HEAD")
        # A path no method has is still 404.
        assert srv.get("/nope").status == 404


SAMPLING_SCRIPT = """
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.enable_access_log(level="info", sample=10)

@app.get("/s")
def s(req):
    return "ok"
""" + RUN


def test_access_log_keeps_one_in_n_per_thread():
    # One TPC thread: its roll starts at its first response, so of 100 responses on it
    # exactly 10 (the 1st, 11th, ...) are logged.
    with serve(SAMPLING_SCRIPT, "tpc", workers=1) as srv:
        conn = http.client.HTTPConnection(HOST, srv.port, timeout=10)
        try:
            for _ in range(100):
                conn.request("GET", "/s")
                r = conn.getresponse()
                r.read()
                assert r.status == 200
        finally:
            conn.close()
        lines = srv.records_at_exit(
            lambda rec: _fields(rec).get("path") == "/s"
            and rec.get("target") == "mipyre::access"
        )
        assert len(lines) == 10, len(lines)


COMPRESSION_SCRIPT = """
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.enable_compression(min_size=64)

@app.get("/big")
def big(req):
    return "abcdefgh" * 200
""" + RUN


@pytest.mark.parametrize("path", ["tpc", "gil"])
def test_no_or_unreadable_accept_encoding_is_not_compressed(path):
    with serve(COMPRESSION_SCRIPT, path) as srv:
        def get(extra: bytes) -> tuple[int, dict]:
            status, headers, _ = raw_exchange(
                srv.port, b"GET /big HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n" + extra + b"\r\n"
            )
            return status, headers

        status, headers = get(b"")
        assert status == 200 and "content-encoding" not in headers
        status, headers = get(b"Accept-Encoding: gzip\xff\r\n")
        assert status == 200 and "content-encoding" not in headers
        status, headers = get(b"Accept-Encoding: gzip\r\n")
        assert status == 200 and headers.get("content-encoding") == "gzip"
