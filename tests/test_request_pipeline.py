"""One request pipeline: GIL mode, the sub-interpreter pool and TPC preprocess a request and
finish its response the same way.

Serving paths, as `app.run` picks them:
  - "gil"       MIPYRE_TEST_MODE=main, MIPYRE_TPC=0  → multi-thread runtime, handlers on main
  - "tpc-gil"   MIPYRE_TEST_MODE=main                  → TPC threads, handlers on main
  - "tpc"       default runtime                        → TPC inline (sub-interp) + main bridge (runtime="main")
  - "pool"      default runtime, MIPYRE_TPC=0        → sub-interp pool + main (runtime="main")
"""

from __future__ import annotations

import json
import os
import re
import select
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import urllib.error
import urllib.request

import pytest

from tests._helpers import bound_port, metric_count, read_file, set_subinterpreters, settle

PYTHON = sys.executable
HOST = "127.0.0.1"

# The request budget (`app.request_timeout`, default 30 s) and body grace (`app.body_grace`,
# default 5 s) the scripts whose tests wait for them set.
REQUEST_BUDGET_S = 1.5
# The slow-handler test's own budget: a sleep past it is all a 504 needs.
SLOW_BUDGET_S = 1

PATHS: dict[str, dict[str, str | None]] = {
    "gil": {"runtime": "main", "tpc": "0"},
    "tpc-gil": {"runtime": "main", "tpc": None},
    "tpc": {"runtime": None, "tpc": None},
    "pool": {"runtime": None, "tpc": "0"},
}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


class Server:
    """A Mipyre server running `script` on one serving path. `app.run` in the script
    reads `port` from `APP_PORT`; the "gil" paths run with `MIPYRE_TEST_MODE=main`."""

    def __init__(self, script: str, path: str, workers: int = 2):
        self.port = 0  # the bound one once the server listens
        fd, self.script_path = tempfile.mkstemp(prefix="mipyre_pipeline_", suffix=".py")
        with os.fdopen(fd, "w") as f:
            f.write(textwrap.dedent(script))
        self.log_path = self.script_path + ".log"
        env = dict(os.environ)
        env.pop("MIPYRE_TEST_MODE", None)
        if (runtime := PATHS[path]["runtime"]) is not None:
            env["MIPYRE_TEST_MODE"] = runtime
        env["APP_PORT"] = "0"
        env.pop("MIPYRE_TPC", None)
        if (tpc := PATHS[path]["tpc"]) is not None:
            env["MIPYRE_TPC"] = tpc
        set_subinterpreters(env, str(workers))
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                [PYTHON, self.script_path],
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=env,
            )
        # The Listening line comes once the sockets are bound: waits for it or the exit.
        try:
            self.port = bound_port(read_file(self.log_path), self.proc)
        except RuntimeError as e:
            raise RuntimeError(f"server ({path}) did not start:\n{self.stop()}") from e

    @property
    def base(self) -> str:
        return f"http://{HOST}:{self.port}"

    def request(
        self,
        path: str,
        method: str = "GET",
        body: bytes | None = None,
        headers: dict | None = None,
        timeout: float = 10,
    ) -> tuple[int, bytes, dict]:
        req = urllib.request.Request(
            self.base + path, data=body, method=method, headers=headers or {}
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, r.read(), {k.lower(): v for k, v in r.headers.items()}
        except urllib.error.HTTPError as e:
            return e.code, e.read(), {k.lower(): v for k, v in e.headers.items()}

    def stop(self) -> str:
        if self.proc.poll() is None:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGINT)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                self.proc.wait(timeout=5)
        with open(self.log_path, errors="replace") as f:
            out = f.read()
        for p in (self.script_path, self.log_path):
            try:
                os.unlink(p)
            except FileNotFoundError:
                pass
        return out


RUN = """
import os
from mipyre import ServerFlags
app.run(ServerFlags(host="127.0.0.1", port=int(os.environ["APP_PORT"])))
"""


def _access_lines(log: str) -> list[str]:
    return [line for line in log.splitlines() if "mipyre::access" in line]


# ---------------------------------------------------------------------------
# Fallback on TPC (TPC ignored app.fallback())
# ---------------------------------------------------------------------------

FALLBACK_SCRIPT = """
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.get("/")
def index(req):
    return "index"

@app.fallback
def fallback(req):
    return f"fallback:{req.method}:{req.path}"
""" + RUN


@pytest.mark.parametrize("path", ["tpc", "pool", "gil"])
def test_fallback_serves_unmatched_paths(path):
    srv = Server(FALLBACK_SCRIPT, path)
    try:
        status, body, _ = srv.request("/nope/deeper")
        assert (status, body) == (200, b"fallback:GET:/nope/deeper")
        assert srv.request("/")[:2] == (200, b"index")
    finally:
        srv.stop()


# ---------------------------------------------------------------------------
# TPC body read has a timeout (TPC collected the body with no deadline)
# ---------------------------------------------------------------------------

BODY_SCRIPT = """
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.request_timeout = REQUEST_BUDGET
app.body_grace = REQUEST_BUDGET

@app.post("/echo")
def echo(req):
    return req.body
""".replace("REQUEST_BUDGET", str(REQUEST_BUDGET_S)) + RUN


def _stalled_post(port: int, wait: float) -> tuple[bytes | None, float]:
    """POST a body that declares 1000 bytes, send 10, stall. Returns what the server
    answered (b"" if it closed without a response, None if nothing came within `wait`) and how
    long that took."""
    start = time.time()
    with socket.create_connection((HOST, port), timeout=wait) as s:
        s.sendall(
            b"POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 1000\r\n\r\n0123456789"
        )
        try:
            data = s.recv(4096)
        except socket.timeout:
            data = None
    return data, time.time() - start


ADMISSION_SCRIPT = """
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.enable_cors(allow_origins="https://app.example")
app.enable_metrics()

@app.post("/upload")
def upload(req):
    return str(len(req.body))
""" + RUN


def _reply_head(s: socket.socket) -> tuple[int, dict]:
    """The status and headers of the response on `s`."""
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    status_line, *header_lines = data.split(b"\r\n\r\n", 1)[0].decode().split("\r\n")
    headers = dict(line.split(": ", 1) for line in header_lines)
    return int(status_line.split()[1]), {k.lower(): v for k, v in headers.items()}


def test_pool_admission_rejects_large_bodies_past_the_permit_budget():
    """The pool's admission gate, behaviourally: a large body takes
    a permit before a byte of it is read; with every permit held, the next one is a 503
    that carries CORS and is counted as dropped. One sync worker → 128 permits.

    129 heads for 128 permits, no body sent: exactly one is refused. Which one depends on
    the order the heads are served in, and Linux spreads connections over several accept
    loops, so the test does not pick it."""
    srv = Server(ADMISSION_SCRIPT, "pool", workers=1)
    conns = []
    try:
        for _ in range(129):
            s = socket.create_connection((HOST, srv.port), timeout=10)
            s.sendall(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 100000\r\n\r\n")
            conns.append(s)
        # The ones holding a permit wait for their body, so the refused one is the only
        # connection with a reply.
        first, _, _ = select.select(conns, [], [], 10)
        assert first, "no connection was refused"
        more, _, _ = select.select([s for s in conns if s not in first], [], [], 1)
        replied = first + more
        assert len(replied) == 1, f"{len(replied)} connections got a reply"
        status, headers = _reply_head(replied[0])
        assert status == 503
        assert headers.get("access-control-allow-origin") == "https://app.example"
        status, body, _ = srv.request("/metrics")
        # This server's own refusals: the one 503 above.
        assert metric_count(body.decode(), "mipyre_http_requests_refused_total") == 1
    finally:
        for s in conns:
            s.close()
        srv.stop()


@pytest.mark.parametrize("path", ["tpc", "gil", "pool"])
def test_slow_body_times_out_with_408(path):
    srv = Server(BODY_SCRIPT, path)
    try:
        data, took = _stalled_post(srv.port, REQUEST_BUDGET_S + 10)
        assert data is not None, f"no answer within {REQUEST_BUDGET_S + 10}s: the body read has no deadline"
        assert data.startswith(b"HTTP/1.1 408"), data[:200]
        assert took >= REQUEST_BUDGET_S - 1
    finally:
        srv.stop()


def test_the_body_time_settings_have_defaults_and_refuse_bad_values():
    from mipyre import Mipyre

    app = Mipyre()
    assert (app.body_grace, app.body_min_rate, app.body_time_limit) == (5.0, 1024, 30.0)
    app.body_grace = 15
    app.body_min_rate = 64 * 1024
    app.body_time_limit = 3600
    assert (app.body_grace, app.body_min_rate, app.body_time_limit) == (15.0, 64 * 1024, 3600.0)
    with pytest.raises(ValueError, match="body_grace"):
        app.body_grace = 0
    with pytest.raises(ValueError, match="body_min_rate"):
        app.body_min_rate = 0
    with pytest.raises(ValueError, match="body_time_limit"):
        app.body_time_limit = 0
    with pytest.raises(TypeError):
        app.body_time_limit = "60"


# ---------------------------------------------------------------------------
# Handler timeout on every path (GIL and TPC inline had none) + DROPPED_REQUESTS
# ---------------------------------------------------------------------------

SLOW_SCRIPT = """
import os
import time
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.request_timeout = REQUEST_BUDGET
app.enable_metrics()

def held():
    # Returns once the test has the 504 of every path that answers while the handler runs, and
    # not before the budget is past: the inline path judges its 504 by how long the call took.
    start = time.monotonic()
    while not os.path.exists(RELEASE):
        time.sleep(0.005)
    time.sleep(max(0.0, start + REQUEST_BUDGET + 0.5 - time.monotonic()))
    return "late"

@app.get("/slow")
def slow(req):
    return held()

@app.get("/slow-gil", runtime="main")
def slow_gil(req):
    return held()
""".replace("REQUEST_BUDGET", str(SLOW_BUDGET_S)) + RUN

# (serving path, route): every way a handler call is dispatched.
SLOW_CASES = [
    ("gil", "/slow"),          # multi-thread runtime, main interpreter
    ("tpc-gil", "/slow"),      # TPC threads, main interpreter
    ("tpc", "/slow"),          # TPC inline sub-interpreter
    ("tpc", "/slow-gil"),      # TPC → main-interp bridge
    ("pool", "/slow"),         # sub-interp pool
    ("pool", "/slow-gil"),     # pool mode, runtime="main" route on main
]


# The path whose handler blocks the thread that would answer: its 504 can only come after the
# handler returns.
INLINE_CASES = {("tpc", "/slow")}


def test_handler_timeout_is_504_on_every_path(tmp_path):
    # One server per case: a slow inline handler blocks its TPC thread, and on macOS one
    # listener takes every connection, so two cases must not share a TPC server.
    release = tmp_path / "release"
    script = SLOW_SCRIPT.replace("RELEASE", repr(str(release)))
    servers = {case: Server(script, case[0], workers=2) for case in SLOW_CASES}
    results: dict = {}

    def hit(case):
        path, route = case
        try:
            status, _, _ = servers[case].request(route, timeout=None)
        except Exception as e:  # noqa: BLE001 — reported below
            status = repr(e)
        results[case] = status

    try:
        threads = {c: threading.Thread(target=hit, args=(c,)) for c in SLOW_CASES}
        for t in threads.values():
            t.start()
        for case, t in threads.items():
            if case not in INLINE_CASES:
                t.join()
        release.touch()
        for t in threads.values():
            t.join()
        wrong = {c: r for c, r in results.items() if r != 504}
        assert not wrong, f"expected 504 on every path, got {wrong}"

        # A request the server gave up on is counted as dropped, on every path.
        for case, srv in servers.items():
            status, body, _ = srv.request("/metrics")
            assert status == 200
            # This server's own refusals: its one request past the budget.
            refused = metric_count(body.decode(), "mipyre_http_requests_refused_total")
            assert refused == 1, f"{case}: {refused} refusals counted"
    finally:
        release.touch()
        for srv in servers.values():
            srv.stop()


# ---------------------------------------------------------------------------
# One access-log line for every response (404 and fast-path wrote none; the pool
# wrote two for a handled request)
# ---------------------------------------------------------------------------

LOG_SCRIPT = """
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.enable_access_log()
app.add_fast_response("GET", "/fast", b"fast")

@app.get("/handled")
def handled(req):
    return "ok"
""" + RUN


@pytest.mark.parametrize("path", ["gil", "tpc-gil", "tpc", "pool"])
def test_every_response_writes_one_access_line(path):
    srv = Server(LOG_SCRIPT, path)
    try:
        assert srv.request("/nope-404")[0] == 404
        assert srv.request("/fast")[0] == 200
        assert srv.request("/handled")[0] == 200
        routes = ("/nope-404", "/fast", "/handled")
        settle(
            lambda: _access_lines(read_file(srv.log_path)()),
            lambda lines: all(any(r in line for line in lines) for r in routes),
        )
    finally:
        log = srv.stop()
    lines = _access_lines(log)
    for route, status in (("/nope-404", "404"), ("/fast", "200"), ("/handled", "200")):
        mine = [line for line in lines if route in line]
        assert len(mine) == 1, f"{path} {route}: expected one access line, got {mine}\n{log[-3000:]}"
        assert status in mine[0], mine[0]


# ---------------------------------------------------------------------------
# Per-request contextvars.Context (ctx leaked into the next request on the thread)
# ---------------------------------------------------------------------------

CTX_SCRIPT = """
import os
from mipyre import Mipyre, Response
from mipyre.context import ctx
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.before_request
def tag(req):
    ctx.set("hook", req.path)

@app.after_request
def echo_hook(req, resp):
    headers = dict(resp.headers)
    headers["x-ctx-hook"] = str(ctx.get("hook"))
    return Response(resp.body, status_code=resp.status_code,
                    content_type=resp.content_type, headers=headers)

def probe(req):
    seen = {"leaked": ctx.get("leak"), "hook": ctx.get("hook")}
    if req.query_params.get("set"):
        ctx.set("leak", "from-an-earlier-request")
    return seen

app.get("/ctx")(probe)
app.get("/ctx-gil", runtime="main")(probe)
""" + RUN

CTX_CASES = [("gil", "/ctx"), ("tpc", "/ctx"), ("tpc", "/ctx-gil"), ("pool", "/ctx"), ("pool", "/ctx-gil")]


@pytest.mark.parametrize("path,route", CTX_CASES)
def test_ctx_does_not_leak_into_the_next_request(path, route):
    # One worker / TPC thread, so consecutive requests run on the same thread.
    srv = Server(CTX_SCRIPT, path, workers=1)
    try:
        assert srv.request(route + "?set=1")[0] == 200
        for _ in range(10):
            status, body, headers = srv.request(route)
            assert status == 200
            seen = json.loads(body)
            assert seen["leaked"] is None, f"{path} {route}: ctx leaked: {seen}"
            # Hooks and handler share the request's context.
            assert seen["hook"] == route
            assert headers["x-ctx-hook"] == route
    finally:
        srv.stop()


# ---------------------------------------------------------------------------
# ws.request, and before_request runs before the 101
# ---------------------------------------------------------------------------

WS_SCRIPT = """
import os
from mipyre import Mipyre, Response
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.enable_cors(allow_origins="https://app.example")

@app.before_request
def guard(req):
    if req.path == "/ws" and req.headers.get("x-token") != "secret":
        return Response("denied", status_code=403)

@app.websocket("/ws")
async def ws_handler(ws):
    r = ws.request
    await ws.send(f"{r.method} {r.path} {r.query_params.get('q')} {r.headers.get('x-token')}")
    ws.close()
""" + RUN


def _ws_connect(port: int, token: str | None):
    from websockets.sync.client import connect

    headers = {"x-token": token} if token is not None else {}
    return connect(f"ws://{HOST}:{port}/ws?q=1", additional_headers=headers, open_timeout=10)


@pytest.mark.parametrize("path", ["gil", "tpc"])
def test_ws_request_carries_the_upgrade_request(path):
    srv = Server(WS_SCRIPT, path)
    try:
        with _ws_connect(srv.port, "secret") as ws:
            assert ws.recv(timeout=10) == "GET /ws 1 secret"
    finally:
        srv.stop()


@pytest.mark.parametrize("path", ["gil", "tpc"])
def test_before_request_rejects_ws_upgrade(path):
    from websockets.exceptions import InvalidStatus

    srv = Server(WS_SCRIPT, path)
    try:
        with pytest.raises(InvalidStatus) as exc:
            _ws_connect(srv.port, None)
        resp = exc.value.response
        assert resp.status_code == 403
        assert resp.body == b"denied"
        # The rejection is a normal response: CORS applies.
        assert resp.headers.get("access-control-allow-origin") == "https://app.example"
    finally:
        srv.stop()


# ---------------------------------------------------------------------------
# Invalid status: a fast response can't be registered with one; a handler's is a
# logged 500 on every path (TPC and the pool mapped it without a log line)
# ---------------------------------------------------------------------------


def test_fast_response_rejects_invalid_status_and_headers_at_registration():
    out = subprocess.run(
        [
            PYTHON,
            "-c",
            textwrap.dedent(
                """
                import os
                from mipyre import Mipyre
                app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
                for kwargs in ({"status_code": 1000}, {"status_code": 42},
                               {"headers": {"x-bad": "line\\nbreak"}},
                               {"headers": {"bad header": "v"}},
                               {"content_type": "text/\\x00plain"}):
                    try:
                        app.add_fast_response("GET", "/bad", b"x", **kwargs)
                    except ValueError as e:
                        print("rejected", e)
                    else:
                        print("accepted", kwargs)
                """
            ),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert out.returncode == 0, out.stderr
    lines = out.stdout.splitlines()
    assert len(lines) == 5, out.stdout
    assert all(line.startswith("rejected") for line in lines), out.stdout


STATUS_SCRIPT = """
import os
from mipyre import Mipyre, Response
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.get("/bad-status")
def bad_status(req):
    return Response("x", status_code=1000)

@app.get("/bad-status-gil", runtime="main")
def bad_status_gil(req):
    return Response("x", status_code=1000)
""" + RUN


@pytest.mark.parametrize(
    "path,route",
    [("gil", "/bad-status"), ("tpc", "/bad-status"), ("tpc", "/bad-status-gil"), ("pool", "/bad-status")],
)
def test_invalid_handler_status_is_a_logged_500(path, route):
    srv = Server(STATUS_SCRIPT, path)
    try:
        assert srv.request(route)[0] == 500
        settle(lambda: "invalid HTTP status" in read_file(srv.log_path)())
    finally:
        log = srv.stop()
    assert re.search(r"invalid HTTP status.*1000|1000.*invalid HTTP status", log), log[-3000:]
