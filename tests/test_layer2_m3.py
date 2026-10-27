"""Registration seal, worker-app registry and inert prerequisites, on the main interpreter.

The worker side is in `test_layer2_m4.py`.
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import signal
import subprocess
import sys
import textwrap
import uuid

import httpx
import pytest

import mipyre.engine as engine
from tests.conftest import fork_panic_lines
from mipyre import LateRegistrationError, Mipyre
from tests._helpers import answers, bound_port, poll_until, read_file

HERE = os.path.dirname(os.path.abspath(__file__))

subinterp_only = pytest.mark.skipif(
    sys.platform not in ("linux", "darwin"), reason="own-GIL sub-interpreters"
)


# ---------------------------------------------------------------------------
# helpers: a server subprocess with a log file, stopped with SIGINT
# ---------------------------------------------------------------------------


def _start(script_path: str, log_path: str, extra_env: dict | None = None):
    env = dict(os.environ)
    env.update(extra_env or {})
    log = open(log_path, "w")
    proc = subprocess.Popen(
        [sys.executable, script_path], stdout=log, stderr=subprocess.STDOUT,
        env=env, cwd=HERE, start_new_session=True,
    )
    log.close()
    return proc


def _bound_base(proc: subprocess.Popen, log_path: str) -> str:
    """The base URL of a server started on port 0, from its startup line."""
    return f"http://127.0.0.1:{bound_port(read_file(log_path), proc)}"


def _wait_up(base: str, path: str, proc: subprocess.Popen, log_path: str) -> None:
    answers(base + path, proc, read_file(log_path))


def _stop(proc: subprocess.Popen) -> None:
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        proc.wait(timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait(timeout=5)


def _write(tmp_path, name: str, source: str) -> str:
    path = tmp_path / name
    path.write_text(textwrap.dedent(source))
    return str(path)


# ---------------------------------------------------------------------------
# main interpreter: seal, _in_worker, module=
# ---------------------------------------------------------------------------


def test_in_worker_is_false_on_main():
    assert engine._in_worker() is False


def test_pyclasses_live_in_mipyre_engine():
    for cls in (engine.Request, engine.Response, engine.SharedState, engine.Stream,
                engine.WebSocket, engine.MipyreApp):
        assert cls.__module__ == "mipyre.engine", cls


def test_non_gil_route_after_seal_raises_with_method_and_path():
    app = Mipyre()

    @app.get("/early")
    def early(req):
        return "ok"

    app._engine._seal_registrations()

    with pytest.raises(LateRegistrationError, match=r"GET /late .*runtime='main'"):
        @app.get("/late")
        def late(req):
            return "no"

    @app.get("/late-gil", runtime="main")
    def late_gil(req):
        return "ok"


def test_seal_is_idempotent():
    app = Mipyre()
    app._engine._seal_registrations()

    @app.get("/after", runtime="main")
    def after(req):
        return "ok"

    # A second run() (TestClient retries on port races) seals again: no error, and the
    # rule still holds.
    app._engine._seal_registrations()
    with pytest.raises(LateRegistrationError, match=r"POST /x "):
        app._engine.route("POST", "/x", lambda req: "no", engine.RuntimeChoice.Worker)


def test_on_startup_non_gil_route_fails_startup(tmp_path):
    script = _write(tmp_path, "startup_route.py", """
        from mipyre import Mipyre, ServerFlags
        app = Mipyre(runtime="main")

        @app.get("/")
        def index(req):
            return "ok"

        @app.on_startup
        def add_route_late():
            @app.get("/late", runtime="subinterp")
            def late(req):
                return "no"

        app.run(ServerFlags(host="127.0.0.1", port=0))
    """)
    out = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=60)
    assert out.returncode != 0, out.stdout + out.stderr
    assert "GET /late is registered after app.run() started" in out.stderr, out.stderr


# ---------------------------------------------------------------------------
# pydantic only for routes with body=
# ---------------------------------------------------------------------------


def test_import_mipyre_does_not_import_pydantic():
    pytest.importorskip("pydantic")
    out = subprocess.run(
        [sys.executable, "-c",
         "import sys, mipyre; from mipyre import Mipyre; Mipyre();"
         "print('pydantic' in sys.modules, 'pydantic_core' in sys.modules)"],
        capture_output=True, text=True, timeout=60,
    )
    assert out.returncode == 0, out.stderr
    assert out.stdout.split() == ["False", "False"]


def test_model_route_still_validates():
    pytest.importorskip("pydantic")
    from mipyre.testing import TestClient

    # Its own module: a worker serves one app per module.
    from tests.apps.l2_model_route import app

    with TestClient(app) as client:
        ok = client.post("/items", body=json.dumps({"name": "a", "qty": 2}))
        bad = client.post("/items", body=json.dumps({"name": "a", "qty": "x"}))
    assert ok.status_code == 200 and ok.json() == {"name": "a", "qty": 2}
    assert bad.status_code == 422


# ---------------------------------------------------------------------------
# GIL mode: request ids never cross between concurrent requests
# ---------------------------------------------------------------------------


# Requests in flight at once in the request-id test: all of them wait at the rendezvous.
_IN_FLIGHT = 32


def test_request_id_is_per_request_under_concurrency(tmp_path):
    """Request ids stay with their requests under the worst interleaving: all `_IN_FLIGHT`
    requests are in their await at once (each says so with a file, then waits for its own
    release file), and they are released in reverse order of sending, each one answered before
    the next is let go (as test_layer2_m4's pool half)."""
    log_path = str(tmp_path / "server.log")
    rendezvous = tmp_path / "rendezvous"
    (rendezvous / "in").mkdir(parents=True)
    (rendezvous / "go").mkdir()
    script = _write(tmp_path, "request_id.py", """
        import asyncio, os
        from mipyre import Mipyre, ServerFlags
        app = Mipyre(runtime="main")
        app.enable_request_id_header()
        RENDEZVOUS = os.environ["RID_RENDEZVOUS"]

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/slow")
        async def slow(req):
            k = req.query_params["k"]
            open(os.path.join(RENDEZVOUS, "in", k), "x").close()
            while not os.path.exists(os.path.join(RENDEZVOUS, "go", k)):
                await asyncio.sleep(0.005)
            return "ok"

        app.run(ServerFlags(host="127.0.0.1", port=0))
    """)
    proc = _start(script, log_path, {"RID_RENDEZVOUS": str(rendezvous)})
    try:
        base = _bound_base(proc, log_path)
        _wait_up(base, "/ping", proc, log_path)

        def one(k):
            rid = uuid.uuid4().hex
            r = httpx.get(base + f"/slow?k={k}", headers={"X-Request-ID": rid}, timeout=None)
            return rid, r.status_code, r.headers.get("x-request-id")

        with concurrent.futures.ThreadPoolExecutor(_IN_FLIGHT) as pool:
            sent = [pool.submit(one, k) for k in range(_IN_FLIGHT)]
            poll_until(lambda: len(os.listdir(rendezvous / "in")) == _IN_FLIGHT, timeout=None)
            results = []
            for k in reversed(range(_IN_FLIGHT)):
                (rendezvous / "go" / str(k)).touch()
                results.append(sent[k].result())
    finally:
        _stop(proc)
    assert all(status == 200 for _, status, _ in results)
    mismatched = [(sent, got) for sent, _, got in results if sent != got]
    assert mismatched == []


# ---------------------------------------------------------------------------
# app.run() only under a __main__ guard, MIPYRE_LOG=1, /mcp
# ---------------------------------------------------------------------------


@subinterp_only
def test_main_guard_logging_and_mcp_in_subinterp_mode(tmp_path):
    log_path = str(tmp_path / "server.log")
    script = _write(tmp_path, "guarded.py", """
        from mipyre import Mipyre, ServerFlags
        app = Mipyre()

        @app.get("/w")
        def w(req):
            return {"ok": True}

        from mipyre.mcp import MCPServer
        mcp = MCPServer(app)

        @mcp.tool(description="add two numbers")
        def add(a: int, b: int) -> int:
            return a + b

        if __name__ == "__main__":
            app.run(ServerFlags(host="127.0.0.1", port=0, threads=2))
    """)
    proc = _start(script, log_path, {"MIPYRE_LOG": "1"})
    try:
        base = _bound_base(proc, log_path)
        _wait_up(base, "/w", proc, log_path)
        assert httpx.get(base + "/w", timeout=5).json() == {"ok": True}
        r = httpx.post(base + "/mcp", timeout=5, headers={"Content-Type": "application/json"}, content=json.dumps(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}))
        assert r.status_code == 200
        assert [t["name"] for t in r.json()["result"]["tools"]] == ["add"]
    finally:
        _stop(proc)
    log = open(log_path).read()
    assert fork_panic_lines(log) == []
    assert "Traceback" not in log, log[-3000:]
