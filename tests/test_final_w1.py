"""Final review, fix wave A, W1 (server / topology).

One test (or more) per item of the W1 row in docs/design/code-review-final-rubric.md;
each fails on the code before the fix. No fixed ports: every server binds port 0.

- Q1: on TPC, `async def` routes run on the async worker pool, off the TPC thread.
- Topology: one `Topology` resolved at start; GC modes a topology can't run are refused.
- `threads` / `subinterpreters` of 0, and a host that isn't an IP, are `ValueError`s naming it.
- G4: a port another server listens on with SO_REUSEPORT is `OSError(EADDRINUSE)`.
- R5: each server has its own stop handle (nested TestClients, a stop before serving).
- TestClient never `os._exit`s the test process; `_prepare` is per server where it must be.
- Registration: `body=BodyStream` without `runtime="main"`, and hooks after the seal, are errors.
- RSS sampler: runs while a server with metrics serves, `None` after, back on the next run.
"""

from __future__ import annotations

import concurrent.futures
import errno
import os
import socket
import subprocess
import sys
import textwrap
import threading
import urllib.error
import urllib.request

import pytest

import mipyre.engine
import mipyre.testing
from mipyre import BodyStream, Mipyre, get_gil_metrics
from mipyre._server_run import ServerRun, runs_of
from mipyre.testing import TestClient, TestServer
from tests._helpers import poll_until
from tests.apps import (
    final_w1_mcp_late,
    final_w1_plain,
    final_w1_sealed,
    final_w1_tpc_async,
    final_w1_tpc_async_capped,
)

HOST = "127.0.0.1"
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _get(url: str, timeout: float = 10) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode()


def _run_script(tmp_path, body: str, timeout: float = 60, env_extra=None):
    script = tmp_path / "script.py"
    script.write_text(textwrap.dedent(body))
    blas = {"OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"}
    env = {
        k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_") and k not in blas
    }
    env.update(env_extra or {})
    return subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=REPO,
        env=env,
    )


# ---------------------------------------------------------------------------
# Q1: TPC `async def` routes go to the async pool
# ---------------------------------------------------------------------------


@pytest.fixture
def one_tpc_thread():
    # One TPC thread: before Q1 it ran every route, async included, one at a time.
    return mipyre.engine.ServerFlags(threads=1)


def _status_and_text(url: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=None) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, ""


def _round_of_four(base: str, round_: str) -> list[tuple[int, str]]:
    """Four `/together` requests of one round sent at once; what each got."""
    with concurrent.futures.ThreadPoolExecutor(4) as ex:
        return list(ex.map(_status_and_text, [f"{base}/together?round={round_}&n=4"] * 4))


def test_tpc_async_handlers_overlap_their_awaits(one_tpc_thread):
    """Four async requests on the one TPC thread are all in their await at once: each waits
    for the fourth to arrive, and answers how many were in flight. Inline on the TPC thread
    the first would block the rest (and get its 504). Until b/441 this was `took < 1.8 s` for
    four 0.6 s sleeps, which two at a time (1.2 s) also passed."""
    with TestServer(final_w1_tpc_async.app, config=one_tpc_thread) as server, TestClient(server) as c:
        assert _round_of_four(c.base_url, "q1") == [(200, "4")] * 4


def test_the_round_sees_awaits_that_overlap_only_two_at_a_time(one_tpc_thread):
    """The round's own input: the same round with two requests let in at a time never has four
    in flight, so none of them completes it (each gets its 504 at the app's short budget)."""
    with TestServer(final_w1_tpc_async_capped.app, config=one_tpc_thread) as server, TestClient(server) as c:
        assert [status for status, _ in _round_of_four(c.base_url, "capped")] == [504] * 4


def test_a_sync_route_answers_while_an_async_one_awaits(one_tpc_thread, tmp_path):
    """An async request in its await lets the TPC thread answer a sync one: the async request
    waits for a gate only the sync request opens, after it is known to be in its await. Blocked
    by the async handler (before Q1), the sync request never runs and the async one gets its 504
    instead of "released". Until b/441 this slept 0.3 s to guess the await had begun and
    checked `took < 1.5 s`."""
    entered, gate = tmp_path / "entered", tmp_path / "gate"
    with TestServer(final_w1_tpc_async.app, config=one_tpc_thread) as server, TestClient(server) as c:
        slow: list[tuple[int, str]] = []
        waiter = threading.Thread(
            target=lambda: slow.append(_status_and_text(f"{c.base_url}/until_open?entered={entered}&gate={gate}"))
        )
        waiter.start()
        poll_until(entered.exists, timeout=None)
        assert c.get(f"/fast?open={gate}").text == "fast"
        waiter.join()
    assert slow == [(200, "released")]


# ---------------------------------------------------------------------------
# Topology, and the arguments parsed at the boundary
# ---------------------------------------------------------------------------


def test_a_gc_mode_the_topology_cannot_run_is_refused_at_start():
    engine = mipyre.engine.MipyreApp()
    # A route on a sub-interpreter is what makes the multi-thread server the pool.
    engine.route("GET", "/", lambda req: "ok", mipyre.engine.RuntimeChoice.Worker)
    flags = mipyre.engine.ServerFlags(host=HOST, port=0, tpc=False, gc_mode="idle")
    with pytest.raises(ValueError, match="gc_mode=idle.*sub-interpreter pool"):
        engine.start(config=flags)


@pytest.mark.parametrize("arg", ["threads", "subinterpreters"])
def test_zero_workers_is_a_value_error_naming_the_argument(arg):
    engine = mipyre.engine.MipyreApp()
    with pytest.raises(ValueError, match=rf'^ServerFlags\({arg}=\): "0" is invalid'):
        engine.start(config=mipyre.engine.ServerFlags(host=HOST, port=0, **{arg: 0}))


@pytest.mark.parametrize("host", ["localhost", "not-a-host", "127.0.0"])
def test_a_host_that_is_not_an_ip_is_a_value_error_naming_it(host):
    engine = mipyre.engine.MipyreApp()
    with pytest.raises(ValueError) as err:
        engine.start(config=mipyre.engine.ServerFlags(host=host, port=0))
    assert f'host "{host}"' in str(err.value), err.value


def test_a_bare_ipv6_host_binds():
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
            s.bind(("::1", 0))
    except OSError:
        pytest.skip("no IPv6 loopback")
    engine = mipyre.engine.MipyreApp()
    server = engine.start(config=mipyre.engine.ServerFlags(host="::1", port=0))
    assert server.port != 0
    server.shutdown()
    server.serve()  # stopped before it served: returns at once, releasing the port


# ---------------------------------------------------------------------------
# G4: a port another server listens on is in use, SO_REUSEPORT or not
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not hasattr(socket, "SO_REUSEPORT"), reason="no SO_REUSEPORT")
def test_a_port_held_with_reuseport_is_eaddrinuse():
    other = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    other.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    other.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    other.bind((HOST, 0))
    other.listen()
    port = other.getsockname()[1]
    try:
        engine = mipyre.engine.MipyreApp()
        with pytest.raises(OSError) as err:
            engine.start(config=mipyre.engine.ServerFlags(host=HOST, port=port))
        assert err.value.errno == errno.EADDRINUSE, err.value
    finally:
        other.close()


@pytest.mark.skipif(not hasattr(socket, "SO_REUSEPORT"), reason="no SO_REUSEPORT")
def test_a_second_server_on_a_served_port_is_eaddrinuse():
    with TestServer(final_w1_plain.app) as server, TestClient(server) as c:
        engine = mipyre.engine.MipyreApp()
        with pytest.raises(OSError) as err:
            engine.start(config=mipyre.engine.ServerFlags(host=HOST, port=c.port))
        assert err.value.errno == errno.EADDRINUSE, err.value
        assert c.get("/").text == "ok"


# ---------------------------------------------------------------------------
# R5: a stop handle per server
# ---------------------------------------------------------------------------


def test_nested_test_clients_on_one_app_stop_independently(monkeypatch):
    # Close waits for its server's thread with no deadline: a close that found no server to stop never returns
    # (pytest-timeout ends it), so the return is the proof; it used to wait out the 60 s default (b/441 D4).
    monkeypatch.setattr(mipyre.testing, "_STOP_TIMEOUT_S", None)
    app = final_w1_plain.app
    with TestServer(app) as server, TestClient(server) as outer:
        inner = TestServer(app)
        assert inner.port != outer.port
        with TestClient(inner) as c:
            assert c.get("/").text == "ok"
        inner.close()
        # The inner close stopped the inner server only.
        assert outer.get("/").text == "ok"
        with pytest.raises(OSError):
            socket.create_connection((HOST, inner.port), timeout=2).close()
    with pytest.raises(OSError):
        socket.create_connection((HOST, outer.port), timeout=2).close()


def test_a_server_stopped_before_it_serves_returns_at_once():
    engine = mipyre.engine.MipyreApp()
    server = engine.start(config=mipyre.engine.ServerFlags(host=HOST, port=0))
    port = server.port
    server.shutdown()
    # Returns at all: a serve that missed the earlier stop serves on and never returns (pytest-timeout ends it;
    # b/441 D5, it was under 5 s).
    server.serve()
    with pytest.raises(RuntimeError, match="already served"):
        server.serve()
    del server
    # The port is released: a plain bind succeeds.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, port))


def test_a_stop_while_starting_stops_the_server_it_was_meant_for():
    # What TestClient.close() during startup does: a stop that came before the server
    # existed stops it as soon as it does, so `serve` returns instead of serving on.
    app = final_w1_plain.app
    run = ServerRun(app, mipyre.engine.Deployment(), mipyre.engine.ServerFlags(host=HOST, port=0))
    run.stop()
    done = threading.Event()

    def start():
        run.serve()
        done.set()

    threading.Thread(target=start, daemon=True).start()
    assert done.wait(20), "the server kept serving after its stop"


def test_app_stop_stops_every_server_of_the_app():
    app = final_w1_plain.app
    a = TestClient(app)
    b = TestClient(app)
    runs_of(app).stop()
    for client in (a, b):
        client.server._thread.join(20)
        assert not client.server._thread.is_alive()
        client.close()


# ---------------------------------------------------------------------------
# TestClient never exits the test process; `_prepare` per server
# ---------------------------------------------------------------------------


def test_abandoned_workers_raise_from_testclient_close(tmp_path):
    r = _run_script(
        tmp_path,
        """
        from mipyre import Mipyre
        from mipyre import AbandonedWorkersError
        from mipyre.testing import TestClient

        class AbandoningEngine:
            # The app's engine, reporting the workers a pool shutdown gave up on.

            def __init__(self, engine, abandoned):
                self._engine, self._abandoned = engine, abandoned

            def __getattr__(self, name):
                return getattr(self._engine, name)

            def _take_abandoned_workers(self):
                taken, self._abandoned = self._abandoned, []
                return taken

        app = Mipyre(runtime="main")

        @app.get("/")
        def index(req):
            return "ok"

        # As a pool shutdown that gave up on a worker reports it.
        app._engine = AbandoningEngine(app._engine, ["mipyre-worker-0 (running GET /)"])

        if __name__ == "__main__":
            c = TestClient(app)
            try:
                c.close()
            except AbandonedWorkersError as e:
                print("RAISED", [str(w) for w in e.workers], flush=True)
            print("STILL-RUNNING", flush=True)
        """,
    )
    assert "RAISED ['mipyre-worker-0 (running GET /)']" in r.stdout, r.stdout + r.stderr
    assert "STILL-RUNNING" in r.stdout, r.stdout + r.stderr


def test_app_run_still_exits_when_workers_are_abandoned(tmp_path):
    r = _run_script(
        tmp_path,
        """
        import threading, time, urllib.request
        from mipyre import Mipyre
        from mipyre._server_run import runs_of

        class AbandoningEngine:
            # The app's engine, reporting the workers a pool shutdown gave up on.

            def __init__(self, engine, abandoned):
                self._engine, self._abandoned = engine, abandoned

            def __getattr__(self, name):
                return getattr(self._engine, name)

            def _take_abandoned_workers(self):
                taken, self._abandoned = self._abandoned, []
                return taken

        app = Mipyre(runtime="main")

        @app.get("/")
        def index(req):
            return "ok"

        app._engine = AbandoningEngine(app._engine, ["mipyre-worker-0"])

        def stop_when_up():
            for _ in range(200):
                if runs_of(app).serving():
                    break
                time.sleep(0.05)
            runs_of(app).stop()

        if __name__ == "__main__":
            threading.Thread(target=stop_when_up, daemon=True).start()
            app.run(host="127.0.0.1", port=0)
            print("RETURNED", flush=True)
        """,
    )
    assert r.returncode == 1, r.stdout + r.stderr
    assert "RETURNED" not in r.stdout
    assert "mipyre-worker-0" in r.stderr and "without finalization" in r.stderr, r.stderr


def test_the_mcp_route_comes_with_the_first_server_that_has_tools():
    app = final_w1_mcp_late.app
    with TestClient(app) as c:
        assert c.post("/mcp", body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).status_code == 404

    from mipyre.mcp import MCPServer
    mcp = MCPServer(app)

    @mcp.tool()
    def late_ping() -> str:
        return "pong"

    with TestClient(app) as c:
        r = c.post("/mcp", body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        assert r.status_code == 200, r.text
        assert [t["name"] for t in r.json()["result"]["tools"]] == ["late_ping"]


def test_servers_leave_the_blas_variables_of_the_server_process_alone(tmp_path):
    """BLAS thread limits belong to the process-lane adapters, in worker processes only
    (tests/test_blas_threads.py): no server changes the server process's environment.
    The same app is served on the main interpreter (MIPYRE_TEST_MODE=main) and on
    sub-interpreter workers (the default runtime), each in its own process."""
    script = """
        import os
        from mipyre import Mipyre
        from mipyre.testing import TestClient

        app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

        @app.get("/")
        def index(req):
            return "ok"

        if __name__ == "__main__":
            with TestClient(app):
                pass
            print("AFTER", os.environ.get("OPENBLAS_NUM_THREADS"), flush=True)
        """
    on_main = _run_script(
        tmp_path, script, timeout=120, env_extra={"MIPYRE_THREADS": "2", "MIPYRE_TEST_MODE": "main"}
    )
    on_workers = _run_script(tmp_path, script, timeout=120, env_extra={"MIPYRE_THREADS": "2"})
    assert "AFTER None" in on_main.stdout, on_main.stdout + on_main.stderr
    assert "AFTER None" in on_workers.stdout, on_workers.stdout + on_workers.stderr


# ---------------------------------------------------------------------------
# Registration errors
# ---------------------------------------------------------------------------


def test_stream_without_gil_is_a_registration_error():
    app = Mipyre()

    def upload(req):
        return "ok"

    with pytest.raises(ValueError, match="body=BodyStream needs runtime='main'"):
        app.post("/up", body=BodyStream)(upload)
    assert app.routes == []


def test_hooks_after_the_seal_are_refused_like_late_routes():
    app = final_w1_sealed.app
    with TestClient(app) as c:
        assert c.get("/").text == "ok"

    def hook(req):
        return None

    sealed = mipyre.engine.LateRegistrationError
    assert issubclass(sealed, mipyre.DeclarationError)
    with pytest.raises(sealed, match="before_request hook is registered after"):
        app.before_request(hook)
    with pytest.raises(sealed, match="after_request hook is registered after"):
        app.after_request(lambda req, resp: resp)
    with pytest.raises(sealed, match="fallback handler is registered after"):
        app.fallback(lambda req: "fallback")
    with pytest.raises(sealed):
        app.enable_cors(allow_origins="*")
    # A late worker route gets the same type.
    with pytest.raises(sealed, match="registered after app.run"):
        app.get("/late", runtime="subinterp")(lambda req: "late")

    # Nothing was added: the next server serves what it did before.
    with TestClient(app) as c:
        r = c.get("/")
        assert r.text == "ok"
        assert "access-control-allow-origin" not in r.headers
        assert c.get("/nope").status_code == 404


# ---------------------------------------------------------------------------
# RSS sampler lifetime
# ---------------------------------------------------------------------------


def _rss_while(predicate):
    """The sampled RSS once `predicate` accepts it: the sampler's first sample comes when it comes (b/441)."""
    return poll_until(lambda: (rss,) if predicate(rss := get_gil_metrics().rss_bytes) else None, timeout=None,
                      what="an RSS sample")[0]


@pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="RSS sampled on Linux/macOS")
def test_the_rss_sampler_runs_per_server_and_resets_after():
    app = final_w1_plain.app
    for _ in range(2):
        with TestClient(app, config=mipyre.engine.ServerFlags(metrics=True)) as c:
            assert c.get("/").text == "ok"
            rss = _rss_while(lambda v: isinstance(v, int))
            assert isinstance(rss, int) and rss > 0, rss
        # Stopped with the server: no stale value, and the next run samples again.
        assert get_gil_metrics().rss_bytes is None
