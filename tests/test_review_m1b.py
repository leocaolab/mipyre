"""Code review cced8c2, milestone M1b: small Rust runtime bugs, end to end.

- Compression on a non-ASCII content type must not panic the connection.
- Idle-mode GC counts dispatched requests, so it runs (and its OOM failsafe trips) on a
  keep-alive connection; a failed `gc.collect()` retires its worker (b/142).
- `MIPYRE_GC_MODE` is parsed once: an unknown or unsupported mode is a startup error.
- The sub-interpreter pool's sync/async worker split is one value, read by the pool and
  the banner; a needed pool with no worker is a startup error.
"""

from __future__ import annotations

import http.client
import json
import os
import signal
import subprocess
import sys
import textwrap
import urllib.error
import urllib.request

import pytest

from tests._helpers import answers, bound_port, poll_until, settle

HOST = "127.0.0.1"
STARTUP_TIMEOUT_S = 30


class Server:
    def __init__(self, tmp_path, script: str, env: dict[str, str], ready_path: str = "/"):
        self.port: int | None = None
        self.script = tmp_path / "app.py"
        self.script.write_text(textwrap.dedent(script).replace("__PORT__", "0"))
        self.log = tmp_path / "server.log"
        full_env = dict(os.environ)
        full_env.update(env)
        with open(self.log, "w") as log:
            self.proc = subprocess.Popen(
                [sys.executable, str(self.script)],
                stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True, env=full_env,
            )
        self.ready_path = ready_path

    def output(self) -> str:
        return self.log.read_text(errors="replace")

    def wait_ready(self) -> None:
        """Waits for the startup line, then for the ready path to answer 200, for no length of time (b/441 T2)."""
        if self.port is None:
            self.port = bound_port(self.output, self.proc)
        answers(f"http://{HOST}:{self.port}{self.ready_path}", self.proc, self.output)

    def wait_exit(self) -> int:
        """The exit code of a server expected to refuse to start."""
        try:
            return self.proc.wait(timeout=STARTUP_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            self.stop()
            pytest.fail(f"server started instead of refusing to:\n{self.output()}")

    def stop(self) -> None:
        if self.proc.poll() is not None:
            return
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGINT)
            self.proc.wait(timeout=20)
        except Exception:  # noqa: BLE001 — best-effort teardown
            os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            self.proc.wait(timeout=5)


@pytest.fixture
def server(tmp_path):
    started: list[Server] = []

    def start(script: str, env: dict[str, str] | None = None, ready_path: str = "/") -> Server:
        s = Server(tmp_path, script, env or {}, ready_path)
        started.append(s)
        return s

    yield start
    for s in started:
        s.stop()


# ---------------------------------------------------------------------------
# Item 1: compression on a non-ASCII content type
# ---------------------------------------------------------------------------

NON_ASCII_CT_APP = """
    from mipyre import Mipyre, Response

    app = Mipyre()
    app.enable_compression(min_size=16)

    @app.get("/")
    def root(req):
        return "ok"

    @app.get("/odd-ct")
    def odd_ct(req):
        # byte 5 falls inside the 3-byte '€': a str slice [..5] would panic
        return Response("x" * 2000, content_type="tex€/plain")

    app.run(host="127.0.0.1", port=__PORT__)
"""


@pytest.mark.parametrize("mode", ["subinterp", "gil"])
def test_non_ascii_content_type_is_served_uncompressed(server, mode):
    # "gil": every handler on the main interpreter, the app's runtime "main".
    s = server(NON_ASCII_CT_APP, {"MIPYRE_RUNTIME": "main"} if mode == "gil" else {})
    s.wait_ready()

    req = urllib.request.Request(
        f"http://{HOST}:{s.port}/odd-ct", headers={"Accept-Encoding": "gzip, br"}
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status == 200
        assert resp.headers.get("Content-Encoding") is None
        assert resp.read() == b"x" * 2000


# ---------------------------------------------------------------------------
# Item 2 + 3: idle-mode GC on keep-alive; failed gc.collect()
# ---------------------------------------------------------------------------

GC_COUNTING_APP = """
    import gc
    from mipyre import Mipyre

    # gc.disable() runs at worker init, so every collection here is the framework's.
    _collections = [0]

    def _on_gc(phase, info):
        if phase == "start":
            _collections[0] += 1

    gc.callbacks.append(_on_gc)

    app = Mipyre()

    @app.get("/")
    def root(req):
        return "ok"

    @app.get("/work")
    def work(req):
        a = []
        a.append(a)  # a cycle only the collector can free
        return "done"

    @app.get("/gc-count")
    def gc_count(req):
        return str(_collections[0])

    app.run(host="127.0.0.1", port=__PORT__, workers=1)
"""


def _get(conn: http.client.HTTPConnection, path: str) -> tuple[int, str]:
    conn.request("GET", path)
    resp = conn.getresponse()
    return resp.status, resp.read().decode()


def _gc_count(conn: http.client.HTTPConnection) -> int:
    status, body = _get(conn, "/gc-count")
    assert status == 200, body
    return int(body)


def test_idle_gc_runs_on_keep_alive_connection(server):
    s = server(GC_COUNTING_APP, {"MIPYRE_GC_MODE": "idle", "MIPYRE_GC_IDLE_MS": "50"})
    s.wait_ready()
    conn = http.client.HTTPConnection(HOST, s.port, timeout=5)

    for _ in range(3):
        assert _get(conn, "/work")[0] == 200
    # Each count probe is a request too; a quiet tick after it collects. Probing every
    # tick (50 ms) would keep the thread from ever going a full tick without a request,
    # so the probes are four ticks apart.
    first = settle(lambda: _gc_count(conn), lambda n: n >= 1, interval=0.2)

    assert _get(conn, "/work")[0] == 200
    second = settle(lambda: _gc_count(conn), lambda n: n > first, interval=0.2)
    conn.close()

    assert first >= 1
    assert second > first, "idle GC stopped collecting once the connection was kept alive"


def test_idle_gc_failsafe_trips_on_keep_alive_connection(server):
    s = server(GC_COUNTING_APP, {
        "MIPYRE_GC_MODE": "idle",
        "MIPYRE_GC_IDLE_MS": "600000",  # the idle tick never fires during the test
        "MIPYRE_GC_OOM_FAILSAFE": "5",
    })
    s.wait_ready()
    conn = http.client.HTTPConnection(HOST, s.port, timeout=5)

    for _ in range(12):
        assert _get(conn, "/work")[0] == 200
    collections = _gc_count(conn)
    conn.close()

    assert collections >= 2, "OOM failsafe never tripped under sustained keep-alive traffic"


FAILING_GC_APP = """
    import gc
    from mipyre import Mipyre

    class BadGcError(Exception):
        def __str__(self):
            raise ValueError("str() of the gc error failed too")

    def _collect(*args):
        raise BadGcError()

    gc.collect = _collect  # the worker's scheduled collect now fails

    app = Mipyre()

    @app.get("/")
    def root(req):
        return "ok"

    @app.get("/work")
    def work(req):
        return "done"

    app.run(host="127.0.0.1", port=__PORT__, workers=1)
"""


# A failed gc.collect() retires its worker (b/142, user ruling A): it takes no more requests,
# the other workers serve, and once none is left the server stops with the cause, so the
# process exits non-zero. Before b/142 the worker kept serving (the test this replaces asserted
# a request after the failure got 200 from the same, only worker).

# How each topology runs the failing collect: the TPC threads' own workers by count and on the
# idle tick, the multi-thread pool's (MIPYRE_TPC=0) by count.
GC_FAILURE_PATHS = {
    "tpc-count": {"MIPYRE_GC_MODE": "count", "MIPYRE_GC_THRESHOLD": "1"},
    "tpc-idle": {"MIPYRE_GC_MODE": "idle", "MIPYRE_GC_IDLE_MS": "50"},
    "pool-count": {"MIPYRE_TPC": "0", "MIPYRE_GC_MODE": "count", "MIPYRE_GC_THRESHOLD": "1"},
}


def _exit_code(s: Server, timeout: float = 20) -> int:
    try:
        return s.proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.fail(f"the server kept running with no worker left:\n{s.output()}")


@pytest.mark.parametrize("path", sorted(GC_FAILURE_PATHS))
def test_a_failed_gc_collect_retires_the_only_worker_and_stops_the_server(server, path):
    s = server(FAILING_GC_APP, GC_FAILURE_PATHS[path])
    # The readiness probe is the first request: its collect (after it, or a tick later) fails.
    s.wait_ready()

    code = _exit_code(s)
    out = s.output()
    assert code != 0, out[-3000:]
    # The cause, in the failure's own words (its str() failing too), reaches the exit.
    stopped = [line for line in out.splitlines() if "the server stopped:" in line]
    assert stopped, out[-3000:]
    assert "is left serving" in stopped[-1], stopped[-1]
    assert "worker 0 retired: gc.collect() raised BadGcError" in stopped[-1], stopped[-1]
    assert "the worker retires and takes no more requests" in out


ONE_FAILING_GC_APP = """
    import gc, os
    from mipyre import Mipyre, get_gil_metrics

    _real_collect = gc.collect

    def _collect(*args):
        # The first collect in any worker claims the marker and fails; every other is real.
        try:
            fd = os.open(__MARKER__, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return _real_collect(*args)
        os.close(fd)
        raise MemoryError("b142-injected")

    gc.collect = _collect

    app = Mipyre()
    # WARN: the retired TPC thread's line once its listeners are closed is the test's event.
    app.enable_access_log(level="warn")

    @app.get("/")
    def root(req):
        return "ok"

    @app.get("/work")
    def work(req):
        return "done"

    @app.get("/retired")
    def retired(req):
        m = get_gil_metrics()
        return {"retired": m.workers_retired, "why": m.last_worker_retirement,
                "gc_failures": m.gc_failures}

    app.run(host="127.0.0.1", port=__PORT__, workers=2)
"""


@pytest.mark.parametrize("path", sorted(GC_FAILURE_PATHS))
def test_a_retired_worker_takes_no_more_requests_and_the_others_serve(server, tmp_path, path):
    marker = tmp_path / "gc-failed"
    s = server(ONE_FAILING_GC_APP.replace("__MARKER__", repr(str(marker))), GC_FAILURE_PATHS[path])
    s.wait_ready()
    # The readiness probe ran the first collect (after it, or a tick later), which retired
    # its worker. A TPC thread then closes its listeners: a connection already queued on one
    # is refused once, the cost of leaving the accept group. It says so once they are closed
    # (b/494: until then a 0.3 s sleep guessed it); a pool worker has no listener of its own.
    poll_until(lambda: "the worker retires" in s.output(), timeout=None, what="the retirement")
    assert marker.exists(), s.output()[-3000:]
    if path.startswith("tpc"):
        poll_until(lambda: "the thread takes no more connections" in s.output(), timeout=None,
                   what="the retired thread's listeners closed")

    # Every request from now on lands on the worker left: none is refused.
    for i in range(40):
        status, body = _get_fresh(s, "/work")
        assert status == 200, f"request {i} after the retirement got {status}: {body}\n{s.output()[-3000:]}"
    status, body = _get_fresh(s, "/retired")
    assert status == 200, body
    got = json.loads(body)
    # A server of its own, and one collect failed: exactly one retirement.
    assert got["retired"] == 1, got
    assert got["gc_failures"] == 1, got
    assert "retired: gc.collect() raised MemoryError: b142-injected" in got["why"], got
    assert s.proc.poll() is None, "the server stopped with a worker still serving"
    retired = [line for line in s.output().splitlines() if "the worker retires" in line]
    assert len(retired) == 1, retired
    assert "MemoryError: b142-injected" in retired[0], retired[0]
    assert '"worker_id":' in retired[0], retired[0]


def _get_fresh(s: Server, path: str) -> tuple[int, str]:
    """`path` on a connection of its own, so the kernel picks the thread that takes it."""
    try:
        with urllib.request.urlopen(f"http://{HOST}:{s.port}{path}", timeout=5) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()



# A `gc.collect()` that raises a BaseException that is no Exception, as PyO3's PanicException is
# (what this test raised before b/300, through the `fault_injection` build). A Rust panic inside
# the collect (`GcCollectError::Panicked`) is unit-tested beside `runtime::worker::gc_failure`.
BASE_EXCEPTION_GC_APP = """
    import gc
    from mipyre import Mipyre

    class Boom(BaseException):
        pass

    def _collect(*args):
        raise Boom("b142-gc-panic")

    gc.collect = _collect

    app = Mipyre()

    @app.get("/")
    def root(req):
        return "ok"

    app.run(host="127.0.0.1", port=__PORT__, workers=1)
"""


@pytest.mark.parametrize("path", ["tpc-count", "pool-count"])
def test_a_base_exception_in_gc_collect_retires_the_last_worker_and_stops_the_server(server, path):
    s = server(BASE_EXCEPTION_GC_APP, GC_FAILURE_PATHS[path])
    s.wait_ready()
    code = _exit_code(s)
    out = s.output()
    assert code != 0, out[-3000:]
    stopped = [line for line in out.splitlines() if "the server stopped:" in line]
    assert stopped and "b142-gc-panic" in stopped[-1], out[-3000:]


# ---------------------------------------------------------------------------
# Item 2: MIPYRE_GC_MODE is parsed once; unknown / unsupported = startup error.
# Both are EnvConfig::parse / the topology check's, tested in crates/pyext/src/boot/config.rs
# (gc_numbers_must_parse) and crates/pyext/src/boot/topology.rs (the_pool_runs_count_and_off_but_not_idle); the start-time wiring is
# test_review_m5::test_a_bad_engine_env_value_is_a_startup_error.
# ---------------------------------------------------------------------------

# The app test_darwin_fanout starts.
PLAIN_APP = """
    from mipyre import Mipyre

    app = Mipyre()

    @app.get("/")
    def root(req):
        return "ok"

    app.run(host="127.0.0.1", port=__PORT__, workers=2)
"""


# ---------------------------------------------------------------------------
# Item 4: the pool's sync/async worker split
# ---------------------------------------------------------------------------

MIXED_APP = """
    import os
    from mipyre import Mipyre

    app = Mipyre()

    @app.get("/")
    def root(req):
        return "sync"

    @app.get("/async")
    async def aroot(req):
        return "async"

    app.run(host="127.0.0.1", port=__PORT__, workers=int(os.environ["M1B_WORKERS"]))
"""

POOL_ENV = {"MIPYRE_TPC": "0"}


def test_one_worker_mixed_sync_async_pool_is_startup_error(server):
    s = server(MIXED_APP, {**POOL_ENV, "M1B_WORKERS": "1"})
    assert s.wait_exit() != 0
    out = s.output()
    assert "workers=1" in out and "sync" in out and "async" in out, out


def test_pool_split_banner_matches_served_pools(server):
    s = server(MIXED_APP, {**POOL_ENV, "M1B_WORKERS": "3"})
    s.wait_ready()

    with urllib.request.urlopen(f"http://{HOST}:{s.port}/async", timeout=60) as resp:
        assert resp.read() == b"async"
    with urllib.request.urlopen(f"http://{HOST}:{s.port}/", timeout=5) as resp:
        assert resp.read() == b"sync"
    assert "Workers: 2 sync + 1 async" in s.output()
