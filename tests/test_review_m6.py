"""Review cced8c2, milestone M6: benchmark code out of the production library.

D1: the built-in `benchmark.BenchmarkService/GetSum` gRPC handler intercepted every
`application/grpc*` POST before routing, so a user route accepting gRPC(-web) was
unreachable. It is now opt-in (`app.enable_grpc_benchmark()`) and, when on, intercepts
only its own path.

D2: `MipyreApp.bench_inmem` / `bench_loopback` exist only in a `--features bench`
build. The bench tests below run only in such a build (they are skipped otherwise):

    maturin develop --release --features bench
    pytest tests/test_review_m6.py -k bench

`test_default_build_has_no_bench_methods` asserts the default build, which is what the
suite and CI install.

Serving paths, as `app.run` picks them:
  - "gil"   MIPYRE_RUNTIME=main, MIPYRE_TPC=0  → multi-thread runtime, handlers on main
  - "tpc"   default runtime                        → TPC inline (sub-interp)
  - "pool"  default runtime, MIPYRE_TPC=0        → sub-interp pool
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import textwrap
import urllib.error
import urllib.request

import pytest

from tests._helpers import bound_port, read_file

from mipyre.engine import MipyreApp

PYTHON = sys.executable
HOST = "127.0.0.1"

PATHS: dict[str, dict[str, str | None]] = {
    "gil": {"runtime": "main", "tpc": "0"},
    "tpc": {"runtime": None, "tpc": None},
    "pool": {"runtime": None, "tpc": "0"},
}

GET_SUM = "/benchmark.BenchmarkService/GetSum"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _write_script(script: str) -> str:
    fd, path = tempfile.mkstemp(prefix="mipyre_m6_", suffix=".py")
    with os.fdopen(fd, "w") as f:
        f.write(textwrap.dedent(script))
    return path


class Server:
    """A Mipyre server running `script` on one serving path. `app.run` in the script
    reads `port` from `M6_PORT`; the "gil" path runs with `MIPYRE_RUNTIME=main`."""

    def __init__(self, script: str, path: str, workers: int = 2):
        self.port = 0  # the bound one once the server listens
        self.script_path = _write_script(script)
        self.log_path = self.script_path + ".log"
        env = dict(os.environ)
        env.pop("MIPYRE_RUNTIME", None)
        if (runtime := PATHS[path]["runtime"]) is not None:
            env["MIPYRE_RUNTIME"] = runtime
        env["M6_PORT"] = "0"
        env["M6_WORKERS"] = str(workers)
        env.pop("MIPYRE_TPC", None)
        if (tpc := PATHS[path]["tpc"]) is not None:
            env["MIPYRE_TPC"] = tpc
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                [PYTHON, self.script_path],
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=env,
            )
        # The Listening line comes once the sockets are bound: waits for it or the exit (b/441 T2).
        try:
            self.port = bound_port(read_file(self.log_path), self.proc)
        except RuntimeError as e:
            raise RuntimeError(f"server ({path}) did not start:\n{self.stop()}") from e

    def post(self, path: str, body: bytes, content_type: str) -> tuple[int, bytes, dict]:
        req = urllib.request.Request(
            f"http://{HOST}:{self.port}{path}",
            data=body,
            method="POST",
            headers={"Content-Type": content_type},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
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
app.run(host="127.0.0.1", port=int(os.environ["M6_PORT"]), workers=int(os.environ["M6_WORKERS"]))
"""


def _sum_request(a: int, b: int) -> bytes:
    """A gRPC-framed `SumRequest { a, b }` (small non-negative ints: one-byte varints)."""
    message = bytes([0x08, a, 0x10, b])
    return b"\x00" + len(message).to_bytes(4, "big") + message


def _sum_reply(result: int) -> bytes:
    message = bytes([0x08, result])
    return b"\x00" + len(message).to_bytes(4, "big") + message


# ---------------------------------------------------------------------------
# D1: the gRPC benchmark service is opt-in and intercepts only its own path
# ---------------------------------------------------------------------------

GRPC_USER_ROUTE = """
from mipyre import Mipyre
app = Mipyre()

@app.post("/rpc")
def rpc(req):
    return "user handler: " + req.headers.get("content-type", "")
"""


GRPC_ON = GRPC_USER_ROUTE + """
app.enable_grpc_benchmark()
"""


def test_grpc_on_get_sum_works_and_intercepts_only_its_path():
    """The wiring: enable_grpc_benchmark() answers GetSum and leaves every other path to the
    routes. Which request is the gRPC method (its path, POST, an application/grpc*
    content-type, and only when enabled) is `decide`'s, unit-tested in
    crates/http/src/plan/request.rs (the_grpc_method_is_decided_first_and_only_when_on); one
    pipeline serves it on every path."""
    srv = Server(GRPC_ON + RUN, "tpc")
    try:
        status, body, headers = srv.post(GET_SUM, _sum_request(2, 3), "application/grpc")
        assert status == 200
        assert headers["content-type"] == "application/grpc"
        assert body == _sum_reply(5)
        status, body, _ = srv.post("/rpc", _sum_request(2, 3), "application/grpc")
        assert (status, body) == (200, b"user handler: application/grpc")
    finally:
        srv.stop()


# ---------------------------------------------------------------------------
# D2: bench_* only in a `--features bench` build
# ---------------------------------------------------------------------------


def test_default_build_has_no_bench_methods():
    assert not hasattr(MipyreApp, "bench_inmem")
    assert not hasattr(MipyreApp, "bench_loopback")


def _run_script(script: str, env_extra: dict[str, str], timeout: float = 60):
    path = _write_script(script)
    env = dict(os.environ)
    env.update(env_extra)
    try:
        return subprocess.run(
            [PYTHON, path], capture_output=True, text=True, timeout=timeout, env=env
        )
    finally:
        os.unlink(path)


# Every execution of the script (main first, then each worker as it is built, in order)
# appends to a counter file; the execution numbered M6_FAIL_AT raises. Worker builds run
# sequentially on the main thread, so the failure hits a known worker deterministically.
FAILING_WORKER = """
import os
_counter = os.environ["M6_COUNTER"]
with open(_counter, "a") as _f:
    _f.write("x")
with open(_counter) as _f:
    _n = len(_f.read())
if _n == int(os.environ["M6_FAIL_AT"]):
    raise RuntimeError(f"m6 injected failure at execution {_n}")

from mipyre import Mipyre
app = Mipyre()

@app.get("/")
def index(req):
    return "ok"
"""

BENCH_CALL = """
if __name__ == "__main__":
    try:
        {call}
    except RuntimeError as e:
        print("caught:", e)
    else:
        print("bench returned")
"""


def _worker_build_failure(call: str, tmp_path) -> subprocess.CompletedProcess:
    counter = tmp_path / "counter"
    # Execution 1 is main, 2 is worker 0, 3 is worker 1: worker 1 fails after worker 0
    # was built.
    return _run_script(
        FAILING_WORKER + BENCH_CALL.format(call=call),
        {"M6_COUNTER": str(counter), "M6_FAIL_AT": "3"},
    )


@pytest.mark.needs_feature("bench")
@pytest.mark.parametrize(
    "call",
    [
        "app._engine.bench_inmem(duration_s=1, workers=2)",
        "app._engine.bench_loopback(duration_s=1, workers=2)",
    ],
)
def test_bench_worker_build_failure_ends_the_built_workers(call, tmp_path):
    r = _worker_build_failure(call, tmp_path)
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    assert "Fatal Python error" not in out, out
    assert "caught:" in r.stdout and "worker 1:" in r.stdout, out
    assert "m6 injected failure at execution 3" in out, out
    # CPython's finalizer names interpreters nobody ended.
    assert "remaining subinterpreters" not in out, out


def test_tpc_worker_build_failure_ends_the_built_workers(tmp_path):
    """Regression: the production TPC path shares the same `build_workers`."""
    counter = tmp_path / "counter"
    script = FAILING_WORKER + """
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, workers=2)
"""
    r = _run_script(script, {"M6_COUNTER": str(counter), "M6_FAIL_AT": "3"})
    out = r.stdout + r.stderr
    assert r.returncode == 1, out
    assert "Fatal Python error" not in out, out
    assert "worker 1:" in out and "m6 injected failure at execution 3" in out, out
    # CPython's finalizer names interpreters nobody ended.
    assert "remaining subinterpreters" not in out, out


NO_ROOT_ROUTE = """
from mipyre import Mipyre
app = Mipyre()

@app.get("/elsewhere")
def elsewhere(req):
    return "not the benched path"
"""


@pytest.mark.needs_feature("bench")
@pytest.mark.parametrize(
    "call",
    [
        "app._engine.bench_inmem(duration_s=1, workers=1)",
        "app._engine.bench_loopback(duration_s=1, workers=1, client_conns=2)",
    ],
)
def test_bench_reports_non_200_responses_instead_of_counting_them(call):
    """The bench drives `GET /`. Here it is a 404: the client must report it, not count
    it as throughput (a client failure was discarded before)."""
    r = _run_script(NO_ROOT_ROUTE + BENCH_CALL.format(call=call), {})
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    assert "caught:" in r.stdout and "404" in r.stdout, out


ROOT_ROUTE = """
from mipyre import Mipyre
app = Mipyre()

@app.get("/")
def index(req):
    return "ok"
"""


@pytest.mark.needs_feature("bench")
@pytest.mark.parametrize(
    "call",
    [
        "total, elapsed = app._engine.bench_inmem(duration_s=1, workers=1); print('total', total, elapsed)",
        "total, elapsed, port = app._engine.bench_loopback(duration_s=1, workers=1, client_conns=2); print('total', total, elapsed)",
    ],
)
def test_bench_measures_a_healthy_app(call):
    r = _run_script(ROOT_ROUTE + BENCH_CALL.format(call=call), {})
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    assert "bench returned" in r.stdout, out
    total = int(next(line for line in r.stdout.splitlines() if line.startswith("total")).split()[1])
    assert total > 0, out
