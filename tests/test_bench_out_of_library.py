"""Benchmark code is kept out of the production library.

`MipyreApp.bench_inmem` / `bench_loopback` exist only in a `--features bench`
build. The bench tests below run only in such a build (they are skipped otherwise):

    maturin develop --release --features bench
    pytest tests/test_bench_out_of_library.py -k bench

`test_default_build_has_no_bench_methods` asserts the default build, which is what the
suite and CI install.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import textwrap

import pytest

from mipyre.engine import MipyreApp

PYTHON = sys.executable


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _write_script(script: str) -> str:
    fd, path = tempfile.mkstemp(prefix="mipyre_bench_", suffix=".py")
    with os.fdopen(fd, "w") as f:
        f.write(textwrap.dedent(script))
    return path


# ---------------------------------------------------------------------------
# bench_* only in a `--features bench` build
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
# appends to a counter file; the execution numbered BUILD_FAIL_AT raises. Worker builds run
# sequentially on the main thread, so the failure hits a known worker deterministically.
FAILING_WORKER = """
import os
_counter = os.environ["BUILD_COUNTER"]
with open(_counter, "a") as _f:
    _f.write("x")
with open(_counter) as _f:
    _n = len(_f.read())
if _n == int(os.environ["BUILD_FAIL_AT"]):
    raise RuntimeError(f"injected build failure at execution {_n}")

from mipyre import Mipyre, ServerFlags
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

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
        {"BUILD_COUNTER": str(counter), "BUILD_FAIL_AT": "3"},
    )


@pytest.mark.needs_feature("bench")
@pytest.mark.parametrize(
    "call",
    [
        "app._engine.bench_inmem(duration_s=1, threads=2)",
        "app._engine.bench_loopback(duration_s=1, threads=2)",
    ],
)
def test_bench_worker_build_failure_ends_the_built_workers(call, tmp_path):
    r = _worker_build_failure(call, tmp_path)
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    assert "Fatal Python error" not in out, out
    assert "caught:" in r.stdout and "worker 1:" in r.stdout, out
    assert "injected build failure at execution 3" in out, out
    # CPython's finalizer names interpreters nobody ended.
    assert "remaining subinterpreters" not in out, out


def test_tpc_worker_build_failure_ends_the_built_workers(tmp_path):
    """The production TPC path shares the same `build_workers`."""
    counter = tmp_path / "counter"
    script = FAILING_WORKER + """
if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0, threads=2))
"""
    r = _run_script(script, {"BUILD_COUNTER": str(counter), "BUILD_FAIL_AT": "3"})
    out = r.stdout + r.stderr
    assert r.returncode == 1, out
    assert "Fatal Python error" not in out, out
    assert "worker 1:" in out and "injected build failure at execution 3" in out, out
    # CPython's finalizer names interpreters nobody ended.
    assert "remaining subinterpreters" not in out, out


NO_ROOT_ROUTE = """
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.get("/elsewhere")
def elsewhere(req):
    return "not the benched path"
"""


@pytest.mark.needs_feature("bench")
@pytest.mark.parametrize(
    "call",
    [
        "app._engine.bench_inmem(duration_s=1, threads=1)",
        "app._engine.bench_loopback(duration_s=1, threads=1, client_conns=2)",
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
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.get("/")
def index(req):
    return "ok"
"""


@pytest.mark.needs_feature("bench")
@pytest.mark.parametrize(
    "call",
    [
        "total, elapsed = app._engine.bench_inmem(duration_s=1, threads=1); print('total', total, elapsed)",
        "total, elapsed, port = app._engine.bench_loopback(duration_s=1, threads=1, client_conns=2); print('total', total, elapsed)",
    ],
)
def test_bench_measures_a_healthy_app(call):
    r = _run_script(ROOT_ROUTE + BENCH_CALL.format(call=call), {})
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    assert "bench returned" in r.stdout, out
    total = int(next(line for line in r.stdout.splitlines() if line.startswith("total")).split()[1])
    assert total > 0, out
