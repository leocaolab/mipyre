"""E2E-8, async-DB part (Layer 2, M1): `PgPool.*_async` on main keeps working once workers
execute the engine.

Runs `_l2_async_db_app.py`, drives an async `runtime="main"` DB route and worker routes
concurrently while a main-interpreter thread awaits `fetch_all_async` on its own loop, stops
the server with SIGINT, and scans the whole log for Rust panics and the PyO3 fork's refusals.

Before M1 this failed: `pyo3-async-runtimes` resolved the futures with a bare
`Python::attach` on its Tokio threads, which the fork refuses once several interpreters have
executed the engine (spike R-3: the await never completed).
"""
from __future__ import annotations

import collections
import concurrent.futures
import os
import signal
import subprocess
import sys

import httpx
import pytest

from tests.conftest import fork_panic_lines
from tests._helpers import answers, bound_port, read_file, set_subinterpreters

PG_DSN = os.environ.get("MIPYRE_TEST_PG_DSN")

pytestmark = [
    pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="own-GIL sub-interpreters"),
    pytest.mark.skipif(PG_DSN is None, reason="MIPYRE_TEST_PG_DSN not set"),
]

HERE = os.path.dirname(os.path.abspath(__file__))
PATHS = ("/adb", "/w")
# The load, by count (b/441; until then 6 s of it, as many requests as the machine managed):
# each of `CLIENTS` threads sends every path `ROUNDS` times.
CLIENTS = 12
ROUNDS = 25


def _wait_up(base: str, proc: subprocess.Popen, log_path: str) -> None:
    answers(base + "/w", proc, read_file(log_path))


def _hammer(base: str) -> collections.Counter:
    def worker(i: int) -> collections.Counter:
        c: collections.Counter = collections.Counter()
        with httpx.Client(base_url=base, timeout=None) as client:
            for n in range(i, i + ROUNDS * len(PATHS)):
                path = PATHS[n % len(PATHS)]
                try:
                    r = client.get(path)
                    ok = r.status_code == 200 and (
                        path != "/adb" or r.json() == {"rows": [{"two": 2, "s": "x"}]}
                    )
                    c[(path, r.status_code if ok else f"BAD {r.status_code} {r.text[:80]}")] += 1
                except httpx.HTTPError as e:
                    c[(path, f"EXC {type(e).__name__}")] += 1
        return c

    statuses: collections.Counter = collections.Counter()
    with concurrent.futures.ThreadPoolExecutor(CLIENTS) as ex:
        for c in ex.map(worker, range(CLIENTS)):
            statuses.update(c)
    return statuses


@pytest.mark.parametrize("tpc", ["1", "0"], ids=["tpc", "pool"])
def test_async_db_on_main_while_workers_execute_engine(tmp_path, tpc):
    log_path = str(tmp_path / "server.log")
    env = dict(os.environ, L2_PORT="0", RUST_BACKTRACE="0", MIPYRE_TPC=tpc)
    set_subinterpreters(env, "2")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "_l2_async_db_app.py")],
            env=env, stdout=log, stderr=subprocess.STDOUT,
        )
    try:
        port = bound_port(read_file(log_path), proc)
        base = f"http://127.0.0.1:{port}"
        _wait_up(base, proc, log_path)
        assert httpx.get(base + "/w", timeout=5).json() == {"real_engine": True}
        statuses = _hammer(base)
        bg = httpx.get(base + "/bg", timeout=5).json()
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
        try:
            exit_code = proc.wait(30)
        except subprocess.TimeoutExpired:
            proc.kill()
            exit_code = "killed after SIGINT timeout"
    log_text = open(log_path).read()
    print("statuses:", dict(sorted(statuses.items(), key=str)), "background:", bg,
          "exit:", exit_code)

    assert [line for line in log_text.splitlines() if "panicked at" in line] == []
    assert fork_panic_lines(log_text) == [], "\n".join(fork_panic_lines(log_text))
    bad = {k: v for k, v in statuses.items() if k[1] != 200}
    assert not bad, f"bad responses under load: {bad}\n{log_text[-4000:]}"
    # Every request sent was answered 200 (and right): `CLIENTS × ROUNDS` per path.
    assert dict(statuses) == {(path, 200): CLIENTS * ROUNDS for path in PATHS}, dict(statuses)
    assert bg["errors"] == [] and bg["ok"] > 0, bg
    assert exit_code == 0, f"exit {exit_code}\n{log_text[-4000:]}"
