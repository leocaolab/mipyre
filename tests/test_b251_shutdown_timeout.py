"""b/251 (guards b/161, b/575): `app.shutdown_timeout` is the grace period a shutdown actually waits.

A worker stuck in a handler that never returns in time keeps its thread past any grace period. The
shutdown waits `shutdown_timeout` for it, reports it with the grace period it waited, and exits 1.
The report's grace is the injected value exactly: a setting that was ignored (falling back to the
5 s default) reports 5s (b/441 D3; it used to be read off the wall clock, under a bound computed
from the injected values). How long the grace is honoured is the pure join's (`join_within`).

Python's setter refuses what is no duration above zero: 0, a negative number, NaN, infinity.
"""

import math
import os
import re
import signal
import subprocess
import sys
import threading

import httpx
import pytest

from mipyre import Mipyre
from tests._helpers import bound_port, poll_until, read_file, set_subinterpreters

REQUEST_TIMEOUT_S = 0.3
SHUTDOWN_TIMEOUT_S = 0.2
# The grace as the engine logs it (a Rust `Duration`'s Debug form).
INJECTED_GRACE = "200ms"
# The line each serving path logs per thread it leaves behind, with the grace it waited.
ABANDONED = {
    "pool": re.compile(r"worker thread \S+.* did not exit within (\S+); abandoning it"),
    "tpc": re.compile(r"TPC thread still running at the join deadline \((\S+)\); left behind"),
    "main-bridge": re.compile(r"main-interp bridge thread \S+ did not exit within (\S+); abandoning it"),
}
# Each path: its environment, and the runtime the stuck route asks for (b/575: a `runtime="main"`
# route runs on the main-interpreter bridge threads of a TPC server with sub-interpreters, which
# `/ok` gives every app here).
PATHS = {"pool": ({"MIPYRE_TPC": "0"}, None), "tpc": ({}, None), "main-bridge": ({}, "main")}

SCRIPT = '''
import time

import os
from mipyre import Mipyre

app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.request_timeout = {request_timeout}
app.shutdown_timeout = {shutdown_timeout}


@app.get("/ok")
def ok(req):
    return "ok"


@app.get("/stuck", runtime={runtime!r})
def stuck(req):
    print("b251-stuck-started", flush=True)
    # One C call the request budget's interrupt cannot break into: the thread stays stuck.
    time.sleep(120)
    return "late"


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0)
'''


def _get_stuck(port: int, outcome: dict) -> None:
    try:
        outcome["response"] = httpx.get(f"http://127.0.0.1:{port}/stuck", timeout=60)
    except httpx.HTTPError as e:
        outcome["error"] = e


# A stuck sync handler on TPC holds the thread that would write an answer, so the stop closes its
# connection; other paths may answer 5xx. Either ends the request; a timeout does not.
CONNECTION_CLOSED = (httpx.RemoteProtocolError, httpx.ConnectError, httpx.ReadError)


def _ends_the_request(outcome: dict) -> bool:
    if (response := outcome.get("response")) is not None:
        return response.is_server_error
    return isinstance(outcome.get("error"), CONNECTION_CLOSED)


@pytest.mark.parametrize(
    ("outcome", "ends"),
    [
        ({"response": httpx.Response(500)}, True),
        ({"response": httpx.Response(503)}, True),
        ({"response": httpx.Response(499)}, False),
        ({"response": httpx.Response(200)}, False),
        ({"error": httpx.RemoteProtocolError("closed")}, True),
        ({"error": httpx.ReadError("reset")}, True),
        ({"error": httpx.ReadTimeout("hung")}, False),
        ({}, False),
    ],
)
def test_a_stuck_request_ends_with_a_server_error_or_a_closed_connection(outcome, ends):
    assert _ends_the_request(outcome) is ends


@pytest.mark.parametrize("seconds", [0, 0.0, -1, -0.5, math.nan, math.inf, -math.inf])
def test_shutdown_timeout_refuses_what_is_no_duration_above_zero(seconds):
    app = Mipyre()
    before = app.shutdown_timeout
    with pytest.raises(ValueError, match="shutdown_timeout must be a finite number of seconds above 0"):
        app.shutdown_timeout = seconds
    assert app.shutdown_timeout == before


def test_shutdown_timeout_keeps_a_valid_value():
    app = Mipyre()
    app.shutdown_timeout = 0.25
    assert app.shutdown_timeout == 0.25


@pytest.mark.parametrize("path", sorted(PATHS))
def test_a_stuck_worker_holds_the_stop_only_for_the_injected_grace_period(tmp_path, path):
    script = tmp_path / "app.py"
    env_extra, runtime = PATHS[path]
    script.write_text(
        SCRIPT.format(request_timeout=REQUEST_TIMEOUT_S, shutdown_timeout=SHUTDOWN_TIMEOUT_S, runtime=runtime)
    )
    log_path = tmp_path / "app.log"
    log = read_file(str(log_path))
    env = {**os.environ, **env_extra}
    env.pop("MIPYRE_TEST_MODE", None)
    if not env_extra:
        env.pop("MIPYRE_TPC", None)
    set_subinterpreters(env, "2")
    with open(log_path, "w") as out:
        proc = subprocess.Popen(
            [sys.executable, str(script)], stdout=out, stderr=subprocess.STDOUT,
            start_new_session=True, env=env,
        )
    try:
        port = bound_port(log, proc)
        stuck_outcome: dict = {}
        client = threading.Thread(target=_get_stuck, args=(port, stuck_outcome), daemon=True)
        client.start()
        poll_until(lambda: "b251-stuck-started" in log(), timeout=None, what="/stuck running")
        proc.send_signal(signal.SIGTERM)
        code = proc.wait()
        # The server's exit closes the stuck request's connection; joined here, the client's
        # outcome is this test's, not an unhandled thread exception in whichever test runs next.
        client.join()
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
    output = log()
    assert _ends_the_request(stuck_outcome), (stuck_outcome, output[-3000:])
    assert code == 1, f"exit {code}:\n{output[-3000:]}"
    assert "did not stop within the shutdown grace period" in output, output[-3000:]
    graces = ABANDONED[path].findall(output)
    assert graces and set(graces) == {INJECTED_GRACE}, (graces, output[-3000:])
    if path == "main-bridge":
        # Kept with the app's abandoned workers, so the caller's report names it too.
        assert re.search(r"worker\(s\) .*mipyre-main-bridge-\d+.* did not stop", output), output[-3000:]
