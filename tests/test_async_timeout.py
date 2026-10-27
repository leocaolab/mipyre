"""Tests for async handler timeout in sub-interpreter mode.

Rust answers 504 when a request outlives its budget (`app.request_timeout`; the script sets
1s). The async engine keeps no timer of its own: the request's task is not cancelled then and
runs to its end, its late answer dropped.

Covers:
- Fast async handlers complete normally
- Slow async handlers get 504, and their task is not cancelled
- Server remains healthy after async timeout
"""

import http.client
import json
import os
import signal
import subprocess
import sys
import urllib.request
import urllib.error

import pytest

from tests._helpers import answers, bound_port, kill_session, read_file


def start_server(script_path):
    """Starts the script (it binds port 0); returns the process and the bound port."""
    # Old pool 28s async-handler watchdog — no TPC equivalent, see
    # test_subinterp_timeout for the same rationale.
    env = dict(os.environ)
    env["MIPYRE_TPC"] = "0"
    log_path = script_path + ".log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, script_path],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        )
    port = bound_port(read_file(log_path), proc)
    # Phase 1: wait for sync endpoint (confirms Rust accept loop is up), for no length of time.
    answers(f"http://127.0.0.1:{port}/", proc, read_file(log_path))
    # Phase 2: wait for async workers (_worker/async_engine.py init: asyncio event
    # loop + sub-interpreter bootstrap). Workers start lazily; on this hardware
    # initialization can take 30+ seconds. One patient request is better than
    # many short-timeout requests that flood the queue without waiting.
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/fast-async", timeout=60)
    except urllib.error.HTTPError:
        pass  # non-200 still means the worker responded
    except (OSError, http.client.HTTPException):
        pass  # timed out — test will handle it
    return proc, port


def stop_server(proc, port):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (OSError, subprocess.TimeoutExpired):
            pass
    kill_session(proc)


ASYNC_TIMEOUT_SCRIPT = r'''
import os
from mipyre import Mipyre, ServerFlags

CANCEL_MARK = "%s"

app = Mipyre()
app.request_timeout = 1

@app.get("/")
def index(req):
    return {"ok": True}

@app.get("/slow-async")
async def slow_async(req):
    import asyncio
    try:
        await asyncio.sleep(35)
    except asyncio.CancelledError:
        with open(CANCEL_MARK, "w") as f:
            f.write("cancelled")
        raise
    return {"should": "never reach"}

@app.get("/fast-async")
async def fast_async(req):
    import asyncio
    await asyncio.sleep(0.01)
    return {"fast": True}

if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0))
'''


@pytest.fixture(scope="module")
def cancel_mark(tmp_path_factory):
    """The file the timed-out handler writes when its task is cancelled: in a directory of
    this run's own, so concurrent runs (xdist workers, other checkouts) never see each
    other's mark."""
    return str(tmp_path_factory.mktemp("async_timeout") / "cancelled")


@pytest.fixture(scope="module")
def server(cancel_mark):
    script = os.path.join(os.path.dirname(cancel_mark), "app.py")
    with open(script, "w") as f:
        f.write(ASYNC_TIMEOUT_SCRIPT % cancel_mark)
    proc, port = start_server(script)
    yield port
    stop_server(proc, port)


def test_fast_async_succeeds(server):
    """Fast async handlers complete normally."""
    try:
        resp = urllib.request.urlopen(
            f"http://127.0.0.1:{server}/fast-async", timeout=10
        )
        status = resp.status
        body = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        status = e.code
        body = e.read()
    assert status == 200
    assert body["fast"] is True


def test_slow_async_times_out(server, cancel_mark):
    """Async handler exceeding the request budget returns 504, and its task is not
    cancelled."""
    try:
        resp = urllib.request.urlopen(
            f"http://127.0.0.1:{server}/slow-async", timeout=35
        )
        status = resp.status
        body = resp.read()
    except urllib.error.HTTPError as e:
        status = e.code
        body = e.read()
    assert status == 504, f"Expected 504, got {status}: {body}"
    # A cancellation at the budget would have reached the handler before this 504 was sent.
    assert not os.path.exists(cancel_mark)


def test_server_healthy_after_async_timeout(server):
    """After async timeout, server still handles requests normally."""
    try:
        resp = urllib.request.urlopen(
            f"http://127.0.0.1:{server}/", timeout=5
        )
        status = resp.status
        body = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        status = e.code
        body = e.read()
    assert status == 200
    assert body["ok"] is True
