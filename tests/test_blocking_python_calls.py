"""A Python call that blocks does not take the server's IO with it.

- 64b0c9ef / 03c61193 (#3): a blocking main-interpreter handler (and a WebSocket handler's
  join) ran on a Tokio IO thread; with as many blocked as there are IO threads, every other
  route starved. Now handlers run on the blocking pool.
- 03c61193 (#4): a WebSocket handler's `recv()` held the GIL while it waited for the client,
  freezing every other Python thread (C5.1). Now it waits detached.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import urllib.request

import pytest

from mipyre.engine import ServerFlags
from mipyre.testing import TestClient, TestServer
from tests._helpers import bound_port, kill_session, poll_until, read_file

IO_THREADS = 2


def test_blocked_main_handlers_past_the_io_thread_count_leave_a_fast_route_answering():
    from tests.apps.blocking_main_handlers import app, entered, release

    blocked = IO_THREADS + 2
    # A test server reads no environment (b/335 M5.6): the multi-thread server and its threads are its config.
    with TestServer(app, config=ServerFlags(tpc=False, threads=IO_THREADS)) as server, TestClient(server) as c:
        statuses: list[int] = []

        def call_slow() -> None:
            with urllib.request.urlopen(f"{c.base_url}/slow", timeout=30) as resp:
                statuses.append(resp.status)

        slow = [threading.Thread(target=call_slow) for _ in range(blocked)]
        try:
            for thread in slow:
                thread.start()
            for i in range(blocked):
                assert entered.acquire(timeout=10), f"only {i} of {blocked} slow handlers ran"
            # Every slow handler is blocked inside Python; the fast one is answered all the same.
            with urllib.request.urlopen(f"{c.base_url}/fast", timeout=10) as resp:
                assert resp.read() == b"fast"
        finally:
            release.set()
            for thread in slow:
                thread.join(30)
        assert statuses == [200] * blocked


WS_SCRIPT = '''
from mipyre import Mipyre

app = Mipyre(runtime="main")

@app.websocket("/wait")
def wait(ws):
    ws.send("ready")
    msg = ws.recv()
    ws.send(f"got {msg}")

@app.get("/python")
def python(req):
    return "ran"

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0)
'''


@pytest.fixture
def ws_server(tmp_path):
    """The script above as its own server process: when a handler froze the GIL, the frozen
    process is the server's, and this test (the client) still sees it and fails."""
    script = tmp_path / "app.py"
    script.write_text(WS_SCRIPT)
    log_path = tmp_path / "server.log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env={**os.environ, "MIPYRE_TPC": "0"},
        )
    try:
        port = bound_port(read_file(str(log_path)), proc)
        poll_until(lambda: _answers(port), timeout=None, what="the server answering")
        yield port
    finally:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            kill_session(proc)


def _answers(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/python", timeout=2) as resp:
            return resp.status == 200
    except OSError:
        return False


def test_a_websocket_handler_waiting_in_recv_leaves_other_python_threads_running(ws_server):
    from websockets.sync.client import connect

    with connect(f"ws://127.0.0.1:{ws_server}/wait", open_timeout=10) as ws:
        assert ws.recv(timeout=10) == "ready"
        # The handler is now waiting in recv() for this client. Another Python thread (a
        # handler on the blocking pool) still runs: recv() waits without the GIL.
        with urllib.request.urlopen(f"http://127.0.0.1:{ws_server}/python", timeout=10) as resp:
            assert resp.read() == b"ran"
        ws.send("done")
        assert ws.recv(timeout=10) == "got done"
