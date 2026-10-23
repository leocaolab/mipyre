"""A TPC thread keeps its worker's thread state current from one request to the next and
lets it go only when it is about to sleep (`worker::keep_attached_until_idle`).
A Python thread the app started in the worker's interpreter must therefore run while the
server is idle, not wait for the next request; and the server must still stop cleanly.

The worker's thread says how far it got through a file, so the test watches it without sending
a request (a request would wake the worker and let the thread run for that reason). It waits for
the steps to arrive, not for a fixed time: under a loaded gate the thread is slower, not stuck
(b/286).
"""

import http.client
import pathlib
import threading
import time
import urllib.parse

from mipyre import Mipyre
from mipyre.testing import TestClient, TestServer

from tests._helpers import poll_until, read_file

STEPS = 50

app = Mipyre()

_progress = {"done": 0}


@app.get("/start")
def start(req):
    progress_file = req.query_params.get("progress")

    def work():
        for _ in range(STEPS):
            time.sleep(0.005)
            _progress["done"] += 1
            if progress_file:
                pathlib.Path(progress_file).write_text(str(_progress["done"]))

    _progress["done"] = 0
    threading.Thread(target=work).start()
    return "started"


@app.get("/done")
def done(req):
    return str(_progress["done"])


def _get(conn, path):
    conn.request("GET", path)
    resp = conn.getresponse()
    return resp.status, resp.read().decode()


def test_a_python_thread_of_the_worker_runs_while_the_server_is_idle(tmp_path):
    """Whether the TPC thread really lets the worker's GIL go when it parks is a runtime fact: only a
    Python thread of that interpreter making progress while no request arrives shows it."""
    progress = tmp_path / "progress"
    with TestServer(app) as server, TestClient(server) as client:
        # One connection: one TPC thread, one worker interpreter.
        conn = http.client.HTTPConnection("127.0.0.1", client.port, timeout=10)
        assert _get(conn, f"/start?progress={urllib.parse.quote(str(progress))}") == (200, "started")
        # Idle: no request for this worker. Its thread must have let the GIL go, or no step
        # after the first request's is ever taken.
        # Waits for the steps, for no length of time (b/441 D9): a thread that never gets the GIL never takes
        # them, and pytest-timeout ends the wait.
        steps = read_file(str(progress))
        poll_until(lambda: steps() == str(STEPS), timeout=None, what=f"the worker's thread taking {STEPS} steps")
        assert steps() == str(STEPS)
        assert _get(conn, "/done") == (200, str(STEPS))
        conn.close()


def test_the_server_stops_after_serving_with_the_thread_state_kept():
    server = TestServer(app)
    conn = http.client.HTTPConnection("127.0.0.1", server.port, timeout=10)
    for _ in range(50):
        assert _get(conn, "/done")[0] == 200
    conn.close()
    server.close()
