"""b/886: SIGINT or SIGTERM that comes while `mipyre run` starts its workers stops the run.

The server binds its port before its sub-interpreter workers execute the app. Before b/886 the
multi-thread server and a TPC server with an async pool registered their stop signals only once
the workers were up, so a SIGINT in between reached only Python's handler, which the main thread
does not run until the server ends: the server came up and kept serving, and only kill stopped it.
A SIGTERM there killed the process outright.

Each worker of the app below says it is starting through a FIFO, then waits on a lock the test
holds; the test signals the process while a worker is held there, then lets the workers go.
"""

import fcntl
import os
import signal
import subprocess
import sys

import pytest

from tests._helpers import read_file

APP = r'''
import fcntl
import os

from mipyre import Mipyre
from mipyre.engine import _in_worker

if _in_worker():
    started = os.open(os.environ["B886_STARTED"], os.O_WRONLY)
    os.write(started, b"s")
    os.close(started)
    with open(os.environ["B886_GATE"]) as gate:
        fcntl.flock(gate, fcntl.LOCK_SH)

app = Mipyre()


@app.get("/sync")
def sync_route(req):
    return "ok"
'''

ASYNC_ROUTE = r'''

@app.get("/async")
async def async_route(req):
    return "ok"
'''


@pytest.mark.parametrize("stop", [signal.SIGINT, signal.SIGTERM], ids=["sigint", "sigterm"])
@pytest.mark.parametrize(
    ("server", "app_tail"),
    [({"MIPYRE_TPC": "0"}, ""), ({}, ASYNC_ROUTE)],
    ids=["multi-thread", "tpc-async-pool"],
)
def test_a_stop_signal_while_the_workers_start_stops_the_run(tmp_path, stop, server, app_tail):
    (tmp_path / "b886app.py").write_text(APP + app_tail)
    started = tmp_path / "started"
    os.mkfifo(started)
    gate = tmp_path / "gate"
    gate.touch()
    log_path = tmp_path / "run.log"
    log = read_file(str(log_path))
    env = {
        **os.environ,
        **server,
        "PYTHONUNBUFFERED": "1",
        "B886_STARTED": str(started),
        "B886_GATE": str(gate),
    }

    # Read and write: the FIFO always has a writer, so a read waits for a worker's byte.
    started_fd = os.open(started, os.O_RDWR)
    held = open(gate)
    fcntl.flock(held, fcntl.LOCK_EX)
    with open(log_path, "w") as out:
        run = subprocess.Popen(
            [sys.executable, "-m", "mipyre", "run", "b886app:app", "--host", "127.0.0.1", "--port", "0",
             "--subinterpreters" if server.get("MIPYRE_TPC") == "0" else "--threads", "1"],
            cwd=tmp_path, stdout=out, stderr=subprocess.STDOUT, start_new_session=True, env=env,
        )
    try:
        assert os.read(started_fd, 1) == b"s"
        run.send_signal(stop)
        held.close()
        code = run.wait(timeout=60)
    finally:
        os.close(started_fd)
        try:
            os.killpg(run.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass  # the run ended and left nothing in its session: the case the test wants
        run.wait()

    # The run stops as a stop signal stops a serving server: gracefully, exit status 0.
    assert code == 0, f"mipyre run exited {code} after {stop.name} during its start:\n{log()[-3000:]}"
