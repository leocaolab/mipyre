"""b/255 (guards b/84): SIGTERM, what systemd sends on stop, stops a running server in order, the
same way SIGINT does.

A real server, a request in flight, then the signal: the request in flight still gets its 200, the
app's `on_shutdown` hook runs, and the process exits 0. SIGINT and SIGTERM are asserted alike, on
both runtimes (the sub-interpreter pool and the TPC threads). Before b/84 only SIGINT was watched;
SIGTERM took its default action and killed the process mid-request (exit -15, no hook).

The handler holds its request until the test releases it, and the test releases it only once the
server has said it is shutting down: the request is in flight when the signal lands, with no
fixed wait on either side.

Why a real process and a real signal: what is tested is the OS delivering SIGTERM to a serving
process and the process's disposition for it (the handler the engine registered, or the default
action that kills it). No input to a function stands for a signal's delivery; the decision after it
(stop, drain, run the hooks) is the same `until_stopped` / drain path SIGINT takes.
"""

import os
import signal
import subprocess
import sys
import threading

import httpx
import pytest

from tests._helpers import bound_port, poll_until, read_file

SCRIPT = r'''
import os
import pathlib
import sys
import time

from mipyre import Mipyre

app = Mipyre()
RELEASE = pathlib.Path(os.environ["B255_RELEASE"])


@app.get("/slow")
def slow(req):
    print("b255-slow-started", flush=True)
    # Held until the test has seen the shutdown begin. Bounded, so a broken test cannot hang it.
    deadline = time.monotonic() + 60
    while not RELEASE.exists() and time.monotonic() < deadline:
        time.sleep(0.005)
    return "slow-done"


@app.on_shutdown
def mark():
    pathlib.Path(os.environ["B255_MARKER"]).write_text("shutdown ran")


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, workers=2)
'''

POOL = {"MIPYRE_TPC": "0"}
TPC = {"MIPYRE_TPC": "1"}


@pytest.mark.parametrize("runtime", [POOL, TPC], ids=["pool", "tpc"])
@pytest.mark.parametrize("stop", [signal.SIGTERM, signal.SIGINT], ids=["sigterm", "sigint"])
def test_a_stop_signal_finishes_the_request_in_flight_runs_on_shutdown_and_exits_0(tmp_path, runtime, stop):
    script = tmp_path / "app.py"
    script.write_text(SCRIPT)
    release, marker = tmp_path / "release", tmp_path / "marker"
    log_path = tmp_path / "app.log"
    log = read_file(str(log_path))
    env = {**os.environ, **runtime, "B255_RELEASE": str(release), "B255_MARKER": str(marker)}
    env.pop("MIPYRE_RUNTIME", None)
    with open(log_path, "w") as out:
        proc = subprocess.Popen(
            [sys.executable, str(script)], stdout=out, stderr=subprocess.STDOUT,
            start_new_session=True, env=env,
        )
    try:
        port = bound_port(log, proc)
        answer: dict = {}

        def get_slow():
            try:
                answer["response"] = httpx.get(f"http://127.0.0.1:{port}/slow", timeout=60)
            except httpx.HTTPError as e:
                answer["error"] = e

        client = threading.Thread(target=get_slow)
        client.start()
        poll_until(lambda: "b255-slow-started" in log(), timeout=30, what="/slow in flight")

        # The signal to the server process alone, as systemd's stop sends it.
        proc.send_signal(stop)
        poll_until(lambda: "Shutting down gracefully" in log() or proc.poll() is not None,
                   timeout=30, what="the shutdown to begin")
        release.touch()
        code = proc.wait(timeout=60)
        client.join(timeout=60)
    finally:
        release.touch()
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()

    output = log()
    assert code == 0, f"exit {code} after {stop.name}:\n{output[-3000:]}"
    assert "error" not in answer, f"the request in flight failed: {answer.get('error')!r}\n{output[-3000:]}"
    assert answer["response"].status_code == 200, output[-3000:]
    assert answer["response"].text == "slow-done"
    assert marker.exists() and marker.read_text() == "shutdown ran", f"on_shutdown did not run:\n{output[-3000:]}"
