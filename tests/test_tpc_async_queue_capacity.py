"""The thread-per-core server sizes its async pool by `MIPYRE_ASYNC_QUEUE_CAPACITY`: with room
for one `async def` request, a second one arriving while the first waits is answered 503, and
the first still finishes once its gate opens.
"""
import os
import signal
import subprocess
import sys
import threading
import urllib.error
import urllib.request

from tests._helpers import bound_port, poll_until, read_file

SERVER_SCRIPT = """
import os
from mipyre.engine import MipyreApp, RuntimeChoice, ServerFlags

app = MipyreApp()
ENTERED, GATE = os.environ["HOLD_ENTERED"], os.environ["HOLD_GATE"]

async def hold(req):
    import asyncio
    open(ENTERED, "x").close()
    while not os.path.exists(GATE):
        await asyncio.sleep(0.005)
    return "held"

def ping(req):
    return "pong"

app.route("GET", "/hold", hold, RuntimeChoice.Worker)
app.route("GET", "/ping", ping, RuntimeChoice.Worker)

if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0))
"""


def _status(url: str) -> int:
    try:
        return urllib.request.urlopen(url, timeout=None).status
    except urllib.error.HTTPError as e:
        return e.code


def test_a_request_past_the_tpc_async_capacity_is_refused(tmp_path):
    script_path = str(tmp_path / "hold_app.py")
    with open(script_path, "w") as f:
        f.write(SERVER_SCRIPT)
    entered, gate = tmp_path / "entered", tmp_path / "gate"
    # Two threads with a def route among the routes give the async pool one worker.
    env = dict(
        os.environ,
        HOLD_ENTERED=str(entered),
        HOLD_GATE=str(gate),
        MIPYRE_THREADS="2",
        MIPYRE_ASYNC_QUEUE_CAPACITY="1",
    )
    env.pop("MIPYRE_TPC", None)

    log_path = script_path + ".log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, script_path],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        )
    try:
        base = f"http://127.0.0.1:{bound_port(read_file(log_path), proc)}"
        first = {}
        first_thread = threading.Thread(target=lambda: first.update(status=_status(base + "/hold")))
        first_thread.start()
        poll_until(entered.exists, timeout=None)

        assert _status(base + "/hold") == 503
        assert _status(base + "/ping") == 200

        gate.touch()
        first_thread.join()
        assert first == {"status": 200}
    finally:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
