"""Test: async bridge isolation — fast routes don't block on slow routes.

Proves that sub-interpreter async mode truly runs concurrent coroutines: a sync route answers
while an async one is in its await. The async request waits for a gate only the sync request
opens, after it said it is in its await; were it blocking the sync route, the sync request
would never run and the async one would end in its 504 instead of "released".
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
ENTERED, GATE = os.environ["ISOLATION_ENTERED"], os.environ["ISOLATION_GATE"]

async def slow(req):
    import asyncio
    open(ENTERED, "x").close()
    while not os.path.exists(GATE):
        await asyncio.sleep(0.005)
    return "released"

def fast(req):
    open(GATE, "x").close()
    return "fast"

app.route("GET", "/slow", slow, RuntimeChoice.Worker)
app.route("GET", "/fast", fast, RuntimeChoice.Worker)

if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0))
"""


def test_async_isolation(tmp_path):
    script_path = str(tmp_path / "async_isolation_app.py")
    with open(script_path, "w") as f:
        f.write(SERVER_SCRIPT)
    entered, gate = tmp_path / "entered", tmp_path / "gate"
    env = dict(os.environ, ISOLATION_ENTERED=str(entered), ISOLATION_GATE=str(gate))

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
        # Ready = the server's "Listening on" line (bound_port polls it).
        base = f"http://127.0.0.1:{bound_port(read_file(log_path), proc)}"
        slow_result = {}

        def call_slow():
            try:
                resp = urllib.request.urlopen(base + "/slow", timeout=None)
                slow_result["status"], slow_result["body"] = resp.status, resp.read().decode()
            except urllib.error.HTTPError as e:
                slow_result["status"] = e.code

        slow_thread = threading.Thread(target=call_slow)
        slow_thread.start()
        poll_until(entered.exists, timeout=None)

        fast_body = urllib.request.urlopen(base + "/fast", timeout=None).read().decode()
        slow_thread.join()

        assert fast_body == "fast", f"Fast route returned wrong body: {fast_body}"
        assert slow_result == {"status": 200, "body": "released"}, slow_result
    finally:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
