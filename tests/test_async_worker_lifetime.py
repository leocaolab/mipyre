"""An async request's deadline runs from the request, not from the worker's start; the
server then stops cleanly.

`_async_engine.py` used to wait for its fetcher thread with a 30 s timeout that started
counting when the worker started. 30 s into normal serving it logged a false "fetcher
did not exit" error and cancelled every task on the worker's loop, so any async request
in flight at that moment failed. The script sets a short request budget B
(`app.request_timeout`); a request that needs less than its task's share of B, sent once
the worker has served for 2 B, must still succeed.
"""
import threading
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

import pytest

from tests._helpers import answers, bound_port, read_file

# The script's request budget; the async task gets B - min(2 s, B / 2) of it (0.5 s).
BUDGET_S = 1.0

SCRIPT = '''
from mipyre import Mipyre
app = Mipyre()
app.request_timeout = BUDGET

@app.get("/")
def index(req):
    return {"ok": True}

@app.get("/a")
async def a(req):
    return {"async": True}

@app.get("/slow")
async def slow(req):
    import asyncio
    await asyncio.sleep(SLOW_FOR)
    return {"slow": True}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, workers=2)
'''.replace("BUDGET", str(BUDGET_S)).replace("SLOW_FOR", str(BUDGET_S / 5))


def _get(port, path, timeout=5):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=timeout) as r:
        return r.status


def _try(port, path, timeout):
    try:
        return _get(port, path, timeout)
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:  # noqa: BLE001 — reported by the assertion
        return repr(e)


@pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="POSIX signals")
def test_async_deadline_runs_from_the_request_not_the_worker_start(tmp_path):
    script = tmp_path / "app.py"
    script.write_text(SCRIPT)
    log = tmp_path / "server.log"
    env = dict(os.environ, MIPYRE_TPC="0")  # the async pool only exists outside TPC
    with open(log, "w") as out:
        proc = subprocess.Popen([sys.executable, str(script)], stdout=out,
                                stderr=subprocess.STDOUT, env=env, start_new_session=True)
    try:
        port = bound_port(read_file(str(log)), proc)
        answers(f"http://127.0.0.1:{port}/a", proc, read_file(str(log)))

        # The workers serve: 2 B later, a request that needs less than its task's budget.
        # A deadline counted from the worker's start would have passed already.
        time.sleep(2 * BUDGET_S)
        result = {}
        t = threading.Thread(target=lambda: result.update(status=_try(port, "/slow", 20)))
        t.start()
        t.join(30)
        assert result.get("status") == 200, (result, log.read_text()[-3000:])
        assert [_get(port, "/a") for _ in range(20)] == [200] * 20
        assert "did not exit within" not in log.read_text()

        proc.send_signal(signal.SIGINT)
        try:
            rc = proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            raise AssertionError("graceful stop hung:\n" + log.read_text()[-3000:])
        assert rc == 0, log.read_text()[-3000:]
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
