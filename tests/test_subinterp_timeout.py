"""The sub-interpreter pool past a request budget, on a real server: a request whose
caller got its 504 while it waited is not run, and a worker that cannot be interrupted
(`time.sleep`) is abandoned and replaced, so the next request is served.

When each step happens is the pool's (`runtime/pool/`: `next_step`, `settle_interrupted`,
unit-tested there); 504 on every serving path is test_review_m2's. This file keeps the one
run of the whole sequence on the pool, with a short budget (`app.request_timeout`) and grace
(`app.interrupt_grace`).
"""

import os
import signal
import subprocess
import sys
import threading
import urllib.error
import urllib.request

import pytest

from tests._helpers import answers, bound_port, kill_session, poll_until, read_file, settle


def get_status(port, path, timeout):
    try:
        return urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=timeout).status
    except urllib.error.HTTPError as e:
        return e.code


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


SINGLE_WORKER_SCRIPT = r'''
import os
os.environ["MIPYRE_WORKER"] = ""
from mipyre import Mipyre

MARK = os.environ["MIPYRE_TEST_MARK"]
SLOW_STARTED = os.environ["MIPYRE_TEST_SLOW_STARTED"]
app = Mipyre()
# The queued request is sent once the slow one is in the worker, so its caller gets its 504
# (3 s after it was sent) before the stuck worker is abandoned (3 + 1.5 s after the slow one
# began): no worker reaches it while its caller still waits; the request after it then has
# 2 s of its own budget left for the replacement.
app.request_timeout = 3
app.interrupt_grace = 1.5

@app.get("/")
def index(req):
    return {"ok": True}

@app.get("/slow")
def slow(req):
    import time
    open(SLOW_STARTED, "x").close()
    time.sleep(20)
    return {"should": "never reach"}

@app.get("/mark")
def mark(req):
    with open(MARK, "a") as f:
        f.write("ran\n")
    return {"ran": True}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, workers=1)
'''


@pytest.fixture
def single_worker(tmp_path):
    """One sync worker: whatever it is still doing blocks everything queued behind it."""
    mark = tmp_path / "mark"
    slow_started = tmp_path / "slow_started"
    script = tmp_path / "single_worker_app.py"
    script.write_text(SINGLE_WORKER_SCRIPT)
    env = dict(os.environ)
    env["MIPYRE_TPC"] = "0"
    env["MIPYRE_TEST_MARK"] = str(mark)
    env["MIPYRE_TEST_SLOW_STARTED"] = str(slow_started)
    log_path = str(script) + ".log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        )
    port = bound_port(read_file(log_path), proc)
    answers(f"http://127.0.0.1:{port}/", proc, read_file(log_path))
    yield port, mark, slow_started, log_path
    stop_server(proc, port)


def test_request_whose_caller_gave_up_is_not_run(single_worker):
    """A request queued behind a slow one, whose caller got its 504 while it waited, is
    skipped when the worker reaches it; the handler does not run for it. The slow one
    (`time.sleep(20)`, past the budget and not interruptible) has its worker abandoned, the
    log says so, and the replacement serves.

    Control: the same handler runs for a live request, so the mark would show if it ran."""
    port, mark, slow_started, log_path = single_worker
    results = {}

    def call(name, path, timeout):
        results[name] = get_status(port, path, timeout)

    slow = threading.Thread(target=call, args=("slow", "/slow", 40))
    slow.start()
    # /slow is in the worker before /mark queues behind it (until b/441: a 0.5 s sleep, after
    # which a loaded machine could still have /mark reach the worker first).
    poll_until(slow_started.exists, timeout=None)
    queued = threading.Thread(target=call, args=("queued", "/mark", 40))
    queued.start()
    slow.join()
    queued.join()
    assert results["slow"] == 504
    assert results["queued"] == 504

    # The worker gets to /mark only after /slow ends (or is abandoned); this request is queued
    # after /mark, so its answer means /mark was taken from the queue already.
    assert get_status(port, "/", timeout=30) == 200
    assert not mark.exists(), "the request whose caller gave up was run anyway"
    log = settle(read_file(log_path), lambda text: "abandon" in text)
    assert "abandoning it and starting a replacement" in log, log
    assert "mipyre-worker-0" in log, log

    # Control: a live request runs the handler.
    assert get_status(port, "/mark", timeout=10) == 200
    assert mark.exists()
