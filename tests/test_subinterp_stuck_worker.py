"""A sync sub-interpreter worker stuck in one handler: the pool gets its capacity back.

The request is answered 504 at the request budget, which the scripts set to 2 s
(`app.request_timeout`; the default is 30 s). The worker still runs the handler:
- a pure-Python loop is interrupted (the pool raises into the unit of work), and the same
  worker serves the next request;
- `time.sleep` releases the GIL but cannot be interrupted before it returns, so the worker
  is abandoned and a replacement serves: test_subinterp_timeout runs that one.

The server runs one worker, so a request answered after the 504 is served only if the pool
recovered that worker.
"""

import os
import signal
import subprocess
import sys
import urllib.error
import urllib.request

import pytest

from tests._helpers import answers, bound_port, kill_session, read_file, text_appears

LOOP_SCRIPT = r'''
import logging, os
from mipyre import Mipyre

app = Mipyre()
app.request_timeout = 2

@app.get("/")
def index(req):
    return {"ok": True}

@app.get("/spin")
def spin(req):
    while True:
        pass

@app.get("/say")
def say(req):
    # The line the fence test logs itself, just before the stop.
    logging.getLogger("stuck").error(req.query_params["text"])
    return "said"

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, subinterpreters=1)
'''

def start_server(tmp_path, source):
    script = tmp_path / "stuck_app.py"
    script.write_text(source)
    env = dict(os.environ)
    env["MIPYRE_TPC"] = "0"  # exercise the sub-interpreter pool, not the TPC inline path
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
    return proc, port, log_path


def stop_server(proc, port):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except OSError:
        pass
    proc.wait(timeout=10)
    kill_session(proc)


def log_at_exit(proc, log_path) -> str:
    """The server's whole log: it is stopped (SIGINT) first, and its exit is the log's fence
    (`run()` waits for every line logged before it, `flush_logs`). For a line that must not be
    there: no wait for it can say it will not come (b/495)."""
    os.killpg(os.getpgid(proc.pid), signal.SIGINT)
    proc.wait(timeout=None)
    log = read_file(log_path)()
    assert proc.returncode >= 0, f"killed by signal {-proc.returncode}, the log is cut:\n{log[-4000:]}"
    return log


def get_status(port, path, timeout):
    try:
        return urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=timeout).status
    except urllib.error.HTTPError as e:
        return e.code


@pytest.fixture
def loop_server(tmp_path):
    proc, port, log_path = start_server(tmp_path, LOOP_SCRIPT)
    yield proc, port, log_path
    stop_server(proc, port)


RAISE_SCRIPT = r'''
import os
from mipyre import Mipyre

app = Mipyre()
app.request_timeout = 4

@app.get("/")
def index(req):
    return {"ok": True}

@app.get("/raise")
def raises(req):
    raise KeyboardInterrupt("raised by the handler itself, not by the watchdog")

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, subinterpreters=1)
'''


def test_a_handler_raising_keyboard_interrupt_by_itself_is_500_not_504(tmp_path):
    """Only the watchdog's own interruption at the request budget is a 504: a handler that
    raises KeyboardInterrupt by itself is the 500 it always was, and its worker still serves."""
    proc, port, log_path = start_server(tmp_path, RAISE_SCRIPT)
    try:
        assert get_status(port, "/raise", timeout=15) == 500
        assert get_status(port, "/", timeout=15) == 200
        # The log writer is non-blocking: the line lands some time after the 500 (b/441 L7).
        text_appears(read_file(log_path), "the handler raised KeyboardInterrupt", proc)
    finally:
        stop_server(proc, port)


def test_interruptible_stuck_worker_is_interrupted_and_serves_again(loop_server):
    """A pure-Python loop takes the interruption: no worker is abandoned, and the one
    worker serves the next request."""
    proc, port, log_path = loop_server

    assert get_status(port, "/spin", timeout=45) == 504

    # One worker, so this is served only once the worker is back in its queue loop.
    assert get_status(port, "/", timeout=15) == 200

    log = log_at_exit(proc, log_path)
    assert "abandon" not in log, log


def test_the_log_at_exit_has_a_line_logged_just_before_the_stop(loop_server):
    """The fence of the test above: a line the worker logs just before the stop is in the log
    it reads, so an abandonment logged that late would be seen too."""
    proc, port, log_path = loop_server
    assert get_status(port, "/say?text=abandon-probe-b495", timeout=15) == 200
    assert "abandon-probe-b495" in log_at_exit(proc, log_path)
