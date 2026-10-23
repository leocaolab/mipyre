"""b/183: the workers of a server start together, or none of them does.

Each worker is created on the thread that serves it. Its thread reports the start, and the
server starts only when every thread has. A failed start (its setup raises, its creation is
refused, its setup panics) aborts the others: the server exits with the failure, and nothing is
served. Both paths are covered: the sub-interpreter pool (`MIPYRE_TPC=0`) and the TPC threads
(the default).

The setup failures come from the script itself: it raises only in a worker interpreter (the main
interpreter is id 0, so the script's module code runs there without raising). A refused creation
and a panicking start abort the set the same way; the gate's verdicts on them are unit-tested
beside it (`runtime::start` tests: `serve_or_abort`, `start_here`, `StartFailure::of`, b/299).
"""

import os
import signal
import subprocess
import sys
import urllib.request

import pytest

from tests._helpers import answers, bound_port, read_file, set_subinterpreters

RAISE_IN_WORKER_SCRIPT = r'''
import os
from concurrent import interpreters

from mipyre import Mipyre

app = Mipyre()


@app.get("/")
def index(req):
    return "ok"


# The main interpreter is id 0; a worker is any other.
if interpreters.get_current().id != interpreters.get_main().id and os.environ.get("B183_RAISE"):
    raise RuntimeError("b183 setup raised in a worker")


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0)
'''


def run_script(tmp_path, env_extra, *, script=RAISE_IN_WORKER_SCRIPT, timeout=90):
    """Runs the script to its end (a failed start exits by itself); returns the exit code and
    the output."""
    script_path = tmp_path / "app.py"
    script_path.write_text(script)
    log_path = tmp_path / "app.log"
    env = dict(os.environ)
    env.update(env_extra)
    set_subinterpreters(env, "3")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, str(script_path)],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        )
        try:
            code = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait()
            raise AssertionError(f"the start hung for {timeout}s; output:\n{read_file(str(log_path))()}")
    return code, read_file(str(log_path))()


def serve_and_get(tmp_path, env_extra):
    """Starts the script, which must serve; returns the body of GET / and stops the server."""
    script_path = tmp_path / "app.py"
    script_path.write_text(RAISE_IN_WORKER_SCRIPT)
    log_path = tmp_path / "app.log"
    env = dict(os.environ)
    env.update(env_extra)
    set_subinterpreters(env, "3")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, str(script_path)],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        )
    try:
        port = bound_port(read_file(str(log_path)), proc)
        answers(f"http://127.0.0.1:{port}/", proc, read_file(str(log_path)))
        return urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=10).read()
    finally:
        if proc.poll() is None:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait(timeout=30)


POOL = {"MIPYRE_TPC": "0"}
TPC = {"MIPYRE_TPC": "1"}


@pytest.mark.parametrize("topology", [POOL, TPC], ids=["pool", "tpc"])
def test_a_set_that_starts_serves(tmp_path, topology):
    assert serve_and_get(tmp_path, topology) == b"ok"


@pytest.mark.parametrize("topology", [POOL, TPC], ids=["pool", "tpc"])
def test_a_setup_that_raises_aborts_the_start(tmp_path, topology):
    code, out = run_script(tmp_path, {**topology, "B183_RAISE": "1"})
    assert code != 0, out
    assert "b183 setup raised in a worker" in out, out
    assert "Listening on" not in out, out
