"""`mipyre run` writes to the cache directory only for an app with process routes.

Process routes need a run directory there, for the server's copy of `mipyre`. An app without them
must start for a container user with no home, or on a read-only root filesystem.

Here the cache directory is under a home this user cannot write (and the cache variable is
unset): the app without process routes serves and nothing is written there; the app with one
stops with an error naming the variable to set, and serves once it is set to a writable directory.
"""

import os
import signal
import stat
import subprocess
import sys

import pytest

from mipyre._process_lane.server import _cache_dir_variable
from tests._helpers import answers, bound_port, kill_session, read_file

APP = r'''
from mipyre import Mipyre

app = Mipyre()


@app.get("/")
def index(req):
    return "main"
'''

PROCESS_ROUTE = r'''

@app.get("/process", runtime="process")
def in_a_process(req):
    return "process"
'''


@pytest.fixture
def read_only_home(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    home.chmod(stat.S_IRUSR | stat.S_IXUSR)
    yield home
    home.chmod(stat.S_IRWXU)


def _environ(home, *, process_routes: bool, **extra):
    # One lane worker for an app with process routes; an app without them has no lanes to size,
    # and a value it does not use would stop its start.
    env = {k: v for k, v in os.environ.items() if k not in ("XDG_CACHE_HOME", "MIPYRE_PROCESS_WORKERS")}
    lanes = {"MIPYRE_PROCESS_WORKERS": "1"} if process_routes else {}
    return env | {"HOME": str(home), "PYTHONUNBUFFERED": "1"} | lanes | extra


def _start(tmp_path, app, env):
    (tmp_path / "b877app.py").write_text(app)
    log_path = tmp_path / "run.log"
    with open(log_path, "w") as out:
        proc = subprocess.Popen(
            [sys.executable, "-m", "mipyre", "run", "b877app:app", "--host", "127.0.0.1", "--port", "0"],
            cwd=tmp_path, env=env, stdout=out, stderr=subprocess.STDOUT, start_new_session=True,
        )
    return proc, read_file(str(log_path))


def _serve_then_stop(proc, log, path):
    try:
        port = bound_port(log, proc)
        answers(f"http://127.0.0.1:{port}{path}", proc, log)
        proc.send_signal(signal.SIGINT)
        assert proc.wait(timeout=60) == 0, log()[-3000:]
    finally:
        kill_session(proc)


def test_an_app_without_process_routes_serves_with_no_writable_cache_dir_and_writes_none(tmp_path, read_only_home):
    proc, log = _start(tmp_path, APP, _environ(read_only_home, process_routes=False))
    _serve_then_stop(proc, log, "/")
    assert os.listdir(read_only_home) == []


def test_process_routes_without_a_writable_cache_dir_stop_the_start_naming_what_to_set(tmp_path, read_only_home):
    proc, log = _start(tmp_path, APP + PROCESS_ROUTE, _environ(read_only_home, process_routes=True))
    try:
        code = proc.wait(timeout=60)
    finally:
        kill_session(proc)
    output = log()
    assert code != 0, output[-3000:]
    assert "Permission denied" in output, output[-3000:]
    assert f"set {_cache_dir_variable()} to a directory this user can write" in output, output[-3000:]


def test_process_routes_serve_once_the_cache_dir_is_moved_to_a_writable_one(tmp_path, read_only_home):
    writable = tmp_path / "cache"
    writable.mkdir()
    variable = _cache_dir_variable()
    proc, log = _start(tmp_path, APP + PROCESS_ROUTE, _environ(read_only_home, process_routes=True, **{variable: str(writable)}))
    _serve_then_stop(proc, log, "/process")
