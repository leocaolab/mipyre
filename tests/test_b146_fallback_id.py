"""b/146: the fallback's handler sits in the same handler array as the routes', named by an id
the route table hands out.

The fallback is registered before every route here, so it takes the first handler slot: a
route on main, on a sub-interpreter worker or on a process lane must still run its own
handler (workers index by route, lanes look their handler up by route key in their own
table), and a request no route matched must run the fallback."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
import urllib.error
import urllib.request

import pytest

from tests._helpers import bound_port, read_file

APP = textwrap.dedent(
    """
    from mipyre import Mipyre

    app = Mipyre()

    @app.fallback
    def fallback(req):
        return f"fallback:{req.path}"

    @app.get("/worker")
    def on_worker(req):
        return "worker"

    @app.get("/main", runtime="main")
    def on_main(req):
        return "main"

    @app.get("/process", runtime="process")
    def on_process(req):
        return "process"
    """
)

# "tpc": TPC threads + main bridge; "pool": the sub-interpreter pool + spawn_blocking on main.
PATHS = {"tpc": {}, "pool": {"MIPYRE_TPC": "0"}}


def get(port: int, path: str) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=30) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


@pytest.mark.parametrize("path", sorted(PATHS))
def test_routes_and_the_fallback_each_run_their_own_handler(tmp_path, path):
    app_file = tmp_path / "b146_app.py"
    app_file.write_text(APP)
    log_path = str(tmp_path / "server.log")
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    env.update(PATHS[path], MIPYRE_PROCESS_WORKERS="1")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "mipyre", "run", f"{app_file}:app", "--port", "0"],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
            cwd=str(tmp_path),
        )
    read = read_file(log_path)
    try:
        port = bound_port(read, proc)
        got = {p: get(port, p) for p in ["/worker", "/main", "/process", "/nope/deeper"]}
        assert got == {
            "/worker": (200, b"worker"),
            "/main": (200, b"main"),
            "/process": (200, b"process"),
            "/nope/deeper": (200, b"fallback:/nope/deeper"),
        }, read()[-3000:]
    finally:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=5)
