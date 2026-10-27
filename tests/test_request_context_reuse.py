"""A worker, a main-interpreter thread and a process-lane worker enter the context the last
request left empty again (`in_reused_context`): no `ContextVar` write crosses from one request to
the next, also when something the first request scheduled still holds its context and writes into
it later.

All requests go over one keep-alive connection, so one TPC worker serves them in turn.
"""

import asyncio
import contextvars
import http.client
import os
import subprocess
import sys
import textwrap

import pytest

from mipyre import Mipyre
from mipyre.testing import TestClient, TestServer

VAR = contextvars.ContextVar("reuse_var", default="unset")

app = Mipyre()


@app.get("/set")
def set_var(req):
    VAR.set("A")
    return "set"


@app.get("/get")
def get_var(req):
    return VAR.get()


@app.get("/main/set", runtime="main")
def set_var_on_main(req):
    VAR.set("A")
    return "set"


@app.get("/main/get", runtime="main")
def get_var_on_main(req):
    return VAR.get()


# What /schedule leaves holding its context, and the events /later lets it go by and waits on.
LEFT: dict = {}


@app.get("/schedule")
def schedule(req):
    # A task in this request's own context, writing into it after the request, once /later
    # lets it go.
    async def run():
        go, written = asyncio.Event(), asyncio.Event()

        async def write_when_let_go():
            await go.wait()
            VAR.set("leaked")
            written.set()

        ctx = asyncio.current_task().get_context()
        LEFT["go"], LEFT["written"] = go, written
        LEFT["task"] = asyncio.get_running_loop().create_task(write_when_let_go(), context=ctx)
        return "scheduled"

    return run()


@app.get("/later")
def later(req):
    # The task /schedule left writes into its context while this request runs.
    async def run():
        LEFT["go"].set()
        await LEFT["written"].wait()
        return VAR.get()

    return run()


@pytest.fixture(scope="module")
def conn():
    with TestServer(app) as server, TestClient(server) as client:
        c = http.client.HTTPConnection("127.0.0.1", client.port, timeout=10)
        yield c
        c.close()


def _get(conn, path):
    conn.request("GET", path)
    resp = conn.getresponse()
    return resp.status, resp.read().decode()


@pytest.mark.parametrize("prefix", ["", "/main"])
def test_a_variable_one_request_set_is_not_seen_by_the_next(conn, prefix):
    assert _get(conn, prefix + "/get") == (200, "unset")
    for _ in range(3):
        assert _get(conn, prefix + "/set") == (200, "set")
        assert _get(conn, prefix + "/get") == (200, "unset")


def test_a_callback_left_holding_a_request_context_does_not_write_into_the_next(conn):
    assert _get(conn, "/schedule") == (200, "scheduled")
    assert _get(conn, "/later") == (200, "unset")
    assert _get(conn, "/get") == (200, "unset")


def test_requests_after_requests_that_set_nothing_still_start_empty(conn):
    for _ in range(20):
        assert _get(conn, "/get") == (200, "unset")
    assert _get(conn, "/set") == (200, "set")
    for _ in range(5):
        assert _get(conn, "/get") == (200, "unset")


PROCESS_APP = """
import contextvars

from mipyre import Mipyre

VAR = contextvars.ContextVar("reuse_var", default="unset")
app = Mipyre()


@app.get("/set", runtime="process")
def set_var(req):
    VAR.set("A")
    return "set"


@app.get("/get", runtime="process")
def get_var(req):
    return VAR.get()
"""

PROCESS_SCRIPT = """
import urllib.request

from mipyre.testing import TestServer

import reuse_app

with TestServer(reuse_app.app) as s:
    for path in ("/get", "/set", "/get", "/set", "/get"):
        with urllib.request.urlopen(s.url + path, timeout=30) as r:
            print("seen", path, r.read().decode(), flush=True)
"""


def test_a_variable_one_process_lane_request_set_is_not_seen_by_the_next(tmp_path):
    # A lane worker and its template are process-wide state, so the server runs in its own process.
    (tmp_path / "reuse_app.py").write_text(textwrap.dedent(PROCESS_APP))
    (tmp_path / "reuse_script.py").write_text(textwrap.dedent(PROCESS_SCRIPT))
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(tmp_path), os.environ.get("PYTHONPATH")]))
    env["MIPYRE_PROCESS_WORKERS"] = "1"
    done = subprocess.run(
        [sys.executable, str(tmp_path / "reuse_script.py")],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120,
    )
    assert done.returncode == 0, done.stderr[-3000:]
    assert [line for line in done.stdout.splitlines() if line.startswith("seen ")] == [
        "seen /get unset",
        "seen /set set",
        "seen /get unset",
        "seen /set set",
        "seen /get unset",
    ], done.stderr[-3000:]
