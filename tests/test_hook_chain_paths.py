"""The before / after hook chain means the same on every path a request can take: a worker's
sync handler, the main interpreter, the async engine, and a process-lane worker.

What each case means (`tests/apps/hook_chain_paths.py`; ``b2`` and ``a2`` are ``async def``):

- the before hooks run in order; the first one to return a response answers the request, and
  neither the rest of the before hooks, the handler nor any after hook runs;
- a hook that raises fails the request with a 500;
- each after hook sees the response so far: ``None`` keeps it, a response replaces it;
- a ``ContextVar`` one step sets reaches every later step of the request, and none of the next.

One server, started as ``mipyre run`` because process routes need it, serves every case on
every path over one keep-alive connection, so one thread answers the requests in turn and a
write leaking into the next request shows in its trace."""

from __future__ import annotations

import http.client
import os
import subprocess
import sys
from dataclasses import dataclass

import pytest

from tests._helpers import bound_port, kill_session, read_file

APP = os.path.join(os.path.dirname(__file__), "apps", "hook_chain_paths.py")
PATHS = ("/subinterp", "/main", "/async", "/process")


@dataclass(frozen=True)
class Answer:
    status: int
    trace: str | None
    a2_saw: str | None


CASES = {
    "plain": Answer(201, "b1,b2,h,a1,a2", "b1,b2,h,a1"),
    "b1-answers": Answer(401, "b1", None),
    "b2-answers": Answer(403, "b1,b2", None),
    "b1-raises": Answer(500, None, None),
    "a1-keeps": Answer(200, "b1,b2,h,a1,a2", "b1,b2,h"),
    "a1-raises": Answer(500, None, None),
}


@pytest.fixture(scope="module")
def conn(tmp_path_factory):
    log_path = str(tmp_path_factory.mktemp("hook_chain") / "server.log")
    env = {**os.environ, "MIPYRE_PROCESS_WORKERS": "1"}
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "mipyre", "run", f"{APP}:app", "--port", "0"],
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True, env=env,
        )
    try:
        port = bound_port(read_file(log_path), proc)
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        yield c
        c.close()
    finally:
        kill_session(proc)


def _pid(conn: http.client.HTTPConnection, path: str) -> str | None:
    conn.request("GET", path, headers={"x-case": "a1-keeps"})
    resp = conn.getresponse()
    resp.read()
    return resp.getheader("x-pid")


def test_the_process_path_is_answered_by_another_process(conn):
    assert _pid(conn, "/process") not in (None, _pid(conn, "/main"))


def _ask(conn: http.client.HTTPConnection, path: str, case: str) -> Answer:
    conn.request("GET", path, headers={"x-case": case})
    resp = conn.getresponse()
    resp.read()
    return Answer(resp.status, resp.getheader("x-trace"), resp.getheader("x-a2-saw"))


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("case", CASES)
def test_hook_chain_means_the_same_on_every_path(conn, path, case):
    assert _ask(conn, path, case) == CASES[case]
