"""An `async def` hook on a sync path, then a `def` handler that runs a loop of its own (b/550).

The hook's coroutine runs on the thread's event loop, marked running only for that run
(`mipyre._awaitable.run`): the handler after it, in the same request and on the same thread,
may call `asyncio.run()`, which refuses to start while another loop runs. On a worker and on
the main interpreter (`runtime="main"`, a bridge thread), each over one keep-alive connection so
one thread serves every request in turn.
"""

import asyncio
import contextvars
import http.client

import pytest

from mipyre import Mipyre
from mipyre.testing import TestClient

SEEN = contextvars.ContextVar("b550_seen", default="unset")

app = Mipyre()


@app.before_request
async def hook(req):
    # Before any await: a real task on the running loop, writing into the request's context.
    SEEN.set(f"task={asyncio.current_task() is not None}")


async def answer():
    return 42


@app.get("/worker")
def on_a_worker(req):
    return f"{SEEN.get()} running={asyncio._get_running_loop() is not None} run={asyncio.run(answer())}"


@app.get("/main", runtime="main")
def on_the_main_interpreter(req):
    return f"{SEEN.get()} running={asyncio._get_running_loop() is not None} run={asyncio.run(answer())}"


@pytest.fixture(scope="module")
def conn():
    with TestClient(app, transport="tcp") as client:
        c = http.client.HTTPConnection("127.0.0.1", client.port, timeout=10)
        yield c
        c.close()


def _get(conn, path):
    conn.request("GET", path)
    resp = conn.getresponse()
    return resp.status, resp.read().decode()


@pytest.mark.parametrize("path", ["/worker", "/main"])
def test_a_def_handler_after_an_async_hook_can_run_a_loop_of_its_own(conn, path):
    for _ in range(3):
        assert _get(conn, path) == (200, "task=True running=False run=42")
