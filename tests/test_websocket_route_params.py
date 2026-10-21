"""A WebSocket route is a route path like any other (b/51 E7): `{name}` parameters match and
reach the handler as `ws.request.params`; a path the router would read differently is refused
at registration."""

import asyncio
import os
import signal
import subprocess
import sys
import tempfile

import pytest

from tests._helpers import answers, bound_port, read_file

SERVER = '''
from mipyre import Mipyre

app = Mipyre(runtime="main")

@app.websocket("/room/{name}")
def room(ws):
    ws.send(ws.request.params["name"])
    ws.recv()

@app.get("/health")
def health(req):
    return {"ok": True}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0)
'''


@pytest.fixture(scope="module")
def server():
    fd, script = tempfile.mkstemp(prefix="mipyre_ws_params_", suffix=".py")
    with os.fdopen(fd, "w") as f:
        f.write(SERVER)
    log_path = tempfile.mkstemp(prefix="mipyre_ws_params_", suffix=".log")[1]
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, script], stdout=log, stderr=subprocess.STDOUT, start_new_session=True
        )
    port = bound_port(read_file(log_path), proc)
    answers(f"http://127.0.0.1:{port}/health", proc, read_file(log_path))
    yield port
    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    proc.wait(timeout=5)


@pytest.mark.asyncio
async def test_a_path_parameter_reaches_the_handler_decoded(server):
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{server}/room/a%20b") as ws:
        assert await asyncio.wait_for(ws.recv(), timeout=5) == "a b"


@pytest.mark.asyncio
async def test_a_path_no_websocket_route_matches_is_not_upgraded(server):
    import websockets

    with pytest.raises(websockets.exceptions.InvalidStatus):
        async with websockets.connect(f"ws://127.0.0.1:{server}/room"):
            pass


def test_a_colon_parameter_is_refused_naming_the_brace_form():
    from mipyre.engine import MipyreApp

    app = MipyreApp()
    with pytest.raises(ValueError, match=r"\{id\}"):
        app.websocket("/ws/:id", lambda ws: None)
