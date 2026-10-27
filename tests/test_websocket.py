"""Tests for WebSocket support."""

import asyncio
import json
import queue
import subprocess
import sys
import os
import signal
import tempfile
import pytest

from mipyre import Mipyre
from mipyre.testing import TestClient, TestServer

from tests._helpers import answers, bound_port, read_file

PYTHON = sys.executable

WS_SERVER = '''
from mipyre import Mipyre, ServerFlags

app = Mipyre(runtime="main")

@app.websocket("/echo")
async def echo(ws):
    while True:
        msg = await ws.recv()
        if msg is None:
            break
        await ws.send(f"echo: {msg}")

@app.websocket("/json")
async def json_ws(ws):
    import json
    while True:
        msg = await ws.recv()
        if msg is None:
            break
        data = json.loads(msg)
        data["server"] = True
        await ws.send(json.dumps(data))

@app.get("/health")
def health(req):
    return {"ok": True}

if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0))
'''


@pytest.fixture(scope="module")
def ws_server(tmp_path_factory):
    """Start a WebSocket-capable server as a subprocess; yields the port it bound."""
    script = str(tmp_path_factory.mktemp("ws_server") / "app.py")
    with open(script, "w") as f:
        f.write(WS_SERVER)

    log_path = tempfile.mkstemp(prefix="mipyre_ws_test_", suffix=".log")[1]
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [PYTHON, script],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    port = bound_port(read_file(log_path), proc)
    answers(f"http://127.0.0.1:{port}/health", proc, read_file(log_path))

    yield port

    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (OSError, subprocess.TimeoutExpired):
            pass


@pytest.mark.asyncio
async def test_websocket_echo(ws_server):
    import websockets
    async with websockets.connect(f"ws://127.0.0.1:{ws_server}/echo") as ws:
        await ws.send("hello")
        resp = await asyncio.wait_for(ws.recv(), timeout=5)
        assert resp == "echo: hello"

        await ws.send("world")
        resp = await asyncio.wait_for(ws.recv(), timeout=5)
        assert resp == "echo: world"


@pytest.mark.asyncio
async def test_websocket_json(ws_server):
    import websockets
    async with websockets.connect(f"ws://127.0.0.1:{ws_server}/json") as ws:
        await ws.send(json.dumps({"key": "value"}))
        resp = await asyncio.wait_for(ws.recv(), timeout=5)
        data = json.loads(resp)
        assert data["key"] == "value"
        assert data["server"] is True


@pytest.mark.asyncio
async def test_websocket_multiple_messages(ws_server):
    import websockets
    async with websockets.connect(f"ws://127.0.0.1:{ws_server}/echo") as ws:
        for i in range(10):
            await ws.send(f"msg-{i}")
            resp = await asyncio.wait_for(ws.recv(), timeout=5)
            assert resp == f"echo: msg-{i}"


# How long any one event may take on a loaded machine; reaching it is the failure.
EVENT_TIMEOUT_S = 30
# More than the send buffer (one message of the limit), the client's queue and the sockets' buffers
# hold together, so the handler finds the buffer full before the client has read it all.
FILL_MESSAGE_BYTES = 65536
FILL_MESSAGES = 512


def _numbered(n: int) -> str:
    return f"{n:06d}".ljust(FILL_MESSAGE_BYTES, "x")


def _filling_app(first_full: queue.Queue, ended: queue.Queue) -> Mipyre:
    """An app whose ``/fill`` handler sends `FILL_MESSAGES` numbered messages, each send waiting
    for room when the buffer is full; it puts the number of the first message whose send had to
    wait on ``first_full``, and how many waited on ``ended``."""
    app = Mipyre(runtime="main")
    app.max_websocket_message_size = FILL_MESSAGE_BYTES

    @app.websocket("/fill")
    async def fill(ws):
        waits = 0
        for n in range(FILL_MESSAGES):
            sent = ws.send(_numbered(n))
            if not sent.done():
                if waits == 0:
                    first_full.put(n)
                waits += 1
            await sent
        ended.put(waits)

    return app


def test_every_message_reaches_a_client_that_reads_only_after_the_send_buffer_filled():
    first_full: queue.Queue = queue.Queue()
    ended: queue.Queue = queue.Queue()
    with TestServer(_filling_app(first_full, ended)) as server, TestClient(server) as client:
        with client.websocket_connect("/fill") as ws:
            first_full.get(timeout=EVENT_TIMEOUT_S)
            got = [ws.recv(timeout=EVENT_TIMEOUT_S) for _ in range(FILL_MESSAGES)]

    assert ended.get(timeout=EVENT_TIMEOUT_S) >= 1
    assert [m[:6] for m in got] == [f"{n:06d}" for n in range(FILL_MESSAGES)]
    assert got == [_numbered(n) for n in range(FILL_MESSAGES)]


def test_a_send_after_close_raises_connection_error():
    said: queue.Queue = queue.Queue()
    app = Mipyre(runtime="main")

    @app.websocket("/ws")
    async def closing(ws):
        await ws.send("open")
        ws.close()
        try:
            await ws.send("after close")
        except ConnectionError as e:
            said.put(str(e))

    with TestServer(app) as server, TestClient(server) as client:
        with client.websocket_connect("/ws") as ws:
            assert ws.recv(timeout=EVENT_TIMEOUT_S) == "open"
            assert said.get(timeout=EVENT_TIMEOUT_S) == "WebSocket closed"


def test_a_def_handler_is_refused_at_registration():
    app = Mipyre(runtime="main")

    def blocking(ws):
        pass

    with pytest.raises(TypeError, match="async def"):
        app.websocket("/ws", blocking)


def test_a_second_task_receiving_at_once_is_refused_and_a_cancelled_receive_frees_the_connection():
    said: queue.Queue = queue.Queue()
    app = Mipyre(runtime="main")

    @app.websocket("/ws")
    async def twice(ws):
        first = ws.recv()
        try:
            ws.recv()
        except RuntimeError as e:
            said.put(str(e))
        first.cancel()
        await ws.send(await ws.recv())

    with TestServer(app) as server, TestClient(server) as client:
        with client.websocket_connect("/ws") as ws:
            assert "another task" in said.get(timeout=EVENT_TIMEOUT_S)
            ws.send("hi")
            assert ws.recv(timeout=EVENT_TIMEOUT_S) == "hi"
