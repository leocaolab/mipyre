"""Tests for WebSocket support."""

import asyncio
import json
import subprocess
import sys
import os
import signal
import tempfile
import pytest

from tests._helpers import answers, bound_port, read_file

PYTHON = sys.executable

WS_SERVER = '''
from mipyre import Mipyre

app = Mipyre(runtime="main")

@app.websocket("/echo")
def echo(ws):
    while True:
        msg = ws.recv()
        if msg is None:
            break
        ws.send(f"echo: {msg}")

@app.websocket("/json")
def json_ws(ws):
    import json
    while True:
        msg = ws.recv()
        if msg is None:
            break
        data = json.loads(msg)
        data["server"] = True
        ws.send(json.dumps(data))

@app.get("/health")
def health(req):
    return {"ok": True}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0)
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
