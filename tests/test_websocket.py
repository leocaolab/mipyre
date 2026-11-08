"""A WebSocket route's handlers are `def`s called once per message, on the worker of the thread
that holds the connection or, with `runtime="main"`, on main; what they send and return goes
out once the call returns."""

import asyncio
import json
import os
import queue
import signal
import subprocess
import sys
import tempfile

import pytest

from mipyre import Mipyre
from mipyre.testing import TestClient, TestServer

from tests._helpers import answers, bound_port, read_file

PYTHON = sys.executable

WS_SERVER = '''
import json
from mipyre import Mipyre, Response, ServerFlags, publish

app = Mipyre()

@app.before_request
def guard(req):
    if req.path == "/refused":
        return Response("refused", status_code=403)

def greet(ws):
    ws.data = []
    ws.send("sent on connect")
    return "hello"

def closed(ws, code, reason):
    publish("closes", json.dumps([code, reason, ws.data]))

@app.websocket("/echo", on_connect=greet, on_close=closed)
def echo(ws, msg):
    ws.data.append(msg if isinstance(msg, str) else msg.hex())
    ws.send("before")
    return msg

@app.websocket("/json")
def json_ws(ws, msg):
    data = json.loads(msg)
    data["server"] = True
    return json.dumps(data)

@app.websocket("/main", runtime="main", on_connect=greet, on_close=closed)
def on_main(ws, msg):
    ws.data.append(msg if isinstance(msg, str) else msg.hex())
    ws.send("before")
    return msg

@app.websocket("/refused")
def refused(ws, msg):
    return msg

@app.websocket("/raises")
def raises(ws, msg):
    ws.send("before")
    raise RuntimeError("boom")

@app.websocket("/closes")
def closes(ws, msg):
    ws.send("bye")
    ws.close(4001, "done")
    return "after close" if ws.send("late") else "send refused"

@app.websocket("/silent")
def silent(ws, msg):
    return None

@app.websocket("/misuse")
def misuse(ws, msg):
    try:
        ws.close(1006)
    except ValueError as e:
        ws.send("close: " + str(e))
    try:
        ws.send(1)
    except TypeError as e:
        ws.send("send: " + str(e))

@app.websocket("/room")
def room(ws, msg):
    topic, _, text = msg.partition(":")
    if text == "join":
        ws.subscribe(topic)
        return "joined " + topic
    if text == "leave":
        ws.unsubscribe(topic)
        return "left " + topic
    ws.publish(topic, text)
    return "published"

@app.post("/announce/{topic}")
def announce(req):
    publish(req.params["topic"], req.text())
    return "ok"

@app.get("/health")
def health(req):
    return {"ok": True}

if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0))
'''

EVENT_TIMEOUT_S = 30


def _start(script_text: str, env: dict | None = None) -> tuple[subprocess.Popen, str]:
    fd, script = tempfile.mkstemp(prefix="mipyre_ws_", suffix=".py")
    with os.fdopen(fd, "w") as f:
        f.write(script_text)
    log_path = tempfile.mkstemp(prefix="mipyre_ws_test_", suffix=".log")[1]
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [PYTHON, script],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env={**os.environ, **(env or {})},
        )
    return proc, log_path


def _stop(proc: subprocess.Popen) -> None:
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (OSError, subprocess.TimeoutExpired):
            pass


@pytest.fixture(scope="module")
def ws_server():
    """Start a WebSocket-capable server as a subprocess; yields the port it bound."""
    proc, log_path = _start(WS_SERVER)
    port = bound_port(read_file(log_path), proc)
    answers(f"http://127.0.0.1:{port}/health", proc, read_file(log_path))
    yield port
    _stop(proc)


async def _recv(ws):
    return await asyncio.wait_for(ws.recv(), timeout=EVENT_TIMEOUT_S)


async def _closes(port: int):
    """Returns a connection subscribed to the `closes` topic, which `on_close` publishes to."""
    import websockets

    watcher = await websockets.connect(f"ws://127.0.0.1:{port}/room")
    await watcher.send("closes:join")
    assert await _recv(watcher) == "joined closes"
    return watcher


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/echo", "/main"])
async def test_each_message_is_answered_with_the_sends_then_the_return_value(ws_server, path):
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{ws_server}{path}") as ws:
        assert await _recv(ws) == "sent on connect"
        assert await _recv(ws) == "hello"
        for i in range(10):
            await ws.send(f"msg-{i}")
            assert await _recv(ws) == "before"
            assert await _recv(ws) == f"msg-{i}"
        await ws.send(b"\x00\x01")
        assert await _recv(ws) == "before"
        assert await _recv(ws) == b"\x00\x01"


@pytest.mark.asyncio
async def test_a_json_message_round_trips(ws_server):
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{ws_server}/json") as ws:
        await ws.send(json.dumps({"key": "value"}))
        data = json.loads(await _recv(ws))
        assert data == {"key": "value", "server": True}


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/echo", "/main"])
async def test_on_close_is_told_the_clients_code_once_and_sees_the_connections_data(ws_server, path):
    import websockets

    watcher = await _closes(ws_server)
    async with websockets.connect(f"ws://127.0.0.1:{ws_server}{path}") as ws:
        await _recv(ws)
        await _recv(ws)
        await ws.send("a")
        await _recv(ws)
        await _recv(ws)
        await ws.close(4000, "bye")
    assert json.loads(await _recv(watcher)) == [4000, "bye", ["a"]]
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(watcher.recv(), timeout=0.5)
    await watcher.close()


@pytest.mark.asyncio
async def test_a_hook_that_answers_refuses_the_upgrade_with_its_response(ws_server):
    import websockets

    with pytest.raises(websockets.exceptions.InvalidStatus) as refused:
        async with websockets.connect(f"ws://127.0.0.1:{ws_server}/refused"):
            pass
    assert refused.value.response.status_code == 403


@pytest.mark.asyncio
async def test_a_handler_that_raises_closes_with_1011_after_what_it_sent(ws_server):
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{ws_server}/raises") as ws:
        await ws.send("x")
        assert await _recv(ws) == "before"
        with pytest.raises(websockets.exceptions.ConnectionClosedError) as closed:
            await _recv(ws)
    assert closed.value.rcvd.code == 1011


@pytest.mark.asyncio
async def test_close_sends_what_came_before_then_the_code_and_later_sends_are_refused(ws_server):
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{ws_server}/closes") as ws:
        await ws.send("x")
        assert await _recv(ws) == "bye"
        with pytest.raises(websockets.exceptions.ConnectionClosed) as closed:
            await _recv(ws)
    assert (closed.value.rcvd.code, closed.value.rcvd.reason) == (4001, "done")


@pytest.mark.asyncio
async def test_a_return_value_of_none_sends_nothing(ws_server):
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{ws_server}/silent") as ws:
        await ws.send("x")
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(ws.recv(), timeout=0.5)


@pytest.mark.asyncio
async def test_a_close_code_an_endpoint_may_not_send_and_a_message_of_another_type_raise(ws_server):
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{ws_server}/misuse") as ws:
        await ws.send("x")
        assert (await _recv(ws)).startswith("close: 1006 is not a close code")
        assert (await _recv(ws)).startswith("send: ")


@pytest.mark.asyncio
async def test_a_publish_reaches_every_subscriber_but_the_sender_until_it_leaves(ws_server):
    import websockets

    url = f"ws://127.0.0.1:{ws_server}/room"
    async with websockets.connect(url) as a, websockets.connect(url) as b:
        for ws in (a, b):
            await ws.send("t:join")
            assert await _recv(ws) == "joined t"
        await a.send("t:hi")
        assert await _recv(a) == "published"
        assert await _recv(b) == "hi"
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(a.recv(), timeout=0.3)

        await b.send("t:leave")
        assert await _recv(b) == "left t"
        await a.send("t:again")
        assert await _recv(a) == "published"
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(b.recv(), timeout=0.3)


@pytest.mark.asyncio
async def test_mipyre_publish_from_an_http_route_reaches_the_subscribers(ws_server):
    import urllib.request

    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{ws_server}/room") as ws:
        await ws.send("news:join")
        assert await _recv(ws) == "joined news"
        req = urllib.request.Request(f"http://127.0.0.1:{ws_server}/announce/news", data=b"extra", method="POST")
        await asyncio.to_thread(lambda: urllib.request.urlopen(req, timeout=EVENT_TIMEOUT_S).read())
        assert await _recv(ws) == "extra"


# One message of the limit is what a connection may hold unsent.
FILL_MESSAGE_BYTES = 65536


def test_a_send_that_does_not_fit_returns_false_and_closes_with_1008():
    said: queue.Queue = queue.Queue()
    app = Mipyre(runtime="main")
    app.max_websocket_message_size = FILL_MESSAGE_BYTES

    @app.websocket("/fill")
    def fill(ws, msg):
        said.put([ws.send("x" * (FILL_MESSAGE_BYTES // 2)) for _ in range(3)])

    from websockets.exceptions import ConnectionClosed

    with TestServer(app) as server, TestClient(server) as client:
        with client.websocket_connect("/fill") as ws:
            ws.send("go")
            assert said.get(timeout=EVENT_TIMEOUT_S) == [True, False, False]
            with pytest.raises(ConnectionClosed) as closed:
                ws.recv(timeout=EVENT_TIMEOUT_S)
    assert closed.value.rcvd.code == 1008


@pytest.mark.parametrize("which", ["on_message", "on_connect", "on_close"])
def test_an_async_def_handler_is_refused_at_registration(which):
    app = Mipyre(runtime="main")

    async def handler(*args):
        pass

    def plain(*args):
        pass

    handlers = {"on_message": plain, "on_connect": None, "on_close": None, which: handler}
    with pytest.raises(TypeError, match="async def"):
        app.websocket(
            "/ws", handlers["on_message"], on_connect=handlers["on_connect"], on_close=handlers["on_close"]
        )


WORKER_ROUTE_SERVER = '''
from mipyre import Mipyre, ServerFlags

app = Mipyre()

@app.websocket("/ws")
def echo(ws, msg):
    return msg

if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0))
'''


def test_a_worker_websocket_route_without_the_tpc_server_is_refused_at_startup():
    proc, log_path = _start(WORKER_ROUTE_SERVER, {"MIPYRE_TPC": "0"})
    try:
        proc.wait(timeout=60)
    finally:
        _stop(proc)
    output = read_file(log_path)()
    assert proc.returncode != 0, output
    assert "websocket /ws runs on a sub-interpreter worker" in output, output
    assert "runtime='main'" in output, output
