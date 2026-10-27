"""With metrics on, the request counter counts every request exactly once.

The count is taken where every answer goes out (`serve::pipeline::finish`), whatever answers it:
a route on a sub-interpreter worker, a route on the main interpreter, a pre-built response, a 405,
a 404, and a WebSocket upgrade. What an answer counts as is unit-tested beside the counters
(`serve::counters`, `serve::error_response::answer_of`); this is the one wiring test that
a real server's paths reach that point once each. Each path is counted on its own, so one path
that misses a count and another that counts twice cannot cancel out.
"""

import asyncio

import pytest
from mipyre import Mipyre
from mipyre.engine import ServerFlags
from mipyre.testing import TestClient, TestServer

websockets = pytest.importorskip("websockets")

ROUNDS = 25


# Module level: a sub-interpreter worker runs this file and serves the app it registers.
app = Mipyre()


@app.get("/worker")
def worker(req):
    return {"ok": True}


@app.get("/main", runtime="main")
def main(req):
    return {"ok": True}


app.add_fast_response("GET", "/fast", b"fast")


@app.websocket("/ws")
async def ws_echo(ws):
    while True:
        msg = await ws.recv()
        if msg is None:
            break
        await ws.send(msg)


@pytest.fixture(scope="module")
def client():
    # The switch is read when the server starts: the run's counters count requests or not.
    with TestServer(app, config=ServerFlags(metrics=True)) as server, TestClient(server) as c:
        yield c


ANSWERS = [
    ("GET", "/worker", 200),
    ("GET", "/main", 200),
    ("GET", "/fast", 200),
    ("HEAD", "/fast", 200),
    ("POST", "/worker", 405),
    ("GET", "/missing", 404),
]


def total_requests(client: TestClient) -> int:
    total = client.server.metrics().total_requests
    assert total is not None, "metrics are off: the metrics flag was not read at server start"
    return total


def ws_round_trip(port: int) -> None:
    async def run():
        async with websockets.connect(f"ws://127.0.0.1:{port}/ws") as ws:
            await ws.send("ping")
            assert await ws.recv() == "ping"

    asyncio.run(run())


def test_each_path_is_counted_exactly_once(client):
    # The server is this test's own: its count is every request sent to it, nothing else.
    sent = 0
    for method, path, status in ANSWERS:
        for _ in range(ROUNDS):
            assert client.request(method, path).status_code == status, (method, path)
        sent += ROUNDS
        assert total_requests(client) == sent, (method, path)

    for _ in range(ROUNDS):
        ws_round_trip(client.port)
    sent += ROUNDS
    assert total_requests(client) == sent, "websocket"
