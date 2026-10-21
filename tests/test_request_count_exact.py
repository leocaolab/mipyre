"""With metrics on, the request counter counts every request exactly once (b/51 E6, design §4.5).

The count is taken where every answer goes out (`serve::pipeline::finish`), whatever answers it:
a route on a sub-interpreter worker, a route on the main interpreter, a pre-built response, a 405,
a 404, and a WebSocket upgrade. What an answer counts as is unit-tested beside the counters
(`telemetry::run_counters`, `serve::error_response::answer_of`); this is the one wiring test that
a real server's paths reach that point once each. Each path is counted on its own, so one path
that misses a count and another that counts twice cannot cancel out.
"""

import asyncio
import os

import pytest
from mipyre import Mipyre, get_gil_metrics
from mipyre.testing import TestClient

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
def ws_echo(ws):
    while True:
        msg = ws.recv()
        if msg is None:
            break
        ws.send(msg)


@pytest.fixture(scope="module")
def client():
    # The switch is read when the server starts: the run's counters count requests or not.
    before = os.environ.get("MIPYRE_METRICS")
    os.environ["MIPYRE_METRICS"] = "1"
    c = TestClient(app, transport="tcp")
    yield c
    c.close()
    if before is None:
        del os.environ["MIPYRE_METRICS"]
    else:
        os.environ["MIPYRE_METRICS"] = before


ANSWERS = [
    ("GET", "/worker", 200),
    ("GET", "/main", 200),
    ("GET", "/fast", 200),
    ("HEAD", "/fast", 200),
    ("POST", "/worker", 405),
    ("GET", "/missing", 404),
]


def total_requests() -> int:
    total = get_gil_metrics().total_requests
    assert total is not None, "metrics are off: MIPYRE_METRICS was not read at server start"
    return total


def ws_round_trip(port: int) -> None:
    async def run():
        async with websockets.connect(f"ws://127.0.0.1:{port}/ws") as ws:
            await ws.send("ping")
            assert await ws.recv() == "ping"

    asyncio.run(run())


def test_each_path_is_counted_exactly_once(client):
    for method, path, status in ANSWERS:
        before = total_requests()
        for _ in range(ROUNDS):
            assert client.request(method, path).status_code == status, (method, path)
        assert total_requests() - before == ROUNDS, (method, path)

    before = total_requests()
    for _ in range(ROUNDS):
        ws_round_trip(client.port)
    assert total_requests() - before == ROUNDS, "websocket"
