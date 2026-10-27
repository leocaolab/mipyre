"""A client that hangs up while its request is being answered.

Whatever a client does with its connection, it cannot keep the server's work for it alive or
make the server log for it: a streamed response's producer is told the client is gone
(`send` raises `ConnectionError("client disconnected")`) and stops; a handler's answer that
nobody waits for any more is dropped at debug, and the worker serves the next request.

The decisions are unit-tested where they live (`exchange/stream.rs`, `runtime/worker_api.rs`
`send_answer`); this file runs each once on a real server over TCP. Every wait is on an
event (the producer's outcome, the handler's own connection to the test, a log line), never
on a fixed time.
"""

import os
import queue
import socket
import subprocess
import sys
import threading

from mipyre import Mipyre, Stream
from mipyre.testing import TestClient, TestServer

from tests._helpers import bound_port, kill_session, lines_with, read_file

# How long any one event may take on a loaded machine; reaching it is the failure.
EVENT_TIMEOUT_S = 30


def _read_until(sock: socket.socket, needle: bytes) -> bytes:
    got = b""
    while needle not in got:
        chunk = sock.recv(4096)
        assert chunk, f"EOF before {needle!r}; got {got!r}"
        got += chunk
    return got


def test_a_streamed_response_whose_client_hangs_up_tells_its_producer_and_stops():
    app = Mipyre(runtime="main")
    outcome: queue.Queue = queue.Queue()

    @app.get("/feed")
    def feed(req):
        stream = Stream(content_type="text/plain")

        def produce():
            sent = 0
            try:
                while True:
                    try:
                        stream.send(f"chunk {sent}\n")
                        sent += 1
                    except BlockingIOError:
                        stream.wait_writable()
            except ConnectionError as e:
                outcome.put((type(e), str(e), sent))

        threading.Thread(target=produce, name="feed-producer", daemon=True).start()
        return stream

    with TestServer(app) as server, TestClient(server) as client:
        sock = socket.create_connection(("127.0.0.1", client.port), timeout=EVENT_TIMEOUT_S)
        sock.sendall(b"GET /feed HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        _read_until(sock, b"chunk 1\n")
        sock.close()

        kind, text, sent = outcome.get(timeout=EVENT_TIMEOUT_S)
        assert kind is ConnectionError
        assert text == "client disconnected"
        assert sent >= 2

        # The server goes on serving.
        assert client.get("/feed-gone").status_code == 404


IN_FLIGHT_SCRIPT = r'''
import asyncio, os
from mipyre import Mipyre, ServerFlags

GATE = int(os.environ["MIPYRE_TEST_GATE_PORT"])
app = Mipyre()
app.enable_access_log(level="debug")

@app.get("/")
async def index(req):
    return {"ok": True}

@app.get("/held")
async def held(req):
    # Tells the test it is running, then waits for the test to let it answer.
    reader, writer = await asyncio.open_connection("127.0.0.1", GATE)
    await reader.readexactly(1)
    writer.close()
    return {"answered": "after the client left"}

if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0, subinterpreters=1))
'''


def test_a_request_whose_client_hangs_up_mid_handler_is_dropped_at_debug_and_the_worker_serves_on(
    tmp_path,
):
    gate = socket.create_server(("127.0.0.1", 0))
    gate.settimeout(EVENT_TIMEOUT_S)
    script = tmp_path / "in_flight_app.py"
    script.write_text(IN_FLIGHT_SCRIPT)
    log_path = str(script) + ".log"
    env = dict(os.environ, MIPYRE_TPC="0", MIPYRE_TEST_GATE_PORT=str(gate.getsockname()[1]))
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        )
    try:
        port = bound_port(read_file(log_path), proc)
        client = socket.create_connection(("127.0.0.1", port), timeout=EVENT_TIMEOUT_S)
        client.sendall(b"GET /held HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        handler, _ = gate.accept()  # the handler is running
        # The client hangs up its side. The server ends the connection while the handler
        # still runs (the client reads EOF: the connection, and the caller waiting on the
        # handler with it, are gone), and only then is the handler let go: its answer finds
        # nobody waiting.
        client.shutdown(socket.SHUT_WR)
        assert client.recv(4096) == b"", "the server answered a request whose client left"
        client.close()
        handler.sendall(b"!")
        handler.close()
        dropped = lines_with(read_file(log_path), "dropping the result", timeout=EVENT_TIMEOUT_S)
        assert dropped, read_file(log_path)()

        # The same (only) worker answers the next request.
        probe = socket.create_connection(("127.0.0.1", port), timeout=EVENT_TIMEOUT_S)
        probe.sendall(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        assert _read_until(probe, b"\r\n").startswith(b"HTTP/1.1 200"), read_file(log_path)()
        probe.close()

        log = read_file(log_path)()
        loud = [line for line in log.splitlines() if '"level":"ERROR"' in line or '"level":"WARN"' in line]
        assert loud == [], log
    finally:
        gate.close()
        kill_session(proc)
