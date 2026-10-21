"""b/305: TestClient's default transport, in-memory connections, and what it does not fake.

The memory transport serves through the server's accept loops and request pipeline with no
listening socket (a connection is a Unix socket pair, b/543); what only a TCP socket has (a peer
address, a port) is absent, not made up.
"""

import time

import pytest

import mipyre.engine
from mipyre import Mipyre, Response
from mipyre.testing import TestClient, Transport

# Module level: sub-interpreter workers rebuild it by executing this module.
app = Mipyre()


@app.get("/")
def index(req):
    return "ok"


@app.get("/who")
def who(req):
    return {"client_ip": req.client_ip, "host": req.headers.get("host")}


@app.get("/slow")
def slow(req):
    time.sleep(0.3)
    return "late"


@app.get("/stream")
def stream(req):
    return Response(body=b"x" * 1_000_000, content_type="application/octet-stream")


@app.websocket("/ws")
def ws(conn):
    conn.send(conn.recv())


@pytest.fixture(scope="module")
def memory():
    with TestClient(app) as c:
        yield c


def test_the_default_transport_is_memory_and_has_no_peer_address(memory):
    assert memory.transport is Transport.MEMORY
    assert memory.get("/who").json() == {"client_ip": None, "host": "testserver"}


def test_tcp_has_the_peer_address_memory_lacks():
    with TestClient(app, transport="tcp") as c:
        assert c.get("/who").json() == {
            "client_ip": "127.0.0.1",
            "host": f"127.0.0.1:{c.port}",
        }


def test_a_memory_client_has_no_port_and_takes_none(memory):
    with pytest.raises(AttributeError, match="no port"):
        memory.port
    assert memory.base_url == "http://testserver"
    with pytest.raises(ValueError, match="transport='tcp'"):
        TestClient(app, port=8123)


def test_an_unknown_transport_is_refused():
    with pytest.raises(ValueError):
        TestClient(app, transport="udp")


def test_a_large_response_streams_through_the_fixed_buffer_whole(memory):
    # 1 MB through a socket pair's buffers: the server's writes wait for the reader.
    assert memory.get("/stream").body == b"x" * 1_000_000


def test_the_client_timeout_is_the_same_error_in_both_transports(memory):
    with TestClient(app, transport="tcp") as tcp:
        for c in (memory, tcp):
            with pytest.raises(TimeoutError, match="timed out"):
                c.get("/slow", timeout=0.05)


def test_a_websocket_over_memory(memory):
    with memory.websocket_connect("/ws") as conn:
        conn.send("ping")
        assert conn.recv(timeout=5) == "ping"


def test_the_engine_refuses_socket_arguments_for_a_memory_server_by_name():
    engine = mipyre.engine.MipyreApp()
    with pytest.raises(ValueError, match="port, tls_cert"):
        engine.start(port=1, tls_cert="c.pem", mode="gil", transport="memory")


def test_a_memory_server_has_no_port_and_refuses_once_stopped():
    engine = mipyre.engine.MipyreApp()
    server = engine.start(mode="gil", transport="memory")
    assert server.port is None
    server.shutdown()
    server.serve()  # stopped before it served: returns at once, its inboxes dropped
    with pytest.raises(ConnectionRefusedError):
        server.connect()


def test_connect_on_a_tcp_server_says_to_use_its_port():
    engine = mipyre.engine.MipyreApp()
    server = engine.start(host="127.0.0.1", port=0, mode="gil")
    try:
        with pytest.raises(RuntimeError, match="connect to its port"):
            server.connect()
    finally:
        server.shutdown()
        server.serve()
