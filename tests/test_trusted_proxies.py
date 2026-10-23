"""``trusted_proxies`` (docs/design/forwarded-client.md): only a peer the deployment lists as a
reverse proxy may name the client in ``X-Forwarded-For``. Each test serves the app on a real
socket, from 127.0.0.1, and sends the header a proxy, or a client forging one, would."""

from __future__ import annotations

import pytest

from mipyre import Mipyre
from mipyre.engine import ServerFlags, get_gil_metrics
from mipyre.testing import TestClient, TestServer

# Module level: sub-interpreter workers rebuild it by executing this module.
app = Mipyre()


@app.get("/who")
def who(req):
    return {"client_ip": req.client_ip}


@pytest.fixture(params=[True, False], ids=["tpc", "pool"])
def serve_with(request):
    """Serve ``app`` with ``trusted_proxies`` set to the given value (not given for ``None``),
    on the thread-per-core server and on the multi-thread server with its pool."""

    def serve(proxies: str | None):
        return TestServer(app, config=ServerFlags(tpc=request.param, trusted_proxies=proxies))

    return serve


def _client_ip(server: TestServer, forwarded_for: str) -> str:
    with TestClient(server) as c:
        return c.get("/who", headers={"X-Forwarded-For": forwarded_for}).json()["client_ip"]


def test_with_no_proxy_trusted_a_forged_header_is_ignored(serve_with):
    with serve_with(None) as server:
        assert _client_ip(server, "203.0.113.9") == "127.0.0.1"


def test_a_peer_that_is_no_trusted_proxy_cannot_name_a_client(serve_with):
    with serve_with("10.0.0.0/8") as server:
        assert _client_ip(server, "203.0.113.9") == "127.0.0.1"


def test_a_trusted_proxy_names_the_client_and_what_the_client_forged_is_not_read(serve_with):
    with serve_with("127.0.0.1") as server:
        assert _client_ip(server, "1.2.3.4, 203.0.113.9") == "203.0.113.9"


def test_trusted_hops_are_walked_past_from_the_right(serve_with):
    with serve_with("127.0.0.0/8, 10.0.0.0/8") as server:
        assert _client_ip(server, "203.0.113.9, 10.1.2.3") == "203.0.113.9"


def test_a_trusted_proxys_unreadable_header_is_ignored_and_counted_never_a_500(serve_with):
    with serve_with("127.0.0.1") as server:
        before = get_gil_metrics().forwarded_for_unreadable
        assert _client_ip(server, "203.0.113.9:4711") == "127.0.0.1"
        assert get_gil_metrics().forwarded_for_unreadable == before + 1


def test_an_entry_that_is_no_address_or_block_stops_the_start_naming_it():
    with pytest.raises(ValueError, match='trusted proxy "proxy.internal"'):
        app._engine.start(config=ServerFlags(host="127.0.0.1", port=0, trusted_proxies=["10.0.0.1", "proxy.internal"]))


def test_a_memory_server_refuses_trusted_proxies():
    with pytest.raises(ValueError, match="trusted_proxies"):
        app._engine.start(config=ServerFlags(trusted_proxies=["127.0.0.1"]), transport="memory")
