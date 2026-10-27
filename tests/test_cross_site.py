"""The engine's site check: a request's host must be one of
the server's names, which stops DNS rebinding, and a request that acts must come from this site
or one `enable_cors(allow_origins=...)` lists. Each test starts a real server on the serving
path it names and sends the raw headers a browser would."""

from __future__ import annotations

import json

import pytest

from mipyre import Mipyre
from mipyre.engine import ServerFlags
from mipyre.testing import TestClient, TestServer
from tests.test_request_path import raw_exchange, ws_request
from tests.test_dispatch_paths import RUN, serve

SITE_SCRIPT = """
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.post("/run")
def run(req):
    app.state.incr("runs", 1)
    return {"ran": True}

@app.get("/secret")
def secret(req):
    return {"secret": "s3cr3t"}

@app.post("/run-main", runtime="main")
def run_main(req):
    app.state.incr("runs", 1)
    return {"ran": True}

@app.websocket("/ws")
async def ws(ws):
    await ws.send("hello")
    ws.close()

@app.get("/runs")
def runs(req):
    return {"runs": app.state.get("runs") or 0}

from mipyre.mcp import MCPServer
mcp = MCPServer(app)

@mcp.tool()
def touch() -> str:
    app.state.incr("runs", 1)
    return "touched"
""" + RUN

TOOLS_CALL = json.dumps(
    {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "touch", "arguments": {}}}
).encode()


@pytest.fixture(scope="module", params=["gil", "tpc", "pool"])
def srv(request):
    with serve(SITE_SCRIPT, request.param) as s:
        yield s


def _runs(srv) -> int:
    return srv.get("/runs").json()["runs"]


def test_a_rebound_name_is_421_for_reads_and_writes_and_no_handler_runs(srv):
    before = _runs(srv)
    rebound = {
        "Host": f"evil.example:{srv.port}",
        "Origin": f"http://evil.example:{srv.port}",
        "Sec-Fetch-Site": "same-origin",
    }
    r = srv.post("/run", b'{"a": 1}', headers={**rebound, "Content-Type": "application/json"})
    assert r.status == 421, r.body
    assert f"evil.example:{srv.port}" in r.json()["detail"]
    r = srv.get("/secret", headers=rebound)
    assert r.status == 421, r.body
    assert b"s3cr3t" not in r.body
    r = srv.post("/mcp", TOOLS_CALL, headers={**rebound, "Content-Type": "application/json"})
    assert r.status == 421, r.body
    assert _runs(srv) == before


def test_a_client_that_is_no_browser_is_served_by_ip_or_localhost(srv):
    for host in (f"127.0.0.1:{srv.port}", f"localhost:{srv.port}"):
        r = srv.post("/run", b'{"a": 1}', headers={"Host": host, "Content-Type": "application/json"})
        assert r.status == 200, (host, r.body)
        assert r.json() == {"ran": True}


def test_an_undeclared_name_says_which_and_the_setting_that_admits_it():
    with serve(SITE_SCRIPT, "tpc") as s:
        r = s.get("/secret", headers={"Host": "api.example.com"})
        assert r.status == 421, r.body
        detail = r.json()["detail"]
        assert "api.example.com" in detail and "allowed_hosts" in detail, detail
    with serve(SITE_SCRIPT, "tpc", env={"MIPYRE_ALLOWED_HOSTS": "api.example.com"}) as s:
        r = s.get("/secret", headers={"Host": "api.example.com"})
        assert r.status == 200, r.body
        assert s.get("/secret", headers={"Host": "other.example"}).status == 421


def test_a_memory_server_checks_no_host_and_refuses_allowed_hosts():
    app = Mipyre(runtime="main")

    @app.get("/")
    def root(req):
        return "ok"

    with TestClient(app) as client:
        assert client.get("/").status_code == 200
    with pytest.raises(ValueError, match="allowed_hosts"):
        app._engine.start(config=ServerFlags(allowed_hosts=["api.example.com"]), transport="memory")


def test_allowed_hosts_that_are_no_names_are_refused_at_start():
    app = Mipyre(runtime="main")
    for bad in (["https://api.example.com"], ["api.example.com:8000"], ["*", "a.example"]):
        with pytest.raises(ValueError, match="allowed host"):
            app._engine.start(config=ServerFlags(host="127.0.0.1", port=0, allowed_hosts=bad))


CROSS_SITE = {"Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site", "Content-Type": "text/plain"}


@pytest.mark.parametrize("target", ["/run", "/run-main"])
def test_a_cross_site_form_post_is_403_on_every_path_and_no_handler_runs(srv, target):
    before = _runs(srv)
    for headers in (CROSS_SITE, {k: v for k, v in CROSS_SITE.items() if k != "Sec-Fetch-Site"}):
        r = srv.post(target, b'{"a":1}', headers=headers)
        assert r.status == 403, (headers, r.body)
        assert "https://evil.example" in r.json()["detail"]
    assert _runs(srv) == before


def test_a_cross_site_websocket_is_403_and_the_same_site_s_is_upgraded(srv):
    for sent in ({"Origin": "https://evil.example"}, {"Origin": "https://evil.example", "Sec_Fetch_Site": "cross-site"}):
        status, _, body = raw_exchange(srv.port, ws_request("/ws", **sent))
        assert status == 403, (sent, body)
        assert "https://evil.example" in json.loads(body)["detail"]
    here = f"127.0.0.1:{srv.port}"
    status, headers, _ = raw_exchange(srv.port, ws_request("/ws", Host=here, Origin=f"http://{here}"))
    assert status == 101, headers
    status, headers, _ = raw_exchange(srv.port, ws_request("/ws", Host=here))
    assert status == 101, headers


TRUSTED_SCRIPT = """
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.enable_cors(allow_origins=["https://app.test"], allow_credentials=True, allow_headers="content-type")

@app.post("/run")
def run(req):
    return {"ran": True}
""" + RUN


def test_the_front_end_named_once_reads_and_posts_across_sites():
    with serve(TRUSTED_SCRIPT, "tpc", env={"MIPYRE_ALLOWED_HOSTS": "api.test"}) as s:
        r = s.request("OPTIONS", "/run", headers={
            "Host": "api.test", "Origin": "https://app.test", "Access-Control-Request-Method": "POST"})
        assert r.status == 204, r.body
        assert r.headers["access-control-allow-origin"] == "https://app.test"
        r = s.post("/run", b'{"a":1}', headers={
            "Host": "api.test", "Origin": "https://app.test", "Sec-Fetch-Site": "cross-site",
            "Content-Type": "application/json"})
        assert r.status == 200, r.body
        assert r.headers["access-control-allow-origin"] == "https://app.test"
        r = s.post("/run", b'{"a":1}', headers={
            "Host": "api.test", "Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site"})
        assert r.status == 403, r.body


def test_a_process_lane_route_is_checked_before_it_is_sent_to_the_lane(tmp_path):
    from tests.test_process_lane import Server as LaneServer

    s = LaneServer(tmp_path, MIPYRE_PROCESS_WORKERS=1)
    try:
        status, _, body = s.get("/echo/x", data=b'{"a":1}', headers=CROSS_SITE)
        assert status == 403, body
        status, _, body = s.get("/echo/x", data=b'{"a":1}', headers={"Content-Type": "application/json"})
        assert status == 201, body
    finally:
        s.stop()


def _trusting_app() -> Mipyre:
    app = Mipyre(runtime="main")
    app.enable_cors(allow_origins=["https://app.test"], allow_credentials=True)

    @app.websocket("/ws")
    async def ws(ws):
        await ws.send("hello")
        ws.close()

    return app


@pytest.fixture(params=["memory", "tcp"])
def ws_client(request):
    app = _trusting_app()
    if request.param == "memory":
        with TestClient(app) as client:
            yield client
    else:
        with TestServer(app) as server, TestClient(server) as client:
            yield client


def test_a_websocket_opens_for_its_own_site_a_trusted_site_and_no_browser(ws_client):
    for origin in (ws_client.base_url, "https://app.test", None):
        with ws_client.websocket_connect("/ws", origin=origin) as ws:
            assert ws.recv(timeout=5) == "hello", origin


def test_a_websocket_from_another_site_is_refused_with_the_cross_site_403(ws_client):
    from websockets.exceptions import InvalidStatus

    for headers in ({}, {"Sec-Fetch-Site": "cross-site"}):
        with pytest.raises(InvalidStatus) as refused:
            ws_client.websocket_connect("/ws", origin="https://evil.example", additional_headers=headers)
        assert refused.value.response.status_code == 403, headers
        assert json.loads(refused.value.response.body) == {
            "detail": 'cross-site request from origin "https://evil.example" refused; '
            "list the origin in enable_cors(allow_origins=...)"
        }
