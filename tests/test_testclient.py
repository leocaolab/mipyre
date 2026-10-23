"""Tests for TestClient and HTTP verbs."""

import threading

import pytest
from mipyre import Mipyre, Response
from mipyre.testing import TestClient, TestServer


# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module.
app = Mipyre()


@app.get("/")
def index(req):
    return {"hello": "world"}


@app.get("/text")
def text(req):
    return "plain text"


@app.get("/user/{name}")
def user(req):
    return {"name": req.params["name"]}


@app.post("/echo")
def echo(req):
    return req.json()


@app.put("/put")
def put_handler(req):
    return {"method": "PUT", "data": req.json()}


@app.delete("/del/{id}")
def delete_handler(req):
    return {"deleted": req.params["id"]}


@app.get("/status")
def custom_status(req):
    return Response(body="created", status_code=201)


@app.get("/headers")
def custom_headers(req):
    return Response(body="ok", headers={"x-custom": "test"})


@pytest.fixture(scope="module")
def client():
    c = TestClient(app)
    yield c
    c.close()


def test_get_json(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json()["hello"] == "world"


def test_get_text(client):
    resp = client.get("/text")
    assert resp.status_code == 200
    assert resp.text == "plain text"


def test_path_params(client):
    resp = client.get("/user/alice")
    assert resp.json()["name"] == "alice"


def test_post_json(client):
    resp = client.post("/echo", body={"key": "value"})
    assert resp.status_code == 200
    assert resp.json()["key"] == "value"


def test_put(client):
    resp = client.put("/put", body={"x": 1})
    assert resp.json()["method"] == "PUT"
    assert resp.json()["data"]["x"] == 1


def test_delete(client):
    resp = client.delete("/del/42")
    assert resp.json()["deleted"] == "42"


def test_custom_status(client):
    resp = client.get("/status")
    assert resp.status_code == 201


def test_custom_headers(client):
    resp = client.get("/headers")
    # Header keys are case-insensitive
    assert any(k.lower() == "x-custom" for k in resp.headers)


def test_404(client):
    resp = client.get("/nonexistent")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# CORS and binary data regression tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def cors_client():
    # Its own module (a worker serves one app per module).
    from tests.apps.testclient_cors import app as cors_app

    c = TestClient(cors_app)
    yield c
    c.close()


def test_cors_on_normal_response(cors_client):
    resp = cors_client.get("/")
    assert resp.status_code == 200
    headers_lower = {k.lower(): v for k, v in resp.headers.items()}
    assert "access-control-allow-origin" in headers_lower


def test_cors_no_duplicates(cors_client):
    """CORS headers should not be duplicated (W3C spec violation)."""
    resp = cors_client.get("/")
    # Count occurrences of access-control-allow-origin
    cors_count = sum(
        1 for k in resp.headers if k.lower() == "access-control-allow-origin"
    )
    assert cors_count <= 1, f"Duplicate CORS headers: {cors_count}"


def test_binary_response_preserved(cors_client):
    """Binary data must not be corrupted by UTF-8 lossy conversion."""
    resp = cors_client.get("/binary")
    assert resp.status_code == 200
    assert resp.body[:2] == b"\xff\xd8", f"JPEG magic bytes corrupted: {resp.body[:4]}"


# ---------------------------------------------------------------------------
# b/845: a TestClient serves as the real server does (moved from the breaker pass)
# ---------------------------------------------------------------------------


def test_testclient_start_sends_no_request_to_the_apps_routes():
    """A TestClient that has made no request has run no handler: the app's `/` (a side
    effect, a counter, an audit row) must not see a request the test never made."""
    app = Mipyre(runtime="main")
    hits = []

    @app.get("/")
    def index(req):
        hits.append(req.path)
        return "index"

    with TestClient(app):
        pass
    assert hits == []


def test_testclient_raw_body_post_carries_no_content_type_it_was_not_given():
    """`post(body=b"...")` with no headers: the server must see what the test sent. urllib
    adds `application/x-www-form-urlencoded` to any request with data."""
    app = Mipyre(runtime="main")

    @app.post("/echo")
    def echo(req):
        return {"content_type": req.headers.get("content-type")}

    with TestClient(app) as memory, TestServer(app) as server, TestClient(server) as tcp:
        for c in (memory, tcp):
            assert c.post("/echo", body=b"raw bytes").json() == {"content_type": None}
            given = c.post("/echo", body=b"raw bytes", headers={"Content-Type": "text/csv"})
            assert given.json() == {"content_type": "text/csv"}


def test_testclient_whose_start_fails_leaves_no_server_running(monkeypatch):
    """When the server does not serve in time (here: a startup hook still waiting), the
    constructor raises, and it first stops the server it started: the caller never got the
    object, so nothing else could close it. The hook is let go when the client stops the
    run, so the run then ends by itself. (The ready time is injected small.)"""
    import mipyre.testing as testing
    from mipyre._server_run import ServerRun

    monkeypatch.setattr(testing, "_READY_TIMEOUT_S", 0.5)
    release = threading.Event()
    stop = ServerRun.stop

    def stop_and_release(run):
        stop(run)
        release.set()

    monkeypatch.setattr(ServerRun, "stop", stop_and_release)
    app = Mipyre(runtime="main")

    @app.on_startup
    def waits_for_a_dependency():
        release.wait()

    @app.get("/")
    def index(req):
        return "ok"

    before = {t.ident for t in threading.enumerate() if t.name == "mipyre-testserver"}
    with pytest.raises(TimeoutError, match="did not serve within"):
        TestClient(app)
    alive = [t for t in threading.enumerate() if t.name == "mipyre-testserver" and t.ident not in before]
    assert alive == [], "the failed TestClient left its server thread running"
