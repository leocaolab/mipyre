"""Tests for RPC system — server-side @rpc(app) + RPCClient."""

import json
import pytest
from mipyre import Mipyre, rpc
from mipyre.testing import TestClient, TestServer


# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module.
app = Mipyre()


@app.get("/")
def health(req):
    return {"ok": True}


@rpc(app)
def add(data):
    return {"sum": data["a"] + data["b"]}


@rpc(app)
def echo(req, data):
    """Handler that takes (req, data) — 2-arg form."""
    return {"echoed": data, "method": req.method}


@rpc(app, "error")
def fail(data):
    raise ValueError("intentional error")


@pytest.fixture(scope="module")
def client():
    c = TestClient(app)
    yield c
    c.close()


def test_rpc_json_basic(client):
    """JSON RPC: send dict, get the result back as the body."""
    resp = client.post(
        "/rpc/add",
        body=json.dumps({"a": 3, "b": 5}).encode(),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["sum"] == 8


def test_rpc_two_arg_handler(client):
    """Handler that takes (req, data)."""
    resp = client.post(
        "/rpc/echo",
        body=json.dumps({"hello": "world"}).encode(),
        headers={"Content-Type": "application/json"},
    )
    data = resp.json()
    assert data["echoed"]["hello"] == "world"
    assert data["method"] == "POST"


def test_rpc_error_envelope(client):
    """Handler that raises → the engine's generic 500 with the request id; the
    exception text goes to the log, not to the client (D4)."""
    resp = client.post(
        "/rpc/error",
        body=b"{}",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 500
    data = resp.json()
    rid = data.get("request_id")
    assert isinstance(rid, str) and rid, data
    assert data == {"detail": "Internal Server Error", "request_id": rid}


def test_rpc_empty_body(client):
    """An empty body is no JSON value: a 400 that says why."""
    resp = client.post(
        "/rpc/add",
        body=b"",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]


def test_rpc_msgpack(client):
    """MsgPack encoding if available."""
    try:
        import msgpack
    except ImportError:
        pytest.skip("msgpack not installed")

    payload = msgpack.packb({"a": 10, "b": 20}, use_bin_type=True)
    resp = client.post(
        "/rpc/add",
        body=payload,
        headers={
            "Content-Type": "application/msgpack",
            "Accept": "application/msgpack",
        },
    )
    assert resp.status_code == 200
    data = msgpack.unpackb(resp.body, raw=False)
    assert data["sum"] == 30


def test_rpc_400_for_a_body_too_deep_says_why():
    """msgpack's depth limit raises `StackError` with no message; the 400 still says why
    (global rule 1: carry what happened, never an empty stand-in)."""
    app = Mipyre(runtime="main")

    @rpc(app)
    def echo(data):
        return data

    with TestClient(app) as c:
        r = c.post(
            "/rpc/echo", body=b"\x91" * 100_000 + b"\x00",
            headers={"Content-Type": "application/msgpack", "Accept": "application/json"},
        )
    assert r.status_code == 400
    assert r.json()["detail"], r.text


# --- which form a handler is: by the positional parameters it requires ---------------


def _data(data): ...
def _data_with_option(data, limit=10): ...
def _data_then_extra(data, *rest): ...
def _req_data(req, data): ...
def _req_data_with_option(req, data, limit=10): ...
def _all_optional(data=None): ...
def _only_varargs(*args): ...
def _three_required(req, data, extra): ...
def _keyword_only_required(data, *, extra): ...
def _none(): ...


@pytest.mark.parametrize("fn,takes_request", [
    (_data, False),
    (_data_with_option, False),
    (_data_then_extra, False),
    (_req_data, True),
    (_req_data_with_option, True),
])
def test_takes_request_counts_the_required_positional_parameters(fn, takes_request):
    from mipyre._rpc import _takes_request

    assert _takes_request(fn) is takes_request


@pytest.mark.parametrize("fn", [_all_optional, _only_varargs, _three_required, _keyword_only_required, _none])
def test_takes_request_refuses_a_handler_it_cannot_call_as_either_form(fn):
    from mipyre._rpc import _takes_request

    with pytest.raises(TypeError, match=fn.__name__):
        _takes_request(fn)


def test_a_handler_with_an_option_after_data_is_handed_the_data():
    app = Mipyre(runtime="main")

    @rpc(app)
    def search(data, limit=10):
        return {"q": data["q"], "limit": limit}

    with TestClient(app) as c:
        r = c.post("/rpc/search", body=b'{"q": "x"}', headers={"Content-Type": "application/json"})
    assert r.status_code == 200, r.text
    assert r.json() == {"q": "x", "limit": 10}


# --- an empty body is decoded by its codec --------------------------------------------


class _Empty:
    """A Protobuf message stand-in: `parse` as betterproto's, where zero bytes are the
    message with every field at its default."""

    def parse(self, body: bytes):
        self.size = len(body)
        return self


def test_an_empty_protobuf_body_is_the_default_message():
    app = Mipyre(runtime="main")

    @rpc(app, proto_model=_Empty)
    def size(msg):
        return {"type": type(msg).__name__, "size": msg.size}

    with TestClient(app) as c:
        r = c.post("/rpc/size", body=b"", headers={"Content-Type": "application/protobuf"})
    assert r.status_code == 200, r.text
    assert r.json() == {"type": "_Empty", "size": 0}


def test_an_empty_json_body_is_400_with_the_reason():
    app = Mipyre(runtime="main")

    @rpc(app)
    def echo(data):
        return data

    with TestClient(app) as c:
        r = c.post("/rpc/echo", body=b"", headers={"Content-Type": "application/json"})
    assert r.status_code == 400
    assert "JSON" in r.json()["detail"], r.text


# --- the method's path: one rule for server and client --------------------------------


def test_the_client_calls_the_method_rpc_registered_by_name():
    from mipyre import RPCClient

    app = Mipyre(runtime="main")

    @rpc(app)
    def get_data(data):
        return {"got": data}

    with TestServer(app) as server, TestClient(server) as c, RPCClient(c.base_url, use_msgpack=False) as client:
        assert client.get_data(a=1) == {"got": {"a": 1}}


@pytest.mark.parametrize("name", ["/rpc/get_data", "get-data", "", "_private", "a/b"])
def test_rpc_refuses_a_name_the_client_cannot_call(name):
    app = Mipyre(runtime="main")

    with pytest.raises(ValueError, match="identifier"):
        rpc(app, name)


# --- FastAPI's shapes: the result is the body, a failure its status and detail (b/845) ---


def test_a_str_result_is_a_json_string_not_text():
    app = Mipyre(runtime="main")

    @rpc(app)
    def name(data):
        return "x"

    with TestClient(app) as c:
        r = c.post("/rpc/name", body=b"{}", headers={"Content-Type": "application/json"})
    assert (r.status_code, r.headers["content-type"], r.body) == (200, "application/json", b'"x"')


def test_the_client_raises_each_failure_as_its_kind_with_its_facts():
    from mipyre import HTTPException, RPCError, RPCRequestError, RPCServerError
    from mipyre import RPCClient

    app = Mipyre(runtime="main")

    @rpc(app)
    def add(data):
        return {"sum": data["a"] + data["b"]}

    @rpc(app)
    def refuse(data):
        raise HTTPException(409, {"held_by": "someone"})

    @rpc(app)
    def boom(data):
        raise RuntimeError("rpc-secret 3c1")

    with TestServer(app) as server, TestClient(server) as c, RPCClient(c.base_url, use_msgpack=False) as client:
        assert client.add(a=1, b=2) == {"sum": 3}
        with pytest.raises(RPCRequestError) as refused:
            client.refuse()
        assert (refused.value.status, refused.value.detail) == (409, {"held_by": "someone"})
        assert (refused.value.method, refused.value.url) == ("refuse", c.base_url + "/rpc/refuse")
        with pytest.raises(RPCServerError) as failed:
            client.boom()
        assert failed.value.status == 500 and failed.value.detail == "Internal Server Error"
        assert isinstance(failed.value.request_id, str) and failed.value.request_id
        assert "rpc-secret" not in str(failed.value)
        assert isinstance(failed.value, RPCError) and not isinstance(failed.value, RuntimeError)


def test_the_client_says_the_first_kib_of_a_failure_that_is_not_json():
    from mipyre import RPCRequestError
    from mipyre.engine import Response
    from mipyre import RPCClient

    app = Mipyre(runtime="main")

    @app.post("/rpc/plain")
    def plain(req):
        return Response(body="x" * 5000, status_code=418, content_type="text/plain")

    with TestServer(app) as server, TestClient(server) as c, RPCClient(c.base_url, use_msgpack=False) as client:
        with pytest.raises(RPCRequestError) as raised:
            client.plain()
    assert raised.value.detail == "x" * 1024


@pytest.mark.parametrize("result,body", [((1, 2), b"[1,2]"), (None, b"null"), (3, b"3"), ([1], b"[1]")])
def test_any_result_is_its_json(result, body):
    app = Mipyre(runtime="main")

    @rpc(app)
    def value(data):
        return result

    with TestClient(app) as c:
        r = c.post("/rpc/value", body=b"{}", headers={"Content-Type": "application/json"})
    assert (r.status_code, r.headers["content-type"], r.body) == (200, "application/json", body)
