"""A handler's response headers past what a header map takes room for are the handler's error, a
500 with the reason in the log, never a panic of the serving thread; up to the bound they
go out, the server's own lines (CORS, request id, server, content-type) still fitting."""

import http.client

import pytest

from mipyre import HTTPException, Mipyre, Request, Response, Stream
from mipyre.testing import TestClient

# `mipyre_http::plan::response::MAX_HANDLER_LINES`: http's header map takes room for 24,576 lines,
# 16 of them kept for the server's own.
MOST = 24_560


def named(n):
    return {f"x-{i}": "v" for i in range(n)}


# Module level: TestClient serves it through sub-interpreter workers, which rebuild it by
# executing this module.
app = Mipyre()
app.enable_cors(
    allow_origins=["https://a.example", "https://b.example"],
    expose_headers="x-0",
    allow_credentials=True,
)
app.enable_request_id_header()


@app.get("/ok")
def ok(req):
    return "ok"


@app.get("/lines/{n}")
def lines(req, n):
    return Response("hi", headers=named(int(n)))


@app.get("/one-name-listed/{n}")
def one_name_listed(req, n):
    return Response("hi", headers={"x-a": ["v"] * int(n)})


@app.get("/stream/{n}")
def stream(req, n):
    return Stream(headers=named(int(n)))


@app.get("/exception/{n}")
def exception(req, n):
    raise HTTPException(400, "no", headers=named(int(n)))


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


@pytest.mark.parametrize(
    "path",
    [
        f"/lines/{MOST + 1}",
        "/lines/24577",
        f"/one-name-listed/{MOST + 1}",
        f"/stream/{MOST + 1}",
        f"/exception/{MOST + 1}",
    ],
)
def test_headers_past_the_bound_are_a_500_and_the_server_keeps_serving(client, path):
    assert client.get(path).status_code == 500
    assert client.get("/ok").status_code == 200


def test_headers_at_the_bound_go_out_with_the_servers_own(client, monkeypatch):
    monkeypatch.setattr(http.client, "_MAXHEADERS", MOST + 100)
    r = client.get(f"/lines/{MOST}", headers={"Origin": "https://a.example"})
    assert r.status_code == 200
    assert r.headers["x-0"] == "v" and r.headers[f"x-{MOST - 1}"] == "v"
    assert r.headers["access-control-allow-origin"] == "https://a.example"
    assert "x-request-id" in r.headers


def test_the_error_names_the_bound():
    with pytest.raises(ValueError, match=rf"more than {MOST} lines \(from 24577 names\)"):
        Response("hi", headers=named(24_577))
    with pytest.raises(ValueError, match=rf"more than {MOST} lines \(from 1 names\)"):
        Response("hi", headers={"x-a": ["v"] * (MOST + 1)})
    with pytest.raises(ValueError, match=f"more than the {MOST} lines"):
        HTTPException(400, headers=named(MOST + 1))
    assert len(Response("hi", headers=named(MOST)).headers) == MOST
    with pytest.raises(ValueError, match="24577 headers, past the 24576 lines a request carries"):
        Request("GET", "/", {}, "", b"", named(24_577), "127.0.0.1")
