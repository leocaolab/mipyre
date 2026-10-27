"""Tests for `@app.post(..., body=BodyStream)` upload streaming.

Scope (matches v1 impl in src/python/body_stream.rs):
  * runtime="main" + sync handler only (registration should reject async / non-gil)
  * Iterator protocol: `for chunk in req.stream: ...` + `.read(n)`
  * max_body_size is still enforced for streaming routes
  * Non-stream routes' `req.stream` is None
"""

import json
import socket
import urllib.request
import urllib.error

import pytest

from mipyre import BodyStream, Mipyre
from mipyre.testing import TestClient, TestServer


# ---------------------------------------------------------------------------
# Registration validation
# ---------------------------------------------------------------------------

def test_stream_requires_gil():
    app = Mipyre()
    with pytest.raises(ValueError, match="runtime='main'"):
        @app.post("/x", body=BodyStream)  # runtime defaults to subinterp
        def handler(req):
            pass


def test_stream_rejects_async():
    app = Mipyre()
    with pytest.raises(ValueError, match="async def"):
        @app.post("/x", runtime="main", body=BodyStream)
        async def handler(req):
            pass


# ---------------------------------------------------------------------------
# Functional tests
# ---------------------------------------------------------------------------

# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module.
app = Mipyre()


@app.get("/")
def root(req):
    return {"ready": True}


@app.post("/upload", runtime="main", body=BodyStream)
def upload(req):
    total = 0
    count = 0
    for chunk in req.stream:
        assert isinstance(chunk, bytes)
        total += len(chunk)
        count += 1
    return {"bytes": total, "chunks": count}


@app.post("/read_all", runtime="main", body=BodyStream)
def read_all(req):
    data = req.stream.read()
    return {"len": len(data), "echo": data.decode("utf-8", errors="replace")}


@app.post("/buffered", runtime="main")
def buffered(req):
    # Non-stream route — req.stream must be None, req.body must work.
    return {
        "stream_is_none": req.stream is None,
        "body_len": len(req.body),
    }


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def test_upload_small_body(client):
    resp = client.post("/upload", body=b"hello world")
    data = resp.json()
    assert data["bytes"] == 11


def test_upload_large_body_does_not_oom(client):
    # 2 MB — under the default 2.5 MiB cap. We're mainly checking the
    # iterator delivers chunks and the total matches.
    payload = b"x" * (2 * 1024 * 1024)
    resp = client.post("/upload", body=payload)
    data = resp.json()
    assert data["bytes"] == 2 * 1024 * 1024
    assert data["chunks"] >= 1


def test_read_all_equivalent_to_body(client):
    resp = client.post("/read_all", body=b"streamed text")
    data = resp.json()
    assert data["len"] == 13
    assert data["echo"] == "streamed text"


def test_non_stream_route_has_no_stream(client):
    resp = client.post("/buffered", body=b"abc")
    data = resp.json()
    assert data["stream_is_none"] is True
    assert data["body_len"] == 3


def test_max_body_size_enforced_on_stream(client):
    # Temporarily shrink max_body_size; with IOError expected on overflow.
    # TestClient's app is the module-scoped one; we can reach into it via
    # the base_url to set the cap then restore.
    # Actually we want a separate app for this to avoid bleeding into other
    # tests. Build one here.
    # Its own module: a worker serves one app per module.
    from tests.apps.upload_small_cap import app

    with TestClient(app) as c:
        # 2 KB body > 1 KB cap → feeder sends ChunkMsg::Err
        try:
            resp = c.post("/up", body=b"y" * 2048)
            data = resp.json()
            assert "error" in data
            assert "max_body_size" in data["error"] or "exceed" in data["error"].lower()
        except urllib.error.HTTPError as e:
            # Acceptable: server may also return 413 via the initial-bounds
            # check path. Either signals the cap worked.
            assert e.code in (400, 413, 500)


def test_a_streamed_route_gets_its_first_chunk_before_the_rest_is_sent():
    """The handler has the first chunk while the client still holds the rest of the body: the
    server does not collect the body first (15549c56: on the sub-interpreter server a
    body=BodyStream route buffered it all; the byte-count test above passes either way). If the
    body were collected, the handler would not run until the rest is sent, and the wait below
    would run out."""
    from tests.apps.upload_first_chunk import app, first_chunk_seen

    first, rest = b"a" * 1000, b"b" * 3000
    with TestServer(app) as server:
        with socket.create_connection((server.host, server.port), timeout=10) as sock:
            sock.sendall(
                b"POST /first-then-rest HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n"
                b"Content-Length: %d\r\n\r\n" % (len(first) + len(rest)) + first
            )
            assert first_chunk_seen.wait(timeout=10), "the handler did not get the first chunk"
            sock.sendall(rest)
            reply = b""
            while chunk := sock.recv(65536):
                reply += chunk
    head, _, body = reply.partition(b"\r\n\r\n")
    assert head.startswith(b"HTTP/1.1 200"), reply[:300]
    got = json.loads(body)
    assert 0 < got["first"] <= len(first)
    assert got["total"] == len(first) + len(rest)
