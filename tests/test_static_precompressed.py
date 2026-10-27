"""`app.static(..., precompressed=True)`: the `.br` / `.gz` twin the client accepts,
the original's Content-Type, and a cache that follows the disk."""

import gzip
import http.client
import os
import tempfile

import pytest

from mipyre.testing import TestClient, TestServer
from tests._helpers import poll_until

ARENA_ACCEPT = "br;q=1, gzip;q=0.8"
CSS = b"body { color: red; }\n" * 50


@pytest.fixture(scope="module")
def root():
    d = tempfile.mkdtemp(prefix="mipyre_precompressed_")
    with open(os.path.join(d, "site.css"), "wb") as f:
        f.write(CSS)
    with open(os.path.join(d, "site.css.br"), "wb") as f:
        f.write(b"BROTLI-BYTES")
    with open(os.path.join(d, "site.css.gz"), "wb") as f:
        f.write(gzip.compress(CSS))
    with open(os.path.join(d, "font.woff2"), "wb") as f:
        f.write(b"WOFF2")
    os.environ["MIPYRE_TEST_PRECOMPRESSED_DIR"] = d
    yield d
    del os.environ["MIPYRE_TEST_PRECOMPRESSED_DIR"]


@pytest.fixture(scope="module")
def client(root):
    from tests.apps.static_precompressed import app

    with TestServer(app) as server, TestClient(server) as c:
        yield c


def _get(port, path, accept=None, method="GET", extra=None):
    """Raw request: the body as sent, with no client-side decoding."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    headers = {"Accept-Encoding": accept} if accept is not None else {}
    headers.update(extra or {})
    try:
        conn.request(method, path, headers=headers)
        resp = conn.getresponse()
        return resp.status, {k.lower(): v for k, v in resp.getheaders()}, resp.read()
    finally:
        conn.close()


def test_brotli_preferred(client):
    status, headers, body = _get(client.port, "/static/site.css", ARENA_ACCEPT)
    assert status == 200
    assert headers["content-encoding"] == "br"
    assert headers["content-type"].startswith("text/css")
    assert headers["vary"] == "Accept-Encoding"
    assert body == b"BROTLI-BYTES"


def test_gzip_only(client):
    status, headers, body = _get(client.port, "/static/site.css", "gzip")
    assert status == 200
    assert headers["content-encoding"] == "gzip"
    assert gzip.decompress(body) == CSS


def test_no_accept_encoding_is_the_original(client):
    status, headers, body = _get(client.port, "/static/site.css")
    assert status == 200
    assert "content-encoding" not in headers
    assert body == CSS
    assert int(headers["content-length"]) == len(CSS)


def test_no_twin_is_the_original(client):
    status, headers, body = _get(client.port, "/static/font.woff2", ARENA_ACCEPT)
    assert status == 200
    assert "content-encoding" not in headers
    assert headers["content-type"] == "font/woff2"
    assert body == b"WOFF2"


def test_head_carries_the_twin_length(client):
    status, headers, body = _get(client.port, "/static/site.css", ARENA_ACCEPT, method="HEAD")
    assert status == 200
    assert headers["content-encoding"] == "br"
    assert int(headers["content-length"]) == len(b"BROTLI-BYTES")
    assert body == b""


def test_missing_is_404(client):
    status, _, _ = _get(client.port, "/static/nonexistent.txt", ARENA_ACCEPT)
    assert status == 404


def test_replaced_files_follow_the_disk(client, root):
    """Arena's staleness probe: original and twins replaced by same-length files (rename
    over), then put back; the replacement is served once the file watcher sees it. How soon is
    the OS watcher's, not a promise: the invalidation is `io/static_files/tests.rs`'s,
    and a watcher that never sees it is a hang pytest-timeout ends."""
    before = _get(client.port, "/static/site.css", ARENA_ACCEPT)[2]
    path = os.path.join(root, "site.css.br")
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(b"X" * len(before))
    os.replace(tmp, path)
    poll_until(
        lambda: _get(client.port, "/static/site.css", ARENA_ACCEPT)[2] != before,
        timeout=None, what="the replaced .br served",
    )
    assert _get(client.port, "/static/site.css", ARENA_ACCEPT)[2] == b"X" * len(before)


@pytest.mark.parametrize("method", ["GET", "HEAD"])
def test_a_client_revalidating_the_twin_it_has_gets_304_without_a_length(client, method):
    """The twin's own ETag answers 304, with the validators and Vary and no body or
    Content-Length (RFC 9110 §8.6: an empty body's 0 is not the representation's length)."""
    status, headers, twin = _get(client.port, "/static/site.css", ARENA_ACCEPT)
    assert status == 200
    etag = headers["etag"]
    status, again, body = _get(client.port, "/static/site.css", ARENA_ACCEPT, method, {"If-None-Match": etag})
    assert status == 304
    assert again["etag"] == etag
    assert again["last-modified"] == headers["last-modified"]
    assert again["vary"] == "Accept-Encoding"
    assert "content-length" not in again
    assert body == b""
    # The original's copy is not the twin's: a brotli client is sent the twin.
    status, plain, _ = _get(client.port, "/static/site.css")
    status, _, body = _get(client.port, "/static/site.css", ARENA_ACCEPT, method, {"If-None-Match": plain["etag"]})
    assert status == 200
    assert body == (twin if method == "GET" else b"")
