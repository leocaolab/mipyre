"""Tests for @cached_json decorator."""

from __future__ import annotations

import json

import pytest

from mipyre import Mipyre, Response, cached_json
from mipyre.cache import _Entries
from mipyre.testing import TestClient

# Module level: TestClient serves it through sub-interpreter workers, which rebuild
# it by executing this module. Tests whose handler bumps a test-local counter build
# their own app with Mipyre(runtime="main"), served on the main interpreter.
app = Mipyre()


@app.get("/s")
@cached_json(ttl=10.0)
def string_handler(req):
    return "hello world"


@app.get("/b")
@cached_json(ttl=10.0)
def bytes_handler(req):
    return b'{"raw":true}'


def test_first_call_runs_handler_and_caches():
    app = Mipyre(runtime="main")
    counter = {"n": 0}

    @app.get("/h")
    @cached_json(ttl=10.0)
    def handler(req):
        counter["n"] += 1
        return {"n": counter["n"]}

    # The handler's counter lives in the test: only the main interpreter shares it.
    with TestClient(app) as c:
        r1 = c.get("/h")
        r2 = c.get("/h")
        r3 = c.get("/h")

    assert r1.status_code == 200
    assert r1.json() == {"n": 1}
    # Cached hits must NOT re-run the handler.
    assert r2.json() == {"n": 1}
    assert r3.json() == {"n": 1}
    assert counter["n"] == 1


def test_an_entry_is_served_until_it_expires():
    entries = _Entries(ttl=1.0)
    entries.put("/h", b"body", now=10.0)
    assert entries.get("/h", now=10.0) == b"body"
    assert entries.get("/h", now=10.999) == b"body"
    assert entries.get("/h", now=11.0) is None
    assert entries.get("/other", now=10.0) is None


def test_an_entry_expires_from_its_store_time():
    # A handler slower than the TTL stores at the time it finished, not when it started.
    entries = _Entries(ttl=1.0)
    entries.put("/h", b"old", now=10.0)
    entries.put("/h", b"new", now=12.0)
    assert entries.get("/h", now=12.5) == b"new"


def test_a_new_key_past_the_limit_drops_every_entry():
    entries = _Entries(ttl=10.0, max_entries=2)
    entries.put("/a", b"a", now=0.0)
    entries.put("/b", b"b", now=0.0)
    entries.put("/b", b"b2", now=0.0)  # an existing key: no drop
    assert entries.get("/a", now=1.0) == b"a"
    entries.put("/c", b"c", now=0.0)
    assert (entries.get("/a", now=1.0), entries.get("/b", now=1.0), entries.get("/c", now=1.0)) == (None, None, b"c")


def test_response_object_passthrough_not_cached():
    """Explicit Response returns signal custom status/headers — don't cache."""
    app = Mipyre(runtime="main")
    counter = {"n": 0}

    @app.get("/h")
    @cached_json(ttl=10.0)
    def handler(req):
        counter["n"] += 1
        return Response(body=b'{"seq":' + str(counter["n"]).encode() + b'}',
                        status_code=202, content_type="application/json")

    # The handler's counter lives in the test: only the main interpreter shares it.
    with TestClient(app) as c:
        r1 = c.get("/h")
        r2 = c.get("/h")

    assert r1.status_code == 202
    assert r2.status_code == 202
    # Each call re-ran the handler because Response objects bypass the cache.
    assert counter["n"] == 2


def test_a_string_return_goes_out_as_the_engine_sends_it():
    with TestClient(app) as c:
        r = c.get("/s")
    assert r.status_code == 200
    assert r.text == "hello world"
    assert r.headers["content-type"].startswith("text/plain")


def test_a_bytes_return_goes_out_verbatim():
    with TestClient(app) as c:
        r = c.get("/b")
    assert r.json() == {"raw": True}


def test_custom_key_function():
    app = Mipyre(runtime="main")
    counter = {"n": 0}

    @app.get("/k")
    @cached_json(ttl=10.0, key=lambda req: req.path + "?" + (req.query or ""))
    def handler(req):
        counter["n"] += 1
        return {"n": counter["n"], "q": req.query}

    # The handler's counter lives in the test: only the main interpreter shares it.
    with TestClient(app) as c:
        r1 = c.get("/k?x=1")
        r2 = c.get("/k?x=1")  # same key → hit
        r3 = c.get("/k?x=2")  # different key → miss

    assert r1.json()["n"] == 1
    assert r2.json()["n"] == 1  # cached
    assert r3.json()["n"] == 2  # new miss
    assert counter["n"] == 2


def test_invalid_ttl_rejected():
    with pytest.raises(ValueError):
        cached_json(ttl=0)
    with pytest.raises(ValueError):
        cached_json(ttl=-1)


def test_the_default_key_does_not_serve_one_query_to_another():
    app = Mipyre(runtime="main")

    @app.get("/search")
    @cached_json(ttl=60.0)
    def search(req):
        return {"q": req.query_params.get("q")}

    with TestClient(app) as c:
        a = c.get("/search", params={"q": "a"}).json()
        b = c.get("/search", params={"q": "b"}).json()
    assert (a, b) == ({"q": "a"}, {"q": "b"})


@pytest.mark.parametrize("header", ["Authorization", "Cookie"])
def test_the_default_key_does_not_serve_one_user_to_another(header):
    app = Mipyre(runtime="main")

    @app.get("/me")
    @cached_json(ttl=60.0)
    def me(req):
        return {"user": req.headers.get(header)}

    with TestClient(app) as c:
        alice = c.get("/me", headers={header: "alice"}).json()
        bob = c.get("/me", headers={header: "bob"}).json()
    assert (alice, bob) == ({"user": "alice"}, {"user": "bob"})


def test_cached_json_does_not_label_plain_text_as_json():
    app = Mipyre(runtime="main")

    @app.get("/s")
    @cached_json(ttl=60.0)
    def s(req):
        return "hello"

    with TestClient(app) as c:
        r = c.get("/s")
    if r.headers["content-type"].startswith("application/json"):
        json.loads(r.body)


def test_a_key_of_none_serves_the_request_uncached():
    calls = []

    @cached_json(ttl=10.0, key=lambda req: None)
    def handler(req):
        calls.append(1)
        return {"n": len(calls)}

    assert [handler(_Req()) for _ in range(3)] == [{"n": 1}, {"n": 2}, {"n": 3}]


def test_unique_keys_never_hold_more_than_the_byte_limit():
    entries = _Entries(ttl=60.0, max_bytes=64 * 1024)
    for i in range(1_000):
        entries.put(f"/k{i}", b"x" * 1000, now=0.0)
        held = sum(len(k) + len(body) for k, (body, _) in entries._entries.items())
        assert held == entries._size <= 64 * 1024


def test_a_body_past_the_byte_limit_is_not_stored_and_drops_nothing():
    entries = _Entries(ttl=60.0, max_bytes=100)
    entries.put("/a", b"a" * 50, now=0.0)
    entries.put("/big", b"b" * 101, now=0.0)
    assert (entries.get("/a", now=1.0), entries.get("/big", now=1.0)) == (b"a" * 50, None)
    entries.put("/c", b"c" * 50, now=0.0)
    assert (entries.get("/a", now=1.0), entries.get("/c", now=1.0)) == (None, b"c" * 50)


class _Req:
    path = "/r"
    query = ""
    headers: dict[str, str] = {}


def test_what_the_key_function_raises_fails_the_request():
    def key_that_raises(req):
        raise KeyError("no tenant")

    keyed = cached_json(ttl=10.0, key=key_that_raises)(lambda req: {"ok": True})
    with pytest.raises(KeyError, match="no tenant"):
        keyed(_Req())


def test_a_result_the_engine_cannot_encode_fails_the_request():
    unserializable = cached_json(ttl=10.0)(lambda req: {"at": object()})
    with pytest.raises(TypeError):
        unserializable(_Req())
