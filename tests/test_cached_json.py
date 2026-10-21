"""Tests for @cached_json decorator."""

from __future__ import annotations

import pytest

from mipyre import Mipyre, Response, cached_json
from mipyre.cache import _Entries, _Failure, _Failures
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
    with TestClient(app, port=None) as c:
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
    with TestClient(app, port=None) as c:
        r1 = c.get("/h")
        r2 = c.get("/h")

    assert r1.status_code == 202
    assert r2.status_code == 202
    # Each call re-ran the handler because Response objects bypass the cache.
    assert counter["n"] == 2


def test_string_return_cached_as_bytes():
    with TestClient(app, port=None) as c:
        r = c.get("/s")
    assert r.status_code == 200
    assert r.text == "hello world"


def test_bytes_return_cached_verbatim():
    with TestClient(app, port=None) as c:
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
    with TestClient(app, port=None) as c:
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


# b/422: a handler that fails on every request counts each failure and logs one line per kind
# a period, not one traceback per request.


def test_a_failure_kind_logs_once_a_period_and_says_how_many_it_held_back():
    failures = _Failures(period=60.0)
    assert failures.failed(_Failure.KEY_RAISED, now=100.0) == 0
    assert failures.failed(_Failure.KEY_RAISED, now=100.5) is None
    assert failures.failed(_Failure.KEY_RAISED, now=159.999) is None
    # Another kind has its own line.
    assert failures.failed(_Failure.NOT_SERIALIZABLE, now=101.0) == 0
    # The period is over: the line goes out, standing in for the two held back.
    assert failures.failed(_Failure.KEY_RAISED, now=160.0) == 2
    assert failures.failed(_Failure.KEY_RAISED, now=160.0) is None
    assert failures.count(_Failure.KEY_RAISED) == 5
    assert failures.count(_Failure.NOT_SERIALIZABLE) == 1


class _Req:
    path = "/r"


def test_a_handler_that_fails_on_every_request_logs_one_line_with_its_type(caplog):
    def key_that_raises(req):
        raise KeyError("no tenant")

    def report(req):
        return {"ok": True}

    def not_json(req):
        return {"at": object()}

    keyed = cached_json(ttl=10.0, key=key_that_raises)(report)
    unserializable = cached_json(ttl=10.0)(not_json)
    with caplog.at_level("ERROR", logger="mipyre.cache"):
        answers = [keyed(_Req()) for _ in range(5)]
        unserialized = [unserializable(_Req()) for _ in range(5)]
    # Every request is still served, uncached.
    assert [a.body for a in answers] == [b'{"ok":true}'] * 5
    assert all(set(r) == {"at"} for r in unserialized)
    records = [r for r in caplog.records if r.name == "mipyre.cache"]
    assert [(r.error_type, r.getMessage()) for r in records] == [
        (
            "KeyError",
            "cached_json test_a_handler_that_fails_on_every_request_logs_one_line_with_its_type.<locals>.report: "
            "key function raised; bypassing cache (0 more since the last such line, 1 in all)",
        ),
        (
            "TypeError",
            "cached_json test_a_handler_that_fails_on_every_request_logs_one_line_with_its_type.<locals>.not_json: "
            "could not serialize result; returning uncached (0 more since the last such line, 1 in all)",
        ),
    ]
    assert all(r.exc_info is not None for r in records), "logged without its traceback"
