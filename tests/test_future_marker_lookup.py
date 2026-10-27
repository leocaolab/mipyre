"""Asking an awaitable whether it is an asyncio `Future` reads its `_asyncio_future_blocking`
marker. A lookup that raises anything but `AttributeError` is the request's failure (500, the
error logged), not a "no, not a Future" (7354f96f: it was swallowed, and the object run as an
ordinary awaitable)."""

import json

import pytest

from mipyre import Mipyre, flush_logs
from mipyre.testing import TestClient


class _MarkerRaises:
    """Awaitable, with a `Future` marker whose lookup raises `ValueError`."""

    def __await__(self):
        return iter(())

    @property
    def _asyncio_future_blocking(self):
        raise ValueError("marker lookup broke")


class _NoMarker:
    """Awaitable, with no `Future` marker (`AttributeError`): run as an ordinary awaitable."""

    def __await__(self):
        if False:
            yield
        return {"ran": True}


# Module level: the sub-interpreter workers rebuild the app by executing this module.
app = Mipyre()


@app.get("/marker-raises")
def marker_raises(req):
    return _MarkerRaises()


@app.get("/no-marker")
def no_marker(req):
    return _NoMarker()


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def _logged_error(capfd, rid: str) -> str:
    """The `error` field of the log line carrying `rid`. The server runs in this process and logs
    the failure before it answers; its log writer is a background thread, so `flush_logs()` is
    the fence: every line logged before it is on stderr when it returns."""
    flush_logs()
    seen = capfd.readouterr().err
    for line in seen.splitlines():
        try:
            fields = json.loads(line).get("fields", {})
        except ValueError:
            continue
        if fields.get("request_id") == rid:
            return fields["error"]
    raise AssertionError(f"no log line with request_id={rid}:\n{seen[-3000:]}")


def test_a_future_marker_lookup_that_raises_fails_the_request_with_that_error(client, capfd):
    resp = client.get("/marker-raises")
    assert resp.status_code == 500, (resp.status_code, resp.text)
    error = _logged_error(capfd, resp.json()["request_id"])
    assert "ValueError" in error and "marker lookup broke" in error, error


def test_an_awaitable_without_the_marker_is_run(client):
    resp = client.get("/no-marker")
    assert resp.status_code == 200, (resp.status_code, resp.text)
    assert resp.json() == {"ran": True}
