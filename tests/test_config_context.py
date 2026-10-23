"""Tests for Settings + request-scoped ctx."""

from __future__ import annotations


import pytest

from mipyre import Mipyre
from mipyre.context import ctx
from mipyre.testing import TestClient


# ---------------------------------------------------------------------------
# Settings — skipped when pydantic-settings isn't installed
# ---------------------------------------------------------------------------


_has_pydantic_settings = True
try:
    import pydantic_settings  # noqa: F401 — imported only to see whether it is installed
except ImportError:
    _has_pydantic_settings = False

skip_no_ps = pytest.mark.skipif(
    not _has_pydantic_settings,
    reason="pydantic-settings not installed",
)


@skip_no_ps
def test_settings_reads_env(monkeypatch):
    from mipyre.config import Settings

    class S(Settings):
        database_url: str
        debug: bool = False

    monkeypatch.setenv("DATABASE_URL", "postgres://x")
    monkeypatch.setenv("DEBUG", "true")
    s = S()
    assert s.database_url == "postgres://x"
    assert s.debug is True


@skip_no_ps
def test_settings_case_insensitive(monkeypatch):
    from mipyre.config import Settings

    class S(Settings):
        api_key: str

    monkeypatch.setenv("API_KEY", "secret")
    assert S().api_key == "secret"


@skip_no_ps
def test_settings_ignores_unknown_env(monkeypatch):
    """Unknown env vars don't crash the app — important for deployments
    where the env is shared across many services."""
    from mipyre.config import Settings

    class S(Settings):
        port: int = 8000

    monkeypatch.setenv("SOMETHING_ELSE", "noise")
    assert S().port == 8000


# ---------------------------------------------------------------------------
# ctx
# ---------------------------------------------------------------------------


def test_ctx_set_get():
    ctx.clear()
    ctx.set("foo", 42)
    # This test writes the test thread's own ctx: it leaves it as it found it, so a later
    # test on the same xdist worker does not read its value.
    try:
        assert ctx.get("foo") == 42
        assert ctx.get("missing", "default") == "default"
    finally:
        ctx.clear()


def test_the_clients_request_id_reaches_the_handler_and_a_ctx_snapshot_is_detached():
    app = Mipyre(runtime="main")
    app.enable_request_id_header()
    seen = {}

    @app.get("/")
    def root(req):
        seen["rid"] = req.request_id
        seen["snap"] = ctx.snapshot()
        return "ok"

    # The handler writes into this test's dict/list: only the main interpreter shares it.
    with TestClient(app) as c:
        r = c.get("/", headers={"X-Request-ID": "trace-xyz"})
        assert r.status_code == 200
        assert seen["rid"] == "trace-xyz"
        # Snapshot is a detached copy — mutating it doesn't affect the live ctx. Compared with
        # what the live ctx held before, under a key no other test writes: until b/441 this
        # asserted `ctx.get("foo") is None`, which a value another test left under "foo" in the
        # same process turned red.
        before = ctx.get(_SNAPSHOT_PROBE)
        seen["snap"][_SNAPSHOT_PROBE] = "written to the snapshot"
        assert ctx.get(_SNAPSHOT_PROBE) == before


# The key the snapshot test writes; no other test uses it.
_SNAPSHOT_PROBE = "test_config_context.snapshot_probe"


def test_the_snapshot_check_sees_a_write_to_the_live_ctx():
    """The comparison above sees a write that reaches the live ctx: the check's own input."""
    before = ctx.get(_SNAPSHOT_PROBE)
    ctx.set(_SNAPSHOT_PROBE, "written to the live ctx")
    try:
        assert ctx.get(_SNAPSHOT_PROBE) != before
    finally:
        ctx.set(_SNAPSHOT_PROBE, before)


def test_ctx_isolated_between_requests():
    app = Mipyre(runtime="main")
    app.enable_request_id_header()
    observed: list[str | None] = []

    @app.get("/set")
    def set_it(req):
        ctx.set("leaky", "from_first")
        return "ok"

    @app.get("/read")
    def read_it(req):
        observed.append(ctx.get("leaky"))
        return "ok"

    # The handler writes into this test's dict/list: only the main interpreter shares it.
    with TestClient(app) as c:
        c.get("/set")
        c.get("/read")
        # Second request started with a fresh ctx — it must not see the
        # value set by the first.
        assert observed == [None]
