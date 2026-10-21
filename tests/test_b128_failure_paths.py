"""b/133: a handler that cannot be called fails its registration, not the first request."""

from __future__ import annotations

import pytest

from mipyre import Mipyre


def test_a_non_callable_handler_fails_the_registration():
    app = Mipyre()
    with pytest.raises(TypeError, match="not callable"):
        app.get("/not-callable")(42)
    # The refused route was never added: the same path can still be registered.
    @app.get("/not-callable")
    def ok(req):
        return {"ok": True}

    assert ok is not None


def test_a_worker_engine_without_an_app_fails_its_start():
    # The main interpreter records no worker app: asking for the worker's handler table
    # is the startup configuration error (b/135), and says so, rather than returning an
    # empty table that fails every request with a KeyError.
    from mipyre import engine

    with pytest.raises(RuntimeError, match="has no app"):
        engine._worker_app_handlers()


def test_a_worker_engine_without_an_app_has_no_hooks_either():
    # The hooks are read from the same app as the handlers: without one there are no empty hooks,
    # only the same startup error.
    from mipyre import engine

    with pytest.raises(RuntimeError, match="has no app"):
        engine._worker_app_hooks()
