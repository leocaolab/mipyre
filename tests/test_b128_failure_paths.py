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

