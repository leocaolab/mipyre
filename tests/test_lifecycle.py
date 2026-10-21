"""Tests for on_startup / on_shutdown lifecycle hooks."""

import pytest
from mipyre import Mipyre
from mipyre.testing import TestClient


@pytest.fixture(scope="module")
def app_with_hooks():
    """Create an app with startup hooks that set state."""
    # Module level (workers rebuild the app by executing its module).
    from tests.apps.lifecycle_hooks import app, hook_log

    return app, hook_log


@pytest.fixture(scope="module")
def client(app_with_hooks):
    app, _ = app_with_hooks
    c = TestClient(app)
    yield c
    c.close()


def test_startup_hooks_executed(client, app_with_hooks):
    """Startup hooks should run before server accepts requests."""
    _, hook_log = app_with_hooks
    assert hook_log["startup_called"] is True


def test_startup_hook_order(client, app_with_hooks):
    """Startup hooks should run in registration order."""
    _, hook_log = app_with_hooks
    assert hook_log["startup_order"] == ["init_cache", "init_counter"]


def test_startup_initialized_state(client):
    """State set by startup hooks should be accessible via routes."""
    resp = client.get("/cache-status")
    assert resp.json()["ready"] == "true"


def test_startup_counter_initialized(client):
    """Multiple startup hooks should all execute."""
    resp = client.get("/counter")
    assert resp.json()["counter"] == "0"


def test_on_startup_as_decorator():
    """on_startup should work as a decorator and return the original function."""
    app = Mipyre()

    @app.on_startup
    def my_hook():
        pass

    assert my_hook is not None
    assert callable(my_hook)
    assert my_hook.__name__ == "my_hook"


def test_on_startup_as_direct_call():
    """on_startup should work as a direct call (non-decorator)."""
    app = Mipyre()
    called = []

    def my_hook():
        called.append(True)

    app.on_startup(my_hook)
    assert len(app._startup_hooks) == 1


def test_on_shutdown_as_decorator():
    """on_shutdown should work as a decorator and return the original function."""
    app = Mipyre()

    @app.on_shutdown
    def cleanup():
        pass

    assert cleanup is not None
    assert callable(cleanup)
    assert cleanup.__name__ == "cleanup"


def test_on_shutdown_as_direct_call():
    """on_shutdown should work as a direct call (non-decorator)."""
    app = Mipyre()

    def cleanup():
        pass

    app.on_shutdown(cleanup)
    assert len(app._shutdown_hooks) == 1


def test_multiple_startup_hooks_registered():
    """Multiple startup hooks should all be registered."""
    app = Mipyre()

    @app.on_startup
    def hook1():
        pass

    @app.on_startup
    def hook2():
        pass

    @app.on_startup
    def hook3():
        pass

    assert len(app._startup_hooks) == 3


def test_multiple_shutdown_hooks_registered():
    """Multiple shutdown hooks should all be registered."""
    app = Mipyre()

    @app.on_shutdown
    def hook1():
        pass

    @app.on_shutdown
    def hook2():
        pass

    assert len(app._shutdown_hooks) == 2


# b/370 (#30/#31): how a server's lifetime (`Mipyre._serve`) treats hooks that raise. `start`
# stands in for binding and serving; `_prepare` (sealing the app, logging setup) is not under
# test and is skipped, as in test_b149_conformance.


def _app_with_hooks(monkeypatch, startup=(), shutdown=()):
    app = Mipyre()
    monkeypatch.setattr(app, "_prepare", lambda settings: None)
    for hook in startup:
        app.on_startup(hook)
    for hook in shutdown:
        app.on_shutdown(hook)
    return app


def test_a_shutdown_hook_that_raises_is_logged_and_the_rest_still_run(monkeypatch, caplog):
    ran = []

    def closes_a_closed_pool():
        ran.append("first")
        raise RuntimeError("pool already closed")

    def flushes_metrics():
        ran.append("second")

    app = _app_with_hooks(monkeypatch, shutdown=[closes_a_closed_pool, flushes_metrics])
    started = []
    with caplog.at_level("ERROR", logger="mipyre.app"):
        app._serve(None, started.append)  # returns: a teardown failure does not fail the run
    assert started == [None]
    assert ran == ["first", "second"]
    [record] = [r for r in caplog.records if r.name == "mipyre.app"]
    assert record.getMessage() == "shutdown hook closes_a_closed_pool raised"
    assert record.exc_info is not None, "logged without its traceback"
    assert isinstance(record.exc_info[1], RuntimeError)
    assert str(record.exc_info[1]) == "pool already closed"


def test_a_shutdown_hook_failure_is_logged_with_its_error_type(monkeypatch, caplog):
    # b/413: an exception log carries the exception's type as a field, beside its traceback.
    def releases_a_lock():
        raise PermissionError("not the owner")

    app = _app_with_hooks(monkeypatch, shutdown=[releases_a_lock])
    with caplog.at_level("ERROR", logger="mipyre.app"):
        app._serve(None, lambda settings: None)
    [record] = [r for r in caplog.records if r.name == "mipyre.app"]
    assert record.error_type == "PermissionError"
    assert record.exc_info is not None and isinstance(record.exc_info[1], PermissionError)


def test_a_startup_hook_that_raises_fails_the_run_unbound_and_shutdown_hooks_still_run(monkeypatch):
    ran = []

    def opens_the_db():
        ran.append("startup")
        raise ConnectionRefusedError("db is down")

    def never_reached():
        ran.append("later startup")

    def closes_the_db():
        ran.append("shutdown")

    app = _app_with_hooks(monkeypatch, startup=[opens_the_db, never_reached], shutdown=[closes_the_db])
    started = []
    with pytest.raises(ConnectionRefusedError, match="db is down"):
        app._serve(None, started.append)
    assert started == [], "the server was started (and bound its port) after a startup hook failed"
    assert ran == ["startup", "shutdown"]
