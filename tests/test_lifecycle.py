"""Tests for on_startup / on_shutdown lifecycle hooks."""

import pytest
from mipyre import Mipyre
from mipyre._server_run import ServerRun, Transport
from mipyre.engine import Deployment, ServerFlags
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


# b/370 (#30/#31), b/845: how a server run (`mipyre._server_run.ServerRun`) treats hooks that
# raise. Each test serves a main-runtime app on a real in-memory server.


def _app_with_hooks(startup=(), shutdown=()):
    app = Mipyre(runtime="main")

    @app.get("/")
    def index(req):
        return "ok"

    for hook in startup:
        app.on_startup(hook)
    for hook in shutdown:
        app.on_shutdown(hook)
    return app


def test_a_shutdown_hook_that_raises_fails_the_run_and_the_rest_still_run():
    ran = []

    def closes_a_closed_pool():
        ran.append("first")
        raise RuntimeError("pool already closed")

    def flushes_metrics():
        ran.append("second")

    client = TestClient(_app_with_hooks(shutdown=[closes_a_closed_pool, flushes_metrics]))
    with pytest.raises(RuntimeError, match="pool already closed"):
        client.close()
    assert ran == ["first", "second"]


def test_a_shutdown_hook_failure_is_raised_as_its_own_type():
    def releases_a_lock():
        raise PermissionError("not the owner")

    client = TestClient(_app_with_hooks(shutdown=[releases_a_lock]))
    with pytest.raises(PermissionError, match="not the owner"):
        client.close()


def test_two_shutdown_hook_failures_are_raised_together():
    def first():
        raise PermissionError("not the owner")

    def second():
        raise OSError("disk gone")

    client = TestClient(_app_with_hooks(shutdown=[first, second]))
    with pytest.raises(ExceptionGroup) as raised:
        client.close()
    assert [type(e) for e in raised.value.exceptions] == [PermissionError, OSError]


def test_a_shutdown_hook_that_exits_stops_the_hooks_after_it():
    ran = []

    def exits():
        ran.append("exits")
        raise SystemExit(3)

    def never_reached():
        ran.append("later")

    client = TestClient(_app_with_hooks(shutdown=[exits, never_reached]))
    with pytest.raises(SystemExit):
        client.close()
    assert ran == ["exits"]


def test_a_startup_hook_that_raises_fails_the_run_unbound_and_shutdown_hooks_still_run():
    ran = []

    def opens_the_db():
        ran.append("startup")
        raise ConnectionRefusedError("db is down")

    def never_reached():
        ran.append("later startup")

    def closes_the_db():
        ran.append("shutdown")

    app = _app_with_hooks(startup=[opens_the_db, never_reached], shutdown=[closes_the_db])
    run = ServerRun(app, Deployment(), ServerFlags(), transport=Transport.MEMORY)
    with pytest.raises(ConnectionRefusedError, match="db is down"):
        run.serve()
    with pytest.raises(ConnectionRefusedError, match="has not started"):
        run.connect()  # the server was never started after a startup hook failed
    assert ran == ["startup", "shutdown"]
