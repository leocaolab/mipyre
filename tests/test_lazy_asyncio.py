"""A worker imports asyncio (which brings ssl, subprocess and more: ~3 MiB a worker) only
when its app needs an event loop: at start when a handler or hook is `async def`, else on
the first awaitable a handler returns."""

import contextlib
import http.client
import os
import signal
import subprocess
import sys
import tempfile
from pathlib import Path

from mipyre.testing import TestClient
from tests._helpers import bound_port, lines_with, read_file

REPO = Path(__file__).resolve().parent.parent


def _get(conn, path):
    conn.request("GET", path)
    resp = conn.getresponse()
    return resp.status, resp.read().decode()


def test_a_worker_of_an_app_without_async_does_not_import_asyncio():
    from tests.apps.lazy_asyncio_sync import app

    with TestClient(app, transport="tcp") as client:
        # One connection: one TPC thread, one worker.
        conn = http.client.HTTPConnection("127.0.0.1", client.port, timeout=10)
        assert _get(conn, "/asyncio-loaded") == (200, "False")
        # A `def` handler returning a coroutine still runs it, on a loop made now.
        assert _get(conn, "/awaitable") == (200, "ran")
        assert _get(conn, "/asyncio-loaded") == (200, "True")
        conn.close()


def test_an_app_with_an_async_route_works_as_before():
    from tests.apps.lazy_asyncio_async import app

    with TestClient(app, transport="tcp") as client:
        conn = http.client.HTTPConnection("127.0.0.1", client.port, timeout=10)
        assert _get(conn, "/asyncio-loaded") == (200, "True")
        assert _get(conn, "/async") == (200, "async ran")
        conn.close()



@contextlib.contextmanager
def _served(app_file: str):
    """The app in tests/apps/`app_file` run as a script (port 0, one worker): its port
    and a reader of the server's log."""
    script = REPO / "tests" / "apps" / app_file
    fd, log_path = tempfile.mkstemp(prefix="mipyre_lazy_asyncio_", suffix=".log")
    env = dict(os.environ)
    env.pop("MIPYRE_LOG", None)
    with os.fdopen(fd, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
            cwd=REPO,
        )
    try:
        yield bound_port(read_file(log_path), proc), read_file(log_path)
    finally:
        if proc.poll() is None:
            os.killpg(os.getpgid(proc.pid), signal.SIGINT)
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.wait(timeout=5)
        os.unlink(log_path)


def _assert_no_event_loop_error(port, read_log, path, handler):
    """`GET path` answers the usual 500, and the log carries mipyre's NoEventLoopError
    for it: the route, that it is not supported, what works instead, and asyncio's own
    error as the cause, with the traceback."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    status, body = _get(conn, path)
    conn.close()
    assert status == 500, body
    assert "NoEventLoopError" not in body and "asyncio" not in body, body
    lines = lines_with(read_log, "NoEventLoopError")
    assert len(lines) == 1, read_log()[-3000:]
    line = lines[0]
    assert f"GET {path} (handler {handler})" in line, line
    assert "mipyre does not support calls that need the current loop" in line, line
    assert "Make the route `async def`" in line, line
    assert "asyncio.run(...)" in line and "asyncio.new_event_loop()" in line, line
    assert "There is no current event loop" in line, line
    assert "Traceback (most recent call last)" in line, line


def test_get_event_loop_in_a_def_handler_is_mipyres_error():
    # Not supported (and never was: a worker's loop, when it has one, is set on the
    # thread that built it, not on the one serving). The module imports asyncio at the
    # top; the handler asks for the current loop.
    with _served("lazy_asyncio_imported.py") as (port, read_log):
        _assert_no_event_loop_error(port, read_log, "/get-event-loop", "get_event_loop")


def test_get_event_loop_after_a_late_asyncio_import_is_mipyres_error():
    # The app imported no asyncio when it loaded; the handler imports it itself.
    with _served("lazy_asyncio_late_import.py") as (port, read_log):
        _assert_no_event_loop_error(port, read_log, "/late-loop", "late_loop")


def test_a_def_handler_runs_a_coroutine_on_a_loop_of_its_own():
    # What the error tells the user to do instead works in a def handler.
    with _served("lazy_asyncio_imported.py") as (port, _read_log):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        assert _get(conn, "/asyncio-run") == (200, "ran")
        assert _get(conn, "/new-event-loop") == (200, "ran")
        conn.close()
