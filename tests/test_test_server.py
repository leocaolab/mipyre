"""b/897 (docs/design/test-server.md): `TestServer` serves one run of the app in the test
process on a real port, any client reaches it; `TestClient(server)` is a client of it.

Cases that start a process-lane template or a sub-interpreter worker run in a process of their
own (`tests/test_template_fork.py`): threads and the template are process-wide state."""

from __future__ import annotations

import errno
import os
import socket
import subprocess
import sys
import textwrap
import threading
import urllib.request

import pytest

from mipyre import Mipyre, Response
from mipyre.testing import TestClient, TestServer, _url

APP = """
import os

from mipyre import Mipyre

app = Mipyre()


@app.get("/m", runtime="main")
def on_main(req):
    return {"pid": os.getpid()}


@app.get("/s")
def on_subinterp(req):
    return {"pid": os.getpid()}


@app.get("/p", runtime="process")
def on_process(req):
    return {"pid": os.getpid()}


@app.get("/template", runtime="main")
def template(req):
    return {"pid": app.process_lanes.template_pid}


if __name__ == "__mipyre_worker__" and os.environ.get("TS_WORKER_REFUSES"):
    raise ValueError("ts-worker-refuses")
"""


def _run(tmp_path, script: str, **env_extra: str) -> subprocess.CompletedProcess:
    (tmp_path / "ts_app.py").write_text(textwrap.dedent(APP))
    (tmp_path / "ts_script.py").write_text(textwrap.dedent(script))
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(tmp_path), os.environ.get("PYTHONPATH")]))
    env["MIPYRE_THREADS"] = "1"
    env["MIPYRE_PROCESS_WORKERS"] = "1"
    env.update(env_extra)
    return subprocess.run(
        [sys.executable, str(tmp_path / "ts_script.py")],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120,
    )


def _lines(done: subprocess.CompletedProcess, tag: str) -> list[str]:
    return [line.split(" ", 1)[1] for line in done.stdout.splitlines() if line.startswith(tag + " ")]


def _main_app() -> Mipyre:
    app = Mipyre(runtime="main")

    @app.get("/")
    def index(req):
        return "ok"

    return app


def _get(url: str) -> str:
    with urllib.request.urlopen(url, timeout=10) as r:
        return r.read().decode()


def _server_threads() -> set[int | None]:
    return {t.ident for t in threading.enumerate() if t.name == "mipyre-testserver"}


# E2E-1


def test_any_client_reaches_every_runtime_and_the_port_is_free_after_close(tmp_path):
    done = _run(tmp_path, """
        import json, os, socket, urllib.request

        from mipyre.testing import TestServer

        import ts_app

        with TestServer(ts_app.app) as s:
            print("port", s.port != 0, s.url == f"http://127.0.0.1:{s.port}", flush=True)
            for path in ("/m", "/s", "/p"):
                with urllib.request.urlopen(s.url + path, timeout=30) as r:
                    print("route", path, r.status, json.loads(r.read())["pid"] == os.getpid(), flush=True)
            port = s.port
        with socket.socket() as again:
            again.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            again.bind(("127.0.0.1", port))
            again.listen()
            print("rebound True", flush=True)
    """)
    assert done.returncode == 0, done.stderr[-3000:]
    assert _lines(done, "port") == ["True True"]
    assert _lines(done, "route") == ["/m 200 True", "/s 200 True", "/p 200 False"], done.stderr[-3000:]
    assert _lines(done, "rebound") == ["True"]


# E2E-2


def test_a_startup_hook_error_is_the_constructors_error_and_leaves_no_thread():
    app = _main_app()

    @app.on_startup
    def needs_a_key():
        raise KeyError("dsn")

    before = _server_threads()
    with pytest.raises(KeyError, match="dsn"):
        TestServer(app)
    assert _server_threads() - before == set()


def test_a_taken_port_is_the_constructors_oserror():
    # A listener without SO_REUSEPORT: the server's SO_REUSEPORT bind can't join it.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        port = taken.getsockname()[1]
        before = _server_threads()
        with pytest.raises(OSError) as raised:
            TestServer(_main_app(), port=port)
    assert raised.value.errno == errno.EADDRINUSE, raised.value
    assert _server_threads() - before == set()


def test_a_worker_that_cannot_build_the_app_is_the_constructors_error(tmp_path):
    done = _run(tmp_path, """
        from mipyre.testing import TestServer

        import ts_app

        try:
            TestServer(ts_app.app)
        except Exception as e:
            print("raised", "ts-worker-refuses" in f"{e} {e.__cause__}", flush=True)
    """, TS_WORKER_REFUSES="1")
    assert done.returncode == 0, done.stderr[-3000:]
    assert _lines(done, "raised") == ["True"], done.stdout + done.stderr[-3000:]


# E2E-3


def test_clients_of_one_server_keep_their_own_cookies_and_leave_it_serving():
    app = _main_app()

    @app.post("/login")
    def login(req):
        return Response(body="", headers={"Set-Cookie": "sid=abc123; Path=/"})

    @app.get("/whoami")
    def whoami(req):
        return {"cookie": req.headers.get("cookie", "")}

    with TestServer(app) as server:
        with TestClient(server) as first:
            first.post("/login")
            assert first.get("/whoami").json() == {"cookie": "sid=abc123"}
        with TestClient(server) as second:
            assert second.get("/whoami").json() == {"cookie": ""}
            assert second.base_url == server.url
        assert _get(server.url + "/") == "ok"


def test_close_raises_the_runs_outcome_once():
    app = _main_app()

    @app.on_shutdown
    def closes_a_pool():
        raise RuntimeError("pool closed")

    server = TestServer(app)
    with TestClient(server) as c:
        assert c.get("/").text == "ok"
    with pytest.raises(RuntimeError, match="pool closed"):
        server.close()
    server.close()


# E2E-4


def test_a_server_that_dies_mid_run_fails_the_next_request_and_close_with_why(tmp_path):
    # The lanes' workers die with their template and none can be forked again: the server stops.
    done = _run(tmp_path, """
        import os, signal

        from mipyre.testing import TestClient, TestServer

        import ts_app

        server = TestServer(ts_app.app)
        client = TestClient(server)
        assert client.get("/p").status_code == 200
        os.kill(client.get("/template").json()["pid"], signal.SIGKILL)
        server._thread.join(60)
        print("stopped", not server._thread.is_alive(), flush=True)
        try:
            client.get("/m")
        except RuntimeError as e:
            failed = e
        cause = failed.__cause__
        print("cause", "the process lanes failed" in str(cause), flush=True)
        try:
            server.close()
        except BaseException as e:
            print("close", e is cause, flush=True)
        server.close()
        print("again quiet", flush=True)
    """)
    assert done.returncode == 0, done.stderr[-3000:]
    assert _lines(done, "stopped") == ["True"], done.stderr[-3000:]
    assert _lines(done, "cause") == ["True"], done.stdout + done.stderr[-3000:]
    assert _lines(done, "close") == ["True"]
    assert _lines(done, "again") == ["quiet"]


# E2E-6


def test_a_testclient_of_an_app_has_no_port():
    with TestClient(_main_app()) as c:
        assert c.get("/").text == "ok"
        with pytest.raises(AttributeError):
            c.port


# U-1


@pytest.mark.parametrize(
    "host,url",
    [
        ("127.0.0.1", "http://127.0.0.1:8080"),
        ("::1", "http://[::1]:8080"),
        ("localhost", "http://localhost:8080"),
    ],
)
def test_url_brackets_an_ipv6_address(host, url):
    assert _url(host, 8080) == url
