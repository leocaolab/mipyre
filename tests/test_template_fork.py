"""The process-lane template is started by the app's first run — from
`app.run()`, `mipyre dev` or a `TestClient` as from `mipyre run`. `mipyre run` starts it with
the platform's driver before it imports the app, forked from a process with no other thread
(Linux), the log's own writer thread stopped for the fork and started again. Every other entry
starts it as a new interpreter on every platform, so a test process
running the test runner's threads (pytest-xdist's, pytest-timeout's) serves process routes too.

Each case runs in a process of its own: the logger and the threads are process-wide state."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import urllib.request

from tests._helpers import bound_port, kill_session, read_file

APP = """
import os

from mipyre import Mipyre

app = Mipyre()


@app.get("/who", runtime="process")
def who(req):
    return {"pid": os.getpid(), "ppid": os.getppid()}


main_app = Mipyre(runtime="main")


@main_app.get("/")
def index(req):
    return "ok"
"""


def _run(tmp_path, script: str) -> subprocess.CompletedProcess:
    (tmp_path / "template_fork_app.py").write_text(textwrap.dedent(APP))
    (tmp_path / "template_fork_script.py").write_text(textwrap.dedent(script))
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(tmp_path), os.environ.get("PYTHONPATH")]))
    env["MIPYRE_PROCESS_WORKERS"] = "1"
    return subprocess.run(
        [sys.executable, str(tmp_path / "template_fork_script.py")],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120,
    )


def _lines(done: subprocess.CompletedProcess, tag: str) -> list[str]:
    return [line.split(" ", 1)[1] for line in done.stdout.splitlines() if line.startswith(tag + " ")]


def test_the_log_writer_stops_and_starts_again_and_no_line_is_lost_between(tmp_path):
    done = _run(tmp_path, """
        from mipyre.engine import (
            LogLevel, _log_writer_thread, _start_log_writer, _stop_log_writer, flush_logs, init_logger,
        )
        from mipyre._process_lane.threads import _other_threads

        print("no-logger", _stop_log_writer(), flush=True)
        init_logger(LogLevel.Info, False, "text")
        flush_logs()  # passes once the writer thread runs (and carries its name)
        name = _log_writer_thread()
        print("running", name in _other_threads(), flush=True)
        print("stopped", _stop_log_writer(), name in _other_threads(), flush=True)
        print("again", _stop_log_writer(), flush=True)
        init_logger(LogLevel.Info, False, "text")  # logs a line, written by this thread
        flush_logs()
        _start_log_writer()
        flush_logs()
        print("restarted", name in _other_threads(), flush=True)
        init_logger(LogLevel.Info, False, "text")  # logs a line, written by the thread
        flush_logs()
    """)
    assert done.returncode == 0, done.stderr[-3000:]
    assert _lines(done, "no-logger") == ["False"]
    assert _lines(done, "running") == ["True"]
    assert _lines(done, "stopped") == ["True False"]
    assert _lines(done, "again") == ["False"]
    assert _lines(done, "restarted") == ["True"]
    assert done.stderr.count("Mipyre tracing engine initialized") == 3, done.stderr[-3000:]


def test_a_testclient_serves_a_process_route_after_another_server_ran_in_the_process(tmp_path):
    # The first server installs the logger, whose writer thread then lives in the process; the
    # second server's template, a new interpreter, does not reach it.
    done = _run(tmp_path, """
        import os, subprocess

        from mipyre.testing import TestClient

        import template_fork_app

        with TestClient(template_fork_app.main_app) as c:
            assert c.get("/").text == "ok"
        with TestClient(template_fork_app.app) as c:
            who = c.get("/who").json()
        print("worker", who["pid"] != os.getpid(), flush=True)
        parent_of_template = subprocess.run(
            ["ps", "-o", "ppid=", "-p", str(who["ppid"])], capture_output=True, text=True, check=True
        ).stdout.strip()
        print("template-parent", int(parent_of_template) == os.getpid(), flush=True)
    """)
    assert done.returncode == 0, done.stderr[-3000:]
    assert _lines(done, "worker") == ["True"]
    assert _lines(done, "template-parent") == ["True"], done.stderr[-3000:]


def test_a_template_started_with_the_driver_after_a_logger_restarts_its_writer(tmp_path):
    # The writer thread lives once a logger is installed: a forked template (Linux) stops it for
    # the fork and starts it again; a spawned one (macOS) leaves it running.
    done = _run(tmp_path, """
        from mipyre.engine import LogLevel, _driver_name, _log_writer_thread, flush_logs, init_logger
        from mipyre._process_lane.server import start_template_with_driver
        from mipyre._process_lane.threads import _other_threads

        init_logger(LogLevel.Info, False, "text")
        flush_logs()
        template = start_template_with_driver("the test", "nothing to fix")
        print("started", template.pid > 0, _driver_name(), flush=True)
        print("writer", _log_writer_thread() in _other_threads(), flush=True)
        template.dismiss()
    """)
    assert done.returncode == 0, done.stderr[-3000:]
    from mipyre.engine import _driver_name

    assert _lines(done, "started") == [f"True {_driver_name()}"], done.stdout + done.stderr[-3000:]
    assert _lines(done, "writer") == ["True"], done.stdout + done.stderr[-3000:]


def test_the_driver_does_not_fork_a_template_from_a_process_running_another_thread(tmp_path):
    done = _run(tmp_path, """
        import threading

        from mipyre import ThreadedTemplateError
        from mipyre.engine import _driver_name
        from mipyre._process_lane.server import start_template_with_driver

        release = threading.Event()
        held = threading.Thread(target=release.wait, name="held-by-test")
        held.start()
        try:
            start_template_with_driver("the test,", "Start it in on_startup.").dismiss()
            print("started", _driver_name(), flush=True)
        except ThreadedTemplateError as e:
            print("refused", "held-by-test" in str(e), "on_startup" in str(e), _driver_name(), flush=True)
        finally:
            release.set()
            held.join()
    """)
    assert done.returncode == 0, done.stderr[-3000:]
    from mipyre.engine import _driver_name

    if _driver_name() == "fork":
        assert _lines(done, "refused") == ["True True fork"], done.stdout + done.stderr[-3000:]
    else:
        # A spawned template is a new interpreter: the thread does not reach it.
        assert _lines(done, "started") == ["spawn"], done.stdout + done.stderr[-3000:]


def test_a_testclient_serves_a_process_route_while_the_process_runs_another_thread(tmp_path):
    # A test server's template is a new interpreter on every platform: the
    # thread does not reach it, so nothing refuses it.
    done = _run(tmp_path, """
        import os, threading

        from mipyre.testing import TestClient

        import template_fork_app

        release = threading.Event()
        held = threading.Thread(target=release.wait, name="held-by-test")
        held.start()
        try:
            with TestClient(template_fork_app.app) as c:
                r = c.get("/who")
                print("served", r.status_code, r.json()["pid"] != os.getpid(), flush=True)
        finally:
            release.set()
            held.join()
    """)
    assert done.returncode == 0, done.stderr[-3000:]
    assert _lines(done, "served") == ["200 True"], done.stdout + done.stderr[-3000:]


def test_app_run_serves_a_process_route_while_the_process_runs_another_thread(tmp_path):
    # `python app.py` imported the app first, and an app's imports may start threads (numpy's
    # BLAS pool, a database pool): its template is a new interpreter on every platform.
    log_path = str(tmp_path / "server.log")
    (tmp_path / "threaded_app.py").write_text(textwrap.dedent("""
        import os, threading

        from mipyre import Mipyre, ServerFlags

        threading.Thread(target=threading.Event().wait, name="held-by-test", daemon=True).start()

        app = Mipyre()


        @app.get("/who", runtime="process")
        def who(req):
            return {"pid": os.getpid()}


        if __name__ == "__main__":
            app.run(ServerFlags(port=0))
    """))
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(tmp_path), os.environ.get("PYTHONPATH")]))
    env["MIPYRE_PROCESS_WORKERS"] = "1"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, str(tmp_path / "threaded_app.py")],
            cwd=tmp_path, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
    try:
        port = bound_port(read_file(log_path), proc)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/who", timeout=30) as r:
            assert r.status == 200 and json.loads(r.read())["pid"] != proc.pid
    finally:
        kill_session(proc)


def test_a_thread_started_before_mipyre_run_imports_the_app_stops_only_a_forked_template(tmp_path):
    # mipyre run starts its template before the import: only code that ran before it, here a
    # sitecustomize, can have started a thread. A forked template (Linux) is refused, naming what
    # to do; a spawned one (macOS) is a new interpreter, which the thread does not reach.
    from mipyre.engine import _driver_name

    (tmp_path / "template_fork_app.py").write_text(textwrap.dedent(APP))
    (tmp_path / "sitecustomize.py").write_text(textwrap.dedent("""
        import threading

        threading.Thread(target=threading.Event().wait, name="held-by-test", daemon=True).start()
    """))
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(tmp_path), os.environ.get("PYTHONPATH")]))
    env["MIPYRE_PROCESS_WORKERS"] = "1"
    log_path = str(tmp_path / "server.log")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "mipyre", "run", "template_fork_app:app", "--port", "0"],
            cwd=tmp_path, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
    try:
        if _driver_name() == "fork":
            proc.wait(timeout=120)
            out = read_file(log_path)()
            assert proc.returncode != 0, out[-3000:]
            assert "ThreadedTemplateError" in out and "held-by-test" in out, out[-3000:]
            assert "sitecustomize" in out and "app.run()" in out, out[-3000:]
        else:
            port = bound_port(read_file(log_path), proc)
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/who", timeout=30) as r:
                assert r.status == 200 and json.loads(r.read())["pid"] != proc.pid
    finally:
        kill_session(proc)


_PYTEST_SUITE = """
import os

from mipyre.testing import TestClient

import template_fork_app


def test_one():
    with TestClient(template_fork_app.app) as c:
        assert c.get("/who").json()["pid"] != os.getpid()


def test_two():
    with TestClient(template_fork_app.app) as c:
        assert c.get("/who").json()["pid"] != os.getpid()
"""


def _pytest(tmp_path, *args: str) -> subprocess.CompletedProcess:
    """Run a two-test suite hitting a process route under pytest with `args`, in a process of its
    own, with a config of its own (not this repository's)."""
    (tmp_path / "template_fork_app.py").write_text(textwrap.dedent(APP))
    (tmp_path / "test_process_route_suite.py").write_text(_PYTEST_SUITE)
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(tmp_path), os.environ.get("PYTHONPATH")]))
    env["MIPYRE_PROCESS_WORKERS"] = "1"
    env["MIPYRE_THREADS"] = "1"
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-q", *args, "test_process_route_suite.py"],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=240,
    )


def test_process_routes_are_tested_under_pytest_xdist(tmp_path):
    # xdist's execnet runs a thread in each worker process.
    done = _pytest(tmp_path, "-n", "2")
    assert done.returncode == 0, done.stdout[-3000:] + done.stderr[-3000:]
    assert "2 passed" in done.stdout, done.stdout[-3000:]


def test_process_routes_are_tested_under_pytest_timeouts_thread_method(tmp_path):
    # The thread method runs a timer thread beside each test.
    done = _pytest(tmp_path, "--timeout", "120", "-o", "timeout_method=thread")
    assert done.returncode == 0, done.stdout[-3000:] + done.stderr[-3000:]
    assert "2 passed" in done.stdout, done.stdout[-3000:]
