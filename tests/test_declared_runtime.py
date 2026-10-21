"""Runtimes declared by the user (docs/process-lanes-declared-design.md): where each route
runs, what each runtime imports, and the errors for declarations that can't be served."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import urllib.request

import pytest

from tests._helpers import bound_port, read_file

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def serve(tmp_path, target: str, **env):
    log_path = str(tmp_path / "server.log")
    full = dict(os.environ, DECLARED_RUNTIME_LOG=str(tmp_path / "imports.log"), MIPYRE_PROCESS_WORKERS="2")
    full.update({k: str(v) for k, v in env.items()})
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "mipyre", "run", target, "--port", "0", "--workers", "2"],
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True, env=full, cwd=ROOT,
        )
    return proc, bound_port(read_file(log_path), proc), log_path


def stop(proc):
    if proc.poll() is None:
        os.killpg(proc.pid, signal.SIGINT)
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=5)


def get(port, path):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=30) as r:
        return json.loads(r.read())


def imports(tmp_path) -> list[tuple[str, int, str]]:
    path = tmp_path / "imports.log"
    if not path.exists():
        return []
    return [(m, int(p), w) for m, p, w in (line.split() for line in path.read_text().splitlines())]


def test_a_mixed_app_runs_each_router_in_its_runtime_and_imports_it_only_there(tmp_path):
    proc, port, log = serve(tmp_path, "tests.apps.declared_runtime.main:app")
    try:
        server = proc.pid
        api, where = get(port, "/api"), get(port, "/where")
        analytics, on_main, admin = get(port, "/analytics"), get(port, "/analytics/main"), get(port, "/admin")
        assert api == {"pid": server, "subinterp": True}
        assert where == {"pid": server, "subinterp": True}
        assert analytics["pid"] != server and analytics["subinterp"] is False and analytics["mean"] == 2.0
        assert on_main == {"pid": server, "subinterp": False}
        assert admin == {"pid": server, "subinterp": False}
        seen = imports(tmp_path)
        in_subinterp = {m for m, _, w in seen if w == "subinterp"}
        # Sub-interpreters import the app module and the router with sub-interpreter routes;
        # never the process router (numpy) or the main-only one.
        assert in_subinterp == {"main", "api"}, seen
        in_process_workers = {m for m, p, w in seen if p not in (server,) and w == "main-interpreter"}
        assert in_process_workers == {"main", "analytics"}, seen
    finally:
        stop(proc)


def test_an_app_with_no_sub_interpreter_route_builds_no_sub_interpreter(tmp_path):
    """b/34: the app module (and whatever it imports) is never imported by a sub-interpreter
    when none of its routes runs on one."""
    proc, port, log = serve(tmp_path, "tests.apps.declared_runtime.process_only:app")
    try:
        p, m = get(port, "/p"), get(port, "/m")
        assert p["pid"] != proc.pid and p["subinterp"] is False
        assert m == {"pid": proc.pid, "subinterp": False}
        seen = imports(tmp_path)
        assert not [s for s in seen if s[2] == "subinterp"], seen
        banner = read_file(log)()
        assert "Sub-interpreters: none (no route runs on one)" in banner, banner
        assert "Routes: 0 sub-interp + 1 GIL + 1 process + 0 async" in banner, banner
    finally:
        stop(proc)


def test_routes_of_one_file_can_run_in_different_runtimes(tmp_path):
    proc, port, _ = serve(tmp_path, "tests.apps.declared_runtime.per_route:app")
    try:
        assert get(port, "/light") == {"pid": proc.pid, "subinterp": True}
        heavy = get(port, "/heavy")
        assert heavy["pid"] != proc.pid and heavy["subinterp"] is False and heavy["sum"] == 45
    finally:
        stop(proc)


def test_an_unknown_runtime_is_a_registration_error():
    from mipyre import Mipyre, Router
    from mipyre._runtime import RuntimeDeclarationError

    with pytest.raises(RuntimeDeclarationError, match="runtime='gpu' is not one of"):
        Mipyre(runtime="gpu")
    app = Mipyre()
    with pytest.raises(RuntimeDeclarationError, match="GET /x: runtime='proc'"):
        app.get("/x", lambda req: 1, runtime="proc")
    with pytest.raises(RuntimeDeclarationError):
        Router(runtime="sub")


def test_a_router_has_every_method_decorator_the_app_has():
    from mipyre import Router

    router = Router()
    for method in ("get", "post", "put", "delete", "patch", "options", "head"):
        getattr(router, method)(f"/{method}")(lambda req: 1)
    assert [(r.method, r.path) for r in router._routes] == [
        (m.upper(), f"/{m}") for m in ("get", "post", "put", "delete", "patch", "options", "head")
    ]


def test_the_removed_route_flags_are_errors_quoting_the_new_spelling():
    from mipyre import Mipyre

    app = Mipyre()
    with pytest.raises(TypeError, match=r'GET /x: gil=True was replaced by runtime=\'main\''):
        app.get("/x", lambda req: 1, gil=True)
    with pytest.raises(TypeError, match=r"POST /y: process=True was replaced by runtime='process'"):
        app.post("/y", lambda req: 1, process=True)
    assert app.routes == []


def test_isolate_is_an_error_naming_the_new_way():
    from mipyre import Mipyre

    with pytest.raises(TypeError, match=r"app.isolate\('numpy'\) was removed .*runtime='process'"):
        Mipyre().isolate("numpy")
