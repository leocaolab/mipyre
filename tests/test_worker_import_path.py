"""A worker resolves the script's imports as main does.

A new sub-interpreter's `sys.path` is only what the process started with. Without what main
added at run time (pytest's `pythonpath` / rootdir entries, the directory `mipyre run` puts
first), a worker re-executing a test file that does `from tests.apps.… import …` fails with
`ModuleNotFoundError: No module named 'tests'` under a bare `pytest` (its `sys.path[0]` is the
venv's `bin`), and `mipyre run app:app` cannot serve an app that imports a module next to it.
Workers start from main's `sys.path` (`WorkerSpec::import_path`).
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
import urllib.request

import pytest

from mipyre import Mipyre
from mipyre.testing import TestClient
from tests._helpers import answers, bound_port, read_file
from tests.apps.worker_import_sibling import add_routes

# Module level: sub-interpreter workers rebuild it by executing this file, and this file
# imports its routes from a sibling module in the `tests` package.
app = Mipyre()
add_routes(app)


def test_module_app_importing_a_sibling_serves_on_workers():
    with TestClient(app) as client:
        r = client.get("/import-env")
        assert r.status_code == 200
        assert r.json()["sibling"] == "tests.apps.worker_import_sibling"
        # Served by a sub-interpreter worker, which imported the sibling itself.
        assert r.json()["in_worker"] is True


def test_worker_sys_path_is_mains():
    # The whole of main's `sys.path`, in order, including the entries main added at run
    # time (here pytest's `tests/` and root dir), which a new interpreter never has.
    main_path = list(sys.path)
    with TestClient(app) as client:
        env = client.get("/import-env").json()
        assert env["in_worker"] is True
        assert env["sys_path"] == main_path


HOST = "127.0.0.1"
SERVE_TIMEOUT_S = 30


@pytest.mark.parametrize("tpc", ["1", "0"], ids=["tpc", "pool"])
def test_cli_run_serves_an_app_importing_a_module_next_to_it(tpc):
    # The console script: its `sys.path[0]` is the venv's `bin`, and `mipyre run` puts
    # the working directory first on main's `sys.path` so `app` imports (cli.py). The
    # workers execute `app.py` by path; its `import helper` must resolve there too.
    cli = shutil.which("mipyre", path=os.path.dirname(sys.executable))
    assert cli is not None, "the mipyre console script is not installed next to python"
    project = tempfile.mkdtemp(prefix="mipyre-import-path-")
    with open(os.path.join(project, "helper.py"), "w") as f:
        f.write("GREETING = 'from helper'\n")
    with open(os.path.join(project, "app.py"), "w") as f:
        f.write(textwrap.dedent("""\
            import helper
            from mipyre import Mipyre

            app = Mipyre()

            @app.get("/")
            def root(req):
                return {"greeting": helper.GREETING}
            """))
    log_path = os.path.join(project, "server.log")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [cli, "run", "app:app", "--host", HOST, "--port", "0",
             "--subinterpreters" if tpc == "0" else "--threads", "2"],
            cwd=project,
            env={**os.environ, "MIPYRE_TPC": tpc},
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
    try:
        try:
            port = bound_port(read_file(log_path), proc)
            answers(f"http://{HOST}:{port}/", proc, read_file(log_path))
        except RuntimeError as e:
            pytest.fail(f"server did not serve: {e}")
        with urllib.request.urlopen(f"http://{HOST}:{port}/", timeout=10) as r:
            body = r.read().decode()
        assert "from helper" in body
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=SERVE_TIMEOUT_S)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        shutil.rmtree(project, ignore_errors=True)
