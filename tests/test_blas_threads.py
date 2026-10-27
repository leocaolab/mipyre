"""BLAS (and polars) thread pools are limited in process-lane worker processes only, before
the app is imported there; the server process is left alone. A pool already sized through any
one of its variables is left whole, as it was set."""

import json
import os
import signal
import subprocess
import sys
import urllib.request

from tests._helpers import bound_port, read_file

APP = os.path.join(os.path.dirname(__file__), "apps", "blas_env_app.py")
VARS = ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "POLARS_MAX_THREADS")


def _serve(tmp_path, start, **user_env):
    env = {k: v for k, v in os.environ.items() if k not in VARS}
    env.update(user_env, MIPYRE_PROCESS_WORKERS="1", MIPYRE_PROCESS_START=start)
    log = str(tmp_path / f"server-{start}.log")
    with open(log, "w") as f:
        proc = subprocess.Popen([sys.executable, "-m", "mipyre", "run", f"{APP}:app", "--port", "0"],
                                stdout=f, stderr=subprocess.STDOUT, start_new_session=True, env=env)
    try:
        port = bound_port(read_file(log), proc)
        def get(p):
            return json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}{p}", timeout=30).read())
        return get("/worker"), get("/server")
    finally:
        os.killpg(proc.pid, signal.SIGINT)
        proc.wait(timeout=15)


def test_workers_import_the_app_with_one_thread_per_pool_and_the_server_is_untouched(tmp_path):
    for start in ("import", "fork"):
        worker, server = _serve(tmp_path, start)
        assert worker["pid"] != server["pid"]
        assert worker["at_import"] == {v: "1" for v in VARS}, (start, worker)
        assert server["now"] == {v: None for v in VARS}, (start, server)


def test_a_variable_the_user_set_is_left_alone(tmp_path):
    worker, server = _serve(tmp_path, "import", OPENBLAS_NUM_THREADS="4")
    assert worker["at_import"]["OPENBLAS_NUM_THREADS"] == "4"
    assert worker["at_import"]["OMP_NUM_THREADS"] is None
    assert server["now"]["OPENBLAS_NUM_THREADS"] == "4"
    assert server["now"]["OMP_NUM_THREADS"] is None
