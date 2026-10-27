"""A start="import" worker imports a mipyre submodule the template never imported.

It must read it from the launcher's copy, not the install. A forked worker (Linux) runs the
launcher's engine (.so, in memory since the fork), so after an in-place change to the install the
new Python file would meet it, and two versions must never meet. A spawned worker (macOS)
imports everything from the copy, and the test holds for it as well.
"""

import json
import os
import shutil
import signal
import subprocess
import sys
import urllib.error
import urllib.request

import mipyre
from tests._helpers import bound_port, poll_until, read_file

_APP = '''
import os

from mipyre import Mipyre, engine

app = Mipyre()


@app.get("/probe", runtime="process")
def probe(req):
    from mipyre import _lazy_probe

    return {"value": _lazy_probe.VALUE, "build": engine.BUILD_ID, "pid": os.getpid(),
            "probe_file": _lazy_probe.__file__}


@app.get("/die", runtime="process")
def die(req):
    os._exit(1)
'''


def _get(port, path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=30) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def test_a_worker_reads_a_not_yet_imported_mipyre_module_from_the_launchers_copy(tmp_path):
    site = tmp_path / "site"
    shutil.copytree(os.path.dirname(mipyre.__file__), site / "mipyre", ignore=shutil.ignore_patterns("__pycache__"))
    probe = site / "mipyre" / "_lazy_probe.py"
    probe.write_text('VALUE = "old"\n')
    (tmp_path / "app.py").write_text(_APP)
    env = {k: v for k, v in os.environ.items() if k != "XDG_CACHE_HOME"}
    env.update(PYTHONPATH=str(site), HOME=str(tmp_path / "home"), MIPYRE_PROCESS_WORKERS="1")
    log_path = tmp_path / "server.log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "mipyre", "run", "app.py:app", "--port", "0"],
            cwd=tmp_path, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
    try:
        port = bound_port(read_file(str(log_path)), proc)

        def answer():
            status, body = _get(port, "/probe")
            return json.loads(body) if status == 200 else None

        first = poll_until(answer, timeout=None, what="the first worker's answer")
        with open(probe, "r+") as f:  # the in-place upgrade of one file
            f.write('VALUE = "new, a different size"\n')
        assert _get(port, "/die")[0] == 502
        second = poll_until(lambda: (a := answer()) and a["pid"] != first["pid"] and a, timeout=None,
                            what="a replacement worker")
        assert second["build"] == first["build"]
        assert second["value"] == "old", f"the replacement worker (old engine) imported the new file: {second}"
    finally:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        proc.wait(timeout=30)
