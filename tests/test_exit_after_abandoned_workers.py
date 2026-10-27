"""A process exits cleanly after its server left its workers running past the grace period.

The script gives its server a grace period no thread can meet (1 ns), so `TestClient.close`
abandons every TPC thread while each is still on its way out, and then the process exits at
once: the threads end their workers while the process finalizes. Two things can go wrong there:

- a thread that was let go drops the last reference to its copy of the site after its worker
  ended, with no Python thread state: PyO3 panics on the first `Py<T>` of the route table and
  prints "a Py<T> was dropped on a thread that has no Python thread state" for the rest;
- a thread's `Py_EndInterpreter` runs while `Py_Finalize` ends the same interpreter
  (`finalize_subinterpreters`): the main thread hangs in `take_gil`, or the process crashes.

The test waits for the process to exit (the global pytest-timeout guards a hang).
"""

import os
import subprocess
import sys

APP = '''
from mipyre import Mipyre

app = Mipyre()


@app.get("/")
def root(req):
    return "ok"
'''

SCRIPT = '''
from mipyre import AbandonedWorkersError
from mipyre.testing import TestClient

import abandoned_workers_app

abandoned_workers_app.app.shutdown_timeout = 1e-9
client = TestClient(abandoned_workers_app.app)
client.__enter__()
try:
    client.close()
except AbandonedWorkersError as e:
    print("abandoned", len(e.workers), flush=True)
'''

PY_DROPPED = "a Py<T> was dropped on a thread that has no Python thread state"


def test_the_process_exits_cleanly_after_its_workers_were_abandoned(tmp_path):
    (tmp_path / "abandoned_workers_app.py").write_text(APP)
    script = tmp_path / "exit_after_abandon.py"
    script.write_text(SCRIPT)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(tmp_path), env.get("PYTHONPATH")]))
    done = subprocess.run(
        [sys.executable, str(script)], cwd=tmp_path, env=env, capture_output=True, text=True
    )
    abandoned = [line for line in done.stdout.splitlines() if line.startswith("abandoned ")]
    assert abandoned and int(abandoned[0].split()[1]) > 0, (done.stdout, done.stderr[-4000:])
    assert PY_DROPPED not in done.stderr, done.stderr[-4000:]
    assert done.returncode == 0, (done.returncode, done.stderr[-4000:])
