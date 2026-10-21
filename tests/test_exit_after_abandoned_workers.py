"""b/496: a process exits cleanly after its server left its workers running past the grace period.

The script gives its server a grace period no thread can meet (1 ns), so `TestClient.close`
abandons every TPC thread while each is still on its way out, and then the process exits at
once: the threads end their workers while the process finalizes. Two faults met there:

- a thread that was let go dropped the last reference to its copy of the site after its worker
  ended, with no Python thread state: PyO3 panicked on the first `Py<T>` of the route table and
  printed "a Py<T> was dropped on a thread that has no Python thread state" for the rest;
- a thread's `Py_EndInterpreter` ran while `Py_Finalize` ended the same interpreter
  (`finalize_subinterpreters`): the main thread hung in `take_gil`, or the process crashed.

Before the fix the script crashed (SIGSEGV) in 20 of 20 runs on a Linux x86-64 machine, each with the
message above. The test waits for the process to exit (the global pytest-timeout guards a hang).
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
from mipyre.app import WorkersAbandoned
from mipyre.testing import TestClient

import b496_app

b496_app.app.shutdown_timeout = 1e-9
client = TestClient(b496_app.app, port=None)
client.__enter__()
try:
    client.close()
except WorkersAbandoned as e:
    print("b496-abandoned", len(e.workers), flush=True)
'''

PY_DROPPED = "a Py<T> was dropped on a thread that has no Python thread state"


def test_the_process_exits_cleanly_after_its_workers_were_abandoned(tmp_path):
    (tmp_path / "b496_app.py").write_text(APP)
    script = tmp_path / "exit_after_abandon.py"
    script.write_text(SCRIPT)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(tmp_path), env.get("PYTHONPATH")]))
    done = subprocess.run(
        [sys.executable, str(script)], cwd=tmp_path, env=env, capture_output=True, text=True
    )
    abandoned = [line for line in done.stdout.splitlines() if line.startswith("b496-abandoned ")]
    assert abandoned and int(abandoned[0].split()[1]) > 0, (done.stdout, done.stderr[-4000:])
    assert PY_DROPPED not in done.stderr, done.stderr[-4000:]
    assert done.returncode == 0, (done.returncode, done.stderr[-4000:])
