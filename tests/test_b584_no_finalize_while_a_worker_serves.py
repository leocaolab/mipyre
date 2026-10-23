"""b/584: a process whose abandoned worker still serves its interpreter exits without finalizing.

`Py_Finalize` (CPython 3.14) ends every sub-interpreter left and deletes its threads' states. A
worker thread left running past the shutdown grace, in a handler that keeps sleeping, restores
its thread state after every sleep inside CPython: once finalization deleted it, that restore is
a use-after-free. Unlike `app.run()`, a `TestClient` caller goes on after `AbandonedWorkersError`, so
the check is at exit: a thread still owning its interpreter then makes the process exit 1 with
no finalization. The b/496 test is the other side: threads only on their way out are waited for,
and the process finalizes and exits 0.

The handler says it started through a pipe, so the script waits for that event, not a timer; the
test waits for the process to exit (the global pytest-timeout guards a hang).
"""

import os
import subprocess
import sys

APP = '''
import os
import time

from mipyre import Mipyre

app = Mipyre()


@app.get("/serving")
def serving(req):
    os.write(int(os.environ["B584_STARTED_FD"]), b"s")
    while True:
        try:
            time.sleep(0.01)
        except BaseException:
            # Ignores the request budget's interrupt too: this handler never returns.
            pass
'''

SCRIPT = '''
import os
import threading

read_started, write_started = os.pipe()
os.environ["B584_STARTED_FD"] = str(write_started)

from mipyre import AbandonedWorkersError
from mipyre.testing import TestClient

import b584_app

b584_app.app.shutdown_timeout = 0.05
client = TestClient(b584_app.app)
client.__enter__()


def never_answered():
    try:
        client.get("/serving")
    except Exception:
        pass


threading.Thread(target=never_answered, daemon=True).start()
os.read(read_started, 1)
try:
    client.close()
except AbandonedWorkersError as e:
    print("b584-abandoned", len(e.workers), flush=True)
'''


def test_a_worker_still_serving_at_exit_keeps_the_process_from_finalizing(tmp_path):
    (tmp_path / "b584_app.py").write_text(APP)
    script = tmp_path / "exit_while_serving.py"
    script.write_text(SCRIPT)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(tmp_path), env.get("PYTHONPATH")]))
    done = subprocess.run(
        [sys.executable, str(script)], cwd=tmp_path, env=env, capture_output=True, text=True
    )
    abandoned = [line for line in done.stdout.splitlines() if line.startswith("b584-abandoned ")]
    assert abandoned and int(abandoned[0].split()[1]) > 0, (done.stdout, done.stderr[-4000:])
    assert "still in use at exit" in done.stderr, done.stderr[-4000:]
    assert "exiting without finalization" in done.stderr, done.stderr[-4000:]
    assert done.returncode == 1, (done.returncode, done.stderr[-4000:])


def test_one_thread_running_python_at_exit_is_a_refusal_and_none_is_not():
    from mipyre._finalize import refusal_at_exit

    assert refusal_at_exit(0, ["w-1"]) is None
    why = refusal_at_exit(1, ["w-1", "w-2"])
    assert why is not None and "still in use at exit" in why and "w-1, w-2" in why
