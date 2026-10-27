"""Each worker's sub-interpreter is created on the thread that serves it.

A worker's interpreter is created by running the app's script in it, so the script's module code
runs on the thread that creates the interpreter. The script notes that thread's native id at
module level; its handler answers with that id and the id of the thread serving the request.
Created on its serving thread means the two are the same, and neither is the main thread (whose
id the main interpreter's run of the same module code writes to a file). An interpreter created
on the main thread and handed over to its thread would answer with the main thread's id as its
creator. Both runtimes are covered: the sub-interpreter pool (`MIPYRE_TPC=0`) and the TPC
threads.

Why end to end: which OS thread runs an interpreter's creation is a fact of the running process
(the native thread id the module code reads), not a value any function returns; it can only be
observed from inside the created interpreter.
"""

import os
import signal
import subprocess
import sys

import httpx
import pytest

from tests._helpers import bound_port, read_file, set_subinterpreters

SCRIPT = r'''
import os
import threading

from mipyre import Mipyre, ServerFlags

CREATED_ON = threading.get_native_id()

if __name__ == "__main__":
    with open(os.environ["B252_MAIN_THREAD_FILE"], "w") as f:
        f.write(str(CREATED_ON))

app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))


@app.get("/")
def index(req):
    return {"created_on": CREATED_ON, "served_on": threading.get_native_id()}


if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0))
'''

POOL = {"MIPYRE_TPC": "0"}
TPC = {"MIPYRE_TPC": "1"}
REQUESTS = 20


@pytest.mark.parametrize("runtime", [POOL, TPC], ids=["pool", "tpc"])
def test_each_worker_is_created_on_the_thread_that_serves_it(tmp_path, runtime):
    script = tmp_path / "app.py"
    script.write_text(SCRIPT)
    main_thread_file = tmp_path / "main_thread"
    log_path = tmp_path / "app.log"
    env = {**os.environ, **runtime, "B252_MAIN_THREAD_FILE": str(main_thread_file)}
    env.pop("MIPYRE_TEST_MODE", None)
    set_subinterpreters(env, "3")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, str(script)], stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True, env=env,
        )
    try:
        port = bound_port(read_file(str(log_path)), proc)
        main_thread = int(main_thread_file.read_text())
        # Fresh connections, so the requests spread over the workers.
        for _ in range(REQUESTS):
            body = httpx.get(f"http://127.0.0.1:{port}/", headers={"Connection": "close"}, timeout=10).json()
            assert body["created_on"] == body["served_on"], (
                f"a worker created on thread {body['created_on']} served on thread "
                f"{body['served_on']} (main thread {main_thread})"
            )
            assert body["created_on"] != main_thread, (
                f"a worker was created on the main thread {main_thread}, not on its own"
            )
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=30)
