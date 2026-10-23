"""TPC teardown, end to end (b/175).

A TPC thread that a stop leaves running past the join deadline is kept with the app's abandoned
workers and named, and the process exits without finalizing. That exit (``os._exit``, no
``Py_Finalize``) is the process's own, so only a real process can show it; the join deadline is
the app's ``shutdown_timeout``, set short here. The drain's timeout and its counters are Rust
tests (``mipyre_tpc::threads`` and ``serve::tpc``), b/301.
"""

from __future__ import annotations

import os
import re
import signal
import socket
import subprocess
import sys
import textwrap
import threading

import httpx

from tests._helpers import read_file, settle


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_listening(port: int, timeout: float = 90) -> None:
    """Waits until something accepts on ``port`` (the server is up)."""

    def up() -> bool:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            return False

    settle(up, timeout=timeout, interval=0.02)
    assert up(), f"nothing listens on {port}"


def _spawn(tmp_path, source: str, env_extra: dict[str, str]) -> tuple[subprocess.Popen, str]:
    script = tmp_path / "app.py"
    script.write_text(textwrap.dedent(source))
    log_path = tmp_path / "server.log"
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    env.update(env_extra)
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=str(tmp_path),
            start_new_session=True,
        )
    return proc, str(log_path)


def _wait_exit(proc: subprocess.Popen, timeout: float = 90) -> int:
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait(timeout=5)
        raise AssertionError("the server did not exit")


def test_a_tpc_thread_abandoned_past_the_join_deadline_exits_without_finalizing(tmp_path):
    """The exit without `Py_Finalize` (`os._exit` with a live sub-interpreter) is the process's own end,
    so only a real process run to its exit code shows it."""
    port = _free_port()
    proc, log_path = _spawn(
        tmp_path,
        f"""
        import threading
        from mipyre import Mipyre

        app = Mipyre()
        # The join deadline after the stop: the stuck thread is given up on this soon.
        app.shutdown_timeout = 0.2

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/stuck")
        def stuck(req):
            print("HANDLER-STUCK", flush=True)
            # Held here until the process exits: the TPC thread never returns to its loop.
            threading.Event().wait()

        if __name__ == "__main__":
            app.run(host="127.0.0.1", port={port})
            print("RETURNED", flush=True)
        """,
        {},
    )
    read = read_file(log_path)
    try:
        _wait_listening(port)
        threading.Thread(
            target=lambda: httpx.get(f"http://127.0.0.1:{port}/stuck", timeout=120),
            daemon=True,
        ).start()
        settle(read, lambda text: "HANDLER-STUCK" in text, timeout=60)
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        rc = _wait_exit(proc)
    finally:
        if proc.poll() is None:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    log = read()
    # The stop went down the abandoned path: the run raised AbandonedWorkersError, naming the TPC
    # thread, and `run()` exited through `os._exit` (no finalization, no abort).
    assert rc == 1, log[-3000:]
    assert "exiting without finalization" in log, log[-3000:]
    assert re.search(r"worker\(s\) mipyre-tpc-\d+.* did not stop", log), log[-3000:]
    assert "RETURNED" not in log, log[-3000:]
    assert "Fatal Python error" not in log and "Abort" not in log, log[-3000:]
    assert "panicked at" not in log, log[-3000:]
