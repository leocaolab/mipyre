"""runtime="process" lanes when every lane is lost: the wiring from a lane that cannot get a
worker to `serve` ending non-zero with the cause. What a lost lane or a failed host does to the
run (isolate or stop, by policy) is decided in `lane/health.rs` and tested there, on data."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import urllib.error
import urllib.request

import pytest

from tests._helpers import answers, bound_port, read_file

APP = os.path.join(os.path.dirname(__file__), "apps", "process_lane_fault_app.py")




def launch_command() -> list[str]:
    return [sys.executable, "-m", "mipyre", "run", f"{APP}:app", "--port", "0"]


class FaultServer:
    def __init__(self, tmp_path, **env):
        self.log_path = str(tmp_path / "server.log")
        full = dict(os.environ)
        full.update({k: str(v) for k, v in env.items()})
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                launch_command(), stdout=log, stderr=subprocess.STDOUT, start_new_session=True, env=full
            )
        self.read = read_file(self.log_path)
        self.port = bound_port(self.read, self.proc)
        answers(f"http://127.0.0.1:{self.port}/hello", self.proc, self.read)

    def get(self, path, timeout=10):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=timeout) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def stop(self):
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGINT)
            self.proc.wait(timeout=15)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.proc.wait(timeout=5)


def two_hosts(**extra) -> dict:
    # Two TPC threads (hosts) and two lanes: lane 0 is host 0's, lane 1 is host 1's.
    return dict(MIPYRE_WORKERS=2, MIPYRE_PROCESS_WORKERS=2, MIPYRE_PROCESS_TIMEOUT_MS=1500, **extra)


@pytest.fixture
def server(tmp_path):
    s = FaultServer(tmp_path, **two_hosts())
    yield s
    s.stop()


def test_with_the_template_gone_every_lane_breaks_and_the_server_exits_non_zero_with_the_cause(server):
    # The workers die with their template, and no replacement can be forked: every lane breaks.
    status, body = server.get("/lanes/template")
    assert status == 200, (status, body)
    os.kill(json.loads(body)["pid"], signal.SIGKILL)
    code = server.proc.wait(timeout=30)
    out = server.read()
    assert code != 0, out[-3000:]
    assert "the process lanes failed, so the server stopped" in out, out[-3000:]
    assert "respawn failed" in out, out[-3000:]
