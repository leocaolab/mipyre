"""runtime="process" lanes when every lane is lost: the wiring from a lane that cannot get a
worker to `serve` ending non-zero with the cause. What a lost lane or a failed host does to the
run (isolate or stop, by policy) is decided in `lane/health.rs` and tested there, on data."""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import urllib.error
import urllib.request

import pytest

from tests._helpers import answers, bound_port, lines_with, read_file

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
    return dict(MIPYRE_THREADS=2, MIPYRE_PROCESS_WORKERS=2, MIPYRE_PROCESS_TIMEOUT=1.5, **extra)


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


def fd_kinds(pid: int) -> list[str]:
    """What each fd of `pid` past the standard streams is: a lane's shared memory (`shm`), a `pipe`, a `socket` or
    `other`; on Linux by the link /proc gives, on macOS by the type lsof lists."""
    if sys.platform == "linux":
        links = [os.readlink(f"/proc/{pid}/fd/{fd}") for fd in os.listdir(f"/proc/{pid}/fd") if int(fd) > 2]
        prefixes = {"/memfd:mipyre-shm": "shm", "pipe:": "pipe", "socket:": "socket"}
        return [next((kind for p, kind in prefixes.items() if link.startswith(p)), "other") for link in links]
    listed = subprocess.run(["lsof", "-a", "-p", str(pid), "-d", "3-9999"], capture_output=True, text=True, check=True)
    types = {"PSXSHM": "shm", "PIPE": "pipe", "unix": "socket", "IPv4": "socket", "IPv6": "socket"}
    return [types.get(line.split()[4], "other") for line in listed.stdout.splitlines()[1:]]


def lane_memory_fds(pid: int) -> int:
    """How many fds of `pid` are a lane's shared memory: on Linux the memfds mipyre_shm names, on macOS the POSIX
    shared memory objects lsof lists."""
    return fd_kinds(pid).count("shm")


def test_the_template_holds_each_lanes_channel_and_maps_none(server):
    # The template hands each lane's channel to the lane's workers and never maps one: it holds one shared
    # memory object per lane (the current run's), and on Linux, where the mappings can be listed, none is mapped.
    status, body = server.get("/lanes/template")
    assert status == 200, (status, body)
    pid = json.loads(body)["pid"]
    assert lane_memory_fds(pid) == 2, server.read()[-3000:]
    if sys.platform == "linux":
        with open(f"/proc/{pid}/maps") as maps:
            mapped = [line for line in maps if "mipyre-shm" in line]
        assert mapped == [], mapped


def tcp_sockets(pid: int) -> list[str]:
    """The TCP sockets `pid` holds, each as the kernel names it: on Linux the inodes /proc lists in its TCP tables, on
    macOS the addresses lsof lists."""
    if sys.platform == "linux":
        held = {os.readlink(f"/proc/{pid}/fd/{fd}")[len("socket:["):-1] for fd in os.listdir(f"/proc/{pid}/fd")}
        tcp: set[str] = set()
        for table in ("/proc/net/tcp", "/proc/net/tcp6"):
            with open(table) as rows:
                next(rows)  # the header
                tcp.update(row.split()[9] for row in rows)
        return sorted(held & tcp)
    # lsof exits 1 when it lists none.
    listed = subprocess.run(["lsof", "-nP", "-a", "-p", str(pid), "-iTCP", "-Fn"], capture_output=True, text=True)
    assert listed.returncode in (0, 1), listed.stdout + listed.stderr
    return [line[1:] for line in listed.stdout.splitlines() if line.startswith("n")]


def test_the_template_holds_no_tcp_socket(server):
    # The template is forked before the server binds its ports, and a worker is forked from it without an exec, so
    # close-on-exec could not keep the server's listening sockets out of either: only that order does.
    status, body = server.get("/lanes/template")
    assert status == 200, (status, body)
    assert tcp_sockets(json.loads(body)["pid"]) == [], server.read()[-3000:]


def test_a_worker_holds_only_its_own_lanes_bell_and_hands_no_channel_fd_to_a_program_it_runs(server):
    # A worker maps its lane and closes the shared memory; of the run's fds it keeps only its own lane's wake pipe
    # end, and neither another lane's channel nor the template's socket. Its pipes and sockets past the standard
    # streams are close-on-exec, so a program its app runs gets none of them.
    status, body = server.get("/lanes/worker-fds")
    assert status == 200, (status, body)
    worker = json.loads(body)
    kinds = fd_kinds(worker["pid"])
    assert [kind for kind in kinds if kind != "other"] == ["pipe"], kinds
    assert worker["inheritable"] == [], worker


def test_a_killed_worker_is_logged_once_with_its_exit_and_counted_on_the_metrics_page(server):
    status, body = server.get("/hello")
    assert status == 200, (status, body)
    pid = json.loads(body)["pid"]

    os.kill(pid, signal.SIGKILL)

    [line] = lines_with(server.read, f"worker {pid} died", timeout=30)
    assert "killed by signal 9" in line, line
    assert "after serving" in line, line
    status, page = server.get("/metrics")
    assert status == 200, (status, page)
    assert re.search(r'^mipyre_workers_died_total\{runtime="process"\} 1$', page.decode(), re.MULTILINE), page
    assert re.search(r'^mipyre_workers_died_total\{runtime="subinterp"\} 0$', page.decode(), re.MULTILINE), page
