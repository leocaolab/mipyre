"""The process-lane template takes the server's end closing as "the server is gone" however
the kernel reports it — EOF, ECONNRESET on Linux when the server left frames unread, EPIPE on a
report — and ends quietly, never with a traceback."""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import textwrap
import urllib.request
from typing import IO

from mipyre._process_lane.template import recv_commands, send_report

from tests._helpers import kill_session, listening_ports


def test_the_server_closing_with_template_frames_unread_reads_as_gone():
    template_end, server_end = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    with template_end:
        server_end.sendall(b"command")
        template_end.sendall(b"a report the server never reads")
        server_end.close()
        assert recv_commands(template_end) == b"command"
        assert recv_commands(template_end) is None


def test_the_server_closing_with_nothing_unread_reads_as_gone():
    template_end, server_end = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    with template_end:
        server_end.close()
        assert recv_commands(template_end) is None


def test_a_report_to_a_closed_server_says_it_is_gone():
    template_end, server_end = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    with template_end:
        assert send_report(template_end, b"a report")
        server_end.close()
        assert not send_report(template_end, b"a report")


APP = """
from mipyre import Mipyre

app = Mipyre()


@app.get("/hello", runtime="process")
def hello(req):
    return "hello"
"""

SCRIPT = """
from mipyre.testing import TestClient, TestServer

from server_gone_app import app

with TestServer(app) as server, TestClient(server) as c:
    assert c.get("/hello").status_code == 200
"""


def test_a_test_process_that_used_a_process_route_exits_without_a_template_traceback(tmp_path):
    """The template and its workers share the test process's stderr: `run` returns once every one
    of them has closed it, so their last words are in `done.stderr`."""
    (tmp_path / "server_gone_app.py").write_text(textwrap.dedent(APP))
    (tmp_path / "server_gone_script.py").write_text(textwrap.dedent(SCRIPT))
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(tmp_path), os.environ.get("PYTHONPATH")]))
    env["MIPYRE_PROCESS_WORKERS"] = "2"
    done = subprocess.run(
        [sys.executable, str(tmp_path / "server_gone_script.py")],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120,
    )
    assert done.returncode == 0, done.stderr
    assert "Traceback" not in done.stderr, done.stderr


def _banner_and_port(output: IO[str]) -> tuple[str, int]:
    """The lines `output` gave up to and with the first "Listening on", and its port."""
    banner = ""
    for line in output:
        banner += line
        ports = listening_ports(line)
        if ports:
            return banner, ports[0]
    raise RuntimeError(f"mipyre run ended before it listened:\n{banner[-4000:]}")


def test_ctrl_c_on_mipyre_run_with_a_process_lane_in_use_exits_0_without_a_traceback(tmp_path):
    """Ctrl-C sends SIGINT to the whole foreground group: the server, the template and its workers.
    The output is read to its end, which comes once every one of them has exited, so the
    template's last words are in it."""
    (tmp_path / "server_gone_app.py").write_text(textwrap.dedent(APP))
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    env["PYTHONUNBUFFERED"] = "1"
    run = subprocess.Popen(
        [sys.executable, "-m", "mipyre", "run", "server_gone_app:app", "--host", "127.0.0.1", "--port", "0",
         "--process-workers", "2"],
        cwd=tmp_path, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        start_new_session=True,
    )
    try:
        banner, port = _banner_and_port(run.stdout)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/hello") as answer:
            assert answer.status == 200
        os.killpg(run.pid, signal.SIGINT)
        output = banner + run.stdout.read()
        code = run.wait()
    finally:
        kill_session(run)
        run.stdout.close()

    assert code == 0, output
    for last_words in ("Traceback", "ConnectionResetError", "BrokenPipeError"):
        assert last_words not in output, output
