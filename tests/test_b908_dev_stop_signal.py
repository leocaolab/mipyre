"""b/908: SIGTERM or SIGINT to `mipyre dev` (the reloader) stops the server it runs, and the
reloader ends with the server's exit status.

The reloader runs the server in a child process. Before b/908 SIGTERM took its default action on
the reloader alone: it died at once, and the child kept serving with ppid 1, holding the port. A
supervisor or an IDE that stops `mipyre dev` with SIGTERM leaked a server.

A real process tree and a real signal: what is tested is the OS delivering the signal to the
reloader alone (not to its process group, as a terminal's Ctrl-C would) and what the reloader then
does with its child.
"""

import os
import signal
import socket
import subprocess
import sys

import httpx
import pytest

from mipyre import _reload
from tests._helpers import bound_port, read_file

APP = r'''
import os

from mipyre import Mipyre

app = Mipyre()


@app.get("/pid")
def pid(req):
    return str(os.getpid())
'''


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


@pytest.mark.parametrize("stop", [signal.SIGTERM, signal.SIGINT], ids=["sigterm", "sigint"])
def test_a_stop_signal_to_mipyre_dev_alone_stops_its_server_and_frees_the_port(tmp_path, stop):
    (tmp_path / "b908app.py").write_text(APP)
    log_path = tmp_path / "dev.log"
    log = read_file(str(log_path))
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    env.pop(_reload.CHILD_ENV, None)
    with open(log_path, "w") as out:
        dev = subprocess.Popen(
            [sys.executable, "-m", "mipyre", "dev", "b908app:app", "--host", "127.0.0.1", "--port", "0"],
            cwd=tmp_path, stdout=out, stderr=subprocess.STDOUT, start_new_session=True, env=env,
        )
    try:
        port = bound_port(log, dev)
        server = int(httpx.get(f"http://127.0.0.1:{port}/pid", timeout=30).text)
        assert server != dev.pid, "the server runs in the reloader's child"

        # The signal to the reloader alone, as a supervisor's stop sends it.
        dev.send_signal(stop)
        code = dev.wait(timeout=60)
        server_left, port_held = _alive(server), not _port_free(port)
    finally:
        # Whatever is left of the session (the reloader, or a server it leaked) goes.
        try:
            os.killpg(dev.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass  # nothing left in the session: the case the test wants
        dev.wait()

    output = log()
    # The reloader waited for its child, so the child is reaped: not even a zombie is left.
    assert not server_left, f"the server (pid {server}) outlived mipyre dev:\n{output[-3000:]}"
    assert not port_held, f"port {port} is still held after mipyre dev exited:\n{output[-3000:]}"
    # The server stops gracefully on either signal and exits 0; the reloader ends the same way.
    assert code == 0, f"mipyre dev exited {code} after {stop.name}:\n{output[-3000:]}"


@pytest.mark.parametrize(
    ("returncode", "status"),
    [(0, 0), (1, 1), (-signal.SIGKILL, 128 + signal.SIGKILL), (-signal.SIGTERM, 128 + signal.SIGTERM)],
)
def test_the_reloader_exits_with_its_childs_status_a_signal_as_a_shell_reports_it(returncode, status):
    assert _reload.exit_status(returncode) == status
