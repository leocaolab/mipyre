"""systemd socket activation (`Accept=no`), end to end: a server started with a listening socket at
descriptor 3 and `LISTEN_PID` / `LISTEN_FDS` naming it serves that socket instead of binding one.

The test plays systemd: it binds the socket, and a launcher moves it to descriptor 3 and sets
`LISTEN_PID` to its own pid before `exec`ing the app, so the variable names the server's pid (as
systemd sets it between fork and exec).

Why a real process: the facts are the kernel's — descriptor 3 being a listening socket this
process inherited across `exec`, and `LISTEN_PID` matching the pid it got. Parsing the variables is
tested as a pure function (`handed_count`, crates/transport/src/tcp/passed.rs); taking over a descriptor
needs the descriptor.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import textwrap
import urllib.request

import pytest

from tests._helpers import bound_port, kill_session, read_file

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="socket activation is Unix only")

APP = """
    from mipyre import Mipyre

    app = Mipyre()

    @app.get("/")
    def root(req):
        return "handed"

    app.run(host="127.0.0.1", port=0, workers=2)
"""


def _handed_socket() -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    s.listen(64)
    return s


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# argv: the handed descriptor, LISTEN_PID ("self" for the launcher's own pid, which exec keeps),
# the app script.
LAUNCHER = """
import os, sys
os.dup2(int(sys.argv[1]), 3)
os.environ["LISTEN_PID"] = str(os.getpid()) if sys.argv[2] == "self" else sys.argv[2]
os.execv(sys.executable, [sys.executable, sys.argv[3]])
"""


def _start(tmp_path, handed: socket.socket, listen_pid: str = "self", listen_fds: str = "1", env=None):
    script = tmp_path / "app.py"
    script.write_text(textwrap.dedent(APP))
    log = tmp_path / "server.log"
    with open(log, "w") as out:
        proc = subprocess.Popen(
            [sys.executable, "-c", LAUNCHER, str(handed.fileno()), listen_pid, str(script)],
            stdout=out,
            stderr=subprocess.STDOUT,
            pass_fds=(handed.fileno(),),
            start_new_session=True,
            env={**os.environ, "LISTEN_FDS": listen_fds, **(env or {})},
        )
    return proc, read_file(str(log))


def test_a_handed_socket_is_served_instead_of_binding_one(tmp_path):
    handed = _handed_socket()
    port = handed.getsockname()[1]
    proc, output = _start(tmp_path, handed)
    try:
        assert bound_port(output, proc) == port, output()
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=10) as r:
            assert r.read() == b"handed"
        assert "systemd handed over" in output(), output()
    finally:
        proc.terminate()
        kill_session(proc)
        handed.close()


def test_sockets_handed_to_another_process_are_not_taken(tmp_path):
    handed = _handed_socket()
    proc, output = _start(tmp_path, handed, listen_pid="1")
    try:
        assert bound_port(output, proc) != handed.getsockname()[1], output()
        assert "systemd handed over" not in output(), output()
    finally:
        proc.terminate()
        kill_session(proc)
        handed.close()


def test_a_malformed_listen_fds_stops_the_start_naming_it(tmp_path):
    handed = _handed_socket()
    proc, output = _start(tmp_path, handed, listen_fds="two")
    try:
        assert proc.wait(timeout=30) != 0, output()
        assert "LISTEN_FDS" in output() and "two" in output(), output()
    finally:
        kill_session(proc)
        handed.close()


def test_extra_ports_beside_handed_sockets_stop_the_start(tmp_path):
    handed = _handed_socket()
    proc, output = _start(tmp_path, handed, env={"MIPYRE_PLAIN_PORTS": str(_free_port())})
    try:
        assert proc.wait(timeout=30) != 0, output()
        assert "LISTEN_FDS" in output() and ".socket unit" in output(), output()
    finally:
        kill_session(proc)
        handed.close()
