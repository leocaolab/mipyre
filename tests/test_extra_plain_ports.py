"""`extra_plain_ports` / MIPYRE_PLAIN_PORTS: more plain listeners on one server.

A plain listener answers HTTP/1.1 and HTTP/2 with prior knowledge (h2c), which is what
a cleartext h2 port (HttpArena baseline-h2c / json-h2c on 8082) needs.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import textwrap
import urllib.request

import pytest

from tests._helpers import bound_port, poll_until, read_file

HOST = "127.0.0.1"

_RUN_PATHS = [
    ("tpc-gil", "gil", {}),
    ("tpc-subinterp", "subinterp", {}),
    ("pool-gil", "gil", {"MIPYRE_TPC": "0"}),
    ("pool-subinterp", "subinterp", {"MIPYRE_TPC": "0"}),
]

_APP = """
    import sys
    from mipyre import Mipyre

    app = Mipyre()

    @app.get("/")
    def root(req):
        return "hello"

    if __name__ == "__main__":
        port, extra = sys.argv[1:3]
        app.run(host="127.0.0.1", port=int(port), workers=2, extra_plain_ports=[int(extra)])
"""


def _unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


def _get(url: str) -> bytes:
    """The body `url` answers, once it does: a refused or reset attempt is a port not serving yet, looked at
    again, for no length of time (b/441 T2)."""

    def body() -> tuple[bytes] | None:
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                return (r.read(),)
        except OSError:
            return None

    return poll_until(body, timeout=None, what=f"{url} answering")[0]


@pytest.mark.parametrize("label,mode,env", _RUN_PATHS, ids=[p[0] for p in _RUN_PATHS])
def test_an_extra_plain_port_serves_http1_and_h2c(tmp_path, label, mode, env):
    script = tmp_path / "plain_app.py"
    script.write_text(textwrap.dedent(_APP))
    # `extra_plain_ports` refuses 0, so the extra port is one the kernel just handed out.
    extra = _unused_port()
    log_path = tmp_path / "server.log"
    # The "gil" paths serve every handler on the main interpreter: the app's runtime "main".
    full_env = {k: v for k, v in os.environ.items() if k != "MIPYRE_RUNTIME"}
    full_env.update(env)
    if mode == "gil":
        full_env["MIPYRE_RUNTIME"] = "main"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, str(script), "0", str(extra)],
            env=full_env, stdout=log, stderr=subprocess.STDOUT, text=True,
        )
    try:
        port = bound_port(read_file(str(log_path)), proc)
        assert _get(f"http://{HOST}:{port}/") == b"hello"
        assert _get(f"http://{HOST}:{extra}/") == b"hello"
        if shutil.which("curl") is not None:
            r = subprocess.run(
                ["curl", "-s", "--http2-prior-knowledge", "-w", " %{http_version}",
                 f"http://{HOST}:{extra}/"],
                capture_output=True, text=True, timeout=10,
            )
            assert r.stdout == "hello 2", (r.stdout, r.stderr)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    out = log_path.read_text(errors="replace")
    assert f"http://{HOST}:{extra}" in out, out
