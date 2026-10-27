"""Start-time IO checks happen before the server is ready: a check that
fails fails the whole start, never a worker later."""

import os
import socket
import subprocess
import sys
import textwrap

import pytest

from mipyre import Mipyre
from mipyre.engine import ServerFlags
from mipyre.testing import TestClient
from tests._helpers import free_port_below_ephemeral, port_free

# Module level: a sub-interpreter worker runs this file and serves the app it registers.
app = Mipyre()


@app.get("/isolated", runtime="isolated")
def isolated(req):
    return "ok"


def test_an_isolate_dir_other_users_can_write_fails_the_start(tmp_path):
    shared = tmp_path / "shared"
    shared.mkdir()
    shared.chmod(0o777)
    with pytest.raises(Exception, match="writable by other users") as raised:
        TestClient(app, config=ServerFlags(isolate_dir=str(shared), threads=1))
    assert type(raised.value).__name__ == "PrivateCopyError", raised.value


def test_a_private_isolate_dir_starts(tmp_path):
    own = tmp_path / "own"
    with TestClient(app, config=ServerFlags(isolate_dir=str(own), threads=1)) as c:
        assert c.get("/isolated").text == "ok"
    assert oct(own.stat().st_mode & 0o777) == oct(0o700)


# A server whose start-time IO fails exits non-zero with the error's own
# words, never says it is listening, and leaves its port free.

APPS = os.path.join(os.path.dirname(__file__), "apps")


def _failed_start(argv: list[str], env: dict[str, str], cwd) -> str:
    """Runs a server that must not start, returning what it wrote."""
    full = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    full.update(env)
    done = subprocess.run(argv, capture_output=True, text=True, timeout=120, env=full, cwd=cwd)
    out = done.stdout + done.stderr
    assert done.returncode != 0, out[-3000:]
    assert "Listening on" not in out, out[-3000:]
    return out


def _script(tmp_path, body: str) -> str:
    path = tmp_path / "app.py"
    path.write_text(textwrap.dedent(body))
    return str(path)


_MAIN_APP = """
    from mipyre import Mipyre

    app = Mipyre(runtime="main")

    @app.get("/")
    def root(req):
        return "ok"
    MOUNT
    app.run()
"""


def test_a_certificate_and_a_key_that_do_not_match_fail_the_start(tmp_path):
    for name in ("a", "b"):
        subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", f"{name}.key",
             "-out", f"{name}.crt", "-days", "1", "-subj", "/CN=localhost"],
            check=True, capture_output=True, cwd=tmp_path,
        )
    port = free_port_below_ephemeral()
    env = {"MIPYRE_PORT": str(port), "MIPYRE_TLS_CERT": "a.crt", "MIPYRE_TLS_KEY": "b.key"}
    out = _failed_start([sys.executable, _script(tmp_path, _MAIN_APP.replace("MOUNT", ""))], env, tmp_path)
    assert 'tls_key (MIPYRE_TLS_KEY): the key in "b.key" is not the private key of the certificate in "a.crt"' in out, (
        out[-3000:]
    )
    assert port_free(port)


def test_a_static_mount_whose_root_does_not_exist_fails_the_start(tmp_path):
    port = free_port_below_ephemeral()
    mount = 'app.static("/s", "/nonexistent/static/root")'
    out = _failed_start(
        [sys.executable, _script(tmp_path, _MAIN_APP.replace("MOUNT", mount))], {"MIPYRE_PORT": str(port)}, tmp_path
    )
    assert 'static directory "/nonexistent/static/root" cannot be resolved' in out, out[-3000:]
    assert port_free(port)


def test_a_port_in_use_fails_the_start(tmp_path):
    # A listener without SO_REUSEPORT: the server's SO_REUSEPORT bind cannot join it.
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        port = taken.getsockname()[1]
        out = _failed_start(
            [sys.executable, _script(tmp_path, _MAIN_APP.replace("MOUNT", ""))], {"MIPYRE_PORT": str(port)}, tmp_path
        )
    assert "Address already in use" in out, out[-3000:]


def test_a_sub_interpreter_that_raises_importing_the_app_fails_the_start(tmp_path):
    port = free_port_below_ephemeral()
    script = _script(
        tmp_path,
        """
        from mipyre import Mipyre
        from mipyre.engine import _in_worker, ServerFlags

        if _in_worker():
            raise RuntimeError("the sub-interpreter's own import error")

        app = Mipyre()

        @app.get("/")
        def root(req):
            return "ok"

        if __name__ == "__main__":
            app.run(ServerFlags(threads=1))
        """,
    )
    out = _failed_start([sys.executable, script], {"MIPYRE_PORT": str(port)}, tmp_path)
    assert "raised while the worker started: RuntimeError: the sub-interpreter's own import error" in out, out[-3000:]
    assert port_free(port)


def test_a_process_worker_that_raises_importing_the_app_fails_the_start(tmp_path):
    port = free_port_below_ephemeral()
    app = os.path.join(APPS, "process_lane_worker_import_raises_app.py")
    env = {"MIPYRE_PROCESS_WORKERS": "1", "LANE_TEST_MARKER": str(tmp_path / "marker")}
    out = _failed_start([sys.executable, "-m", "mipyre", "run", f"{app}:app", "--port", str(port)], env, tmp_path)
    assert "RuntimeError: the lane worker's own import error" in out, out[-3000:]
    assert port_free(port)
