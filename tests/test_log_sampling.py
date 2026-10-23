"""Sanity tests for the access-log sampling knobs.

Doesn't try to capture actual log output — that needs cooperation from
tracing-subscriber + capture infra. These tests just exercise the
Rust-side wiring (enable_access_log accepts sample / always_log_status,
set_request_log_sampling is callable, server starts cleanly with the
options set).
"""

import os
import subprocess
import sys
import textwrap

import pytest

from mipyre import Mipyre


def _serve_in_own_process(module: str, requests: int) -> None:
    """Serves ``module``'s app in its own process: it turns the access log on, and a process has
    one log configuration (b/335 D14), which another test's app in this one may have set."""
    script = f"""
from tests.{module} import app
from mipyre.testing import TestClient
with TestClient(app) as c:
    for _ in range({requests}):
        assert c.get("/h").status_code == 200
"""
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stderr[-2000:]


def test_enable_access_log_with_sample_arg():
    # Its own module: a worker serves one app per module.
    _serve_in_own_process("apps.log_sampling_sample_arg", 5)


def test_set_request_log_sampling_directly():
    # Its own module: a worker serves one app per module.
    _serve_in_own_process("apps.log_sampling_direct_setter", 1)


def test_sample_zero_is_rejected():
    """sample_n=0 would divide by zero — Rust rejects it with ValueError."""
    app = Mipyre()
    with pytest.raises(ValueError, match="sample_n must be at least 1"):
        app._engine.set_request_log_sampling(0, None)


_SAMPLED_APP = """
    from mipyre import Mipyre
    from mipyre.testing import TestClient

    app = Mipyre(runtime="main")
    app.enable_access_log(sample=1000)

    @app.get("/x")
    def x(req):
        return "ok"

    with TestClient(app) as c:
        for _ in range(200):
            c.get("/x")
"""


def _access_lines(tmp_path, **env: str) -> int:
    """Serve ``_SAMPLED_APP`` in its own process (logging is set up once per process) and
    count its access lines."""
    script = tmp_path / "app_script.py"
    script.write_text(textwrap.dedent(_SAMPLED_APP))
    full = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    full.update(env)
    proc = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, env=full, timeout=120, cwd=tmp_path
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    return sum("mipyre::access" in line for line in (proc.stdout + proc.stderr).splitlines())


def test_mipyre_log_keeps_the_apps_access_log_sampling(tmp_path):
    """``enable_access_log(sample=1000)`` then ``MIPYRE_LOG=1``: the same 200 requests log the
    same number of access lines as without the variable."""
    assert _access_lines(tmp_path, MIPYRE_LOG="1") == _access_lines(tmp_path)
