"""WebSocket limits and message typing, static-file errors and percent-decoding, logging level
mapping and config errors, named metrics.
"""

import asyncio
import http.client
import os
import signal
import subprocess
import sys
import tempfile
import textwrap

import pytest

from tests._helpers import answers, bound_port, read_file

PYTHON = sys.executable


MIB = 1024 * 1024


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _run_script(body: str, timeout: float = 20) -> subprocess.CompletedProcess:
    """Run a Python snippet in a fresh process (tracing's global subscriber and the
    metrics statics are process-wide, so each case needs its own process)."""
    return subprocess.run(
        [PYTHON, "-c", textwrap.dedent(body)],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


class _Server:
    """`script` in a subprocess; the script binds port 0, `port` is the one it bound."""

    def __init__(self, script: str):
        fd, self.path = tempfile.mkstemp(prefix="mipyre_limits_", suffix=".py")
        with os.fdopen(fd, "w") as f:
            f.write(textwrap.dedent(script))
        self.log_path = self.path + ".log"
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                [PYTHON, self.path],
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        try:
            self.port = bound_port(read_file(self.log_path), self.proc)
        except RuntimeError:
            raise RuntimeError(f"server did not start:\n{self.stop()}") from None
        try:
            answers(f"http://127.0.0.1:{self.port}/health", self.proc, read_file(self.log_path))
        except RuntimeError:
            raise RuntimeError(f"server did not start:\n{self.stop()}") from None

    def stop(self) -> str:
        if self.proc.poll() is None:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
        self.proc.wait(timeout=10)
        out = read_file(self.log_path)()
        os.unlink(self.path)
        os.unlink(self.log_path)
        return out


# ---------------------------------------------------------------------------
# WebSocket limits and typed messages
# ---------------------------------------------------------------------------

WS_SERVER = f"""
from mipyre import Mipyre, ServerFlags

app = Mipyre(runtime="main")

@app.websocket("/echo")
async def echo(ws):
    while True:
        msg = await ws.recv()
        if msg is None:
            break
        await ws.send(f"echo: {{len(msg)}}")

@app.websocket("/strict")
async def strict(ws):
    while True:
        try:
            msg = await ws.recv()
        except TypeError:
            data = await ws.recv_bytes()
            await ws.send(f"binary:{{len(data)}}")
            continue
        if msg is None:
            break
        await ws.send(f"text:{{msg}}")

@app.websocket("/kinds")
async def kinds(ws):
    async for msg in ws:
        await ws.send(type(msg).__name__)

@app.websocket("/big-send")
async def big_send(ws):
    await ws.recv()
    try:
        await ws.send("x" * (2 * {MIB}))
        await ws.send("sent")
    except ValueError as e:
        await ws.send(f"ValueError: {{e}}")

@app.get("/health")
def health(req):
    return {{"ok": True}}

if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0))
"""


@pytest.fixture(scope="module")
def ws_server():
    s = _Server(WS_SERVER)
    yield s
    s.stop()


@pytest.mark.asyncio
async def test_ws_default_max_message_size_closes_with_1009(ws_server):
    """The default cap is not tungstenite's 64 MiB: a 2 MiB message is refused."""
    import websockets

    # No `async with`: the server closes this connection, and re-closing it from the
    # client's __aexit__ while its send is being torn down trips a websockets/asyncio
    # teardown race (`abort` on a transport whose loop is already gone).
    ws = await websockets.connect(f"ws://127.0.0.1:{ws_server.port}/echo", max_size=None)
    # The server may close while the client is still writing, so send can raise too.
    with pytest.raises(websockets.ConnectionClosed) as info:
        await ws.send("y" * (2 * MIB))
        await asyncio.wait_for(ws.recv(), timeout=5)
    await asyncio.wait_for(ws.wait_closed(), timeout=5)
    assert info.value.rcvd is not None and info.value.rcvd.code == 1009, info.value


@pytest.mark.asyncio
async def test_ws_small_message_still_echoes(ws_server):
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{ws_server.port}/echo") as ws:
        await ws.send("z" * 1000)
        assert await asyncio.wait_for(ws.recv(), timeout=5) == "echo: 1000"


@pytest.mark.asyncio
async def test_ws_send_over_max_message_size_raises_value_error(ws_server):
    """The outgoing cap counts bytes, not messages: an oversized send raises."""
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{ws_server.port}/big-send", max_size=None) as ws:
        await ws.send("go")
        reply = await asyncio.wait_for(ws.recv(), timeout=5)
    assert reply.startswith("ValueError: "), reply[:200]
    assert str(2 * MIB) in reply


@pytest.mark.asyncio
async def test_ws_recv_does_not_drop_binary(ws_server):
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{ws_server.port}/strict") as ws:
        await ws.send(b"\x01\x02\x03")
        await ws.send("hi")
        assert await asyncio.wait_for(ws.recv(), timeout=5) == "binary:3"
        assert await asyncio.wait_for(ws.recv(), timeout=5) == "text:hi"


@pytest.mark.asyncio
async def test_ws_recv_message_returns_str_or_bytes(ws_server):
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{ws_server.port}/kinds") as ws:
        await ws.send(b"\x00")
        assert await asyncio.wait_for(ws.recv(), timeout=5) == "bytes"
        await ws.send("t")
        assert await asyncio.wait_for(ws.recv(), timeout=5) == "str"


def test_ws_limit_setters_reject_bad_values():
    from mipyre import Mipyre

    app = Mipyre()
    with pytest.raises(ValueError):
        app.max_websocket_connections = 0
    with pytest.raises(ValueError):
        app.max_websocket_message_size = 0
    with pytest.raises(TypeError):
        app.max_websocket_message_size = "1MB"


def test_ws_keepalive_settings_default_to_20_s_take_seconds_and_refuse_zero():
    from mipyre import Mipyre

    app = Mipyre()
    assert (app.websocket_ping_interval, app.websocket_ping_timeout) == (20.0, 20.0)
    app.websocket_ping_interval = 0.5
    app.websocket_ping_timeout = 1.5
    assert (app.websocket_ping_interval, app.websocket_ping_timeout) == (0.5, 1.5)
    for setting in ("websocket_ping_interval", "websocket_ping_timeout"):
        with pytest.raises(ValueError, match=setting):
            setattr(app, setting, 0)


def test_max_body_size_takes_zero_and_refuses_one_below_or_a_non_int():
    from mipyre import Mipyre

    app = Mipyre()
    app.max_body_size = 0
    assert app.max_body_size == 0
    with pytest.raises(ValueError, match="max_body_size must be a number of bytes, 0 or more, got -1"):
        app.max_body_size = -1
    for bad in (True, 1.0, "1"):
        with pytest.raises(TypeError, match="max_body_size must be int"):
            app.max_body_size = bad
    assert app.max_body_size == 0


def test_a_time_limit_takes_int_seconds_and_refuses_a_bool():
    from mipyre import Mipyre

    app = Mipyre()
    app.body_grace = 7
    assert app.body_grace == 7.0
    with pytest.raises(TypeError, match="body_grace must be int or float seconds, got bool"):
        app.body_grace = True
    with pytest.raises(TypeError, match="body_min_rate must be int, got float"):
        app.body_min_rate = 1024.0


def test_request_timeout_is_per_app_and_rejects_what_is_no_duration():
    from mipyre import Mipyre

    app, other = Mipyre(), Mipyre()
    assert app.request_timeout == 30.0
    app.request_timeout = 1.5
    assert (app.request_timeout, other.request_timeout) == (1.5, 30.0)
    for bad in (0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="request_timeout"):
            app.request_timeout = bad
    for bad in ("30", True, None):
        with pytest.raises(TypeError, match="request_timeout"):
            app.request_timeout = bad
    assert app.request_timeout == 1.5


# ---------------------------------------------------------------------------
# Static files. What a request path maps to and which status a refusal is are
# mipyre_http's (plan/static_plan.rs, io/static_files/tests.rs: encoded traversal, NUL,
# an absolute segment, a symlink loop as 500, a missing file); here, the wiring through an
# app, and the permission case Rust does not test.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def static_tree():
    base = tempfile.mkdtemp(prefix="mipyre_static_")
    root = os.path.join(base, "public")
    os.makedirs(root)
    with open(os.path.join(base, "secret.txt"), "w") as f:
        f.write("TOP-SECRET")
    with open(os.path.join(root, "my file.txt"), "w") as f:
        f.write("spaced")
    locked = os.path.join(root, "locked.txt")
    with open(locked, "w") as f:
        f.write("no read permission")
    os.chmod(locked, 0)
    yield base, root
    os.chmod(locked, 0o600)


@pytest.fixture(scope="module")
def static_server(static_tree):
    from mipyre.testing import TestClient, TestServer

    _, root = static_tree
    # The app's module reads its root from here, in main and in every worker.
    os.environ["MIPYRE_TEST_STATIC_ROOT"] = root
    from tests.apps.static_root_from_env import app

    with TestServer(app) as server, TestClient(server) as c:
        yield c
    del os.environ["MIPYRE_TEST_STATIC_ROOT"]


def _raw_get(port: int, path: str) -> tuple[int, bytes]:
    """Send the path byte-for-byte: http clients may normalise `%2e%2e`."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def test_static_percent_decodes_the_path(static_server):
    status, body = _raw_get(static_server.port, "/static/my%20file.txt")
    assert (status, body) == (200, b"spaced")


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file permissions")
def test_static_permission_denied_is_403_not_404(static_server):
    status, _ = _raw_get(static_server.port, "/static/locked.txt")
    assert status == 403


def test_static_root_must_be_a_directory(static_tree):
    from mipyre import Mipyre

    base, _ = static_tree
    with pytest.raises(ValueError, match="not a directory"):
        Mipyre().static("/s", os.path.join(base, "secret.txt"))


def test_static_prefix_must_start_with_slash(static_tree):
    from mipyre import Mipyre

    _, root = static_tree
    with pytest.raises(ValueError, match="must start with '/'"):
        Mipyre().static("assets", root)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_BRIDGE = """
import logging
from mipyre.engine import flush_logs, init_logger
from mipyre._log.bridge import install_root_handler
from mipyre.engine import _python_log_level, emit_python_log
"""


def test_logging_custom_level_between_info_and_warning_is_kept():
    """A custom level maps by its number, not its `levelname`, so it is not dropped as TRACE."""
    r = _run_script(_BRIDGE + """
init_logger("INFO", False, "text")
install_root_handler(emit_python_log, _python_log_level())
logging.addLevelName(25, "NOTICE")
logging.getLogger("custom_level").log(25, "custom-level-25")
logging.getLogger("custom_level").log(5, "below-debug-5")
flush_logs()  # every line logged above is on stderr now
""")
    assert r.returncode == 0, r.stderr
    assert "custom-level-25" in r.stderr
    assert "INFO" in r.stderr.split("custom-level-25")[0].splitlines()[-1]
    assert "below-debug-5" not in r.stderr


def test_logging_invalid_level_raises():
    r = _run_script("""
from mipyre.engine import init_logger
try:
    init_logger("verbose", False, "text")
except ValueError as e:
    print("ValueError:", e)
""")
    assert "ValueError:" in r.stdout and "verbose" in r.stdout, (r.stdout, r.stderr)


def test_logging_invalid_format_raises():
    r = _run_script("""
from mipyre.engine import init_logger
try:
    init_logger("INFO", False, "yaml")
except ValueError as e:
    print("ValueError:", e)
""")
    assert "ValueError:" in r.stdout and "yaml" in r.stdout, (r.stdout, r.stderr)


def test_logging_second_init_with_another_config_is_refused_and_names_both():
    """One process has one log: a second, different configuration is an error naming both, not
    a silent keep or a silent switch."""
    r = _run_script("""
from mipyre.engine import init_logger
init_logger("ERROR", False, "text")
try:
    init_logger("INFO", False, "json")
except RuntimeError as e:
    print("RuntimeError:", e)
""")
    assert r.returncode == 0, r.stderr
    assert "RuntimeError:" in r.stdout, (r.stdout, r.stderr)
    assert "level error, access log off, format text" in r.stdout, r.stdout
    assert "level info, access log off, format json" in r.stdout, r.stdout


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def test_metrics_is_named_and_reading_does_not_reset_peaks():
    """The metrics are named fields, and reading them leaves the peaks."""
    r = _run_script("""
from mipyre import Mipyre, get_gil_metrics, reset_peaks
from mipyre.engine import ServerFlags
from mipyre.testing import TestClient, TestServer

app = Mipyre(runtime="main")

@app.get("/heavy", runtime="main")
def heavy(req):
    return {"t": sum(range(300_000))}

# Main interpreter: this measures the main GIL, and workers would re-run this unguarded
# script (TestClient included).
c = TestClient(app, config=ServerFlags(metrics=True))
assert c.get("/heavy").status_code == 200
first = get_gil_metrics()
second = get_gil_metrics()
assert first.gil_hold_peak_us > 0, first
assert second.gil_hold_peak_us == first.gil_hold_peak_us, (first, second)
assert c.server.metrics().total_requests == 1, c.server.metrics()
reset_peaks()
third = get_gil_metrics()
assert third.gil_hold_peak_us == 0 and third.gil_wait_peak_us == 0, third
c.close()
print("OK")
""", timeout=60)
    assert "OK" in r.stdout, (r.stdout, r.stderr)


def test_metrics_rss_is_none_when_never_sampled():
    r = _run_script("""
from mipyre import get_gil_metrics
print(repr(get_gil_metrics().rss_bytes))
""")
    assert r.stdout.strip() == "None", (r.stdout, r.stderr)
