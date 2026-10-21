"""Test: Mipyre logging system — Rust tracing engine + Python bridge.

Verifies:
1. Framework enable_access_log() produces structured output (GIL mode)
2. Rust-level sub-interp request logging works via tracing
3. User print() visible in sub-interpreter
4. User logging.info() routed through Rust tracing in sub-interpreter
5. debug=True enables access log + tracing output
6. JSON format output when configured
7. Python logging bridge works in main interpreter
"""

import subprocess
import sys
import os
import signal
import tempfile
import urllib.request

from tests._helpers import bound_port, read_file, settle

PYTHON = sys.executable


def run_server_and_check(script: str, label: str, expected_strings: list[str]):
    """Start a server, send requests, check output for expected strings."""
    workdir = tempfile.mkdtemp(prefix=f"mipyre_log_{label}_")
    script_path = os.path.join(workdir, "app.py")
    with open(script_path, "w") as f:
        f.write(script)

    # One file for stdout + stderr: tracing writes to stderr, println (and the
    # "Listening on" line with the bound port) to stdout.
    log_path = os.path.join(workdir, "app.log")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [PYTHON, script_path],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    try:
        # Ready = the "Listening on" line (bound_port polls it; raises with the
        # server's output if the process exits first).
        port = bound_port(read_file(log_path), proc)
        for _ in range(2):
            urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5).close()
        # The log writer is non-blocking: the access line may land a moment after the
        # response, so wait for the expected strings (capped), not a fixed sleep.
        settle(
            read_file(log_path),
            lambda out: all(s in out for s in expected_strings),
            timeout=5.0,
        )
    finally:
        if proc.poll() is None:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
    output = read_file(log_path)()

    passed = 0
    for s in expected_strings:
        if s in output:
            print(f"  ✅ {label}: found '{s}'")
            passed += 1
        else:
            print(f"  ❌ {label}: missing '{s}'")
            print(f"     output: {output[:2000]}")

    return passed == len(expected_strings)


def test_gil_mode_logging():
    """Framework logging in GIL mode — the Rust access log (one line per request)."""
    script = '''
from mipyre import Mipyre
app = Mipyre()
app.enable_access_log()

@app.get("/", runtime="main")
def index(req): return "ok"

app.run(host="127.0.0.1", port=0)
'''
    assert run_server_and_check(script, "gil_logging", [
        "mipyre::access",
        "Request handled",
        '"method":"GET"',
        '"path":"/"',
        '"status":200',
    ])


def test_subinterp_rust_logging():
    """Rust-level request logging in sub-interpreter mode via tracing."""
    script = '''
from mipyre import Mipyre
app = Mipyre()
app.enable_access_log()

@app.get("/")
def index(req): return "ok"

app.run(host="127.0.0.1", port=0)
'''
    assert run_server_and_check(script, "subinterp_logging", [
        "mipyre::access",
        "Request handled",
    ])


def test_user_print_in_subinterp():
    """User print() works in sub-interpreter handlers."""
    script = '''
from mipyre import Mipyre
app = Mipyre()

@app.get("/")
def index(req):
    print("USER_PRINT_MARKER", flush=True)
    return "ok"

app.run(host="127.0.0.1", port=0)
'''
    assert run_server_and_check(script, "user_print", [
        "USER_PRINT_MARKER",
    ])


def test_user_logging_in_subinterp():
    """Python logging module routed through Rust tracing in sub-interpreter."""
    script = '''
import logging
logger = logging.getLogger("test")

from mipyre import Mipyre
app = Mipyre(debug=True)

@app.get("/")
def index(req):
    logger.info("LOGGER_MARKER_12345")
    return "ok"

app.run(host="127.0.0.1", port=0)
'''
    assert run_server_and_check(script, "user_logging", [
        "LOGGER_MARKER_12345",
    ])


def test_debug_mode_tracing():
    """debug=True enables tracing output with access log."""
    script = '''
from mipyre import Mipyre
app = Mipyre(debug=True)

@app.get("/")
def index(req): return "ok"

app.run(host="127.0.0.1", port=0)
'''
    assert run_server_and_check(script, "debug_mode", [
        "mipyre::server",
        "Mipyre tracing engine initialized",
        "Mipyre started",
    ])


def test_debug_mode_access_log():
    """debug=True produces access log with latency."""
    script = '''
from mipyre import Mipyre
app = Mipyre(debug=True)

@app.get("/")
def index(req): return "ok"

app.run(host="127.0.0.1", port=0)
'''
    assert run_server_and_check(script, "debug_access_log", [
        "mipyre::access",
        "Request handled",
        "latency_us",
    ])


def test_python_logging_bridge_main():
    """Python logging in main interpreter routes through Rust tracing."""
    script = '''
import logging
from mipyre import Mipyre

app = Mipyre(debug=True)
logger = logging.getLogger("myapp")

@app.get("/")
def index(req):
    logger.info("BRIDGE_TEST_MARKER")
    return "ok"

app.run(host="127.0.0.1", port=0)
'''
    assert run_server_and_check(script, "python_bridge_main", [
        "BRIDGE_TEST_MARKER",
        "mipyre::app",
    ])


def test_json_format():
    """JSON format output when configured."""
    script = '''
from mipyre import Mipyre
app = Mipyre(debug=True, log_config={"format": "json"})

@app.get("/")
def index(req): return "ok"

app.run(host="127.0.0.1", port=0)
'''
    assert run_server_and_check(script, "json_format", [
        '"target":"mipyre::server"',
        '"message":"Mipyre tracing engine initialized"',
    ])


if __name__ == "__main__":
    print("=== Logging Tests ===")
    print()
    test_gil_mode_logging()
    print()
    test_subinterp_rust_logging()
    print()
    test_user_print_in_subinterp()
    print()
    test_user_logging_in_subinterp()
    print()
    test_debug_mode_tracing()
    print()
    test_debug_mode_access_log()
    print()
    test_python_logging_bridge_main()
    print()
    test_json_format()
    print()
    print("=== All logging tests passed ===")
