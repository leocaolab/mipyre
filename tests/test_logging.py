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

import json
import subprocess
import sys
import os
import signal
import tempfile
import urllib.error
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


def test_a_process_workers_logging_is_the_same_json_line_as_a_sub_interpreters(tmp_path):
    """b/876: a `runtime="process"` worker's Python logging goes through the engine's log, so a
    collector that parses JSON keeps its line: the same keys as a sub-interpreter worker's line,
    with the worker's `lane` where a sub-interpreter's has its `worker`."""
    (tmp_path / "process_log_app.py").write_text('''
import logging
from mipyre import Mipyre

app = Mipyre(log_config={"level": "WARN"})
log = logging.getLogger("b876")

@app.get("/sub")
def sub(req):
    log.warning("SUB_MARK %s", req.request_id)
    return "ok"

@app.get("/proc", runtime="process")
def proc(req):
    log.warning("PROC_MARK %s", req.request_id)
    return "ok"
''')
    log_path = str(tmp_path / "server.log")
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [PYTHON, "-m", "mipyre", "run", "process_log_app:app", "--port", "0"],
            cwd=tmp_path, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
    try:
        port = bound_port(read_file(log_path), proc)
        for path in ("/sub", "/proc"):
            urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10).close()
        output = settle(read_file(log_path), lambda out: "SUB_MARK" in out and "PROC_MARK" in out)
    finally:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        proc.wait(timeout=30)

    def line_with(mark: str) -> dict:
        found = [line for line in output.splitlines() if mark in line]
        assert len(found) == 1, (mark, output[-3000:])
        return json.loads(found[0])

    sub, process = line_with("SUB_MARK"), line_with("PROC_MARK")
    assert set(sub) == set(process) == {"timestamp", "level", "fields", "target"}, (sub, process)
    assert sub["target"] == process["target"] == "mipyre::app"
    assert sub["level"] == process["level"] == "WARN"
    assert set(sub["fields"]) - {"worker"} == set(process["fields"]) - {"lane"}, (sub, process)
    assert process["fields"]["logger"] == "b876" and isinstance(process["fields"]["lane"], int), process


_TRACED_APP = """
from mipyre import Mipyre
app = Mipyre()
app.enable_access_log()
app.enable_request_id_header("traceparent")

@app.get("/hello")
def hello(req): return {"request_id": req.request_id}

app.run(host="127.0.0.1", port=0)
"""

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def test_the_access_line_carries_the_request_id_the_response_carries_and_the_client_address():
    """A handler's answer and the engine's own 404 each have one access line, named by the id
    their response carries (the client's trace id, or the one the server made) and the client's
    address, so the line is found by the trace id and joins the handler's lines (b/882)."""
    workdir = tempfile.mkdtemp(prefix="mipyre_log_traced_")
    script_path = os.path.join(workdir, "app.py")
    with open(script_path, "w") as f:
        f.write(_TRACED_APP)
    log_path = os.path.join(workdir, "app.log")
    with open(log_path, "w") as log:
        proc = subprocess.Popen([PYTHON, script_path], stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    read = read_file(log_path)
    try:
        port = bound_port(read, proc)
        hello = urllib.request.Request(f"http://127.0.0.1:{port}/hello", headers={"traceparent": TRACEPARENT})
        with urllib.request.urlopen(hello, timeout=5) as r:
            assert r.headers["traceparent"] == TRACEPARENT
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/nope", timeout=5)
        except urllib.error.HTTPError as e:
            assert e.code == 404
            minted = e.headers["traceparent"]
            e.close()

        def access_lines(out: str) -> dict:
            lines = [json.loads(line) for line in out.splitlines() if '"mipyre::access"' in line]
            return {line["fields"]["path"]: line["fields"] for line in lines}

        lines = settle(lambda: access_lines(read()), lambda got: {"/hello", "/nope"} <= got.keys())
    finally:
        if proc.poll() is None:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)

    assert lines["/hello"]["request_id"] == TRACEPARENT, lines
    assert lines["/hello"]["client_ip"] == "127.0.0.1", lines
    assert lines["/nope"]["request_id"] == minted, lines
    assert lines["/nope"]["client_ip"] == "127.0.0.1", lines


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
