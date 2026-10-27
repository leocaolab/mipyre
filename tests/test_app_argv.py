"""The app's own command line: under `mipyre run`, what follows
`--` is the app's, and every interpreter that runs the app has the same `sys.argv`, so an app that
parses its arguments with argparse as it is imported reads the same values on main, in each
sub-interpreter and in each process worker."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import urllib.request

import pytest

from tests._helpers import bound_port, read_file

_APP = '''
import argparse
import os
import sys

from mipyre import Mipyre

parser = argparse.ArgumentParser(prog="argv_app")
parser.add_argument("--db-dsn", required=True)
ARGS = parser.parse_args()

app = Mipyre()


def seen():
    # As bytes: an argument that is not UTF-8 reaches Python as a str JSON can't carry.
    return {
        "app_file": sys.argv[0],
        "args": [os.fsencode(a).hex() for a in sys.argv[1:]],
        "db_dsn": os.fsencode(ARGS.db_dsn).hex(),
        "pid": os.getpid(),
    }


@app.get("/main", runtime="main")
def on_main(req):
    return seen()


@app.get("/subinterp", runtime="subinterp")
def on_subinterp(req):
    return seen()


@app.get("/process", runtime="process")
def on_process(req):
    return seen()
'''


def _run(tmp_path, *args: str | bytes) -> tuple[subprocess.Popen, str]:
    (tmp_path / "app.py").write_text(_APP)
    log_path = str(tmp_path / "server.log")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "mipyre", "run", "app.py:app", "--port", "0", *args],
            cwd=tmp_path, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
    return proc, log_path


def _get(port: int, path: str) -> dict:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=30) as r:
        return json.loads(r.read())


DSN = b"postgres://db/\xff"  # not UTF-8: an argument is OS bytes


@pytest.mark.parametrize("start", ["import", "fork"])
def test_every_interpreter_sees_the_app_s_argv(tmp_path, start):
    proc, log_path = _run(
        tmp_path, "--threads", "2", "--process-workers", "1", "--process-start", start, "--", "--db-dsn", DSN
    )
    try:
        port = bound_port(read_file(log_path), proc)
        main, subinterp, process = (_get(port, p) for p in ("/main", "/subinterp", "/process"))
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGINT)
        proc.wait()
    assert process["pid"] != main["pid"] == subinterp["pid"] == proc.pid
    for seen in (main, subinterp, process):
        assert os.path.realpath(seen["app_file"]) == os.path.realpath(tmp_path / "app.py"), seen
        assert seen["args"] == [b"--db-dsn".hex(), DSN.hex()], seen
        assert seen["db_dsn"] == DSN.hex(), seen


def test_an_argument_after_the_separator_is_the_app_s_to_reject(tmp_path):
    proc, log_path = _run(tmp_path, "--", "--db-dsn", "x", "--bogus")
    assert proc.wait() == 2
    output = read_file(log_path)()
    assert "usage: argv_app" in output, output
    assert "argv_app: error: unrecognized arguments: --bogus" in output, output
