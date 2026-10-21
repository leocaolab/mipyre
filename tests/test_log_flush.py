"""`flush_logs()` (b/441 L6): every line logged before it is on stderr when it returns, and `Mipyre.run()` calls it
as it ends, so a server's exit is a fence for its log.

The log is written by a background thread (`telemetry/log_writer.rs`) that the process exit does not wait
for. Each case logs many lines and ends the process the way that does not wait (`os._exit`, or the end of `run()`),
then counts them all: a missing flush loses the tail. The fence's own logic (a fence passes only after the lines
before it, a stopped writer fails it) is `telemetry/log_writer.rs`'s, tested there.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys

from tests._helpers import answers, bound_port, read_file

LINES = 5000

FLUSH_THEN_EXIT = f"""
import logging, os
from mipyre.engine import flush_logs, init_logger
from mipyre._log_bridge import install_root_handler
from mipyre.engine import _python_log_level, emit_python_log
init_logger("INFO", False, "text")
install_root_handler(emit_python_log, _python_log_level())
for i in range({LINES}):
    logging.getLogger("flush").warning("flush-line %d", i)
flush_logs()
os._exit(0)
"""


def test_every_line_logged_before_flush_logs_is_on_stderr_when_it_returns():
    r = subprocess.run([sys.executable, "-c", FLUSH_THEN_EXIT], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr[-3000:]
    lines = [line for line in r.stderr.splitlines() if "flush-line" in line]
    assert len(lines) == LINES, (len(lines), r.stderr[-2000:])
    assert f"flush-line {LINES - 1} " in lines[-1], lines[-1]


def test_flush_logs_without_a_logger_returns_at_once():
    r = subprocess.run([sys.executable, "-c", "from mipyre import flush_logs; flush_logs(); print('done')"],
                       capture_output=True, text=True)
    assert (r.returncode, r.stdout) == (0, "done\n"), r.stderr


SHUTDOWN_LOGS = f"""
import logging
from mipyre import Mipyre
app = Mipyre(log_config={{"level": "INFO", "format": "text", "access_log": False}})

@app.get("/")
def index(req):
    return "ok"

@app.on_shutdown
def say_goodbye():
    for i in range({LINES}):
        logging.getLogger("bye").warning("bye-line %d", i)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, workers=1)
"""


def test_the_lines_a_server_logs_as_it_stops_are_written_before_it_exits(tmp_path):
    script, log_path = tmp_path / "app.py", tmp_path / "app.log"
    script.write_text(SHUTDOWN_LOGS)
    env = {k: v for k, v in os.environ.items() if k not in ("MIPYRE_RUNTIME", "MIPYRE_TPC")}
    with open(log_path, "w") as out:
        proc = subprocess.Popen([sys.executable, str(script)], stdout=out, stderr=subprocess.STDOUT,
                                start_new_session=True, env=env)
    try:
        port = bound_port(read_file(str(log_path)), proc)
        answers(f"http://127.0.0.1:{port}/", proc, read_file(str(log_path)))
        os.killpg(proc.pid, signal.SIGINT)
        code = proc.wait()
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
    output = log_path.read_text(errors="replace")
    assert code == 0, output[-3000:]
    lines = [line for line in output.splitlines() if "bye-line" in line]
    assert len(lines) == LINES, (len(lines), output[-2000:])
