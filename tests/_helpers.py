"""Helpers shared by the server tests: deadline polling, and the port a server bound.

Every test server binds port 0 and the test reads back the port the kernel picked, so
suites can run concurrently: `TestClient.port` in-process, `bound_port` for a subprocess
server (from its "Listening on" startup line).
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import time
import urllib.error
import urllib.request
from typing import Callable, TypeVar

from tests._test_cost import polling

T = TypeVar("T")

# The startup line every serving path prints once its sockets are bound, e.g.
# "  Listening on http://127.0.0.1:52341" (an IPv6 host is bracketed).
LISTENING = re.compile(r"Listening on (?P<scheme>https?)://(?P<host>\[[^\]]+\]|[^\s:/]+):(?P<port>\d+)")

POLL_INTERVAL_S = 0.05


def poll_until(
    fetch: Callable[[], T | None],
    *,
    timeout: float | None = 5.0,
    interval: float = POLL_INTERVAL_S,
    what: str = "the condition",
) -> T:
    """Calls `fetch` until it returns a truthy value, and returns that value.

    Raises `TimeoutError` naming `what` and the last value `fetch` returned when
    `timeout` seconds pass first. `timeout=None` waits for the value alone, for no length of
    time: for a wait whose length is not what the test checks (a server starting, b/325).
    """
    deadline = None if timeout is None else time.monotonic() + timeout
    while True:
        with polling():
            value = fetch()
        if value:
            return value
        if deadline is not None and time.monotonic() > deadline:
            raise TimeoutError(f"{what} not met within {timeout}s; last value: {value!r}")
        time.sleep(interval)


def settle(
    fetch: Callable[[], T],
    done: Callable[[T], bool] = bool,
    *,
    timeout: float = 5.0,
    interval: float = POLL_INTERVAL_S,
) -> T:
    """Calls `fetch` until `done` accepts its value or `timeout` seconds pass, and returns
    the last value either way: for a check that asserts on what arrived (a server's log
    writer is non-blocking, so a line may land after the response it belongs to)."""
    deadline = time.monotonic() + timeout
    while True:
        with polling():
            value = fetch()
        if done(value) or time.monotonic() > deadline:
            return value
        time.sleep(interval)


def lines_with(read_output: Callable[[], str], needle: str, *, timeout: float = 5.0) -> list[str]:
    """The lines of the output that contain `needle`, once at least one is there."""
    return settle(
        lambda: [line for line in read_output().splitlines() if needle in line],
        timeout=timeout,
    )


def listening_ports(output: str) -> list[int]:
    """The ports of every "Listening on" line in `output`, in order."""
    return [int(m["port"]) for m in LISTENING.finditer(output)]


def bound_port(read_output: Callable[[], str], proc: subprocess.Popen) -> int:
    """The port a server started with port 0 bound: the first "Listening on" line of the
    output `read_output` returns.

    Waits for one of two events, the line or `proc` exiting, and for no length of time: a
    loaded machine starts a server late, and that is not the server failing (b/325). Raises
    `RuntimeError` carrying the output when `proc` exits first.
    """
    while True:
        with polling():
            output = read_output()
        ports = listening_ports(output)
        if ports:
            return ports[0]
        if proc.poll() is not None:
            raise RuntimeError(
                f"server exited with {proc.returncode} before listening:\n{output[-4000:]}"
            )
        time.sleep(POLL_INTERVAL_S)


def read_file(path: str) -> Callable[[], str]:
    """A `read_output` for a server whose output goes to the file at `path`."""

    def read() -> str:
        try:
            with open(path, errors="replace") as f:
                return f.read()
        except FileNotFoundError:
            return ""

    return read


def kill_session(proc: subprocess.Popen) -> None:
    """SIGKILL whatever is left of the process group `proc` leads (a server started with
    `start_new_session=True`, so the group id is its pid): the children a SIGTERM to the
    leader left behind, or the leader itself if it is still running. Then reaps the leader.

    By group, not by port: `lsof -ti:<port> | xargs kill -9` kills every process with a
    socket on that port number, local or remote end, and under a parallel run (xdist, or a
    second checkout on the host) that number is soon another test's server or the client
    side of a connection a pytest worker holds.
    """
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass  # the group is empty: everything in it has exited
    except PermissionError:
        # macOS answers EPERM, not ESRCH, when every member left is a zombie (the leader,
        # killed and not yet reaped; measured): there is nothing left to kill. A live group
        # of our own child is always ours to signal, so EPERM means nothing else here.
        pass
    proc.wait(timeout=10)


# --- b/441 B block: waits for an event, for no length of time -----------------------------------------------------
# A wait here has no deadline of its own: how long a server takes to start or a line to land is the machine's, not a
# fact the test checks. A wait that never ends is pytest-timeout's (pyproject.toml, the one hang guard).


def answers(url: str, proc: subprocess.Popen, read_output: Callable[[], str], *, status: int = 200) -> None:
    """Waits until a GET of `url` answers `status`, or `proc` exits (`RuntimeError` carrying its output). A
    refused, reset or timed-out attempt, and any other status, is a server still starting: it looks again."""
    def answered() -> bool:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited with {proc.returncode} before {url} answered:\n{read_output()[-4000:]}")
        try:
            with urllib.request.urlopen(url, timeout=1) as resp:
                return resp.status == status
        except urllib.error.HTTPError as e:
            e.close()
            return e.code == status
        except OSError:
            return False

    poll_until(answered, timeout=None, what=f"{url} answering {status}")


def text_appears(read_output: Callable[[], str], text: str, proc: subprocess.Popen | None = None) -> str:
    """Waits until `text` is in the output, and returns the output. With `proc`, its exit first is a `RuntimeError`
    carrying the output (the text will not come)."""

    def seen() -> str:
        output = read_output()
        if text in output:
            return output
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"process exited with {proc.returncode} before {text!r} appeared:\n{output[-4000:]}")
        return ""

    return poll_until(seen, timeout=None, what=f"{text!r} in the output")


# Put at the top of a server script: writes "finalized" to the file named by the script's
# FINALIZE_MARK environment variable from an atexit callback. CPython runs atexit callbacks
# as it finalizes and `os._exit` skips them, so the mark says the process finalized instead
# of exiting hard (b/402). No braces: scripts are `.format`ted.
FINALIZE_MARK_SNIPPET = """
import atexit as _atexit, os as _os
def _write_finalize_mark(path=_os.environ["FINALIZE_MARK"]):
    with open(path, "w") as f:
        f.write("finalized")
_atexit.register(_write_finalize_mark)
"""


def finalized(mark: str | os.PathLike[str]) -> bool:
    """Whether the process that ran `FINALIZE_MARK_SNIPPET` with `mark` finalized."""
    try:
        with open(mark) as f:
            return f.read() == "finalized"
    except FileNotFoundError:
        return False
