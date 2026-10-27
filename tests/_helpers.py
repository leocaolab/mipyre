"""Helpers shared by the server tests: deadline polling, and the port a server bound.

Every test server binds port 0 and the test reads back the port the kernel picked, so
suites can run concurrently: `TestClient.port` in-process, `bound_port` for a subprocess
server (from its "Listening on" startup line). A test that names the port before the server
starts, or checks that a stopped server let it go, draws one below the ephemeral range instead
(`free_port_below_ephemeral`): a port 0 bind or a connect anywhere on the machine can take a
port inside it.
"""

from __future__ import annotations

import itertools
import os
import random
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Callable, MutableMapping, TypeVar

import pytest

from tests._test_cost import polling

T = TypeVar("T")

# The startup line every serving path prints once its sockets are bound, e.g.
# "  Listening on http://127.0.0.1:52341" (an IPv6 host is bracketed).
LISTENING = re.compile(r"Listening on (?P<scheme>https?)://(?P<host>\[[^\]]+\]|[^\s:/]+):(?P<port>\d+)")

POLL_INTERVAL_S = 0.05


def set_subinterpreters(env: MutableMapping[str, str], count: str) -> None:
    """Sets in `env` how many sub-interpreters a server started with it runs: the thread-per-core
    server runs one per thread (`MIPYRE_THREADS`), the `MIPYRE_TPC=0` server a pool of
    `MIPYRE_SUBINTERPRETERS`. A `MIPYRE_TPC=0` server of a script given `MIPYRE_TEST_MODE=main`
    runs no sub-interpreter, so nothing is set: a size it does not use is an error."""
    if env.get("MIPYRE_TPC") != "0":
        env["MIPYRE_THREADS"] = count
    elif env.get("MIPYRE_TEST_MODE") != "main":
        env["MIPYRE_SUBINTERPRETERS"] = count


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
    time: for a wait whose length is not what the test checks (a server starting).
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
    loaded machine starts a server late, and that is not the server failing. Raises
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


# --- waits for an event, for no length of time ---------------------------------------------------------------------
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
# of exiting hard. No braces: scripts are `.format`ted.
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


_ADAPTER_SOURCES = ("_clone.py", "_worker/extension_support.py", "_isolated/isolation.py")


def isolation_adapter_ns(isolate_dir: str | os.PathLike[str] | None = None) -> dict:
    """A namespace holding the `runtime="isolated"` adapter as a worker's bootstrap does: the engine runs these files,
    in this order, in one module's namespace (registered in `sys.modules`, as the bootstrap module is), with
    `ISOLATE_DIR` the server's isolate dir as the engine sets it. Nothing is installed: the bootstrap inserts the finder
    itself."""
    import sys
    import types
    from pathlib import Path

    package = Path(__file__).resolve().parent.parent / "python" / "mipyre"
    module = types.ModuleType(f"__mipyre_adapter_under_test_{len(sys.modules)}__")
    sys.modules[module.__name__] = module
    module.__dict__["ISOLATE_DIR"] = None if isolate_dir is None else os.fspath(isolate_dir)
    for name in _ADAPTER_SOURCES:
        path = package / name
        exec(compile(path.read_text(), str(path), "exec"), module.__dict__)  # noqa: S102 — the adapter runs only in a worker's bootstrap namespace, so the test builds that namespace
    return module.__dict__


# A Prometheus page's samples (`mipyre_x{a="b"} 3`): a server's counts as its `/metrics` page
# shows them, the view of them a test outside the server's process has.
_METRIC_SAMPLE = re.compile(r'^([a-z_]+)(?:\{(.*)\})? (\S+)$')
_METRIC_LABEL = re.compile(r'([a-z_]+)="((?:[^"\\]|\\.)*)"')


def metric_value(body: str, name: str, **labels: str) -> float:
    """The sum of ``name``'s samples whose labels include ``labels``; a family that shows only
    its non-zero samples has none for what was not counted, so that is 0. A family missing from
    the page fails."""
    if f"# TYPE {name} " not in body and f"# TYPE {name.rsplit('_', 1)[0]} histogram" not in body:
        raise AssertionError(f"metric {name!r} not found in:\n{body}")
    total = 0.0
    for line in body.splitlines():
        sample = _METRIC_SAMPLE.match(line)
        if sample is None or sample[1] != name:
            continue
        have = dict(_METRIC_LABEL.findall(sample[2] or ""))
        if all(have.get(k) == v for k, v in labels.items()):
            total += float(sample[3])
    return total


def metric_count(body: str, name: str, **labels: str) -> int:
    return int(metric_value(body, name, **labels))


def port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


# Ports below this need privileges to bind.
_FIRST_UNPRIVILEGED_PORT = 1024
_LINUX_PORT_RANGE = "/proc/sys/net/ipv4/ip_local_port_range"
_DARWIN_PORT_RANGE_FIRST = "net.inet.ip.portrange.first"


def _ephemeral_first() -> int:
    """Return the lowest port the OS hands out to a bind to port 0 or to a connect."""
    if sys.platform == "linux":
        with open(_LINUX_PORT_RANGE) as f:
            text = f.read()
        return int(text.split()[0])
    done = subprocess.run(["sysctl", "-n", _DARWIN_PORT_RANGE_FIRST], capture_output=True, text=True)
    if done.returncode != 0:
        pytest.fail(f"sysctl -n {_DARWIN_PORT_RANGE_FIRST} exited {done.returncode}: {done.stdout}{done.stderr}")
    return int(done.stdout)


def port_below(ephemeral_first: int, draw: int) -> int:
    """Return the port that ``draw`` picks among the unprivileged ports below ``ephemeral_first``."""
    span = ephemeral_first - _FIRST_UNPRIVILEGED_PORT
    if span <= 0:
        pytest.fail(f"no unprivileged port lies below the ephemeral range, which starts at {ephemeral_first}")
    return _FIRST_UNPRIVILEGED_PORT + draw % span


def free_port_below_ephemeral() -> int:
    """Return a free port the OS never hands out to a bind to port 0 or to a connect, so between
    the draw and the server's bind, and once the server lets it go, only a process of the test can
    be holding it."""
    first = _ephemeral_first()
    drawn = (port_below(first, random.getrandbits(32)) for _ in itertools.count())
    return next(port for port in drawn if port_free(port))
