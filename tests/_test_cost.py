"""What each test costs, counted (b/441, b/452): the seconds of sleep it asked for and the work it did.

The merge gate used to judge a test by its wall clock (b/253). Wall clock is work + waiting + queueing, and the
queueing moves with whatever else the merge host runs, so the gate went red under load on tests that had not
changed. These counts do not move with the machine: they are what the test asked for, not how long it took.

Per test, for its call phase (fixtures excluded: a module-scoped fixture runs once per xdist worker, so whichever
test comes first on a worker would carry it), written as JUnit properties that `dev/test_time_gate.py` holds to
`dev/test_cost_budget.toml`:

- `sleep_s`: the seconds `time.sleep` / `asyncio.sleep` were asked for by code under `tests/`, one call of at least
  `FIXED_SLEEP_S` at a time. The requested value, not the time slept: a loaded machine sleeps longer, the request
  is the same. A shorter call is a poll interval (how often a wait looks, not how long it lasts), and the polls of
  `tests/_helpers.py` are not counted at all: how many times a wait looks depends on the machine.
- `subprocesses`: processes started (`subprocess.Popen`).
- `servers`: in-process servers started (`mipyre.testing.TestClient`).
- `requests`: HTTP requests sent (`http.client`, which urllib and the TestClient go through, and httpx).
- `connections`: sockets connected (`socket.socket.connect`: raw clients, WebSockets, and every request's own).

Work done while a wait helper is polling (`polling()`: a `fetch` that sends a request to see if a server is up)
is not counted, for the same reason as the poll intervals.
"""

from __future__ import annotations

import asyncio
import contextlib
import http.client
import os
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

TESTS_DIR = str(Path(__file__).resolve().parent)
HELPERS = str(Path(__file__).resolve().with_name("_helpers.py"))
# A sleep this long or longer is a fixed sleep; a shorter one is a poll interval. The same line as
# dev/sleep_gate.py draws.
FIXED_SLEEP_S = 0.05

COUNTS = ("sleep_s", "subprocesses", "servers", "requests", "connections")

_polling = threading.local()


@contextlib.contextmanager
def polling() -> Iterator[None]:
    """Work done inside is a wait's polling, and is not counted: how often a wait looks is the machine's."""
    depth = getattr(_polling, "depth", 0)
    _polling.depth = depth + 1
    try:
        yield
    finally:
        _polling.depth = depth


def _is_polling() -> bool:
    return getattr(_polling, "depth", 0) > 0


class TestCost:
    """The counters, and the hooks that write them per test. One per pytest process."""

    __test__ = False

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, float] = dict.fromkeys(COUNTS, 0)
        self._undo: list[tuple[object, str, object]] = []

    def add(self, name: str, amount: float = 1) -> None:
        if _is_polling():
            return
        with self._lock:
            self._counts[name] += amount

    def take(self) -> dict[str, float]:
        with self._lock:
            counts, self._counts = self._counts, dict.fromkeys(COUNTS, 0)
        return counts

    def add_sleep(self, seconds: object, caller: str) -> None:
        """A sleep of `seconds` asked for from the file `caller`."""
        if not caller.startswith(TESTS_DIR + os.sep) or caller == HELPERS:
            return
        if isinstance(seconds, (int, float)) and seconds >= FIXED_SLEEP_S:
            self.add("sleep_s", float(seconds))

    def _wrap(self, owner: object, attr: str, make) -> None:
        original = getattr(owner, attr)
        setattr(owner, attr, make(original))
        self._undo.append((owner, attr, original))

    def install(self) -> None:
        cost = self

        def sleep(original):
            def counted_sleep(seconds):
                cost.add_sleep(seconds, sys._getframe(1).f_code.co_filename)
                return original(seconds)

            return counted_sleep

        def async_sleep(original):
            def counted_async_sleep(delay, *args, **kwargs):
                cost.add_sleep(delay, sys._getframe(1).f_code.co_filename)
                return original(delay, *args, **kwargs)

            return counted_async_sleep

        def counting(name):
            def make(original):
                def counted(*args, **kwargs):
                    cost.add(name)
                    return original(*args, **kwargs)

                return counted

            return make

        self._wrap(time, "sleep", sleep)
        self._wrap(asyncio, "sleep", async_sleep)
        self._wrap(subprocess.Popen, "__init__", counting("subprocesses"))
        self._wrap(http.client.HTTPConnection, "putrequest", counting("requests"))
        self._wrap(socket.socket, "connect", counting("connections"))
        self._wrap(httpx.Client, "send", counting("requests"))
        self._wrap(httpx.AsyncClient, "send", counting("requests"))
        from mipyre import testing

        self._wrap(testing.TestClient, "__init__", counting("servers"))

    def uninstall(self) -> None:
        while self._undo:
            owner, attr, original = self._undo.pop()
            setattr(owner, attr, original)

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_call(self, item: pytest.Item):
        self.take()
        yield
        counts = self.take()
        for name in COUNTS:
            value = counts[name]
            item.user_properties.append((name, round(value, 3) if name == "sleep_s" else int(value)))
