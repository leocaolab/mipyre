"""Hot reload: run the app in a child process, restart it when a .py file changes.

The parent only watches; the child re-runs the exact command that started this
process, with ``CHILD_ENV`` set so it serves instead of reloading again. A child
that exits (a syntax error, a crash on startup) is restarted by the next change.

SIGINT or SIGTERM to the parent stops the child with that signal, waits for it, and
the parent ends with the child's exit status: whoever stops ``mipyre dev`` (a
terminal, a supervisor, an IDE) stops the server too and sees how it ended.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass

CHILD_ENV = "_MIPYRE_RELOAD_CHILD"

_STOP_TIMEOUT_S = 3
_POLL_INTERVAL_S = 1.0
# Wait this long after a change for the editor to finish writing every file.
_DEBOUNCE_MS = 500
_SKIP_DIRS = frozenset({".git", ".venv", "venv", "node_modules", "__pycache__"})


@dataclass(frozen=True)
class ReloadTarget:
    argv: tuple[str, ...]
    watch_dir: str

    @classmethod
    def of_this_process(cls, app_file: str) -> ReloadTarget:
        """Re-run this process's own command line (``sys.orig_argv`` keeps
        ``-m mipyre dev app`` and console-script paths intact), watching the
        directory the app's source lives in."""
        return cls(
            argv=(sys.executable, *sys.orig_argv[1:]),
            watch_dir=os.path.dirname(app_file),
        )


def is_reload_child() -> bool:
    return os.environ.get(CHILD_ENV) == "1"


def run_with_reload(target: ReloadTarget) -> int:
    """Serve ``target`` in a child, restarting it on every change, until SIGINT or SIGTERM;
    the child's exit status (a shell's ``128 + N`` for a child ended by signal N)."""
    try:
        import watchfiles
    except ImportError:
        print("  [reload] Install 'watchfiles' for efficient file watching:")
        print("           pip install watchfiles")
        print("  [reload] Falling back to polling mode...")
        print(f"  [reload] Watching {target.watch_dir} for .py changes (polling)...")
        return _serve(target, _polling(target.watch_dir))
    print(f"  [reload] Watching {target.watch_dir} for .py changes (watchfiles)...")
    return _serve(target, _watchfiles(watchfiles, target.watch_dir))


class _StopRequest:
    """The SIGINT or SIGTERM that asked the reloader to stop, once one has."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._signum: signal.Signals | None = None

    def on_signal(self, signum: int, _frame: object) -> None:
        self._signum = signal.Signals(signum)
        self._event.set()

    def received(self) -> signal.Signals:
        """The signal that set the request; asked only once a wait saw it set."""
        assert self._signum is not None, "on_signal records the signal before it sets the event a wait sees"
        return self._signum

    def is_set(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout: float) -> bool:
        return self._event.wait(timeout)


@contextlib.contextmanager
def _stop_requested_by_signal() -> Iterator[_StopRequest]:
    """SIGINT and SIGTERM set the request instead of ending the process, for the block."""
    stop = _StopRequest()
    previous = {signum: signal.signal(signum, stop.on_signal) for signum in (signal.SIGINT, signal.SIGTERM)}
    try:
        yield stop
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


# Waits for a change under the watched directory; the changed files, or None once a stop is requested.
_WaitForChange = Callable[[_StopRequest], "list[str] | None"]


def _serve(target: ReloadTarget, wait_for_change: _WaitForChange) -> int:
    with _stop_requested_by_signal() as stop:
        while True:
            proc = _start(target)
            changed = wait_for_change(stop)
            if changed is None:
                return exit_status(_stop(proc, stop.received()))
            _announce(changed)
            _stop(proc, signal.SIGTERM)


def _watchfiles(watchfiles, watch_dir: str) -> _WaitForChange:
    def wait(stop: _StopRequest) -> list[str] | None:
        # A stop request ends the watch with no changes.
        changes = next(
            watchfiles.watch(
                watch_dir, watch_filter=watchfiles.PythonFilter(), debounce=_DEBOUNCE_MS, stop_event=stop
            ),
            None,
        )
        return None if changes is None else [path for _change, path in changes]

    return wait


def _polling(watch_dir: str) -> _WaitForChange:
    snapshot = _snapshot(watch_dir)

    def wait(stop: _StopRequest) -> list[str] | None:
        nonlocal snapshot
        found = _wait_for_change(watch_dir, snapshot, stop)
        if found is None:
            return None
        changed, snapshot = found
        return changed

    return wait


def _wait_for_change(watch_dir: str, before: dict, stop: _StopRequest) -> tuple[list[str], dict] | None:
    while _snapshot(watch_dir) == before:
        if stop.wait(_POLL_INTERVAL_S):
            return None
    if stop.wait(_DEBOUNCE_MS / 1000):
        return None
    after = _snapshot(watch_dir)
    return _changed(before, after), after


def _changed(before: dict, after: dict) -> list[str]:
    return sorted(f for f in before.keys() | after.keys() if before.get(f) != after.get(f))


def _snapshot(watch_dir: str) -> dict[str, tuple[int, int]]:
    """(mtime_ns, size) of every .py file under ``watch_dir``."""
    snapshot = {}
    for root, dirs, files in os.walk(watch_dir):
        dirs[:] = [d for d in dirs if d not in _SKIP_DIRS]
        for name in files:
            if not name.endswith(".py"):
                continue
            path = os.path.join(root, name)
            try:
                st = os.stat(path)
            except FileNotFoundError:
                continue  # deleted while walking; the next snapshot sees it gone
            snapshot[path] = (st.st_mtime_ns, st.st_size)
    return snapshot


def _announce(changed: list[str]) -> None:
    print(f"\n  [reload] File changed: {', '.join(os.path.basename(p) for p in changed[:3])}")
    print("  [reload] Restarting...\n")


def _start(target: ReloadTarget) -> subprocess.Popen:
    return subprocess.Popen(list(target.argv), env={**os.environ, CHILD_ENV: "1"})


def _stop(proc: subprocess.Popen, signum: signal.Signals) -> int:
    """Stop the child with ``signum``, killing it if it has not exited after
    ``_STOP_TIMEOUT_S``; its return code."""
    proc.send_signal(signum)
    try:
        return proc.wait(timeout=_STOP_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        print(f"  [reload] The server did not stop within {_STOP_TIMEOUT_S}s of {signum.name}; killing it.")
        proc.kill()
        return proc.wait()


def exit_status(returncode: int) -> int:
    """A child's ``Popen.returncode`` as this process's exit status: the code it exited with,
    or a shell's ``128 + N`` for a child ended by signal N (``returncode == -N``)."""
    return returncode if returncode >= 0 else 128 - returncode
