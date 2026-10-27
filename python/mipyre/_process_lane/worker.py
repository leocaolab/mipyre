"""A process-lane worker's start: the import worker gets the app itself;
the fork worker inherits it from the template. Either way it ends in the engine's loop
(`MipyreApp._serve_lane_worker`) and never comes back.

`python -m mipyre._process_lane.worker` is the entry of a worker the SpawnDriver
started (macOS); the ForkDriver's workers come in through `run` in the
forked template.
"""

from __future__ import annotations

import os
import signal
import sys
import traceback
from collections.abc import Callable
from typing import Any

from mipyre.engine import CommandLine, _LaneSet, _set_routers_to_load

from mipyre._process_lane.server import START_REFUSED, LaneSetup, LaneWorker, refuse_unpinned_start, unpin_env


def _leave_template(sock, template_pid: int, template_fds: tuple[int, ...]) -> None:
    """Drop what the template had and a worker must not: its socket, its signal wakeup pipe and
    selector (`template_fds`), its signal handlers."""
    if sock is not None:
        sock.close()
    signal.set_wakeup_fd(-1)
    for fd in template_fds:
        os.close(fd)
    for sig in (signal.SIGCHLD, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # the server stops workers, not a Ctrl-C
    _die_with_parent()
    if os.getppid() != template_pid:
        os._exit(0)  # the template died before the death signal was armed


def install_command_line(text: str) -> None:
    """Make the run's command line (`CommandLine.encode()` text) this process's: its `sys.argv`
    and its flag values."""
    from mipyre import flags

    flags._install(CommandLine.decode(text))


def load_app(setup: LaneSetup):
    """Imports the app as the server did, with its `sys.path` and only the routers that have
    process routes, after the thread-pool limits the libraries read when they load. The run's
    command line is installed first."""
    os.environ.update(setup.thread_limits)
    os.chdir(setup.cwd)
    sys.path[:] = setup.sys_path
    _set_routers_to_load(list(setup.routers))
    from mipyre import _target

    _module, app = _target.import_target(_target.parse_target(setup.target))
    return app


class _ReportedImportFailure(Exception):
    """The app's import failed in this worker and the failure went to the server, which prints it."""


def import_app(lanes: _LaneSet, worker: LaneWorker):
    """`start="import"`: the app, imported here with the run's command line; a failed import is
    reported to the server, which says it once for every worker, and the worker ends without
    printing it."""
    try:
        install_command_line(worker.command_line)
        return load_app(worker.setup)
    except BaseException as failure:
        lanes._report_start_failure(traceback.format_exc())
        raise _ReportedImportFailure from failure


def attach(worker: LaneWorker) -> _LaneSet:
    """Map `worker`'s lane from the fds the template handed it. A worker that cannot map its lane
    cannot report into it either: it refuses its start (START_REFUSED), which the server takes as
    final for the lane."""
    setup = worker.setup
    shm, bell = worker.fds
    try:
        # A spawned worker got its pair inheritable; the bell stays open for the worker's life,
        # and a program the app runs must not get it.
        os.set_inheritable(bell, False)
        return _LaneSet._attach(worker.lane, worker.attempt, setup.depth, setup.slot_bytes, setup.worker_spin_us, shm, bell)
    except BaseException:  # noqa: BLE001 — printed; the process exits START_REFUSED
        traceback.print_exc()
        sys.stderr.flush()
        os._exit(START_REFUSED)


def run(worker: LaneWorker, sock, template_fds: tuple[int, ...], app_of: Callable[[_LaneSet], Any]) -> None:
    """Leave the template, map the lane, then serve the app `app_of` returns until the process ends.

    Each worker reseeds before the app is had: a forked worker drops the random state it copied
    from the template, and an imported one keeps any seed its app sets at import.
    """
    try:
        _leave_template(sock, worker.template_pid, template_fds)
        lanes = attach(worker)
        _log_as_the_server(worker)
        _reseed()
        app_of(lanes)._engine._serve_lane_worker(lanes, worker.template_pid)
        print(f"mipyre: process-lane worker {worker.lane} left its loop", file=sys.stderr, flush=True)
    except _ReportedImportFailure:
        pass
    except BaseException:  # noqa: BLE001 — a worker never returns into the template
        traceback.print_exc()
        sys.stderr.flush()
    finally:
        os._exit(70)


def _log_as_the_server(worker: LaneWorker) -> None:
    """Route this worker's Python logging, its app's import included, into the engine's log at
    the server's level and format, each line with the worker's lane: the same lines a
    sub-interpreter worker writes. Each line is written by the call that logs it, with no writer
    thread: a worker the server kills (a handler past its timeout) loses none."""
    from mipyre._log import bridge
    from mipyre.engine import _python_log_level, _stop_log_writer, emit_python_log, init_logger

    init_logger(worker.setup.log_level, False, worker.setup.log_format)
    _stop_log_writer()
    bridge.install_root_handler(emit_python_log, _python_log_level(), lane=worker.lane)


def _spawned_main(argv: list[str]) -> None:
    """A worker the SpawnDriver started: `argv` is `--setup-fd N`, the inherited socket its
    template writes its setup to; its lane's two fds are inherited too, and its `mipyre` is the
    launcher's pinned copy (the first PYTHONPATH entry), which the processes the app starts do
    not inherit. A setup it cannot read, or one from another build, ends it with START_REFUSED
    before it touches a lane."""
    run_dir = os.environ.get("PYTHONPATH", "").split(os.pathsep)[0]
    try:
        worker = LaneWorker.decode(_read_setup(argv))
    except (SetupFdError, OSError, ValueError, KeyError, TypeError):  # each says what was wrong
        traceback.print_exc()
        sys.stderr.flush()
        os._exit(START_REFUSED)
    refuse_unpinned_start(run_dir, worker.build)
    unpin_env(os.environ, run_dir)
    run(worker, None, (), lambda lanes: import_app(lanes, worker))


class SetupFdError(Exception):
    """A spawned worker's command line does not name its setup fd as `--setup-fd N`."""


def _read_setup(argv: list[str]) -> str:
    """Reads the setup the template writes to the fd `argv` names, to its end (the template
    closes its side once it is written)."""
    if len(argv) != 2 or argv[0] != "--setup-fd" or not argv[1].isdigit():
        raise SetupFdError(f"a spawned process-lane worker takes --setup-fd N, got {argv!r}")
    with os.fdopen(int(argv[1]), "rb") as setup:
        return setup.read().decode()


def _die_with_parent() -> None:
    """Linux: the kernel kills this worker when the template dies (PR_SET_PDEATHSIG), even
    while it is stuck in a handler. macOS has no such call: there a worker notices only
    while it waits for a request (it checks its parent between waits), and the template
    kills its workers when it is stopped or loses the server."""
    if sys.platform != "linux":
        return
    import ctypes

    libc = ctypes.CDLL(None, use_errno=True)
    PR_SET_PDEATHSIG = 1
    if libc.prctl(PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0) != 0:
        err = ctypes.get_errno()
        raise OSError(err, f"prctl(PR_SET_PDEATHSIG): {os.strerror(err)}")


def _reseed() -> None:
    """Every worker starts from the template's random state; give each its own."""
    import random

    random.seed()
    np = sys.modules.get("numpy")
    if np is not None:
        np.random.seed()


if __name__ == "__main__":
    _spawned_main(sys.argv[1:])
