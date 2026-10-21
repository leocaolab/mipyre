"""A process-lane worker's start (design §11.4): the import worker gets the app itself;
the fork worker inherits it from the template. Either way it ends in the engine's loop
(`MipyreApp._serve_lane_worker`) and never comes back.

`python -m mipyre._process_lane.worker` is the entry of a worker the SpawnDriver
started (macOS while b/32 lasts); the ForkDriver's workers come in through `run` in the
forked template.
"""

from __future__ import annotations

import os
import signal
import sys
import traceback
from collections.abc import Callable
from typing import Any

from mipyre.engine import LaneSet, _set_routers_to_load

from mipyre._process_lane import LaneSetup, LaneWorker, adapters, refuse_unpinned_start, unpin_env


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


def load_app(setup: LaneSetup):
    """Imports the app as the server did, with only the routers that have process routes."""
    os.chdir(setup.cwd)
    sys.path[:] = setup.sys_path
    _set_routers_to_load(list(setup.routers))
    from mipyre import _runtime

    _module, app = _runtime.import_target(setup.target)
    return app


def import_app(lanes: LaneSet, worker: LaneWorker):
    """`start="import"`: the adapters, then the app; a failed import is reported to the server."""
    adapters.prepare_all(os.environ)
    try:
        return load_app(worker.setup)
    except BaseException:
        lanes._report_start_failure(worker.lane, traceback.format_exc())
        raise


def run(lanes: LaneSet, worker: LaneWorker, sock, template_fds: tuple[int, ...], app: Callable[[], Any]) -> None:
    """Leave the template, then serve the app `app` returns until the process ends.

    Each worker reseeds before `app` runs: a forked worker drops the random state it copied
    from the template, and an imported one keeps any seed its app sets at import.
    """
    try:
        _leave_template(sock, worker.template_pid, template_fds)
        _reseed()
        app()._engine._serve_lane_worker(lanes, worker.lane, worker.template_pid)
        print(f"mipyre: process-lane worker {worker.lane} left its loop", file=sys.stderr, flush=True)
    except BaseException:  # noqa: BLE001 — a worker never returns into the template
        traceback.print_exc()
        sys.stderr.flush()
    finally:
        os._exit(70)


def _spawned_main() -> None:
    """A worker the SpawnDriver started: its setup is in MIPYRE_LANE_WORKER, its fds
    (the shared memory, the wake pipes) inherited, its `mipyre` the launcher's pinned copy
    (the first PYTHONPATH entry), which the processes the app starts do not inherit."""
    run_dir = os.environ.get("PYTHONPATH", "").split(os.pathsep)[0]
    refuse_unpinned_start(run_dir, os.environ.get("MIPYRE_BUILD_ID"))
    unpin_env(os.environ, run_dir)
    worker = LaneWorker.decode(os.environ.pop("MIPYRE_LANE_WORKER"))
    setup = worker.setup
    try:
        lanes = LaneSet._attach(setup.workers, setup.depth, setup.slot_bytes, setup.worker_spin_us, list(worker.fds))
    except BaseException:  # noqa: BLE001 — printed; the process exits 70
        traceback.print_exc()
        os._exit(70)
    run(lanes, worker, None, (), lambda: import_app(lanes, worker))


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
    _spawned_main()
