"""Starting and stopping a server over and over in one process frees every worker's app (b/31).

Each start builds the workers' sub-interpreters, each runs the app script; each stop
ends them. A handler's globals hold the app, so app -> handler -> globals -> app is a
cycle through the engine's route table; while the GC could not see the table's
references, that cycle outlived `Py_EndInterpreter`, and CPython keeps every obmalloc
arena of an interpreter that still has a live block. Every cycle then leaked each
worker's whole interpreter: ~8,000 blocks and ~11 MiB per worker.

What is counted: each worker's app writes a file when it is freed (`tests/apps/restart_const.py`),
so after every stop the files number exactly `WORKERS` times the stops so far. Until b/441 this
was the process's allocated blocks and anonymous memory against thresholds (2,000 blocks, 8 MiB
over four cycles), which let a worker keep its app now and then inside the noise; the anonymous
memory given back to the OS, the allocator's part, is measured before a release
(`benchmarks/soak_restart_memory.py`, `dev/release_soak.sh`).

The driver runs in a subprocess so the test process's own servers don't count. It reloads the
app module (a fresh app, as a test suite reloading an app does), serves it with `WORKERS`
workers, makes one request and stops, `CYCLES` times, printing after each stop how many apps
were freed.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

CYCLES = 3
WORKERS = 2

DRIVER = textwrap.dedent(
    """
    import gc, importlib, os, sys
    sys.path.insert(0, {apps!r})
    from mipyre.engine import ServerFlags
    from mipyre.testing import TestClient
    import restart_const as mod

    for _ in range({cycles}):
        mod = importlib.reload(mod)
        with TestClient(mod.app, config=ServerFlags(threads={workers})) as client:
            assert client.get("/").status_code == 200
        del client
        gc.collect()
        print("FREED", len(os.listdir(os.environ["RESTART_MARKS"])), flush=True)
    """
)


def _freed_after_each_stop(marks: Path, **env_extra: str) -> list[int]:
    """How many workers' apps were freed after each stop, in order."""
    apps = str(Path(__file__).parent / "apps")
    env = {**os.environ, "RESTART_MARKS": str(marks), **env_extra}
    done = subprocess.run(
        [sys.executable, "-c", DRIVER.format(apps=apps, cycles=CYCLES, workers=WORKERS)],
        env=env,
        capture_output=True,
        text=True,
        timeout=None,
    )
    assert done.returncode == 0, f"driver failed ({done.returncode}):\n{done.stdout}\n{done.stderr}"
    return [int(line.split()[1]) for line in done.stdout.splitlines() if line.startswith("FREED ")]


def test_every_stop_frees_every_workers_app(tmp_path):
    assert _freed_after_each_stop(tmp_path) == [WORKERS * (i + 1) for i in range(CYCLES)]


def test_the_count_sees_an_app_that_outlives_its_interpreter(tmp_path):
    """The app module's own leak (`RESTART_KEEP_APP`: a reference to each worker's app nothing
    gives back) leaves no app freed, so the count above sees a single kept app."""
    assert _freed_after_each_stop(tmp_path, RESTART_KEEP_APP="1") == [0] * CYCLES
