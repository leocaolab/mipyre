"""Creating and dropping apps with a fast response leaves nothing of them behind.

A fast response's bytes belong to the app's route table and are freed with it: that is
counted exactly in Rust (`plan/route.rs`, `every_registered_body_is_freed_with_its_table`:
each registered body's drop, == the number registered). Here, the Python side of the same
registration: an app that registered one, once dropped, leaves no block of the object allocator
(`sys.getallocatedblocks()`) behind. Until b/441 both were one RSS threshold (`2 * 8 MiB` over 7
apps here, 32 MiB over 200 apps in test_fast_response.py), which a leak had to cross several
registrations to trip.

Servers are not started: starting and stopping one is test_restart_memory's.

Why a subprocess: the count is of the whole process's allocator, which the test process's other
threads and earlier tests would move.
"""

import json
import subprocess
import sys

APPS = 200

_SCRIPT = r"""
import gc, json, sys

from mipyre import Mipyre


def one_app(kept):
    app = Mipyre()
    app.add_fast_response("GET", "/big", b"x" * 1024, content_type="application/octet-stream")
    kept.append(app)


def blocks_left(apps, keep):
    # The allocator's blocks after `apps` apps were made (and dropped, unless `keep`), minus
    # before. With no app it is what the measurement itself leaves (its `before`).
    kept = []
    gc.collect()
    before = sys.getallocatedblocks()
    for _ in range(apps):
        one_app(kept)
        if not keep:
            kept.clear()
    gc.collect()
    return sys.getallocatedblocks() - before


blocks_left(20, False)  # first-use caches (the engine module's, the allocator's pools)
print(json.dumps({
    "none": blocks_left(0, False),
    "dropped": blocks_left(APPS, False),
    "kept": blocks_left(APPS, True),
}))
""".replace("APPS", str(APPS))


def _blocks_left() -> dict:
    out = subprocess.run([sys.executable, "-c", _SCRIPT], capture_output=True, text=True, timeout=None)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout.strip().splitlines()[-1])


def test_dropped_apps_with_a_fast_response_leave_no_block():
    """`APPS` apps made and dropped leave exactly what no app leaves; and the count sees apps
    kept alive, at least one block each (the run's own input: the same apps, not dropped)."""
    left = _blocks_left()
    assert left["dropped"] == left["none"], left
    assert left["kept"] - left["none"] >= APPS, left
