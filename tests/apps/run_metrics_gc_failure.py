"""An app whose first ``gc.collect()`` in any worker fails (the marker file named by
``RUN_METRICS_GC_MARKER`` is claimed once), retiring that worker. The main interpreter's
collect stays real."""

import gc
import os

from mipyre import Mipyre
from mipyre.engine import _in_worker

_real_collect = gc.collect


def _collect(*args):
    try:
        fd = os.open(os.environ["RUN_METRICS_GC_MARKER"], os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return _real_collect(*args)
    os.close(fd)
    raise MemoryError("run-metrics-injected")


if _in_worker():
    gc.collect = _collect

app = Mipyre()


@app.get("/work")
def work(req):
    return "done"
