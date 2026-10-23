"""App for tests/test_start_io_checks.py: the first process to import it (the server) leaves
the marker file named by LANE_TEST_MARKER; every later import (a process worker's) finds it and
raises, as an app whose import fails only in a worker does."""

import os

from mipyre import Mipyre

_marker = os.environ["LANE_TEST_MARKER"]
if os.path.exists(_marker):
    raise RuntimeError("the lane worker's own import error")
with open(_marker, "w") as f:
    f.write(str(os.getpid()))

app = Mipyre(runtime="process")


@app.get("/hello")
def hello(req):
    return "hello"
