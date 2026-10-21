"""App for tests/test_process_lane_start_failures.py: the first process to import it (the
server) leaves the marker file named by LANE_TEST_MARKER; every later import (a worker's)
finds it and ends at once with status 3, leaving no start failure behind."""

import os

from mipyre import Mipyre

_marker = os.environ["LANE_TEST_MARKER"]
if os.path.exists(_marker):
    os._exit(3)
with open(_marker, "w") as f:
    f.write(str(os.getpid()))

app = Mipyre(runtime="process")


@app.get("/hello")
def hello(req):
    return "hello"
