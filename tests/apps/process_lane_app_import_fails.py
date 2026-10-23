"""App for tests/test_process_lane_start_failures.py: it registers a process route, then its
import fails, so `mipyre run` exits before it hands the template its lanes."""

from mipyre import Mipyre

app = Mipyre(runtime="process")


@app.get("/hello")
def hello(req):
    return "hello"


raise RuntimeError("the app's own import error")
