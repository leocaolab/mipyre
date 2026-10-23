"""App for tests/test_process_lane_start_failures.py: its startup hook fails, so `mipyre run`
exits right after it handed the template its lanes, while a spawned template may still be
importing `mipyre` from the run directory."""

from mipyre import Mipyre

app = Mipyre(runtime="process")


@app.on_startup
def fails():
    raise RuntimeError("the app's own startup error")


@app.get("/hello")
def hello(req):
    return "hello"
