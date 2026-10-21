"""App for tests/test_health.py::test_check_registered_after_enable_still_runs. Its own
module: a worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()
app.enable_health_probes()


@app.readiness_check("late")
def _():
    raise RuntimeError("late check ran")
