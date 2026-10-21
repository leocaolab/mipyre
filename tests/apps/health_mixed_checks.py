"""App for tests/test_health.py::test_readyz_aggregates_multiple_checks. Its own module:
a worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()


@app.readiness_check("ok1")
def _():
    return True


@app.readiness_check("bad")
def _():
    raise ValueError("nope")


@app.readiness_check("ok2")
def _():
    return "healthy"

app.enable_health_probes()
