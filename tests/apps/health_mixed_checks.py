"""App for tests/test_health.py::test_readyz_aggregates_multiple_checks. Its own module:
a worker serves one app per module."""

from mipyre import HealthProbes, Mipyre

app = Mipyre()
probes = HealthProbes(app)


@probes.check("ok1")
def _():
    return True


@probes.check("bad")
def _():
    raise ValueError("nope")


@probes.check("ok2")
def _():
    return "healthy"
