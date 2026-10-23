"""App for tests/test_health.py::test_readyz_503_on_exception. Its own module: a worker
serves one app per module."""

from mipyre import HealthProbes, Mipyre

app = Mipyre()
probes = HealthProbes(app)


@probes.check("db")
def _():
    raise RuntimeError("connection refused")
