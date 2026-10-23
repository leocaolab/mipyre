"""App for tests/test_health.py::test_readyz_ok_when_no_checks. Its own module: a worker
serves one app per module."""

from mipyre import HealthProbes, Mipyre

app = Mipyre()
HealthProbes(app)
