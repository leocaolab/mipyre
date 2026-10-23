"""App for tests/test_health.py::test_readyz_503_on_false_return. Its own module: a
worker serves one app per module."""

from mipyre import HealthProbes, Mipyre

app = Mipyre()
probes = HealthProbes(app)


@probes.check("feature_flag")
def _():
    return False
