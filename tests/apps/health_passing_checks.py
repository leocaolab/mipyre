"""App for tests/test_health.py::test_readyz_ok_with_passing_checks. Its own module: a
worker serves one app per module."""

from mipyre import HealthProbes, Mipyre

app = Mipyre()
probes = HealthProbes(app)


@probes.check("always_ok")
def _():
    return True


@probes.check("none_also_ok")
def _():
    return None  # None is fine — only False / exception fail
