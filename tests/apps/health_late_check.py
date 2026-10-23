"""App for tests/test_health.py::test_check_registered_after_the_probes_still_runs. Its own
module: a worker serves one app per module."""

from mipyre import HealthProbes, Mipyre

app = Mipyre()
probes = HealthProbes(app)


@probes.check("late")
def _():
    raise RuntimeError("late check ran")
