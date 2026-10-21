"""App for tests/test_health.py::test_readyz_ok_with_passing_checks. Its own module: a
worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()


@app.readiness_check("always_ok")
def _():
    return True


@app.readiness_check("none_also_ok")
def _():
    return None  # None is fine — only False / exception fail

app.enable_health_probes()
