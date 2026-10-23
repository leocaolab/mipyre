"""App for tests/test_health.py::test_async_readiness_check_supported. Its own module: a
worker serves one app per module."""

from mipyre import HealthProbes, Mipyre

app = Mipyre()
probes = HealthProbes(app)


@probes.check("async_ok")
async def _():
    return True


@probes.check("async_fail")
async def _():
    raise ConnectionError("timeout")
