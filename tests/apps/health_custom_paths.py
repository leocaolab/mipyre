"""App for tests/test_health.py::test_custom_paths. Its own module: a worker serves one
app per module."""

from mipyre import HealthProbes, Mipyre

app = Mipyre()
HealthProbes(app, livez_path="/_alive", readyz_path="/_ready")
