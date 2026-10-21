"""App for tests/test_observability.py::test_custom_metrics_path. Its own module: a
worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()
app.enable_metrics(path="/_/prom")


@app.get("/")
def root(req):
    return "ok"
