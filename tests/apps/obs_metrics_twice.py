"""App for tests/test_observability.py::test_enable_metrics_idempotent. Its own module:
a worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()
app.enable_metrics()
app.enable_metrics()  # no duplicate route


@app.get("/")
def root(req):
    return "ok"
