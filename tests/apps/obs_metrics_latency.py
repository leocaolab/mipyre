"""App for tests/test_observability.py::test_metrics_records_latency. Its own module: a
worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()
app.enable_metrics()


@app.get("/slow")
def slow(req):
    import time
    time.sleep(0.01)
    return "ok"
