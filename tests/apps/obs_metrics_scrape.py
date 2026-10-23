"""App for tests/test_observability.py::test_metrics_scrape_is_counted. Its own
module: a worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()
app.enable_metrics()


@app.get("/")
def root(req):
    return "ok"
