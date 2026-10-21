"""App for tests/test_observability.py::test_metrics_counts_requests. Its own module: a
worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()
app.enable_metrics()


@app.get("/")
def root(req):
    return "ok"


@app.get("/boom")
def boom(req):
    from mipyre import Response
    return Response(body="broken", status_code=500)
