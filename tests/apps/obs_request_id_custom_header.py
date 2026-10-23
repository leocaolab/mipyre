"""App for tests/test_observability.py::test_custom_header_name. Its own module: a
worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()
app.enable_request_id_header(header="X-Trace-Id")


@app.get("/")
def root(req):
    return "ok"
