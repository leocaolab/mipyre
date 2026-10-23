"""App for tests/test_observability.py::test_enable_request_id_header_idempotent. Its own
module: a worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()
app.enable_request_id_header()
app.enable_request_id_header()  # the same header again: no error, one header line


@app.get("/")
def root(req):
    return "ok"
