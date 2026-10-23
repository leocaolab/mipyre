"""App for tests/test_observability.py::test_request_id_echoed_when_client_supplies. Its
own module: a worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()
app.enable_request_id_header()


@app.get("/")
def root(req):
    return "ok"
