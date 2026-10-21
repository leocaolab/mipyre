"""App for tests/test_fast_response.py::test_bytes_body_fast_path_in_mipyre_response.
Its own module: a worker serves one app per module."""

from mipyre import Mipyre, Response

app = Mipyre()


@app.get("/")
def root(req):
    return "probe"


@app.get("/raw")
def raw(req):
    return Response(b"\x00\x01\x02\xff", content_type="application/octet-stream")
