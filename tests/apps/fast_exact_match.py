"""App for tests/test_fast_response.py::test_fast_exact_match_only. Its own module: a
worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()


@app.get("/")
def root(req):
    return "probe"


# Only registers GET
app.add_fast_response("GET", "/health", b"ok")


@app.post("/health")
def post_health(req):
    return "posted"
