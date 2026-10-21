"""App for tests/test_log_sampling.py::test_enable_access_log_with_sample_arg. Its own
module: a worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()
app.enable_access_log(level="info", sample=100, always_log_status=400)


@app.get("/h")
def handler(req):
    return "ok"
