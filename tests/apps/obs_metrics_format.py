"""App for tests/test_observability.py::test_metrics_format_has_help_and_type_lines. Its
own module: a worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()
app.enable_metrics()


@app.get("/")
def root(req):
    return "ok"
