"""App for tests/test_final_w1.py: one route, served by several servers at once (nested
TestClients) and with metrics. Served on the main interpreter
(`runtime="main"`)."""

from mipyre import Mipyre

app = Mipyre(runtime="main")


@app.get("/")
def index(req):
    return "ok"
