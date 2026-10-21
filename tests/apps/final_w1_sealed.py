"""App for tests/test_final_w1.py (F3): hooks registered after the first server started
are refused. Served on the main interpreter (runtime="main")."""

from mipyre import Mipyre

app = Mipyre(runtime="main")


@app.get("/")
def index(req):
    return "ok"
