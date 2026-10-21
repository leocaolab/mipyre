"""One file, routes on two runtimes (design §3.4): the top imports nothing a
sub-interpreter can't load; the process route imports one inside its function."""

import os

from mipyre import Mipyre
from mipyre.engine import _in_worker

app = Mipyre()


@app.get("/light")
def light(req):
    return {"pid": os.getpid(), "subinterp": _in_worker()}


@app.get("/heavy", runtime="process")
def heavy(req):
    from tests.apps.declared_runtime import heavy

    return {"pid": os.getpid(), "subinterp": _in_worker(), "sum": int(heavy.mean(range(10)) * 10)}
