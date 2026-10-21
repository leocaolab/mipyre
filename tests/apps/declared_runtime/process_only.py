"""All routes on process lanes or main: no route runs on a sub-interpreter, so the server
builds none (b/34), and this module is never imported by one."""

import os

from mipyre import Mipyre
from mipyre.engine import _in_worker

from tests.apps.declared_runtime import imports_log

imports_log.record("process_only")
app = Mipyre(runtime="process")


@app.get("/p")
def p(req):
    return {"pid": os.getpid(), "subinterp": _in_worker()}


@app.get("/m", runtime="main")
def m(req):
    return {"pid": os.getpid(), "subinterp": _in_worker()}
