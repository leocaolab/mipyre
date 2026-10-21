"""The app tests/test_worker_startup_modules.py serves: one route that answers with the modules its worker holds.

A module of its own, importing nothing but `sys` and mipyre: a sub-interpreter worker rebuilds the app by executing
this file, so what the worker imports for it is mipyre's own start and nothing a test file brings in.
"""

import sys

from mipyre import Mipyre

app = Mipyre()


@app.get("/")
def modules(req):
    return sorted(sys.modules)
