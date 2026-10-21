"""Mixed app for tests/test_declared_runtime.py: API on sub-interpreters, analytics on
process lanes, admin on the main interpreter — one Router module each, this module light."""

import os

from mipyre import Mipyre
from mipyre.engine import _in_worker

from tests.apps.declared_runtime import imports_log

imports_log.record("main")

app = Mipyre(
    routers=[
        "tests.apps.declared_runtime.api:router",
        "tests.apps.declared_runtime.analytics:router",
        "tests.apps.declared_runtime.admin:router",
    ]
)


@app.get("/where")
def where(req):
    """Declared on the app: its default runtime (sub-interpreter)."""
    return {"pid": os.getpid(), "subinterp": _in_worker()}
