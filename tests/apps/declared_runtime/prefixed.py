"""The mixed app's API and analytics routers, each included under a prefix, for
tests/test_declared_runtime.py."""

import os
import sys

from mipyre import Mipyre
from mipyre.engine import _in_worker

from tests.apps.declared_runtime import imports_log

imports_log.record("prefixed")

app = Mipyre()
app.include_router("tests.apps.declared_runtime.api:router", prefix="/v1")
app.include_router("tests.apps.declared_runtime.analytics:router", prefix="/stats")


@app.get("/routers")
def routers(req):
    """Name the router modules imported where this route runs: a sub-interpreter."""
    loaded = [name for name in ("api", "analytics") if f"tests.apps.declared_runtime.{name}" in sys.modules]
    return {"pid": os.getpid(), "subinterp": _in_worker(), "loaded": loaded}
