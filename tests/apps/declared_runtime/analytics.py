import os

from tests.apps.declared_runtime import heavy  # a process router may import what a sub-interpreter can't load

from mipyre import Router
from mipyre.engine import _in_worker

from tests.apps.declared_runtime import imports_log

imports_log.record("analytics")
router = Router(runtime="process")


@router.get("/analytics")
def analytics(req):
    return {"pid": os.getpid(), "subinterp": _in_worker(), "mean": heavy.mean([0, 1, 2, 3, 4])}


@router.get("/analytics/main", runtime="main")
def analytics_on_main(req):
    """A route can override its router's runtime."""
    return {"pid": os.getpid(), "subinterp": _in_worker()}
