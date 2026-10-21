import os

from mipyre import Router
from mipyre.engine import _in_worker

from tests.apps.declared_runtime import imports_log

imports_log.record("admin")
router = Router(runtime="main")


@router.get("/admin")
def admin(req):
    return {"pid": os.getpid(), "subinterp": _in_worker()}
