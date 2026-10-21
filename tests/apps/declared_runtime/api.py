import os

from mipyre import Router
from mipyre.engine import _in_worker

from tests.apps.declared_runtime import imports_log

imports_log.record("api")
router = Router()  # the app's default: sub-interpreters


@router.get("/api")
def api(req):
    return {"pid": os.getpid(), "subinterp": _in_worker()}
