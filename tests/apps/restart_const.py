"""An app module holding a 1 MiB constant, served and stopped over and over by
tests/test_restart_memory.py.

In a worker, when `RESTART_MARKS` names a directory, the app writes one file there when it is
freed, so the test counts the workers' apps that were."""

import os

import mipyre.engine
from mipyre import Mipyre

BIG = b"x" * (1 << 20)


class _FreedMark:
    """Writes its file when it is freed, which is when the app holding it is."""

    def __init__(self, path):
        self.path = path

    # The os functions are bound now: this may run while the interpreter's modules are being
    # torn down.
    def __del__(self, open_=os.open, close=os.close, flags=os.O_CREAT | os.O_EXCL | os.O_WRONLY):
        close(open_(self.path, flags))


class _MarkedApp(Mipyre):
    freed_mark: _FreedMark | None = None


app = _MarkedApp()


@app.get("/")
def index(req):
    return "ok"


_marks = os.environ.get("RESTART_MARKS")
if _marks and mipyre.engine._in_worker():
    app.freed_mark = _FreedMark(os.path.join(_marks, f"{os.getpid()}-{os.urandom(8).hex()}"))
    if os.environ.get("RESTART_KEEP_APP"):
        # This file's own leak, for the test to see: one reference to the app that nothing
        # gives back, so it outlives its interpreter as the app -> handler -> globals -> app
        # cycle did while the GC could not see the route table's references.
        import ctypes

        ctypes.pythonapi.Py_IncRef(ctypes.py_object(app))
