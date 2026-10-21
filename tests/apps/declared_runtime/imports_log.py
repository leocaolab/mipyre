"""Records which modules each interpreter imported, one line per import, in the file named
by DECLARED_RUNTIME_LOG (the test reads it)."""

import os


def record(module: str) -> None:
    from mipyre.engine import _in_worker

    path = os.environ.get("DECLARED_RUNTIME_LOG")
    if not path:
        return
    where = "subinterp" if _in_worker() else "main-interpreter"
    with open(path, "a") as f:
        f.write(f"{module} {os.getpid()} {where}\n")
