"""App for tests/test_lazy_worker_imports.py: no `async def` handler or hook, no cookie, no isolated
extension. Its own module: a worker serves one app per module."""

import sys

from mipyre import Mipyre

# What a worker used to import for everyone (a90a637a): importlib.metadata for `__version__` (~1.3 MiB),
# email.utils for the cookie expires formatter (~0.4 MiB), ctypes in the bootstrap (~0.2 MiB).
DEFERRED = ("importlib.metadata", "email.utils", "ctypes")

app = Mipyre()


@app.get("/deferred-loaded")
def deferred_loaded(req):
    return ",".join(name for name in DEFERRED if name in sys.modules)


@app.get("/import-email-utils")
def import_email_utils(req):
    # The import is the point (the handler loads the module the test then looks for).
    __import__("email.utils")

    return "imported"


@app.get("/modules")
def modules(req):
    return str(len(sys.modules))
