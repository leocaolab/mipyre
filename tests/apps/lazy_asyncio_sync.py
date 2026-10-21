"""App for tests/test_lazy_asyncio.py: no `async def` handler or hook. Its own module: a
worker serves one app per module."""

import sys

from mipyre import Mipyre

app = Mipyre()


@app.get("/asyncio-loaded")
def asyncio_loaded(req):
    return str("asyncio" in sys.modules)


@app.get("/awaitable")
def awaitable(req):
    # A `def` handler may still return an awaitable: the worker's loop is made for it then.
    async def run():
        return "ran"

    return run()
