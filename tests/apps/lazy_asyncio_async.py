"""App for tests/test_lazy_asyncio.py: one `async def` handler. Its own module: a worker
serves one app per module."""

import sys

from mipyre import Mipyre

app = Mipyre()


@app.get("/asyncio-loaded")
def asyncio_loaded(req):
    return str("asyncio" in sys.modules)


@app.get("/async")
async def async_route(req):
    return "async ran"
