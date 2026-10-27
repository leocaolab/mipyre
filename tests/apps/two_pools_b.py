"""The second of the two apps tests/test_worker_lifecycle.py serves at once in pool mode (see
two_pools_a.py)."""

import asyncio

from mipyre import Mipyre

app = Mipyre()


# TestClient's readiness probe hits "/".
@app.get("/")
def root(req):
    return {"ready": True}


@app.get("/which")
async def which(req):
    await asyncio.sleep(0)
    return {"app": "b"}
