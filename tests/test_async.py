"""Test: async def handlers in both GIL and sub-interpreter modes.

Run it with MIPYRE_TEST_MODE=main to serve every handler on the main interpreter (the GIL
mode); without it, it serves on sub-interpreters."""
import os

from mipyre import Mipyre, ServerFlags

app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))


@app.get("/sync")
def sync_handler(req):
    return "sync ok"


@app.get("/async")
async def async_handler(req):
    import asyncio
    await asyncio.sleep(0.001)
    return "async ok"


@app.get("/async-json")
async def async_json(req):
    import asyncio
    await asyncio.sleep(0.001)
    return {"status": "async", "value": 42}


if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0))
