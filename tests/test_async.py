"""Test: async def handlers in both GIL and sub-interpreter modes.

Run it with MIPYRE_RUNTIME=main to serve every handler on the main interpreter (the GIL
mode); without it, it serves on sub-interpreters."""
from mipyre import Mipyre

app = Mipyre()


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
    app.run(host="127.0.0.1", port=0)
