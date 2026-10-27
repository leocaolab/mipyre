"""Main-interpreter routes while workers execute the real engine.

Every worker imports the real `mipyre` package and engine, so more than one interpreter has
executed the engine. From that moment the PyO3 fork refuses a bare foreign-thread
`Python::attach` (and `Py<T>` drop); GIL-bridge threads, main's async engine (WebSocket
connections) and `/metrics` must keep serving the main interpreter.
"""
import os

import mipyre.engine as _engine
from mipyre import Mipyre, ServerFlags

app = Mipyre()
app.enable_metrics()


@app.get("/w")
def worker_route(req):
    # Served by a worker, which runs the real engine.
    return {"real_engine": _engine._in_worker() and hasattr(_engine, "_worker_take")}


@app.get("/g", runtime="main")
def main_sync(req):
    return {"main": "sync"}


@app.get("/ga", runtime="main")
async def main_async(req):
    import asyncio

    await asyncio.sleep(0)
    return {"main": "async"}


@app.websocket("/ws")
async def echo(sock):
    while True:
        m = await sock.recv()
        if m is None:
            break
        await sock.send("echo:" + m)


if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=int(os.environ["L2_PORT"])))
