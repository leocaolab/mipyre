"""App for tests/test_lazy_asyncio.py: only `def` routes, and the module imports asyncio
at the top. `asyncio.get_event_loop()` in a def handler is not supported (mipyre's
NoEventLoopError); running a coroutine on a loop of the handler's own is. Run as a script
(port 0): the test reads the server's log."""

import asyncio

from mipyre import Mipyre

app = Mipyre()


async def _work():
    return "ran"


@app.get("/get-event-loop")
def get_event_loop(req):
    return type(asyncio.get_event_loop()).__name__


@app.get("/asyncio-run")
def asyncio_run(req):
    return asyncio.run(_work())


@app.get("/new-event-loop")
def new_event_loop(req):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_work())
    finally:
        loop.close()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, threads=1)
