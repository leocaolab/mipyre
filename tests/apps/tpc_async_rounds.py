"""App for tests/test_server_topology.py: a sync route and `async def` routes served by the TPC
sub-interpreter server. Its own module: a worker serves one app per module.

With one TPC thread the async pool has one worker, so every `async def` request runs on one
event loop and shares this module's state."""

import asyncio
import os

from mipyre import Mipyre

app = Mipyre()


@app.get("/fast")
def fast(req):
    # `open`: the gate an awaiting `/until_open` request waits for, opened by this request.
    gate = req.query_params.get("open")
    if gate:
        open(gate, "x").close()
    return "fast"


# Requests of a round of `/together` that have arrived, and the event the round's last one sets.
_rounds: dict[str, tuple[list[int], asyncio.Event]] = {}


@app.get("/together")
async def together(req):
    """Waits until `n` requests of its `round` are in flight at once, then answers how many
    were: `n`, unless a request arrives after the round was let go."""
    arrived, all_in = _rounds.setdefault(req.query_params["round"], ([0], asyncio.Event()))
    arrived[0] += 1
    if arrived[0] == int(req.query_params["n"]):
        all_in.set()
    await all_in.wait()
    return str(arrived[0])


@app.get("/until_open")
async def until_open(req):
    """Says it is in its await (`entered`, a file), then awaits until the `gate` file exists."""
    open(req.query_params["entered"], "x").close()
    while not os.path.exists(req.query_params["gate"]):
        await asyncio.sleep(0.005)
    return "released"
