"""App for tests/test_server_topology.py: the `/together` round of `tpc_async_rounds`, with at most two
of its requests let into it at a time (the test's own input: what the rendezvous sees when awaits
overlap two at a time instead of four). A short request budget answers the requests that can never
complete their round. Its own module: a worker serves one app per module."""

import asyncio

from mipyre import Mipyre

app = Mipyre()
app.request_timeout = 0.5

_two = asyncio.Semaphore(2)
# Requests in the round now: one that got its 504 has left it (its task was cancelled), so the
# two let in after it do not count it.
_in_round = [0]
_all_in = asyncio.Event()


@app.get("/together")
async def together(req):
    async with _two:
        _in_round[0] += 1
        try:
            if _in_round[0] == int(req.query_params["n"]):
                _all_in.set()
            await _all_in.wait()
            return str(_in_round[0])
        finally:
            _in_round[0] -= 1
