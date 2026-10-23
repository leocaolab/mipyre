"""App for tests/test_hook_chain_paths.py: one hook chain, the same handler on four paths.

The request's ``x-case`` header picks what each hook does. Every step adds its name to
``TRACE`` by setting a new tuple, so a name shows only where the step's ``ContextVar`` write
reached, and a write left over from an earlier request on the same thread shows too. Every
response carries the trace at the time it was made in ``x-trace``."""

import contextvars
import os

from mipyre import Mipyre, Response

TRACE: contextvars.ContextVar[tuple[str, ...]] = contextvars.ContextVar("hook_chain_trace", default=())

app = Mipyre()


class HookError(Exception):
    """The failure a hook raises when its case asks it to."""


def _step(name: str) -> tuple[str, ...]:
    trace = TRACE.get() + (name,)
    TRACE.set(trace)
    return trace


def _answer(status: int, trace: tuple[str, ...], **headers: str) -> Response:
    return Response(body="ok", status_code=status, headers={"x-trace": ",".join(trace), **headers})


@app.before_request
def b1(req):
    trace = _step("b1")
    case = req.headers.get("x-case")
    if case == "b1-answers":
        return _answer(401, trace)
    if case == "b1-raises":
        raise HookError("b1")
    return None


@app.before_request
async def b2(req):
    trace = _step("b2")
    if req.headers.get("x-case") == "b2-answers":
        return _answer(403, trace)
    return None


@app.after_request
def a1(req, resp):
    trace = _step("a1")
    case = req.headers.get("x-case")
    if case == "a1-keeps":
        return None
    if case == "a1-raises":
        raise HookError("a1")
    return _answer(201, trace)


@app.after_request
async def a2(req, resp):
    trace = _step("a2")
    kept = {name: value for name, value in resp.headers.items() if name != "x-trace"}
    return _answer(resp.status_code, trace, **kept, **{"x-a2-saw": resp.headers["x-trace"]})


def _handle(req):
    return _answer(200, _step("h"), **{"x-pid": str(os.getpid())})


async def _handle_async(req):
    return _answer(200, _step("h"), **{"x-pid": str(os.getpid())})


app.get("/subinterp", _handle)
app.get("/main", _handle, runtime="main")
app.get("/async", _handle_async)
app.get("/process", _handle, runtime="process")
