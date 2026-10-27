"""What the async engine (`_worker/async_engine.py`) does with the requests Rust hands it, without the engine.

`job_runner` makes the coroutine function that runs one job: the hook chain and the handler, then the job's answer,
given right where the request ends (a response or its failure) to the engine's answer functions the caller passes in.
`serve` starts a Task of it for every job Rust hands over each time the engine's doorbell rings. The loop, the tasks,
their contexts and their end at shutdown are asyncio's; how long a request may take and how many may run at once are
Rust's. Nothing here reads a module global the engine sets, so each part is a call with the inputs it depends on.

This runs once per async request, and every instruction here is in that path's cost: no object
stands for how a request ended (it is answered where it ends), the handler is called without a coroutine of its own, the
answer functions are closure values (no attribute lookup), and a handler's value becomes a
`Response` only when an after hook is there to see it.
"""

from __future__ import annotations

import asyncio
from types import CoroutineType
from typing import Any, Callable, Coroutine, Sequence


def _awaitable(res: Any) -> bool:
    return asyncio.iscoroutine(res) or asyncio.isfuture(res) or hasattr(res, "__await__")


async def _call(fn: Callable, *args: Any) -> Any:
    res = fn(*args)
    # An `async def`'s coroutine is told by its type first: no call for the common case.
    if type(res) is CoroutineType or _awaitable(res):
        res = await res
    return res


class NoHandler(LookupError):
    """A job for a route the worker's app has no handler for: the engine's mistake, never the
    client's; the request fails with it (500), naming the route."""


def _no_handler(route: int) -> Callable[[Any], Any]:
    def handler(req: Any) -> Any:
        raise NoHandler(f"route {route} has no handler on this worker's app")

    return handler


def job_runner(
    handlers: Sequence[Callable | None],
    before: Sequence[Callable],
    after: Sequence[Callable],
    to_response: Callable[[Any], Any],
    send: Callable[[Any, Any], None],
    fail: Callable[[Any, BaseException], None],
    *,
    unhooked: tuple[type, ...] = (),
) -> Callable[[Any], Coroutine[Any, Any, None]]:
    """Return the coroutine function that runs one job (its ``route`` indexes ``handlers``, its
    ``request`` is what the hooks and the handler get) and answers it with ``send`` or
    ``fail``, the engine's answer functions. A route without a handler
    (``None`` in ``handlers``) fails its jobs with `NoHandler`. A handler's value of an
    ``unhooked`` type goes to ``send`` as returned, after hooks unrun (main's `Stream`, as on
    main's other paths)."""
    table = [_no_handler(route) if h is None else h for route, h in enumerate(handlers)]

    async def run(job: Any) -> None:
        """Run the job's hooks and handler in the order and with the semantics of every other
        path (`runtime/hook_chain.rs`): a before hook that returns something short-circuits;
        after hooks get a response and may replace it; a hook that raises fails the request.
        Only the task's own cancellation propagates, as asyncio requires; anything else raised
        is the request's failure. The handler is called here, not through `_call`: one
        coroutine fewer for every request (hooks, where there are any, go through `_call`).

        A value goes to ``send`` as it was returned unless an after hook is there to see it
        as a `Response` (``to_response``): ``send`` maps any value with the same mapping, so
        a `Response` made only to be taken apart again would cost two copies of the body
        and change nothing."""
        try:
            req = job.request
            res = None
            for hook in before:
                res = await _call(hook, req)
                if res is not None:
                    break
            else:
                res = table[job.route](req)
                if type(res) is CoroutineType or _awaitable(res):
                    res = await res
                if after and not isinstance(res, unhooked):
                    res = to_response(res)
                    for hook in after:
                        replaced = await _call(hook, req, res)
                        if replaced is not None:
                            res = to_response(replaced)
        except BaseException as exc:
            if isinstance(exc, (asyncio.CancelledError, GeneratorExit)):
                raise
            fail(job, exc)
            return
        # Sending can fail too (a Rust panic arrives as the engine's RuntimeError): that is
        # this request's failure, handed over the same way, not left to end the task.
        try:
            send(job, res)
        except BaseException as exc:  # noqa: BLE001 — the request's failure, handed to the engine like the handler's own
            fail(job, exc)

    return run


def connection_runner(
    handlers: Sequence[Callable | None],
    before: Sequence[Callable],
    waits: Any,
) -> Callable[[Any], Coroutine[Any, Any, None]]:
    """Return the coroutine function that runs one WebSocket connection's job: the
    ``before_request`` hooks on its upgrade request, which may refuse it with what one returns,
    then its ``async def`` handler (``handlers[job.route]``) on the connection, whose waits are
    ``waits``'s. A hook that raises refuses the upgrade with a 500; a handler that raises is
    logged, the connection having no response left to carry it. A route without a handler here
    is refused like a hook that raised."""

    async def run(job: Any) -> None:
        req = job.request
        try:
            handler = handlers[job.route]
            if handler is None:
                raise NoHandler(f"websocket route {job.route} has no handler on this engine")
            for hook in before:
                res = await _call(hook, req)
                if res is not None:
                    job.refuse(res)
                    return
        except BaseException as exc:
            if isinstance(exc, (asyncio.CancelledError, GeneratorExit)):
                raise
            job.fail(exc)
            return
        ws = job.accept(waits)
        if ws is None:
            return
        try:
            await handler(ws)
        except BaseException as exc:
            if isinstance(exc, (asyncio.CancelledError, GeneratorExit)):
                raise
            job.raised(exc)

    return run


async def serve(
    doorbell: int,
    take: Callable[[], Sequence[Any] | None],
    run: Callable[[Any], Coroutine[Any, Any, None]],
) -> None:
    """Start a Task of ``run(job)`` for every job ``take`` returns, each time the fd
    ``doorbell`` is readable, until ``take`` returns None (the queue closed). Each Task starts
    eagerly: a handler that never awaits is answered before the next job's Task is made.

    An exception from ``take`` ends serving with it: the doorbell may then stay readable, and
    looking again would spin."""
    loop = asyncio.get_running_loop()
    ended = loop.create_future()
    task = asyncio.Task

    def start() -> None:
        try:
            jobs = take()
        except BaseException as exc:  # noqa: BLE001 — ends serving, raised from serve below
            loop.remove_reader(doorbell)
            ended.set_exception(exc)
            return
        if jobs is None:
            loop.remove_reader(doorbell)
            ended.set_result(None)
            return
        for job in jobs:
            task(run(job), loop=loop, eager_start=True)

    loop.add_reader(doorbell, start)
    await ended
