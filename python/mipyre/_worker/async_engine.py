"""Async engine — run in each async sub-interpreter worker, and on the main interpreter for
its `async def` routes on the thread-per-core server.

It drives a Python asyncio event loop that runs the requests a fetcher thread pulls from
Rust through `mipyre.engine._worker_recv`. It runs as the module
`__mipyre_async_engine__`, in its own namespace; Rust sets in it before it runs:

- `ENGINE`: which engine this is, `worker-<index>` or `main` (thread names, log records);
- `CHANNEL`: this engine's request inbox;
- `TASK_TIMEOUT`: seconds a request's task may run, a little under the request budget
  Rust answers 504 at, so the task is cancelled rather than left computing a result
  nobody takes;
- `HANDLERS`: the handlers indexed like main's route table, `None` for a route this engine
  does not serve;
- `BEFORE_HOOKS`, `AFTER_HOOKS`: the app's hooks;
- `UNHOOKED`: the types of a handler's value sent as returned, after hooks unrun: main's
  `Stream`, none in a worker (which cannot stream).

Each request comes as a job that carries where its answer goes; the engine answers it
with `_worker_send` / `_worker_fail` / `_worker_timed_out`. A job the engine lets go of
unanswered (its task cancelled when the engine stops) answers itself with an error, so
no caller waits for a request that will never run.

In a worker the handlers and hooks are the ones the user's script registered on the
worker's app; on main they are the app's own.
"""

import asyncio
import functools

import mipyre.engine as _engine
from mipyre._worker import async_requests

# Set in this namespace by Rust before the module runs (see the docstring); read here by name, so a
# missing one is a KeyError naming it when the worker starts, and each has its type.
ENGINE: str = globals()["ENGINE"]
CHANNEL: _engine._AsyncInbox = globals()["CHANNEL"]
TASK_TIMEOUT: float = globals()["TASK_TIMEOUT"]
HANDLERS: list = globals()["HANDLERS"]
BEFORE_HOOKS: list = globals()["BEFORE_HOOKS"]
AFTER_HOOKS: list = globals()["AFTER_HOOKS"]
UNHOOKED: tuple = globals()["UNHOOKED"]


# One job's coroutine. Each runs as its own Task, so per-request state kept in ContextVars
# (observability's request id, mipyre.context) stays apart from concurrent requests on this
# loop. How the request ends is `async_requests.job_runner`'s; the engine logs a failure
# once, with its traceback and the request id (the client gets a generic 500), and answers a
# timeout 504.
_process_request = async_requests.job_runner(
    HANDLERS, BEFORE_HOOKS, AFTER_HOOKS, _engine._worker_to_response, TASK_TIMEOUT,
    _engine._worker_send, _engine._worker_fail, _engine._worker_timed_out,
    unhooked=UNHOOKED,
)


async def _mipyre_engine():
    loop = asyncio.get_running_loop()
    t = async_requests.Fetcher(
        f"mipyre-async-fetcher-{ENGINE}",
        functools.partial(_engine._worker_recv, CHANNEL),
        _process_request,
        async_requests.scheduler(loop),
    )
    t.start()
    try:
        # The fetcher returns when the inbox closes: at shutdown, or when this engine
        # stops (below). No timeout: this wait lasts the worker's whole serving life.
        await t.returned()
    finally:
        # However the engine stops (shutdown, the fetcher failing, the loop dying), close
        # the inbox so the fetcher returns: the interpreter waits for it at its end.
        _engine._worker_close(CHANNEL)
        # Pending tasks and async generators end before Py_EndInterpreter tears the loop
        # down (`end_pending`: cancelled, waited for at most TASK_TIMEOUT).
        loop = asyncio.get_running_loop()
        pending = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        await async_requests.end_pending(pending, TASK_TIMEOUT, ENGINE)
        await loop.shutdown_asyncgens()
    if t.error is not None:
        raise t.error


asyncio.run(_mipyre_engine())
