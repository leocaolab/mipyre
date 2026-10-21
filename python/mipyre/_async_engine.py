"""Async engine — run in each async sub-interpreter worker (pool mode).

It drives a Python asyncio event loop that runs the requests a fetcher thread pulls from
Rust through `mipyre.engine._worker_recv`. It runs as the module
`__mipyre_async_engine__`, in its own namespace; Rust sets in it before it runs:

- `WORKER_ID`: this worker's index (log records);
- `CHANNEL`: this worker's request inbox;
- `TASK_TIMEOUT`: seconds a request's task may run, a little under the request budget
  Rust answers 504 at, so the task is cancelled rather than left computing a result
  nobody takes.

Each request comes as a job that carries where its answer goes; the engine answers it
with `_worker_send` / `_worker_fail` / `_worker_timed_out`. A job the engine lets go of
unanswered (its task cancelled when the engine stops) answers itself with an error, so
no caller waits for a request that will never run.

The handlers and hooks are the ones the user's script registered on its app, indexed
like the main interpreter's routes.
"""

import asyncio
import functools

import mipyre.engine as _engine
from mipyre import _async_requests

# Set in this namespace by Rust before the module runs (see the docstring); read here by name, so a
# missing one is a KeyError naming it when the worker starts, and each has its type.
WORKER_ID: int = globals()["WORKER_ID"]
CHANNEL: _engine._AsyncInbox = globals()["CHANNEL"]
TASK_TIMEOUT: float = globals()["TASK_TIMEOUT"]

# Fetched once: the script has run, so the app's table is final.
_HANDLERS = _engine._worker_app_handlers()
_HOOKS = _engine._worker_app_hooks()
_BEFORE_HOOKS, _AFTER_HOOKS = _HOOKS.before, _HOOKS.after


# One job's coroutine. Each runs as its own Task, so per-request state kept in ContextVars
# (observability's request id, mipyre.context) stays apart from concurrent requests on this
# loop. How the request ends is `_async_requests.job_runner`'s; the engine logs a failure
# once, with its traceback and the request id (the client gets a generic 500), and answers a
# timeout 504.
_process_request = _async_requests.job_runner(
    _HANDLERS, _BEFORE_HOOKS, _AFTER_HOOKS, _engine._worker_to_response, TASK_TIMEOUT,
    _engine._worker_send, _engine._worker_fail, _engine._worker_timed_out,
)


async def _mipyre_engine():
    loop = asyncio.get_running_loop()
    t = _async_requests.Fetcher(
        f"mipyre-async-fetcher-{WORKER_ID}",
        functools.partial(_engine._worker_recv, CHANNEL),
        _process_request,
        _async_requests.scheduler(loop),
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
        await _async_requests.end_pending(pending, TASK_TIMEOUT, WORKER_ID)
        await loop.shutdown_asyncgens()
    if t.error is not None:
        raise t.error


asyncio.run(_mipyre_engine())
