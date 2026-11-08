"""Async engine — run in each async sub-interpreter worker, and on the main interpreter for
its `async def` routes, on one thread with one stock asyncio loop.

Rust's queue of requests sits under the loop as a doorbell fd the loop waits on with its other
readers; when it is readable the engine takes what is queued and starts a Task per request
(`async_requests.serve`). It runs as the module `__mipyre_async_engine__`, in its own namespace;
Rust sets in it before it runs:

- `INBOX`: this engine's end of its request queue;
- `HANDLERS`: the handlers indexed like main's route table, `None` for a route this engine
  does not serve;
- `BEFORE_HOOKS`, `AFTER_HOOKS`: the app's hooks;
- `UNHOOKED`: the types of a handler's value sent as returned, after hooks unrun: main's
  `Stream`, none in a worker (which cannot stream).

Each request comes as a job that carries where its answer goes; the engine answers it
with `_worker_send` / `_worker_fail`. A job the engine lets go of unanswered (its task
cancelled when the loop stops) answers itself with an error, so no caller waits for a
request that will never run. The request budget is Rust's: past it the caller answers 504.

In a worker the handlers and hooks are the ones the user's script registered on the
worker's app; on main they are the app's own.
"""

import asyncio

import mipyre.engine as _engine
from mipyre._worker import async_requests

# Set in this namespace by Rust before the module runs (see the docstring); read here by name, so a
# missing one is a KeyError naming it when the worker starts, and each has its type.
INBOX: _engine._AsyncInbox = globals()["INBOX"]
HANDLERS: list = globals()["HANDLERS"]
BEFORE_HOOKS: list = globals()["BEFORE_HOOKS"]
AFTER_HOOKS: list = globals()["AFTER_HOOKS"]
UNHOOKED: tuple = globals()["UNHOOKED"]


# One job's coroutine. Each runs as its own Task, so per-request state kept in ContextVars
# (observability's request id, mipyre.context) stays apart from concurrent requests on this
# loop. How the request ends is `async_requests.job_runner`'s; the engine logs a failure
# once, with its traceback and the request id (the client gets a generic 500).
_process_request = async_requests.job_runner(
    HANDLERS, BEFORE_HOOKS, AFTER_HOOKS, _engine._worker_to_response,
    _engine._worker_send, _engine._worker_fail,
    unhooked=UNHOOKED,
)


def _take():
    return _engine._worker_take(INBOX)


asyncio.run(async_requests.serve(INBOX.doorbell, _take, _process_request))
