"""Running a hook's or handler's awaitable on a synchronous path.

A sync path (a worker, a bridge thread) has an event loop of its own, set as the thread's
current one, but not running between requests: a sync handler on the same thread may call
``asyncio.run()``, which refuses to start while another loop runs. An awaitable a hook or
handler returns runs to completion on that loop with ``run``, as a task in the request's own
``contextvars.Context``, so the ``ctx`` writes it makes are seen by the rest of the request.

A task takes only a coroutine; a Cython or mypyc coroutine, or an object with ``__await__``, is
run through ``drive`` instead.
"""

from asyncio import Task


async def drive(awaitable):
    return await awaitable


def run(loop, coro, context):
    """Runs ``coro`` to completion on ``loop`` as a task in ``context``; what it returns or raises.

    What ``loop.run_until_complete(loop.create_task(coro, context=context))`` does, without
    turning the loop for a coroutine that finishes before its first suspension (most hooks):
    the loop is marked running for the call (``_run_forever_setup``, the half of
    ``run_forever`` that CPython keeps apart for loops driven from outside), so the task starts
    eagerly, its first step running inside its construction as a real task (``current_task()``,
    ``get_running_loop()`` and ``asyncio.timeout()`` work as on the loop). A task that suspended
    is driven one loop iteration at a time until it is done.

    A coroutine that finished at once may have scheduled work of its own (a ``create_task`` it
    did not await): the loop is turned once more when anything is ready, as ``run_until_complete``
    does in its last iteration, so that work takes its first step before the request ends, as
    before.
    """
    loop._run_forever_setup()
    task = None
    try:
        if loop._task_factory is None:
            task = Task(coro, loop=loop, context=context, eager_start=True)
        else:
            # A factory decides what a task is; whether it starts eagerly is its call.
            task = loop.create_task(coro, context=context)
        while not task.done():
            # As run_forever: a stop() asked for in the last step ends the run.
            if loop._stopping:
                raise RuntimeError("Event loop stopped before Future completed.")
            loop._run_once()
        if loop._ready or (loop._scheduled and loop._scheduled[0]._when <= loop.time()):
            loop._run_once()
        return task.result()
    except BaseException:
        # As run_until_complete: an exception that ends the call is the caller's to see, not
        # also the task's to log as never retrieved; a task left pending is not reported at
        # its destruction either.
        if task is not None:
            if task.done():
                if not task.cancelled():
                    task.exception()
            else:
                task._log_destroy_pending = False
        raise
    finally:
        loop._run_forever_cleanup()
