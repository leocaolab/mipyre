"""What the async engine (`_worker/async_engine.py`) does with one request, without the engine.

`job_runner` makes the coroutine function that runs one job: the hook chain and the handler
under the task budget (`Deadlines`), then the job's answer, given right where the request ends
(a response, its failure, its timeout) to the engine's answer functions the caller passes in.
`Fetcher` pulls jobs with the `recv` it is given and hands each one's coroutine to the
`schedule` it is given (`scheduler`: one wake-up of the loop for all the jobs that arrive while
it is busy); `end_pending` ends the tasks still pending when the engine stops. Nothing here
reads a module global the engine sets, so each decision is a call with the inputs it depends on.

This runs once per async request, and every instruction here is in that path's count
(`benchmarks/bench_paths.py`, b/334, b/431, b/598): no object stands for how a request ended
(it is answered where it ends), the handler is called without a coroutine of its own, the
answer functions are closure values (no attribute lookup), a handler's value becomes a
`Response` only when an after hook is there to see it, the budgets share one timer, and the
loop is woken once per batch of jobs rather than once per job.
"""

from __future__ import annotations

import asyncio
import collections
import logging
import threading
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


class Deadlines:
    """The budgets of the request tasks running on one event loop: each task is cancelled
    once ``timeout`` seconds have passed since it started, as under `asyncio.timeout`, with
    one timer for all of them instead of one per task (a `Timeout`, a `TimerHandle` with its
    copied context, and the two coroutines of ``async with``).

    One timer is enough because every budget is the same length: tasks start in the loop's
    order and its clock only goes forward, so the deadlines are in the order the tasks
    started, and the earliest one still to come is the first one in ``_running``. The timer
    is set for it; when it rings, the tasks whose deadline has come
    are cancelled and it is set for the next. A task that ends takes itself out, so the
    timer rings about once per ``timeout`` however many requests start, and nothing keeps
    a finished task."""

    def __init__(self, timeout: float):
        self._timeout = timeout
        # The tasks whose budget has not run out, each with its deadline on its loop's clock,
        # in the order they started.
        self._running: dict[asyncio.Task, float] = {}
        # The tasks whose budget ran out (cancelled by it) and that have not ended yet.
        self._ran_out: set[asyncio.Task] = set()
        # The loop's timer for the earliest deadline still running, while one is set.
        self._timer: asyncio.TimerHandle | None = None

    def start(self, task: asyncio.Task) -> None:
        """``task``'s budget starts now."""
        loop = task.get_loop()
        deadline = loop.time() + self._timeout
        self._running[task] = deadline
        if self._timer is None:
            self._timer = loop.call_at(deadline, self._ring, loop, deadline)

    def end(self, task: asyncio.Task) -> bool:
        """``task``'s budget ends (the task is done with its request); whether it ran out.
        A budget that ran out cancelled its task; that cancellation is taken back here
        (`Task.uncancel`), as `asyncio.timeout` does when its block exits, so
        ``task.cancelling()`` then says whether anything else cancelled it too."""
        if self._running.pop(task, None) is not None:
            return False
        self._ran_out.remove(task)
        task.uncancel()
        return True

    def due(self, now: float) -> tuple[list[asyncio.Task], float | None]:
        """The tasks whose deadline is ``now`` or earlier and had not run out yet, now run
        out, and the earliest deadline still to come (None: no task is running on a budget
        that has not run out)."""
        ran_out: list[asyncio.Task] = []
        next_deadline: float | None = None
        for task, deadline in self._running.items():
            if deadline > now:
                next_deadline = deadline
                break
            ran_out.append(task)
        for task in ran_out:
            del self._running[task]
        self._ran_out.update(ran_out)
        return ran_out, next_deadline

    def _ring(self, loop: asyncio.AbstractEventLoop, deadline: float) -> None:
        # The loop rings a timer when its time is within the clock's resolution of now; what
        # was due at ``deadline`` is due, even if ``loop.time()`` reads a hair earlier.
        self._timer = None
        ran_out, next_deadline = self.due(max(deadline, loop.time()))
        for task in ran_out:
            task.cancel()
        if next_deadline is not None:
            self._timer = loop.call_at(next_deadline, self._ring, loop, next_deadline)


def job_runner(
    handlers: Sequence[Callable | None],
    before: Sequence[Callable],
    after: Sequence[Callable],
    to_response: Callable[[Any], Any],
    timeout: float,
    send: Callable[[Any, Any], None],
    fail: Callable[[Any, BaseException], None],
    timed_out: Callable[[Any], None],
    *,
    unhooked: tuple[type, ...] = (),
) -> Callable[[Any], Coroutine[Any, Any, None]]:
    """The coroutine function that runs one job (its ``route`` indexes ``handlers``, its
    ``request`` is what the hooks and the handler get) and answers it with ``send``,
    ``fail`` or ``timed_out``, the engine's answer functions. A route without a handler
    (``None`` in ``handlers``) fails its jobs with `NoHandler`. A handler's value of an
    ``unhooked`` type goes to ``send`` as returned, after hooks unrun (main's `Stream`, as on
    main's other paths)."""
    table = [_no_handler(route) if h is None else h for route, h in enumerate(handlers)]
    deadlines = Deadlines(timeout)
    current_task = asyncio.current_task

    async def run(job: Any) -> None:
        """The job's hooks and handler under a budget of ``timeout`` seconds, in the order
        and with the semantics of every other path (`runtime/hook_chain.rs`): a before hook
        that returns something short-circuits; after hooks get a response and may replace
        it; a hook that raises fails the request. Only the task's own cancellation
        propagates, as asyncio requires; anything else raised is the request's failure. A
        ``TimeoutError`` the handler raises itself is a failure, not the budget running out.
        The handler is called here, not through `_call`: one coroutine fewer for every
        request (hooks, where there are any, go through `_call`).

        A value goes to ``send`` as it was returned unless an after hook is there to see it
        as a `Response` (``to_response``): ``send`` maps any value with the same mapping, so
        a `Response` made only to be taken apart again would cost two copies of the body
        and change nothing."""
        task = current_task()
        assert task is not None, "a job's coroutine runs as its own Task (the fetcher's create_task)"
        deadlines.start(task)
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
            # The request's failure, handed to the engine. As `asyncio.timeout` decides on exit: the budget's own cancellation, with
            # nothing else cancelling the task, is the timeout; so is a TimeoutError raised
            # after the budget ran out. Any other cancellation (the engine stopping) is the
            # task's own and propagates.
            ran_out = deadlines.end(task)
            if ran_out and (
                isinstance(exc, TimeoutError)
                or (isinstance(exc, asyncio.CancelledError) and task.cancelling() == 0)
            ):
                timed_out(job)
            elif isinstance(exc, (asyncio.CancelledError, GeneratorExit)):
                raise
            else:
                fail(job, exc)
            return
        # A handler that caught the budget's cancellation and returned anyway is answered
        # with what it returned, as under `asyncio.timeout`.
        deadlines.end(task)
        # Sending can fail too (a Rust panic arrives as the engine's RuntimeError): that is
        # this request's failure, handed over the same way, not left to end the task.
        try:
            send(job, res)
        except BaseException as exc:  # noqa: BLE001 — the request's failure, handed to the engine like the handler's own
            fail(job, exc)

    return run


type Schedule = Callable[[Coroutine[Any, Any, None]], object]


def scheduler(loop: asyncio.AbstractEventLoop) -> Schedule:
    """How the fetcher, on its own thread, starts a job's coroutine as a Task on ``loop``.

    Waking the loop from another thread (`call_soon_threadsafe`) costs a handle, a lock and
    a write to the loop's self-pipe; jobs that arrive while the loop is busy share one wake-up:
    the fetcher queues each coroutine and wakes the loop only when no wake-up is already on
    its way, and the loop starts everything queued when it gets there. Not
    `asyncio.run_coroutine_threadsafe`: its `concurrent.futures.Future`, chained to the task
    and resolved under a lock, would be read by nobody (the job carries the answer)."""
    queued: collections.deque[Coroutine[Any, Any, None]] = collections.deque()
    append, popleft = queued.append, queued.popleft
    create_task, call_soon_threadsafe = loop.create_task, loop.call_soon_threadsafe
    # True from the fetcher asking for a wake-up until the loop starts on the queue. No job is
    # left queued without one: the loop clears this before it takes from the queue, so a job
    # queued after its last take finds it cleared and asks for the next.
    waking = False

    def start_queued() -> None:
        nonlocal waking
        waking = False
        while queued:
            create_task(popleft())

    def schedule(coroutine: Coroutine[Any, Any, None]) -> None:
        nonlocal waking
        append(coroutine)
        if not waking:
            waking = True
            call_soon_threadsafe(start_queued)

    return schedule


class Fetcher(threading.Thread):
    """Pulls jobs with ``recv`` (the engine's, which releases the GIL while it waits) and
    hands each one's coroutine, ``coroutine(job)``, to ``schedule``, until ``recv`` returns
    None (the inbox closed). Whatever it raises ends it, kept in ``error`` for the engine to
    raise after the join: it is never retried."""

    def __init__(
        self,
        name: str,
        recv: Callable[[], Any],
        coroutine: Callable[[Any], Any],
        schedule: Callable[[Any], Any],
    ):
        super().__init__(name=name)
        self._recv = recv
        self._coroutine = coroutine
        self._schedule = schedule
        self.error: BaseException | None = None

    def run(self) -> None:
        recv, coroutine, schedule = self._recv, self._coroutine, self._schedule
        try:
            while (job := recv()) is not None:
                schedule(coroutine(job))
        except BaseException as exc:  # noqa: BLE001 — kept for the engine, which raises it after the join
            self.error = exc

    async def returned(self) -> None:
        """Waits, off the event loop, until the fetcher has returned: for as long as it runs.
        The fetcher returns when its inbox closes (at shutdown, or when the engine stops), so
        the wait lasts the engine's whole serving life and has no bound of its own; the bound
        on shutdown is the server's, which joins the engine's thread with its own deadline."""
        await asyncio.to_thread(self.join)


async def end_pending(pending: Sequence[asyncio.Task], timeout: float, engine: str) -> set[asyncio.Task]:
    """End the tasks still pending when the engine stops (background `create_task` work, which
    may hold sockets): cancel each (a request's job answers itself) and wait for them at most
    ``timeout`` seconds, so a task that ignores cancellation can't hold the interpreter's end
    forever. The ones that ended have their outcome collected; the ones still running then
    are said (a warning naming ``engine``) and returned: the interpreter ends with them."""
    for task in pending:
        task.cancel()
    if not pending:
        return set()
    done, stuck = await asyncio.wait(pending, timeout=timeout)
    await asyncio.gather(*done, return_exceptions=True)
    if stuck:
        logging.getLogger("mipyre.async").warning(
            "async engine %s: %d task(s) did not end within %ss of being cancelled at "
            "shutdown; the interpreter ends with them pending",
            engine, len(stuck), timeout,
        )
    return stuck
