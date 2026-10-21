"""`mipyre._awaitable.run` against what it replaces (b/550).

A sync path runs a hook's or handler's coroutine as a task in the request's context. It did so
with `loop.run_until_complete(loop.create_task(coro, context=ctx))`, which turns the loop at
least twice (two `epoll_wait`s) even for a coroutine that never suspends; `run` starts the task
eagerly on a loop marked running for the call instead. Every test that is about what the
coroutine sees or what the caller gets runs on both, `reference` and `run`, and expects the same;
the ones about how often the loop turns are `run`'s own.

Nothing here waits on the clock: a coroutine that suspends waits for a callback the loop runs.
"""

import asyncio
import contextvars
import gc

import pytest

from mipyre._awaitable import run

VAR = contextvars.ContextVar("b550_var", default="unset")


def reference(loop, coro, context):
    return loop.run_until_complete(loop.create_task(coro, context=context))


RUNNERS = [pytest.param(reference, id="run_until_complete"), pytest.param(run, id="run")]


@pytest.fixture
def loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    reported = []
    loop.set_exception_handler(lambda _loop, context: reported.append(context))
    loop.reported = reported
    yield loop
    asyncio.set_event_loop(None)
    loop.close()


@pytest.fixture
def turns(loop):
    """How many loop iterations ran: `_run_once` counted on the instance."""
    counted = []
    turn = loop._run_once

    def counting():
        counted.append(1)
        turn()

    loop._run_once = counting
    return counted


async def let_the_loop_turn():
    """Suspends once, until a callback the loop runs next resumes it."""
    resumed = asyncio.get_running_loop().create_future()
    asyncio.get_running_loop().call_soon(resumed.set_result, None)
    await resumed


@pytest.mark.parametrize("runner", RUNNERS)
@pytest.mark.parametrize("suspends", [False, True], ids=["returns-at-once", "suspends-once"])
def test_the_first_step_runs_as_a_task_on_the_running_loop(loop, runner, suspends):
    # tests/test_request_context_reuse.py's /schedule reads current_task() before any await.
    async def coro():
        task, running = asyncio.current_task(), asyncio.get_running_loop()
        async with asyncio.timeout(3600):
            if suspends:
                await let_the_loop_turn()
        return task is not None, running is loop

    assert runner(loop, coro(), contextvars.Context()) == (True, True)


@pytest.mark.parametrize("runner", RUNNERS)
@pytest.mark.parametrize("suspends", [False, True], ids=["returns-at-once", "suspends-once"])
def test_a_variable_set_is_in_the_given_context(loop, runner, suspends):
    async def coro():
        VAR.set("before")
        if suspends:
            await let_the_loop_turn()
            VAR.set("after")

    ctx = contextvars.Context()
    runner(loop, coro(), ctx)
    assert ctx.run(VAR.get) == ("after" if suspends else "before")
    assert VAR.get() == "unset"


@pytest.mark.parametrize("runner", RUNNERS)
def test_the_loop_runs_only_for_the_call(loop, runner):
    # A sync handler on the same thread may call asyncio.run(): no loop may be running then.
    async def coro():
        return asyncio._get_running_loop() is loop

    assert runner(loop, coro(), contextvars.Context()) is True
    assert asyncio._get_running_loop() is None
    assert not loop.is_running()

    async def answer():
        return 42

    assert asyncio.run(answer()) == 42


@pytest.mark.parametrize("runner", RUNNERS)
def test_a_task_left_unawaited_takes_its_first_step_and_no_more(loop, runner):
    steps = []

    async def background():
        steps.append("first")
        await let_the_loop_turn()
        steps.append("second")

    async def coro():
        asyncio.get_running_loop().create_task(background())
        return "done"

    assert runner(loop, coro(), contextvars.Context()) == "done"
    assert steps == ["first"]
    for task in asyncio.all_tasks(loop):
        task.cancel()
    loop.run_until_complete(asyncio.sleep(0))


@pytest.mark.parametrize("runner", RUNNERS)
def test_a_timer_due_now_runs_and_a_later_one_waits(loop, runner):
    fired = []

    async def coro():
        running = asyncio.get_running_loop()
        running.call_later(0, fired.append, "now")
        return running.call_later(3600, fired.append, "later")

    later = runner(loop, coro(), contextvars.Context())
    assert fired == ["now"]
    assert not later.cancelled()
    later.cancel()


@pytest.mark.parametrize("runner", RUNNERS)
@pytest.mark.parametrize("suspends", [False, True], ids=["before-the-first-await", "after-it"])
@pytest.mark.parametrize("error", [ValueError("v"), asyncio.CancelledError(), KeyboardInterrupt(), SystemExit(3)],
                         ids=["ValueError", "CancelledError", "KeyboardInterrupt", "SystemExit"])
def test_what_the_coroutine_raises_is_raised(loop, runner, suspends, error):
    async def coro():
        if suspends:
            await let_the_loop_turn()
        raise error

    with pytest.raises(type(error)):
        runner(loop, coro(), contextvars.Context())
    assert asyncio._get_running_loop() is None
    assert not loop.is_running()


@pytest.mark.parametrize("runner", RUNNERS)
@pytest.mark.parametrize("error", [ValueError("v"), KeyboardInterrupt()], ids=["ValueError", "KeyboardInterrupt"])
def test_an_exception_raised_after_a_suspension_is_not_also_reported_by_the_task(loop, runner, error):
    async def coro():
        await let_the_loop_turn()
        raise error

    with pytest.raises(type(error)):
        runner(loop, coro(), contextvars.Context())
    gc.collect()
    assert loop.reported == []


@pytest.mark.parametrize("runner", RUNNERS)
def test_a_stop_asked_for_by_a_suspended_coroutine_ends_the_call(loop, runner):
    async def coro():
        asyncio.get_running_loop().stop()
        await let_the_loop_turn()

    with pytest.raises(RuntimeError, match="Event loop stopped before Future completed."):
        runner(loop, coro(), contextvars.Context())
    assert not loop.is_running()
    gc.collect()
    assert loop.reported == []


@pytest.mark.parametrize("runner", RUNNERS)
def test_a_task_factory_makes_the_task(loop, runner):
    made = []

    def factory(loop, coro, **kwargs):
        made.append(sorted(kwargs))
        return asyncio.Task(coro, loop=loop, **kwargs)

    loop.set_task_factory(factory)

    async def coro():
        return asyncio.current_task() is not None

    assert runner(loop, coro(), contextvars.Context()) is True
    assert made == [["context"]]


@pytest.mark.parametrize("runner", RUNNERS)
def test_a_closed_loop_runs_nothing(loop, runner):
    async def coro():
        return None

    loop.close()
    unrun = coro()
    with pytest.raises(RuntimeError, match="Event loop is closed"):
        runner(loop, unrun, contextvars.Context())
    unrun.close()


def test_a_coroutine_that_returns_at_once_does_not_turn_the_loop(loop, turns):
    async def coro():
        return "at once"

    assert run(loop, coro(), contextvars.Context()) == "at once"
    assert turns == []


def test_work_left_ready_turns_the_loop_once(loop, turns):
    called = []

    async def coro():
        asyncio.get_running_loop().call_soon(called.append, "ready")

    run(loop, coro(), contextvars.Context())
    assert called == ["ready"]
    assert turns == [1]


def test_a_suspended_coroutine_turns_the_loop_until_it_is_done(loop, turns):
    async def coro():
        await let_the_loop_turn()
        await let_the_loop_turn()
        return "done"

    assert run(loop, coro(), contextvars.Context()) == "done"
    assert len(turns) >= 2
