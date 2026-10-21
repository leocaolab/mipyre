"""How the async engine ends one request (`mipyre._async_requests`), on a plain asyncio loop
with recording answer functions: the hook chain, the budget, what counts as the request's
failure, the fetcher that ends instead of retrying, and how the engine ends the tasks still
pending when it stops. The engine's own wiring (a real async worker) is tested by
test_final_w3 and test_async_timeout."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from mipyre._async_requests import (
    FETCHER_JOIN_TIMEOUT, Deadlines, Fetcher, NoHandler, end_pending, job_runner, scheduler,
)


def _identity(value):
    return value


class _Answers:
    """The engine's answer functions, recording each answer."""

    def __init__(self, send_error=None):
        self.calls = []
        self._send_error = send_error

    def send(self, job, response):
        self.calls.append(("send", job, response))
        if self._send_error is not None:
            raise self._send_error

    def fail(self, job, error):
        self.calls.append(("fail", job, error))

    def timed_out(self, job):
        self.calls.append(("timed_out", job))


_JOB = SimpleNamespace(route=0, request="req")


def _runner(handler, before=(), after=(), timeout=1.0, to_response=_identity, answers=None):
    answers = answers or _Answers()
    run = job_runner([handler], before, after, to_response, timeout,
                     answers.send, answers.fail, answers.timed_out)
    return run, answers


def _run(handler, **kwargs):
    """The one answer the job got."""
    run, answers = _runner(handler, **kwargs)
    asyncio.run(run(_JOB))
    [answer] = answers.calls
    return answer


def _sent(value):
    return ("send", _JOB, value)


def _failed(error):
    return ("fail", _JOB, error)


def test_a_handler_s_value_is_the_answer():
    async def handler(req):
        return {"ok": req}

    assert _run(handler) == _sent({"ok": "req"})
    assert _run(lambda req: "sync") == _sent("sync")


def test_a_before_hook_that_returns_something_short_circuits():
    seen = []

    def handler(req):
        seen.append("handler")
        return "handler"

    async def guard(req):
        return "refused"

    assert _run(handler, before=[lambda req: None, guard]) == _sent("refused")
    assert seen == []


def test_after_hooks_get_the_response_and_may_replace_it():
    def tag(req, res):
        return res + "+tag"

    async def keep(req, res):
        return None

    assert _run(lambda req: "r", after=[tag, keep, tag]) == _sent("r+tag+tag")


def test_the_budget_running_out_is_a_timeout_and_cancels_the_handler():
    cancelled = []

    async def slow(req):
        # Never ends by itself: the budget is the only way out.
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    assert _run(slow, timeout=0.01) == ("timed_out", _JOB)
    assert cancelled == [True]


def test_a_handler_that_swallows_the_budget_s_cancellation_is_answered_with_its_value():
    # As under `asyncio.timeout`: the block exits without an exception, so it is no timeout.
    async def stubborn(req):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return "anyway"

    assert _run(stubborn, timeout=0.0) == _sent("anyway")


def test_a_timeout_error_raised_after_the_budget_ran_out_is_the_timeout():
    async def converts(req):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise TimeoutError("its own") from None

    assert _run(converts, timeout=0.0) == ("timed_out", _JOB)


def test_the_budget_running_out_while_the_engine_also_cancels_propagates_the_cancellation():
    # The engine stopping cancels the task too: that cancellation is the task's own, so the
    # task ends cancelled and the job is not answered as a timeout (its drop answers it).
    budget_ran_out = asyncio.Event()

    async def handler(req):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            budget_ran_out.set()
            await asyncio.Event().wait()  # goes on; the engine's cancellation ends it

    run, answers = _runner(handler, timeout=0.0)

    async def main():
        task = asyncio.ensure_future(run(_JOB))
        await budget_ran_out.wait()
        task.cancel()  # the engine's cancellation
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(main())
    assert answers.calls == []


def test_a_timeout_error_the_handler_raises_itself_is_a_failure():
    error = TimeoutError("upstream")

    def handler(req):
        raise error

    assert _run(handler, timeout=10.0) == _failed(error)


@pytest.mark.parametrize("error", [ValueError("v"), SystemExit("exit"), KeyboardInterrupt()])
def test_whatever_the_chain_raises_is_the_request_s_failure(error):
    def handler(req):
        raise error

    assert _run(handler) == _failed(error)
    assert _run(lambda req: "r", before=[handler]) == _failed(error)
    assert _run(lambda req: "r", after=[lambda req, res: handler(req)]) == _failed(error)


def test_a_failing_response_conversion_is_the_request_s_failure():
    error = TypeError("a Stream on the async path")

    def to_response(value):
        raise error

    # Converted only for an after hook to see (b/598): one is there.
    assert _run(lambda req: "r", after=[lambda req, res: None], to_response=to_response) == _failed(error)


class _Conversions:
    """A ``to_response`` that records what it converted and marks its result."""

    def __init__(self):
        self.converted = []

    def __call__(self, value):
        self.converted.append(value)
        return ("response", value)


# b/598: a handler's value becomes a `Response` only for an after hook to see; `send` maps any
# value the same way, so with no after hook the value goes to it as returned. The boundary: no
# after hook, one after hook.
def test_without_an_after_hook_the_value_is_sent_as_returned():
    to_response = _Conversions()
    assert _run(lambda req: "r", to_response=to_response) == _sent("r")
    assert to_response.converted == []


def test_with_an_after_hook_the_value_is_converted_once_for_it_to_see():
    to_response = _Conversions()
    seen = []

    def look(req, res):
        seen.append(res)

    assert _run(lambda req: "r", after=[look], to_response=to_response) == _sent(("response", "r"))
    assert (to_response.converted, seen) == (["r"], [("response", "r")])


def test_a_before_hook_s_short_circuit_is_sent_as_returned_even_with_after_hooks():
    # After hooks do not run on a short-circuit, so nothing would see a Response.
    to_response = _Conversions()
    after = [lambda req, res: pytest.fail("an after hook ran on a short-circuit")]
    assert _run(lambda req: "r", before=[lambda req: "refused"], after=after,
                to_response=to_response) == _sent("refused")
    assert to_response.converted == []


def test_the_task_s_own_cancellation_propagates():
    run, answers = _runner(lambda req: asyncio.Event().wait(), timeout=10.0)

    async def main():
        task = asyncio.ensure_future(run(_JOB))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(main())
    assert answers.calls == []


def test_a_send_that_fails_is_the_request_s_failure():
    error = RuntimeError("panic in send")
    answers = _Answers(send_error=error)
    run, _ = _runner(lambda req: "r", answers=answers)
    asyncio.run(run(_JOB))
    assert answers.calls == [_sent("r"), _failed(error)]


def test_the_fetcher_schedules_each_job_until_the_inbox_closes():
    jobs = iter(["a", "b", None, "never"])
    scheduled = []
    fetcher = Fetcher("t", lambda: next(jobs), str.upper, scheduled.append)
    # The loop itself, on this thread: what it does with each job is the test, not the thread
    # it runs on (started, it is the same `run`).
    fetcher.run()
    assert (scheduled, fetcher.error) == (["A", "B"], None)


def test_a_failing_fetcher_ends_once_with_its_error_instead_of_retrying():
    error = RuntimeError("recv broken")
    calls = []

    def recv():
        calls.append(1)
        raise error

    fetcher = Fetcher("t", recv, _identity, lambda coroutine: None)
    fetcher.run()  # returns: the error ends the loop instead of retrying it
    assert (len(calls), fetcher.error) == (1, error)


def test_the_job_s_route_picks_its_handler():
    answers = _Answers()
    run = job_runner([lambda req: "first", lambda req: "second"], (), (), _identity, 1.0,
                     answers.send, answers.fail, answers.timed_out)
    job = SimpleNamespace(route=1, request="req")
    asyncio.run(run(job))
    assert answers.calls == [("send", job, "second")]


def test_a_route_without_a_handler_fails_its_job_naming_the_route():
    answers = _Answers()
    run = job_runner([lambda req: "r", None], (), (), _identity, 1.0,
                     answers.send, answers.fail, answers.timed_out)
    job = SimpleNamespace(route=1, request="req")
    asyncio.run(run(job))
    [(kind, answered, error)] = answers.calls
    assert (kind, answered, type(error), str(error)) == (
        "fail", job, NoHandler, "route 1 has no handler on this worker's app")


# b/401: the engine waits for its fetcher with no bound of its own. 3764081f wrapped the join in
# a 30 s timeout counted from the worker's start, and 30 s into normal serving the engine
# cancelled every request in flight; the bound on shutdown is the shutdown side's.
def test_the_engine_waits_for_its_fetcher_without_a_bound_of_its_own():
    assert FETCHER_JOIN_TIMEOUT is None


def test_waiting_for_the_fetcher_ends_when_its_inbox_closes():
    fetcher = Fetcher("t", lambda: None, _identity, lambda coroutine: None)
    fetcher.start()
    asyncio.run(fetcher.returned())
    assert not fetcher.is_alive()


# b/368 (#25): the engine's stop cancels its pending tasks, waits for them a bounded time, and
# says which outlived the wait. The bound is the test's own (short) timeout: only the task that
# ignores cancellation makes the wait last it.
_SHORT = 0.01


def test_pending_tasks_that_honour_cancellation_end_and_none_is_reported(caplog):
    ended = []

    async def background(n):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            ended.append(n)
            raise

    async def stop():
        tasks = [asyncio.create_task(background(n)) for n in range(2)]
        await asyncio.sleep(0)  # each task reaches its wait
        stuck = await end_pending(tasks, timeout=60.0, worker=3)
        return tasks, stuck

    tasks, stuck = asyncio.run(stop())
    assert stuck == set()
    assert sorted(ended) == [0, 1]
    assert all(t.cancelled() for t in tasks)
    assert not [r for r in caplog.records if r.name == "mipyre.async"]


def test_a_task_that_ignores_cancellation_is_waited_for_only_the_timeout_and_reported(caplog):
    async def stubborn(released):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await released.wait()  # swallows the cancel and keeps running

    async def polite():
        await asyncio.Event().wait()

    async def stop():
        released = asyncio.Event()
        holdout = asyncio.create_task(stubborn(released))
        honours = asyncio.create_task(polite())
        await asyncio.sleep(0)
        stuck = await end_pending([holdout, honours], timeout=_SHORT, worker=3)
        still_running = not holdout.done()
        released.set()
        await holdout
        return holdout, honours, stuck, still_running

    with caplog.at_level("WARNING", logger="mipyre.async"):
        holdout, honours, stuck, still_running = asyncio.run(stop())
    assert stuck == {holdout}
    assert still_running, "end_pending returned before its timeout with the task still running"
    assert honours.cancelled()
    warnings = [r.getMessage() for r in caplog.records if r.name == "mipyre.async"]
    assert warnings == [
        f"worker=3: 1 task(s) did not end within {_SHORT}s of being cancelled at shutdown; "
        "the interpreter ends with them pending"
    ]


def test_a_task_that_fails_while_cancelled_has_its_error_collected():
    # Its exception is gathered at the stop, not left for "Task exception was never retrieved".
    async def cleanup_fails():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise OSError("socket already closed") from None

    async def stop():
        task = asyncio.create_task(cleanup_fails())
        await asyncio.sleep(0)
        stuck = await end_pending([task], timeout=60.0, worker=0)
        return task, stuck

    task, stuck = asyncio.run(stop())
    assert stuck == set()
    assert isinstance(task.exception(), OSError)


def test_no_pending_task_is_no_wait():
    assert asyncio.run(end_pending([], timeout=60.0, worker=0)) == set()


# b/598: `Deadlines`, one timer for the budgets of all the tasks on a loop. Its decisions are
# fed a clock the test sets; a "task" is anything with the calls the budget makes on it.
class _Clock:
    """A loop's clock and timers: the time is what the test sets, the timers are recorded."""

    def __init__(self):
        self.now = 0.0
        self.timers = []

    def time(self):
        return self.now

    def call_at(self, when, callback, *args):
        self.timers.append((when, callback, args))
        return object()  # the timer's handle: set

    def ring(self):
        """The timer set last rings, at the time the test set."""
        when, callback, args = self.timers[-1]
        callback(*args)


class _Task:
    def __init__(self, loop):
        self._loop = loop
        self.cancels = 0

    def get_loop(self):
        return self._loop

    def cancel(self):
        self.cancels += 1

    def uncancel(self):
        self.cancels -= 1
        return self.cancels


def test_a_budget_runs_out_at_its_deadline_and_not_before():
    clock = _Clock()
    deadlines = Deadlines(10.0)
    task = _Task(clock)
    clock.now = 5.0
    deadlines.start(task)
    assert deadlines.due(14.999) == ([], 15.0)
    assert deadlines.due(15.0) == ([task], None)
    # Run out once: the next look finds nothing more to cancel.
    assert deadlines.due(20.0) == ([], None)


def test_a_task_that_ends_before_its_deadline_did_not_run_out_and_one_after_did():
    clock = _Clock()
    deadlines = Deadlines(10.0)
    early, late = _Task(clock), _Task(clock)
    deadlines.start(early)
    deadlines.start(late)
    assert deadlines.end(early) is False
    assert deadlines.due(10.0) == ([late], None)
    late.cancel()  # what the timer does to a task that ran out
    assert deadlines.end(late) is True
    assert late.cancels == 0, "the budget's cancellation is taken back when its task ends"
    assert deadlines.due(100.0) == ([], None)


def test_one_timer_rings_at_the_earliest_deadline_still_running_then_at_the_next():
    clock = _Clock()
    deadlines = Deadlines(10.0)
    first, second, third = _Task(clock), _Task(clock), _Task(clock)
    deadlines.start(first)  # deadline 10: the timer is set for it
    clock.now = 4.0
    deadlines.start(second)  # deadline 14: the timer is already set
    clock.now = 6.0
    deadlines.start(third)  # deadline 16
    assert [when for when, _, _ in clock.timers] == [10.0]
    deadlines.end(first)
    # At 10 nothing is due (the first ended); the timer moves to the second's deadline.
    clock.now = 10.0
    clock.ring()
    assert (second.cancels, third.cancels) == (0, 0)
    assert [when for when, _, _ in clock.timers] == [10.0, 14.0]
    # At 14 the second runs out, the third has not: the timer moves to 16.
    clock.now = 14.0
    clock.ring()
    assert (second.cancels, third.cancels) == (1, 0)
    assert [when for when, _, _ in clock.timers] == [10.0, 14.0, 16.0]
    # With nothing left running on a budget, no timer is set until a task starts.
    deadlines.end(second)
    deadlines.end(third)
    clock.now = 16.0
    clock.ring()
    assert len(clock.timers) == 3
    deadlines.start(_Task(clock))
    assert [when for when, _, _ in clock.timers][-1] == 26.0


def test_a_timer_that_rings_a_hair_before_its_deadline_by_the_clock_still_runs_it_out():
    # The loop rings a timer within its clock's resolution of the deadline.
    clock = _Clock()
    deadlines = Deadlines(10.0)
    task = _Task(clock)
    deadlines.start(task)
    clock.now = 9.9999999
    clock.ring()
    assert task.cancels == 1


# b/598: `scheduler`, the fetcher's hand-off of jobs to the loop: one wake-up of the loop for
# the jobs queued while it is busy, never a job queued without one on its way.
class _Loop:
    """A loop's thread-safe hand-off, recorded: the wake-ups asked for, the tasks started."""

    def __init__(self):
        self.wakes = []
        self.started = []
        self.on_start = None

    def call_soon_threadsafe(self, callback, *args):
        self.wakes.append((callback, args))

    def create_task(self, coroutine):
        self.started.append(coroutine)
        if self.on_start is not None:
            self.on_start(coroutine)

    def run_wakes(self):
        while self.wakes:
            callback, args = self.wakes.pop(0)
            callback(*args)


def test_jobs_queued_before_the_loop_wakes_share_one_wake_up_and_start_in_order():
    loop = _Loop()
    schedule = scheduler(loop)
    schedule("a")
    schedule("b")
    assert len(loop.wakes) == 1
    loop.run_wakes()
    assert loop.started == ["a", "b"]


def test_a_job_queued_after_the_loop_took_the_queue_asks_for_a_wake_up_of_its_own():
    loop = _Loop()
    schedule = scheduler(loop)
    schedule("a")
    loop.run_wakes()
    schedule("b")
    assert len(loop.wakes) == 1
    loop.run_wakes()
    assert loop.started == ["a", "b"]


def test_a_job_queued_while_the_loop_starts_the_queue_is_never_left_without_a_wake_up():
    loop = _Loop()
    schedule = scheduler(loop)
    # The fetcher queues "c" while the loop is in the middle of starting the queue.
    loop.on_start = lambda coroutine: schedule("c") if coroutine == "a" else None
    schedule("a")
    schedule("b")
    loop.run_wakes()
    assert loop.started == ["a", "b", "c"]
    assert loop.wakes == []


def test_a_fetcher_thread_s_jobs_all_start_on_a_real_loop():
    jobs = 1000
    started = []

    async def main():
        loop = asyncio.get_running_loop()
        done = asyncio.Event()

        async def job(n):
            started.append(n)
            if len(started) == jobs:
                done.set()

        source = iter(range(jobs))
        fetcher = Fetcher("t", lambda: next(source, None), job, scheduler(loop))
        fetcher.start()
        await done.wait()
        await fetcher.returned()
        assert fetcher.error is None

    asyncio.run(main())
    assert sorted(started) == list(range(jobs))
