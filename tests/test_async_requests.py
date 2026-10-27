"""How the async engine ends one request (`mipyre._worker.async_requests.job_runner`), on a plain asyncio
loop with recording answer functions: the hook chain and what counts as the request's failure. How the
engine starts its requests is tested by test_async_engine_serve; its own wiring (a real async worker) by
test_worker_lifecycle and test_async_timeout."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from mipyre._worker.async_requests import NoHandler, job_runner


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


_JOB = SimpleNamespace(route=0, request="req")


def _runner(handler, before=(), after=(), to_response=_identity, answers=None):
    answers = answers or _Answers()
    run = job_runner([handler], before, after, to_response, answers.send, answers.fail)
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


def test_a_timeout_error_the_handler_raises_itself_is_a_failure():
    error = TimeoutError("upstream")

    def handler(req):
        raise error

    assert _run(handler) == _failed(error)


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

    # Converted only for an after hook to see: one is there.
    assert _run(lambda req: "r", after=[lambda req, res: None], to_response=to_response) == _failed(error)


class _Conversions:
    """A ``to_response`` that records what it converted and marks its result."""

    def __init__(self):
        self.converted = []

    def __call__(self, value):
        self.converted.append(value)
        return ("response", value)


# A handler's value becomes a `Response` only for an after hook to see; `send` maps any
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
    run, answers = _runner(lambda req: asyncio.Event().wait())

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


def test_the_job_s_route_picks_its_handler():
    answers = _Answers()
    run = job_runner([lambda req: "first", lambda req: "second"], (), (), _identity, answers.send, answers.fail)
    job = SimpleNamespace(route=1, request="req")
    asyncio.run(run(job))
    assert answers.calls == [("send", job, "second")]


def test_a_route_without_a_handler_fails_its_job_naming_the_route():
    answers = _Answers()
    run = job_runner([lambda req: "r", None], (), (), _identity, answers.send, answers.fail)
    job = SimpleNamespace(route=1, request="req")
    asyncio.run(run(job))
    [(kind, answered, error)] = answers.calls
    assert (kind, answered, type(error), str(error)) == (
        "fail", job, NoHandler, "route 1 has no handler on this worker's app")
