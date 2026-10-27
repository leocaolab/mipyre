"""Kubernetes-style health probes — ``/livez`` + ``/readyz``.

Wire-up::

    from mipyre import HealthProbes, Mipyre
    from mipyre.db import PgPool

    app = Mipyre()
    probes = HealthProbes(app)   # /livez + /readyz registered

    pool = PgPool.connect(...)

    @probes.check("db")
    def _db_ready():
        pool.fetch_scalar("SELECT 1")         # raises on failure

    @probes.check("cache")
    async def _cache_ready():
        await redis.ping()

Behaviour:

- ``GET /livez`` always returns ``200 {"status":"alive"}``, answered by the engine without
  entering Python or running a ``before_request`` hook: a pod whose interpreter is busy, or
  whose hooks ask for credentials, is still alive. k8s uses it to decide whether to restart
  the pod.
- ``GET /readyz`` runs every registered check, and on ``runtime="main"`` one of its own,
  ``abandoned_workers``: it fails while a sub-interpreter pool is at its limit of abandoned
  workers (as many as its sync workers, each stuck in a request an interruption did not end).
  Those threads can't be taken back, so the pod is to be restarted. Success → ``200
  {"status":"ready","checks":{"db":{"ok":true},...}}``. Any failure
  (an exception, a timeout, or a ``False`` return) → ``503 {"status":"not_ready",
  "checks":{"db":{"ok":false},...},"request_id":"..."}``. The probe is
  unauthenticated, so why a check failed is never in the body: it goes
  to the log, under the request id the body gives. k8s uses this to gate traffic.

Anyone who can reach the port can probe, so neither the checks nor their log lines run as
often as a client asks. A probe within a second of the last run answers with that run's
result. Why checks fail is logged when the set of failing checks changes, and again once a
minute while it holds, with how many failing probes were not logged in between; the body of
a probe that was not logged gives the request id of the one that was.

Checks run sequentially in the handler, on the main interpreter or in a worker process (not
in a sub-interpreter: see ``HealthProbes``). Sync + async both work, and each
is bounded by the same timeout: one that takes longer fails. A check that hangs is left
running (a thread can't be killed), and later probes wait on that same run rather than
starting another: a stuck check costs one thread, not one per probe.
"""

from __future__ import annotations

import _thread
import contextvars
import inspect
import json
import logging
import threading
import time
from concurrent.futures import Future, wait
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Awaitable, Callable, NamedTuple, Sequence, TypeVar, Union

from mipyre import Runtime, RuntimeDeclarationError, engine
from mipyre.engine import Response

if TYPE_CHECKING:
    from mipyre import Mipyre

_log = logging.getLogger(__name__)


CheckFn = Union[Callable[[], Any], Callable[[], Awaitable[Any]]]

# A readiness check must fail fast: a hung one (DB deadlock, a partition without a
# connection timeout) would otherwise hold the readyz handler forever while k8s keeps
# probing.
_CHECK_TIMEOUT_S = 10.0

_RESULT_TTL_S = 1.0

_LOG_PERIOD_S = 60.0

_NONE_FAILING: frozenset[str] = frozenset()

_LIVEZ_BODY = json.dumps({"status": "alive"}).encode("utf-8")


@dataclass(frozen=True)
class ReadinessCheck:
    """A check: its name and the call that runs it. A registered check runs bounded by the probe
    timeout (``of``); one that only reads the engine's counters cannot hang and runs as it is."""

    name: str
    call: Callable[[], Any]

    @classmethod
    def of(cls, name: str, fn: CheckFn) -> ReadinessCheck:
        return cls(name, BoundedCall(fn, _CHECK_TIMEOUT_S))


@dataclass(frozen=True)
class _CheckFailure:
    """A readiness check that failed, for one of two reasons: it raised ``error`` (a check
    that timed out raised ``TimeoutError``), or, ``error`` None, it reported not ready by
    returning ``False``."""

    check: str
    error: Exception | None

    def log(self, request_id: str, held: int) -> None:
        if self.error is None:
            _log.error(
                "readiness check %r returned False (request_id=%s; %d failing probes not logged before)",
                self.check,
                request_id,
                held,
            )
            return
        _log.error(
            "readiness check %r raised (request_id=%s; %d failing probes not logged before)",
            self.check,
            request_id,
            held,
            exc_info=self.error,
            extra={"error_type": type(self.error).__name__},
        )


class _CheckResults(NamedTuple):
    """One run of every check: whether each passed, in registration order, and why the ones
    that failed did."""

    passed: dict[str, bool]
    failures: tuple[_CheckFailure, ...]

    @property
    def ok(self) -> bool:
        return not self.failures

    @property
    def checks(self) -> dict[str, dict[str, bool]]:
        return {name: {"ok": ok} for name, ok in self.passed.items()}


def _run_check(check: ReadinessCheck) -> _CheckFailure | None:
    """Return why ``check`` failed, or None when it passed. A check fails by raising (a
    timeout included) or by returning ``False``; any other value, ``None`` included, passes."""
    try:
        result = check.call()
    except Exception as e:  # noqa: BLE001 — a failing check is a 503, never a crash; its reason goes to the log
        return _CheckFailure(check.name, e)
    return _CheckFailure(check.name, None) if result is False else None


def _run_checks(checks: Sequence[ReadinessCheck]) -> _CheckResults:
    passed: dict[str, bool] = {}
    failures: list[_CheckFailure] = []
    for check in checks:
        failure = _run_check(check)
        passed[check.name] = failure is None
        if failure is not None:
            failures.append(failure)
    return _CheckResults(passed, tuple(failures))


class _FailureLog:
    """When /readyz logs why it is not ready: when the set of failing checks changes (from
    ready included), and again once ``period`` has passed while it holds. Time is a value the
    caller passes in; nothing here reads a clock or writes the log."""

    def __init__(self, period: float) -> None:
        self._period = period
        self._failing: frozenset[str] = frozenset()
        self._next_at = 0.0
        self._held = 0
        # The probe whose log lines say why the current failing set fails.
        self.request_id: str | None = None

    def note(self, failing: frozenset[str], request_id: str, now: float) -> int | None:
        """Take one probe's failing checks. When the probe is to be logged (its reasons, or
        that it is ready again), return how many failing probes were not logged since the
        last time; None when it is not."""
        changed = failing != self._failing
        self._failing = failing
        if not failing:
            if not changed:
                return None
            held, self._held, self.request_id = self._held, 0, None
            return held
        if not changed and now < self._next_at:
            self._held += 1
            return None
        held, self._held = self._held, 0
        self._next_at = now + self._period
        self.request_id = request_id
        return held


class _Verdict(NamedTuple):
    """The readiness verdict of one run of the checks: the /readyz status and body, kept to
    answer probes until ``expires_at``."""

    status: int
    body: bytes
    expires_at: float

    def response(self) -> Response:
        return Response(body=self.body, status_code=self.status, content_type="application/json")


class _Readiness:
    """One app's /readyz: the last verdict, kept for ``ttl``, and the log of why checks fail.
    A ``runtime="main"`` route runs on several bridge threads at once: the failure log is
    locked, and the verdict is immutable and replaced whole, so reading it needs no lock. The
    checks run outside the lock; concurrent runs of one check share a run (``BoundedCall``)."""

    def __init__(self, checks: Sequence[ReadinessCheck], ttl: float, period: float) -> None:
        self._checks = checks
        self._ttl = ttl
        self._lock = threading.Lock()
        self._verdict: _Verdict | None = None
        self._failure_log = _FailureLog(period)

    def cached(self, now: float) -> _Verdict | None:
        """Return the last verdict while ``now`` is before it expires."""
        verdict = self._verdict
        return verdict if verdict is not None and now < verdict.expires_at else None

    def settle(self, results: _CheckResults, request_id: str, now: float) -> _Verdict:
        """Return the verdict on ``results`` and keep it until ``now`` + ttl, logging its
        failures (or the recovery) when the failure log says to. ``now`` is when the run
        finished: a run slower than the TTL must not leave a verdict that is already stale."""
        failing = frozenset(failure.check for failure in results.failures) if results.failures else _NONE_FAILING
        with self._lock:
            held = self._failure_log.note(failing, request_id, now)
            cited = self._failure_log.request_id
        body: dict[str, Any] = {"status": "ready" if results.ok else "not_ready", "checks": results.checks}
        if not results.ok:
            body["request_id"] = cited
        verdict = _Verdict(200 if results.ok else 503, json.dumps(body).encode("utf-8"), now + self._ttl)
        self._verdict = verdict
        if held is not None:
            _log_results(results, request_id, held)
        return verdict

    def probe(self, request_id: str) -> _Verdict:
        verdict = self.cached(time.monotonic())
        if verdict is None:
            verdict = self.settle(_run_checks(self._checks), request_id, time.monotonic())
        return verdict


def _log_results(results: _CheckResults, request_id: str, held: int) -> None:
    if results.ok:
        _log.info("readiness checks pass again (request_id=%s; %d failing probes not logged before)", request_id, held)
    for failure in results.failures:
        failure.log(request_id, held)


def _build_readyz_handler(checks: Sequence[ReadinessCheck]):
    readiness = _Readiness(checks, _RESULT_TTL_S, _LOG_PERIOD_S)

    def readyz(req):
        return readiness.probe(req.request_id).response()

    return readyz


_Check = TypeVar("_Check", bound=CheckFn)


def _no_pool_at_abandon_limit() -> bool:
    """Return whether no sub-interpreter pool of this process is at its limit of abandoned
    workers. It reads the counters of the process it runs in, so it is registered only where
    /readyz runs in the server's own process."""
    return engine.get_gil_metrics().pools_at_abandon_limit == 0


class HealthProbes:
    """Kubernetes-style ``/livez`` and ``/readyz`` routes on ``app``.

    ``/livez`` always returns 200: the process is alive. The engine answers it without entering
    Python or running ``before_request`` hooks. ``/readyz`` runs every check registered with
    ``@probes.check(name)`` and returns 200 only when all pass; any failure produces 503 with a
    JSON body listing the failing check. A check registered after the probes still runs.

    ``runtime`` is where ``/readyz`` and its checks run: ``"main"`` (the default; most checks touch
    globals, DB pools and cache clients, that live on the main interpreter) or ``"process"``.
    On ``"main"`` it also checks ``abandoned_workers``: not ready while a sub-interpreter pool is at
    its limit of abandoned workers. A ``"process"`` probe runs in a worker process, which cannot see
    the server's pool, so it does not have that check.

    :raises RuntimeDeclarationError: ``runtime`` is ``"subinterp"`` or ``"isolated"``. A check that
        hangs is left on a thread of its own, and a sub-interpreter can only start threads it waits
        for at exit, so one hung check would keep its worker from stopping: every shutdown while the
        dependency is down would fail.
    """

    def __init__(
        self,
        app: Mipyre,
        *,
        livez_path: str = "/livez",
        readyz_path: str = "/readyz",
        runtime: Runtime | str = Runtime.MAIN,
    ) -> None:
        where = f"HealthProbes(runtime={runtime!r})"
        parsed = Runtime.parse(runtime, where)
        if parsed.on_subinterpreters:
            raise RuntimeDeclarationError(
                f"{where}: readiness checks can't run in a sub-interpreter. A hung check is left "
                "running on its own thread, and a sub-interpreter only has threads it waits for "
                "at exit, so its worker would not stop. Use runtime=\"main\" (the default) or "
                "\"process\"."
            )
        self._checks: list[ReadinessCheck] = []
        if parsed is Runtime.MAIN:
            self._checks.append(ReadinessCheck("abandoned_workers", _no_pool_at_abandon_limit))
        app.add_fast_response("GET", livez_path, _LIVEZ_BODY, content_type="application/json")
        app.get(readyz_path, runtime=runtime)(_build_readyz_handler(self._checks))

    def check(self, name: str) -> Callable[[_Check], _Check]:
        """Register a readiness check named ``name``, sync or async. Decorator form::

            @probes.check("db")
            def _db_ready():
                pool.fetch_scalar("SELECT 1")

        A check fails when it raises, takes longer than the probe timeout (10 s), or returns
        ``False``; any other value, ``None`` included, passes. A failing check is reported in
        ``/readyz`` and the whole probe returns 503.
        """

        def decorator(fn: _Check) -> _Check:
            self._checks.append(ReadinessCheck.of(name, fn))
            return fn

        return decorator


# An awaitable a check returns is driven on its thread's own event loop, so the caller may itself
# be inside a running loop. The thread is a daemon wherever the interpreter allows one (main; a
# sub-interpreter worker does not), so a hung check never holds up exit there. ``asyncio`` is
# imported only when a check returns an awaitable: it pulls in ``ssl``, ``subprocess`` and the
# rest, about 3 MiB in every interpreter that imports mipyre.


def _start(fn: Callable[[], Any], timeout: float) -> Future:
    """Start ``fn`` on a new thread, in a copy of the caller's context, and return the
    future of its value."""
    future: Future = Future()
    deadline = time.monotonic() + timeout

    def run() -> None:
        try:
            result = fn()
            if inspect.isawaitable(result):
                import asyncio

                result = asyncio.run(_await_by(result, deadline))
        except BaseException as e:  # noqa: BLE001 — handed to the waiting caller
            future.set_exception(e)
        else:
            future.set_result(result)

    threading.Thread(
        target=contextvars.copy_context().run,
        args=(run,),
        name=f"mipyre-bounded:{getattr(fn, '__qualname__', repr(fn))}",
        daemon=_thread.daemon_threads_allowed(),
    ).start()
    return future


async def _await_by(awaitable: Any, deadline: float) -> Any:
    """``awaitable``'s value, cancelled at ``deadline`` (so its thread ends too)."""
    import asyncio

    return await asyncio.wait_for(awaitable, max(0.0, deadline - time.monotonic()))


class BoundedCall:
    """``fn`` called with a timeout, at most one run at a time: while a run is still going
    (a hung check), a new call waits on that run instead of starting another thread."""

    def __init__(self, fn: Callable[[], Any], timeout: float) -> None:
        self.fn = fn
        self.timeout = timeout
        self._lock = threading.Lock()
        self._run: Future | None = None

    def __call__(self) -> Any:
        """Return ``fn``'s value or raise its exception; raise ``TimeoutError`` when it has not
        finished within the timeout."""
        with self._lock:
            if self._run is None or self._run.done():
                self._run = _start(self.fn, self.timeout)
            run = self._run
        if not wait((run,), self.timeout).done:
            raise TimeoutError(f"did not finish within {self.timeout:g}s")
        return run.result()


__all__ = ["CheckFn", "HealthProbes", "ReadinessCheck"]
