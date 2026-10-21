"""Response caching decorators.

Trade freshness for throughput: stash a handler's JSON-serialized bytes
for a fixed TTL, serve hits straight from cache without re-running the
handler or re-running json.dumps. Designed for public read endpoints
that tolerate brief staleness (``/health``, ``/status``, homepage
leaderboards, public product listings, rate-limited feeds).

Usage::

    from mipyre import Mipyre
    from mipyre.cache import cached_json

    app = Mipyre()

    @app.get("/health")
    @cached_json(ttl=1.0)
    def health(req):
        return {"status": "ok"}

Order matters: ``@cached_json`` must sit **inside** ``@app.get`` so the
framework sees the wrapped function, not the raw handler.

The cache is per interpreter (each sub-interpreter worker has its own dict):
with N workers a hot endpoint runs its handler up to N times per TTL window,
and every other hit in the window is a dict lookup. For one miss per TTL
across all workers, cache in ``app.state`` yourself.

A handler with path params works as usual (``def item(req, item_id)``): the
wrapper passes them on, and the default key, the path, already tells
``/item/1`` from ``/item/2``.

Cache key is the request path only. Query strings are ignored. If you
need query-aware caching, pre-compose the key yourself:

    @app.get("/search")
    @cached_json(ttl=5.0, key=lambda req: req.path + "?" + req.query)
    def search(req): ...
"""

from __future__ import annotations

import enum
import functools
import inspect
import json
import logging
import threading
import time
from typing import Callable

from .app import Response

__all__ = ["cached_json"]

_log = logging.getLogger("mipyre.cache")

# Entries per handler. Keys come from the request, so a client cycling unique paths
# would otherwise grow the dict until the worker runs out of memory. On overflow the
# whole dict is dropped: the TTL is short, so refilling it is cheap.
_MAX_ENTRIES = 10_000

# A failing handler logs at most one line per kind of failure per this many seconds: a route
# that fails on every request (or that a client makes fail) must not fill the log.
_LOG_PERIOD = 60.0


class _Failure(enum.Enum):
    """What went wrong in a cached handler; the request is still served, uncached."""

    KEY_RAISED = "key function raised; bypassing cache"
    NOT_SERIALIZABLE = "could not serialize result; returning uncached"


class _Failures:
    """One handler's failures: each kind counted, and its log line let through at most once per
    period, saying how many lines it stands in for (the engine's ``degraded`` gate has the same
    shape). Time is a value the caller reads and passes in, as for ``_Entries``. Locked: one
    route's handler runs on several threads at once."""

    def __init__(self, period: float = _LOG_PERIOD) -> None:
        self._period = period
        self._lock = threading.Lock()
        self._counts = dict.fromkeys(_Failure, 0)
        self._held = dict.fromkeys(_Failure, 0)
        # From when the kind's next line may go out; None until its first.
        self._open_at: dict[_Failure, float | None] = dict.fromkeys(_Failure, None)

    def failed(self, kind: _Failure, now: float) -> int | None:
        """Count one failure of ``kind`` at ``now``. Its line goes out when this returns the
        number of lines held back since the last one (0 when none); None: held back."""
        with self._lock:
            self._counts[kind] += 1
            open_at = self._open_at[kind]
            if open_at is not None and now < open_at:
                self._held[kind] += 1
                return None
            self._open_at[kind] = now + self._period
            held, self._held[kind] = self._held[kind], 0
            return held

    def count(self, kind: _Failure) -> int:
        """How many failures of ``kind`` this handler has had."""
        with self._lock:
            return self._counts[kind]


class _Entries:
    """One handler's cached bodies, each with the time it expires. Time is a value the
    caller reads (``time.monotonic()``) and passes in; nothing here reads a clock. Locked:
    one route's handler runs on several threads at once."""

    def __init__(self, ttl: float, max_entries: int = _MAX_ENTRIES) -> None:
        self._ttl = ttl
        self._max = max_entries
        self._entries: dict[str, tuple[bytes, float]] = {}
        self._lock = threading.Lock()

    def get(self, key: str, now: float) -> bytes | None:
        """The body stored under ``key``, while ``now`` is before its expiry."""
        with self._lock:
            entry = self._entries.get(key)
        if entry is not None and now < entry[1]:
            return entry[0]
        return None

    def put(self, key: str, body: bytes, now: float) -> None:
        """Store ``body`` until ``now`` + ttl. ``now`` is the store time, not the request's:
        a handler slower than the TTL must not store an entry that is already stale. A new
        key past the limit drops every entry first."""
        with self._lock:
            if len(self._entries) >= self._max and key not in self._entries:
                self._entries.clear()
            self._entries[key] = (body, now + self._ttl)


def cached_json(ttl: float, key: Callable | None = None):
    """Cache a handler's JSON response for ``ttl`` seconds (per worker).

    :param ttl: lifetime in seconds. Must be > 0. Hits older than this
        re-run the handler and replace the cached entry.
    :param key: optional ``f(req) -> str`` to derive the cache key. Default
        keys on ``req.path`` alone.
    """
    if ttl <= 0:
        raise ValueError("cached_json ttl must be > 0")
    key_fn = key if key is not None else (lambda req: req.path)

    def decorator(handler):
        _cache = _Entries(ttl)
        _failures = _Failures()
        _name = getattr(handler, "__qualname__", repr(handler))
        _is_async = inspect.iscoroutinefunction(handler)

        def _serialize(result) -> bytes:
            if isinstance(result, (bytes, bytearray)):
                return bytes(result)
            if isinstance(result, str):
                return result.encode("utf-8")
            return json.dumps(result, separators=(",", ":")).encode("utf-8")

        def _hit(k: str, now: float):
            body = _cache.get(k, now)
            return None if body is None else Response(body=body, content_type="application/json")

        def _store(k: str, body: bytes) -> None:
            _cache.put(k, body, time.monotonic())

        def _failed(kind: _Failure, error: BaseException) -> None:
            held = _failures.failed(kind, time.monotonic())
            if held is not None:
                _log.error(
                    "cached_json %s: %s (%d more since the last such line, %d in all)",
                    _name,
                    kind.value,
                    held,
                    _failures.count(kind),
                    exc_info=error,
                    extra={"error_type": type(error).__name__},
                )

        def _safe_key(req):
            # A raising key function costs the cache, not the request.
            try:
                return key_fn(req), True
            except Exception as e:  # noqa: BLE001 — counted and logged (throttled) with its traceback; the request is served uncached
                _failed(_Failure.KEY_RAISED, e)
                return None, False

        if _is_async:
            @functools.wraps(handler)
            async def async_wrapper(req, **path_params):
                now = time.monotonic()
                k, ok = _safe_key(req)
                if ok:
                    cached = _hit(k, now)
                    if cached is not None:
                        return cached
                result = await handler(req, **path_params)
                if isinstance(result, Response):
                    return result
                # Not JSON-serializable here: the engine's own serialization answers it.
                try:
                    body = _serialize(result)
                except (TypeError, ValueError) as e:
                    _failed(_Failure.NOT_SERIALIZABLE, e)
                    return result
                if ok:
                    _store(k, body)
                return Response(body=body, content_type="application/json")
            return async_wrapper

        @functools.wraps(handler)
        def wrapper(req, **path_params):
            now = time.monotonic()
            k, ok = _safe_key(req)
            if ok:
                cached = _hit(k, now)
                if cached is not None:
                    return cached
            result = handler(req, **path_params)
            # An explicit Response (custom status / headers) is neither cached nor rewrapped.
            if isinstance(result, Response):
                return result
            try:
                body = _serialize(result)
            except (TypeError, ValueError) as e:
                _failed(_Failure.NOT_SERIALIZABLE, e)
                return result
            if ok:
                _store(k, body)
            return Response(body=body, content_type="application/json")

        return wrapper
    return decorator
