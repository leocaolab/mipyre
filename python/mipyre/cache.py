"""Response caching decorators.

Trade freshness for throughput: stash a handler's JSON response for a fixed TTL, serve hits
straight from cache without re-running the handler or encoding it again. Designed for public
read endpoints that tolerate brief staleness (``/health``, ``/status``, homepage leaderboards,
public product listings, rate-limited feeds).

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

The default key is the path and the query string, so ``/search?q=a`` and ``/search?q=b``
are cached apart, and so are ``/item/1`` and ``/item/2`` of a handler with path params. A
request carrying ``Authorization`` or ``Cookie`` is served uncached: its answer may be one
user's. A ``key`` function replaces all of that; it returns ``None`` for a request it must not
cache::

    @app.get("/me")
    @cached_json(ttl=5.0, key=lambda req: req.headers.get("authorization"))
    def me(req): ...

Only a ``dict`` or ``list`` result is cached, encoded as the engine encodes it
(``mipyre.engine.encode_json``); anything else, a ``Response`` among them, goes out as the
handler returned it, uncached.
"""

from __future__ import annotations

import functools
import inspect
import threading
import time
from typing import Callable

from .engine import Response, encode_json

__all__ = ["cached_json"]

# Keys and bodies come from requests and handlers, so a client cycling unique query strings
# would otherwise grow one handler's dict until the worker runs out of memory. Past either
# limit the whole dict is dropped: the TTL is short, so refilling it is cheap.
_MAX_ENTRIES = 10_000
_MAX_BYTES = 64 * 1024 * 1024


class _Entries:
    """One handler's cached bodies, each with the time it expires. Time is a value the
    caller reads (``time.monotonic()``) and passes in; nothing here reads a clock. Locked:
    one route's handler runs on several threads at once."""

    def __init__(self, ttl: float, max_entries: int = _MAX_ENTRIES, max_bytes: int = _MAX_BYTES) -> None:
        self._ttl = ttl
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._entries: dict[str, tuple[bytes, float]] = {}
        # The characters of every key plus the bytes of every body held.
        self._size = 0
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
        entry past either limit drops every entry first; one larger than the byte limit by
        itself is not stored."""
        size = len(key) + len(body)
        with self._lock:
            old = self._entries.pop(key, None)
            if old is not None:
                self._size -= len(key) + len(old[0])
            if size > self._max_bytes:
                return
            if len(self._entries) >= self._max_entries or self._size + size > self._max_bytes:
                self._entries.clear()
                self._size = 0
            self._entries[key] = (body, now + self._ttl)
            self._size += size


def _default_key(req) -> str | None:
    headers = req.headers
    if "authorization" in headers or "cookie" in headers:
        return None
    query = req.query
    return f"{req.path}?{query}" if query else req.path


def cached_json(ttl: float, key: Callable | None = None):
    """Cache a handler's JSON response for ``ttl`` seconds (per worker).

    :param ttl: lifetime in seconds. Must be > 0. Hits older than this
        re-run the handler and replace the cached entry.
    :param key: optional ``f(req) -> str | None`` to derive the cache key; ``None`` serves
        the request uncached. What it raises fails the request. Default: the path and the
        query string, and ``None`` for a request carrying ``Authorization`` or ``Cookie``.
    """
    if ttl <= 0:
        raise ValueError("cached_json ttl must be > 0")
    key_fn = key if key is not None else _default_key

    def decorator(handler):
        entries = _Entries(ttl)

        def hit(k: str | None) -> Response | None:
            body = None if k is None else entries.get(k, time.monotonic())
            return None if body is None else Response(body=body, content_type="application/json")

        def store(k: str | None, result):
            if k is None or not isinstance(result, (dict, list)):
                return result
            body = encode_json(result)
            entries.put(k, body, time.monotonic())
            return Response(body=body, content_type="application/json")

        if inspect.iscoroutinefunction(handler):
            @functools.wraps(handler)
            async def async_wrapper(req, **path_params):
                k = key_fn(req)
                cached = hit(k)
                if cached is not None:
                    return cached
                return store(k, await handler(req, **path_params))
            return async_wrapper

        @functools.wraps(handler)
        def wrapper(req, **path_params):
            k = key_fn(req)
            cached = hit(k)
            if cached is not None:
                return cached
            return store(k, handler(req, **path_params))

        return wrapper
    return decorator
