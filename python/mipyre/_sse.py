"""Server-sent events from a generator: a handler yields events and mipyre writes them as
``text/event-stream`` on a `Stream`.

Only a route that returns an `EventSourceResponse` runs any of this; the engine sees the
`Stream` it returns, as for a handler that writes its own.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncGenerator, Generator
from dataclasses import dataclass

from mipyre.engine import HTTPException, Stream

# At most this many sync generators are read at once, each on a thread of its own.
MAX_PUMP_THREADS = 256

_pumping: set[asyncio.Task[None]] = set()


@dataclass(frozen=True, slots=True)
class ServerSentEvent:
    """One event. Without ``data`` the client dispatches nothing (a ``retry``-only event sets
    its reconnection delay); ``data=""`` is an empty message. ``event`` and ``id`` must hold no
    line break, or sending the event raises ``ValueError``. ``retry`` is in milliseconds."""

    data: str | None = None
    event: str | None = None
    id: str | None = None
    retry: int | None = None


def EventSourceResponse(
    source: Generator[ServerSentEvent, None, None] | AsyncGenerator[ServerSentEvent, None],
) -> Stream:
    """Return a `Stream` that sends what ``source`` yields, then ends the response.

    A generator is read on a thread of its own, at most `MAX_PUMP_THREADS` at once; an async
    generator on the handler's event loop, so it is returned from an ``async def`` handler.
    While the client is slow, reading waits until it takes more. When the client leaves,
    ``source`` is closed; an exception it raises ends the response and is raised on that
    thread or task. Needs a ``runtime="main"`` route, as `Stream` does.

    :raises HTTPException: 503 when `MAX_PUMP_THREADS` generators are being read already;
        ``source`` is closed unread.
    :raises RuntimeError: an async generator outside a running event loop.
    """
    stream = Stream(headers={"cache-control": "no-cache"})
    if isinstance(source, AsyncGenerator):
        task = asyncio.get_running_loop().create_task(_pump_async(stream, source))
        _pumping.add(task)
        task.add_done_callback(_pumping.discard)
    else:
        _pump_threads.start(stream, source)
    return stream


class PumpThreads:
    """The threads that read sync generators, at most ``limit`` at once. One more is refused
    rather than queued, so nothing waits for a thread and nothing grows."""

    def __init__(self, limit: int) -> None:
        self._free = threading.BoundedSemaphore(limit)

    def start(self, stream: Stream, source: Generator[ServerSentEvent, None, None]) -> threading.Thread:
        """Start a thread that sends what ``source`` yields on ``stream``.

        :raises HTTPException: 503 when ``limit`` threads are running; ``source`` is closed.
        """
        if not self._free.acquire(blocking=False):
            source.close()
            raise HTTPException(503, "too many event streams are being generated; try again later")
        thread = threading.Thread(target=self._run, args=(stream, source), name="mipyre-sse", daemon=True)
        try:
            thread.start()
        except BaseException:
            self._free.release()
            raise
        return thread

    def _run(self, stream: Stream, source: Generator[ServerSentEvent, None, None]) -> None:
        try:
            _pump(stream, source)
        finally:
            self._free.release()


_pump_threads = PumpThreads(MAX_PUMP_THREADS)


def _pump(stream: Stream, source: Generator[ServerSentEvent, None, None]) -> None:
    try:
        for event in source:
            try:
                _send(stream, event)
            except ConnectionError:
                source.close()
                return
    finally:
        stream.close()


async def _pump_async(stream: Stream, source: AsyncGenerator[ServerSentEvent, None]) -> None:
    try:
        async for event in source:
            try:
                await _send_async(stream, event)
            except ConnectionError:
                await source.aclose()
                return
    finally:
        stream.close()


def _send(stream: Stream, event: ServerSentEvent) -> None:
    while True:
        try:
            stream.send_event(event.data, event.event, event.id, event.retry)
            return
        except BlockingIOError:
            stream.wait_writable()


async def _send_async(stream: Stream, event: ServerSentEvent) -> None:
    while True:
        try:
            stream.send_event(event.data, event.event, event.id, event.retry)
            return
        except BlockingIOError:
            await stream.wait_writable_async()
