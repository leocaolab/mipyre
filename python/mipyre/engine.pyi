"""Type stubs for mipyre.engine (Rust extension module)."""

import socket
from typing import Any, Awaitable, Callable, Dict, Iterator, List, Literal, Optional, Tuple, Union, final

__all__ = [
    "_mimalloc_purge_delay", "MipyreApp", "Server", "LaneSet", "StartStrategy", "BUILD_ID", "_driver_name",
    "_driver_start_template", "_driver_start_import_worker", "_driver_fork_worker", "_routers_to_load",
    "_set_routers_to_load", "NoWorkerScript", "RegistrationSealed", "Mode", "SizeOrigin", "GivenSize", "LogLevel",
    "Compression", "Request", "Response", "Headers", "WebSocket", "SharedState", "Stream", "BodyStream",
    "AbandonedWorker", "WorkerException", "BodyRejected", "PgPool", "PgCursor", "DatabaseError", "IntegrityError",
    "UniqueViolation", "ParamError", "Metrics", "get_gil_metrics", "reset_peaks", "init_logger", "flush_logs",
    "emit_python_log", "_python_log_level", "workrequest_counts", "_in_worker", "_owned_interpreters_at_exit", "_physical_cores", "_route_params",
    "_worker_recv", "_worker_close", "_worker_send", "_worker_fail", "_worker_timed_out", "_worker_to_response",
    "_worker_app_handlers", "_worker_app_hooks", "_note_isolated",
]

@final
class LogLevel:
    """The minimum level of what is logged."""

    Off: LogLevel
    Error: LogLevel
    Warn: LogLevel
    Info: LogLevel
    Debug: LogLevel
    Trace: LogLevel
    @staticmethod
    def parse(name: str) -> LogLevel:
        """``"OFF" | "ERROR" | "WARN" | "WARNING" | "INFO" | "DEBUG" | "TRACE"``, in any
        case; anything else raises ``ValueError``."""
        ...

def init_logger(level: LogLevel | str, access_log: bool, format: str) -> None:
    """Install the Rust tracing engine, or reconfigure it if already installed.

    :param level: a ``LogLevel``, or its name (see ``LogLevel.parse``).
    :param format: ``"text"`` (human-readable) or ``"json"`` (structured).
    :raises ValueError: on an unknown level or format.
    :raises RuntimeError: if the subscriber cannot be installed because a
        foreign global tracing subscriber already holds the slot.

    tracing allows one global subscriber per process; a later call applies its
    level, access-log switch and format to that subscriber.
    """
    ...

def flush_logs() -> None:
    """Block until every line logged before the call is written to stderr.

    The logger writes from a background thread, so a line reaches stderr some time after
    it is logged, and the process exit does not wait for that thread: ``Mipyre.run()``
    calls this as it ends. Returns at once when no logger is installed.

    :raises RuntimeError: if the writer thread has stopped (the lines may never be written).
    """
    ...

def emit_python_log(
    levelno: int,
    name: str,
    message: str,
    pathname: str,
    lineno: int,
    worker_id: Optional[int] = None,
) -> None:
    """Route a Python log record through Rust tracing.

    ``levelno`` is the record's numeric level; it maps to the highest standard
    threshold it reaches, so a custom level 25 logs at INFO.
    """
    ...

def _python_log_level() -> Optional[int]:
    """The Python ``logging`` level matching the level ``init_logger`` applied, or
    ``None`` before any ``init_logger``. Every interpreter gates its root logger on it."""
    ...

@final
class Metrics:
    """Snapshot of the engine counters. Times are in microseconds."""

    gil_wait_last_us: int
    gil_wait_peak_us: int
    """Longest GIL acquisition wait since the last ``reset_peaks()``."""
    gil_wait_count: int
    gil_wait_total_us: int
    gil_queue_length: int
    gil_hold_peak_us: int
    """Longest handler GIL hold since the last ``reset_peaks()``."""
    rss_bytes: Optional[int]
    """Process RSS; ``None`` until the sampler (``MIPYRE_METRICS=1``) reads one."""
    degraded: List[str]
    """What runs degraded, as ``key: reason``; empty while everything runs as configured."""
    dropped_requests: int
    """Requests the server gave up on without an answer from Python (every refusal): 503
    overloaded or shutting down, 504 past the request budget, 502 a process-lane worker
    died, 500 a worker dropped the request."""
    total_requests: Optional[int]
    """Requests counted; ``None`` while hot-path metrics are off (``MIPYRE_METRICS`` unset)."""
    workers_serving: int
    """Workers serving now: started, less those that stopped."""
    workers_died: int
    """Workers that stopped serving on their own (thread state not bound, engine failed,
    retired), while the server ran."""
    gc_failures: int
    """``gc.collect()`` calls that raised or panicked; each retired its worker."""
    workers_retired: int
    """Workers retired because their ``gc.collect()`` failed: they take no more requests."""
    last_worker_retirement: Optional[str]
    """Why the last worker retired, in its own words; ``None`` while none has."""
    precompressed_fallbacks: int
    """Pre-compressed static files that could not be read; the original was served."""
    static_unwatched_mounts: int
    """Static mounts served uncached because their file watch failed or was lost."""
    db_results_undeliverable: int
    """Async database results that could not reach a closed asyncio loop."""
    gil_congested_waits: int
    """GIL acquisition waits past the congestion threshold (50 ms)."""
    cpu_sizing_fallback: Optional[str]
    """Why the server is sized from a fallback CPU count; ``None`` when the OS gave the counts."""
    client_disconnects: int
    """Peer-gone events (reset, aborted, closed mid-IO): the client's doing. One abandoned connection can count more than once (its body read and its response write)."""
    accept_failures: int
    """accept() failures other than resource exhaustion or a peer going away."""
    accept_exhausted: int
    """accept() failures for lack of file descriptors, sockets or kernel buffers."""
    tls_handshake_failures: int
    """TLS handshakes that failed (bad ClientHello, probes, slow clients timing out)."""
    malformed_requests: int
    """Connections closed with a 400 because the client sent no HTTP/1 request (garbage, a bad request line or header)."""
    oversized_request_heads: int
    """Connections closed with a 414 or 431 because the client's request head was past the limits."""
    first_request_timeouts: int
    """Connections closed because the client sent no first request within the request-head timeout of connecting."""
    request_head_timeouts: int
    """Kept-alive HTTP/1 connections closed because the client sent no next request head within the request-head timeout."""
    quickack_failures: int
    """Connections without TCP_QUICKACK (the delayed-ACK optimization is off there)."""
    defer_accept_failures: int
    """Listeners without TCP_DEFER_ACCEPT (the cold-connect mitigation is off)."""
    nodelay_failures: int
    """Connections without TCP_NODELAY (they keep Nagle's algorithm)."""
    cpu_pin_failures: int
    """Threads that could not be pinned to their core (they run unpinned)."""
    cpu_qos_failures: int
    """macOS threads whose QoS class could not be raised (they may run on efficiency cores)."""
    cpu_affinity_unreadable: int
    """Reads of the process's CPU affinity that failed (no thread was pinned)."""
    cpu_topology_unreadable: int
    """Reads of the physical core topology that failed (no thread was pinned)."""
    cpu_threads_beyond_cpus: int
    """Starts with more TPC threads than CPUs to pin them to (the extra threads run unpinned)."""
    drain_timeouts: int
    """Drains of stopped accept loops that ran out of time."""
    drain_closed_connections: int
    """Connections still open when such a drain gave up (they were closed)."""
    stop_watcher_failures: int
    """Stop watchers (the thread that stops a TPC run on SIGINT or SIGTERM) that panicked or were still running when their run ended."""
    blocking_calls_left: int
    """Blocking calls (main-interpreter handlers) still running when their TPC thread's runtime was shut down, and left running."""

def get_gil_metrics() -> Metrics:
    """Read every counter. Has no side effect."""
    ...

def reset_peaks() -> None:
    """Clear ``gil_wait_peak_us`` and ``gil_hold_peak_us``."""
    ...

@final
class Headers:
    """The request's header fields: a read-only mapping, names case-insensitive.

    A name sent on several field lines reads as one value joined with ``", "``
    (RFC 9110 §5.3), except ``cookie``, joined with ``"; "`` (RFC 9113
    §8.2.3). ``get_all`` returns each field line as sent.
    """

    def __getitem__(self, name: str, /) -> str: ...
    def get(self, name: str, default: Optional[str] = None) -> Optional[str]: ...
    def get_all(self, name: str) -> list[str]:
        """Every field line named ``name``, in the order received; ``[]`` if none."""
        ...
    def __contains__(self, name: object, /) -> bool: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[str]: ...
    def keys(self) -> list[str]: ...
    def values(self) -> list[str]: ...
    def items(self) -> list[tuple[str, str]]: ...

@final
class BodyStream:
    """A ``stream=True`` route's request body, read as it arrives (``req.stream``).

    Iterating yields each chunk as ``bytes``, blocking until it arrives, and stops at the
    body's end. A body the server rejects (larger than ``max_body_size``, too slow, a
    failed read) raises ``BodyRejected``, and every later read raises it again. Two
    threads reading one stream take chunks one at a time, in order.
    """

    def __iter__(self) -> BodyStream: ...
    def __next__(self) -> bytes: ...
    def read(self, n: Optional[int] = None) -> bytes:
        """Reads whole chunks until at least ``n`` bytes, or to the end (all of it when
        ``n`` is ``None``); ``b""`` at the end. Chunks are never split, so the result may
        be longer than ``n``."""
        ...
    def drain_count(self) -> int:
        """Reads the rest of the body without handing it to Python and returns its size
        in bytes.

        :raises BodyRejected: the server rejected the body.
        :raises RuntimeError: the stream was already read to its end.
        """
        ...

@final
class Request:
    """An HTTP request, as a handler and its hooks see it. Immutable."""

    def __new__(
        cls,
        method: str,
        path: str,
        params: dict[str, str],
        query: str,
        body_bytes: bytes,
        headers: dict[str, str],
        client_ip: str,
    ) -> Request:
        """Builds a request as the server does, with a fresh ``request_id``.

        :raises ValueError: a method, path or query that is not valid in a request line,
            a header that is not a valid field, or a ``client_ip`` that is not an IP
            address.
        """
        ...
    method: str
    path: str
    params: dict[str, str]
    query: str
    authority: str | None
    """The ``host[:port]`` the request was sent to: the HTTP/2 ``:authority`` or an
    absolute-form target's authority, else the ``Host`` header; ``None`` with neither."""
    headers: Headers
    client_ip: str | None
    """The client's IP address; ``None`` for an in-memory connection
    (``start(transport="memory")``, ``TestClient``'s default), which has no peer address."""
    request_id: str
    """The request's correlation id: the client's own (``app.enable_request_id()``)
    or one the server minted. A 5xx body and the error's log line carry it."""
    body: bytes
    """The whole body, copied into ``bytes``; ``memoryview(req)`` reads it in place. A
    ``stream=True`` route's body is read through ``stream``: reading it here (or through
    ``text()``, ``json()``, ``memoryview(req)``) raises ``RuntimeError`` (``BufferError``
    for ``memoryview``)."""
    stream: Optional[BodyStream]
    """The body as it arrives, on a ``stream=True`` route; ``None`` on any other. Taken
    on first access: a second access in the same request raises ``RuntimeError``."""
    query_params: dict[str, str]
    """Query parameters, a new dict on every access. On a duplicate key the FIRST value
    wins (as most proxies and WAFs read it); ``query_params_all()`` keeps every value."""
    def query_params_all(self) -> dict[str, list[str]]:
        """Query parameters with every value of a repeated key, in order."""
        ...
    def query_param(self, key: str) -> Optional[str]:
        """One query parameter (the first value of a repeated key), or ``None``."""
        ...
    def __buffer__(self, flags: int, /) -> memoryview: ...
    def text(self) -> str:
        """Decode the request body as UTF-8 text.

        Raises ``ValueError`` if the body is not valid UTF-8, and ``RuntimeError`` on
        a ``stream=True`` route, whose body is read through ``stream``.
        """
        ...
    def json(self) -> Any:
        """Parse the request body as JSON: an integer of any size stays an exact
        ``int``, a float reads back as the value sent.

        :raises ValueError: the body is not well-formed JSON.
        :raises RuntimeError: on a ``stream=True`` route, whose body is read through
            ``stream``.
        """
        ...

@final
class Response:
    """A handler's response.

    The body's content type comes from what it is, never from its text: a
    ``dict`` / ``list`` is JSON, a ``str`` is ``text/plain``, ``bytes`` are
    ``application/octet-stream`` — unless ``content_type`` names one.

    A header value is a ``str``, or a list of ``str`` to send the name on
    several lines (e.g. ``Set-Cookie``). A ``Content-Type`` or ``Server`` in
    ``headers`` replaces the default. A non-``str`` value raises
    ``TypeError`` and an invalid one (CR, LF, NUL) ``ValueError``, naming the
    header. ``headers`` reads back with lower-case names. A ``status_code``
    that is not an HTTP status (100-999) raises ``ValueError`` here.
    """

    body: object
    status_code: int
    content_type: Optional[str]
    """The type the response goes out with: a ``Content-Type`` in ``headers``
    (which replaces any other), else ``content_type=``; ``None`` when the body's
    kind decides it."""
    headers: dict[str, str | list[str]]
    def __new__(
        cls,
        body: object,
        status_code: int = 200,
        content_type: Optional[str] = None,
        headers: Optional[dict[str, str | list[str]]] = None,
    ) -> Response: ...

@final
class WebSocket:
    """One WebSocket connection, handed to an ``@app.websocket`` handler.

    Every receive returns ``None`` once the connection has ended (the peer
    closed it, or it was dropped after a read error, which the server logs).

    The ``before_request`` hooks run on the upgrade request before the 101 is
    sent; a hook that returns a response refuses the upgrade with it.
    """

    request: Request
    """The upgrade request (method, path, query, headers such as ``Origin``
    and ``Cookie``, client IP); the object the ``before_request`` hooks saw."""

    def recv_message(self) -> Optional[str | bytes]:
        """Receive the next message: ``str`` for text, ``bytes`` for binary."""
        ...
    def recv(self) -> Optional[str]:
        """Receive the next text message.

        :raises TypeError: if the next message is binary; it stays queued for
            ``recv_bytes()`` / ``recv_message()``.
        """
        ...
    def recv_bytes(self) -> Optional[bytes]:
        """Receive the next binary message.

        :raises TypeError: if the next message is text; it stays queued for
            ``recv()`` / ``recv_message()``.
        """
        ...
    def send(self, msg: str) -> None:
        """Queue a text message. Never blocks.

        :raises ValueError: the message exceeds ``max_websocket_message_size``.
        :raises BlockingIOError: the send buffer is full (the client reads
            slowly); retry after a pause.
        :raises ConnectionError: the connection is closed.
        """
        ...
    def send_bytes(self, data: bytes) -> None:
        """Queue a binary message. Same errors as ``send``."""
        ...
    def close(self) -> None: ...

@final
class Stream:
    """A streamed response body (e.g. server-sent events) a handler returns and then
    writes to. Needs a ``runtime="main"`` route."""

    status_code: int
    content_type: str
    """The type the stream goes out with: a ``Content-Type`` in ``headers``, else
    ``content_type=``."""
    headers: dict[str, str | list[str]]
    def __new__(
        cls,
        content_type: Optional[str] = None,
        status_code: int = 200,
        headers: Optional[dict[str, str | list[str]]] = None,
    ) -> Stream:
        """``content_type`` defaults to ``text/event-stream``; ``headers`` and
        ``status_code`` as for ``Response``."""
        ...
    def send(self, data: str) -> None:
        """Sends a raw chunk. Never blocks.

        :raises BlockingIOError: the buffer is full (a slow client); retry after a pause.
        :raises ConnectionError: the stream is closed.
        """
        ...
    def send_event(self, data: str, event: Optional[str] = None, id: Optional[str] = None) -> None:
        """Sends one SSE event: ``id:`` / ``event:`` lines if given, a ``data:`` line per
        line of ``data``. ``ValueError`` for a line break in ``id`` or ``event``."""
        ...
    def close(self) -> None:
        """Ends the response; a later ``send`` raises ``ConnectionError``."""
        ...

@final
class SharedState:
    """Concurrent key-value store shared across all workers / sub-interpreters.

    Backed by a single lock-free concurrent map (Rust ``DashMap``) held behind
    an ``Arc``, so every worker observes the same state in shared memory.

    Concurrency contract:

    - Each individual method call (``__getitem__``, ``__setitem__``,
      ``__delitem__``, ``get``, ``__contains__``, ``incr``, ``decr``, ...) is
      atomic with respect to other calls; there is no need for external
      locking around a single operation.
    - Compound *check-then-act* sequences across two or more calls are **not**
      atomic. For example ``if key not in state: state[key] = v`` can race
      another worker between the membership test and the assignment.
    - For atomic counters use ``incr`` / ``decr`` instead of read-modify-write
      via ``__getitem__`` + ``__setitem__``.
    - Snapshot methods (``keys`` / ``values`` / ``items`` / ``__len__``)
      return a point-in-time view; concurrent writers may change the map
      immediately after the snapshot is taken.
    """

    def __init__(self) -> None:
        """A new, empty map; in a sub-interpreter worker, the running app's map."""
        ...
    def __getitem__(self, key: str, /) -> str:
        """``state[key]`` — raises ``KeyError`` if the key is absent
        (standard mapping semantics). Use ``get`` for a default instead.

        Every text read (``[]``, ``get``, ``values``, ``items``) raises
        ``TypeError`` naming the key when its value is not UTF-8 (set with
        ``set_bytes``); read it with ``get_bytes``."""
        ...
    def __setitem__(self, key: str, value: str, /) -> None: ...
    def __delitem__(self, key: str, /) -> None:
        """``del state[key]`` — raises ``KeyError`` if the key is absent."""
        ...
    def __contains__(self, key: str, /) -> bool:
        """Whether the key exists, whatever its value."""
        ...
    def get(self, key: str, default: str | None = None) -> str | None: ...
    def set(self, key: str, value: str) -> None: ...
    def set_bytes(self, key: str, value: bytes) -> None: ...
    def get_bytes(self, key: str) -> Optional[bytes]:
        """The raw value, or ``None`` if the key is absent."""
        ...
    def delete(self, key: str) -> bool:
        """Remove the key; whether it existed."""
        ...
    def incr(self, key: str, amount: int) -> int:
        """Atomically add ``amount`` and return the new value; a missing key is created
        with ``amount``. Raises ``TypeError`` if the stored value is not an integer (it is
        never reset)."""
        ...
    def decr(self, key: str, amount: int) -> int:
        """Atomically subtract ``amount`` and return the new value."""
        ...
    def keys(self) -> list[str]: ...
    def values(self) -> list[str]: ...
    def items(self) -> list[tuple[str, str]]: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...

@final
class Mode:
    """Where non-``runtime="main"`` handlers run."""

    Gil: Mode
    """Every handler on the main interpreter."""
    Subinterp: Mode
    """Handlers in sub-interpreter workers; ``runtime="main"`` routes on the main interpreter."""
    @staticmethod
    def parse(name: str) -> Mode:
        """``"gil"`` (or ``"default"``) / ``"subinterp"`` (or ``"auto"``); anything else
        raises ``ValueError``."""
        ...
    @property
    def uses_workers(self) -> bool: ...

@final
class SizeOrigin:
    """Where a size given to ``start()`` came from, for the note about a size the server
    does not use: ``run()``'s argument, a direct ``engine.start()`` argument, or the
    environment variable of the same name."""

    RunArg: SizeOrigin
    """``run(workers=)`` / ``run(io_workers=)``."""
    StartArg: SizeOrigin
    """``engine.start(workers=)`` / ``engine.start(io_workers=)``."""
    EnvVar: SizeOrigin
    """``MIPYRE_WORKERS`` / ``MIPYRE_IO_WORKERS``."""

@final
class GivenSize:
    """A count (``workers`` / ``io_workers``) with where it was given: ``GivenSize(4,
    SizeOrigin.StartArg)``. ``start()`` takes one or none for each count; one below 1 is a
    ``ValueError`` there (``{name} must be >= 1, got {value}``)."""

    def __new__(cls, value: int, origin: SizeOrigin) -> GivenSize: ...
    @property
    def value(self) -> int: ...
    @property
    def origin(self) -> SizeOrigin: ...

@final
class Compression:
    """An app's response-compression settings, validated when made."""

    def __new__(
        cls,
        *,
        min_size: int = ...,
        gzip: bool = True,
        brotli: bool = True,
        gzip_level: int = ...,
        brotli_quality: int = ...,
    ) -> Compression:
        """:raises ValueError: ``gzip_level`` outside 1..=9, ``brotli_quality`` outside
        0..=11, a negative ``min_size``, or neither algorithm enabled."""
        ...
    @property
    def min_size(self) -> int: ...
    @property
    def gzip(self) -> bool: ...
    @property
    def brotli(self) -> bool: ...
    @property
    def gzip_level(self) -> int: ...
    @property
    def brotli_quality(self) -> int: ...

@final
class MipyreApp:
    """The engine app ``Mipyre`` wraps: routes, hooks, limits, and ``start``/``run``.

    Route registration raises ``ValueError`` for a bad path, a duplicate route, or
    ``stream=True`` without ``runtime="main"`` or on an ``async def`` handler (a streamed body
    is fed only to a sync handler on the main interpreter); ``RegistrationSealed`` for a
    route other than ``runtime="main"`` registered after the first server started.
    """

    def __init__(self) -> None: ...
    def get(self, path: str, handler: Callable[..., Any], gil: bool = False, process: bool = False) -> None: ...
    def post(
        self, path: str, handler: Callable[..., Any], gil: bool = False, stream: bool = False, process: bool = False
    ) -> None: ...
    def put(
        self, path: str, handler: Callable[..., Any], gil: bool = False, stream: bool = False, process: bool = False
    ) -> None: ...
    def delete(self, path: str, handler: Callable[..., Any], gil: bool = False) -> None: ...
    def route(
        self,
        method: str,
        path: str,
        handler: Callable[..., Any],
        gil: bool = False,
        stream: bool = False,
        process: bool = False,
    ) -> None: ...
    def set_interrupt_grace(self, seconds: float) -> None:
        """Seconds an interrupted sync request's worker gets to end it before it is
        abandoned. Default 2. Per app. :raises ValueError: not a valid duration."""
        ...
    def interrupt_grace(self) -> float: ...
    def set_shutdown_timeout(self, seconds: float) -> None:
        """Seconds a shutdown waits for each of the app's threads to end. Default 5. Per
        app. :raises ValueError: not a valid duration."""
        ...
    def shutdown_timeout(self) -> float: ...
    # Called by mipyre.app and mipyre._process_lane; not a public API.
    def _set_subinterp_routers(self, routers: List[str]) -> None: ...
    def _use_isolation(self) -> None: ...
    def _set_process_lanes(self, lanes: LaneSet) -> None: ...
    def _has_process_routes(self) -> bool: ...
    def _serve_lane_worker(self, lanes: LaneSet, lane: int, template_pid: int) -> None: ...
    def add_fast_response(
        self,
        method: str,
        path: str,
        body: bytes,
        content_type: str = ...,
        status_code: int = 200,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        """Serve a constant response for exactly ``(method, path)`` without entering
        Python. ``ValueError`` for an invalid status or header, or a pair already
        registered."""
        ...
    def before_request(self, handler: Callable[..., Any]) -> None:
        """:raises RegistrationSealed: registered after the first server started."""
        ...
    def after_request(self, handler: Callable[..., Any]) -> None:
        """:raises RegistrationSealed: registered after the first server started."""
        ...
    def fallback(self, handler: Callable[..., Any]) -> None:
        """:raises RegistrationSealed: registered after the first server started."""
        ...
    def websocket(self, path: str, handler: Callable[..., Any]) -> None:
        """:raises ValueError: a handler is already registered for ``path``, or ``path`` is
        not a valid route path (``{name}`` parameters land in ``ws.request.params``)."""
        ...
    def set_max_body_size(self, size: int) -> None:
        """This app's largest request body, in bytes (413 above it). Per app."""
        ...
    def configure_compression(self, settings: Compression | None) -> None:
        """This app's response compression; ``None`` turns it off. Per app."""
        ...
    def max_body_size(self) -> int: ...
    def set_request_timeout(self, seconds: float) -> None:
        """Seconds each step of a request may take (body: 408, handler: 504). Per app;
        raises ``ValueError`` unless finite and above 0."""
        ...
    def request_timeout(self) -> float: ...
    def set_body_min_rate(self, bytes_per_second: int) -> None:
        """Bytes per second at which a request body earns a second past the request timeout.
        Per app; raises ``ValueError`` below 1."""
        ...
    def body_min_rate(self) -> int: ...
    def set_body_time_limit(self, seconds: float) -> None:
        """The most seconds a request body may take, whatever it earns. Per app; raises
        ``ValueError`` unless finite and above 0."""
        ...
    def body_time_limit(self) -> float: ...
    def set_max_websocket_message_size(self, size: int) -> None:
        """Per app; raises ``ValueError`` outside ``1..=2**32-65``."""
        ...
    def max_websocket_message_size(self) -> int: ...
    def set_max_websocket_connections(self, count: int) -> None:
        """Per app (counted per server run); raises ``ValueError`` below 1."""
        ...
    def max_websocket_connections(self) -> int: ...
    def static_dir(self, prefix: str, directory: str, precompressed: bool = False) -> None:
        """Serve files under ``directory`` at URL ``prefix``.

        With ``precompressed``, a file is answered with the ``<file>.br`` or
        ``<file>.gz`` beside it when the client's ``Accept-Encoding`` takes
        that coding (brotli first), with the original's ``Content-Type``,
        ``Content-Encoding`` and ``Vary: Accept-Encoding``.

        Files are cached in memory and the cache follows the disk through a
        filesystem watcher (inotify / kqueue): a hit reads nothing from disk,
        and a changed, replaced or removed file (or twin) is seen as soon as
        the watcher delivers the event. A directory that cannot be watched is
        served uncached.

        :raises ValueError: ``prefix`` does not start with ``/``, or
            ``directory`` does not resolve to a directory.

        The request path is percent-decoded, then refused with 403 if it
        climbs out with ``..`` (literal or encoded) or resolves outside
        ``directory`` through a symlink. A missing file falls through to
        routing (404); an unreadable one is 403; any other IO error is
        logged and answered 500.
        """
        ...
    def set_cors_config(
        self,
        origin: str,
        methods: str,
        headers: str,
        expose_headers: Optional[str] = None,
        allow_credentials: bool = False,
        max_age: Optional[int] = None,
    ) -> None:
        """Apply these CORS headers to every response, and answer a preflight
        (``OPTIONS`` with ``Access-Control-Request-Method``) to a path with no
        ``OPTIONS`` route 204, with ``Access-Control-Max-Age: max_age`` when given.
        ``origin`` is one origin, ``*``, or several separated by commas; with several,
        a response names the request's ``Origin`` when it is one of them (and none
        when it is not), with ``Vary: Origin``.

        :raises ValueError: a value is not a valid header value (e.g. contains
            a newline), ``origin`` names no origin or lists an empty one, or
            ``origin`` lists ``*`` with ``allow_credentials``.
        :raises RegistrationSealed: called after ``app.run()`` started.
        """
        ...
    def enable_request_logging(self, enabled: bool) -> None: ...
    def set_request_log_sampling(
        self, sample_n: int = 1, always_status: Optional[int] = None
    ) -> None:
        """Log 1 in ``sample_n`` requests; responses with a status at or above
        ``always_status`` always log. ``ValueError`` for ``sample_n < 1`` or an
        ``always_status`` that isn't an HTTP status (100-999)."""
        ...
    def set_request_id_header(self, header: str) -> None:
        """Take ``req.request_id`` from the client's ``header`` when it sends a
        usable one (visible ASCII, at most 128 bytes); otherwise the server
        mints one. ``ValueError`` for an invalid header name."""
        ...
    def enable_grpc_benchmark(self) -> None:
        """Answer ``POST /benchmark.BenchmarkService/GetSum`` (``application/grpc*``)
        with the built-in HttpArena service. Off by default."""
        ...
    @property
    def state(self) -> SharedState: ...
    def _seal_registrations(self) -> None:
        """Main interpreter only, idempotent: marks the end of the script's registrations.
        A route registered after it must be ``runtime="main"``."""
        ...
    def set_script_path(self, path: str) -> None:
        """The script sub-interpreter workers execute, when it isn't
        ``__main__.__file__`` (the CLI sets it to the app's module)."""
        ...
        ...
    def _take_abandoned_workers(self) -> List[AbandonedWorker]:
        """The worker threads this app's servers abandoned at shutdown (still
        running past the grace period); taking them clears the list."""
        ...
    def start(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        workers: Optional[GivenSize] = None,
        mode: Union[Mode, str, None] = None,
        io_workers: Optional[GivenSize] = None,
        tls_cert: Optional[str] = None,
        tls_key: Optional[str] = None,
        extra_tls_ports: Optional[List[int]] = None,
        extra_plain_ports: Optional[List[int]] = None,
        transport: Optional[Literal["tcp", "memory"]] = None,
    ) -> Server:
        """Prepare one server of this app and bind its listeners; ``Server.serve()`` runs
        it. ``mode`` defaults to ``Mode.Gil``.

        ``transport="memory"`` binds no socket: the server takes only the connections its
        ``Server.connect()`` makes, served from the accept loop on as TCP ones are; giving
        it ``host``, ``port``, TLS or extra ports raises ``ValueError`` naming them.

        ``host`` is an IP address (``"127.0.0.1"``, ``"::1"``, ``"[::1]"``); anything
        else raises ``ValueError`` naming it. ``workers`` / ``io_workers`` below 1 raise
        ``ValueError`` (``workers must be >= 1, got 0``).

        Which server runs decides which size it uses. A size it does not use is ignored
        (not an error: one script can run under several ``MIPYRE_TPC`` /
        ``MIPYRE_RUNTIME`` settings), and a ``Note:`` line on stdout says which size,
        where it was given (the ``origin`` of ``workers`` / ``io_workers``), and why it is
        not used:

        - thread-per-core (the default): ``workers`` TPC threads (default: physical
          cores); ``io_workers`` is not used, each TPC thread does its own IO.
        - ``MIPYRE_TPC=0``, ``Mode.Subinterp``: ``workers`` pool sub-interpreters and
          ``io_workers`` Tokio IO threads (both default: logical CPUs). An app none of
          whose routes runs on a sub-interpreter starts no workers, so ``workers`` is not
          used.
        - ``MIPYRE_TPC=0``, ``Mode.Gil``: ``io_workers`` Tokio IO threads; ``workers``
          is not used, every handler runs on the main interpreter.

        The engine's environment variables (``MIPYRE_TPC``,
        ``MIPYRE_GC_*``, ``MIPYRE_GIL_BRIDGE_*``, ``MIPYRE_ASYNC_QUEUE_CAPACITY``,
        ``MIPYRE_METRICS``, ``MIPYRE_TPC_DARWIN``) are parsed once here; an invalid mode or value raises
        ``ValueError`` before anything starts.

        With ``extra_tls_ports``, ``port`` serves plain HTTP and each extra port serves
        TLS (``tls_cert``/``tls_key`` required); without them ``port`` serves TLS when a
        certificate is given. Each of ``extra_plain_ports`` serves plain HTTP/1.1 and
        h2c (HTTP/2 with prior knowledge) whatever ``port`` is. Every listener is bound here, before any thread or worker
        starts: a port another server listens on (in this process or another, with or
        without ``SO_REUSEPORT``) raises ``OSError`` (``errno.EADDRINUSE``)."""
        ...
    def run(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        workers: Optional[int] = None,
        mode: Union[Mode, str, None] = None,
        io_workers: Optional[int] = None,
        tls_cert: Optional[str] = None,
        tls_key: Optional[str] = None,
        extra_tls_ports: Optional[List[int]] = None,
        extra_plain_ports: Optional[List[int]] = None,
    ) -> None:
        """``start(...).serve()``: bind, then serve until SIGINT."""
        ...
    # Feature-gated: present only in an engine built with
    # `maturin develop --release --features bench` (absent from the default build and
    # from published wheels).
    def bench_inmem(
        self,
        duration_s: int = 10,
        workers: Optional[int] = None,
        conns_per_worker: int = 8,
    ) -> Tuple[int, float]:
        """``--features bench`` only. In-memory bench (no TCP): pipelines ``GET /``.
        Returns ``(requests, elapsed_s)``.

        :raises ValueError: ``workers`` is 0.
        :raises RuntimeError: a route is ``runtime="main"``, ``async def`` or ``stream=True``;
            a worker failed to start; or a worker or client failed during the run (a
            non-200 response included).
        """
        ...
    def bench_loopback(
        self,
        duration_s: int = 10,
        workers: Optional[int] = None,
        client_conns: int = 32,
    ) -> Tuple[int, float, int]:
        """``--features bench`` only. Real TCP on an ephemeral 127.0.0.1 port, client in
        this process: pipelines ``GET /``. Returns ``(requests, elapsed_s, port)``.

        :raises ValueError: ``workers`` is 0.
        :raises RuntimeError: as for :meth:`bench_inmem`.
        """
        ...

# --- Postgres (see mipyre.db) ---------------------------------------------

class DatabaseError(RuntimeError):
    """A query failed. ``sqlstate`` is the server's SQLSTATE code, or None when the
    server never reported one (pool timeout, dropped connection)."""

    sqlstate: Optional[str]

class IntegrityError(DatabaseError):
    """An integrity constraint was violated (SQLSTATE class 23)."""

class UniqueViolation(IntegrityError):
    """A unique or primary-key constraint was violated (SQLSTATE 23505)."""

class ParamError(TypeError, ValueError):
    """A value a statement parameter can't take (wrong type, out of range, not
    encodable), refused before the query is sent."""

@final
class PgCursor:
    """Streaming result set from ``PgPool.fetch_iter``; yields one dict per row."""

    def __iter__(self) -> "PgCursor": ...
    def __next__(self) -> Dict[str, Any]: ...
    def to_list(self) -> List[Dict[str, Any]]:
        """Every remaining row as a list of dicts, as ``fetch_all`` returns them."""
        ...

@final
class PgPool:
    """A DSN's Postgres pool, one per DSN per process. Parameters are encoded as the
    statement declares them; a query failure raises ``DatabaseError``."""

    @classmethod
    def connect(
        cls,
        dsn: str,
        max_connections: Optional[int] = None,
        acquire_timeout_secs: Optional[int] = None,
        runtime_threads: Optional[int] = None,
        max_lifetime_secs: Optional[int] = None,
        idle_timeout_secs: Optional[int] = None,
        ping_on_acquire: Optional[bool] = None,
    ) -> "PgPool":
        """Open ``dsn``'s pool, or return the one open for it. Raises ``ValueError`` if
        it is open with settings that differ from the ones asked for; settings left out
        match the open pool; another DSN gets a pool of its own. Defaults on the first
        call: 10 connections, 30 s, and ``runtime_threads`` (the threads every pool's
        query I/O runs on, fixed by the first call of the process) one per 4 CPUs the
        process may use, at least 2. Connections close after ``max_lifetime_secs``
        (1800) or ``idle_timeout_secs`` idle (300); 0 is never. ``ping_on_acquire``
        (False) tests a connection with a round trip before each use; without it, the
        first query on a connection that died while idle raises ``DatabaseError`` with
        the server's or socket's own error, not retried, and the pool replaces the
        connection (see ``mipyre.db``).
        A failed connect raises ``ConnectionError`` with ``.sqlstate``: the server's code
        when it answered (``28P01`` bad password, ``3D000`` no such database), else ``None``."""
        ...
    def fetch_one(self, sql: str, *params: Any) -> Optional[Dict[str, Any]]: ...
    def fetch_all(self, sql: str, *params: Any) -> List[Dict[str, Any]]: ...
    def fetch_scalar(self, sql: str, *params: Any) -> Any: ...
    def execute(self, sql: str, *params: Any) -> int: ...
    def fetch_iter(self, sql: str, *params: Any) -> PgCursor: ...
    def fetch_one_async(self, sql: str, *params: Any) -> Awaitable[Optional[Dict[str, Any]]]: ...
    def fetch_all_async(self, sql: str, *params: Any) -> Awaitable[List[Dict[str, Any]]]: ...
    def fetch_scalar_async(self, sql: str, *params: Any) -> Awaitable[Any]: ...
    def execute_async(self, sql: str, *params: Any) -> Awaitable[int]: ...

def _in_worker() -> bool:
    """Whether this code runs in a sub-interpreter worker (not the main interpreter)."""
    ...

def _owned_interpreters_at_exit() -> int:
    """Wait up to the time a stopped worker thread needs to end its interpreter, and return
    how many sub-interpreters threads still own then (GIL released while waiting)."""
    ...

def _mimalloc_purge_delay() -> int:
    """The engine allocator's (mimalloc) purge delay in milliseconds: mimalloc's default
    (1000) unless ``MIMALLOC_PURGE_DELAY`` (or ``MIMALLOC_RESET_DELAY``) sets it."""
    ...

@final
class Server:
    """One server of an app, bound by ``MipyreApp.start()``."""

    @property
    def port(self) -> Optional[int]:
        """The port it listens on (its first listener; a ``port=0`` resolved to the
        kernel's pick); ``None`` for a ``transport="memory"`` server."""
        ...
    def connect(self) -> socket.socket:
        """A new in-memory connection to a ``transport="memory"`` server: the client's
        end of a Unix socket pair, blocking, whose other end the server's accept loop
        takes. One made before the server serves waits in its backlog, as a TCP one does.

        :raises ConnectionRefusedError: the server takes no more connections.
        :raises ConnectionError: every accept loop's backlog is full.
        :raises OSError: the socket pair could not be made.
        :raises RuntimeError: the server listens on sockets."""
        ...
    def serve(self) -> None:
        """Serve until SIGINT or ``shutdown()``, blocking this thread; then drain the
        in-flight connections and return. A server stopped before it served returns at
        once. :raises RuntimeError: it has already served."""
        ...
    def shutdown(self) -> None:
        """Stop this server as SIGINT does. Callable from any thread, before or during
        ``serve()``; idempotent. Other servers of the same app keep serving."""
        ...

class NoWorkerScript(RuntimeError):
    """A server with sub-interpreter workers could not start: the workers rebuild the app
    by executing its script, and ``__main__`` has no ``__file__`` (``python -c``, the
    interactive interpreter, a host embedding Python) while no script was named. Start the
    app from a file, or create it with ``Mipyre(runtime='main')``."""

class RegistrationSealed(ValueError):
    """A route (other than ``runtime="main"``), hook or fallback registered after the app's
    registrations were sealed (the first server's start). Workers rebuild the app by
    running its script, so they would never see it."""

class BodyRejected(OSError):
    """A streamed request body (``req.stream``) was rejected as a buffered one would be:
    larger than ``max_body_size``, too slow, or a failed read. Left uncaught, the request
    gets the buffered body's 413 / 408 / 400."""

def _route_params(path: str) -> List[str]:
    """The parameter names of a route path, in order (``{*rest}`` gives ``rest``).
    ``ValueError`` for a ``:name`` segment, which the router would take literally."""
    ...


@final
class AbandonedWorker:
    """A sub-interpreter worker thread a server's shutdown gave up on: still running past
    the grace period. ``str()`` gives ``"<thread> (running <METHOD path>)"``."""

    @property
    def thread(self) -> str: ...
    @property
    def route(self) -> Optional[str]:
        """``METHOD path`` of the route its sync worker was running, if any."""
        ...

class WorkerException(Exception):
    """An exception a sub-interpreter worker raised, as the traceback it printed there.
    The ``__cause__`` of the error a failed worker start raises on the main interpreter
    (``ImportError`` / ``ModuleNotFoundError`` for an import that failed in the worker,
    ``RuntimeError`` otherwise)."""


def workrequest_counts() -> Optional[Tuple[int, int]]:
    """``(created, completed)`` worker requests, for leak checks; ``None`` unless the
    engine was built with ``--features leak_detect`` (nothing is counted then)."""
    ...

# Called by the async engine in sub-interpreter workers; not a public API.
class _AsyncInbox: ...
class _AsyncJob:
    @property
    def route(self) -> int: ...
    @property
    def request(self) -> Request: ...
class _WorkerHooks:
    @property
    def before(self) -> List[Callable[..., Any]]: ...
    @property
    def after(self) -> List[Callable[..., Any]]: ...
def _worker_recv(inbox: _AsyncInbox) -> Optional[_AsyncJob]: ...
def _worker_close(inbox: _AsyncInbox) -> None: ...
def _worker_send(job: _AsyncJob, response: Any) -> None: ...
def _worker_fail(job: _AsyncJob, exception: BaseException) -> None: ...
def _worker_timed_out(job: _AsyncJob) -> None: ...
def _worker_to_response(value: Any) -> Response: ...
def _worker_app_handlers() -> List[Optional[Callable[..., Any]]]: ...
def _worker_app_hooks() -> _WorkerHooks: ...

def _note_isolated(lib: str, bytes: int) -> None:
    """Records a library a worker isolated, once per process, and announces it at once if the
    startup line is already out."""
    ...

def _physical_cores() -> int:
    """Physical cores as the server counts them (performance cores on Apple Silicon): the
    default number of process-lane workers."""
    ...
def _routers_to_load() -> Optional[List[str]]:
    """The routers this interpreter imports: ``None`` = all of them (the main interpreter)."""
    ...
def _set_routers_to_load(routers: List[str]) -> None:
    """A process-lane worker's routers, set before it imports the app. :raises RuntimeError:
    already set."""
    ...

# Process lanes (`runtime="process"`), driven by mipyre._process_lane; not a public API.
@final
class StartStrategy:
    """How a process-lane worker gets the app: ``Import`` (it imports it) or ``Fork`` (the
    template imported it and forks)."""

    Import: StartStrategy
    Fork: StartStrategy
    @staticmethod
    def parse(text: str) -> StartStrategy:
        """``"import"`` or ``"fork"``. :raises ValueError: anything else."""
        ...
    @property
    def name(self) -> str:
        """``"import"`` or ``"fork"``."""
        ...

@final
class LaneSet:
    """The process lanes of an app: the server's, or the template's view of them."""

    def __new__(
        cls,
        workers: int,
        slot_bytes: int = ...,
        depth: int = 8,
        timeout_ms: int = 30000,
        queue_per_worker: int = 128,
        server_spin_us: int = 20,
        exit_on_failure: bool = False,
    ) -> LaneSet: ...
    @staticmethod
    def _attach(workers: int, depth: int, slot_bytes: int, worker_spin_us: int, fds: List[int]) -> LaneSet: ...
    def _setup_template(self, message: bytes) -> None: ...
    @property
    def workers(self) -> int: ...
    @property
    def template_pid(self) -> Optional[int]: ...
    def worker_pids(self) -> List[Optional[int]]: ...
    def check_lane(self, lane: int) -> None:
        """:raises IndexError: ``lane`` is not one of these lanes."""
        ...
    def _attach_template(self, pid: int, fd: int) -> None: ...
    def _report_start_failure(self, lane: int, text: str) -> None: ...
    def stats(self) -> Dict[str, Any]: ...

BUILD_ID: str
"""This build of the engine: the package version and a hash of the source tree it was built from."""

def _driver_name() -> str:
    """The process driver: ``"fork"`` or ``"spawn"``."""
    ...
def _driver_start_template(
    python: str, module: str, args: List[str], keep_fds: List[int], env: List[Tuple[str, str]]
) -> int:
    """Starts the template; 0 in a forked child (the caller becomes the template)."""
    ...
def _driver_start_import_worker(
    python: str, module: str, args: List[str], keep_fds: List[int], env: List[Tuple[str, str]]
) -> int:
    """Starts a ``start="import"`` worker; 0 in a forked child (the caller becomes the worker)."""
    ...
def _driver_fork_worker() -> int:
    """Forks a ``start="fork"`` worker; 0 in the child."""
    ...
