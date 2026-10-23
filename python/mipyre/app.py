"""High-level Mipyre application class with decorator syntax."""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Callable

from mipyre.engine import Compression, LogLevel, MipyreApp as _MipyreApp, SharedState, _route_params
from mipyre.engine import _routers_to_load as _engine_routers_to_load, _in_worker
from mipyre.engine import BodyStream
from mipyre._log.settings import LogConfig, LogLevelName, _AccessLog, _LogSettings, _parse_level
from mipyre._bind import bind_handler
from mipyre._routes import (
    _ENGINE_RUNTIME,
    FastRoute,
    Route,
    RouteDeclaration,
    Runtime,
    _body_model,
    _pydantic_core_facts,
    _pydantic_refusal,
    _RouteVerbs,
    _validates,
    app_runtime,
    import_router,
    reject_removed_serve_args,
)
from mipyre._process_lane.settings import ProcessLanes

if TYPE_CHECKING:
    from types import FrameType

    from mipyre._server_run import AppRuns
    from mipyre.engine import LaneSet


def _decorator_or_call(register: Callable[[Callable], object], handler: Callable | None) -> Callable:
    """Register ``handler`` and return it; with no ``handler``, return a decorator that
    registers the function it decorates and returns it."""
    def decorator(fn: Callable) -> Callable:
        register(fn)
        return fn

    return decorator(handler) if handler is not None else decorator


def _require_int(name: str, value: object) -> None:
    """``bool`` is an ``int`` subclass; a limit set to ``True`` is a bug, not 1."""
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be int, got {type(value).__name__}")


def _require_seconds(name: str, value: object) -> None:
    """Reject anything but ``int`` or ``float`` seconds; a ``bool`` is an ``int``, but a
    timeout set to ``True`` is a bug, not 1 s."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be int or float seconds, got {type(value).__name__}")


class Mipyre(_RouteVerbs):
    """Mipyre web framework — decorator-friendly wrapper around the Rust engine.

    Usage::

        from mipyre import Mipyre

        app = Mipyre()

        @app.get("/")
        def index(req):
            return "Hello from Mipyre!"

        app.run()

    Auto-detects ``def`` vs ``async def`` and routes to the right pool::

        @app.get("/fast")
        def fast(req):              # → sync pool (220k req/s)
            return "hello"

        @app.get("/io")
        async def io(req):          # → async pool (133k req/s)
            await asyncio.sleep(0.1)
            return "done"

        @app.get("/numpy", runtime="process")
        def compute(req):           # → a worker process
            import numpy as np
            return {"mean": float(np.mean([1,2,3]))}

        app.run()                   # zero config, auto dual-pool
    """

    def __init__(
        self,
        debug: bool = False,
        log_config: LogConfig | None = None,
        *,
        runtime: Runtime | str | None = None,
        routers: list[str] | tuple[str, ...] = (),
        process: ProcessLanes | None = None,
    ) -> None:
        """``runtime``: where this app's routes run unless a route or its router says
        otherwise — ``"subinterp"`` (default), ``"isolated"``, ``"process"`` or ``"main"``. ``routers``:
        ``Router``s by import string (``"myapp.analytics:router"``); each runtime imports
        only the routers that have routes it serves. ``process``: process-lane settings."""
        self._engine = _MipyreApp()
        self.runtime = app_runtime(runtime, "Mipyre(...)")
        if process is not None and not isinstance(process, ProcessLanes):
            raise TypeError(f"process= takes a mipyre.ProcessLanes, got {type(process).__name__}")
        self._process_declared = process
        self._router_targets = tuple(routers)
        # Routers by runtime they have routes in (filled by _load_routers, main only).
        self._routers_by_runtime: dict[Runtime, list[str]] = {r: [] for r in Runtime}
        self.debug = debug
        self._startup_hooks: list[Callable] = []
        self._shutdown_hooks: list[Callable] = []
        self._routes: list[Route] = []
        self._fast_routes: list[FastRoute] = []
        self._app_file_path: str | None = None
        # Parsed here, so a bad level fails at construction; applied by the first server.
        self._configured_log = _LogSettings.resolve(debug, log_config or {})
        self._access_log: _AccessLog | None = None
        # Made by the app's first run (`_server_run.runs_of`), on main only; a worker never serves.
        self._runs: AppRuns | None = None
        self._defined_in = _defining_module_file(self)
        # The modules whose code makes this app's PgPool handles: a server run of it takes those
        # and no other app's (b/335 D17).
        self._engine._add_home_module(_creating_globals(self))
        # Guards the enable_* helpers' check-then-set: two threads must not both pass
        # "already enabled?" and register the routes/hooks twice.
        self._has_isolated_routes = False
        self._load_routers()

    def _load_routers(self) -> None:
        """Imports this app's routers and registers their routes: all of them on the main
        interpreter; in a worker, only the ones its runtime serves (the engine says which:
        a router with process routes may import what a sub-interpreter can't load)."""
        only = _engine_routers_to_load()
        for target in self._router_targets:
            if only is not None and target not in only:
                continue
            router = import_router(target)
            self._engine._add_home_module(vars(sys.modules[target.partition(":")[0]]))
            for declaration in router._routes:
                self._add(declaration)
            for runtime in {self._runtime_of(declaration) for declaration in router._routes}:
                self._routers_by_runtime[runtime].append(target)
        self._engine._set_subinterp_routers(
            list(dict.fromkeys(t for r in Runtime if r.on_subinterpreters for t in self._routers_by_runtime[r]))
        )

    @property
    def max_body_size(self) -> int:
        """Max request body size in bytes; a larger body is answered 413. Default: 10 MB.
        Per app: another app in the same process keeps its own."""
        return self._engine.max_body_size()

    @max_body_size.setter
    def max_body_size(self, size: int) -> None:
        """Set max request body size. Example: ``app.max_body_size = 50 * 1024 * 1024``"""
        _require_int("max_body_size", size)
        if size < 0:
            raise ValueError(f"max_body_size must be non-negative, got {size}")
        self._engine.set_max_body_size(size)

    @property
    def request_timeout(self) -> float:
        """Seconds each step of a request may take before the server gives up on it: its
        whole body arriving (past it: ``408 Request Timeout``), then its handler answering
        (past it: ``504 Gateway Timeout``). Default: 30. Per app: another app in the same
        process keeps its own.

        A sync handler in a sub-interpreter worker still running then is interrupted; an
        ``async def`` handler's task is cancelled a little before it (2 s, or half the
        timeout when that is less), so it is answered 504 rather than left computing a
        result nobody takes. Example: ``app.request_timeout = 60``.
        """
        return self._engine.request_timeout()

    @request_timeout.setter
    def request_timeout(self, seconds: float) -> None:
        _require_seconds("request_timeout", seconds)
        self._engine.set_request_timeout(float(seconds))

    @property
    def body_min_rate(self) -> int:
        """Bytes per second at which a request body earns time past ``request_timeout``: a
        body gets one more second for every ``body_min_rate`` bytes it delivers, so an upload
        that keeps arriving at least this fast is never cut off as too slow (up to
        ``body_time_limit``), and one that trickles slower gets ``408`` when the time it earned
        runs out. For a streamed body (``body=BodyStream``), the time it waits for the handler to
        read is not counted. Default: 1024. Per app. Example: ``app.body_min_rate = 64 * 1024``.
        """
        return self._engine.body_min_rate()

    @body_min_rate.setter
    def body_min_rate(self, bytes_per_second: int) -> None:
        _require_int("body_min_rate", bytes_per_second)
        self._engine.set_body_min_rate(bytes_per_second)

    @property
    def body_time_limit(self) -> float:
        """The most seconds a request body may take, however fast it keeps arriving (see
        ``body_min_rate``); past it: ``408``. It bounds how long one client can hold a
        connection with an upload. Default: 600. Per app. Example: ``app.body_time_limit = 3600``.
        """
        return self._engine.body_time_limit()

    @body_time_limit.setter
    def body_time_limit(self, seconds: float) -> None:
        _require_seconds("body_time_limit", seconds)
        self._engine.set_body_time_limit(float(seconds))

    @property
    def interrupt_grace(self) -> float:
        """Seconds a sync handler that was interrupted at its request budget (see
        ``request_timeout``) gets to end its request before its worker is abandoned and
        replaced. Default: 2. Per app. Example: ``app.interrupt_grace = 0.5``.
        """
        return self._engine.interrupt_grace()

    @interrupt_grace.setter
    def interrupt_grace(self, seconds: float) -> None:
        _require_seconds("interrupt_grace", seconds)
        self._engine.set_interrupt_grace(float(seconds))

    @property
    def shutdown_timeout(self) -> float:
        """Seconds a shutdown waits for each of the app's threads to end: its workers finish
        the requests they run and end their interpreters; a thread still busy then is
        abandoned and reported. A TPC thread's open connections get the grace less 1 s to
        finish (half of a grace under 2 s), and are closed after that, so the thread ends
        inside the grace. Default: 5. Per app. Example: ``app.shutdown_timeout = 1``.
        """
        return self._engine.shutdown_timeout()

    @shutdown_timeout.setter
    def shutdown_timeout(self, seconds: float) -> None:
        _require_seconds("shutdown_timeout", seconds)
        self._engine.set_shutdown_timeout(float(seconds))

    @property
    def max_websocket_message_size(self) -> int:
        """Largest WebSocket message, in bytes, in either direction. Default: 1 MiB.

        A bigger client message closes the connection with 1009 (Message Too Big);
        a bigger ``ws.send`` raises ``ValueError``. Each connection also buffers at
        most about this many bytes per direction.
        """
        return self._engine.max_websocket_message_size()

    @max_websocket_message_size.setter
    def max_websocket_message_size(self, size: int) -> None:
        _require_int("max_websocket_message_size", size)
        self._engine.set_max_websocket_message_size(size)

    @property
    def max_websocket_connections(self) -> int:
        """Concurrent WebSocket connections (one handler thread each). Default: 1024.

        An upgrade beyond the cap is answered ``503 Service Unavailable``.
        """
        return self._engine.max_websocket_connections()

    @max_websocket_connections.setter
    def max_websocket_connections(self, count: int) -> None:
        _require_int("max_websocket_connections", count)
        self._engine.set_max_websocket_connections(count)

    def enable_compression(
        self,
        *,
        min_size: int = 512,
        gzip: bool = True,
        brotli: bool = True,
        gzip_level: int = 6,
        brotli_quality: int = 4,
    ) -> None:
        """Enable gzip / brotli response compression for this app.

        Disabled by default; call once at startup to turn on. Per app: another app in
        the same process keeps its own setting. The server negotiates with the client's
        ``Accept-Encoding`` header and prefers brotli when both are enabled. Skips
        responses under ``min_size``, non-text content types (images, octet-stream),
        streaming responses (SSE), and responses that set ``Content-Encoding``
        explicitly. A large body is compressed off the I/O threads.

        Args:
            min_size: minimum body size (bytes) to compress. Default 512.
            gzip: enable gzip (``Content-Encoding: gzip``). Default True.
            brotli: enable brotli (``Content-Encoding: br``). Default True.
            gzip_level: 1..=9, default 6 (balanced speed/ratio).
            brotli_quality: 0..=11, default 4 (production sweet spot).

        :raises ValueError: a level out of its range, a negative ``min_size``, or
            ``gzip=False, brotli=False`` (nothing to enable; use
            ``disable_compression()``).
        """
        self._engine.configure_compression(
            Compression(
                min_size=min_size,
                gzip=gzip,
                brotli=brotli,
                gzip_level=gzip_level,
                brotli_quality=brotli_quality,
            )
        )

    def disable_compression(self) -> None:
        """Disable response compression. No-op if already disabled."""
        self._engine.configure_compression(None)

    def enable_grpc_benchmark(self) -> None:
        """Serve HttpArena's ``benchmark.BenchmarkService/GetSum`` gRPC method.

        Off by default. When enabled, only a ``POST`` to exactly
        ``/benchmark.BenchmarkService/GetSum`` with an ``application/grpc*``
        content-type is answered by the built-in service; every other request,
        gRPC or not, is routed to your handlers as usual.
        """
        self._engine.enable_grpc_benchmark()

    def add_fast_response(
        self,
        method: str,
        path: str,
        body: bytes | str,
        *,
        content_type: str = "text/plain",
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Register a route that returns a pre-built response without
        entering Python.

        Use for constant-body endpoints — health checks, ``/robots.txt``,
        ``/pipeline`` probe routes, maintenance pages. The accept loop
        serves the exact bytes stored here on every match, skipping GIL
        acquisition, handler dispatch, and response serialization.

        Match is exact ``(method, path)`` — no path parameters, no
        globbing. If you need those, register a normal decorator route.

        Example::

            app.add_fast_response("GET", "/health", b'{"ok":true}',
                                  content_type="application/json")
            app.add_fast_response("GET", "/robots.txt",
                                  b"User-agent: *\\nDisallow: /\\n")

        :raises ValueError: ``status_code`` is not an HTTP status (100-999),
            or a header name or value (``content_type`` included) is invalid.
        """
        if isinstance(body, str):
            body = body.encode("utf-8")
        self._engine.add_fast_response(
            method.upper(),
            path,
            body,
            content_type=content_type,
            status_code=status_code,
            headers=headers,
        )
        self._fast_routes.append(FastRoute(
            method=method.upper(),
            path=path,
            status_code=status_code,
            content_type=content_type,
            body_bytes=len(body),
        ))

    @property
    def state(self) -> SharedState:
        """Shared state across all sub-interpreters (nanosecond latency).

        Usage::

            app.state["session:user_1"] = json.dumps({"role": "admin"})
            data = json.loads(app.state["session:user_1"])
        """
        return self._engine.state

    # ------------------------------------------------------------------
    # Route registration: the declaration methods are _RouteVerbs'
    # ------------------------------------------------------------------

    def _runtime_of(self, declaration: RouteDeclaration) -> Runtime:
        return self.runtime if declaration.runtime is None else declaration.runtime

    def _add(self, declaration: RouteDeclaration) -> Callable:
        """Serve ``declaration`` and return its handler as bound: calling the decorated name
        does what the route does (model validation, path-param injection)."""
        method, path, fn = declaration.method, declaration.path, declaration.handler
        runtime = self._runtime_of(declaration)
        bound = bind_handler(fn, path, _body_model(declaration), declaration.query)
        if _validates(declaration) and runtime is Runtime.SUBINTERP and not _in_worker():
            refusal = _pydantic_refusal(declaration, _pydantic_core_facts())
            if refusal is not None:
                raise refusal
        if runtime is Runtime.ISOLATED:
            # Every sub-interpreter of this app loads the isolation adapter (design §14.3).
            self._engine._use_isolation()
            self._has_isolated_routes = True

        is_async = self._engine.route(method, path, bound, _ENGINE_RUNTIME[runtime], declaration.body is BodyStream)
        self._routes.append(Route(
            method=method,
            path=path,
            handler=fn,
            runtime=runtime,
            is_async=is_async,
            body=declaration.body,
            query=declaration.query,
            in_openapi=declaration.in_openapi,
        ))
        return bound

    # ------------------------------------------------------------------
    # Middleware
    # ------------------------------------------------------------------

    def enable_cors(
        self,
        *,
        allow_origins: str | list[str],
        allow_methods: str | list[str] = "GET, POST, PUT, DELETE, PATCH, OPTIONS",
        allow_headers: str | list[str] = "*",
        expose_headers: str | list[str] = "",
        allow_credentials: bool = False,
        max_age: int | None = 86400,
    ) -> None:
        """Enable CORS (Cross-Origin Resource Sharing), and name the other sites whose pages
        this server trusts.

        ``allow_origins`` is the one list of other sites: their pages may read this
        server's responses (CORS), and send it requests that act (``POST``, ``PUT``,
        ``PATCH``, ``DELETE``, WebSocket upgrades), which the engine refuses with 403 from
        any other site's page. ``"*"`` trusts every site for both; with cookie
        authentication, list the sites instead, since a cross-site form POST carries the
        visitor's cookies whatever ``allow_credentials`` says.

        The headers go on every response; a preflight (an ``OPTIONS`` with
        ``Access-Control-Request-Method``) to a path with no ``OPTIONS`` route is
        answered 204 with ``Access-Control-Max-Age: max_age`` (``None``: no such header).
        ``Access-Control-Allow-Origin`` holds a single origin, so with several
        ``allow_origins`` a response names the request's ``Origin`` when it is one of
        them and carries no CORS header when it is not, with ``Vary: Origin`` either way.
        Each origin is ``http[s]://host[:port]`` with no path, as a browser sends
        ``Origin``; an entry of another shape, and ``allow_origins='*'`` with
        ``allow_credentials=True``, raise ``ValueError``.

        Usage::

            app.enable_cors(allow_origins="*")  # Any site

            app.enable_cors(
                allow_origins=["https://example.com", "https://app.example.com"],
                allow_credentials=True,
            )
        """
        def _normalize_cors_value(name: str, value: object) -> str:
            # Contract is `str | list[str]`, but accept any non-str
            # sequence (e.g. tuple) too. Reject everything else loudly
            # here, naming the setting, rather than as the engine's
            # opaque argument TypeError.
            if isinstance(value, str):
                return value
            if isinstance(value, (list, tuple)):
                # str(o) would silently coerce ints/None/objects into
                # syntactically-valid-but-wrong header tokens (e.g.
                # [None, 8080] -> "None, 8080"). Reject mixed types loudly
                # so a typo fails at config time, not as a broken header.
                if not all(isinstance(o, str) for o in value):
                    bad = next(o for o in value if not isinstance(o, str))
                    raise TypeError(
                        f"CORS: {name} list/tuple must contain only str, "
                        f"got {type(bad).__name__}"
                    )
                return ", ".join(value)
            raise TypeError(
                f"CORS: {name} must be a str or list/tuple of str, "
                f"got {type(value).__name__}"
            )

        # The engine owns the rule: it parses each origin, refuses `*` with credentials, puts the
        # headers on every response and answers preflights.
        if isinstance(allow_origins, str):
            origins = [allow_origins]
        elif isinstance(allow_origins, (list, tuple)) and all(isinstance(o, str) for o in allow_origins):
            origins = list(allow_origins)
        else:
            raise TypeError(
                f"CORS: allow_origins must be a str or a list/tuple of str, got {allow_origins!r}"
            )
        self._engine.set_cors_config(
            origins,
            _normalize_cors_value("allow_methods", allow_methods),
            _normalize_cors_value("allow_headers", allow_headers),
            _normalize_cors_value("expose_headers", expose_headers) or None,
            allow_credentials,
            max_age,
        )

    # ------------------------------------------------------------------

    def before_request(self, handler: Callable | None = None) -> Callable:
        """Register a before-request hook. Use as decorator or direct call.

        The hook receives ``(request)`` and should return ``None`` to continue
        or a response to short-circuit.
        """
        return _decorator_or_call(self._engine.before_request, handler)

    def after_request(self, handler: Callable | None = None) -> Callable:
        """Register an after-request hook. Use as decorator or direct call.

        The hook receives ``(request, response)`` and must return a
        ``Response``.
        """
        return _decorator_or_call(self._engine.after_request, handler)

    # ------------------------------------------------------------------
    # Lifecycle hooks
    # ------------------------------------------------------------------

    def on_startup(self, handler: Callable | None = None) -> Callable:
        """Register a startup hook. Called before the server starts accepting requests.

        Usage::

            @app.on_startup
            def init_db():
                app.state["db"] = create_pool()
        """
        return _decorator_or_call(self._startup_hooks.append, handler)

    def on_shutdown(self, handler: Callable | None = None) -> Callable:
        """Register a shutdown hook. Called after the server stops.

        Usage::

            @app.on_shutdown
            def cleanup():
                close_pool(app.state["db"])
        """
        return _decorator_or_call(self._shutdown_hooks.append, handler)

    # ------------------------------------------------------------------
    # Observability — X-Request-ID + /metrics
    # ------------------------------------------------------------------

    def enable_request_id_header(self, header: str = "X-Request-ID") -> None:
        """Put the request's id in ``header`` on every response, whoever answered it: a
        handler, a ``before_request`` hook, a raised exception's 500, the engine's own 404 or
        503.

        A client that sends ``header`` with a usable id (letters, digits and ``-_.:/+=;@~``,
        at most 128 bytes) gets it back verbatim, so a trace id from an upstream proxy
        survives; otherwise the server's own id (32 hex digits) is used. Either way it is
        ``req.request_id``, and a 5xx body and its error log line carry the same id. The
        engine writes it as the response leaves, replacing a value the handler set.

        :raises ValueError: ``header`` is not a header name, or a second call names another
            header (the same one again changes nothing).
        """
        self._engine.set_request_id_header(header)

    def enable_metrics(self, path: str = "/metrics") -> None:
        """Serve Prometheus metrics at ``GET path`` (default ``/metrics``).

        The engine counts every response of the server's run as it leaves, whoever answered it
        (a handler, a ``before_request`` hook, a raised exception's 500, the engine's own 404 or
        503), by method and status class, with a histogram of how long it took to its response
        head. The page shows that run's counts with the engine's own, and is a ``runtime="main"``
        route that goes through the app's ``before_request`` hooks like any other, so they can
        guard it; a scrape is counted too. ``MIPYRE_METRICS=1`` counts without the page.

        :raises ValueError: ``path`` has a path parameter, or a second call asks for a different
            path; an app with its own ``GET path`` route fails when it starts.
        """
        if _route_params(path):
            raise ValueError(f"enable_metrics: path {path!r} has a path parameter; the page has one fixed path")
        self._engine.enable_metrics(path)

    # ------------------------------------------------------------------
    # Fallback (custom 404)
    # ------------------------------------------------------------------

    def fallback(self, handler: Callable | None = None) -> Callable:
        """Register a fallback handler for unmatched routes."""
        return _decorator_or_call(self._engine.fallback, handler)

    # ------------------------------------------------------------------
    # WebSocket
    # ------------------------------------------------------------------

    def websocket(self, path: str, handler: Callable | None = None) -> Callable:
        """Register a WebSocket handler. Use as decorator or direct call.

        The handler receives a ``WebSocket`` object with ``recv()``,
        ``send(msg)``, and ``close()`` methods::

            @app.websocket("/ws")
            def ws_handler(ws):
                while True:
                    msg = ws.recv()
                    if msg is None:
                        break
                    ws.send(f"echo: {msg}")

        ``path`` is a route path like any other: ``{name}`` parameters land in
        ``ws.request.params``.

        Concurrency: each open connection holds one OS thread for its handler,
        attached to the main interpreter, because ``recv()`` blocks until a
        message comes. All connections share the main interpreter's GIL, so
        handlers run one at a time between their waits; the number of open
        connections (and threads) is capped by ``max_websocket_connections``
        (default 1024; past it an upgrade is answered 503). An ``async def``
        handler is run with ``asyncio.run`` on its connection's thread: it is
        a thread per connection too, and the ``WebSocket`` methods block that
        thread's event loop while they wait.
        """
        return _decorator_or_call(lambda fn: self._engine.websocket(path, fn), handler)

    # ------------------------------------------------------------------
    # Static files
    # ------------------------------------------------------------------

    def static(self, prefix: str, directory: str, *, precompressed: bool = False) -> None:
        """Serve static files from *directory* under URL *prefix*.

        With ``precompressed=True``, a request whose ``Accept-Encoding`` takes
        brotli or gzip is answered with the ``.br`` / ``.gz`` file sitting next
        to the requested one (brotli first), carrying the original's
        ``Content-Type``, ``Content-Encoding`` and ``Vary: Accept-Encoding``;
        a file without such a twin is served as is. The twins are not made
        for you: compress them at build time (``brotli -k``, ``gzip -k``).

        Files are cached in memory, and the cache follows the disk: a file
        (or twin) that is edited, replaced or removed is served fresh once the
        filesystem watcher reports it, which is immediate in practice.

        Example::

            app.static("/static", "./public")
            app.static("/assets", "./dist", precompressed=True)
        """
        self._engine.static_dir(prefix, directory, precompressed)

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def enable_access_log(
        self,
        level: LogLevel | LogLevelName | None = None,
        sample: int = 1,
        always_log_status: int | None = None,
    ) -> None:
        """Turn on the per-request access log (``mipyre::access``).

        One line per request from the Rust side, in every mode and for every
        response — including 404s, static files, fast-path routes and 5xx that
        never reach a Python handler::

            INFO  mipyre::access Request handled method=GET path=/ status=200 latency_us=198 source="main"

        :param level: minimum log level — a ``LogLevel``, or its name ("debug" /
            "info" / "warn" / "error" / ...). Given, it is the level, whatever
            ``log_config``, ``debug=True`` or ``MIPYRE_LOG=1`` set, and a later call
            without one keeps it. Left out, the level stays as configured, raised to
            "info" when it is "error" or "off" (the access lines are INFO). An unknown
            name raises ``ValueError`` here.
        :param sample: log 1 in every ``sample`` requests. ``1`` (default)
            logs every request. ``100`` keeps roughly 1% — production knob
            to recover the 25-30% throughput tax of full access logging
            while retaining a usable observability sample. Each serving
            thread keeps 1 in ``sample`` of its own responses (no shared
            counter), so the log keeps about 1 in ``sample`` overall.
        :param always_log_status: bypass sampling for responses whose
            status is >= this value. ``400`` keeps full visibility of
            4xx/5xx errors while sampling 2xx success traffic. ``None``
            (default) applies sampling uniformly.

        ``sample < 1`` or an ``always_log_status`` that isn't an HTTP status
        raises ``ValueError``.
        """
        # Validated first, so a bad value leaves the logging settings as they were.
        requested = None if level is None else _parse_level(level)
        self._engine.set_request_log_sampling(sample, always_log_status)
        self._engine.enable_request_logging(True)
        kept = None if self._access_log is None else self._access_log.level
        self._access_log = _AccessLog(kept if requested is None else requested)

    @property
    def routes(self) -> list[Route]:
        """The registered routes, in registration order.

        Populated as routes are registered via decorators or direct calls.
        Fast-path routes (``add_fast_response``) appear in ``fast_routes``.
        """
        return list(self._routes)

    @property
    def fast_routes(self) -> list[FastRoute]:
        """Routes registered via ``add_fast_response``."""
        return list(self._fast_routes)

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(
        self,
        *,
        host: str | None = None,
        port: int | None = None,
        threads: int | None = None,
        subinterpreters: int | None = None,
        reload: bool = False,
        tls_cert: str | None = None,
        tls_key: str | None = None,
        extra_tls_ports: list[int] | None = None,
        extra_plain_ports: list[int] | None = None,
        allowed_hosts: list[str] | None = None,
        trusted_proxies: list[str] | None = None,
        **removed: object,
    ) -> None:
        """Start the Mipyre server.

        Args:
            threads: The server's threads: the thread-per-core threads (default:
                        physical cores), or with ``MIPYRE_TPC=0`` the Tokio I/O threads
                        and accept loops (default: logical CPUs). Default:
                        ``MIPYRE_THREADS``.
            subinterpreters: With ``MIPYRE_TPC=0`` only, the sub-interpreter pool's size
                        (default: logical CPUs); the thread-per-core server runs one
                        sub-interpreter per thread. Default: ``MIPYRE_SUBINTERPRETERS``.

            A size the selected server does not use is ignored, not an error: one script
            can run under several ``MIPYRE_TPC`` settings. The
            server prints a ``Note:`` line saying which size, where it was given, and why
            it is not used: ``subinterpreters`` on the thread-per-core server (one per
            thread) and when no handler runs on a sub-interpreter (``runtime="main"``
            app, or ``MIPYRE_TPC=0`` with no route on one).
            tls_cert: Path to PEM certificate chain (enables HTTPS).
                      Required together with ``tls_key``. Default None → plain HTTP.
            tls_key:  Path to PEM private key. Required together with ``tls_cert``.
            extra_tls_ports: More ports serving TLS (``MIPYRE_TLS_PORTS``); ``port``
                        then serves plain HTTP.
            extra_plain_ports: More ports serving plain HTTP/1.1 and h2c (HTTP/2 with
                        prior knowledge) (``MIPYRE_PLAIN_PORTS``).
            allowed_hosts: The DNS names this server is reached by
                        (``MIPYRE_ALLOWED_HOSTS``, comma-separated). ``localhost`` and IP
                        addresses are always served; a request to any other name gets
                        421, which stops DNS rebinding. ``["*"]`` serves any name, for a
                        reverse proxy that checks it.
            trusted_proxies: The addresses and CIDR blocks of the reverse proxies in front
                        of this server (``MIPYRE_TRUSTED_PROXIES``, comma-separated). A
                        request from one of them gets its ``req.client_ip`` from
                        ``X-Forwarded-For``, read from the right past every trusted proxy;
                        from anyone else the header is ignored. None by default: the TCP
                        peer is the client.
        """
        reject_removed_serve_args("app.run()", removed)
        from mipyre.engine import Deployment, EntryPoint, ServerFlags, _in_worker

        # In a worker the script only registers routes; the server runs on main, which reads
        # the deployment once.
        if _in_worker():
            return
        # Imported here: only the main interpreter serves, and a worker never loads it.
        from mipyre import _server_run

        deployment = Deployment.from_process({}, EntryPoint.Script)
        config = ServerFlags._from_run(
            host=host,
            port=port,
            threads=threads,
            subinterpreters=subinterpreters,
            tls_cert=tls_cert,
            tls_key=tls_key,
            extra_tls_ports=extra_tls_ports,
            extra_plain_ports=extra_plain_ports,
            allowed_hosts=allowed_hosts,
            trusted_proxies=trusted_proxies,
        )
        _server_run.run(self, deployment, config, _server_run.Launch(), reload=deployment.reload(reload))

    @property
    def process_lanes(self) -> LaneSet | None:
        """The engine's lanes of this app once a server prepared it (``None`` before, or
        without process routes): worker pids and respawn counters (``stats()``)."""
        return None if self._runs is None else self._runs.process_lanes


def _creating_globals(app: object) -> dict:
    """The globals of the code that created ``app``: the module of the function or module-level
    code that called ``Mipyre(...)``."""
    frame: FrameType | None = sys._getframe(1)
    while frame is not None and frame.f_locals.get("self") is app:
        frame = frame.f_back
    return frame.f_globals if frame is not None else {}


def _defining_module_file(app: object) -> str | None:
    """The file whose module-level code created ``app`` — the script sub-interpreter
    workers can execute to rebuild it — or None when a function created it."""
    frame: FrameType | None = sys._getframe(1)
    # Step out of Mipyre.__init__ and any subclass __init__ that called it.
    while frame is not None and frame.f_locals.get("self") is app:
        frame = frame.f_back
    if frame is None or frame.f_code.co_name != "<module>":
        return None
    file = frame.f_globals.get("__file__")
    return os.path.abspath(file) if file else None
