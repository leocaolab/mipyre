"""High-level Mipyre application class with decorator syntax."""

from __future__ import annotations

import os
import sys
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Callable

from mipyre.engine import Compression, Deployment, EntryPoint, LogLevel, MipyreApp as _MipyreApp, ServerFlags, SharedState, _route_params
from mipyre.engine import _routers_to_load as _engine_routers_to_load, _in_worker
from mipyre.engine import BodyStream
from mipyre._log.settings import LogConfig, LogLevelName, _AccessLog, _parse_level, _parsed_level
from mipyre._bind import bind_handler
from mipyre._routes import (
    _ENGINE_RUNTIME,
    FastRoute,
    Route,
    RouteDeclaration,
    Runtime,
    _body_model,
    _is_prefix,
    _pydantic_core_facts,
    _pydantic_refusal,
    _RouteVerbs,
    _validates,
    app_runtime,
    import_router,
)
from mipyre._process_lane.settings import ProcessLanes

if TYPE_CHECKING:
    from types import FrameType

    from mipyre._server_run import AppRuns


def _decorator_or_call(register: Callable[[Callable], object], handler: Callable | None) -> Callable:
    """Register ``handler`` and return it; with no ``handler``, return a decorator that
    registers the function it decorates and returns it."""
    def decorator(fn: Callable) -> Callable:
        register(fn)
        return fn

    return decorator(handler) if handler is not None else decorator


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
        process: ProcessLanes | None = None,
    ) -> None:
        """``runtime``: where this app's routes run unless a route or its router says
        otherwise — ``"subinterp"`` (default), ``"isolated"``, ``"process"`` or ``"main"``.
        ``process``: process-lane settings. Routers are added with :meth:`include_router`."""
        self._engine = _MipyreApp()
        self.runtime = app_runtime(runtime, "Mipyre(...)")
        if process is not None and not isinstance(process, ProcessLanes):
            raise TypeError(f"process= takes a mipyre.ProcessLanes, got {type(process).__name__}")
        self._process_declared = process
        self._routers_by_runtime: dict[Runtime, list[str]] = {r: [] for r in Runtime}
        self.debug = debug
        self._startup_hooks: list[Callable] = []
        self._shutdown_hooks: list[Callable] = []
        self._routes: list[Route] = []
        self._fast_routes: list[FastRoute] = []
        # Its level is parsed here, so a bad one fails at construction; the first server
        # resolves and applies it.
        self._log_config = _parsed_level(log_config or {})
        self._access_log: _AccessLog | None = None
        # Made by the app's first run (`_server_run.runs_of`), on main only; a worker never serves.
        self._runs: AppRuns | None = None
        creator = _creating_frame(self)
        self._defined_in = _module_file(creator)
        # The modules whose code makes this app's PgPool handles: a server run of it takes those
        # and no other app's.
        self._engine._add_home_module({} if creator is None else creator.f_globals)

    def include_router(self, target: str, *, prefix: str = "") -> None:
        """Serve the routes of the :class:`~mipyre.Router` that ``target`` names
        (``"myapp.admin:router"``), each at ``prefix`` followed by its own path.

        The router is named by import string, not passed as an object, so that each runtime
        imports it only if it serves one of its routes: the server imports every router, a
        sub-interpreter only those with sub-interpreter routes, a process worker only those
        with process routes. A library a process router imports never loads in a
        sub-interpreter.

        :raises TypeError: ``target`` is not a ``str``, or names something that is not a Router.
        :raises ValueError: ``prefix`` is neither empty nor starts with ``/`` without ending with one.
        :raises MissingTargetError: the module or its attribute does not exist.
        """
        if not isinstance(target, str):
            raise TypeError(
                "include_router takes a router's import string, as in "
                f'app.include_router("myapp.admin:router"), not a {type(target).__name__}: each '
                "runtime imports only the routers that have routes it serves, so a library a "
                "router imports (numpy, say) never loads in a sub-interpreter. An object passed "
                "here was imported by the app's module, which every runtime imports."
            )
        if not _is_prefix(prefix):
            raise ValueError(
                f'include_router({target!r}, prefix={prefix!r}): a prefix starts with "/" and does '
                'not end with "/"'
            )
        only = _engine_routers_to_load()
        if only is not None and target not in only:
            return
        module, router = import_router(target)
        self._engine._add_home_module(vars(module))
        for declaration in router._routes:
            self._add(replace(declaration, path=prefix + declaration.path))
        for runtime in {self._runtime_of(declaration) for declaration in router._routes}:
            self._routers_by_runtime[runtime].append(target)
        self._engine._set_subinterp_routers(
            list(dict.fromkeys(t for r in Runtime if r.on_subinterpreters for t in self._routers_by_runtime[r]))
        )

    @property
    def max_body_size(self) -> int:
        """Max request body size in bytes; a larger body is answered 413. Default: 2.5 MiB
        (2621440, as Django's ``DATA_UPLOAD_MAX_MEMORY_SIZE``).
        Per app: another app in the same process keeps its own."""
        return self._engine.max_body_size()

    @max_body_size.setter
    def max_body_size(self, size: int) -> None:
        """Set max request body size. Example: ``app.max_body_size = 50 * 1024 * 1024``"""
        self._engine.set_max_body_size(size)

    @property
    def request_timeout(self) -> float:
        """Seconds a request's handler may take to answer before the server gives up on it
        (past it: ``504 Gateway Timeout``); for a streamed body (``body=BodyStream``), also how
        long the handler may leave each chunk untaken. The body's own time is ``body_grace``.
        Default: 30. Per app: another app in the same process keeps its own.

        A sync handler in a sub-interpreter worker still running then is interrupted. An
        ``async def`` handler's task is not cancelled: it runs to its end and its late answer
        is dropped; only stopping the server cancels it. Example: ``app.request_timeout = 60``.
        """
        return self._engine.request_timeout()

    @request_timeout.setter
    def request_timeout(self, seconds: float) -> None:
        self._engine.set_request_timeout(seconds)

    @property
    def body_grace(self) -> float:
        """Seconds a request body may take to arrive however slowly it comes; past it the body
        goes on only while it averages at least ``body_min_rate``, else ``408 Request Timeout``.
        Default: 5. Per app. Example: ``app.body_grace = 15``.
        """
        return self._engine.body_grace()

    @body_grace.setter
    def body_grace(self, seconds: float) -> None:
        self._engine.set_body_grace(seconds)

    @property
    def body_min_rate(self) -> int:
        """The average bytes per second a request body must keep up to go on past
        ``body_grace``: its bytes pay for one second each ``body_min_rate`` of them, so an
        upload averaging at least this fast is never cut off as too slow (up to
        ``body_time_limit``), and one averaging slower gets ``408`` at ``body_grace``, or as
        soon as its bytes stop paying for the time it has taken. For a streamed body
        (``body=BodyStream``), the time it waits for the handler to read is not counted.
        Default: 1024. Per app. Example: ``app.body_min_rate = 64 * 1024``.
        """
        return self._engine.body_min_rate()

    @body_min_rate.setter
    def body_min_rate(self, bytes_per_second: int) -> None:
        self._engine.set_body_min_rate(bytes_per_second)

    @property
    def body_time_limit(self) -> float:
        """The most seconds a request body may take, however fast it keeps arriving (see
        ``body_min_rate``); past it: ``408``. It bounds how long one client can hold a
        connection with an upload. Default: 30. Per app. Example: ``app.body_time_limit = 3600``.
        """
        return self._engine.body_time_limit()

    @body_time_limit.setter
    def body_time_limit(self, seconds: float) -> None:
        self._engine.set_body_time_limit(seconds)

    @property
    def interrupt_grace(self) -> float:
        """Seconds a sync handler that was interrupted at its request budget (see
        ``request_timeout``) gets to end its request before its worker is abandoned and
        replaced. Default: 2. Per app. Example: ``app.interrupt_grace = 0.5``.
        """
        return self._engine.interrupt_grace()

    @interrupt_grace.setter
    def interrupt_grace(self, seconds: float) -> None:
        self._engine.set_interrupt_grace(seconds)

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
        self._engine.set_shutdown_timeout(seconds)

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
        self._engine.set_max_websocket_message_size(size)

    @property
    def max_websocket_connections(self) -> int:
        """Concurrent WebSocket connections. Default: 1024.

        An upgrade beyond the cap is answered ``503 Service Unavailable``.
        """
        return self._engine.max_websocket_connections()

    @max_websocket_connections.setter
    def max_websocket_connections(self, count: int) -> None:
        self._engine.set_max_websocket_connections(count)

    @property
    def websocket_ping_interval(self) -> float:
        """Seconds a WebSocket client may go unheard before the server pings it. Default: 20.

        A client that sends no frame within ``websocket_ping_timeout`` of the ping is closed
        with ``1011`` and counted in ``mipyre_websocket_keepalive_timeouts_total``.
        """
        return self._engine.websocket_ping_interval()

    @websocket_ping_interval.setter
    def websocket_ping_interval(self, seconds: float) -> None:
        self._engine.set_websocket_ping_interval(seconds)

    @property
    def websocket_ping_timeout(self) -> float:
        """Seconds a pinged WebSocket client has to send a frame before it is closed with
        ``1011``. Default: 20.
        """
        return self._engine.websocket_ping_timeout()

    @websocket_ping_timeout.setter
    def websocket_ping_timeout(self, seconds: float) -> None:
        self._engine.set_websocket_ping_timeout(seconds)

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
            # Every sub-interpreter of this app loads the isolation adapter.
            self._engine._use_isolation()

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
        self._engine.set_cors_config(
            _cors_values("allow_origins", allow_origins),
            ", ".join(_cors_values("allow_methods", allow_methods)),
            ", ".join(_cors_values("allow_headers", allow_headers)),
            ", ".join(_cors_values("expose_headers", expose_headers)) or None,
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

    def websocket(
        self, path: str, handler: Callable | None = None, *, runtime: Runtime | str | None = None
    ) -> Callable:
        """Register an ``async def`` WebSocket handler. Use as decorator or direct call.

        The handler receives a ``WebSocket`` whose receives and sends are awaited::

            @app.websocket("/ws")
            async def ws_handler(ws):
                async for msg in ws:
                    await ws.send(f"echo: {msg}")

        ``path`` is a route path like any other: ``{name}`` parameters land in
        ``ws.request.params``. A ``def`` handler raises ``TypeError``.

        ``runtime``: where the handler runs, the app's runtime unless given. By default each
        connection is a task on one of the async sub-interpreter workers' engines, which share
        no GIL; module state is per worker, so connections on different workers do not see
        each other's globals (use ``app.state`` to share). ``runtime="main"`` runs every
        connection on the main interpreter's async engine instead, where they share the main
        interpreter's globals and its GIL. ``runtime="process"`` raises ``ValueError``.

        The ``before_request`` hooks run on the engine's loop before the 101: write the hooks
        an app with WebSocket routes has as ``async def``, since a ``def`` hook that blocks
        stalls every connection on that engine while it runs.

        A connection waiting for a message holds no thread. The number of open connections is
        capped by ``max_websocket_connections`` (default 1024; past it an upgrade is answered
        503).
        """
        place = self.runtime if runtime is None else Runtime.parse(runtime, f"websocket {path}")
        if place is Runtime.ISOLATED:
            self._engine._use_isolation()
        engine_runtime = _ENGINE_RUNTIME[place]
        return _decorator_or_call(lambda fn: self._engine.websocket(path, fn, engine_runtime), handler)

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

    def run(self, config: ServerFlags | None = None, *, reload: bool = False) -> None:
        """Serve the app until SIGINT: this process's deployment (environment,
        ``pyproject.toml``) laid over ``config``, the flags the code gives
        (``ServerFlags(port=8000, threads=4)``); a deployment value wins. In a sub-interpreter
        worker, which executes the script to register its routes, it returns at once."""
        if _in_worker():
            return
        # Imported here: only the main interpreter serves, and a worker never loads it.
        from mipyre import _server_run

        deployment = Deployment.from_process({}, EntryPoint.Script)
        flags = ServerFlags() if config is None else config
        _server_run.run(self, deployment, flags, _server_run.Launch(), reload=deployment.reload(reload))

    def stop(self) -> None:
        """Stop this app's servers in this process the way SIGTERM does, from any thread.

        Each running ``run()`` (and ``TestClient`` server) of the app drains its connections,
        runs the shutdown hooks and returns; this call does not wait for that. A run whose
        startup hooks are still running stops as soon as its server starts. With none running
        it does nothing, and a run started afterwards serves."""
        if self._runs is not None:
            self._runs.stop()

    def lane_stats(self) -> dict[str, Any] | None:
        """Return a snapshot of the process lanes' counters and worker pids (``respawn_us`` in
        microseconds), or ``None`` before a server prepared the app or when it has no process
        routes."""
        lanes = None if self._runs is None else self._runs.process_lanes
        return None if lanes is None else lanes.stats()


def _cors_values(name: str, value: object) -> list[str]:
    """Return a CORS setting given as one ``str`` or a list or tuple of them as a list.

    Raises ``TypeError`` naming the setting for anything else, so a typo fails here rather
    than as the engine's argument error or as a header such as ``None, 8080``."""
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)) and all(isinstance(v, str) for v in value):
        return list(value)
    raise TypeError(f"CORS: {name} must be a str or a list/tuple of str, got {value!r}")


def _creating_frame(app: object) -> FrameType | None:
    """Return the frame of the code that called ``Mipyre(...)`` for ``app``, stepping out of
    ``Mipyre.__init__`` and any subclass ``__init__`` that called it; None when none is left."""
    frame: FrameType | None = sys._getframe(1)
    while frame is not None and frame.f_locals.get("self") is app:
        frame = frame.f_back
    return frame


def _module_file(frame: FrameType | None) -> str | None:
    """Return the file whose module-level code ``frame`` runs — the script sub-interpreter
    workers can execute to rebuild the app — or None when it is a function's frame."""
    if frame is None or frame.f_code.co_name != "<module>":
        return None
    file = frame.f_globals.get("__file__")
    return os.path.abspath(file) if file else None
