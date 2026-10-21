"""High-level Mipyre application class with decorator syntax."""

from __future__ import annotations

import contextlib
import enum
import sys
import threading
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Callable, Iterator, Literal, Mapping, TypedDict, overload
import inspect
import json as _json_module

import os

from mipyre.engine import Compression, GivenSize, LogLevel, Mode, MipyreApp as _MipyreApp, Response, SharedState, SizeOrigin, flush_logs, init_logger, emit_python_log, _in_worker, _python_log_level, _route_params, _physical_cores
from mipyre.engine import _routers_to_load as _engine_routers_to_load
from mipyre import _log_bridge
from mipyre._bind import bind_handler
from mipyre._csrf import OriginPolicy
from mipyre.mcp import MCPServer
from mipyre import _reload
from mipyre import _runtime
from mipyre._runtime import ProcessLanes
import logging as _logging

if TYPE_CHECKING:
    from types import FrameType

    from mipyre.engine import LaneSet
    from mipyre.health import ReadinessCheck


# A level by name, in either case; `LogLevel.parse` reads it.
LogLevelName = Literal[
    "off", "error", "warn", "warning", "info", "debug", "trace",
    "OFF", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "TRACE",
]


class LogConfig(TypedDict, total=False):
    """Logging configuration dictionary.

    Keys:
        level: a ``LogLevel``, or its name ("OFF", "ERROR", "WARN", "INFO", "DEBUG",
            "TRACE"); an unknown name raises ``ValueError`` in ``Mipyre()``
        access_log: Whether to log every HTTP request (method, path, status, latency)
        format: "text" (human-readable) or "json" (structured, for ELK/Datadog)
    """
    level: LogLevel | LogLevelName
    access_log: bool
    format: Literal["text", "json"]


def _parse_level(level: LogLevel | LogLevelName) -> LogLevel:
    """Return ``level`` as a ``LogLevel``; an unknown name raises ``ValueError``."""
    return level if isinstance(level, LogLevel) else LogLevel.parse(level)


@dataclass(frozen=True)
class _AccessLogChoice:
    """The access log the user turned on with ``enable_access_log()``, at the level they
    gave it (``None``: no level given, the configured one stands)."""

    level: LogLevel | None


@dataclass(frozen=True)
class _LogSettings:
    """Every key of a ``LogConfig`` set and its level parsed."""

    level: LogLevel
    access_log: bool
    format: Literal["text", "json"]

    @classmethod
    def resolve(cls, debug: bool, user: LogConfig) -> _LogSettings:
        """Return the user's settings over the defaults of the mode: debug logs text with
        the access log at DEBUG, production logs JSON at ERROR without it."""
        if debug:
            return cls(
                level=_parse_level(user.get("level", LogLevel.Debug)),
                access_log=user.get("access_log", True),
                format=user.get("format", "text"),
            )
        return cls(
            level=_parse_level(user.get("level", LogLevel.Error)),
            access_log=user.get("access_log", False),
            format=user.get("format", "json"),
        )

    def with_access_log(self, choice: _AccessLogChoice | None, launch_log: bool) -> _LogSettings:
        """Return the settings a server applies: these, with the access log on when the
        user's ``choice`` or the launch (``MIPYRE_LOG=1``, ``mipyre dev``) asks for it, at
        the level the choice gave, else at the configured one raised to INFO when that
        would hide the access lines (ERROR, OFF). Asked by neither, these as they are."""
        if choice is None and not launch_log:
            return self
        if choice is not None and choice.level is not None:
            level = choice.level
        elif self.level in (LogLevel.Error, LogLevel.Off):
            level = LogLevel.Info
        else:
            level = self.level
        return replace(self, level=level, access_log=True)


@dataclass(frozen=True)
class RouteInfo:
    """A registered route, as ``app.routes`` lists it."""

    method: str
    path: str
    handler: str  # the handler's qualified name
    gil: bool
    stream: bool
    model: str | None  # the ``model=`` class's name
    is_async: bool
    runtime: str = "subinterp"  # "subinterp" | "isolated" | "process" | "main"
    query: str | None = None  # the ``query=`` class's name


@dataclass(frozen=True)
class RouteDeclaration:
    """A route as the code declared it: the handler before the body, query and path params are
    bound to it, and the ``model=`` and ``query=`` classes. ``app.enable_openapi()`` describes
    the routes from these."""

    method: str
    path: str
    handler: Callable
    model: type | None
    query: type | None = None


@dataclass(frozen=True)
class _OpenApiSettings:
    path: str
    title: str
    version: str


@dataclass(frozen=True)
class FastRouteInfo:
    """A route registered with ``add_fast_response``, as ``app.fast_routes`` lists it."""

    method: str
    path: str
    status_code: int
    content_type: str
    body_bytes: int


def _is_worker() -> bool:
    """Whether this code runs in a sub-interpreter worker (not the main interpreter)."""
    return _in_worker()


def _require_int(name: str, value: object) -> None:
    """``bool`` is an ``int`` subclass; a limit set to ``True`` is a bug, not 1."""
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be int, got {type(value).__name__}")


class Transport(enum.Enum):
    """How a server takes connections: TCP sockets (what ``run()`` serves), or in-memory
    connections made in this process (``mipyre.testing.TestClient``'s default), which reach
    the same accept loops and request pipeline with no socket."""

    TCP = "tcp"
    MEMORY = "memory"


class Mipyre:
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
        runtime: str | None = None,
        routers: list[str] | tuple[str, ...] = (),
        process: ProcessLanes | None = None,
    ) -> None:
        """``runtime``: where this app's routes run unless a route or its router says
        otherwise — ``"subinterp"`` (default), ``"isolated"``, ``"process"`` or ``"main"``. ``routers``:
        ``Router``s by import string (``"myapp.analytics:router"``); each runtime imports
        only the routers that have routes it serves. ``process``: process-lane settings."""
        self._engine = _MipyreApp()
        self.runtime = _runtime.resolve_app_runtime(
            runtime,
            "Mipyre(...)",
            _runtime.deployment_runtime(os.environ, _runtime.ConfigFile.load(os.getcwd())),
        )
        if process is not None and not isinstance(process, ProcessLanes):
            raise TypeError(f"process= takes a mipyre.ProcessLanes, got {type(process).__name__}")
        self._process_declared = process
        self._router_targets = tuple(routers)
        # Routers by runtime they have routes in (filled by _load_routers, main only).
        self._routers_by_runtime: dict[str, list[str]] = {r: [] for r in _runtime.RUNTIMES}
        self._fallback_handler: Callable | None = None
        self._fallback_name: str | None = None
        self._mcp = MCPServer()
        self.debug = debug
        self._startup_hooks: list[Callable] = []
        self._shutdown_hooks: list[Callable] = []
        self._routes_meta: list[RouteInfo] = []
        self._openapi_routes: list[RouteDeclaration] = []
        self._fast_routes_meta: list[FastRouteInfo] = []
        self._readiness_checks: list[ReadinessCheck] = []
        self._health_probes_enabled: bool = False
        self._app_file_path: str | None = None
        self._origin_policy = OriginPolicy()
        # Parsed here, so a bad level fails at construction; applied by the first server.
        self._configured_log = _LogSettings.resolve(debug, log_config or {})
        self._access_log_choice: _AccessLogChoice | None = None
        # _prepare()'s state, under its lock: two servers of one app may start at once
        # (nested TestClients), and each must find the app prepared exactly once.
        self._prepare_lock = threading.Lock()
        self._prepared = False
        self._process_lanes: LaneSet | None = None
        self._mcp_route_registered = False
        self._openapi: _OpenApiSettings | None = None
        self._openapi_route_registered = False
        # The servers this app is serving (engine `Server`s), for `_stop()`.
        self._servers_lock = threading.Lock()
        self._servers: set = set()
        self._defined_in = _defining_module_file(self)
        # Guards the enable_* helpers' check-then-set: two threads must not both pass
        # "already enabled?" and register the routes/hooks twice.
        self._enable_lock = threading.Lock()
        self._request_id_enabled = False
        self._metrics_enabled = False
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
            router = _runtime.import_router(target)
            runtimes = set()
            for r in router._routes:
                runtime = r.runtime or router.runtime or self.runtime
                runtimes.add(runtime)
                self._route(r.method, r.path, r.handler, model=r.model, query=r.query, stream=r.stream, runtime=runtime)
            for runtime in runtimes:
                self._routers_by_runtime[runtime].append(target)
        self._engine._set_subinterp_routers(
            list(dict.fromkeys(t for r in _runtime.SUBINTERP_RUNTIMES for t in self._routers_by_runtime[r]))
        )

    @property
    def mcp(self) -> MCPServer:
        """MCP (Model Context Protocol) server for AI tool integration."""
        return self._mcp

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
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            raise TypeError(f"request_timeout must be int or float seconds, got {type(seconds).__name__}")
        self._engine.set_request_timeout(float(seconds))

    @property
    def body_min_rate(self) -> int:
        """Bytes per second at which a request body earns time past ``request_timeout``: a
        body gets one more second for every ``body_min_rate`` bytes it delivers, so an upload
        that keeps arriving at least this fast is never cut off as too slow (up to
        ``body_time_limit``), and one that trickles slower gets ``408`` when the time it earned
        runs out. For a streamed body (``stream=True``), the time it waits for the handler to
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
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            raise TypeError(f"body_time_limit must be int or float seconds, got {type(seconds).__name__}")
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
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            raise TypeError(f"interrupt_grace must be int or float seconds, got {type(seconds).__name__}")
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
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            raise TypeError(f"shutdown_timeout must be int or float seconds, got {type(seconds).__name__}")
        self._engine.set_shutdown_timeout(float(seconds))

    @property
    def trusted_origins(self) -> list[str]:
        """Other sites whose pages may call ``/mcp`` and ``@app.rpc`` endpoints from a
        browser, as ``scheme://host[:port]``. Default: none.

        Those endpoints act on a JSON (or MsgPack / Protobuf) POST, so they refuse a
        request whose ``Origin`` is another site with 403, and any other body type with
        415: a page can't make a visitor's browser call them (CSRF). Requests without an
        ``Origin`` (curl, SDKs, servers) and from this server's own host are always
        admitted. A bad entry raises ``ValueError``.
        """
        return sorted(
            f"{o.scheme}://{o.host}:{o.port}" for o in self._origin_policy.trusted
        )

    @trusted_origins.setter
    def trusted_origins(self, origins: list[str]) -> None:
        self._origin_policy = OriginPolicy.of(origins)

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
        self._fast_routes_meta.append(FastRouteInfo(
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

    def _serving_runtime(self) -> str:
        """The runtime the server's topology follows: the app's, except that an app whose
        default is "main" but which declares sub-interpreter routes gets sub-interpreters
        (they never run on main in its place)."""
        if self.runtime == "main" and any(r.runtime in _runtime.SUBINTERP_RUNTIMES for r in self._routes_meta):
            return "subinterp"
        return self.runtime

    def isolate(self, *libraries: str) -> None:
        """Raise TypeError: ``isolate()`` was removed, and the error says what to write instead."""
        raise TypeError(
            f"app.isolate({', '.join(repr(lib) for lib in libraries)}) was removed in favour of per-route runtimes. "
            "Routes that use C extensions run with runtime='process' (worker processes); "
            "runtime='isolated' keeps the per-worker copies in sub-interpreters as a "
            "compatibility option (all such extensions, automatically; with risks)."
        )

    # ------------------------------------------------------------------
    # Route registration (decorator + direct call)
    # ------------------------------------------------------------------

    def get(self, path: str, handler: Callable | None = None, *, model: type | None = None, query: type | None = None, runtime: str | None = None, **removed: object) -> Callable:
        return self._route("GET", path, handler, model=model, query=query, runtime=runtime, removed=removed)

    def post(self, path: str, handler: Callable | None = None, *, model: type | None = None, query: type | None = None, stream: bool = False, runtime: str | None = None, **removed: object) -> Callable:
        return self._route("POST", path, handler, model=model, query=query, stream=stream, runtime=runtime, removed=removed)

    def put(self, path: str, handler: Callable | None = None, *, model: type | None = None, query: type | None = None, stream: bool = False, runtime: str | None = None, **removed: object) -> Callable:
        return self._route("PUT", path, handler, model=model, query=query, stream=stream, runtime=runtime, removed=removed)

    def delete(self, path: str, handler: Callable | None = None, *, model: type | None = None, query: type | None = None, runtime: str | None = None, **removed: object) -> Callable:
        return self._route("DELETE", path, handler, model=model, query=query, runtime=runtime, removed=removed)

    def patch(self, path: str, handler: Callable | None = None, *, model: type | None = None, query: type | None = None, stream: bool = False, runtime: str | None = None, **removed: object) -> Callable:
        return self._route("PATCH", path, handler, model=model, query=query, stream=stream, runtime=runtime, removed=removed)

    def options(self, path: str, handler: Callable | None = None, *, model: type | None = None, query: type | None = None, runtime: str | None = None, **removed: object) -> Callable:
        return self._route("OPTIONS", path, handler, model=model, query=query, runtime=runtime, removed=removed)

    def head(self, path: str, handler: Callable | None = None, *, model: type | None = None, query: type | None = None, runtime: str | None = None, **removed: object) -> Callable:
        return self._route("HEAD", path, handler, model=model, query=query, runtime=runtime, removed=removed)

    def route(self, method: str, path: str, handler: Callable | None = None, *, model: type | None = None, query: type | None = None, stream: bool = False, runtime: str | None = None, **removed: object) -> Callable:
        return self._route(method.upper(), path, handler, model=model, query=query, stream=stream, runtime=runtime, removed=removed)

    def _route(self, method: str, path: str, handler: Callable | None, *, model: type | None = None, query: type | None = None, stream: bool = False, runtime: str | None = None, removed: dict | None = None, in_openapi: bool = True) -> Callable:
        where = f"{method} {path}"
        _runtime.reject_removed_route_args(where, removed or {})
        _runtime.check_runtime(runtime, where)
        if model is not None and stream:
            raise ValueError(
                f"{where}: model= and stream=True can't be used together: model= validates the "
                "whole body before the handler runs, and a stream=True route's body is never "
                "whole; it is read as it arrives, through req.stream. Drop one of them."
            )
        runtime = runtime or self.runtime
        if query is not None and runtime == "subinterp":
            raise _runtime.RuntimeDeclarationError(
                f"{where}: query= runs on runtime={runtime!r}, but query= validates with pydantic, "
                "whose core (pydantic_core) can't load in a sub-interpreter. Declare the route "
                "runtime=\"main\", \"process\" or \"isolated\" (a per-worker copy of pydantic)."
            )
        gil, process = runtime == "main", runtime == "process"
        if runtime == "isolated":
            # Every sub-interpreter of this app loads the isolation adapter (design §14.3).
            self._engine._use_isolation()
            self._has_isolated_routes = True

        def register(fn: Callable) -> Callable:
            bound = bind_handler(fn, path, model, query)
            self._engine.route(method, path, bound, gil, stream, process)
            self._routes_meta.append(RouteInfo(
                method=method,
                path=path,
                handler=getattr(fn, "__qualname__", getattr(fn, "__name__", repr(fn))),
                gil=gil,
                stream=stream,
                model=model.__name__ if model is not None else None,
                is_async=inspect.iscoroutinefunction(fn),
                runtime=runtime,
                query=query.__name__ if query is not None else None,
            ))
            if in_openapi:
                self._openapi_routes.append(RouteDeclaration(method=method, path=path, handler=fn, model=model, query=query))
            # The bound callable, not fn: calling the decorated name does what the
            # route does (model validation, path-param injection).
            return bound

        return register(handler) if handler is not None else register

    # ------------------------------------------------------------------
    # Middleware
    # ------------------------------------------------------------------

    def enable_cors(
        self,
        *,
        allow_origins: str | list[str] = "*",
        allow_methods: str | list[str] = "GET, POST, PUT, DELETE, PATCH, OPTIONS",
        allow_headers: str | list[str] = "*",
        expose_headers: str | list[str] = "",
        allow_credentials: bool = False,
        max_age: int | None = 86400,
    ) -> None:
        """Enable CORS (Cross-Origin Resource Sharing).

        The headers go on every response; a preflight (an ``OPTIONS`` with
        ``Access-Control-Request-Method``) to a path with no ``OPTIONS`` route is
        answered 204 with ``Access-Control-Max-Age: max_age`` (``None``: no such header).
        ``Access-Control-Allow-Origin`` holds a single origin, so with several
        ``allow_origins`` a response names the request's ``Origin`` when it is one of
        them and carries no CORS header when it is not, with ``Vary: Origin`` either way.
        ``allow_origins='*'`` with ``allow_credentials=True`` raises ``ValueError``.

        Usage::

            app.enable_cors()  # Allow all origins

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

        # The engine owns the rule: it refuses `*` with credentials, puts the headers on every
        # response and answers preflights.
        self._engine.set_cors_config(
            _normalize_cors_value("allow_origins", allow_origins),
            _normalize_cors_value("allow_methods", allow_methods),
            _normalize_cors_value("allow_headers", allow_headers),
            _normalize_cors_value("expose_headers", expose_headers) or None,
            allow_credentials,
            max_age,
        )

    # ------------------------------------------------------------------

    def rpc(self, path: str, *, proto_model: type | None = None) -> Callable:
        """Register an RPC endpoint with content negotiation.

        Supports MsgPack, JSON, and optional Protobuf auto-decode/encode.

        Usage::

            @app.rpc("/rpc/get_data")
            def get_data(req):
                return {"prices": [150.1, 150.2]}
        """
        from mipyre.rpc import rpc_decorator
        return rpc_decorator(self, path, proto_model)

    # ------------------------------------------------------------------

    def before_request(self, handler: Callable | None = None) -> Callable:
        """Register a before-request hook. Use as decorator or direct call.

        The hook receives ``(request)`` and should return ``None`` to continue
        or a response to short-circuit.
        """
        if handler is not None:
            self._engine.before_request(handler)
            return handler

        def decorator(fn: Callable) -> Callable:
            self._engine.before_request(fn)
            return fn

        return decorator

    def after_request(self, handler: Callable | None = None) -> Callable:
        """Register an after-request hook. Use as decorator or direct call.

        The hook receives ``(request, response)`` and must return a
        ``Response``.
        """
        if handler is not None:
            self._engine.after_request(handler)
            return handler

        def decorator(fn: Callable) -> Callable:
            self._engine.after_request(fn)
            return fn

        return decorator

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
        if handler is not None:
            self._startup_hooks.append(handler)
            return handler

        def decorator(fn: Callable) -> Callable:
            self._startup_hooks.append(fn)
            return fn

        return decorator

    def on_shutdown(self, handler: Callable | None = None) -> Callable:
        """Register a shutdown hook. Called after the server stops.

        Usage::

            @app.on_shutdown
            def cleanup():
                close_pool(app.state["db"])
        """
        if handler is not None:
            self._shutdown_hooks.append(handler)
            return handler

        def decorator(fn: Callable) -> Callable:
            self._shutdown_hooks.append(fn)
            return fn

        return decorator

    # ------------------------------------------------------------------
    # Observability — X-Request-ID + /metrics
    # ------------------------------------------------------------------

    def enable_request_id(self, header: str = "X-Request-ID") -> None:
        """Guarantee every response carries an ``X-Request-ID`` header.

        If the client sent one (visible ASCII, at most 128 bytes), it's
        echoed back verbatim, so trace IDs propagated from an upstream proxy
        survive. If not, the server's own id (32 hex digits) is used. Either
        way it is ``req.request_id``, and a 5xx response and its error log
        line report the same id. Idempotent.
        """
        with self._enable_lock:
            if self._request_id_enabled:
                return
            from mipyre.observability import install_request_id
            self._engine.set_request_id_header(header)
            install_request_id(self, header)
            self._request_id_enabled = True

    def enable_metrics(self, path: str = "/metrics") -> None:
        """Expose Prometheus metrics at ``path`` (default ``/metrics``).

        Registers a ``GET /metrics`` route plus before/after-request hooks
        that maintain counters in ``app.state``. The scrape endpoint does
        not count itself. Idempotent.
        """
        with self._enable_lock:
            if self._metrics_enabled:
                return
            from mipyre.observability import install_metrics
            install_metrics(self, path)
            self._metrics_enabled = True

    def enable_openapi(self, *, title: str, version: str, path: str = "/openapi.json") -> None:
        """Serve this app's OpenAPI 3.1 document at ``GET path`` (default ``/openapi.json``).

        The document is built once, when the first server starts, from the routes registered
        by then, and each request gets the same bytes. ``title`` and ``version`` are the
        document's ``info``. Off by default: the document tells anyone who can reach the
        server every route and body schema, so an app opts in, and the route goes through the
        app's ``before_request`` hooks like any other.

        :raises ValueError: ``path`` has a path parameter, or a second call asks for a
            different path, title or version.
        """
        if _route_params(path):
            raise ValueError(f"enable_openapi: path {path!r} has a path parameter; the document has one fixed path")
        wanted = _OpenApiSettings(path=path, title=title, version=version)
        with self._enable_lock:
            if self._openapi is not None and self._openapi != wanted:
                raise ValueError(f"enable_openapi: already enabled as {self._openapi}, asked again as {wanted}")
            self._openapi = wanted

    def openapi(self) -> dict:
        """Return the OpenAPI 3.1 document of the routes registered so far: what
        ``enable_openapi()`` serves once a server starts. A test or a build step can hand it to
        a tool (Schemathesis, a client generator) without serving the app.

        :raises NoOpenApiDocument: ``enable_openapi()`` has not been called.
        """
        if self._openapi is None:
            raise NoOpenApiDocument()
        from mipyre import openapi

        return openapi.document(self._openapi.title, self._openapi.version, self._openapi_routes, self._fast_routes_meta)

    # ------------------------------------------------------------------
    # Health probes — /livez + /readyz
    # ------------------------------------------------------------------

    def enable_health_probes(
        self,
        *,
        livez_path: str = "/livez",
        readyz_path: str = "/readyz",
        runtime: str = "main",
    ) -> None:
        """Register Kubernetes-style ``/livez`` and ``/readyz`` routes.

        ``/livez`` always returns 200 — the process is alive. ``/readyz``
        runs every function registered with ``@app.readiness_check(name)``
        and returns 200 only when all pass; any failure produces 503 with a
        JSON body listing the failing check.

        Call once at startup, typically right after ``Mipyre()``. Idempotent —
        calling again is a no-op.

        Args:
            livez_path: route for the liveness probe (default ``/livez``).
            readyz_path: route for the readiness probe (default ``/readyz``).
            runtime: where the probes run. Default ``"main"`` because most
                 readiness checks touch globals (DB pools, cache clients) that
                 live on the main interpreter.
        """
        with self._enable_lock:
            if self._health_probes_enabled:
                return
            from mipyre.health import _build_livez_handler, _build_readyz_handler

            self._route("GET", livez_path, _build_livez_handler(), runtime=runtime)
            self._route(
                "GET",
                readyz_path,
                _build_readyz_handler(self._readiness_checks),
                runtime=runtime,
            )
            self._health_probes_enabled = True

    def readiness_check(self, name: str) -> Callable:
        """Register a readiness check. Sync or async. Decorator form::

            @app.readiness_check("db")
            def _db_ready():
                pool.fetch_scalar("SELECT 1")

        A check fails when it raises, takes longer than the probe timeout (10 s),
        or returns ``False``; any other value, ``None`` included, passes. A failing
        check is reported in ``/readyz`` and the whole probe returns 503.
        """
        from mipyre.health import ReadinessCheck

        def decorator(fn: Callable) -> Callable:
            self._readiness_checks.append(ReadinessCheck.of(name, fn))
            return fn

        return decorator

    # ------------------------------------------------------------------
    # Fallback (custom 404)
    # ------------------------------------------------------------------

    def fallback(self, handler: Callable | None = None) -> Callable:
        """Register a fallback handler for unmatched routes."""
        if handler is not None:
            self._engine.fallback(handler)
            return handler

        def decorator(fn: Callable) -> Callable:
            self._engine.fallback(fn)
            return fn

        return decorator

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
        if handler is not None:
            self._engine.websocket(path, handler)
            return handler

        def decorator(fn: Callable) -> Callable:
            self._engine.websocket(path, fn)
            return fn

        return decorator

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

            INFO  mipyre::access Request handled method=GET path=/ status=200 latency_us=198 mode="gil"

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
        kept = None if self._access_log_choice is None else self._access_log_choice.level
        self._access_log_choice = _AccessLogChoice(kept if requested is None else requested)

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def _set_app_file(self, path: str) -> None:
        """The source file that defines this app: workers execute it, and the
        reloader watches its directory. Defaults to ``__main__``'s file."""
        self._app_file_path = path
        self._engine.set_script_path(path)

    def _app_file(self) -> str:
        if self._app_file_path is not None:
            return self._app_file_path
        main = sys.modules["__main__"]
        main_file = getattr(main, "__file__", None)
        if main_file is None:
            raise NoReloadScript(main=repr(main), argv0=_argv0())
        return os.path.abspath(main_file)

    @property
    def routes(self) -> list[RouteInfo]:
        """The registered routes, in registration order.

        Populated as routes are registered via decorators or direct calls.
        Fast-path routes (``add_fast_response``) appear in ``fast_routes``.
        """
        return list(self._routes_meta)

    @property
    def fast_routes(self) -> list[FastRouteInfo]:
        """Routes registered via ``add_fast_response``."""
        return list(self._fast_routes_meta)

    def run(
        self,
        *,
        host: str | None = None,
        port: int | None = None,
        workers: int | None = None,
        reload: bool = False,
        io_workers: int | None = None,
        tls_cert: str | None = None,
        tls_key: str | None = None,
        extra_tls_ports: list[int] | None = None,
        extra_plain_ports: list[int] | None = None,
        **removed: object,
    ) -> None:
        """Start the Mipyre server.

        Args:
            workers: The thread-per-core server's thread count (default: physical
                        cores), or with ``MIPYRE_TPC=0`` the sub-interpreter pool's size
                        (default: logical CPUs). Default: ``MIPYRE_WORKERS``.
            io_workers: With ``MIPYRE_TPC=0`` only, the Tokio I/O thread count and
                        accept loop count (default: logical CPUs). Default:
                        ``MIPYRE_IO_WORKERS``.

            A size the selected server does not use is ignored, not an error: one script
            can run under several ``MIPYRE_TPC`` / ``MIPYRE_RUNTIME`` settings. The
            server prints a ``Note:`` line saying which size, where it was given, and why
            it is not used: ``io_workers`` on the thread-per-core server (each TPC thread
            does its own I/O), and ``workers`` when no handler runs on a sub-interpreter
            (``runtime="main"`` app, or ``MIPYRE_TPC=0`` with no route on one).
            tls_cert: Path to PEM certificate chain (enables HTTPS).
                      Required together with ``tls_key``. Default None → plain HTTP.
            tls_key:  Path to PEM private key. Required together with ``tls_cert``.
            extra_tls_ports: More ports serving TLS (``MIPYRE_TLS_PORTS``); ``port``
                        then serves plain HTTP.
            extra_plain_ports: More ports serving plain HTTP/1.1 and h2c (HTTP/2 with
                        prior knowledge) (``MIPYRE_PLAIN_PORTS``).
        """
        _runtime.reject_removed_serve_args("app.run()", removed)
        self._run(
            os.environ,
            _Launch(),
            host=host,
            port=port,
            workers=workers,
            reload=reload,
            io_workers=io_workers,
            tls_cert=tls_cert,
            tls_key=tls_key,
            extra_tls_ports=extra_tls_ports,
            extra_plain_ports=extra_plain_ports,
        )

    def _run(
        self,
        env: Mapping[str, str],
        launch: _Launch,
        *,
        host: str | None,
        port: int | None,
        workers: int | None,
        reload: bool,
        io_workers: int | None = None,
        tls_cert: str | None = None,
        tls_key: str | None = None,
        extra_tls_ports: list[int] | None = None,
        extra_plain_ports: list[int] | None = None,
    ) -> None:
        """``run()`` with the environment it reads and what the ``mipyre`` launcher adds
        (``launch``) given."""
        # In a worker the script only registers routes; the server runs on main.
        if _is_worker():
            return
        settings = _ServeSettings.resolve(
            env=env,
            launch=launch,
            host=host,
            port=port,
            workers=workers,
            app_runtime=self._serving_runtime(),
            io_workers=io_workers,
            tls_cert=tls_cert,
            tls_key=tls_key,
            extra_tls_ports=extra_tls_ports,
            extra_plain_ports=extra_plain_ports,
        )

        reload = reload or env.get("MIPYRE_RELOAD") == "1"
        if reload and not _reload.is_reload_child():
            _reload.run_with_reload(_reload.ReloadTarget.of_this_process(self._app_file()))
            return

        # The log is written by a background thread the process exit does not wait for: every
        # way out of a run waits for the lines logged before it (b/441 L6).
        try:
            self._serve(settings, self._start)
        except WorkersAbandoned as e:
            from mipyre import _finalize

            _finalize.exit_without_finalization(str(e))
        finally:
            flush_logs()

    def _serve(self, settings: _ServeSettings, start: Callable[[_ServeSettings], None]) -> None:
        """One server's lifetime: prepare the app (once), run the startup hooks, ``start``
        it (bind and serve until it stops), run the shutdown hooks."""
        self._prepare(settings)

        graceful = False
        run_error = None
        try:
            # Run startup hooks inside the try so shutdown hooks still run on failure
            for hook in self._startup_hooks:
                hook()
            start(settings)
            graceful = True  # engine returned after its own drain (SIGINT or _stop())
        except KeyboardInterrupt:
            # SIGINT reaches BOTH Rust (which drains connections and returns) and
            # Python's main thread (which raises KeyboardInterrupt — here, or a
            # few bytecodes later). Either way it's a graceful stop.
            graceful = True
        except BaseException as e:  # noqa: BLE001 — real failure, re-raised below
            run_error = e

        with _sigint_ignored() if graceful else contextlib.nullcontext():
            # Best-effort teardown barrier: one hook failing (e.g. a pool already closed)
            # must not stop the rest, so log each failure with its type and traceback.
            for hook in self._shutdown_hooks:
                try:
                    hook()
                except Exception as e:
                    _logging.getLogger("mipyre.app").exception(
                        "shutdown hook %s raised",
                        getattr(hook, "__name__", repr(hook)),
                        extra={"error_type": type(e).__name__},
                    )

        # A worker thread that outlived the shutdown grace period (a handler that ignores
        # shutdown) may still be in its interpreter, which finalization would free under it.
        # Say which; the caller decides what the process does now (`run()` exits, a
        # TestClient raises), and the process does not finalize while the thread still owns
        # its interpreter. A run that also failed keeps its error as the cause.
        abandoned = [str(w) for w in self._engine._take_abandoned_workers()]
        if abandoned:
            # Imported here: only the main interpreter serves, and a worker never loads it.
            from mipyre import _finalize

            _finalize.refuse_finalization_while_owned(abandoned)
            _logging.getLogger("mipyre.app").error(
                "worker(s) %s did not stop within the shutdown grace period",
                ", ".join(abandoned),
            )
            raise WorkersAbandoned(abandoned) from run_error

        # Not a graceful stop (real startup/run error): surface it normally.
        if run_error is not None:
            raise run_error

    @property
    def process_lanes(self):
        """The engine's lanes of this app once a server prepared it (``None`` before, or
        without process routes): worker pids and respawn counters (``stats()``)."""
        return self._process_lanes

    def _start_process_lanes(self, settings: _ServeSettings) -> None:
        """Set up the process lanes for ``runtime="process"`` routes: the template, forked
        by ``mipyre run`` before the app was imported, gets the lanes and what to import.
        Without that launcher the start is refused (the template must hold no app code)."""
        from mipyre import _process_lane

        template = _process_lane.LAUNCHER_TEMPLATE
        if not self._engine._has_process_routes():
            if template is not None:
                template.dismiss()
            return
        if self._process_lanes is not None:
            return
        if template is None:
            raise _process_lane.ProcessLanesNeedLauncher(_equivalent_command(self, settings))
        lane_settings = self._lane_settings(settings)
        lanes = _process_lane.start(template, lane_settings, self._routers_by_runtime["process"])
        self._engine._set_process_lanes(lanes)
        self._process_lanes = lanes
        print(
            f"  Process lanes: {lane_settings.workers} workers (template pid {lanes.template_pid}, "
            f"timeout {lane_settings.timeout_ms} ms, queue {lane_settings.queue_per_worker}/worker)",
            flush=True,
        )

    def _lane_settings(self, settings: _ServeSettings) -> _runtime.LaneSettings:
        """The process lanes' settings for this server: the app's own, the launcher's flags,
        the environment the server was resolved from (``settings.env``), the working
        directory's ``[tool.mipyre.process]``."""
        return _runtime.LaneSettings.resolve(
            self._process_declared,
            _physical_cores(),
            launched=settings.lanes,
            env=settings.env,
            config=_runtime.ConfigFile.load(os.getcwd()),
        )

    def _prepare(self, settings: _ServeSettings) -> None:
        """Get the app ready for this server. Once per app, by its first server: seal the
        script's registrations and set up logging. Per server, as that server needs it:
        the ``/mcp`` route once there are MCP tools (a later server still gets it when the
        first had none). Everything registered here exists only on main. Locked: two servers of one app may start at once."""
        with self._prepare_lock:
            if not self._prepared:
                self._engine._seal_registrations()
                self._start_process_lanes(settings)

                log = self._configured_log.with_access_log(self._access_log_choice, settings.log)
                self._engine.enable_request_logging(log.access_log)
                init_logger(log.level, log.access_log, log.format)
                # Main and every worker gate Python logging on the level just applied.
                _log_bridge.install_root_handler(emit_python_log, _python_log_level())
                self._prepared = True

            if self._has_isolated_routes and (settings.workers is None or settings.workers.value != 1):
                from mipyre import _subinterp

                print(f"  BLAS: {_subinterp.blas_threads_limited()} (runtime='isolated'; override: set OPENBLAS_NUM_THREADS)", flush=True)

            if not self._mcp_route_registered and not self._mcp.is_empty():
                # Its contract is JSON-RPC, which MCP's own `tools/list` describes, not an OpenAPI operation.
                self._route("POST", "/mcp", self._mcp.http_handler(self), runtime="main", in_openapi=False)
                self._mcp_route_registered = True
                print(f"  MCP: {self._mcp.summary()} → POST /mcp")

            if self._openapi is not None and not self._openapi_route_registered:
                body = _json_module.dumps(self.openapi()).encode()

                def _openapi_handler(req):
                    return Response(body=body, content_type="application/json")

                self._route("GET", self._openapi.path, _openapi_handler, runtime="main", in_openapi=False)
                self._openapi_route_registered = True

    def _start(
        self,
        settings: _ServeSettings,
        on_bound: Callable[[object], None] | None = None,
        transport: Transport = Transport.TCP,
    ) -> None:
        """Bind a server (an ``OSError`` if its port is taken), hand it to ``on_bound``,
        then serve until it stops: SIGINT, its ``shutdown()``, or ``_stop()``. A
        ``Transport.MEMORY`` server binds nothing: it takes only the connections its
        ``connect()`` makes, and the settings' address and TLS are not used."""
        match transport:
            case Transport.TCP:
                server = self._engine.start(
                    host=settings.host,
                    port=settings.port,
                    workers=settings.workers,
                    mode=settings.mode,
                    io_workers=settings.io_workers,
                    tls_cert=settings.tls_cert,
                    tls_key=settings.tls_key,
                    extra_tls_ports=settings.extra_tls_ports,
                    extra_plain_ports=settings.extra_plain_ports,
                )
            case Transport.MEMORY:
                server = self._engine.start(
                    workers=settings.workers,
                    mode=settings.mode,
                    io_workers=settings.io_workers,
                    transport=transport.value,
                )
        with self._servers_lock:
            self._servers.add(server)
        try:
            if on_bound is not None:
                on_bound(server)
            server.serve()
        finally:
            with self._servers_lock:
                self._servers.discard(server)

    def _stop(self) -> None:
        """Stop every server this app is serving, as SIGINT does: each ``_start`` drains
        and returns. A no-op while nothing is serving. To stop one server of several,
        call that server's ``shutdown()``."""
        with self._servers_lock:
            servers = list(self._servers)
        for server in servers:
            server.shutdown()


@contextlib.contextmanager
def _sigint_ignored() -> Iterator[None]:
    """Ignore SIGINT for the block, then put the previous handler back.

    After a graceful stop the ctrl-C that caused it is often still pending: it fires once
    the code is back in Python bytecode, which would interrupt the first shutdown hook or
    the caller after ``run()`` returns. ``signal.signal`` delivers a pending signal before it
    installs the handler, so a ``KeyboardInterrupt`` raised by it is retried; once it
    returns none is left. Off the main thread (``ValueError``) nothing is changed. The
    handler is put back so a program that goes on after ``run()`` (stopped with
    ``_stop()``) still gets ``KeyboardInterrupt`` on ctrl-C."""
    import signal

    previous = None
    while True:
        try:
            previous = signal.signal(signal.SIGINT, signal.SIG_IGN)
            break
        except KeyboardInterrupt:
            continue
        except (ValueError, OSError):
            break
    try:
        yield
    finally:
        if previous is not None:
            signal.signal(signal.SIGINT, previous)


class WorkersAbandoned(RuntimeError):
    """Worker threads outlived the shutdown grace period (a handler that ignores the
    stop) and may still be in their interpreters, which finalizing the process would
    free under them. ``Mipyre.run()`` exits at once without finalizing (``os._exit(1)``);
    a ``TestClient`` raises this instead, and the process exits the same way at exit if
    a thread still owns its interpreter then."""

    def __init__(self, workers: list[str]):
        self.workers = list(workers)
        super().__init__(
            "worker(s) " + ", ".join(self.workers) + " did not stop within the "
            "shutdown grace period"
        )


class NoOpenApiDocument(RuntimeError):
    """``app.openapi()`` was called on an app that never called ``enable_openapi()``, which
    gives the document its title and version."""

    def __init__(self) -> None:
        super().__init__(
            "this app has no OpenAPI document: call app.enable_openapi(title=..., version=...) "
            "first; it names the document and serves it at /openapi.json"
        )


class NoReloadScript(RuntimeError):
    """``run(reload=True)`` could not start: the reloader watches the directory of the
    app's source file, and ``__main__`` has no ``__file__`` (``python -c``, the
    interactive interpreter, a host embedding Python) while no file was named. Carries
    what ``__main__`` and ``sys.argv[0]`` are, as found."""

    def __init__(self, main: str, argv0: str):
        self.main = main
        self.argv0 = argv0
        super().__init__(
            "reload watches the directory of the app's source file and restarts the app "
            f"when a file there changes, but there is no source file: __main__ is {main}, "
            f"which has no __file__ (sys.argv[0] is {argv0}). Python sets __main__.__file__ "
            "only when it runs a file (`python app.py`, `python -m pkg`); `python -c`, the "
            "interactive interpreter and hosts that embed Python leave it unset. Start the "
            "app from a file, or with `mipyre dev module:app`."
        )


def _argv0() -> str:
    """``repr(sys.argv[0])``, or why there is none (an embedding host may set no argv)."""
    argv = getattr(sys, "argv", None)
    if argv is None:
        return "absent: sys has no argv"
    if not argv:
        return "absent: sys.argv is empty"
    return repr(argv[0])


@dataclass(frozen=True)
class _Launch:
    """What the ``mipyre`` launcher sets beyond ``run()``'s arguments: its
    ``--process-*`` flags, and ``mipyre dev``'s logging."""

    lanes: ProcessLanes | None = None
    log: bool = False


@dataclass(frozen=True)
class _ServeSettings:
    """Where and how one server runs, resolved once: explicit argument, else environment
    variable, else default."""

    host: str
    port: int
    mode: Mode
    workers: GivenSize | None
    io_workers: GivenSize | None
    tls_cert: str | None
    tls_key: str | None
    extra_tls_ports: list[int] | None
    extra_plain_ports: list[int] | None
    # The launcher's process-lane flags (`mipyre run --process-*`), resolved with the app's
    # own when its lanes start.
    lanes: ProcessLanes | None
    # Python logging on: `MIPYRE_LOG=1`, or `mipyre dev`.
    log: bool
    # The environment this server was resolved from: its process lanes resolve their
    # settings from the same one when they start.
    env: Mapping[str, str]

    @classmethod
    def resolve(
        cls,
        *,
        env: Mapping[str, str],
        launch: _Launch = _Launch(),
        host: str | None = None,
        port: int | None = None,
        workers: int | None = None,
        app_runtime: str = "subinterp",
        io_workers: int | None = None,
        tls_cert: str | None = None,
        tls_key: str | None = None,
        extra_tls_ports: list[int] | None = None,
        extra_plain_ports: list[int] | None = None,
    ) -> _ServeSettings:
        # The app's runtime decides how the server serves: "main" runs every handler on the
        # main interpreter (the GIL topologies); any other has sub-interpreters.
        mode = Mode.Gil if app_runtime == "main" else Mode.Subinterp
        if port is None:
            port = _env_int(env, "MIPYRE_PORT", "8000")
        # Read here; checked (at least 1) by the engine's start(), the one place a size is
        # checked, which names the argument.
        given_workers = _given_size(workers, env, "MIPYRE_WORKERS")
        given_io_workers = _given_size(io_workers, env, "MIPYRE_IO_WORKERS")
        tls_cert = tls_cert or env.get("MIPYRE_TLS_CERT")
        tls_key = tls_key or env.get("MIPYRE_TLS_KEY")
        if bool(tls_cert) != bool(tls_key):
            raise ValueError(
                f"tls_cert and tls_key must be provided together: "
                f"tls_cert={'set' if tls_cert else 'missing'}, "
                f"tls_key={'set' if tls_key else 'missing'}"
            )
        if extra_tls_ports is None:
            extra_tls_ports = _env_ports(env, "MIPYRE_TLS_PORTS")
        else:
            for p in extra_tls_ports:
                _check_port("extra_tls_ports", p)
        if extra_plain_ports is None:
            extra_plain_ports = _env_ports(env, "MIPYRE_PLAIN_PORTS")
        else:
            for p in extra_plain_ports:
                _check_port("extra_plain_ports", p)
        return cls(
            host=host or env.get("MIPYRE_HOST", "127.0.0.1"),
            port=port,
            mode=mode,
            workers=given_workers,
            io_workers=given_io_workers,
            tls_cert=tls_cert,
            tls_key=tls_key,
            extra_tls_ports=extra_tls_ports,
            extra_plain_ports=extra_plain_ports,
            lanes=launch.lanes,
            log=launch.log or env.get("MIPYRE_LOG") == "1",
            env=env,
        )


def _given_size(arg: int | None, env: Mapping[str, str], name: str) -> GivenSize | None:
    """A size as ``run()`` takes it: its argument, else the variable ``name`` of ``env``
    (the origin says which one it came from). Not checked here: ``start()`` refuses one
    below 1."""
    if arg is not None:
        return GivenSize(arg, SizeOrigin.RunArg)
    value = _env_int(env, name)
    return None if value is None else GivenSize(value, SizeOrigin.EnvVar)


@overload
def _env_int(env: Mapping[str, str], name: str, default: str) -> int: ...
@overload
def _env_int(env: Mapping[str, str], name: str, default: None = None) -> int | None: ...
def _env_int(env: Mapping[str, str], name: str, default: str | None = None) -> int | None:
    """An integer variable of ``env``; the error names the variable."""
    v = env.get(name, default)
    if v is None:
        return None
    try:
        return int(v)
    except ValueError:
        raise ValueError(
            f"environment variable {name}={v!r} is not a valid integer"
        ) from None


def _env_ports(env: Mapping[str, str], name: str) -> list[int] | None:
    """A comma-separated port list variable of ``env``; a bad entry is named."""
    raw = env.get(name)
    if not raw:
        return None
    ports = []
    for p in raw.split(","):
        p = p.strip()
        if not p:
            continue
        try:
            port = int(p)
        except ValueError:
            raise ValueError(f"{name} contains non-integer port {p!r}") from None
        ports.append(_check_port(name, port))
    return ports


def _check_port(source: str, port: int) -> int:
    """An extra listening port: 1-65535 (an ephemeral port 0 could not be reached)."""
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError(f"{source} contains {port!r}, which is not a port (1-65535)")
    return port


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


def _equivalent_command(app: "Mipyre", settings: "_ServeSettings") -> str:
    """The `mipyre run` command that starts what `app.run(...)` was asked to start."""
    main = sys.modules.get("__main__")
    target_file = getattr(main, "__file__", None) or app._app_file()
    attr = next((name for name, value in vars(main).items() if value is app), "app") if main else "app"
    try:
        shown = os.path.relpath(target_file)
    except ValueError:
        shown = target_file
    parts = ["mipyre", "run", f"{shown}:{attr}", "--host", settings.host, "--port", str(settings.port)]
    if settings.workers is not None:
        parts += ["--workers", str(settings.workers.value)]
    if settings.io_workers is not None:
        parts += ["--io-workers", str(settings.io_workers.value)]
    if settings.tls_cert:
        parts += ["--tls-cert", settings.tls_cert, "--tls-key", settings.tls_key or ""]
    import shlex

    return " ".join(shlex.quote(p) for p in parts)
