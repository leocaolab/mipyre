"""Where routes run, as the user declares it (docs/process-lanes-declared-design.md).

Where a route runs:

- ``"subinterp"`` — an own-GIL sub-interpreter per core in the server process (default):
  pure Python, and extensions that declare multi-interpreter support;
- ``"isolated"``  — the same sub-interpreters, with every extension that can't load in one
  given to each worker as a private copy (a compatibility option, with risks; design §14);
- ``"process"``   — a worker process (process lanes): C extensions such as numpy;
- ``"main"``      — the server's main interpreter (one GIL; few, stateful routes).

A route's runtime is, most specific first: its decorator's ``runtime=``, its
:class:`Router`'s, the app's ``Mipyre(runtime=...)``; when the app names none, the
deployment's default (``mipyre run --runtime``, ``MIPYRE_RUNTIME``, ``runtime`` in
``[tool.mipyre]`` of ``pyproject.toml``); else ``"subinterp"``. Deployment config never
rewrites what the code says: an app that names a runtime the deployment contradicts does
not start, and the error names both values and where each came from.
"""

from __future__ import annotations

import dataclasses
import enum
import importlib
import importlib.util
import os
import sys
from typing import Any, Callable, Mapping

RUNTIMES = ("subinterp", "isolated", "process", "main")
# The runtimes whose routes run on the server's sub-interpreters.
SUBINTERP_RUNTIMES = ("subinterp", "isolated")
DEFAULT_RUNTIME = "subinterp"


class RuntimeDeclarationError(ValueError):
    """A runtime declaration that can't be served: an unknown name, or code and deployment
    config that disagree."""


def check_runtime(value: object, where: str) -> str | None:
    return None if value is None else parse_runtime(value, where)


def parse_runtime(value: object, where: str) -> str:
    if value not in RUNTIMES:
        raise RuntimeDeclarationError(
            f"{where}: runtime={value!r} is not one of {', '.join(repr(r) for r in RUNTIMES)}"
        )
    return value


_REMOVED_ROUTE_ARGS = {"gil": "main", "process": "process"}


def reject_removed_route_args(where: str, removed: dict) -> None:
    """Raise for ``gil=`` / ``process=`` on a route, which ``runtime=`` replaced, quoting what
    was written and the new spelling; nothing is mapped silently."""
    for name, value in removed.items():
        if name in _REMOVED_ROUTE_ARGS:
            new = _REMOVED_ROUTE_ARGS[name]
            raise TypeError(
                f"{where}: {name}={value!r} was replaced by runtime={new!r}. "
                f"Write: runtime={new!r}" + ("" if value else ", or leave runtime unset")
            )
        raise TypeError(f"{where}: unexpected keyword argument {name!r}")


def reject_removed_serve_args(where: str, removed: dict) -> None:
    """Raise for ``mode=`` on ``run()`` / ``TestClient``, which the app's runtime replaced,
    quoting what was written and the new spelling."""
    for name, value in removed.items():
        if name == "mode":
            raise TypeError(
                f"{where}: mode={value!r} was removed. Where handlers run is the "
                f"app's runtime: Mipyre(runtime='main') serves every route on the main "
                f"interpreter (was mode='gil'); the default, 'subinterp', was mode='subinterp'. "
                f"A deployment sets the default with MIPYRE_RUNTIME."
            )
        raise TypeError(f"{where}: unexpected keyword argument {name!r}")


# -- deployment config ------------------------------------------------------------------
#
# Every resolver below takes what it reads as arguments: the environment as a ``Mapping``
# and ``[tool.mipyre]`` as a ``ConfigFile``. Reading them (``os.environ``, the file) is
# the caller's: ``Mipyre(...)`` and ``run()``.


@dataclasses.dataclass(frozen=True)
class ConfigFile:
    """``[tool.mipyre]`` of a ``pyproject.toml``, and the file it came from."""

    path: str
    table: Mapping[str, Any]

    @classmethod
    def load(cls, directory: str) -> "ConfigFile | None":
        """``[tool.mipyre]`` of ``directory``/pyproject.toml; None when there is no such
        file or no such table."""
        path = os.path.join(directory, "pyproject.toml")
        try:
            with open(path, "rb") as f:
                import tomllib

                table = tomllib.load(f).get("tool", {}).get("mipyre")
        except FileNotFoundError:
            return None
        if table is None:
            return None
        if not isinstance(table, dict):
            raise RuntimeDeclarationError(f"{path}: [tool.mipyre] must be a table, got {table!r}")
        return cls(path, table)

    def process(self) -> Mapping[str, Any]:
        """``[tool.mipyre.process]`` (empty when absent)."""
        process = self.table.get("process", {})
        if not isinstance(process, dict):
            raise ValueError(f"{self.path}: [tool.mipyre.process] must be a table, got {process!r}")
        return process


@dataclasses.dataclass(frozen=True)
class DeployedRuntime:
    """The deployment's default runtime, and the words that name where it came from."""

    runtime: str
    source: str


def deployment_runtime(env: Mapping[str, str], config: ConfigFile | None) -> DeployedRuntime | None:
    """The deployment's default runtime: ``MIPYRE_RUNTIME`` in ``env``, else ``runtime`` in
    ``config``; None when neither names one."""
    raw = env.get("MIPYRE_RUNTIME")
    if raw:
        source = env.get("MIPYRE_RUNTIME_SOURCE") or "the environment variable MIPYRE_RUNTIME"
        return DeployedRuntime(parse_runtime(raw, source), source)
    if config is not None and "runtime" in config.table:
        source = f"runtime in [tool.mipyre] of {config.path}"
        return DeployedRuntime(parse_runtime(config.table["runtime"], source), source)
    return None


def resolve_app_runtime(declared: str | None, where: str, deployed: DeployedRuntime | None) -> str:
    """The app's runtime: what the code declared, else the deployment's, else the default.
    Code and deployment disagreeing is an error, never a silent pick."""
    check_runtime(declared, where)
    if declared is None:
        return DEFAULT_RUNTIME if deployed is None else deployed.runtime
    if deployed is not None and deployed.runtime != declared:
        raise RuntimeDeclarationError(
            f"{where} declares runtime={declared!r}, but {deployed.source} says {deployed.runtime!r}. "
            f"Deployment config only fills in a runtime the code leaves unset; remove one "
            f"of the two."
        )
    return declared


# -- process-lane settings ----------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class ProcessLanes:
    """Settings of the worker processes ``runtime="process"`` routes run on. Each one left
    ``None`` comes from ``mipyre run --process-workers`` etc., else
    ``MIPYRE_PROCESS_WORKERS`` / ``_TIMEOUT_MS`` / ``_QUEUE`` / ``_SLOT_BYTES``, else
    ``[tool.mipyre.process]`` (``workers``, ``timeout``, ``queue_per_worker``,
    ``slot_bytes``), else the default: one worker per physical core, a 30 s handler
    timeout (the worker is killed, 504), 128 requests per worker before 503, 256 KiB per
    request and per answer.

    ``host_failure``: what a failed host or lane does (``MIPYRE_LANE_HOST_FAILURE``,
    ``host_failure`` in the table). ``"isolate"`` (default): the failed part is taken out of
    service, the other hosts keep serving, and the server ends only once no lane is left.
    ``"exit"``: the server ends at the first one, as ``panic_on_oops`` does. Either way the
    server ends with the first cause, and the process exits non-zero (systemd restarts it).

    ``start``: how a worker gets the app (``MIPYRE_PROCESS_START``, ``start`` in the table).
    ``"import"`` (default): it starts without the app and imports it itself — safe whatever
    the app's libraries do at import. ``"fork"``: a template imports the app, then forks each
    worker — faster to replace and less memory, for apps that replace workers often; the
    template must be single-threaded when it forks, or the start fails naming the threads."""

    start: str | None = None
    workers: int | None = None
    timeout: float | None = None
    queue_per_worker: int | None = None
    slot_bytes: int | None = None


class _Source(enum.Enum):
    """Where a process-lane setting came from, first to last."""

    CODE = enum.auto()
    FLAGS = enum.auto()
    ENV = enum.auto()
    FILE = enum.auto()
    DEFAULT = enum.auto()


@dataclasses.dataclass(frozen=True)
class _Given:
    """One process-lane setting as it was written (``raw``, before any conversion), where it
    came from, and the words that name that place in an error."""

    raw: Any
    source: _Source
    where: str


def _positive_int(name: str, given: _Given) -> int:
    try:
        value = int(given.raw)
    except (TypeError, ValueError):
        raise ValueError(
            f"process lanes: {name} must be an integer, got {given.raw!r} ({given.where})"
        ) from None
    if value < 1:
        raise ValueError(f"process lanes: {name} must be >= 1, got {given.raw!r} ({given.where})")
    return value


def _timeout_ms(given: _Given) -> int:
    """The handler timeout in milliseconds: ``MIPYRE_PROCESS_TIMEOUT_MS`` is written in
    milliseconds, every other place in seconds. An error quotes what was written, in its own
    unit, not the converted value."""
    in_ms = given.source is _Source.ENV
    try:
        ms = int(given.raw) if in_ms else int(float(given.raw) * 1000)
    except (TypeError, ValueError, OverflowError):
        unit = "a whole number of milliseconds" if in_ms else "a number of seconds"
        raise ValueError(f"process lanes: timeout must be {unit}, got {given.raw!r} ({given.where})") from None
    if ms < 1:
        raise ValueError(f"process lanes: timeout must be at least 1 ms, got {given.raw!r} ({given.where})")
    return ms


@dataclasses.dataclass(frozen=True)
class LaneSettings:
    """`ProcessLanes` resolved: every value, with where it came from left to the error."""

    start: Any  # mipyre.engine.StartStrategy
    workers: int
    timeout_ms: int
    queue_per_worker: int
    slot_bytes: int
    worker_spin_us: int
    host_failure: str  # "isolate" or "exit"

    @classmethod
    def resolve(
        cls,
        declared: ProcessLanes | None,
        physical_cores: int,
        *,
        launched: ProcessLanes | None = None,
        env: Mapping[str, str],
        config: ConfigFile | None,
    ) -> "LaneSettings":
        """Each setting from, first to last: the code (``declared``), the launcher's flags
        (``launched``: ``mipyre run --process-*``), ``env``, ``config``, the default."""
        code = declared or ProcessLanes()
        flags = launched or ProcessLanes()
        table = config.process() if config is not None else {}

        def pick(field: str, var: str, flag: str, default: Any) -> _Given:
            if getattr(code, field) is not None:
                return _Given(getattr(code, field), _Source.CODE, f"Mipyre(process=ProcessLanes({field}=...))")
            if getattr(flags, field) is not None:
                return _Given(getattr(flags, field), _Source.FLAGS, f"mipyre run {flag}")
            if env.get(var):
                return _Given(env[var], _Source.ENV, f"the environment variable {var}")
            if config is not None and field in table:
                return _Given(table[field], _Source.FILE, f"{field} in [tool.mipyre.process] of {config.path}")
            return _Given(default, _Source.DEFAULT, "the default")

        from mipyre.engine import StartStrategy

        host_failure = env.get("MIPYRE_LANE_HOST_FAILURE") or table.get("host_failure", "isolate")
        if host_failure not in ("isolate", "exit"):
            raise ValueError(
                f"process lanes: host_failure must be 'isolate' or 'exit', got {host_failure!r} "
                "(MIPYRE_LANE_HOST_FAILURE / host_failure in [tool.mipyre.process])"
            )
        start_given = pick("start", "MIPYRE_PROCESS_START", "--process-start", "import")
        try:
            start = StartStrategy.parse(str(start_given.raw))
        except ValueError as e:
            raise ValueError(f"process lanes: {e} ({start_given.where})") from None
        spin = env.get("MIPYRE_PROCESS_SPIN_US", "50")
        try:
            worker_spin_us = int(spin)
        except ValueError:
            raise ValueError(
                f"process lanes: the environment variable MIPYRE_PROCESS_SPIN_US must be an integer "
                f"(microseconds), got {spin!r}"
            ) from None
        return cls(
            start=start,
            workers=_positive_int(
                "workers", pick("workers", "MIPYRE_PROCESS_WORKERS", "--process-workers", physical_cores)
            ),
            timeout_ms=_timeout_ms(pick("timeout", "MIPYRE_PROCESS_TIMEOUT_MS", "--process-timeout", 30)),
            queue_per_worker=_positive_int(
                "queue_per_worker", pick("queue_per_worker", "MIPYRE_PROCESS_QUEUE", "--process-queue", 128)
            ),
            slot_bytes=_positive_int(
                "slot_bytes", pick("slot_bytes", "MIPYRE_PROCESS_SLOT_BYTES", "--process-slot-bytes", 256 * 1024)
            ),
            worker_spin_us=worker_spin_us,
            host_failure=host_failure,
        )


# -- routers -------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class _RouterRoute:
    method: str
    path: str
    handler: Callable
    runtime: str | None
    model: type | None
    query: type | None
    stream: bool


class Router:
    """A group of routes, usually one module, with a runtime of its own::

        # myapp/analytics.py
        import polars as pl
        router = Router(runtime="process")

        @router.get("/report")
        def report(req): ...

    The app lists routers by import string (``Mipyre(routers=["myapp.analytics:router"])``),
    never by object: each runtime imports only the routers that have routes it serves, so
    polars above is imported by the process workers (and the server), never by a
    sub-interpreter."""

    def __init__(self, runtime: str | None = None) -> None:
        self.runtime = check_runtime(runtime, "Router(...)")
        self._routes: list[_RouterRoute] = []

    def _add(self, method: str, path: str, handler: Callable | None, runtime: str | None, model: type | None, query: type | None, stream: bool):
        check_runtime(runtime, f"{method} {path}")

        def register(fn: Callable) -> Callable:
            self._routes.append(_RouterRoute(method, path, fn, runtime, model, query, stream))
            return fn

        return register(handler) if handler is not None else register

    def get(self, path: str, handler: Callable | None = None, *, runtime: str | None = None, model: type | None = None, query: type | None = None):
        return self._add("GET", path, handler, runtime, model, query, False)

    def post(self, path: str, handler: Callable | None = None, *, runtime: str | None = None, model: type | None = None, query: type | None = None, stream: bool = False):
        return self._add("POST", path, handler, runtime, model, query, stream)

    def put(self, path: str, handler: Callable | None = None, *, runtime: str | None = None, model: type | None = None, query: type | None = None, stream: bool = False):
        return self._add("PUT", path, handler, runtime, model, query, stream)

    def delete(self, path: str, handler: Callable | None = None, *, runtime: str | None = None, model: type | None = None, query: type | None = None):
        return self._add("DELETE", path, handler, runtime, model, query, False)

    def patch(self, path: str, handler: Callable | None = None, *, runtime: str | None = None, model: type | None = None, query: type | None = None, stream: bool = False):
        return self._add("PATCH", path, handler, runtime, model, query, stream)

    def options(self, path: str, handler: Callable | None = None, *, runtime: str | None = None, model: type | None = None, query: type | None = None):
        return self._add("OPTIONS", path, handler, runtime, model, query, False)

    def head(self, path: str, handler: Callable | None = None, *, runtime: str | None = None, model: type | None = None, query: type | None = None):
        return self._add("HEAD", path, handler, runtime, model, query, False)

    def route(self, method: str, path: str, handler: Callable | None = None, *, runtime: str | None = None, model: type | None = None, query: type | None = None, stream: bool = False):
        return self._add(method.upper(), path, handler, runtime, model, query, stream)


# -- loading the app ----------------------------------------------------------------------


def split_target(target: str) -> tuple[str, str]:
    """``module[:attr]`` or ``path/to/file.py[:attr]`` → (module or path, attr)."""
    if target.count(":") > 1:
        raise ValueError(f"invalid target {target!r}: expected module:attr (exactly one ':')")
    module, _, attr = target.partition(":")
    if not module:
        raise ValueError(f"invalid target {target!r}: module name must not be empty")
    return module, attr or "app"


def import_target(target: str):
    """The module ``target`` names, imported (a ``.py`` path is loaded under its stem's
    name), and the object its attribute names."""
    module_name, attr = split_target(target)
    if module_name.endswith(".py") or os.sep in module_name:
        path = os.path.abspath(module_name)
        name = os.path.splitext(os.path.basename(path))[0]
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        directory = os.path.dirname(path)
        if directory not in sys.path:
            sys.path.insert(0, directory)
        spec.loader.exec_module(module)
    else:
        module = importlib.import_module(module_name)
    try:
        return module, getattr(module, attr)
    except AttributeError:
        raise AttributeError(f"module {module_name!r} has no attribute {attr!r}") from None


def import_router(target: str) -> Router:
    module_name, attr = split_target(target)
    module = importlib.import_module(module_name)
    try:
        router = getattr(module, attr)
    except AttributeError:
        raise AttributeError(f"router {target!r}: module {module_name!r} has no attribute {attr!r}") from None
    if not isinstance(router, Router):
        raise TypeError(f"router {target!r} is a {type(router).__name__}, expected mipyre.Router")
    return router
