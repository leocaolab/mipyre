"""The route vocabulary: the runtimes a route runs on, a route as the code declares it and as
the app serves it, and the declaration methods :class:`Router` and ``Mipyre`` share.

A route's runtime is, most specific first: its decorator's ``runtime=``, its
:class:`~mipyre.Router`'s, the app's ``Mipyre(runtime=...)``, else ``"subinterp"``. It is the
code's alone (b/335 D9): an app that wants a deployment to choose it reads its own flag and
passes the value in, ``Mipyre(runtime=MY_RUNTIME.value)``.
"""

from __future__ import annotations

import enum
import functools
import importlib
import sys
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Callable

from mipyre._target import split_target
from mipyre.engine import BodyStream, DeclarationError, RuntimeChoice

if TYPE_CHECKING:
    from mipyre._worker.extension_support import ExtensionFacts


class RuntimeDeclarationError(DeclarationError):
    """A runtime declaration that can't be served: an unknown name, or code and deployment
    config that disagree."""


class Runtime(enum.StrEnum):
    """Where a route's handler runs. A ``runtime=`` argument takes a member or its value.

    - ``SUBINTERP`` — an own-GIL sub-interpreter per core in the server process (the
      default): pure Python, and extensions that declare multi-interpreter support;
    - ``ISOLATED`` — the same sub-interpreters, with every extension that can't load in one
      given to each worker as a private copy (a compatibility option, with risks);
    - ``PROCESS`` — a worker process (process lanes): C extensions such as numpy;
    - ``MAIN`` — the server's main interpreter (one GIL; few, stateful routes).
    """

    SUBINTERP = "subinterp"
    ISOLATED = "isolated"
    PROCESS = "process"
    MAIN = "main"

    @property
    def on_subinterpreters(self) -> bool:
        """Whether the route runs on the server's sub-interpreters."""
        return self in (Runtime.SUBINTERP, Runtime.ISOLATED)

    @classmethod
    def parse(cls, value: object, where: str) -> Runtime:
        """Return the runtime ``value`` names; anything else raises
        :class:`RuntimeDeclarationError` naming ``where`` and every runtime."""
        if isinstance(value, str) and value in cls._value2member_map_:
            return cls(value)
        raise RuntimeDeclarationError(
            f"{where}: runtime={value!r} is not one of {', '.join(repr(r.value) for r in cls)}"
        )


# The engine's executor of each runtime: both sub-interpreter runtimes run on its workers.
_ENGINE_RUNTIME = {
    Runtime.SUBINTERP: RuntimeChoice.Worker,
    Runtime.ISOLATED: RuntimeChoice.Worker,
    Runtime.PROCESS: RuntimeChoice.Lane,
    Runtime.MAIN: RuntimeChoice.Main,
}


def app_runtime(written: Runtime | str | None, where: str) -> Runtime:
    """The app's runtime: what its code declares, else ``SUBINTERP``. It is the code's: no
    deployment value replaces it (b/335 D9); code that wants it set from outside reads its own
    flag and passes it in."""
    return Runtime.SUBINTERP if written is None else Runtime.parse(written, where)


_REMOVED_RUNTIME_ARGS = {"gil": Runtime.MAIN, "process": Runtime.PROCESS}


def _replacement(name: str, value: object) -> tuple[str, str] | None:
    """The argument that replaced the removed route argument ``name=value``, and how to write
    it; None when ``name`` was never a route argument."""
    if name in _REMOVED_RUNTIME_ARGS:
        new = f"runtime={_REMOVED_RUNTIME_ARGS[name].value!r}"
        return new, new + ("" if value else ", or leave runtime unset")
    if name == "model":
        return "body=", f"body={getattr(value, '__name__', repr(value))}"
    if name == "stream":
        return "body=", "body=BodyStream (from mipyre import BodyStream)" if value else "leave body unset"
    return None


def reject_removed_route_args(where: str, removed: dict) -> None:
    """Raise for an argument a route no longer takes — ``gil=`` / ``process=``, which
    ``runtime=`` replaced, and ``model=`` / ``stream=``, which ``body=`` replaced — quoting
    what was written and the new spelling; nothing is mapped silently."""
    for name, value in removed.items():
        replaced = _replacement(name, value)
        if replaced is None:
            raise TypeError(f"{where}: unexpected keyword argument {name!r}")
        by, write = replaced
        raise TypeError(f"{where}: {name}={value!r} was replaced by {by}. Write: {write}")


def reject_removed_serve_args(where: str, removed: dict) -> None:
    """Raise for ``mode=`` on ``run()`` / ``TestClient``, which the app's runtime replaced,
    quoting what was written and the new spelling."""
    for name, value in removed.items():
        if name == "mode":
            raise TypeError(
                f"{where}: mode={value!r} was removed. Where handlers run is the "
                f"app's runtime: Mipyre(runtime='main') serves every route on the main "
                f"interpreter (was mode='gil'); the default, 'subinterp', was mode='subinterp'."
            )
        raise TypeError(f"{where}: unexpected keyword argument {name!r}")


@dataclass(frozen=True)
class RouteDeclaration:
    """A route as the code declared it; ``runtime`` is ``None`` when the code left it to the
    router or the app."""

    method: str
    path: str
    handler: Callable
    runtime: Runtime | None
    body: type | None
    query: type | None
    in_openapi: bool


def _body_model(declaration: RouteDeclaration) -> type | None:
    """The model ``body=`` validates the whole body with; None for a streamed or a plain body."""
    return None if declaration.body is BodyStream else declaration.body


def _validates(declaration: RouteDeclaration) -> bool:
    return _body_model(declaration) is not None or declaration.query is not None


@functools.cache
def _pydantic_core_facts() -> ExtensionFacts:
    """What the pydantic_core extension loaded in this process is, read from the module already
    loaded (a body= or query= model loaded it); once per process."""
    from mipyre._worker import extension_support

    core = sys.modules["pydantic_core._pydantic_core"]
    if core.__file__ is None:
        return extension_support.UnknownFormat(core.__name__, "it was not loaded from a file")
    return extension_support.facts_of_loaded(core.__file__)


def _pydantic_refusal(declaration: RouteDeclaration, facts: ExtensionFacts) -> RuntimeDeclarationError | None:
    """The error for a ``runtime="subinterp"`` route that validates with pydantic when the
    pydantic_core extension ``facts`` describe can't load in an own-GIL sub-interpreter; None
    when it can."""
    from mipyre._worker.extension_support import Supported, per_interpreter_support

    support = per_interpreter_support(facts)
    if isinstance(support, Supported):
        return None
    given = [f"{name}=" for name, cls in (("body", _body_model(declaration)), ("query", declaration.query)) if cls]
    verb = "validates" if len(given) == 1 else "validate"
    return RuntimeDeclarationError(
        f"{declaration.method} {declaration.path}: {' and '.join(given)} {verb} with pydantic, whose core can't load "
        f"in a sub-interpreter ({support}), so this runtime=\"subinterp\" route could never be served. "
        "It needs one of:\n"
        "  1. a pydantic-core that declares per-interpreter GIL support (built from source against "
        "mipyre's PyO3 fork), installed in place of PyPI's;\n"
        "  2. runtime=\"main\" or runtime=\"process\": pydantic loads there as anywhere else;\n"
        "  3. runtime=\"isolated\": each sub-interpreter loads its own copy of pydantic (a "
        "compatibility option, with risks; see the runtimes guide)."
    )


@dataclass(frozen=True)
class Route:
    """A route the app serves, as ``app.routes`` lists it. ``handler`` is the function as it was
    declared, before the body, query and path parameters are bound to it. ``body`` is what the
    route declared with ``body=``: a pydantic model, :class:`BodyStream`, or ``None``."""

    method: str
    path: str
    handler: Callable
    runtime: Runtime
    is_async: bool
    body: type | None
    query: type | None
    in_openapi: bool


@dataclass(frozen=True)
class FastRoute:
    """A route registered with ``add_fast_response``, as ``app.fast_routes`` lists it."""

    method: str
    path: str
    status_code: int
    content_type: str
    body_bytes: int


class _RouteVerbs:
    """The route declaration methods. Each checks what the declaration alone decides, then
    hands the :class:`RouteDeclaration` to ``_add``, whose result the decorator returns."""

    def _add(self, declaration: RouteDeclaration) -> Callable:
        raise NotImplementedError

    def _declare(
        self,
        method: str,
        path: str,
        handler: Callable | None,
        *,
        body: type | None,
        query: type | None,
        runtime: Runtime | str | None,
        in_openapi: bool,
        streams: bool,
        removed: dict,
    ) -> Callable:
        where = f"{method} {path}"
        reject_removed_route_args(where, removed)
        declared = None if runtime is None else Runtime.parse(runtime, where)
        if body is BodyStream and not streams:
            raise TypeError(
                f"{where}: body=BodyStream is for a method that carries a body: post, put, patch "
                "or route"
            )

        def register(fn: Callable) -> Callable:
            return self._add(RouteDeclaration(method, path, fn, declared, body, query, in_openapi))

        return register(handler) if handler is not None else register

    def get(self, path: str, handler: Callable | None = None, *, body: type | None = None, query: type | None = None, runtime: Runtime | str | None = None, in_openapi: bool = True, **removed: object) -> Callable:
        return self._declare("GET", path, handler, body=body, query=query, runtime=runtime, in_openapi=in_openapi, streams=False, removed=removed)

    def post(self, path: str, handler: Callable | None = None, *, body: type | None = None, query: type | None = None, runtime: Runtime | str | None = None, in_openapi: bool = True, **removed: object) -> Callable:
        return self._declare("POST", path, handler, body=body, query=query, runtime=runtime, in_openapi=in_openapi, streams=True, removed=removed)

    def put(self, path: str, handler: Callable | None = None, *, body: type | None = None, query: type | None = None, runtime: Runtime | str | None = None, in_openapi: bool = True, **removed: object) -> Callable:
        return self._declare("PUT", path, handler, body=body, query=query, runtime=runtime, in_openapi=in_openapi, streams=True, removed=removed)

    def delete(self, path: str, handler: Callable | None = None, *, body: type | None = None, query: type | None = None, runtime: Runtime | str | None = None, in_openapi: bool = True, **removed: object) -> Callable:
        return self._declare("DELETE", path, handler, body=body, query=query, runtime=runtime, in_openapi=in_openapi, streams=False, removed=removed)

    def patch(self, path: str, handler: Callable | None = None, *, body: type | None = None, query: type | None = None, runtime: Runtime | str | None = None, in_openapi: bool = True, **removed: object) -> Callable:
        return self._declare("PATCH", path, handler, body=body, query=query, runtime=runtime, in_openapi=in_openapi, streams=True, removed=removed)

    def options(self, path: str, handler: Callable | None = None, *, body: type | None = None, query: type | None = None, runtime: Runtime | str | None = None, in_openapi: bool = True, **removed: object) -> Callable:
        return self._declare("OPTIONS", path, handler, body=body, query=query, runtime=runtime, in_openapi=in_openapi, streams=False, removed=removed)

    def head(self, path: str, handler: Callable | None = None, *, body: type | None = None, query: type | None = None, runtime: Runtime | str | None = None, in_openapi: bool = True, **removed: object) -> Callable:
        return self._declare("HEAD", path, handler, body=body, query=query, runtime=runtime, in_openapi=in_openapi, streams=False, removed=removed)

    def route(self, method: str, path: str, handler: Callable | None = None, *, body: type | None = None, query: type | None = None, runtime: Runtime | str | None = None, in_openapi: bool = True, **removed: object) -> Callable:
        return self._declare(method.upper(), path, handler, body=body, query=query, runtime=runtime, in_openapi=in_openapi, streams=True, removed=removed)


class Router(_RouteVerbs):
    """A group of routes, usually one module, with a runtime of its own::

        # myapp/analytics.py
        import polars as pl
        router = Router(runtime="process")

        @router.get("/report")
        def report(req): ...

    The app lists routers by import string (``Mipyre(routers=["myapp.analytics:router"])``),
    never by object: each runtime imports only the routers that have routes it serves, so
    polars above is imported by the process workers (and the server), never by a
    sub-interpreter. A route neither it nor the router names a runtime for gets the app's."""

    def __init__(self, runtime: Runtime | str | None = None) -> None:
        self.runtime = None if runtime is None else Runtime.parse(runtime, "Router(...)")
        self._routes: list[RouteDeclaration] = []

    def _add(self, declaration: RouteDeclaration) -> Callable:
        if declaration.runtime is None:
            declaration = replace(declaration, runtime=self.runtime)
        self._routes.append(declaration)
        return declaration.handler


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
