"""Mipyre — A high-performance Python web framework powered by Rust."""

from collections.abc import Mapping as _Mapping

from mipyre.engine import Request, Response, Headers, WebSocket, SharedState, Stream, BodyStream, Metrics, get_gil_metrics, reset_peaks, flush_logs, LogLevel, Compression, HTTPException
from mipyre.engine import BodyReadError, DeclarationError, LateRegistrationError, MissingAppFileError
from mipyre.app import Mipyre
from mipyre._routes import FastRoute, Route, Router, Runtime, RuntimeDeclarationError
from mipyre._process_lane.settings import ProcessLanes
from mipyre.cookies import Cookie, SameSite, get_cookies, get_cookie
from mipyre.uploads import parse_multipart, MultipartError, Form, FormPart
from mipyre.cache import cached_json


def redirect(url: str, status_code: int = 302) -> Response:
    """Return a redirect response.

    Usage::

        @app.get("/old")
        def old_page(req):
            return redirect("/new")

        @app.get("/permanent")
        def moved(req):
            return redirect("/new-home", status_code=301)
    """
    # A redirect that isn't a 3xx is a contract violation: the browser
    # won't follow the Location header. Reject non-redirect codes up front
    # rather than emitting a response that silently does nothing.
    if status_code not in (301, 302, 303, 307, 308):
        raise ValueError(
            f"invalid redirect status code {status_code}; "
            f"must be one of 301, 302, 303, 307, 308"
        )
    # Reject CR/LF/NUL: HTTP response splitting. Open-redirect (scheme/host
    # allow-list) is left to the caller; we only block header injection.
    for _ch in ("\r", "\n", "\0"):
        if _ch in url:
            raise ValueError(
                f"redirect url contains forbidden control character "
                f"{_ch!r}; refusing to emit (HTTP response splitting risk)"
            )
    return Response(
        body="",
        status_code=status_code,
        headers={"location": url},
    )


_Mapping.register(Headers)

__all__ = [
    "Router", "Runtime", "ProcessLanes",
    "Mipyre", "Route", "FastRoute", "Request", "Response", "Headers", "WebSocket", "SharedState", "Stream", "BodyStream",
    "Metrics", "get_gil_metrics", "reset_peaks", "flush_logs",
    "LogLevel", "Compression",
    "redirect", "rpc", "RPCClient", "HealthProbes", "OpenApi",
    "HTTPException", "DeclarationError", "LateRegistrationError", "MissingAppFileError",
    "RuntimeDeclarationError", "QueryDeclarationError", "BodyReadError",
    "RPCError", "RPCRequestError", "RPCServerError", "RPCReplyError",
    "Cookie", "SameSite", "get_cookies", "get_cookie",
    "parse_multipart", "MultipartError", "Form", "FormPart",
    "cached_json",
    "AbandonedWorkersError", "ThreadedTemplateError",
]


#: What only some programs use, by the module it is imported from on first use: RPC, `@rpc(app)` and
#: its client (with msgpack), the `query=` binder, `HealthProbes`, `OpenApi`, a server run's `AbandonedWorkersError`
#: (only the main interpreter runs a server) and the threaded template's `ThreadedTemplateError`.
_ON_FIRST_USE = {
    "rpc": "mipyre._rpc", "RPCClient": "mipyre._rpc", "RPCError": "mipyre._rpc", "RPCRequestError": "mipyre._rpc",
    "RPCServerError": "mipyre._rpc", "RPCReplyError": "mipyre._rpc",
    "QueryDeclarationError": "mipyre._query",
    "HealthProbes": "mipyre._health",
    "OpenApi": "mipyre._openapi",
    "AbandonedWorkersError": "mipyre._server_run",
    "ThreadedTemplateError": "mipyre._process_lane.threads",
}


def __getattr__(name):
    # Every sub-interpreter worker imports this package, so what only some programs use is
    # imported on first use: `_ON_FIRST_USE`, and
    # `__version__`, read from the installed distribution through `importlib.metadata`,
    # which pulls in zipfile, email, pathlib and more (about 1.3 MiB per interpreter).
    home = _ON_FIRST_USE.get(name)
    if home is not None:
        import importlib

        value = getattr(importlib.import_module(home), name)
        globals()[name] = value
        return value
    if name != "__version__":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib.metadata import PackageNotFoundError, version

    global __version__
    try:
        __version__ = version("mipyre")
    except PackageNotFoundError:  # a source checkout that was never installed
        __version__ = "dev"
    return __version__
