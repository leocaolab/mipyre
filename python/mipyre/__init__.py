"""Mipyre — A high-performance Python web framework powered by Rust."""

from collections.abc import Mapping as _Mapping

from mipyre.engine import Request, Response, Headers, WebSocket, SharedState, Stream, Metrics, get_gil_metrics, reset_peaks, flush_logs, LogLevel, Compression
from mipyre.app import Mipyre, RouteInfo, FastRouteInfo
from mipyre._runtime import Router, ProcessLanes
from mipyre.rpc import RPCClient
from mipyre.cookies import SameSite, get_cookies, get_cookie, set_cookie, delete_cookie
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
    # Reject CR/LF/NUL — the same HTTP Response Splitting class of attack
    # the cookie helpers defend against. Open-redirect (scheme/host
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
    "Router", "ProcessLanes",
    "Mipyre", "RouteInfo", "FastRouteInfo", "Request", "Response", "Headers", "WebSocket", "SharedState", "Stream",
    "Metrics", "get_gil_metrics", "reset_peaks", "flush_logs",
    "LogLevel", "Compression",
    "redirect", "RPCClient",
    "SameSite", "get_cookies", "get_cookie", "set_cookie", "delete_cookie",
    "parse_multipart", "MultipartError", "Form", "FormPart",
    "cached_json",
]


def __getattr__(name):
    # `__version__` is read from the installed distribution on first use, not at
    # import: `importlib.metadata` pulls in zipfile, email, pathlib and more (about
    # 1.3 MiB per interpreter), and every sub-interpreter worker imports this package.
    if name != "__version__":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib.metadata import PackageNotFoundError, version

    global __version__
    try:
        __version__ = version("mipyre")
    except PackageNotFoundError:  # a source checkout that was never installed
        __version__ = "dev"
    return __version__
