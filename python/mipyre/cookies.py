"""Cookies: read the request's, send new ones on a response.

Usage::

    from mipyre import Cookie, Response
    from mipyre.cookies import get_cookies

    @app.get("/")
    def index(req):
        session = get_cookies(req).get("session_id", "none")
        return Response(
            body=f"session={session}",
            headers={"set-cookie": Cookie("session_id", "abc123", max_age=3600, httponly=True)},
        )

    @app.post("/logout")
    def logout(req):
        return Response(body="bye", headers={"set-cookie": Cookie.expired("session_id", httponly=True)})

``Cookie`` checks its name, value and attributes when it is made (RFC 6265), so a
response never carries a malformed ``Set-Cookie`` line.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from mipyre.engine import Cookie, SameSite

if TYPE_CHECKING:
    from mipyre.engine import Request

__all__ = ["Cookie", "SameSite", "get_cookies", "get_cookie"]


def get_cookies(req: Request) -> dict[str, str]:
    """Parse cookies from request headers.

    Returns a dict of cookie name → value.
    """
    cookie_header = req.headers.get("cookie", "")
    if not cookie_header:
        return {}
    cookies = {}
    for pair in cookie_header.split(";"):
        pair = pair.strip()
        if "=" in pair:
            name, _, value = pair.partition("=")
            name = name.strip()
            value = value.strip()
            # RFC 6265 allows a DQUOTE-wrapped value: unwrap a matched pair only. A lone
            # `"` is not a quoted-string and is dropped, so it can't be re-emitted.
            if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
                value = value[1:-1]
            if value == '"':
                value = ""
            # RFC 6265 cookie-names are non-empty: `Cookie: =value` is dropped.
            if not name:
                continue
            # First occurrence wins: a duplicate appended after the browser's own
            # (`session=evil`) is ignored.
            if name not in cookies:
                cookies[name] = value
    return cookies


def get_cookie(req: Request, name: str, default: str | None = None) -> str | None:
    """Get a single cookie value by name."""
    return get_cookies(req).get(name, default)
