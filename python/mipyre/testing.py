"""TestClient — exercise a Mipyre app without manually managing a server.

Starts the app in a background thread, in the mode ``app.run()`` uses by
default. By default (``transport="memory"``) the server binds no socket: each
request is a connection made in this process, handed to the server's accept loop
and served from there exactly as a TCP connection is (hyper, routing, sub-interp
dispatch, CORS, compression, hooks). ``transport="tcp"`` serves on a real TCP
socket instead, for what only a socket has (see below). Define the app at module
level: sub-interpreter workers rebuild it by executing that module. Pure stdlib
for HTTP; WebSocket support requires the ``websockets`` package.

A memory connection is one end of a Unix socket pair whose other end the
server's accept loop takes: a real socket, so ``http.client`` and ``websockets``
drive it as any socket (timeouts, half-close, reading and writing at once are
the kernel's). What it does not have, it does not fake:

- no client address: ``req.client_ip`` is ``None``;
- no port: ``client.port`` raises ``AttributeError``; ``client.base_url`` is
  ``http://testserver`` and requests carry ``Host: testserver``;
- no TLS and no TCP options; its buffers are a Unix socket's, not a TCP
  socket's, so how many unread bytes stall a writer differs.

Timeouts are the server's own either way (header read, write stall), and the
client's ``timeout`` raises ``TimeoutError`` in both transports.

Basics::

    from mipyre.testing import TestClient

    with TestClient(app) as c:
        r = c.get("/users", params={"limit": 10})
        assert r.ok
        assert r.json()["count"] == 10

Persistent cookies::

    with TestClient(app) as c:
        c.post("/login", body={"user": "alice"})
        # Cookies set by /login are resent automatically on later calls.
        r = c.get("/me")

WebSocket::

    with TestClient(app) as c, c.websocket_connect("/ws") as ws:
        ws.send("ping")
        assert ws.recv() == "pong"
"""

from __future__ import annotations

import http.client
import json
import logging as _logging
import os
import socket
import threading
import urllib.parse
import urllib.request
import urllib.error
from collections import defaultdict
from dataclasses import dataclass, field
from http.cookiejar import CookieJar
from typing import Any, Iterator, NoReturn

from mipyre.app import Mipyre, Transport, WorkersAbandoned, _ServeSettings

__all__ = ["TestClient", "TestResponse", "Transport", "WebSocketSession"]

_logger = _logging.getLogger("mipyre.testing")

# How long the server may take to answer its first request; building the
# sub-interpreter workers (each executes the app's module) dominates it.
_READY_TIMEOUT_S = 30
# How long close() waits for the server to drain and run its shutdown hooks.
_STOP_TIMEOUT_S = 60
# The host name a memory-transport client sends: the name it used, not an address.
_MEMORY_HOST = "testserver"


def _collapse_headers(msg) -> "dict[str, str | list[str]]":
    """Build a headers dict that preserves multi-valued entries (e.g. Set-Cookie).
    Single-valued headers remain plain strings; repeated headers become lists."""
    raw: dict[str, list[str]] = defaultdict(list)
    for k, v in msg.items():
        raw[k.lower()].append(v)
    return {k: (vs[0] if len(vs) == 1 else vs) for k, vs in raw.items()}


@dataclass
class TestResponse:
    """Response from TestClient.

    Attributes:
        status_code: HTTP status code.
        body: raw response body bytes.
        headers: response headers keyed by lower-case name; a header sent more than
            once (e.g. Set-Cookie) is a list of its values, any other a plain string.
            ``get_header_list()`` always gives a list.
    """

    status_code: int
    body: bytes
    headers: dict[str, "str | list[str]"] = field(default_factory=dict)

    def get_header_list(self, name: str) -> list[str]:
        """Return all values for a header as a list (always a list, even for single values)."""
        v = self.headers.get(name.lower())
        if v is None:
            return []
        return v if isinstance(v, list) else [v]

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

    @property
    def ok(self) -> bool:
        """True when the response is a success (2xx/3xx)."""
        return self.status_code < 400

    def json(self, **loads_kwargs) -> Any:
        """Decode the body as JSON. Passes kwargs to ``json.loads``.

        Raises ``json.JSONDecodeError`` (a ``ValueError`` subclass) when the
        body is not valid JSON — same contract as ``requests``/``httpx``
        ``.json()``. Catch ``ValueError`` if the endpoint may return non-JSON.
        """
        return json.loads(self.body, **loads_kwargs)

    def raise_for_status(self) -> None:
        """Raise ``RuntimeError`` if the status code is 4xx or 5xx."""
        if self.status_code >= 400:
            snippet = self.text[:200] if self.body else ""
            raise RuntimeError(
                f"TestClient HTTP {self.status_code}: {snippet!r}"
            )


class TestClient:
    """Test client — serves the app in a background thread, the way ``app.run()`` does.

    The server serves as ``app.run()`` does, so routes run where they are declared
    (sub-interpreter routes in sub-interpreter workers). Workers rebuild the app by
    executing the module that created it, so define the app at module level (as in
    production). An app created inside a function can only be served with
    ``Mipyre(runtime="main")``.

    Args:
        app: Mipyre app instance.
        transport: ``Transport.MEMORY`` / ``"memory"`` (default): no socket, each
              request a connection made in this process (see the module docstring for
              what it lacks). ``Transport.TCP`` / ``"tcp"``: a real TCP listener, for
              tests of what only a socket has (the client address, the port, raw
              socket clients).
        host: bind address with ``transport="tcp"`` (default ``127.0.0.1``).
        port: bind port with ``transport="tcp"``. ``None`` has the server bind one the
              kernel picks; the ``port`` attribute is the one it bound. ``host`` and
              ``port`` with the memory transport are a ``ValueError``.
        timeout: default per-request timeout in seconds (default 10).
        follow_redirects: whether to follow 3xx redirects (default True,
              matching urllib's historical behavior).

    ``close()`` (or leaving the ``with`` block) stops the server: shutdown hooks
    run and the port, if any, is released.
    """

    # Tell pytest this is not a test class (silences the collection
    # warning when the client lands in a test module namespace).
    __test__ = False

    def __init__(
        self,
        app: Mipyre,
        host: str | None = None,
        port: int | None = None,
        *,
        transport: Transport | str = Transport.MEMORY,
        timeout: float = 10.0,
        follow_redirects: bool = True,
        **removed: object,
    ):
        from mipyre import _runtime

        _runtime.reject_removed_serve_args("TestClient(...)", removed)
        self.transport = Transport(transport)
        if self.transport is Transport.MEMORY and (host is not None or port is not None):
            raise ValueError(
                "TestClient(transport='memory') listens on no socket, so host/port cannot "
                "be given; pass transport='tcp' for a real TCP listener"
            )
        self.host = "127.0.0.1" if host is None else host
        self.timeout = timeout
        self.follow_redirects = follow_redirects

        # Cookie jar — cookies set by the server persist across requests
        # made through this client, matching httpx.Client / requests.Session.
        self.cookies: CookieJar = CookieJar()
        http_handler: urllib.request.HTTPHandler = (
            _MemoryHTTPHandler(self) if self.transport is Transport.MEMORY
            else urllib.request.HTTPHandler()
        )
        self._opener = urllib.request.build_opener(
            http_handler,
            urllib.request.HTTPCookieProcessor(self.cookies),
            _NoRedirectHandler() if not follow_redirects else urllib.request.HTTPRedirectHandler(),
        )

        self._app = app
        # Port 0: the server binds a port the kernel picks and reports it (no pick-then-
        # rebind race with other processes).
        self._settings = _ServeSettings.resolve(
            env=os.environ,
            host=self.host, port=0 if port is None else port, app_runtime=app._serving_runtime()
        )
        # Under a test runner `__main__` is the runner, not the app's script.
        if app._app_file_path is None and app._defined_in is not None:
            app._set_app_file(app._defined_in)

        # This client's own server (an engine `Server`), once bound; `close()` stops that
        # one server, never another server of the same app (e.g. an outer TestClient).
        # `_closing` records a close() that came before the server existed, so the
        # server is stopped as soon as it is bound. `_started` is set once the server is
        # bound or its thread has ended, whichever comes first.
        self._lock = threading.Lock()
        self._server = None
        self._closing = False
        self._server_error: Exception | None = None
        self._started = threading.Event()
        self._thread = threading.Thread(
            target=self._serve, name="mipyre-testclient", daemon=True
        )
        self._thread.start()
        self._wait_until_ready()

    @property
    def port(self) -> int:
        """The port the server listens on (the one the kernel picked for ``port=None``).
        A memory-transport client has none: ``AttributeError``."""
        if self.transport is Transport.MEMORY:
            raise AttributeError(
                "a TestClient with transport='memory' listens on no port; "
                "pass transport='tcp' for a real TCP listener"
            )
        server = self._server
        return self._settings.port if server is None else server.port

    @property
    def base_url(self) -> str:
        if self.transport is Transport.MEMORY:
            return f"http://{_MEMORY_HOST}"
        return f"http://{self.host}:{self.port}"

    def _describe(self) -> str:
        """The server, as messages name it."""
        if self.transport is Transport.MEMORY:
            return "in-memory server"
        return f"server on port {self.port}"

    def _serve(self) -> None:
        try:
            self._app._serve(
                self._settings,
                lambda settings: self._app._start(
                    settings, on_bound=self._bound, transport=self.transport
                ),
            )
        except Exception as e:
            self._server_error = e
            _logger.error(
                "TestClient %s stopped with an error; clients will see connection "
                "refused", self._describe(), exc_info=e,
            )
        finally:
            self._started.set()

    def _bound(self, server) -> None:
        """The engine bound this client's server; it serves next unless close() came first."""
        with self._lock:
            self._server = server
            if self._closing:
                server.shutdown()
        self._started.set()

    def _connect(self) -> socket.socket:
        """A new in-memory connection to this client's server (memory transport): the
        client's end of a socket pair, blocking."""
        server = self._server
        if server is None:
            raise ConnectionRefusedError("the TestClient's server is not bound")
        return server.connect()

    def _wait_until_ready(self) -> None:
        # The engine binds every listener (or opens its in-memory inboxes) before it builds
        # a worker, and the server is this client's from then on; a connection made after
        # that waits in the backlog until the workers serve it. So one request, once bound,
        # says when it serves: any HTTP response (2xx-5xx) proves it, a failure means the
        # start failed (the thread then ends with its error).
        if not self._started.wait(_READY_TIMEOUT_S):
            raise RuntimeError(
                f"TestClient: server was not bound within {_READY_TIMEOUT_S}s"
            )
        if self._server is None:
            self._thread.join(_STOP_TIMEOUT_S)
            self._raise_exited_early()
        try:
            with self._opener.open(f"{self.base_url}/", timeout=_READY_TIMEOUT_S):
                pass
            return
        except urllib.error.HTTPError as e:
            # An HTTP error still proves the server is up. It holds the open
            # connection: close it so the probe doesn't leak it.
            e.close()
            return
        except (urllib.error.URLError, OSError) as e:
            probe_error = e
        # The server ended while starting: its own error is the cause, not the probe's.
        self._thread.join(_STOP_TIMEOUT_S)
        if not self._thread.is_alive():
            self._raise_exited_early()
        raise RuntimeError(
            f"TestClient: {self._describe()} did not answer its first request within "
            f"{_READY_TIMEOUT_S}s: {probe_error}"
        ) from probe_error

    def _raise_exited_early(self) -> NoReturn:
        """The server thread ended before serving: raise what stopped it, as it is."""
        err = self._server_error
        if err is None:
            raise RuntimeError("TestClient: server thread exited before accepting connections")
        if self._app._defined_in is None and self._settings.mode.uses_workers:
            err.add_note(
                "This app was created inside a function, so sub-interpreter workers "
                "cannot rebuild it by executing its module. Define it at module level, "
                "or create it with Mipyre(runtime='main') to serve every handler on the "
                "main interpreter."
            )
        raise err

    def close(self) -> None:
        """Stop this client's server and wait until it has: its shutdown hooks have run and
        the port (if any) is free. Other servers of the same app keep serving. Safe from
        any thread, also while the server is still starting. Idempotent.

        Raises ``WorkersAbandoned`` when worker threads outlived the shutdown grace
        period: ``app.run()`` would exit the process there; a test process is left
        running, and exits with status 1 without finalizing if such a thread still
        owns its interpreter at exit."""
        with self._lock:
            self._closing = True
            server = self._server
        if server is not None:
            server.shutdown()
        self._thread.join(_STOP_TIMEOUT_S)
        if self._thread.is_alive():
            raise RuntimeError(
                f"TestClient: {self._describe()} did not stop within {_STOP_TIMEOUT_S}s"
            )
        err, self._server_error = self._server_error, None
        if isinstance(err, WorkersAbandoned):
            raise err

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def request(
        self,
        method: str,
        path: str,
        *,
        body: bytes | str | dict | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> TestResponse:
        """Issue a raw request. Prefer the method-specific helpers."""
        url = f"{self.base_url}{path}"
        if params:
            # Append (don't replace) — path may already carry a query.
            if path.endswith(("?", "&")):
                sep = ""
            else:
                sep = "&" if "?" in path else "?"
            url = f"{url}{sep}{urllib.parse.urlencode(params, doseq=True)}"

        req_headers = dict(headers or {})
        if isinstance(body, dict):
            body = json.dumps(body).encode("utf-8")
            req_headers.setdefault("Content-Type", "application/json")
        elif isinstance(body, str):
            body = body.encode("utf-8")

        req = urllib.request.Request(
            url, data=body, headers=req_headers, method=method.upper()
        )
        eff_timeout = timeout if timeout is not None else self.timeout

        try:
            with self._opener.open(req, timeout=eff_timeout) as resp:
                return TestResponse(
                    status_code=resp.status,
                    body=resp.read(),
                    headers=_collapse_headers(resp.headers),
                )
        except urllib.error.HTTPError as e:
            # Reading the error body can fail (timeout, connection reset): the status
            # is still the answer, so the body is empty rather than the error escaping.
            try:
                err_body = e.read()
            except (OSError, http.client.HTTPException):
                err_body = b""
            finally:
                e.close()
            return TestResponse(
                status_code=e.code,
                body=err_body,
                headers=_collapse_headers(e.headers),
            )
        except urllib.error.URLError as e:
            raise RuntimeError(f"TestClient: request failed: {e.reason}") from e

    def get(self, path: str, **kwargs) -> TestResponse:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs) -> TestResponse:
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs) -> TestResponse:
        return self.request("PUT", path, **kwargs)

    def delete(self, path: str, **kwargs) -> TestResponse:
        return self.request("DELETE", path, **kwargs)

    def patch(self, path: str, **kwargs) -> TestResponse:
        return self.request("PATCH", path, **kwargs)

    def options(self, path: str, **kwargs) -> TestResponse:
        return self.request("OPTIONS", path, **kwargs)

    def head(self, path: str, **kwargs) -> TestResponse:
        return self.request("HEAD", path, **kwargs)

    # ------------------------------------------------------------------
    # WebSocket
    # ------------------------------------------------------------------

    def websocket_connect(self, path: str, **connect_kwargs) -> "WebSocketSession":
        """Open a WebSocket to ``path``. Requires the ``websockets`` package.

        Usage::

            with c.websocket_connect("/chat") as ws:
                ws.send("hello")
                reply = ws.recv()

        Extra kwargs are forwarded to ``websockets.sync.client.connect``
        (e.g. ``additional_headers={"Authorization": "Bearer ..."}``).
        """
        try:
            from websockets.sync.client import connect as _ws_connect, unix_connect as _ws_unix_connect
        except ImportError as e:
            raise ImportError(
                "TestClient.websocket_connect requires the websockets package. "
                "Install with: pip install websockets"
            ) from e
        if self.transport is Transport.MEMORY:
            # A Unix socket: told so, the library sets no TCP option on it.
            uri = f"ws://{_MEMORY_HOST}{path}"
            return WebSocketSession(_ws_unix_connect(uri=uri, sock=self._connect(), **connect_kwargs))
        uri = f"ws://{self.host}:{self.port}{path}"
        return WebSocketSession(_ws_connect(uri, **connect_kwargs))


class WebSocketSession:
    """Thin adapter around ``websockets.sync.client.ClientConnection``
    that supports use as a context manager and exposes ``send``/``recv``
    directly so tests don't have to learn the underlying library."""

    def __init__(self, conn: Any):
        self._conn = conn

    def __enter__(self) -> "WebSocketSession":
        return self

    def __exit__(self, *args):
        self.close()

    def send(self, msg: str | bytes) -> None:
        self._conn.send(msg)

    def recv(self, timeout: float | None = None) -> str | bytes:
        if timeout is not None:
            return self._conn.recv(timeout=timeout)
        return self._conn.recv()

    def __iter__(self) -> Iterator[str | bytes]:
        yield from self._conn

    def close(self) -> None:
        self._conn.close()


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Suppresses urllib's automatic redirect following."""

    def redirect_request(self, *args, **kwargs):
        return None


class _MemoryHTTPConnection(http.client.HTTPConnection):
    """An ``HTTPConnection`` whose socket is a new in-memory connection to the client's
    server: the request and response bytes are HTTP as on TCP, parsed by hyper and by
    ``http.client``."""

    def __init__(self, host: str, *, client: TestClient, **kwargs: Any) -> None:
        super().__init__(host, **kwargs)
        self._client = client

    def connect(self) -> None:
        sock = self._client._connect()
        # A number or None as given; anything else is http.client's "the global default"
        # marker, which leaves the connection's default (block until done).
        timeout = self.timeout
        if timeout is None or isinstance(timeout, (int, float)):
            sock.settimeout(timeout)
        self.sock = sock


class _MemoryHTTPHandler(urllib.request.HTTPHandler):
    """urllib's ``http://`` handler, opening each request on an in-memory connection."""

    def __init__(self, client: TestClient) -> None:
        super().__init__()
        self._client = client

    def http_open(self, req):
        def connection(host: str, **kwargs: Any) -> _MemoryHTTPConnection:
            return _MemoryHTTPConnection(host, client=self._client, **kwargs)

        return self.do_open(connection, req)
