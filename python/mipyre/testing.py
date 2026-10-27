"""TestServer and TestClient — integration-test a Mipyre app in the test process.

``TestServer(app)`` serves the app in a background thread on a real TCP port, as
``mipyre run`` does (every runtime, process lanes too); any HTTP client reaches it at
``server.url``. ``TestClient`` is an HTTP / WebSocket client: ``TestClient(server)``
talks to a running ``TestServer``; ``TestClient(app)`` serves the app itself.

``TestClient(app)`` starts the app in a background thread, in the mode ``app.run()`` uses by
default, on a server that binds no socket: each request is a connection made in this
process, handed to the server's accept loop and served from there exactly as a TCP
connection is (hyper, routing, sub-interp dispatch, CORS, compression, hooks). For what
only a socket has (see below), serve the app with ``TestServer`` and use
``TestClient(server)``. Define the app at module level: sub-interpreter workers rebuild
it by executing that module. Pure stdlib for HTTP; WebSocket support requires the
``websockets`` package.

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
client's ``timeout`` raises ``TimeoutError`` with either server.

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

One server for the session, a fresh client per test::

    @pytest.fixture(scope="session")
    def server():
        with TestServer(app) as s:
            yield s

    @pytest.fixture
    def client(server):
        with TestClient(server) as c:
            yield c
"""

from __future__ import annotations

import http.client
import json
import socket
import threading
import urllib.parse
import urllib.request
import urllib.error
from collections import defaultdict
from contextlib import ExitStack
from dataclasses import dataclass, field
from http.cookiejar import CookieJar
from typing import Any, Iterator

from mipyre.app import Mipyre
from mipyre._server_run import ServerRun, Transport
from mipyre.engine import RunMetrics, ServerFlags

__all__ = ["TestClient", "TestResponse", "TestServer", "WebSocketSession"]

# How long the server may take to serve; building the sub-interpreter workers (each
# executes the app's module) dominates it.
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


def _url(host: str, port: int) -> str:
    """``http://host:port``, an IPv6 address in brackets."""
    if ":" in host:
        host = f"[{host}]"
    return f"http://{host}:{port}"


class TestServer:
    """One run of ``app`` in a background thread, as ``mipyre run`` serves it: every runtime
    (process lanes too), on a real TCP port. Returns once serving; any HTTP client reaches it
    at ``url``.

    The run is ``app.run()``'s, so routes run where they are declared. Workers rebuild the app
    by executing the module that created it, so define the app at module level (as in
    production). An app created inside a function can only be served with
    ``Mipyre(runtime="main")``.

    Args:
        app: Mipyre app instance.
        host: bind address (default ``127.0.0.1``).
        port: bind port. ``0`` (default) has the server bind one the kernel picks; the
              ``port`` attribute is the one it bound.
        config: the server's other flags, as code gives them (``ServerFlags(threads=2)``).
              A test server reads no environment variable and no ``pyproject.toml``: what it
              runs with does not depend on where the test runs.

    If the server cannot start (a startup hook raises, the port is taken, a worker cannot
    build the app), the constructor raises that error and leaves no thread and no bound port.

    ``close()`` (or leaving the ``with`` block) stops the server: shutdown hooks run and the
    port is released. ``TestClient(server)`` is a client of this server that does not own it.
    """

    # Tell pytest this is not a test class.
    __test__ = False

    def __init__(
        self, app: Mipyre, *, host: str = "127.0.0.1", port: int = 0, config: ServerFlags | None = None
    ) -> None:
        self._start(app, host, (config or ServerFlags())._with(host=host, port=port), Transport.TCP)

    @classmethod
    def _in_memory(cls, app: Mipyre, config: ServerFlags | None) -> TestServer:
        """A server that binds no socket, reached only through ``_connect()``: the server a
        ``TestClient(app)`` owns."""
        server = cls.__new__(cls)
        server._start(app, _MEMORY_HOST, config or ServerFlags(), Transport.MEMORY)
        return server

    def _start(self, app: Mipyre, host: str, config: ServerFlags, transport: Transport) -> None:
        self.host = host
        self._transport = transport
        self._run = ServerRun.in_test_process(app, config, transport=transport)
        # `_serving` is set once the server serves, `_settled` once it serves or its thread
        # ended; `_outcome` is how the run ended, until raised.
        self._serving = threading.Event()
        self._settled = threading.Event()
        self._outcome: BaseException | None = None
        self._run.prepare()
        self._thread = threading.Thread(target=self._serve, name="mipyre-testserver", daemon=True)
        self._thread.start()
        self._wait_until_serving()

    def metrics(self) -> RunMetrics:
        """This server's counts so far (its answers, refusals, workers), apart from any other
        server in the process: the engine server's ``metrics()``."""
        return self._run.metrics()

    @property
    def port(self) -> int:
        """The port the server listens on (the one the kernel picked for ``port=0``). The
        in-memory server of a ``TestClient(app)`` has none: ``AttributeError``."""
        return self._run.port

    @property
    def url(self) -> str:
        """``http://127.0.0.1:54321``; ``http://testserver`` for an in-memory server."""
        if self._transport is Transport.MEMORY:
            return f"http://{_MEMORY_HOST}"
        return _url(self.host, self.port)

    def _describe(self) -> str:
        """The server, as messages name it."""
        if self._transport is Transport.MEMORY:
            return "in-memory server"
        return f"server on port {self.port}"

    def _serve(self) -> None:
        try:
            self._outcome = self._run.serve_to_end(on_serving=self._on_serving)
        finally:
            self._settled.set()

    def _on_serving(self) -> None:
        self._serving.set()
        self._settled.set()

    def _connect(self) -> socket.socket:
        """A new in-memory connection to this server (memory transport): the client's end of
        a socket pair, blocking."""
        return self._run.connect()

    def _alive(self) -> bool:
        """Whether the run is still serving: its thread ends when it stops."""
        return self._thread.is_alive()

    def _wait_until_serving(self) -> None:
        """Return once the server serves; raise the start's own error if its thread ended
        first, or ``TimeoutError`` after stopping the server if neither happened in time."""
        self._settled.wait(_READY_TIMEOUT_S)
        if self._serving.is_set():
            return
        if self._settled.is_set():
            self._thread.join()
            self._raise_outcome()
            raise RuntimeError("TestServer: the server stopped before it served, with no error")
        self._run.stop()
        self._thread.join(_STOP_TIMEOUT_S)
        still = " and did not stop" if self._thread.is_alive() else ""
        raise TimeoutError(f"TestServer: {self._describe()} did not serve within {_READY_TIMEOUT_S}s{still}")

    def _raise_outcome(self) -> None:
        """Raise how the server's run ended, once; a clean end raises nothing."""
        outcome, self._outcome = self._outcome, None
        if outcome is not None:
            raise outcome

    def close(self) -> None:
        """Stop this server and wait until it has: its shutdown hooks have run and the port
        (if any) is free. Other servers of the same app keep serving. Safe from any thread.
        Raises how the run ended (a shutdown hook's error, ``AbandonedWorkersError`` when
        worker threads outlived the shutdown grace period, …) once; a second ``close()``
        returns. ``app.run()`` would exit the process on ``AbandonedWorkersError``; a test
        process is left running, and exits with status 1 without finalizing if such a thread
        still owns its interpreter at exit."""
        self._run.stop()
        self._thread.join(_STOP_TIMEOUT_S)
        if self._thread.is_alive():
            raise RuntimeError(f"TestServer: {self._describe()} did not stop within {_STOP_TIMEOUT_S}s")
        self._raise_outcome()

    def __enter__(self) -> TestServer:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class TestClient:
    """Test client — an HTTP / WebSocket client of a server of the app.

    ``TestClient(app)`` serves the app itself, in a background thread, the way
    ``app.run()`` does, and stops it on ``close()``. ``TestClient(server)`` is a client of a
    running :class:`TestServer` and leaves it running on ``close()``: one server, a fresh
    client (and cookie jar) per test.

    Args:
        target: a Mipyre app, served by this client on a server that binds no socket (see
              the module docstring for what it lacks), or a :class:`TestServer`.
        timeout: default per-request timeout in seconds (default 10).
        follow_redirects: whether to follow 3xx redirects (default True,
              matching urllib's historical behavior).
        config: the flags of the server this client serves the app on
              (``ServerFlags(threads=2)``); like ``TestServer``'s, it reads no environment
              variable and no ``pyproject.toml``.

    ``close()`` (or leaving the ``with`` block) stops the server this client serves, if it
    serves one: shutdown hooks run and the port, if any, is released.
    """

    # Tell pytest this is not a test class (silences the collection
    # warning when the client lands in a test module namespace).
    __test__ = False

    def __init__(
        self,
        target: Mipyre | TestServer,
        *,
        timeout: float = 10.0,
        follow_redirects: bool = True,
        config: ServerFlags | None = None,
    ):
        if isinstance(target, TestServer):
            if config is not None:
                raise TypeError("TestClient(server, config=...): the server was configured when it was made")
            self.server, self._owns_server = target, False
        else:
            self.server, self._owns_server = TestServer._in_memory(target, config), True
        self.timeout = timeout
        self.follow_redirects = follow_redirects

        # Cookie jar — cookies set by the server persist across requests
        # made through this client, matching httpx.Client / requests.Session.
        self.cookies: CookieJar = CookieJar()
        http_handler: _HTTPHandler = (
            _MemoryHTTPHandler(self.server) if self.server._transport is Transport.MEMORY else _HTTPHandler()
        )
        self._opener = urllib.request.build_opener(
            http_handler,
            urllib.request.HTTPCookieProcessor(self.cookies),
            _NoRedirectHandler() if not follow_redirects else urllib.request.HTTPRedirectHandler(),
        )

    @property
    def port(self) -> int:
        """The port of the server. An in-memory server has none: ``AttributeError``."""
        return self.server.port

    @property
    def base_url(self) -> str:
        return self.server.url

    def close(self) -> None:
        """Stop the server this client serves, if it serves one (``TestClient(app)``), and
        raise how its run ended, as :meth:`TestServer.close` does. A client of a given
        ``TestServer`` stops nothing."""
        if self._owns_server:
            self.server.close()

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
            with e:
                return TestResponse(
                    status_code=e.code,
                    body=e.read(),
                    headers=_collapse_headers(e.headers),
                )
        except urllib.error.URLError as e:
            failed = RuntimeError(f"TestClient: request failed: {e.reason}")
            if not self.server._alive():
                # The server is gone: why it went is the cause, not the refused connection.
                raise failed from (self.server._outcome or e)
            raise failed from e

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
        if self.server._transport is Transport.MEMORY:
            # A Unix socket: told so, the library sets no TCP option on it.
            uri = f"ws://{_MEMORY_HOST}{path}"
            return WebSocketSession(_ws_unix_connect(uri=uri, sock=self.server._connect(), **connect_kwargs))
        uri = "ws://" + self.server.url.removeprefix("http://") + path
        return WebSocketSession(_ws_connect(uri, **connect_kwargs))


class WebSocketSession:
    """Thin adapter around ``websockets.sync.client.ClientConnection``
    that supports use as a context manager and exposes ``send``/``recv``
    directly so tests don't have to learn the underlying library."""

    def __init__(self, conn: Any):
        # The session owns the connection's lifetime from here, so it enters the connection's own context:
        # websockets (17.1+) warns on every send/recv of a connection that was never entered.
        self._lifetime = ExitStack()
        self._conn = self._lifetime.enter_context(conn)

    def __enter__(self) -> "WebSocketSession":
        return self

    def __exit__(self, *args) -> None:
        self._lifetime.__exit__(*args)

    def send(self, msg: str | bytes) -> None:
        self._conn.send(msg)

    def recv(self, timeout: float | None = None) -> str | bytes:
        return self._conn.recv(timeout=timeout)

    def __iter__(self) -> Iterator[str | bytes]:
        yield from self._conn

    def close(self) -> None:
        self._lifetime.close()


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Suppresses urllib's automatic redirect following."""

    def redirect_request(self, *args, **kwargs):
        return None


class _MemoryHTTPConnection(http.client.HTTPConnection):
    """An ``HTTPConnection`` whose socket is a new in-memory connection to a server: the
    request and response bytes are HTTP as on TCP, parsed by hyper and by ``http.client``."""

    def __init__(self, host: str, *, server: TestServer, **kwargs: Any) -> None:
        super().__init__(host, **kwargs)
        self._server = server

    def connect(self) -> None:
        sock = self._server._connect()
        # A number or None as given; anything else is http.client's "the global default"
        # marker, which leaves the connection's default (block until done).
        timeout = self.timeout
        if timeout is None or isinstance(timeout, (int, float)):
            sock.settimeout(timeout)
        self.sock = sock


class _HTTPHandler(urllib.request.HTTPHandler):
    """urllib's ``http://`` handler, sending the request as given: urllib adds
    ``Content-Type: application/x-www-form-urlencoded`` to any request with a body the caller
    gave no type for, and that one is taken off again."""

    def http_request(self, req):
        given = req.has_header("Content-type")
        req = super().http_request(req)
        if not given:
            req.remove_header("Content-type")
        return req


class _MemoryHTTPHandler(_HTTPHandler):
    """urllib's ``http://`` handler, opening each request on an in-memory connection."""

    def __init__(self, server: TestServer) -> None:
        super().__init__()
        self._server = server

    def http_open(self, req):
        def connection(host: str, **kwargs: Any) -> _MemoryHTTPConnection:
            return _MemoryHTTPConnection(host, server=self._server, **kwargs)

        return self.do_open(connection, req)
