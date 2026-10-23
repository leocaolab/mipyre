"""Mipyre RPC — MsgPack/JSON/Protobuf content-negotiated RPC over HTTP.

Server: ``@rpc(app)`` serves a function at ``POST /rpc/<its name>``, decoding and encoding the
body. Client: ``RPCClient``, whose ``client.name(**kwargs)`` posts to that same path. Both are
imported from ``mipyre``.
"""

from __future__ import annotations

import json
import inspect
from enum import Enum, auto
from typing import TYPE_CHECKING, Callable, Iterable

from mipyre.engine import HTTPException, Response, encode_json

if TYPE_CHECKING:
    from mipyre import Mipyre

try:
    import msgpack
    HAS_MSGPACK = True
except ImportError:
    HAS_MSGPACK = False


def _method_path(name: str) -> str:
    """Return the path an RPC method is served at: ``/rpc/<name>``, for the server that
    registers it and the client that calls it alike. ``ValueError`` for a name that is not
    an identifier or starts with ``_``: ``RPCClient`` calls methods as attributes, and
    leaves ``_`` names to Python."""
    if not isinstance(name, str) or not name.isidentifier() or name.startswith("_"):
        raise ValueError(
            f"RPC method name {name!r} must be an identifier that does not start with '_' "
            "(the client calls it as client.<name>(...))"
        )
    return f"/rpc/{name}"


#: How much of a reply an `RPCError` keeps: a server cannot fill the caller's log through it.
_REPLY_KEPT = 1024


class RPCError(Exception):
    """An RPC call the server answered with a failure, or with a reply that is not one. Its
    ``method``, ``url`` and ``status`` say which call and what HTTP status. A transport
    failure is httpx's own exception, not this."""

    def __init__(self, method: str, url: str, status: int, said: str):
        self.method = method
        self.url = url
        self.status = status
        super().__init__(f"RPC {method} at {url}: HTTP {status}: {said}")


class RPCRequestError(RPCError):
    """The server refused the call (4xx): the call itself is wrong, and sending it again
    gets the same answer. ``detail`` is the reply's ``"detail"``, or the first KiB of a
    reply that is not a JSON object with one."""

    def __init__(self, method: str, url: str, status: int, detail: object):
        self.detail = detail
        super().__init__(method, url, status, repr(detail))


class RPCServerError(RPCError):
    """The server failed the call (5xx). ``detail`` as for ``RPCRequestError``;
    ``request_id`` (``None`` when the reply has none) finds the real error in the server's
    log."""

    def __init__(self, method: str, url: str, status: int, detail: object, request_id: str | None):
        self.detail = detail
        self.request_id = request_id
        where = f" (request_id={request_id})" if request_id else ""
        super().__init__(method, url, status, f"{detail!r}{where}")


class RPCReplyError(RPCError):
    """A reply that is neither a result nor a failure: a 2xx whose body does not decode,
    or a 1xx / 3xx. ``body`` is its first KiB."""

    def __init__(self, method: str, url: str, status: int, body: bytes):
        self.body = body
        super().__init__(method, url, status, f"a reply that is no result: {body!r}")


def _failure_said(body: bytes) -> tuple[object, str | None]:
    """A failure reply's ``detail`` and ``request_id``: the fields of a JSON object that
    has a ``"detail"``, else the first KiB of the body as text and no id."""
    try:
        reply = json.loads(body)
    except ValueError:
        reply = None
    if isinstance(reply, dict) and "detail" in reply:
        rid = reply.get("request_id")
        return reply["detail"], rid if isinstance(rid, str) else None
    return body[:_REPLY_KEPT].decode("utf-8", "replace"), None


class RPCClient:
    """Magic RPC client — call remote methods like local functions.

    A 2xx reply's body is the result. A 4xx raises ``RPCRequestError``, a 5xx
    ``RPCServerError``, a reply that is no result ``RPCReplyError``: all ``RPCError``.

    Usage::

        client = RPCClient("http://127.0.0.1:8000")
        result = client.get_market_snapshot(tickers=["AAPL", "TSLA"])
    """

    def __init__(self, base_url: str, use_msgpack: bool = True, timeout: float = 30.0):
        try:
            import httpx
        except ImportError as e:
            raise ImportError("RPCClient requires httpx; install with: pip install httpx") from e
        self.base_url = base_url.rstrip("/")
        self.use_msgpack = use_msgpack and HAS_MSGPACK
        self.timeout = timeout
        self._client = httpx.Client(
            http2=False,
            timeout=timeout,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )

    def __getattr__(self, method_name: str):
        if method_name.startswith("_"):
            raise AttributeError(method_name)
        url = self.base_url + _method_path(method_name)

        def remote_call(**kwargs):
            if self.use_msgpack:
                payload = msgpack.packb(kwargs, use_bin_type=True)
                content_type = "application/msgpack"
            else:
                payload = json.dumps(kwargs).encode("utf-8")
                content_type = "application/json"

            resp = self._client.post(
                url,
                content=payload,
                headers={
                    "Content-Type": content_type,
                    "Accept": content_type,
                },
            )
            status, body = resp.status_code, resp.content
            if 200 <= status < 300:
                try:
                    if self.use_msgpack and "msgpack" in resp.headers.get("content-type", ""):
                        return msgpack.unpackb(body, raw=False)
                    return json.loads(body)
                except ValueError as e:
                    # JSON's and msgpack's decode errors are all ValueErrors.
                    raise RPCReplyError(method_name, url, status, body[:_REPLY_KEPT]) from e
            if 400 <= status < 500:
                raise RPCRequestError(method_name, url, status, _failure_said(body)[0])
            if 500 <= status < 600:
                raise RPCServerError(method_name, url, status, *_failure_said(body))
            raise RPCReplyError(method_name, url, status, body[:_REPLY_KEPT])

        return remote_call

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class _Codec(Enum):
    """The body encodings an RPC endpoint speaks."""

    JSON = auto()
    MSGPACK = auto()
    PROTOBUF = auto()


_MEDIA_TYPES = {
    "application/json": _Codec.JSON,
    "application/msgpack": _Codec.MSGPACK,
    "application/x-msgpack": _Codec.MSGPACK,
    "application/protobuf": _Codec.PROTOBUF,
    "application/x-protobuf": _Codec.PROTOBUF,
}


def media_type(content_type: str | None) -> str | None:
    """``application/json; charset=utf-8`` → ``application/json``; ``None`` without one."""
    if content_type is None:
        return None
    return content_type.split(";", 1)[0].strip().lower() or None


def unsupported_media_type(content_type: str | None, accepted: Iterable[str]) -> str:
    """The reason a body whose ``content_type`` is not one of ``accepted`` is refused (415)."""
    got = "none" if content_type is None else repr(content_type)
    return f"Content-Type must be {' or '.join(sorted(accepted))}, got {got}"


def _accepted_media_types(proto_model) -> dict[str, _Codec]:
    """The request body types this endpoint decodes: JSON always, MsgPack when msgpack is
    installed, Protobuf when the endpoint has a ``proto_model``."""
    available = {_Codec.JSON}
    if HAS_MSGPACK:
        available.add(_Codec.MSGPACK)
    if proto_model is not None:
        available.add(_Codec.PROTOBUF)
    return {media: codec for media, codec in _MEDIA_TYPES.items() if codec in available}


def _reply_codec(req, request_codec: _Codec) -> _Codec:
    """MsgPack when the client asks for it (``Accept``, else the request's own type) and
    msgpack is installed; JSON otherwise."""
    accept = req.headers.get("accept")
    if accept is None:
        wanted = {request_codec}
    else:
        media = (media_type(part) for part in accept.split(","))
        wanted = {_MEDIA_TYPES[m] for m in media if m is not None and m in _MEDIA_TYPES}
    return _Codec.MSGPACK if HAS_MSGPACK and _Codec.MSGPACK in wanted else _Codec.JSON


def _undecodable(codec: _Codec, error: ValueError) -> str:
    """Return the 400's reason for a body ``codec`` could not decode. msgpack's depth limit
    raises ``StackError`` with no message, so an exception without one is told by its
    class's own description."""
    detail = str(error) or type(error).__doc__ or type(error).__name__
    return f"the body is not valid {codec.name}: {detail}"


def _takes_request(fn: Callable) -> bool:
    """Return whether ``fn`` is called ``fn(req, data)`` rather than ``fn(data)``: by the
    positional parameters it requires, one meaning ``(data)`` and two ``(req, data)``. A
    parameter with a default is never handed a value, so ``fn(data, limit=10)`` takes the
    data alone. ``TypeError`` for a signature the dispatcher can't call, or one requiring
    no positional parameter, which doesn't say which of the two it is."""
    params = inspect.signature(fn).parameters.values()
    positional = [
        p for p in params
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    required = [p.name for p in positional if p.default is inspect.Parameter.empty]
    keyword_only = [
        p.name for p in params
        if p.kind is inspect.Parameter.KEYWORD_ONLY and p.default is inspect.Parameter.empty
    ]
    forms = "fn(data) or fn(req, data)"
    if keyword_only:
        raise TypeError(
            f"RPC handler {fn.__name__!r} requires keyword-only argument(s) {keyword_only}, "
            f"which the dispatcher never supplies: an RPC handler is {forms}"
        )
    if len(required) == 1:
        return False
    if len(required) == 2:
        return True
    if not required:
        raise TypeError(
            f"RPC handler {fn.__name__!r} requires no positional argument, so it does not "
            f"say whether it is {forms}: make data (and req) parameters without defaults"
        )
    raise TypeError(
        f"RPC handler {fn.__name__!r} requires {len(required)} positional arguments "
        f"{required}, more than the dispatcher supplies: an RPC handler is {forms}; give "
        "the others defaults or remove them"
    )


def rpc(app: Mipyre, name: str | None = None, *, proto_model=None) -> Callable[[Callable], Callable]:
    """Serve the decorated function as the RPC method ``name`` (default: the function's
    name) at ``POST /rpc/<name>``, the path ``RPCClient(...).<name>(...)`` calls::

        from mipyre import Mipyre, rpc

        app = Mipyre()

        @rpc(app)
        def get_data(data):
            return {"prices": [150.1, 150.2]}

        @rpc(app, "add", proto_model=AddRequest)
        def add_proto(req, data):
            return {"sum": data.a + data.b}

    The handler is ``fn(data)`` or ``fn(req, data)``, told apart by how many positional
    parameters it requires, and runs on the main interpreter, where any library loads.
    ``proto_model`` is a betterproto message class: a Protobuf body is
    ``proto_model().parse(body)``.
    The body's ``Content-Type`` must be JSON, MsgPack (when msgpack is installed) or, with
    a ``proto_model``, Protobuf (415 otherwise); the engine has already refused a request
    from another site's page that ``enable_cors(allow_origins=...)`` does not trust. The
    body is decoded by that codec whatever its length, so an empty body is a Protobuf
    message with every field at its default, and no JSON or MsgPack value. The reply's body
    is the handler's result itself, in the codec the client asked for. A refused or
    undecodable body answers its status with ``{"detail": <reason>}``; a handler that
    raises is the engine's 500 ``{"detail": "Internal Server Error", "request_id": ...}``,
    and an ``HTTPException`` it raises answers as it says.

    :raises ValueError: a name ``RPCClient`` can't call (not an identifier, or starting
        with ``_``).
    :raises TypeError: a handler signature that is neither ``fn(data)`` nor ``fn(req, data)``.
    """
    given_path = None if name is None else _method_path(name)
    accepted = _accepted_media_types(proto_model)

    def decorator(fn: Callable) -> Callable:
        path = given_path or _method_path(fn.__name__)
        takes_request = _takes_request(fn)

        def _encode(result, req, codec: _Codec):
            """The result itself, in the codec the client asked for. A ``dict`` or ``list``
            is returned as it is: the engine encodes it as JSON, the bytes ``encode_json``
            gives, without a ``Response`` in between; any other result is encoded here, so a
            ``str`` is a JSON string, not text, and a tuple is not read as (body, status)."""
            if _reply_codec(req, codec) is _Codec.MSGPACK:
                return Response(body=msgpack.packb(result, use_bin_type=True), content_type="application/msgpack")
            if type(result) is dict or type(result) is list:
                return result
            return Response(body=encode_json(result), content_type="application/json")

        def _admit(req) -> tuple[_Codec, object]:
            """Return the request's codec and decoded body; raises the ``HTTPException``
            refusing it."""
            content_type = req.headers.get("content-type")
            media = media_type(content_type)
            codec = None if media is None else accepted.get(media)
            if codec is None:
                raise HTTPException(415, unsupported_media_type(content_type, accepted))
            match codec:
                case _Codec.JSON:
                    return codec, req.json()
                case _Codec.MSGPACK | _Codec.PROTOBUF:
                    try:
                        if codec is _Codec.MSGPACK:
                            return codec, msgpack.unpackb(req.body, raw=False)
                        return codec, proto_model().parse(req.body)
                    except ValueError as e:
                        # msgpack's and protobuf's decode errors are ValueErrors.
                        raise HTTPException(400, _undecodable(codec, e)) from e

        def sync_wrapper(req):
            codec, data = _admit(req)
            return _encode(fn(req, data) if takes_request else fn(data), req, codec)

        async def async_wrapper(req):
            codec, data = _admit(req)
            return _encode(await (fn(req, data) if takes_request else fn(data)), req, codec)

        handler = async_wrapper if inspect.iscoroutinefunction(fn) else sync_wrapper
        # Named after fn, but no __wrapped__: the route calls handler(req), and path-param
        # injection must not read fn's (req, data) signature through it.
        handler.__name__ = fn.__name__
        handler.__qualname__ = fn.__qualname__
        handler.__doc__ = fn.__doc__

        # On main: an RPC handler may use any library, not only sub-interpreter-safe ones.
        app.post(path, handler, runtime="main")
        return fn

    return decorator
