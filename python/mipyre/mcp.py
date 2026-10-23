"""MCP (Model Context Protocol) server support for Mipyre.

Implements JSON-RPC 2.0 over HTTP at the /mcp endpoint.
AI applications can discover and invoke tools,
read resources, and use prompt templates.

Usage::

    from mipyre import Mipyre
    from mipyre.mcp import MCPServer

    app = Mipyre()
    mcp = MCPServer(app)

    @mcp.tool(description="Add two numbers")
    def add(a: int, b: int) -> int:
        return a + b

    @mcp.resource("config://app")
    def get_config():
        return {"version": "1.0", "debug": False}

    @mcp.prompt("greeting", description="Generate a greeting")
    def greeting(name: str) -> str:
        return f"Hello {name}, how can I help you today?"

    app.run()
"""

from __future__ import annotations

import inspect
import json
import logging
import typing
from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Callable

from mipyre.engine import HTTPException, Response, encode_json
from mipyre._rpc import media_type, unsupported_media_type

if TYPE_CHECKING:
    from mipyre import Mipyre

_log = logging.getLogger(__name__)

# `/mcp` takes a JSON-RPC body only (MCP's HTTP transport).
_BODY_TYPES = frozenset({"application/json"})


class ToolError(Exception):
    """Raised by a tool to report a failure to the model that called it: the call's result
    carries the message with ``isError: true``, and nothing is logged. Any other exception
    is the server's failure: the client gets -32603 and the traceback is logged."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


def _then(value: Any, finish: Callable[[Any], Any]) -> Any:
    """Return ``finish(value)``; when ``value`` is awaitable (an ``async def`` handler's
    coroutine), a coroutine that awaits it first. The ``/mcp`` route returns that coroutine
    and the engine drives it, as it does any handler's."""
    if not inspect.isawaitable(value):
        return finish(value)

    async def finish_later() -> Any:
        return finish(await value)

    return finish_later()


def _as_text(result: Any) -> str:
    """A handler's result as the text MCP carries: text as is; any other value as JSON (a
    list, a number, None), never its repr."""
    return result if isinstance(result, str) else json.dumps(result)


def _extract_schema(fn: Callable) -> dict:
    """The JSON Schema of ``fn``'s arguments, from its type hints (see ``hint_schema``).
    A parameter without a hint accepts any JSON value; one whose hint has no JSON form
    is a registration error naming it."""
    # Imported here: every worker imports this module, and only an app with MCP tools needs it.
    from mipyre._json_schema import NoJsonForm, hint_schema

    sig = inspect.signature(fn)
    # Not `fn.__annotations__`: under `from __future__ import annotations` those are
    # strings (`'int'`).
    try:
        hints = typing.get_type_hints(fn)
    except NameError as e:
        # A forward ref that doesn't resolve (a TYPE_CHECKING-only import):
        # without the real type there is no schema for it.
        raise TypeError(
            f"MCP tool {fn.__qualname__}: cannot resolve a parameter's type hint "
            f"({e}); import the type at runtime or pass input_schema="
        ) from e
    properties = {}
    required = []

    for name, param in sig.parameters.items():
        if name in ("self", "cls"):
            continue
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue
        try:
            properties[name] = hint_schema(hints[name]) if name in hints else {}
        except NoJsonForm as e:
            raise TypeError(
                f"MCP tool {fn.__qualname__}: parameter {name!r} is annotated {e.hint!r}, "
                "which has no JSON Schema form (supported: bool, int, float, str, None, "
                "Any, list[T], dict[str, T], T | U, Optional[T], Literal[...]); change the "
                "hint or pass input_schema="
            ) from None
        if param.default is inspect.Parameter.empty:
            required.append(name)

    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


# JSON Schema's primitive types, as checks on the Python value json.loads produced.
_JSON_TYPE_CHECKS: dict[str, Callable[[object], bool]] = {
    "null": lambda v: v is None,
    "boolean": lambda v: isinstance(v, bool),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "string": lambda v: isinstance(v, str),
    "array": lambda v: isinstance(v, list),
    "object": lambda v: isinstance(v, dict),
}


def _json_equal(a: object, b: object) -> bool:
    """Whether two decoded JSON values are the same JSON value: ``true`` is not ``1`` and
    ``1`` is ``1.0``, as JSON Schema compares ``enum`` and ``const``."""
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(map(_json_equal, a, b))
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(_json_equal(a[k], b[k]) for k in a)
    return type(a) is type(b) and a == b


def _json_type_name(value: object) -> str:
    return next(
        (name for name, check in _JSON_TYPE_CHECKS.items() if check(value)),
        type(value).__name__,
    )


class _Invalid(Exception):
    """An argument value its schema does not accept, with where and why."""


def _check_value(schema: dict, value: object, where: str) -> None:
    """Checks ``value`` against ``schema``: ``type``, ``enum``, ``const``, ``anyOf``,
    ``items``, ``properties``, ``required`` and ``additionalProperties``. Other keywords
    (``format``, ``minimum``…) are annotations here and are not checked."""
    expected = schema.get("type")
    if expected is not None:
        names = expected if isinstance(expected, list) else [expected]
        if not any(_JSON_TYPE_CHECKS[n](value) for n in names):
            raise _Invalid(f"{where}: expected {' or '.join(names)}, got {_json_type_name(value)}")
    if "enum" in schema and not any(_json_equal(value, option) for option in schema["enum"]):
        raise _Invalid(f"{where}: {value!r} is not one of {schema['enum']!r}")
    if "const" in schema and not _json_equal(value, schema["const"]):
        raise _Invalid(f"{where}: must be {schema['const']!r}, got {value!r}")
    if "anyOf" in schema:
        reasons = []
        for option in schema["anyOf"]:
            try:
                _check_value(option, value, where)
                break
            except _Invalid as e:
                reasons.append(str(e))
        else:
            raise _Invalid(" / ".join(reasons))
    if isinstance(value, list) and "items" in schema:
        for i, item in enumerate(value):
            _check_value(schema["items"], item, f"{where}[{i}]")
    if isinstance(value, dict):
        _check_object(schema, value, where)


def _check_object(schema: dict, value: dict, where: str) -> None:
    missing = [k for k in schema.get("required", ()) if k not in value]
    if missing:
        raise _Invalid(f"{where}: missing required key(s) {missing}")
    properties = schema.get("properties", {})
    extra = schema.get("additionalProperties", True)
    for key, item in value.items():
        if key in properties:
            _check_value(properties[key], item, f"{where}.{key}")
        elif extra is False:
            raise _Invalid(f"{where}: unexpected key {key!r}")
        elif isinstance(extra, dict):
            _check_value(extra, item, f"{where}.{key}")


def _check_arguments(schema: dict, arguments: dict) -> None:
    """Every argument value against its property in the tool's input schema; -32602 with
    the reason for one it does not accept."""
    properties = schema.get("properties", {})
    try:
        for name, value in arguments.items():
            if name in properties:
                _check_value(properties[name], value, f"argument {name!r}")
    except _Invalid as e:
        raise JsonRpcError(JsonRpcCode.INVALID_PARAMS, f"invalid params: {e}") from None


class JsonRpcCode(IntEnum):
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603
    # MCP: resources/read of a URI no resource is registered for.
    RESOURCE_NOT_FOUND = -32002


class JsonRpcError(Exception):
    """A request the server rejects, with the code and message the client gets."""

    def __init__(self, code: JsonRpcCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class _Params:
    """What a handler accepts by name, read off its signature at registration."""

    names: tuple[str, ...]  # in signature order
    required: tuple[str, ...]
    accepts_any: bool  # **kwargs

    @classmethod
    def of(cls, fn: Callable) -> _Params:
        params = list(inspect.signature(fn).parameters.values())
        positional_only = [p.name for p in params if p.kind == inspect.Parameter.POSITIONAL_ONLY]
        if positional_only:
            raise TypeError(
                f"MCP handler {fn.__qualname__} has positional-only parameter(s) "
                f"{positional_only}; MCP arguments are passed by name"
            )
        named = [p for p in params if p.kind in _NAMED]
        return cls(
            names=tuple(p.name for p in named),
            required=tuple(p.name for p in named if p.default is inspect.Parameter.empty),
            accepts_any=any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params),
        )

    def check(self, arguments: dict) -> None:
        unexpected = [] if self.accepts_any else sorted(a for a in arguments if a not in self.names)
        if unexpected:
            raise JsonRpcError(JsonRpcCode.INVALID_PARAMS, f"unexpected argument(s): {unexpected}")
        missing = [n for n in self.required if n not in arguments]
        if missing:
            raise JsonRpcError(JsonRpcCode.INVALID_PARAMS, f"missing required argument(s): {missing}")


_NAMED = (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)


@dataclass(frozen=True)
class _Tool:
    name: str
    description: str
    input_schema: dict
    params: _Params
    handler: Callable


@dataclass(frozen=True)
class _Resource:
    uri: str
    name: str
    description: str
    mime_type: str
    handler: Callable


@dataclass(frozen=True)
class _PromptArgument:
    """One argument a prompt template takes, as ``prompts/list`` announces it."""

    name: str
    description: str | None
    required: bool

    def as_json(self) -> dict:
        announced: dict[str, Any] = {"name": self.name, "required": self.required}
        if self.description is not None:
            announced["description"] = self.description
        return announced


_PROMPT_ARGUMENT_KEYS = frozenset({"name", "description", "required"})


def _prompt_arguments(prompt: str, declared: object, params: _Params) -> tuple[_PromptArgument, ...]:
    """The arguments ``prompt`` announces: ``declared`` (MCP's ``{"name", "description",
    "required"}`` objects) checked against what its handler takes, or, without it, the
    handler's parameters. ``TypeError`` / ``ValueError`` name the entry a client could
    never call it with."""
    if declared is None:
        return tuple(_PromptArgument(n, None, n in params.required) for n in params.names)
    if not isinstance(declared, (list, tuple)):
        raise TypeError(f"MCP prompt {prompt!r}: arguments= takes a list of dicts, got {type(declared).__name__}")
    arguments = tuple(_prompt_argument(prompt, entry) for entry in declared)
    names = [a.name for a in arguments]
    repeated = sorted({n for n in names if names.count(n) > 1})
    if repeated:
        raise ValueError(f"MCP prompt {prompt!r}: arguments= names {repeated} more than once")
    untaken = [] if params.accepts_any else [n for n in names if n not in params.names]
    if untaken:
        raise ValueError(f"MCP prompt {prompt!r}: arguments= declares {untaken}, which its handler does not take")
    required = {a.name for a in arguments if a.required}
    unannounced = [n for n in params.required if n not in required]
    if unannounced:
        raise ValueError(
            f"MCP prompt {prompt!r}: its handler requires {unannounced}, which arguments= "
            "does not declare as required"
        )
    return arguments


def _prompt_argument(prompt: str, entry: object) -> _PromptArgument:
    if not isinstance(entry, dict):
        raise TypeError(f"MCP prompt {prompt!r}: each of arguments= is a dict, got {type(entry).__name__}")
    unknown = sorted(set(entry) - _PROMPT_ARGUMENT_KEYS)
    if unknown:
        raise ValueError(f"MCP prompt {prompt!r}: argument {entry!r} has unknown key(s) {unknown}")
    name, description, required = entry.get("name"), entry.get("description"), entry.get("required", False)
    if not isinstance(name, str) or not name:
        raise TypeError(f"MCP prompt {prompt!r}: argument {entry!r} needs a non-empty str 'name'")
    if description is not None and not isinstance(description, str):
        raise TypeError(f"MCP prompt {prompt!r}: argument {name!r} has a non-str 'description'")
    if not isinstance(required, bool):
        raise TypeError(f"MCP prompt {prompt!r}: argument {name!r} has a non-bool 'required'")
    return _PromptArgument(name, description, required)


@dataclass(frozen=True)
class _Prompt:
    name: str
    description: str
    arguments: tuple[_PromptArgument, ...]
    params: _Params
    handler: Callable


class MCPServer:
    """An MCP server on ``app``: its tools, resources and prompts, served at ``POST /mcp``
    on the main interpreter."""

    def __init__(self, app: Mipyre) -> None:
        """Register ``POST /mcp`` on ``app`` (not in its OpenAPI document: the contract is
        JSON-RPC, which ``tools/list`` describes) and its startup line."""
        self._tools: dict[str, _Tool] = {}
        self._resources: dict[str, _Resource] = {}
        self._prompts: dict[str, _Prompt] = {}
        app.post("/mcp", self._http_handler(app), runtime="main", in_openapi=False)
        app.on_startup(lambda: print(f"  MCP: {self._summary()} → POST /mcp", flush=True))

    # ------------------------------------------------------------------
    # Decorators
    # ------------------------------------------------------------------

    def tool(
        self,
        fn: Callable | None = None,
        *,
        name: str | None = None,
        description: str | None = None,
        input_schema: dict | None = None,
    ):
        """Register a tool that AI models can invoke. ``ValueError`` when a tool of that
        name is already registered. The tool reports a failure to the model by raising
        ``ToolError``."""

        def register(f: Callable) -> Callable:
            tool_name = name or f.__name__
            _refuse_repeat("tool", tool_name, self._tools)
            self._tools[tool_name] = _Tool(
                name=tool_name,
                description=description or f.__doc__ or "",
                input_schema=input_schema or _extract_schema(f),
                params=_Params.of(f),
                handler=f,
            )
            return f

        if fn is not None:
            return register(fn)
        return register

    def resource(
        self,
        uri: str,
        *,
        name: str | None = None,
        description: str | None = None,
        mime_type: str = "application/json",
    ):
        """Register a readable resource at an exact URI. ``ValueError`` when one is already
        registered there."""

        def register(fn: Callable) -> Callable:
            _refuse_repeat("resource", uri, self._resources)
            self._resources[uri] = _Resource(
                uri=uri,
                name=name or fn.__name__,
                description=description or fn.__doc__ or "",
                mime_type=mime_type,
                handler=fn,
            )
            return fn

        return register

    def prompt(
        self,
        name: str,
        *,
        description: str | None = None,
        arguments: list[dict] | None = None,
    ):
        """Register a prompt template. ``arguments``: what ``prompts/list`` announces, as
        MCP's ``{"name", "description", "required"}`` objects (default: the handler's
        parameters); an entry the handler could not be called with raises ``TypeError`` or
        ``ValueError`` here. ``ValueError`` too when a prompt of that name is registered."""

        def register(fn: Callable) -> Callable:
            _refuse_repeat("prompt", name, self._prompts)
            params = _Params.of(fn)
            self._prompts[name] = _Prompt(
                name=name,
                description=description or fn.__doc__ or "",
                arguments=_prompt_arguments(name, arguments, params),
                params=params,
                handler=fn,
            )
            return fn

        return register

    def _summary(self) -> str:
        return f"{len(self._tools)} tools, {len(self._resources)} resources, {len(self._prompts)} prompts"

    def _http_handler(self, app: Mipyre) -> Callable:
        """Return the route handler of ``POST /mcp``: it refuses a body that is not JSON
        (415), answers a body that does not decode with -32700, a notification with ``202``
        and no body, and the rest with ``answer``. A request from another site's page has
        already been refused by the engine."""

        def mcp_handler(req):
            content_type = req.headers.get("content-type")
            if media_type(content_type) not in _BODY_TYPES:
                return _reply(_refusal(unsupported_media_type(content_type, _BODY_TYPES)), 415)
            try:
                message = req.json()
            except HTTPException as e:
                return _reply(_error_response(None, JsonRpcCode.PARSE_ERROR, f"Parse error: {e.detail}"))
            return _then(self.answer(message, request_id=req.request_id), _reply)

        return mcp_handler

    # ------------------------------------------------------------------
    # JSON-RPC 2.0 handler
    # ------------------------------------------------------------------

    def answer(self, message: Any, request_id: str | None = None) -> Any:
        """Return the reply to one decoded JSON-RPC 2.0 request, or ``None`` for a
        notification, which gets none. When the tool, resource or prompt it calls is
        ``async``, return a coroutine that resolves to the reply.

        ``request_id`` is the HTTP request's id: an internal error (-32603) reports it
        in ``error.data.request_id`` and logs it with the exception, so the two can be
        matched up. Without one (a direct call) the error has no ``data``.
        """
        # A valid JSON-RPC 2.0 request is an Object; primitives and arrays
        # (the latter reserved for batch requests, which this server does
        # not support) must be rejected with -32600 per the spec.
        if not isinstance(message, dict):
            return _error_response(None, JsonRpcCode.INVALID_REQUEST, "Invalid Request")

        # JSON-RPC 2.0 §4.2: the "jsonrpc" member MUST be exactly "2.0".
        if message.get("jsonrpc") != "2.0":
            return _error_response(
                message.get("id"), JsonRpcCode.INVALID_REQUEST, "Invalid Request: jsonrpc must be '2.0'"
            )

        # JSON-RPC 2.0 §4: no "id" makes it a notification, which gets no reply.
        is_notification = "id" not in message
        req_id = message.get("id")
        # §4: an id is a String, Number or Null; echoed back as is.
        if not _is_json_rpc_id(req_id):
            return _error_response(
                None, JsonRpcCode.INVALID_REQUEST,
                f"Invalid Request: id must be a string, number or null, got {_json_type_name(req_id)}",
            )
        method = message.get("method")
        if not isinstance(method, str):
            return _error_response(
                req_id, JsonRpcCode.INVALID_REQUEST,
                "Invalid Request: method must be a string, got "
                + ("nothing" if "method" not in message else _json_type_name(method)),
            )
        # JSON-RPC 2.0 §5.1: when present, `params` MUST be a Structured
        # value (Object or Array). Treat absent as empty dict.
        params = message.get("params", {})
        if not isinstance(params, (dict, list)):
            return _error_response(
                req_id, JsonRpcCode.INVALID_REQUEST, "Invalid Request: params must be Object or Array"
            )
        # Array (positional) params are structurally valid JSON-RPC, but every
        # method here takes named arguments.
        if isinstance(params, list):
            return _error_response(
                req_id, JsonRpcCode.INVALID_PARAMS,
                "Invalid params: this server requires named parameters (Object), "
                "positional Array params are not supported",
            )

        handler = _METHODS.get(method)
        if handler is None:
            if is_notification:
                return None
            return _error_response(req_id, JsonRpcCode.METHOD_NOT_FOUND, f"Method not found: {method}")

        call = _Call(req_id, method, request_id, is_notification)
        try:
            result = handler(self, params)
        except Exception as e:  # noqa: BLE001 — reply_to_error logs what is not the client's, with its traceback
            return call.reply_to_error(e)
        if inspect.isawaitable(result):
            return call.reply_later(result)
        return call.reply(result)

    # ------------------------------------------------------------------
    # Method handlers
    # ------------------------------------------------------------------

    def _handle_initialize(self, params: dict) -> dict:
        from mipyre import __version__

        return {
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {"listChanged": False},
                "resources": {"subscribe": False, "listChanged": False},
                "prompts": {"listChanged": False},
            },
            "serverInfo": {"name": "mipyre-mcp", "version": __version__},
        }

    def _handle_tools_list(self, params: dict) -> dict:
        tools = [
            {"name": t.name, "description": t.description, "inputSchema": t.input_schema}
            for t in self._tools.values()
        ]
        return {"tools": tools}

    def _handle_tools_call(self, params: dict) -> Any:
        tool_name = _str_param(params, "name")
        tool = self._tools.get(tool_name)
        if tool is None:
            raise JsonRpcError(JsonRpcCode.INVALID_PARAMS, f"Unknown tool: {tool_name}")
        arguments = _arguments_object(params)
        missing = [r for r in tool.input_schema.get("required", []) if r not in arguments]
        if missing:
            raise JsonRpcError(JsonRpcCode.INVALID_PARAMS, f"missing required argument(s): {missing}")
        tool.params.check(arguments)
        _check_arguments(tool.input_schema, arguments)

        try:
            value = tool.handler(**arguments)
        except ToolError as e:
            return _tool_result(e.message, is_error=True)
        if inspect.isawaitable(value):
            return _tool_result_later(value)
        return _tool_result(_as_text(value), is_error=False)

    def _handle_resources_list(self, params: dict) -> dict:
        resources = [
            {"uri": r.uri, "name": r.name, "description": r.description, "mimeType": r.mime_type}
            for r in self._resources.values()
        ]
        return {"resources": resources}

    def _handle_resources_read(self, params: dict) -> Any:
        uri = _str_param(params, "uri")
        resource = self._resources.get(uri)
        if resource is None:
            raise JsonRpcError(JsonRpcCode.RESOURCE_NOT_FOUND, f"Resource not found: {uri}")

        return _then(resource.handler(), lambda value: {
            "contents": [{"uri": uri, "mimeType": resource.mime_type, "text": _as_text(value)}]
        })

    def _handle_prompts_list(self, params: dict) -> dict:
        prompts = [
            {"name": p.name, "description": p.description, "arguments": [a.as_json() for a in p.arguments]}
            for p in self._prompts.values()
        ]
        return {"prompts": prompts}

    def _handle_prompts_get(self, params: dict) -> Any:
        prompt_name = _str_param(params, "name")
        prompt = self._prompts.get(prompt_name)
        if prompt is None:
            raise JsonRpcError(JsonRpcCode.INVALID_PARAMS, f"Unknown prompt: {prompt_name}")
        arguments = _arguments_object(params)
        missing = [a.name for a in prompt.arguments if a.required and a.name not in arguments]
        if missing:
            raise JsonRpcError(JsonRpcCode.INVALID_PARAMS, f"missing required argument(s): {missing}")
        prompt.params.check(arguments)

        return _then(prompt.handler(**arguments), lambda value: {
            "description": prompt.description,
            "messages": [
                {"role": "user", "content": {"type": "text", "text": _as_text(value)}}
            ],
        })


# The server's methods by JSON-RPC name, as functions of the class: bound methods kept on an
# instance would make every server a reference cycle, freed only by the cyclic collector.
_METHODS: dict[str, Callable[[MCPServer, dict], Any]] = {
    "initialize": MCPServer._handle_initialize,
    "tools/list": MCPServer._handle_tools_list,
    "tools/call": MCPServer._handle_tools_call,
    "resources/list": MCPServer._handle_resources_list,
    "resources/read": MCPServer._handle_resources_read,
    "prompts/list": MCPServer._handle_prompts_list,
    "prompts/get": MCPServer._handle_prompts_get,
}


@dataclass(frozen=True)
class _Call:
    """One JSON-RPC call: its id, its method, whether it is a notification (which gets no
    reply) and the id of the HTTP request it came in."""

    req_id: Any
    method: str
    request_id: str | None
    is_notification: bool

    def reply(self, result: Any) -> dict | None:
        if self.is_notification:
            return None
        return {"jsonrpc": "2.0", "id": self.req_id, "result": result}

    def reply_to_error(self, error: Exception) -> dict | None:
        """A ``JsonRpcError`` is the client's and goes back as it is; anything else is logged
        with its traceback and the request id, and the client gets -32603 carrying that id."""
        if isinstance(error, JsonRpcError):
            code, message, data = error.code, error.message, None
        else:
            _log.error("MCP handler %r raised (request_id=%s)", self.method, self.request_id, exc_info=error)
            code, message = JsonRpcCode.INTERNAL_ERROR, "Internal error"
            data = None if self.request_id is None else {"request_id": self.request_id}
        if self.is_notification:
            return None
        return _error_response(self.req_id, code, message, data=data)

    async def reply_later(self, pending: Any) -> dict | None:
        # gather hands the handler's exception back as a value, so reply_to_error stays the
        # one place that decides what a failure is.
        import asyncio

        outcome: Any
        (outcome,) = await asyncio.gather(pending, return_exceptions=True)
        if isinstance(outcome, Exception):
            return self.reply_to_error(outcome)
        if isinstance(outcome, BaseException):
            raise outcome
        return self.reply(outcome)


def _tool_result(text: str, *, is_error: bool) -> dict:
    return {"content": [{"type": "text", "text": text}], "isError": is_error}


async def _tool_result_later(pending: Any) -> dict:
    try:
        value = await pending
    except ToolError as e:
        return _tool_result(e.message, is_error=True)
    return _tool_result(_as_text(value), is_error=False)


def _refuse_repeat(kind: str, key: str, registered: dict) -> None:
    if key in registered:
        raise ValueError(f"MCP {kind} {key!r} is already registered")


def _reply(reply: dict | None, status: int = 200) -> Response:
    """The HTTP response carrying a JSON-RPC reply; ``202`` with no body for none."""
    if reply is None:
        return Response(body=b"", status_code=202)
    return Response(body=encode_json(reply), status_code=status, content_type="application/json")


def _refusal(reason: str) -> dict:
    """The reply to an HTTP-level refusal (a body type ``/mcp`` does not take): a JSON-RPC
    Invalid Request error carrying the reason."""
    return _error_response(None, JsonRpcCode.INVALID_REQUEST, reason)


def _error_response(req_id: Any, code: JsonRpcCode, message: str, data: dict | None = None) -> dict:
    error: dict[str, Any] = {"code": int(code), "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": error}


def _is_json_rpc_id(value: object) -> bool:
    return value is None or isinstance(value, str) or (
        isinstance(value, (int, float)) and not isinstance(value, bool)
    )


def _str_param(params: dict, key: str) -> str:
    """``params[key]``, which must be a string: -32602 with the reason otherwise."""
    value = params.get(key)
    if not isinstance(value, str):
        got = "nothing" if key not in params else _json_type_name(value)
        raise JsonRpcError(
            JsonRpcCode.INVALID_PARAMS, f"Invalid params: {key!r} must be a string, got {got}"
        )
    return value


def _arguments_object(params: dict) -> dict:
    arguments = params.get("arguments", {})
    if not isinstance(arguments, dict):
        raise JsonRpcError(
            JsonRpcCode.INVALID_PARAMS,
            f"arguments must be an object, got {type(arguments).__name__}",
        )
    return arguments
