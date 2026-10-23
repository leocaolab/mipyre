"""The OpenAPI 3.1 document that describes an app's routes: :class:`OpenApi` serves it.

The document is built on the first request for it, from the routes the app serves by then
(a running server takes no new ones), and every request after gets the same bytes.

The document says only what the code declares. A route's ``body=`` model is the request body and
its ``query=`` model's fields are the query parameters; its return annotation is the response body
when the engine's encoding of that type is known (``str`` is text, ``bytes`` is binary, ``dict`` /
``list`` are JSON). Any other handler gets a response with no ``content``, which claims nothing
about the body, rather than a guessed schema.
"""

from __future__ import annotations

import http
import inspect
import json
import re
import typing
from typing import TYPE_CHECKING, Callable, Sequence

from mipyre._bind import DETAIL_LIMIT
from mipyre._json_schema import NoJsonForm, hint_schema
from mipyre.engine import BodyStream, Response, _route_params

if TYPE_CHECKING:
    from mipyre import FastRoute, Mipyre, Request, Route

OPENAPI_VERSION = "3.1.0"

# OpenAPI 3.1 has a key for these methods only; a route on any other method has no place in it.
_OPERATION_METHODS = frozenset({"get", "put", "post", "delete", "options", "head", "patch", "trace"})

# OpenAPI's path templating has no catch-all: a `{*rest}` segment is written `{rest}`.
_CATCH_ALL = re.compile(r"\{\*([^}]+)\}")

_TEXT = "text/plain; charset=utf-8"

# What a `body=` model / `query=` route answers when the body or query does not validate.
_VALIDATION_FAILED_CONTENT = {
    "application/json": {
        "schema": {
            "type": "object",
            "properties": {
                "detail": {"type": "array", "items": {"type": "object"}, "maxItems": DETAIL_LIMIT},
                "error_count": {"type": "integer", "minimum": 1},
            },
            "required": ["detail", "error_count"],
        }
    }
}
_VALIDATION_FAILED = {
    (True, False): "The body is not JSON the model accepts; `detail` lists pydantic's first errors "
                   "and `error_count` how many there were.",
    (False, True): "The query does not validate; `detail` lists the first errors, each `loc` led by "
                   "`query`, and `error_count` how many there were.",
    (True, True): "The body or the query does not validate; `detail` lists the first errors of both "
                  "(a query error's `loc` is led by `query`) and `error_count` how many there were.",
}


class OpenApi:
    """Serve ``app``'s OpenAPI 3.1 document at ``GET path``::

        from mipyre import Mipyre, OpenApi

        app = Mipyre()
        docs = OpenApi(app, title="Shop", version="1.4.0")   # GET /openapi.json

    ``title`` and ``version`` are the document's ``info``. The document describes every route
    of the app but those declared ``in_openapi=False`` (its own is one); it is built on the
    first ``GET``, from the routes the app serves then, and that request and every later one
    get the same bytes. The route runs on the main interpreter and goes through the app's
    ``before_request`` hooks like any other: the document tells anyone who can reach it every
    route and body schema, so an app that serves one says so by making this object.

    :raises ValueError: ``path`` has a path parameter.
    """

    def __init__(self, app: Mipyre, *, title: str, version: str, path: str = "/openapi.json") -> None:
        if _route_params(path):
            raise ValueError(f"OpenApi: path {path!r} has a path parameter; the document has one fixed path")
        self._app = app
        self._title = title
        self._version = version
        self._served: bytes | None = None
        app.get(path, self._serve, runtime="main", in_openapi=False)

    def document(self) -> dict:
        """Return the document of the routes ``app`` has now, as a dict: for a test or a build
        step that hands it to a tool (Schemathesis, a client generator) without serving the app."""
        return document(
            self._title, self._version, [r for r in self._app.routes if r.in_openapi], self._app.fast_routes
        )

    def _serve(self, req: Request) -> Response:
        # Two first requests at once each build the same bytes; either may be kept.
        if self._served is None:
            self._served = json.dumps(self.document()).encode()
        return Response(body=self._served, content_type="application/json")


def _body_model(route: Route) -> type | None:
    """The pydantic model ``route`` validates its body with; None for a streamed or plain body."""
    return None if route.body is BodyStream else route.body


def document(
    title: str,
    version: str,
    routes: Sequence[Route],
    fast_routes: Sequence[FastRoute],
) -> dict:
    """Return the OpenAPI 3.1 document of ``routes`` and ``fast_routes``.

    A fast route answers before the router does, so on the same method and path it is the one
    documented.
    """
    bodies = [m for m in map(_body_model, routes) if m is not None]
    queries = [r.query for r in routes if r.query is not None]
    schemas, components = _model_schemas(bodies + queries)
    query_params = {q: _query_parameters(schemas[q], components) for q in queries}
    _drop_unreferenced(components, {schemas[q]["$ref"] for q in queries} - {schemas[b]["$ref"] for b in bodies})

    paths: dict[str, dict] = {}
    for route in routes:
        method = route.method.lower()
        if method in _OPERATION_METHODS:
            model = _body_model(route)
            body = schemas[model] if model is not None else None
            query = query_params[route.query] if route.query is not None else []
            paths.setdefault(_openapi_path(route.path), {})[method] = _operation(route, body, query)
    for fast in fast_routes:
        method = fast.method.lower()
        if method in _OPERATION_METHODS:
            paths.setdefault(fast.path, {})[method] = _fast_operation(fast)

    doc: dict = {"openapi": OPENAPI_VERSION, "info": {"title": title, "version": version}, "paths": paths}
    if components:
        doc["components"] = {"schemas": components}
    return doc


def _model_schemas(models: list[type]) -> tuple[dict[type, dict], dict[str, dict]]:
    """Return each model's request-body schema and the shared definitions they refer to."""
    distinct = list(dict.fromkeys(models))
    if not distinct:
        return {}, {}
    # Imported only for an app with `body=` model routes, which has already imported pydantic.
    from pydantic.json_schema import models_json_schema

    refs, top = models_json_schema(
        [(m, "validation") for m in distinct], ref_template="#/components/schemas/{model}"
    )
    return {m: refs[(m, "validation")] for m in distinct}, top.get("$defs", {})


def _query_parameters(ref: dict, components: dict[str, dict]) -> list[dict]:
    """The query parameters of a ``query=`` model: one per field, by the key the query string
    carries it under, ``required`` and ``schema`` as pydantic gives them. A list field is one
    parameter whose key repeats, OpenAPI's default for a query array (``style: form``,
    ``explode: true``)."""
    model = components[_component_name(ref["$ref"])]
    required = set(model.get("required", ()))
    return [
        {"name": key, "in": "query", "required": key in required, "schema": schema}
        for key, schema in model.get("properties", {}).items()
    ]


def _component_name(ref: str) -> str:
    """The name in ``components`` of the schema a ``#/components/schemas/<name>`` ref names."""
    return ref.rsplit("/", 1)[1]


def _drop_unreferenced(components: dict[str, dict], refs: set[str]) -> None:
    """Remove each of ``refs`` (a ``query=`` model's own schema, spread into parameters) that
    no other schema refers to."""
    for ref in refs:
        name = _component_name(ref)
        others = json.dumps({k: v for k, v in components.items() if k != name})
        if f'"{ref}"' not in others:
            del components[name]


def _openapi_path(path: str) -> str:
    return _CATCH_ALL.sub(r"{\1}", path)


def _operation(route: Route, body_schema: dict | None, query_params: list[dict]) -> dict:
    op: dict = {}
    description = inspect.getdoc(route.handler)
    if description:
        op["description"] = description
    # The router hands every path param to the handler as the text it matched.
    params = [
        {"name": n, "in": "path", "required": True, "schema": {"type": "string"}}
        for n in _route_params(route.path)
    ] + query_params
    if params:
        op["parameters"] = params
    if body_schema is not None:
        op["requestBody"] = {"required": True, "content": {"application/json": {"schema": body_schema}}}
    op["responses"] = {"200": _success(route.handler)}
    validated = (body_schema is not None, route.query is not None)
    if any(validated):
        op["responses"]["422"] = {
            "description": _VALIDATION_FAILED[validated], "content": _VALIDATION_FAILED_CONTENT
        }
    return op


def _success(handler: Callable) -> dict:
    response: dict = {"description": _reason(200)}
    content = _content_of(_return_hint(handler))
    if content is not None:
        response["content"] = content
    return response


def _return_hint(handler: Callable) -> object | None:
    """Return the handler's return annotation, or None when it has none that resolves."""
    # A class-based view's annotations are on its `__call__`.
    annotated = handler if inspect.isroutine(handler) else type(handler).__call__
    try:
        hints = typing.get_type_hints(annotated)
    except NameError:
        # A forward reference to a name imported only under TYPE_CHECKING.
        return None
    return hints.get("return")


def _content_of(hint: object | None) -> dict | None:
    """Return the response ``content`` a handler annotated ``hint`` sends, or None when the
    body's media type does not follow from the annotation."""
    if hint is str:
        return {_TEXT: {"schema": {"type": "string"}}}
    if hint in (bytes, bytearray):
        return {"application/octet-stream": {}}
    if hint in (dict, list) or typing.get_origin(hint) in (dict, list):
        try:
            return {"application/json": {"schema": hint_schema(hint)}}
        except NoJsonForm:
            # A dict or list of values with no JSON Schema form: JSON, of an undescribed shape.
            return {"application/json": {}}
    return None


def _fast_operation(fast: FastRoute) -> dict:
    return {
        "responses": {
            str(fast.status_code): {
                "description": _reason(fast.status_code),
                "content": {fast.content_type: {}},
            }
        }
    }


def _reason(status: int) -> str:
    try:
        return http.HTTPStatus(status).phrase
    except ValueError:
        # A status code with no registered phrase (fast routes take any code from 100 to 999).
        return f"Status {status}"
