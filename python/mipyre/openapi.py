"""Builds the OpenAPI 3.1 document that describes an app's routes.

``app.enable_openapi()`` builds it once, when the first server starts, and serves the bytes;
nothing here runs per request.

The document says only what the code declares. A handler's ``model=`` is the request body and
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

from mipyre._json_schema import NoJsonForm, hint_schema
from mipyre.engine import _route_params

if TYPE_CHECKING:
    from mipyre.app import FastRouteInfo, RouteDeclaration

OPENAPI_VERSION = "3.1.0"

# OpenAPI 3.1 has a key for these methods only; a route on any other method has no place in it.
_OPERATION_METHODS = frozenset({"get", "put", "post", "delete", "options", "head", "patch", "trace"})

# OpenAPI's path templating has no catch-all: a `{*rest}` segment is written `{rest}`.
_CATCH_ALL = re.compile(r"\{\*([^}]+)\}")

_TEXT = "text/plain; charset=utf-8"

# What a `model=` / `query=` route answers when the body or query does not validate.
_VALIDATION_FAILED_CONTENT = {
    "application/json": {
        "schema": {
            "type": "object",
            "properties": {"detail": {"type": "array", "items": {"type": "object"}}},
            "required": ["detail"],
        }
    }
}
_VALIDATION_FAILED = {
    (True, False): "The body is not JSON the model accepts; `detail` lists pydantic's errors.",
    (False, True): "The query does not validate; `detail` lists the errors, each `loc` led by `query`.",
    (True, True): "The body or the query does not validate; `detail` lists the errors of both "
                  "(a query error's `loc` is led by `query`).",
}


def document(
    title: str,
    version: str,
    routes: Sequence[RouteDeclaration],
    fast_routes: Sequence[FastRouteInfo],
) -> dict:
    """Return the OpenAPI 3.1 document of ``routes`` and ``fast_routes``.

    A fast route answers before the router does, so on the same method and path it is the one
    documented.
    """
    bodies = [r.model for r in routes if r.model is not None]
    queries = [r.query for r in routes if r.query is not None]
    schemas, components = _model_schemas(bodies + queries)
    query_params = {q: _query_parameters(schemas[q], components) for q in queries}
    _drop_unreferenced(components, {schemas[q]["$ref"] for q in queries} - {schemas[b]["$ref"] for b in bodies})

    paths: dict[str, dict] = {}
    for route in routes:
        method = route.method.lower()
        if method in _OPERATION_METHODS:
            body = schemas[route.model] if route.model is not None else None
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
    # Imported only for an app with `model=` routes, which has already imported pydantic.
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
    model = components[_component_name(ref)]
    required = set(model.get("required", ()))
    return [
        {"name": key, "in": "query", "required": key in required, "schema": schema}
        for key, schema in model.get("properties", {}).items()
    ]


def _component_name(ref: dict) -> str:
    return ref["$ref"].rsplit("/", 1)[1]


def _drop_unreferenced(components: dict[str, dict], refs: set[str]) -> None:
    """Remove each of ``refs`` (a ``query=`` model's own schema, spread into parameters) that
    no other schema refers to."""
    for ref in refs:
        name = ref.rsplit("/", 1)[1]
        others = json.dumps({k: v for k, v in components.items() if k != name})
        if f'"{ref}"' not in others:
            del components[name]


def _openapi_path(path: str) -> str:
    return _CATCH_ALL.sub(r"{\1}", path)


def _operation(route: RouteDeclaration, body_schema: dict | None, query_params: list[dict]) -> dict:
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


def _fast_operation(fast: FastRouteInfo) -> dict:
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
