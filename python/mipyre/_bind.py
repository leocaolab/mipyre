"""Turns a route's handler into the callable the engine dispatches: path params, the
``model=`` body and the ``query=`` string bound to the parameters its signature declares,
and a 422 for a body or query that fails validation."""

from __future__ import annotations

import functools
import inspect
import json
import logging
from typing import Callable

from mipyre.engine import Response, _route_params


def bind_handler(fn: Callable, path: str, model: type | None, query: type | None = None) -> Callable:
    """The callable the engine dispatches for a route: ``fn`` itself when it
    takes only the request, else a wrapper that validates the body against
    ``model`` and the query string against ``query`` and injects the path params
    ``fn`` declares. Signature mistakes and a path the router would not take as
    written (``:name``) raise here, at registration, not on every request."""
    template = frozenset(_route_params(path))
    if query is not None:
        return _bind_query(fn, path, template, model, query)
    if model is None:
        return _bind_path_params(fn, path, template)
    return _bind_model(fn, path, template, model)


def _bind_path_params(fn: Callable, path: str, template: frozenset[str]) -> Callable:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return fn  # a builtin/C callable: nothing to inject
    names = _path_param_names(fn, sig, path, template, leading=1)
    if not names:
        return fn  # hot path — the handler is registered as is
    _require_accepts(fn, leading=1, names=names)

    # Every name is in the template, so the router always fills it: `p[n]`, never a
    # silent None.
    if inspect.iscoroutinefunction(fn):
        async def bound_async(req):
            p = req.params
            return await fn(req, **{n: p[n] for n in names})
        return _named_like(bound_async, fn)

    def bound(req):
        p = req.params
        return fn(req, **{n: p[n] for n in names})
    return _named_like(bound, fn)


def _bind_model(fn: Callable, path: str, template: frozenset[str], model: type) -> Callable:
    """``fn(req, body, **path_params)`` or ``fn(body, **path_params)``, by the rule
    ``_takes_request`` checks at registration."""
    # Imported here, only for routes that declare model=: importing pydantic at
    # module level would load pydantic_core in every worker of every app. If it
    # can't be imported, route registration fails with the ImportError.
    from pydantic import BaseModel, ValidationError

    if not (isinstance(model, type) and issubclass(model, BaseModel)):
        raise TypeError(f"model= must be a pydantic BaseModel subclass, got {model!r}")
    sig = inspect.signature(fn)
    takes_request = _takes_request(fn, sig, template, (("body", model),))
    names = _path_param_names(fn, sig, path, template, leading=2 if takes_request else 1)
    if names:
        _require_accepts(fn, leading=2 if takes_request else 1, names=names)

    def args(req, body):
        return (req, body) if takes_request else (body,)

    def kwargs(req):
        p = req.params
        return {n: p[n] for n in names}

    if inspect.iscoroutinefunction(fn):
        async def bound_async(req):
            try:
                body = model.model_validate_json(req.body)
            except ValidationError as e:
                return _validation_error_response(e)
            return await fn(*args(req, body), **kwargs(req))
        return _named_like(bound_async, fn)

    def bound(req):
        try:
            body = model.model_validate_json(req.body)
        except ValidationError as e:
            return _validation_error_response(e)
        return fn(*args(req, body), **kwargs(req))
    return _named_like(bound, fn)


def _bind_query(
    fn: Callable, path: str, template: frozenset[str], model: type | None, query: type
) -> Callable:
    """``fn(req, body, query, **path_params)`` or without ``req``, and without ``body`` when
    the route has no ``model=``; by the rule ``_takes_request`` checks at registration."""
    # Imported here, only for routes that declare query= (as for model=).
    from pydantic import BaseModel, ValidationError

    from mipyre._query import QueryFields

    if model is not None and not (isinstance(model, type) and issubclass(model, BaseModel)):
        raise TypeError(f"model= must be a pydantic BaseModel subclass, got {model!r}")
    if not (isinstance(query, type) and issubclass(query, BaseModel)):
        raise TypeError(f"query= must be a pydantic BaseModel subclass, got {query!r}")
    fields = QueryFields.of(query)
    sig = inspect.signature(fn)
    declared = ((("body", model),) if model is not None else ()) + (("query", query),)
    takes_request = _takes_request(fn, sig, template, declared)
    leading = len(declared) + takes_request
    names = _path_param_names(fn, sig, path, template, leading=leading)
    if names:
        _require_accepts(fn, leading=leading, names=names)

    def call_args(req) -> tuple[list, list[dict]]:
        """The handler's positional arguments, or the errors that answer 422 instead."""
        args: list = [req] if takes_request else []
        detail: list[dict] = []
        if model is not None:
            try:
                args.append(model.model_validate_json(req.body))
            except ValidationError as e:
                detail.extend(_validation_errors(e, "body"))
        given, repeated = fields.input(req.query_params_all())
        if repeated:
            # The repeated key was left out of `given`; validating would add "missing" for it.
            detail.extend(repeated)
            return args, detail
        try:
            args.append(query.model_validate(given))
        except ValidationError as e:
            detail.extend(_validation_errors(e, "query", loc=("query",)))
        return args, detail

    def kwargs(req):
        p = req.params
        return {n: p[n] for n in names}

    if inspect.iscoroutinefunction(fn):
        async def bound_async(req):
            args, detail = call_args(req)
            if detail:
                return _unprocessable(detail)
            return await fn(*args, **kwargs(req))
        return _named_like(bound_async, fn)

    def bound(req):
        args, detail = call_args(req)
        if detail:
            return _unprocessable(detail)
        return fn(*args, **kwargs(req))
    return _named_like(bound, fn)


def _takes_request(
    fn: Callable,
    sig: inspect.Signature,
    template: frozenset[str],
    declared: tuple[tuple[str, type], ...],
) -> bool:
    """The rule for a route that validates values for its handler (``declared``: the body
    for ``model=``, then the query for ``query=``): the handler's leading positional
    parameters that are not path params are the request and those values, in that order,
    or the values alone; every other parameter names a path param. Whether it takes the
    request follows from that count, never from a guess. A signature the rule rejects raises
    here: a parameter missing for a value, or a parameter annotated as a declared class in
    the request's place. (More than one extra is left to the path-param check, which names
    them.)"""
    leading = []
    for param in sig.parameters.values():
        positional = param.kind in (
            inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD
        )
        if not positional or param.name in template:
            break
        leading.append(param)
    values = ", ".join(what for what, _ in declared)
    with_ = " and ".join(f"{'model' if what == 'body' else what}=" for what, _ in declared)
    forms = f"fn(req, {values}, **path_params) or fn({values}, **path_params)"
    if len(leading) < len(declared):
        what, cls = declared[len(leading)]
        raise TypeError(
            f"handler {fn.__name__!r} has no parameter for the validated "
            f"{cls.__name__} {what}: with {with_}, a handler is {forms}"
        )
    takes_request = len(leading) > len(declared)
    if takes_request:
        for _, cls in declared:
            if _annotated_as(leading[0], cls):
                raise TypeError(
                    f"handler {fn.__name__!r}: parameter {leading[0].name!r} is annotated "
                    f"{cls.__name__} but stands where the request goes (with {with_}, a handler "
                    f"is {forms}; is {leading[-1].name!r} a path param missing from the URL "
                    "template?)"
                )
    return takes_request


def _annotated_as(param: inspect.Parameter, cls: type) -> bool:
    """Whether ``param`` is annotated as ``cls``: the class itself, or its name as a string
    (``from __future__ import annotations``)."""
    return param.annotation is cls or param.annotation in (cls.__name__, cls.__qualname__)


def _validation_error_response(e) -> Response:
    logging.getLogger("mipyre.validation").warning(
        "request body validation failed: %s", type(e).__name__, exc_info=True
    )
    return Response(
        body=json.dumps({"detail": e.errors(include_url=False, include_input=False)}),
        status_code=422,
        content_type="application/json",
    )


def _validation_errors(e, what: str, loc: tuple[str, ...] = ()) -> list[dict]:
    """pydantic's errors for one part of the request, each ``loc`` led by ``loc``."""
    logging.getLogger("mipyre.validation").warning(
        "request %s validation failed: %s", what, type(e).__name__, exc_info=True
    )
    errors = e.errors(include_url=False, include_input=False)
    if loc:
        for error in errors:
            error["loc"] = (*loc, *error["loc"])
    return errors


def _unprocessable(detail: list[dict]) -> Response:
    """The 422 of a ``model=`` route, with the errors of every part that failed."""
    return Response(
        body=json.dumps({"detail": detail}),
        status_code=422,
        content_type="application/json",
    )


def _path_param_names(
    fn: Callable, sig: inspect.Signature, path: str, template: frozenset[str], leading: int
) -> tuple[str, ...]:
    """The parameters after the ``leading`` ones the dispatcher fills (request,
    body), each of which must name a param in the URL template."""
    names = tuple(
        p.name for p in list(sig.parameters.values())[leading:]
        if p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    )
    missing = [n for n in names if n not in template]
    if missing:
        raise ValueError(
            f"handler {fn.__name__!r} declares parameter(s) {missing!r} "
            f"that are not in the URL template {path!r}. Path-param "
            f"injection only fills names that appear as `{{name}}` "
            f"or `{{*name}}` in the route path."
        )
    return names


def _require_accepts(fn: Callable, leading: int, names: tuple[str, ...]) -> None:
    """The path params are read off ``inspect.signature(fn)``, which follows a decorator's
    ``__wrapped__`` to the function it wraps. The call goes to ``fn`` itself, so it must take
    them too: a wrapper that doesn't (``def wrapper(req)``) is a registration error here,
    not a TypeError on every request."""
    try:
        own = inspect.signature(fn, follow_wrapped=False)
    except (TypeError, ValueError):
        return  # a builtin/C callable: its signature can't be read
    try:
        own.bind(*([None] * leading), **dict.fromkeys(names))
    except TypeError as e:
        raise TypeError(
            f"handler {getattr(fn, '__qualname__', fn)!r} declares path param(s) "
            f"{list(names)!r} through __wrapped__, but the wrapper the route calls has "
            f"signature {own} and can't take them ({e}); have the wrapper accept and pass "
            "them on (e.g. **path_params)"
        ) from None


def _named_like(wrapper: Callable, fn: Callable) -> Callable:
    # Only the names, not fn's __doc__ / __dict__; __wrapped__ lets inspect.signature and
    # static analyzers see fn's own signature.
    return functools.update_wrapper(wrapper, fn, assigned=("__name__", "__qualname__"), updated=())

