"""Turns a route's handler into the callable the engine dispatches: path params, the
``body=`` model and the ``query=`` string bound to the parameters its signature declares,
and a 422 for a body or query that fails validation."""

from __future__ import annotations

import functools
import inspect
from typing import Callable

from mipyre.engine import Response, _is_async_handler, _route_params

DETAIL_LIMIT = 20


def bind_handler(fn: Callable, path: str, model: type | None, query: type | None = None) -> Callable:
    """Return the callable the engine dispatches for a route: ``fn`` itself when it takes
    only the request, else a wrapper that injects the path params ``fn`` declares and, with
    ``model`` / ``query``, validates the body and the query string first (a 422 when they do
    not validate). Signature mistakes and a path the router would not take as written
    (``:name``) raise here, at registration, not on every request.

    The wrapper is a plain function that returns what ``fn`` returns: for an async ``fn``,
    its coroutine, which the engine awaits, so a request pays for no coroutine of the
    wrapper's own. It is marked a coroutine function exactly when the engine takes ``fn``
    for one (`_is_async_handler`), so its route goes to the same workers ``fn``'s would."""
    template = frozenset(_route_params(path))
    if model is None and query is None:
        return _bind_path_params(fn, path, template)
    return _bind_validated(fn, path, template, model, query)


def _bind_path_params(fn: Callable, path: str, template: frozenset[str]) -> Callable:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return fn  # a builtin/C callable: nothing to inject
    names = _path_param_names(fn, sig, path, template, leading=1)
    if not names:
        return fn  # hot path — the handler is registered as is
    _require_accepts(fn, leading=1, names=names)

    if _takes_every_param(names, template):
        def bound(req):
            return fn(req, **req.params)
    else:
        def bound(req):
            p = req.params
            return fn(req, **{n: p[n] for n in names})
    return _wrapping(bound, fn, _is_async_handler(fn))


def _bind_validated(
    fn: Callable, path: str, template: frozenset[str], model: type | None, query: type | None
) -> Callable:
    """Bind ``fn(req, body, query, **path_params)``, or the same without ``req``, without
    ``body`` when the route has no ``body=`` model and without ``query`` when it has no
    ``query=``, by the rule ``_takes_request`` checks at registration."""
    # Imported here, only for routes that declare a body= model or query=: importing pydantic at
    # module level would load pydantic_core in every worker of every app. If it can't be
    # imported, route registration fails with the ImportError.
    from pydantic import BaseModel, ValidationError

    from mipyre._query import QueryFields

    if model is not None and not (isinstance(model, type) and issubclass(model, BaseModel)):
        raise TypeError(f"body= must be a pydantic BaseModel subclass or BodyStream, got {model!r}")
    if query is not None and not (isinstance(query, type) and issubclass(query, BaseModel)):
        raise TypeError(f"query= must be a pydantic BaseModel subclass, got {query!r}")
    query_input = QueryFields.of(query).input if query is not None else None
    sig = inspect.signature(fn)
    declared = ((("body", model),) if model is not None else ()) + (
        (("query", query),) if query is not None else ()
    )
    takes_request = _takes_request(fn, sig, template, declared)
    leading = len(declared) + takes_request
    names = _path_param_names(fn, sig, path, template, leading=leading)
    if names:
        _require_accepts(fn, leading=leading, names=names)
    every_param = _takes_every_param(names, template)
    is_async = _is_async_handler(fn)
    unprocessable = _unprocessable_awaitable if is_async else _unprocessable
    # Looked up once: a classmethod read off the class makes a new bound method each time.
    validate_body = model.model_validate_json if model is not None else None
    validate_query = query.model_validate if query is not None else None

    def bound(req):
        args = (req,) if takes_request else ()
        errors = None
        if validate_body is not None:
            try:
                args += (validate_body(req.body),)
            except ValidationError as e:
                errors = _errors(e)
        if query_input is not None:
            given, repeated = query_input(req.query_params_all())
            if repeated:
                # The repeated key was left out of `given`; validating would add "missing" for it.
                errors = (errors or []) + repeated
            else:
                try:
                    args += (validate_query(given),)
                except ValidationError as e:
                    errors = (errors or []) + _errors(e, ("query",))
        if errors is not None:
            return unprocessable(errors)
        if not names:
            return fn(*args)
        if every_param:
            return fn(*args, **req.params)
        p = req.params
        return fn(*args, **{n: p[n] for n in names})
    return _wrapping(bound, fn, is_async)


def _takes_request(
    fn: Callable,
    sig: inspect.Signature,
    template: frozenset[str],
    declared: tuple[tuple[str, type], ...],
) -> bool:
    """The rule for a route that validates values for its handler (``declared``: the body
    for ``body=``, then the query for ``query=``): the handler's leading positional
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
    with_ = " and ".join(f"{what}=" for what, _ in declared)
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


def _errors(e, loc: tuple[str, ...] = ()) -> list[dict]:
    """Return pydantic's errors for one part of the request, each ``loc`` led by ``loc``."""
    errors = e.errors(include_url=False, include_input=False)
    if loc:
        for error in errors:
            error["loc"] = (*loc, *error["loc"])
    return errors


def _unprocessable(errors: list[dict]) -> Response:
    """Return the 422 for a request that failed validation: the first ``DETAIL_LIMIT``
    errors and how many there were, so a body that fails in a million places costs a
    bounded answer. A validator's exception in an error's ``ctx`` goes out as its text, as in
    pydantic's own JSON.

    The client decides how many of these there are, so the server counts each in the run
    that served it (``mipyre_validation_failures_total``) and logs at most one line a minute
    for that run, saying how many were not logged since the last one."""
    from pydantic_core import to_json

    body = to_json({"detail": errors[:DETAIL_LIMIT], "error_count": len(errors)}, fallback=str)
    return Response._validation_failure(body, len(errors))


async def _unprocessable_awaitable(errors: list[dict]) -> Response:
    """Return the 422 from a coroutine, for the wrapper of an async handler: whoever calls a
    coroutine function awaits what it returns, so the 422 must be awaitable too."""
    return _unprocessable(errors)


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


def _takes_every_param(names: tuple[str, ...], template: frozenset[str]) -> bool:
    """Return whether the handler takes every param of the URL template, so the router's
    params can be handed over as they are. Every name is in the template (checked at
    registration), and the router fills every one of them: `p[n]` is never a silent None."""
    return len(names) == len(template)


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


def _wrapping(wrapper: Callable, fn: Callable, is_async: bool) -> Callable:
    # Only the names, not fn's __doc__ / __dict__; __wrapped__ lets inspect.signature and
    # static analyzers see fn's own signature.
    functools.update_wrapper(wrapper, fn, assigned=("__name__", "__qualname__"), updated=())
    if is_async:
        inspect.markcoroutinefunction(wrapper)
    return wrapper
