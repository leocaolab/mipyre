"""Maps Python type hints to the JSON Schema of the JSON values they stand for.

MCP tool arguments and OpenAPI response bodies both read their schemas from here, so a hint
means the same schema in both.
"""

from __future__ import annotations

import types
import typing
from typing import Any, Literal, Union

_SCALAR_TYPES = {bool: "boolean", int: "integer", float: "number", str: "string", type(None): "null"}


class NoJsonForm(Exception):
    """A type hint that no JSON value matches: a class, ``set[int]``, ``tuple``, ``dict[int, str]``…"""

    def __init__(self, hint: object) -> None:
        super().__init__(repr(hint))
        self.hint = hint


def hint_schema(hint: object) -> dict:
    """Return the JSON Schema ``hint`` stands for; raise ``NoJsonForm`` for a hint that has none."""
    if hint is Any:
        return {}
    if hint in _SCALAR_TYPES:
        return {"type": _SCALAR_TYPES[hint]}
    origin, args = typing.get_origin(hint), typing.get_args(hint)
    if hint is list or origin is list:
        return {"type": "array", **({"items": hint_schema(args[0])} if args else {})}
    if hint is dict or origin is dict:
        if not args:
            return {"type": "object"}
        if args[0] is not str:
            raise NoJsonForm(hint)
        return {"type": "object", "additionalProperties": hint_schema(args[1])}
    if origin in (Union, types.UnionType):
        return {"anyOf": [hint_schema(a) for a in args]}
    if origin is Literal:
        if not all(type(v) in _SCALAR_TYPES for v in args):
            raise NoJsonForm(hint)
        return {"enum": list(args)}
    raise NoJsonForm(hint)
