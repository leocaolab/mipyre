"""Maps Python type hints to the JSON Schema of the JSON values they stand for, and checks a
decoded JSON value against such a schema.

MCP tool arguments and OpenAPI response bodies both read their schemas from here, so a hint
means the same schema in both.
"""

from __future__ import annotations

import types
import typing
from typing import Any, Callable, Literal, Union

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


def json_type_name(value: object) -> str:
    return next(
        (name for name, check in _JSON_TYPE_CHECKS.items() if check(value)),
        type(value).__name__,
    )


class SchemaViolation(Exception):
    """A value its schema does not accept, with where and why."""


def check_value(schema: dict, value: object, where: str) -> None:
    """Checks ``value`` against ``schema``: ``type``, ``enum``, ``const``, ``anyOf``,
    ``items``, ``properties``, ``required`` and ``additionalProperties``. Other keywords
    (``format``, ``minimum``…) are annotations here and are not checked."""
    expected = schema.get("type")
    if expected is not None:
        names = expected if isinstance(expected, list) else [expected]
        if not any(_JSON_TYPE_CHECKS[n](value) for n in names):
            raise SchemaViolation(f"{where}: expected {' or '.join(names)}, got {json_type_name(value)}")
    if "enum" in schema and not any(_json_equal(value, option) for option in schema["enum"]):
        raise SchemaViolation(f"{where}: {value!r} is not one of {schema['enum']!r}")
    if "const" in schema and not _json_equal(value, schema["const"]):
        raise SchemaViolation(f"{where}: must be {schema['const']!r}, got {value!r}")
    if "anyOf" in schema:
        reasons = []
        for option in schema["anyOf"]:
            try:
                check_value(option, value, where)
                break
            except SchemaViolation as e:
                reasons.append(str(e))
        else:
            raise SchemaViolation(" / ".join(reasons))
    if isinstance(value, list) and "items" in schema:
        for i, item in enumerate(value):
            check_value(schema["items"], item, f"{where}[{i}]")
    if isinstance(value, dict):
        check_object(schema, value, where)


def check_object(schema: dict, value: dict, where: str) -> None:
    missing = [k for k in schema.get("required", ()) if k not in value]
    if missing:
        raise SchemaViolation(f"{where}: missing required key(s) {missing}")
    properties = schema.get("properties", {})
    extra = schema.get("additionalProperties", True)
    for key, item in value.items():
        if key in properties:
            check_value(properties[key], item, f"{where}.{key}")
        elif extra is False:
            raise SchemaViolation(f"{where}: unexpected key {key!r}")
        elif isinstance(extra, dict):
            check_value(extra, item, f"{where}.{key}")
