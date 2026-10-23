"""A ``query=`` model's fields as a query string carries them.

A query string is flat: each key carries text, and a key may repeat. A field annotated as a
sequence (``list[int]``, ``tuple[str, ...]``, ``set[str]``, optional or not) takes every value of
its key, in order: ``?tag=a&tag=b`` is ``["a", "b"]``. Any other field takes one value, and a key
given twice for it is an error, never a silent pick of the first or the last. A field whose
annotation has no flat form (a model, a dict, a list of lists) is refused when the route is
declared.
"""

from __future__ import annotations

import collections.abc
import dataclasses
import types
import typing
from typing import TYPE_CHECKING, Mapping

from mipyre.engine import DeclarationError

if TYPE_CHECKING:
    from pydantic import BaseModel

# Annotations whose values a repeated key fills, one item per value.
_SEQUENCES = frozenset({
    list, tuple, set, frozenset,
    collections.abc.Sequence, collections.abc.MutableSequence,
    collections.abc.Set, collections.abc.MutableSet,
    collections.abc.Collection, collections.abc.Iterable,
})
_MAPPINGS = frozenset({dict, collections.abc.Mapping, collections.abc.MutableMapping})


class QueryDeclarationError(DeclarationError):
    """A ``query=`` model with a field a query string can't carry."""


@dataclasses.dataclass(frozen=True)
class QueryFields:
    """Which keys of a ``query=`` model take every value (``sequences``) and which take one
    (``scalars``). A key the model does not name goes to it as given, for its ``extra`` setting
    to judge."""

    sequences: frozenset[str]
    scalars: frozenset[str]

    @classmethod
    def of(cls, model: type[BaseModel]) -> QueryFields:
        """Raises ``QueryDeclarationError`` for a field with no flat form."""
        sequences: set[str] = set()
        scalars: set[str] = set()
        for name, field in model.model_fields.items():
            if field.validation_alias is not None and not isinstance(field.validation_alias, str):
                raise QueryDeclarationError(
                    f"query={model.__name__}: field {name!r} has validation_alias="
                    f"{field.validation_alias!r}; a query key is one name, so give it a plain "
                    "alias (alias='...')"
                )
            keys = {name, field.alias or name}
            if isinstance(field.validation_alias, str):
                keys.add(field.validation_alias)
            try:
                takes_every_value = _is_sequence(field.annotation)
            except _NoFlatForm as e:
                raise QueryDeclarationError(
                    f"query={model.__name__}: field {name!r} is {e.annotation!r}, which a query "
                    "string can't carry: a key holds text, repeated for a list. Take it in the "
                    "body (body=) instead"
                ) from None
            (sequences if takes_every_value else scalars).update(keys)
        return cls(frozenset(sequences), frozenset(scalars))

    def input(self, params: Mapping[str, list[str]]) -> tuple[dict[str, object], list[dict]]:
        """The values to validate, from every value of each key (``req.query_params_all()``),
        and an error for each one-value field whose key came more than once."""
        values: dict[str, object] = {}
        errors: list[dict] = []
        for key, given in params.items():
            if key in self.sequences:
                values[key] = given
            elif len(given) == 1:
                values[key] = given[0]
            elif key in self.scalars:
                errors.append({
                    "type": "multiple_values",
                    "loc": ["query", key],
                    "msg": f"Expected one value, got {len(given)}; a field annotated as a list "
                           "takes a repeated key",
                })
            else:
                values[key] = given
        return values, errors


class _NoFlatForm(Exception):
    def __init__(self, annotation: object) -> None:
        self.annotation = annotation


def _is_sequence(annotation: object) -> bool:
    """Whether a field annotated ``annotation`` takes every value of its key; raises
    ``_NoFlatForm`` when it can take none."""
    origin = typing.get_origin(annotation)
    if origin is typing.Annotated:
        return _is_sequence(typing.get_args(annotation)[0])
    if origin in (typing.Union, types.UnionType):
        arms = [a for a in typing.get_args(annotation) if a is not type(None)]
        shapes = {_is_sequence(a) for a in arms}
        if len(shapes) > 1:
            # `list[str] | str`: whether a single value is a list of one is the model's guess.
            raise _NoFlatForm(annotation)
        return shapes.pop()
    if origin in _SEQUENCES or annotation in _SEQUENCES:
        for item in typing.get_args(annotation):
            if item is not Ellipsis and _is_sequence(item):
                raise _NoFlatForm(annotation)
        return True
    if origin in _MAPPINGS or annotation in _MAPPINGS or _is_model(annotation):
        raise _NoFlatForm(annotation)
    return False


def _is_model(annotation: object) -> bool:
    # A pydantic model or dataclass is a nested object: no flat form.
    return isinstance(annotation, type) and (
        hasattr(annotation, "model_fields") or dataclasses.is_dataclass(annotation)
    )
