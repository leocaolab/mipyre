"""`query=` validates the query string against a pydantic model and hands the handler the
instance; it fails like `body=` does, with a 422 whose `detail` lists the errors."""

from __future__ import annotations

import dataclasses
import enum
import operator
import types
from collections.abc import Sequence
from typing import Annotated, Any, Literal

import pydantic
import pytest
from hypothesis import given
from hypothesis import strategies as st

from mipyre import Mipyre, Router
from mipyre._query import QueryDeclarationError, QueryFields
from mipyre import RuntimeDeclarationError
from mipyre.testing import TestClient


class Search(pydantic.BaseModel):
    q: str
    page: int = 1
    tag: list[str] = []
    kind: Literal["book", "film"] | None = None


class Item(pydantic.BaseModel):
    name: str


# --- which keys take every value (pure) ---------------------------------------------------------


class Shapes(pydantic.BaseModel):
    one: str = ""
    many: list[int] = []
    maybe_many: list[int] | None = None
    tuple_many: tuple[str, ...] = ()
    set_many: set[str] = set()
    seq_many: Sequence[str] = ()
    annotated_many: Annotated[list[int], pydantic.Field(max_length=3)] = []
    aliased: list[str] = pydantic.Field([], alias="a")


def test_a_field_annotated_as_a_sequence_takes_every_value_of_its_key_and_any_other_one():
    fields = QueryFields.of(Shapes)

    assert fields.sequences == {"many", "maybe_many", "tuple_many", "set_many", "seq_many",
                                "annotated_many", "aliased", "a"}
    assert fields.scalars == {"one"}


def test_input_hands_a_sequence_key_every_value_and_a_scalar_key_its_one():
    given, repeated = QueryFields.of(Shapes).input({"one": ["x"], "many": ["1", "2"], "a": ["z"]})

    assert given == {"one": "x", "many": ["1", "2"], "a": ["z"]}
    assert repeated == []


def test_a_scalar_key_given_twice_is_an_error_not_a_pick():
    given, repeated = QueryFields.of(Shapes).input({"one": ["x", "y"]})

    assert given == {}
    assert repeated == [{"type": "multiple_values", "loc": ["query", "one"],
                         "msg": "Expected one value, got 2; a field annotated as a list takes a repeated key"}]


def test_a_key_the_model_does_not_name_goes_to_it_as_given():
    assert QueryFields.of(Shapes).input({"x": ["1"], "y": ["1", "2"]}) == ({"x": "1", "y": ["1", "2"]}, [])


class Nested(pydantic.BaseModel):
    item: Item


class Mapped(pydantic.BaseModel):
    m: dict[str, str]


class ListOfLists(pydantic.BaseModel):
    rows: list[list[int]]


class EitherOr(pydantic.BaseModel):
    v: list[str] | str


class Choices(pydantic.BaseModel):
    v: str = pydantic.Field(validation_alias=pydantic.AliasChoices("v", "value"))


@pytest.mark.parametrize("model", [Nested, Mapped, ListOfLists, EitherOr, Choices])
def test_a_field_a_query_string_cannot_carry_is_refused_when_declared(model):
    with pytest.raises(QueryDeclarationError, match=rf"query={model.__name__}: field"):
        QueryFields.of(model)


# --- which keys take every value, over any annotation (properties) ----------------------------


class Shape(enum.Enum):
    """How a query string carries a field: one value, every value of its key, or not at all."""

    ONE = enum.auto()
    EVERY = enum.auto()
    NONE = enum.auto()


@dataclasses.dataclass
class Point:
    x: int


# An annotation built at run time, and how a query string carries a field annotated so.
FieldAnnotation = tuple[Any, Shape]


def _optional(child: FieldAnnotation) -> FieldAnnotation:
    annotation, shape = child
    return annotation | None, shape


def _annotated(child: FieldAnnotation) -> FieldAnnotation:
    annotation, shape = child
    return operator.getitem(Annotated, (annotation, "note")), shape


def _sequence(origin: type, child: FieldAnnotation) -> FieldAnnotation:
    # A sequence of single values is carried by a repeated key; a sequence of anything else is not.
    annotation, shape = child
    args = (annotation, ...) if origin is tuple else (annotation,)
    return types.GenericAlias(origin, args), Shape.EVERY if shape is Shape.ONE else Shape.NONE


def _mapping(child: FieldAnnotation) -> FieldAnnotation:
    return types.GenericAlias(dict, (str, child[0])), Shape.NONE


def _union(left: FieldAnnotation, right: FieldAnnotation) -> FieldAnnotation:
    # A union is carried only when every arm is carried the same way.
    shapes = {left[1], right[1]}
    return left[0] | right[0], shapes.pop() if len(shapes) == 1 else Shape.NONE


field_annotations = st.recursive(
    st.sampled_from([
        (int, Shape.ONE), (str, Shape.ONE), (float, Shape.ONE), (bool, Shape.ONE),
        (Literal["book", "film"], Shape.ONE), (Item, Shape.NONE), (Point, Shape.NONE),
    ]),
    lambda children: st.one_of(
        children.map(_optional),
        children.map(_annotated),
        st.builds(_sequence, st.sampled_from([list, set, frozenset, Sequence, tuple]), children),
        children.map(_mapping),
        st.builds(_union, children, children),
    ),
    max_leaves=6,
)


@given(field_annotations)
def test_a_field_is_carried_as_its_annotation_shape_says(annotated):
    annotation, shape = annotated
    model = pydantic.create_model(
        "Q", __config__=pydantic.ConfigDict(defer_build=True), f=(annotation, None)
    )

    if shape is Shape.NONE:
        with pytest.raises(QueryDeclarationError, match="query=Q: field 'f'"):
            QueryFields.of(model)
        return
    fields = QueryFields.of(model)
    assert (fields.sequences, fields.scalars) == (
        ({"f"}, set()) if shape is Shape.EVERY else (set(), {"f"})
    )


keys = st.sampled_from(["a", "b", "c", "d"])


@given(
    st.sets(keys), st.sets(keys),
    st.dictionaries(keys, st.lists(st.text(max_size=3), min_size=1, max_size=3)),
)
def test_input_hands_each_key_on_once_either_as_values_or_as_an_error(sequences, scalars, params):
    fields = QueryFields(frozenset(sequences), frozenset(scalars - sequences))

    values, errors = fields.input(params)

    refused = {e["loc"][1] for e in errors}
    assert values.keys() | refused == params.keys() and not values.keys() & refused
    for key, given_values in params.items():
        if key in fields.sequences:
            assert values[key] == given_values
        elif len(given_values) == 1:
            assert values[key] == given_values[0]
        elif key in fields.scalars:
            assert key in refused
        else:
            assert values[key] == given_values


# --- a served route ---------------------------------------------------------------------------


app = Mipyre(runtime="main")


@app.get("/search", query=Search)
def search(req, s: Search) -> dict:
    return s.model_dump()


@app.get("/shelf/{shelf}", query=Search)
async def shelf(s: Search, shelf: str) -> dict:
    return {"shelf": shelf, "q": s.q}


@app.post("/items", body=Item, query=Search)
def create(req, item: Item, s: Search) -> dict:
    return {"name": item.name, "q": s.q}


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


@pytest.mark.parametrize(("url", "values"), [
    ("/search?q=x", {"q": "x", "page": 1, "tag": [], "kind": None}),
    ("/search?q=x&page=3&kind=film", {"q": "x", "page": 3, "tag": [], "kind": "film"}),
    ("/search?q=x&tag=a&tag=b", {"q": "x", "page": 1, "tag": ["a", "b"], "kind": None}),
    ("/search?q=a%20b+c&tag=%E4%B8%AD", {"q": "a b c", "page": 1, "tag": ["中"], "kind": None}),
    ("/search?q=", {"q": "", "page": 1, "tag": [], "kind": None}),
])
def test_the_handler_gets_the_validated_query(client, url, values):
    r = client.get(url)

    assert (r.status_code, r.json()) == (200, values)


@pytest.mark.parametrize(("url", "detail"), [
    ("/search", [{"type": "missing", "loc": ["query", "q"], "msg": "Field required"}]),
    ("/search?q=x&page=two", [{"type": "int_parsing", "loc": ["query", "page"],
                               "msg": "Input should be a valid integer, unable to parse string as an integer"}]),
    # An empty value is the empty string, not an absent key.
    ("/search?q=x&page=", [{"type": "int_parsing", "loc": ["query", "page"],
                            "msg": "Input should be a valid integer, unable to parse string as an integer"}]),
    ("/search?q=a&q=b", [{"type": "multiple_values", "loc": ["query", "q"],
                          "msg": "Expected one value, got 2; a field annotated as a list takes a repeated key"}]),
])
def test_a_query_that_does_not_validate_is_a_422_listing_why(client, url, detail):
    r = client.get(url)

    assert r.status_code == 422
    assert r.headers["content-type"] == "application/json"
    assert [{k: e[k] for k in ("type", "loc", "msg")} for e in r.json()["detail"]] == detail


def test_an_async_handler_without_the_request_gets_the_query_and_its_path_params(client):
    assert client.get("/shelf/top?q=x").json() == {"shelf": "top", "q": "x"}


def test_a_route_with_a_body_and_a_query_gets_both_and_reports_both_failing(client):
    assert client.post("/items?q=x", body=b'{"name": "n"}').json() == {"name": "n", "q": "x"}

    r = client.post("/items", body=b"{}")

    assert r.status_code == 422
    assert [(e["type"], e["loc"]) for e in r.json()["detail"]] == [("missing", ["name"]), ("missing", ["query", "q"])]


def test_the_route_listing_names_the_query_model():
    [info] = [r for r in app.routes if r.path == "/search"]

    assert (info.query, info.body) == (Search, None)


# --- declaring one ----------------------------------------------------------------------------


def test_a_query_route_on_the_sub_interpreters_is_declared_as_the_decision_says_for_the_installed_pydantic_core():
    import re

    from mipyre._routes import RouteDeclaration, Runtime
    from mipyre._routes import _pydantic_core_facts, _pydantic_refusal

    app = Mipyre()
    declared = RouteDeclaration("GET", "/s", lambda req, s: "", Runtime.SUBINTERP, None, Search, True)
    expected = _pydantic_refusal(declared, _pydantic_core_facts())

    if expected is None:
        app.get("/s", query=Search)(lambda req, s: "")
        assert [r.path for r in app.routes] == ["/s"]
    else:
        with pytest.raises(RuntimeDeclarationError, match=re.escape(str(expected))):
            app.get("/s", query=Search)(lambda req, s: "")


@pytest.mark.parametrize("runtime", ["main", "process", "isolated"])
def test_a_query_route_is_declared_on_every_runtime_that_loads_pydantic(runtime):
    Mipyre(runtime="main").get("/s", query=Search, runtime=runtime)(lambda req, s: "")


def test_a_router_carries_query_to_the_app():
    router = Router(runtime="main")
    router.get("/s", query=Search)(lambda req, s: "")

    [route] = router._routes
    assert route.query is Search


def test_query_must_be_a_pydantic_model():
    with pytest.raises(TypeError, match=r"query= must be a pydantic BaseModel subclass"):
        Mipyre(runtime="main").get("/s", query=dict)(lambda req, s: "")


def test_a_handler_with_no_parameter_for_the_query_is_refused_naming_the_forms():
    def handler(item: Item): ...

    with pytest.raises(TypeError, match=r"no parameter for the validated Search query: with body= and query=, "
                                        r"a handler is fn\(req, body, query, \*\*path_params\)"):
        Mipyre(runtime="main").post("/s", body=Item, query=Search)(handler)


def test_the_query_model_where_the_request_goes_is_refused():
    def handler(s: Search, extra): ...

    with pytest.raises(TypeError, match=r"'s' is annotated Search but stands where the request goes"):
        Mipyre(runtime="main").get("/s", query=Search)(handler)
