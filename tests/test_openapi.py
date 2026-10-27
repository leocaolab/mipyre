"""The OpenAPI 3.1 document `OpenApi(app)` serves: what it says about each route,
that it is valid against the official OpenAPI 3.1 schema, and that Schemathesis can test a served
app from it."""

from __future__ import annotations

from typing import Callable

import pydantic
import pytest
import schemathesis
from hypothesis import settings
from schemathesis.checks import negative_data_rejection
from openapi_spec_validator import validate

from mipyre import FastRoute, Mipyre, OpenApi, Response, Route, Runtime
from mipyre._openapi import document
from mipyre.engine import VALIDATION_DETAIL_LIMIT
from mipyre.testing import TestClient, TestServer
from tests.apps.openapi_items import app as items_app, docs as items_docs


class Point(pydantic.BaseModel):
    x: int
    y: int


def _route(method: str, path: str, handler: Callable, model: type | None, query: type | None = None) -> Route:
    return Route(method, path, handler, Runtime.MAIN, False, model, query, True)


def _doc(*routes: Route, fast: tuple[FastRoute, ...] = ()) -> dict:
    doc = document("T", "1", list(routes), list(fast))
    validate(doc)
    return doc


def _success_content(handler) -> dict | None:
    return _doc(_route("GET", "/r", handler, None))["paths"]["/r"]["get"]["responses"]["200"].get("content")


def _returns_str(req) -> str:
    raise NotImplementedError
def _returns_bytes(req) -> bytes:
    raise NotImplementedError
def _returns_dict(req) -> dict[str, list[int]]:
    raise NotImplementedError
def _returns_list(req) -> list:
    raise NotImplementedError
def _returns_set_list(req) -> list[set[int]]:
    raise NotImplementedError
def _returns_none(req) -> None: ...
def _returns_point(req) -> Point:
    raise NotImplementedError
def _unannotated(req): ...
def _unresolvable(req): ...


# A forward reference that never resolves, as a name imported only under TYPE_CHECKING is.
_unresolvable.__annotations__ = {"return": "NotImportedHere"}


class _View:
    def __call__(self, req) -> str:
        raise NotImplementedError


@pytest.mark.parametrize(
    ("handler", "content"),
    [
        (_View(), {"text/plain; charset=utf-8": {"schema": {"type": "string"}}}),
        (_returns_str, {"text/plain; charset=utf-8": {"schema": {"type": "string"}}}),
        (_returns_bytes, {"application/octet-stream": {}}),
        (
            _returns_dict,
            {"application/json": {"schema": {
                "type": "object", "additionalProperties": {"type": "array", "items": {"type": "integer"}},
            }}},
        ),
        (_returns_list, {"application/json": {"schema": {"type": "array"}}}),
        # JSON, but the items have no JSON Schema form: the shape is left undescribed.
        (_returns_set_list, {"application/json": {}}),
        # The engine answers None with an empty body and a pydantic model with its str(): no
        # media type follows from either annotation, nor from none at all.
        (_returns_none, None),
        (_returns_point, None),
        (_unannotated, None),
        (_unresolvable, None),
    ],
)
def test_the_success_body_is_described_only_as_far_as_the_return_annotation_says(handler, content):
    assert _success_content(handler) == content


def test_a_model_route_takes_the_model_as_its_required_json_body_and_documents_the_422():
    def create(p: Point) -> dict:
        raise NotImplementedError

    op = _doc(_route("POST", "/points", create, Point))["paths"]["/points"]["post"]

    assert op["requestBody"] == {
        "required": True,
        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Point"}}},
    }
    assert set(op["responses"]) == {"200", "422"}


def test_the_documented_422_lists_at_most_the_engines_detail_limit():
    def create(p: Point) -> dict:
        raise NotImplementedError

    op = _doc(_route("POST", "/points", create, Point))["paths"]["/points"]["post"]

    schema = op["responses"]["422"]["content"]["application/json"]["schema"]
    assert schema["properties"]["detail"]["maxItems"] == VALIDATION_DETAIL_LIMIT


def test_a_model_used_by_two_routes_is_one_component():
    def make(p: Point) -> dict:
        raise NotImplementedError

    doc = _doc(_route("POST", "/a", make, Point), _route("PUT", "/b", make, Point))

    assert list(doc["components"]["schemas"]) == ["Point"]


class Search(pydantic.BaseModel):
    q: str = pydantic.Field(description="what to look for")
    page: int = 1
    tag: list[str] = []
    since: int | None = pydantic.Field(None, alias="from")


def test_a_query_model_s_fields_are_the_query_parameters_and_its_own_schema_is_not_a_component():
    def search(req, s: Search) -> dict:
        raise NotImplementedError

    doc = _doc(_route("GET", "/items/{shelf}", search, None, Search))
    op = doc["paths"]["/items/{shelf}"]["get"]

    assert op["parameters"] == [
        {"name": "shelf", "in": "path", "required": True, "schema": {"type": "string"}},
        {"name": "q", "in": "query", "required": True,
         "schema": {"type": "string", "title": "Q", "description": "what to look for"}},
        {"name": "page", "in": "query", "required": False,
         "schema": {"type": "integer", "title": "Page", "default": 1}},
        # One parameter whose key repeats: OpenAPI's default for a query array (form, explode).
        {"name": "tag", "in": "query", "required": False,
         "schema": {"type": "array", "items": {"type": "string"}, "title": "Tag", "default": []}},
        # By the key the query string carries: the alias.
        {"name": "from", "in": "query", "required": False,
         "schema": {"anyOf": [{"type": "integer"}, {"type": "null"}], "title": "From", "default": None}},
    ]
    assert "requestBody" not in op
    assert op["responses"]["422"]["description"].startswith("The query does not validate")
    assert "components" not in doc


def test_a_query_model_that_is_also_a_body_stays_a_component():
    def search(req, s: Search) -> dict:
        raise NotImplementedError

    doc = _doc(_route("GET", "/s", search, None, Search), _route("POST", "/s", search, Search))

    assert list(doc["components"]["schemas"]) == ["Search"]


def test_a_route_with_a_body_and_a_query_documents_both_and_one_422():
    def tag(p: Point, s: Search) -> dict:
        raise NotImplementedError

    op = _doc(_route("POST", "/t", tag, Point, Search))["paths"]["/t"]["post"]

    assert [p["name"] for p in op["parameters"]] == ["q", "page", "tag", "from"]
    assert op["requestBody"]["content"]["application/json"]["schema"] == {"$ref": "#/components/schemas/Point"}
    assert op["responses"]["422"]["description"].startswith("The body or the query does not validate")


def test_without_a_model_route_there_are_no_components():
    assert "components" not in _doc(_route("GET", "/r", _returns_str, None))


def test_path_params_are_required_strings_and_a_catch_all_is_a_plain_template():
    def get(req, a, rest): ...

    paths = _doc(_route("GET", "/x/{a}/{*rest}", get, None))["paths"]

    assert list(paths) == ["/x/{a}/{rest}"]
    assert paths["/x/{a}/{rest}"]["get"]["parameters"] == [
        {"name": "a", "in": "path", "required": True, "schema": {"type": "string"}},
        {"name": "rest", "in": "path", "required": True, "schema": {"type": "string"}},
    ]


def test_the_handler_docstring_is_the_operation_description():
    def get(req):
        """Return the thing."""

    assert _doc(_route("GET", "/r", get, None))["paths"]["/r"]["get"]["description"] == "Return the thing."


def test_methods_share_a_path_item_and_a_method_openapi_has_no_key_for_is_left_out():
    paths = _doc(
        _route("GET", "/r", _returns_str, None),
        _route("DELETE", "/r", _returns_str, None),
        _route("PURGE", "/r", _returns_str, None),
    )["paths"]

    assert set(paths["/r"]) == {"get", "delete"}


def test_a_fast_route_is_documented_with_its_status_and_content_type_and_wins_its_path():
    fast = FastRoute(method="GET", path="/r", status_code=299, content_type="text/csv", body_bytes=3)

    op = _doc(_route("GET", "/r", _returns_str, None), fast=(fast,))["paths"]["/r"]["get"]

    assert op == {"responses": {"299": {"description": "Status 299", "content": {"text/csv": {}}}}}


def test_without_openapi_there_is_no_document():
    app = Mipyre(runtime="main")
    app.get("/r", _returns_str)

    with TestClient(app) as c:
        assert c.get("/openapi.json").status_code == 404


def test_a_before_request_hook_guards_the_document_like_any_route():
    app = Mipyre(runtime="main")
    OpenApi(app, title="T", version="1")

    @app.before_request
    def guard(req):
        return Response(body="sign in first", status_code=401) if req.path == "/openapi.json" else None

    with TestClient(app) as c:
        assert c.get("/openapi.json").status_code == 401


def test_openapi_refuses_a_path_with_a_parameter():
    with pytest.raises(ValueError, match="path parameter"):
        OpenApi(Mipyre(runtime="main"), title="T", version="1", path="/docs/{name}")


def test_a_second_document_at_the_same_path_is_refused_as_any_second_route_is():
    app = Mipyre(runtime="main")
    OpenApi(app, title="T", version="1")

    with pytest.raises(ValueError, match="/openapi.json"):
        OpenApi(app, title="T", version="2")


def test_the_document_describes_the_routes_declared_after_it():
    app = Mipyre(runtime="main")
    docs = OpenApi(app, title="T", version="1")
    app.get("/later", lambda req: "a")

    with TestClient(app) as c:
        assert set(c.get("/openapi.json").json()["paths"]) == {"/later"}
    assert set(docs.document()["paths"]) == {"/later"}


def test_the_served_document_is_valid_openapi_3_1_and_lists_every_route_but_its_own():
    with TestClient(items_app) as c:
        r = c.get("/openapi.json")

    assert r.status_code == 200
    assert r.headers["content-type"] == "application/json"
    doc = r.json()
    validate(doc)
    assert doc["info"] == {"title": "Items", "version": "1.0.0"}
    # `/mcp` speaks JSON-RPC, which MCP's `tools/list` describes.
    assert set(doc["paths"]) == {"/items", "/items/{item_id}", "/items/{item_id}/tags", "/search", "/files/{rest}",
                                 "/raw", "/plain", "/ping"}


def test_a_route_declared_in_openapi_false_is_served_but_not_documented():
    app = Mipyre(runtime="main")
    docs = OpenApi(app, title="T", version="1")
    app.get("/listed", lambda req: "a")
    app.route("POST", "/hidden", lambda req: "b", in_openapi=False)

    with TestClient(app) as c:
        assert c.post("/hidden").text == "b"
    assert set(docs.document()["paths"]) == {"/listed"}


def test_document_is_the_served_document():
    with TestClient(items_app) as c:
        assert c.get("/openapi.json").json() == items_docs.document()


_items = schemathesis.openapi.from_dict(items_docs.document())


@pytest.fixture(scope="module")
def items_url():
    with TestServer(items_app) as server, TestClient(server) as c:
        yield f"http://127.0.0.1:{c.port}"


@_items.parametrize()
# Fixed: every run takes the same cases.
@settings(derandomize=True, database=None, deadline=None)
def test_schemathesis_finds_the_served_operation_answering_as_documented(case, items_url):
    # Left out: pydantic's default (lax) mode takes `false` and `"7"` for an int, which its own JSON
    # Schema excludes, so this check reports the `body=` route accepting them. Strict mode does not
    # help: it refuses `7.0`, which JSON Schema's "integer" includes.
    case.call_and_validate(base_url=items_url, excluded_checks=[negative_data_rejection])
