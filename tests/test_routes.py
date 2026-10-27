"""The route vocabulary: the runtime names, and one declaration
surface that a Router and the app share."""

from __future__ import annotations

import inspect

import pydantic
import pytest

from mipyre import BodyStream, Mipyre, OpenApi, Route, Router, Runtime, RuntimeDeclarationError
from mipyre._routes import _is_prefix, _is_route_path

VERBS = ("get", "post", "put", "delete", "patch", "options", "head", "route")
EVERY_RUNTIME = "'subinterp', 'isolated', 'process', 'main'"


class Point(pydantic.BaseModel):
    x: int


@pytest.mark.parametrize("runtime", list(Runtime))
def test_a_runtime_parses_from_its_member_and_its_value(runtime):
    assert Runtime.parse(runtime, "w") is runtime
    assert Runtime.parse(runtime.value, "w") is runtime


@pytest.mark.parametrize("name", ["proces", "gil", "auto", "default", "MAIN", "", None, 1])
def test_anything_else_is_refused_naming_every_runtime(name):
    with pytest.raises(RuntimeDeclarationError) as err:
        Runtime.parse(name, "GET /r")

    assert str(err.value) == f"GET /r: runtime={name!r} is not one of {EVERY_RUNTIME}"


def test_the_runtimes_on_sub_interpreters_are_subinterp_and_isolated():
    assert {r for r in Runtime if r.on_subinterpreters} == {Runtime.SUBINTERP, Runtime.ISOLATED}


@pytest.mark.parametrize("verb", VERBS)
def test_a_router_declares_routes_with_the_same_signature_as_the_app(verb):
    assert inspect.signature(getattr(Router, verb)) == inspect.signature(getattr(Mipyre, verb))


def _error_of(declare) -> tuple[type, str]:
    with pytest.raises(Exception) as err:
        declare()
    return type(err.value), str(err.value)


@pytest.mark.parametrize(
    "declare",
    [
        lambda owner: owner.get("/x", lambda req: 1, body=BodyStream),
        lambda owner: owner.get("/x", lambda req: 1, typo=1),
        lambda owner: owner.get("/x", lambda req: 1, runtime="proces"),
    ],
    ids=["stream-on-get", "unknown-keyword", "unknown-runtime"],
)
def test_a_router_refuses_a_declaration_as_the_app_does(declare):
    assert _error_of(lambda: declare(Router())) == _error_of(lambda: declare(Mipyre()))


@pytest.mark.parametrize(("path", "taken"), [("/", True), ("/x", True), ("", False), ("x/", False), ("nopath", False)])
def test_a_route_path_is_one_that_starts_with_a_slash(path, taken):
    assert _is_route_path(path) is taken


@pytest.mark.parametrize("owner", [Mipyre, Router], ids=["app", "router"])
def test_a_route_path_without_a_leading_slash_is_refused_at_declaration_quoting_it(owner):
    with pytest.raises(ValueError) as err:
        owner().get("nopath", lambda req: 1)

    assert str(err.value) == """GET 'nopath': a route path starts with "/\""""


def test_a_route_lists_the_function_it_was_declared_with_and_its_classes():
    app = Mipyre()

    def create(req, p: Point):
        return p.x

    app.post("/p", create, body=Point, runtime="process")

    assert app.routes == [
        Route("POST", "/p", create, Runtime.PROCESS, False, Point, None, True),
    ]


def test_a_route_neither_it_nor_its_router_places_runs_on_the_apps_runtime(tmp_path, monkeypatch):
    (tmp_path / "routes_mod.py").write_text(
        "from mipyre import Router\n"
        "router = Router()\n"
        "router.get('/plain', lambda req: 1)\n"
        "router.get('/hidden', lambda req: 1, in_openapi=False)\n"
        "placed = Router(runtime='process')\n"
        "placed.get('/placed', lambda req: 1)\n"
        "placed.get('/own', lambda req: 1, runtime='main')\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    app = Mipyre(runtime="main")
    app.include_router("routes_mod:router")
    app.include_router("routes_mod:placed")
    docs = OpenApi(app, title="T", version="1")

    assert [(r.path, r.runtime) for r in app.routes] == [
        ("/plain", Runtime.MAIN), ("/hidden", Runtime.MAIN), ("/placed", Runtime.PROCESS), ("/own", Runtime.MAIN),
        ("/openapi.json", Runtime.MAIN),
    ]
    assert "/hidden" not in docs.document()["paths"]
    assert "/plain" in docs.document()["paths"]


def _router_module(tmp_path, monkeypatch) -> None:
    (tmp_path / "router_targets.py").write_text("from mipyre import Router\nrouter = Router()\nnot_a_router = 1\n")
    monkeypatch.syspath_prepend(str(tmp_path))


def test_a_router_target_imports_the_module_and_the_router_it_names(tmp_path, monkeypatch):
    from mipyre._routes import import_router

    _router_module(tmp_path, monkeypatch)
    module, router = import_router("router_targets:router")

    assert module.__name__ == "router_targets"
    assert router is module.router


def test_a_router_target_naming_no_attribute_is_a_missing_target_quoting_it(tmp_path, monkeypatch):
    from mipyre._routes import import_router
    from mipyre._target import MissingTargetError

    _router_module(tmp_path, monkeypatch)
    with pytest.raises(MissingTargetError) as err:
        import_router("router_targets:absent")

    assert str(err.value) == "cannot import 'router_targets:absent': module 'router_targets' has no attribute 'absent'"


def test_a_router_target_naming_something_else_is_refused_naming_its_type(tmp_path, monkeypatch):
    from mipyre._routes import import_router

    _router_module(tmp_path, monkeypatch)
    with pytest.raises(TypeError) as err:
        import_router("router_targets:not_a_router")

    assert str(err.value) == "router 'router_targets:not_a_router' is a int, expected mipyre.Router"


@pytest.mark.parametrize(
    ("prefix", "taken"),
    [("", True), ("/a", True), ("/a/b", True), ("/", False), ("/a/", False), ("a", False), ("a/", False)],
)
def test_a_prefix_is_none_or_starts_with_a_slash_and_does_not_end_with_one(prefix, taken):
    assert _is_prefix(prefix) is taken


@pytest.mark.parametrize("prefix", ["", "/a", "/a/b"])
@pytest.mark.parametrize("path", ["/", "/x", "/x/"])
def test_a_prefix_joined_to_a_route_path_is_a_route_path_with_no_empty_segment(prefix, path):
    joined = prefix + path

    assert _is_route_path(joined)
    assert "//" not in joined


def test_an_included_router_serves_each_route_under_the_prefix(tmp_path, monkeypatch):
    (tmp_path / "admin_mod.py").write_text(
        "from mipyre import Router\n"
        "router = Router()\n"
        "router.get('/', lambda req: 1)\n"
        "router.get('/users/{id}', lambda req, id: id)\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    app = Mipyre(runtime="main")

    app.include_router("admin_mod:router", prefix="/admin")

    assert [r.path for r in app.routes] == ["/admin/", "/admin/users/{id}"]


@pytest.mark.parametrize("prefix", ["/", "/admin/", "admin"])
def test_an_include_with_a_prefix_of_another_shape_is_refused_quoting_it(prefix):
    with pytest.raises(ValueError) as err:
        Mipyre().include_router("admin_mod:router", prefix=prefix)

    assert str(err.value) == (
        f"include_router('admin_mod:router', prefix={prefix!r}): a prefix starts with \"/\" and does not end with \"/\""
    )


def test_a_router_passed_as_an_object_is_refused_saying_why_and_how_to_name_it():
    with pytest.raises(TypeError) as err:
        Mipyre().include_router(Router())

    assert str(err.value) == (
        'include_router takes a router\'s import string, as in app.include_router("myapp.admin:router"), '
        "not a Router: each runtime imports only the routers that have routes it serves, so a library a "
        "router imports (numpy, say) never loads in a sub-interpreter. An object passed here was imported "
        "by the app's module, which every runtime imports."
    )


def test_every_runtime_has_an_engine_executor():
    from mipyre._routes import _ENGINE_RUNTIME

    assert set(_ENGINE_RUNTIME) == set(Runtime)


class _AsyncCall:
    __name__ = "async_call"

    async def __call__(self, req):
        return 1


def _sync(req):
    return 1


async def _async(req):
    return 1


@pytest.mark.parametrize(
    ("handler", "is_async"),
    [(_sync, False), (_async, True), (_AsyncCall(), True)],
    ids=["def", "async-def", "async-call"],
)
@pytest.mark.parametrize("choice", ["Worker", "Main"])
def test_the_engine_says_whether_it_runs_a_handler_as_async(handler, is_async, choice):
    from mipyre.engine import MipyreApp, RuntimeChoice

    assert MipyreApp().route("GET", "/r", handler, getattr(RuntimeChoice, choice)) is is_async


def test_a_process_route_is_a_sync_handler():
    from mipyre.engine import MipyreApp, RuntimeChoice

    assert MipyreApp().route("GET", "/r", _sync, RuntimeChoice.Lane) is False


def test_a_route_lists_the_colour_the_engine_runs_it_in():
    app = Mipyre()
    app.get("/c", _AsyncCall(), runtime="main")

    [route] = app.routes
    assert route.is_async is True


class Search(pydantic.BaseModel):
    q: str = ""


_CORE = "/site-packages/pydantic_core/_pydantic_core.so"


def _declared(body=None, query=None):
    from mipyre._routes import RouteDeclaration

    return RouteDeclaration("POST", "/v", lambda req: 1, Runtime.SUBINTERP, body, query, True)


@pytest.mark.parametrize("given", [{"body": Point}, {"query": Search}, {"body": Point, "query": Search}])
def test_a_pydantic_core_that_declares_per_interpreter_gil_support_lets_a_validating_route_run_on_them(given):
    from mipyre._worker.extension_support import MultiPhase, MultipleInterpreters
    from mipyre._routes import _pydantic_refusal

    facts = MultiPhase(_CORE, MultipleInterpreters.PER_INTERPRETER_GIL_SUPPORTED)

    assert _pydantic_refusal(_declared(**given), facts) is None


def _unsupported_facts():
    from mipyre._worker.extension_support import MultiPhase, MultipleInterpreters, SinglePhase, UnknownFormat

    return [
        SinglePhase(_CORE),
        MultiPhase(_CORE, MultipleInterpreters.ABSENT),
        MultiPhase(_CORE, MultipleInterpreters.NOT_SUPPORTED),
        MultiPhase(_CORE, MultipleInterpreters.SUPPORTED),
        UnknownFormat("pydantic_core._pydantic_core", "it was not loaded from a file"),
    ]


@pytest.mark.parametrize("facts", _unsupported_facts(), ids=str)
def test_one_that_does_not_is_refused_naming_the_library_the_fact_and_the_ways_out(facts):
    from mipyre._routes import _pydantic_refusal

    refusal = _pydantic_refusal(_declared(body=Point, query=Search), facts)

    assert isinstance(refusal, RuntimeDeclarationError)
    text = str(refusal)
    assert text.startswith("POST /v: body= and query= validate with pydantic, whose core can't load")
    assert f"({facts})" in text
    for way in ("in place of PyPI's", 'runtime="main"', 'runtime="process"', 'runtime="isolated"'):
        assert way in text, way


def test_a_model_route_on_the_sub_interpreters_is_declared_as_the_decision_says_for_the_installed_pydantic_core():
    import re

    from mipyre._routes import _pydantic_core_facts, _pydantic_refusal

    def handler(req, p):
        return 1

    app = Mipyre()
    expected = _pydantic_refusal(_declared(body=Point), _pydantic_core_facts())

    if expected is None:
        app.post("/v", handler, body=Point)
        assert [r.runtime for r in app.routes] == [Runtime.SUBINTERP]
    else:
        with pytest.raises(RuntimeDeclarationError, match=re.escape(str(expected))):
            app.post("/v", handler, body=Point)
        assert app.routes == []


@pytest.mark.parametrize("given", [{"body": Point}, {"query": Search}])
def test_an_isolated_route_validates_with_its_own_copy_of_pydantic(given):
    app = Mipyre()

    app.post("/p", lambda req, p: 1, runtime="isolated", **given)

    assert [r.runtime for r in app.routes] == [Runtime.ISOLATED]
