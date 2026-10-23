"""The route vocabulary (docs/design/route-vocabulary.md): the runtime names, and one declaration
surface that a Router and the app share."""

from __future__ import annotations

import inspect

import pydantic
import pytest

from mipyre import BodyStream, Mipyre, OpenApi, Route, Router, Runtime, RuntimeDeclarationError

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
        lambda owner: owner.post("/x", lambda req, p: 1, model=Point),
        lambda owner: owner.post("/x", lambda req: 1, stream=True),
        lambda owner: owner.get("/x", lambda req: 1, gil=True),
        lambda owner: owner.get("/x", lambda req: 1, process=False),
        lambda owner: owner.get("/x", lambda req: 1, typo=1),
        lambda owner: owner.get("/x", lambda req: 1, runtime="proces"),
    ],
    ids=["stream-on-get", "model", "stream", "gil", "process", "unknown-keyword", "unknown-runtime"],
)
def test_a_router_refuses_a_declaration_as_the_app_does(declare):
    assert _error_of(lambda: declare(Router())) == _error_of(lambda: declare(Mipyre()))


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

    app = Mipyre(runtime="main", routers=["routes_mod:router", "routes_mod:placed"])
    docs = OpenApi(app, title="T", version="1")

    assert [(r.path, r.runtime) for r in app.routes] == [
        ("/plain", Runtime.MAIN), ("/hidden", Runtime.MAIN), ("/placed", Runtime.PROCESS), ("/own", Runtime.MAIN),
        ("/openapi.json", Runtime.MAIN),
    ]
    assert "/hidden" not in docs.document()["paths"]
    assert "/plain" in docs.document()["paths"]


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
