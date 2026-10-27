"""Binding a handler to its route's path params, ``body=`` body and ``query=`` string,
checked on plain request stand-ins: no app, no server."""

import asyncio
import inspect
import json
import os
import re
import signal
import subprocess
import sys
import urllib.error
import urllib.request
from types import SimpleNamespace

import pytest
from hypothesis import example, given
from hypothesis import strategies as st
from pydantic import BaseModel, field_validator

from mipyre import Mipyre
from mipyre._bind import _is_str_annotation, bind_handler
from mipyre.engine import VALIDATION_DETAIL_LIMIT, Response
from mipyre.testing import TestClient
from tests._helpers import bound_port, lines_with, read_file


class Item(BaseModel):
    n: int


class Positive(BaseModel):
    n: int

    @field_validator("n")
    @classmethod
    def positive(cls, n: int) -> int:
        if n < 1:
            raise ValueError("n must be positive")
        return n


class Numbers(BaseModel):
    xs: list[int]


class Page(BaseModel):
    page: int


def _request(body: bytes = b"", query: dict[str, list[str]] | None = None, **params: str):
    given_query = query or {}
    return SimpleNamespace(params=params, body=body, query_params_all=lambda: given_query)


def _numbers(n: int) -> bytes:
    return json.dumps({"xs": ["x"] * n}).encode()


def test_a_handler_taking_only_the_request_is_registered_as_is():
    def index(req):
        return "ok"

    assert bind_handler(index, "/", None) is index


def test_path_params_are_passed_by_name():
    def show(req, user, post):
        return (user, post)

    bound = bind_handler(show, "/u/{user}/p/{post}", None)
    assert bound(SimpleNamespace(params={"user": "7", "post": "9"})) == ("7", "9")
    assert bound.__wrapped__ is show


def test_a_parameter_missing_from_the_template_is_a_registration_error():
    def show(req, user):
        return user

    with pytest.raises(ValueError, match=r"\['user'\].*not in the URL template '/u/{id}'"):
        bind_handler(show, "/u/{id}", None)


def test_a_first_parameter_named_as_a_path_param_is_a_registration_error():
    def hello(name):
        return f"Hello, {name}!"

    with pytest.raises(TypeError, match=r"'name' receives the request.*'/hello/{name}'.*def hello\(req, name\)"):
        bind_handler(hello, "/hello/{name}", None)


def test_a_first_parameter_named_as_no_path_param_takes_the_request():
    def hello(name):
        return name

    assert bind_handler(hello, "/hello/{who}", None) is hello


@pytest.mark.parametrize(
    ("annotation", "taken"),
    [(inspect.Parameter.empty, True), (str, True), ("str", True), (int, False), ("int", False), (str | None, False)],
)
def test_a_path_param_takes_no_annotation_or_str(annotation, taken):
    assert _is_str_annotation(annotation) is taken


def test_a_path_param_annotated_other_than_str_is_refused_at_registration_naming_handler_and_param():
    def q(req, id: int):
        return {"id": id}

    with pytest.raises(TypeError) as err:
        Mipyre().get("/q/{id}", q)

    assert str(err.value) == (
        "handler 'q': path param 'id' is annotated int, but a path param is passed as the str the "
        "URL holds; annotate it str and convert it in the handler"
    )


def test_a_validated_routes_path_param_annotated_other_than_str_is_refused_at_registration():
    def update(body: Item, id: float):
        return {"id": id}

    with pytest.raises(TypeError, match=r"handler 'update': path param 'id' is annotated float"):
        Mipyre().put("/items/{id}", update, body=Item)


def test_a_valid_body_is_passed_validated_and_an_invalid_one_is_422():
    def create(body, id):
        return (body.n, id)

    bound = bind_handler(create, "/items/{id}", Item)
    assert bound(SimpleNamespace(params={"id": "1"}, body=b'{"n": 3}')) == (3, "1")
    refused = bound(SimpleNamespace(params={"id": "1"}, body=b'{"n": "x"}'))
    assert refused.status_code == 422
    assert json.loads(refused.body)["detail"][0]["loc"] == ["n"]


def test_a_validator_that_raises_is_a_422_carrying_its_message():
    bound = bind_handler(lambda body: body.n, "/p", Positive)

    refused = bound(_request(b'{"n": 0}'))

    assert refused.status_code == 422
    [error] = json.loads(refused.body)["detail"]
    assert (error["type"], error["ctx"]) == ("value_error", {"error": "n must be positive"})


@pytest.mark.parametrize(("failing", "listed"), [(1, 1), (20, 20), (21, 20), (5000, 20)])
def test_a_422_lists_the_first_twenty_errors_and_counts_them_all(failing, listed):
    bound = bind_handler(lambda body: body, "/n", Numbers)

    answer = json.loads(bound(_request(_numbers(failing))).body)

    assert answer["error_count"] == failing
    assert [e["loc"] for e in answer["detail"]] == [["xs", i] for i in range(listed)]


def test_a_422_lists_at_most_the_engines_detail_limit():
    bound = bind_handler(lambda body: body, "/n", Numbers)

    answer = json.loads(bound(_request(_numbers(VALIDATION_DETAIL_LIMIT + 1))).body)

    assert len(answer["detail"]) == VALIDATION_DETAIL_LIMIT


def test_a_route_with_a_body_and_a_query_counts_the_errors_of_both_and_lists_the_body_first():
    bound = bind_handler(lambda body, query: (body, query), "/n", Numbers, Page)

    answer = json.loads(bound(_request(_numbers(25), {"page": ["two"]})).body)

    assert answer["error_count"] == 26
    assert len(answer["detail"]) == 20
    assert answer["detail"][0]["loc"] == ["xs", 0]


def test_a_query_that_fails_is_a_422_counting_its_errors():
    bound = bind_handler(lambda query: query, "/n", None, Page)

    answer = json.loads(bound(_request(query={"page": ["two"]})).body)

    assert answer["error_count"] == 1
    assert answer["detail"][0]["loc"] == ["query", "page"]


_VALIDATING_APP = """
from pydantic import BaseModel
from mipyre import Mipyre

class Item(BaseModel):
    n: int

app = Mipyre(runtime="main")
app.enable_metrics()
app.enable_access_log()

@app.post("/items", body=Item)
def create(body):
    return {"n": body.n}
"""


def test_a_burst_of_422s_is_counted_each_and_logged_once_without_a_traceback(tmp_path):
    (tmp_path / "validating_app.py").write_text(_VALIDATING_APP)
    log_path = tmp_path / "server.log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "mipyre", "run", "validating_app:app", "--port", "0"],
            cwd=tmp_path, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
    read = read_file(str(log_path))
    try:
        port = bound_port(read, proc)
        for _ in range(3):
            request = urllib.request.Request(f"http://127.0.0.1:{port}/items", data=b'{"n": "x"}', method="POST")
            with pytest.raises(urllib.error.HTTPError) as refused:
                urllib.request.urlopen(request, timeout=5)
            assert refused.value.code == 422
            refused.value.close()
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as r:
            page = r.read().decode()
        [line] = lines_with(read, "failed validation")
    finally:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        proc.wait(timeout=15)

    assert _validation_failures_on(page) == 3
    assert "a request failed validation with 1 error(s) (0 more since the last line)" in line, line
    assert "Traceback" not in read()


def test_the_metrics_page_counts_the_422s():
    app = Mipyre(runtime="main")
    app.enable_metrics()

    @app.post("/items", body=Item)
    def create(body):
        return {"n": body.n}

    with TestClient(app) as c:
        before = _validation_failures_on(c.get("/metrics").text)
        assert c.post("/items", body=b'{"n": "x"}').status_code == 422
        assert c.post("/items", body=b"not json").status_code == 422
        assert c.post("/items", body=b'{"n": 1}').status_code == 200
        assert _validation_failures_on(c.get("/metrics").text) - before == 2


def _validation_failures_on(page: str) -> int:
    [value] = re.findall(r"^mipyre_validation_failures_total (\d+)$", page, re.MULTILINE)
    return int(value)


@given(body=st.binary() | st.recursive(
    st.none() | st.booleans() | st.integers() | st.floats() | st.text(),
    lambda inner: st.lists(inner) | st.dictionaries(st.text(), inner),
).map(lambda value: json.dumps({"n": value}).encode()))
@example(body=b"[" * 100_000)
@example(body=b'{"n": ' + b"9" * 5000 + b"}")
@example(body=b'{"n": NaN}')
def test_any_body_is_the_handlers_answer_or_a_422(body):
    bound = bind_handler(lambda body: "ok", "/p", Positive)

    answer = bound(_request(body))

    assert answer == "ok" or (isinstance(answer, Response) and answer.status_code == 422)


async def _create_async(body, id):
    return (body.n, id)


def test_an_async_handlers_wrapper_hands_back_the_handlers_own_coroutine():
    bound = bind_handler(_create_async, "/items/{id}", Item)
    coroutine = bound(_request(b'{"n": 3}', id="1"))

    assert inspect.iscoroutinefunction(bound) and not inspect.iscoroutinefunction(bound.__call__)
    assert coroutine.cr_code is _create_async.__code__
    assert asyncio.run(coroutine) == (3, "1")


def test_an_async_handlers_422_is_awaited_like_its_answer():
    bound = bind_handler(_create_async, "/items/{id}", Item)

    refused = asyncio.run(bound(_request(b'{"n": "x"}', id="1")))

    assert refused.status_code == 422


class _View:
    async def __call__(self, req, id):
        return id


class _BodyView:
    async def __call__(self, req, body, id):
        return f"{id}:{body.n}"


@pytest.mark.parametrize(("view", "model"), [(_View(), None), (_BodyView(), Item)])
def test_a_class_based_views_wrapper_is_async_when_its_call_is(view, model):
    assert inspect.iscoroutinefunction(bind_handler(view, "/v/{id}", model))


def test_a_class_based_async_view_with_a_path_param_is_served_on_the_async_workers():
    app = Mipyre(runtime="main")
    app.get("/v/{id}")(_View())
    app.post("/b/{id}", body=Item)(_BodyView())

    with TestClient(app) as client:
        assert client.get("/v/7").text == "7"
        assert client.post("/b/7", body=b'{"n": 3}').text == "7:3"
