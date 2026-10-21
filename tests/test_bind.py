"""Binding a handler to its route's path params and ``model=`` body, checked on plain
request stand-ins: no app, no server."""

import json
from types import SimpleNamespace

import pytest
from pydantic import BaseModel

from mipyre._bind import bind_handler


class Item(BaseModel):
    n: int


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


def test_a_valid_body_is_passed_validated_and_an_invalid_one_is_422():
    def create(body, id):
        return (body.n, id)

    bound = bind_handler(create, "/items/{id}", Item)
    assert bound(SimpleNamespace(params={"id": "1"}, body=b'{"n": 3}')) == (3, "1")
    refused = bound(SimpleNamespace(params={"id": "1"}, body=b'{"n": "x"}'))
    assert refused.status_code == 422
    assert json.loads(refused.body)["detail"][0]["loc"] == ["n"]
