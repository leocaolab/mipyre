"""The public errors, by who decides: each importable from `mipyre`, each
in its family."""

from __future__ import annotations

import pytest

import mipyre


@pytest.mark.parametrize("name", [
    "LateRegistrationError", "MissingAppFileError",
    "RuntimeDeclarationError", "QueryDeclarationError",
])
def test_a_declaration_error_is_one_the_apps_author_fixes(name):
    assert issubclass(getattr(mipyre, name), mipyre.DeclarationError)


@pytest.mark.parametrize("name", ["RPCRequestError", "RPCServerError", "RPCReplyError"])
def test_an_rpc_failure_is_an_rpc_error(name):
    assert issubclass(getattr(mipyre, name), mipyre.RPCError)


def test_the_request_and_run_families():
    assert issubclass(mipyre.MultipartError, mipyre.HTTPException)
    assert issubclass(mipyre.BodyReadError, OSError)
    assert not issubclass(mipyre.BodyReadError, mipyre.HTTPException)


def test_every_exported_error_is_named_as_one():
    errors = [n for n in mipyre.__all__ if isinstance(getattr(mipyre, n), type)
              and issubclass(getattr(mipyre, n), BaseException)]
    assert errors and all(n.endswith("Error") or n == "HTTPException" for n in errors), errors

