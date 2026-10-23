"""Tests for `mipyre.engine.encode_json`, the one JSON encoder responses and the framework's
own JSON go through."""

import datetime
import decimal
import json
import math
import sys
import uuid

import pytest
from hypothesis import given, strategies as st

from mipyre.engine import encode_json


def test_encode_json_raises_type_error_for_a_value_it_cannot_encode():
    for value in (b"x", object(), {1.5: "v"}):
        with pytest.raises(TypeError):
            encode_json(value)


@pytest.mark.parametrize("n", [2**64, 2**65, -(2**63) - 1, -(2**64) - 1, 10**100])
def test_encode_json_writes_an_int_wider_than_64_bits_as_json_dumps_does(n):
    assert encode_json({"n": n}) == json.dumps({"n": n}, separators=(",", ":")).encode()


def test_encode_json_refuses_an_int_past_the_digit_limit_as_json_dumps_does():
    limit = sys.get_int_max_str_digits()
    sys.set_int_max_str_digits(640)
    try:
        with pytest.raises(ValueError):
            json.dumps(10**700)
        with pytest.raises(ValueError):
            encode_json(10**700)
    finally:
        sys.set_int_max_str_digits(limit)


_I64 = st.integers(-(2**63), 2**63 - 1)
_UTC = datetime.timezone.utc

# What `json`/`jsonb` columns decode to; integers stay within 64 bits, since isojson cannot
# encode a wider one.
_JSON_DOCUMENT = st.recursive(
    st.none() | st.booleans() | st.integers(-(2**63), 2**64 - 1)
    | st.floats(allow_nan=False, allow_infinity=False) | st.text(),
    lambda inner: st.lists(inner, max_size=4) | st.dictionaries(st.text(), inner, max_size=4),
    max_leaves=12,
)

# Every Python type `crates/pyext/src/db/cell.rs` makes of a non-bytes column; `bytea` and
# columns it has no decoder for come out as `bytes`, which has no JSON form and stays refused.
_CELL = st.one_of(
    st.none(),
    st.booleans(),
    _I64,
    st.floats(width=32),
    st.floats(),
    st.text(),
    _JSON_DOCUMENT,
    st.decimals(),
    st.uuids(),
    st.dates(),
    st.datetimes(),
    st.datetimes(timezones=st.just(_UTC)),
)


def _as_json(value):
    if isinstance(value, (uuid.UUID, decimal.Decimal)):
        return str(value)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {k: _as_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_as_json(v) for v in value]
    return value


def _refuse_constant(name):
    raise AssertionError(f"encoded JSON carries {name}")


@given(st.dictionaries(st.text(), _CELL, max_size=6))
def test_every_row_a_query_returns_encodes(row):
    encoded = encode_json(row)
    assert json.loads(encoded, parse_constant=_refuse_constant) == _as_json(row)
