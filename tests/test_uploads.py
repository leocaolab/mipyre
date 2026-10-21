"""`parse_multipart`: a repeated field name, and the limits on parts and part headers."""

from __future__ import annotations

import pytest

from mipyre.uploads import Form, MultipartError, FormPart, parse_multipart


class _Req:
    def __init__(self, body: bytes) -> None:
        self.headers = {"content-type": "multipart/form-data; boundary=b"}
        self.body = body


def _body(*parts: tuple[str, bytes], header: bytes = b"") -> bytes:
    out = b""
    for name, data in parts:
        out += b"--b\r\nContent-Disposition: form-data; name=\"" + name.encode() + b"\"" + header
        out += b"\r\n\r\n" + data + b"\r\n"
    return out + b"--b--\r\n"


def _field(name: str, data: bytes) -> FormPart:
    return FormPart(name=name, filename=None, content_type="text/plain", data=data)


def test_a_repeated_name_reads_as_its_first_part_and_get_all_returns_every_part():
    form = parse_multipart(_Req(_body(("tag", b"a"), ("one", b"x"), ("tag", b"b"))))

    assert form["tag"] == _field("tag", b"a")
    assert form.get("tag") == _field("tag", b"a")
    assert form.get_all("tag") == [_field("tag", b"a"), _field("tag", b"b")]
    assert form.get_all("one") == [_field("one", b"x")]
    assert form.get_all("missing") == []
    assert form.get("missing") is None
    assert list(form) == ["tag", "one"]


def test_forms_differing_only_in_a_repeated_part_are_not_equal():
    once = parse_multipart(_Req(_body(("tag", b"a"))))
    twice = parse_multipart(_Req(_body(("tag", b"a"), ("tag", b"b"))))

    assert once != twice
    assert once == Form({"tag": [_field("tag", b"a")]})


def test_a_body_of_max_parts_parts_parses_and_one_more_is_a_multipart_error():
    assert len(parse_multipart(_Req(_body(*[("f", b"")] * 3)), max_parts=3).get_all("f")) == 3

    with pytest.raises(MultipartError, match="max_parts=3"):
        parse_multipart(_Req(_body(*[("f", b"")] * 4)), max_parts=3)


def test_parts_after_the_closing_delimiter_are_epilogue_not_counted():
    body = _body(("f", b"")) + _body(*[("g", b"")] * 5)

    assert list(parse_multipart(_Req(body), max_parts=1)) == ["f"]


def test_a_part_header_section_at_the_limit_parses_and_one_byte_more_is_a_multipart_error():
    header_section = len(b"Content-Disposition: form-data; name=\"f\"")

    assert parse_multipart(_Req(_body(("f", b"x"))), max_part_header_bytes=header_section)["f"].data == b"x"

    with pytest.raises(MultipartError, match=f"max_part_header_bytes={header_section - 1}"):
        parse_multipart(_Req(_body(("f", b"x"))), max_part_header_bytes=header_section - 1)


def test_the_header_limit_counts_every_header_line_of_the_part():
    long_type = b"\r\nContent-Type: text/plain; " + b"x" * 100

    with pytest.raises(MultipartError, match="max_part_header_bytes=100"):
        parse_multipart(_Req(_body(("f", b"x"), header=long_type)), max_part_header_bytes=100)


def test_the_default_limits_refuse_a_flood_of_empty_parts_and_a_huge_header_line():
    with pytest.raises(MultipartError, match="max_parts=1000"):
        parse_multipart(_Req(_body(*[("f", b"")] * 1001)))

    with pytest.raises(MultipartError, match="max_part_header_bytes=16384"):
        parse_multipart(_Req(_body(("f", b"x"), header=b"; filler=\"" + b"x" * 16384 + b"\"")))


@pytest.mark.parametrize("limits", [{"max_parts": 0}, {"max_part_header_bytes": 0}, {"max_parts": -1}])
def test_a_limit_below_one_is_refused(limits):
    with pytest.raises(ValueError, match="at least 1"):
        parse_multipart(_Req(_body(("f", b"x"))), **limits)
