"""Unit tests for cookie utilities (no server needed)."""

import string

import pytest
from hypothesis import given
from hypothesis import strategies as st

from mipyre.cookies import Cookie, SameSite, get_cookies, get_cookie
from mipyre.engine import Response


class FakeRequest:
    def __init__(self, cookie_header=""):
        self.headers = {"cookie": cookie_header} if cookie_header else {}


def test_get_cookies_single():
    req = FakeRequest("session=abc123")
    cookies = get_cookies(req)
    assert cookies == {"session": "abc123"}


def test_get_cookies_multiple():
    req = FakeRequest("a=1; b=2; c=3")
    cookies = get_cookies(req)
    assert cookies == {"a": "1", "b": "2", "c": "3"}


def test_get_cookies_empty():
    req = FakeRequest("")
    assert get_cookies(req) == {}


def test_get_cookies_no_header():
    req = FakeRequest()
    req.headers = {}
    assert get_cookies(req) == {}


def test_get_cookie_found():
    req = FakeRequest("token=xyz; lang=en")
    assert get_cookie(req, "token") == "xyz"
    assert get_cookie(req, "lang") == "en"


def test_get_cookie_missing():
    req = FakeRequest("token=xyz")
    assert get_cookie(req, "missing") is None
    assert get_cookie(req, "missing", "default") == "default"


def _set_cookie(cookie: Cookie) -> str:
    line = Response(body="ok", headers={"set-cookie": cookie}).headers["set-cookie"]
    assert isinstance(line, str), line
    return line


def test_set_cookie_basic():
    cookie_header = _set_cookie(Cookie("session", "abc123"))
    assert "session=abc123" in cookie_header
    assert "Path=/" in cookie_header


def test_set_cookie_httponly():
    cookie_header = _set_cookie(Cookie("token", "secret", httponly=True, secure=True))
    assert "HttpOnly" in cookie_header
    assert "Secure" in cookie_header


def test_set_cookie_max_age():
    cookie_header = _set_cookie(Cookie("prefs", "dark", max_age=3600))
    assert "Max-Age=3600" in cookie_header


def test_delete_cookie():
    cookie_header = _set_cookie(Cookie.expired("session"))
    assert "session=" in cookie_header
    assert "Max-Age=0" in cookie_header


def test_delete_cookie_forwards_httponly():
    cookie_header = _set_cookie(Cookie.expired("session", httponly=True, secure=True))
    assert "Max-Age=0" in cookie_header
    assert "HttpOnly" in cookie_header
    assert "Secure" in cookie_header


def test_delete_cookie_default_not_httponly():
    assert "HttpOnly" not in _set_cookie(Cookie.expired("session"))


@pytest.mark.parametrize("max_age", ["0\r\nSet-Cookie: admin=1", "3600", True, 1.5])
def test_set_cookie_rejects_a_max_age_that_is_not_an_int(max_age):
    # Max-Age is interpolated verbatim, so a str carrying CR/LF would
    # inject a header of its own; bool is an int subclass but never a Max-Age.
    with pytest.raises(ValueError, match="max_age must be an int"):
        Cookie("prefs", "dark", max_age=max_age)


def test_each_cookie_is_its_own_set_cookie_line_beside_str_lines():
    resp = Response(
        body="ok",
        headers={"set-cookie": ["a=1", Cookie("b", "2"), Cookie.expired("c")], "x-one": "1"},
    )
    assert resp.headers["set-cookie"] == [
        "a=1",
        "b=2; Path=/; SameSite=Lax",
        "c=; Max-Age=0; Path=/; SameSite=Lax",
    ]
    assert resp.headers["x-one"] == "1"


def test_a_cookie_goes_under_set_cookie_only():
    with pytest.raises(ValueError, match="set-cookie"):
        Response(body="ok", headers={"x-one": Cookie("n", "v")})
    with pytest.raises(ValueError, match="set-cookie"):
        Response(body="ok", headers={"x-one": ["a", Cookie("n", "v")]})
    assert Response(body="ok", headers={"Set-Cookie": Cookie("n", "v")}).headers["set-cookie"] == (
        "n=v; Path=/; SameSite=Lax"
    )


def test_a_header_value_that_is_neither_str_nor_cookie_is_a_type_error():
    with pytest.raises(TypeError, match="set-cookie"):
        Response(body="ok", headers={"set-cookie": 5})
    with pytest.raises(TypeError, match="x-one"):
        Response(body="ok", headers={"x-one": [b"1"]})


@pytest.mark.parametrize("name", ["", "a=b", "a b", "a;b", "a\r\nb", "sé"])
def test_a_cookie_name_is_an_rfc_7230_token(name):
    # `a=b` used to go out as `a=b=v`: a cookie named `a` with the value `b=v`.
    with pytest.raises(ValueError, match="cookie name"):
        Cookie(name, "v")


@pytest.mark.parametrize("value", ["a b", "a,b", "a;b", "a\\b", 'a"b', "bad\r\nSet-Cookie: evil=1", "é"])
def test_a_cookie_value_is_cookie_octets(value):
    with pytest.raises(ValueError, match="cookie-octet"):
        Cookie("n", value)


def test_a_quoted_cookie_value_is_kept_as_given():
    assert _set_cookie(Cookie("n", '"quoted"')).startswith('n="quoted";')


@pytest.mark.parametrize("attribute", ["path", "domain"])
def test_path_and_domain_cannot_add_an_attribute(attribute):
    with pytest.raises(ValueError, match=attribute):
        Cookie("n", "v", **{attribute: "x; Secure"})


def test_same_site_none_needs_secure():
    with pytest.raises(ValueError, match="SameSite=None requires secure=True"):
        Cookie("n", "v", samesite=SameSite.NONE)
    assert "SameSite=None" in _set_cookie(Cookie("n", "v", samesite=SameSite.NONE, secure=True))


def test_a_cookie_is_a_value():
    assert Cookie("n", "v", max_age=1) == Cookie("n", "v", max_age=1)
    assert Cookie("n", "v") != Cookie("n", "w")
    assert len({Cookie("n", "v"), Cookie("n", "v")}) == 1
    assert str(Cookie("n", "v")) == "n=v; Path=/; SameSite=Lax"


# --- properties ---------------------------------------------------------------------------------

# RFC 7230 tchar and RFC 6265 cookie-octet.
TOKEN_CHARS = "!#$%&'*+-.^_`|~" + string.digits + string.ascii_letters
COOKIE_OCTETS = "".join(
    chr(c) for c in range(0x21, 0x7F) if chr(c) not in '",;\\'
)
names = st.text(alphabet=TOKEN_CHARS, min_size=1, max_size=12)
octets = st.text(alphabet=COOKIE_OCTETS, max_size=24)
# A value as a browser keeps it, and what the server reads back from it.
values = octets.map(lambda v: (v, v)) | octets.map(lambda v: (f'"{v}"', v))


def _pair(cookie: Cookie) -> str:
    """The `name=value` a browser sends back for a cookie set with this line."""
    return str(cookie).split(";", 1)[0]


@given(st.lists(st.tuples(names, values), min_size=1, max_size=6))
def test_every_cookie_set_reads_back_with_its_value_the_first_of_a_name_winning(jar):
    header = "; ".join(_pair(Cookie(name, sent)) for name, (sent, _) in jar)
    first: dict[str, str] = {}
    for name, (_, read) in jar:
        first.setdefault(name, read)

    assert get_cookies(FakeRequest(header)) == first


@given(st.text(max_size=16), st.text(max_size=16))
def test_any_name_and_value_make_one_clean_set_cookie_line_or_a_value_error(name, value):
    try:
        line = str(Cookie(name, value))
    except ValueError:
        return
    assert line.startswith(f"{name}={value};")
    assert not set(line) & {"\r", "\n", "\0"}


@given(st.text(max_size=64))
def test_any_cookie_header_parses_to_names_a_cookie_can_have(header):
    for name, value in get_cookies(FakeRequest(header)).items():
        assert name and name == name.strip() and ";" not in name and "=" not in name
        assert ";" not in value
