"""A `Response`'s header lines go out, and read back, in the order the handler gave them,
wherever its `Content-Type` is among them."""

from mipyre import Mipyre, Response
from mipyre.testing import TestClient

# Module level: TestClient serves it through sub-interpreter workers, which rebuild it by
# executing this module.
app = Mipyre()

GIVEN = {"x-a": "1", "Content-Type": "text/html", "x-b": "2", "x-c": "3"}


@app.get("/typed-between")
def typed_between(req):
    return Response("hi", headers=GIVEN)


def test_the_lines_go_out_in_the_handlers_order():
    with TestClient(app) as client:
        r = client.get("/typed-between")
    assert r.status_code == 200
    assert r.headers["content-type"] == "text/html"
    ours = [name for name in r.headers if name.startswith("x-")]
    assert ours == ["x-a", "x-b", "x-c"]


def test_the_headers_read_back_as_given():
    resp = Response("hi", headers=GIVEN)
    assert list(resp.headers.items()) == [
        ("x-a", "1"),
        ("content-type", "text/html"),
        ("x-b", "2"),
        ("x-c", "3"),
    ]
    assert resp.content_type == "text/html"


def test_a_type_given_last_or_alone_reads_back_where_it_was():
    assert list(Response("hi", headers={"x-a": "1", "content-type": "t"}).headers) == ["x-a", "content-type"]
    assert dict(Response("hi", headers={"content-type": "t"}).headers) == {"content-type": "t"}
    assert dict(Response("hi", headers={"x-a": "1"}).headers) == {"x-a": "1"}
    assert Response("hi", headers={"x-a": "1"}, content_type="text/csv").content_type == "text/csv"
