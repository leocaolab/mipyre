"""`HTTPException`: a handler, a hook, `req.json()` / `req.text()` or `parse_multipart` that
raises it answers its status, its headers and `{"detail": …}` on every runtime, logged once at
debug with no traceback; the engine's own refusals answer `{"detail": …}` too."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request

import pytest

from mipyre import HTTPException, MultipartError
from tests._helpers import bound_port, kill_session, read_file, text_appears

APP = os.path.join(os.path.dirname(__file__), "apps", "http_exception_app.py")
RUNTIMES = ("subinterp", "main", "async", "process")


class Server:
    def __init__(self, tmp_path, env: dict[str, str]):
        self.log_path = str(tmp_path / "server.log")
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                [sys.executable, "-m", "mipyre", "run", f"{APP}:app", "--port", "0"],
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                env={**os.environ, "MIPYRE_PROCESS_WORKERS": "1", **env},
            )
        self.read = read_file(self.log_path)
        self.port = bound_port(self.read, self.proc)

    def request(self, method: str, path: str, body: bytes | None = None, headers: dict | None = None):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=body, headers=headers or {}, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status, r.headers, r.read()
        except urllib.error.HTTPError as e:
            with e:
                return e.code, e.headers, e.read()

    def lines_of(self, path: str) -> list[str]:
        """The server's log lines about a request to `path`, once one has landed."""
        text_appears(self.read, f'"path":"{path}"', self.proc)
        return [line for line in self.read().splitlines() if f'"path":"{path}"' in line]

    def stop(self) -> None:
        kill_session(self.proc)


@pytest.fixture(scope="module", params=[{}, {"MIPYRE_TPC": "0"}], ids=["tpc", "pool"])
def srv(request, tmp_path_factory):
    s = Server(tmp_path_factory.mktemp("http_exception"), request.param)
    yield s
    s.stop()


def answered_once_at_debug(srv: Server, path: str) -> None:
    lines = srv.lines_of(path)
    assert len(lines) == 1, lines
    assert '"level":"DEBUG"' in lines[0] and "traceback" not in lines[0], lines[0]


@pytest.mark.parametrize("runtime", RUNTIMES)
def test_a_raised_http_exception_is_its_status_and_detail_logged_once_at_debug(srv, runtime):
    status, headers, body = srv.request("GET", f"/{runtime}/missing")
    assert status == 404, body
    assert headers["Content-Type"] == "application/json"
    assert json.loads(body) == {"detail": "no order 17"}
    if runtime != "process":  # the lane answers it as a response, and logs nothing of it
        answered_once_at_debug(srv, f"/{runtime}/missing")


@pytest.mark.parametrize("runtime", RUNTIMES)
def test_an_http_exception_carries_any_json_detail_and_its_headers(srv, runtime):
    status, headers, body = srv.request("GET", f"/{runtime}/busy")
    assert status == 503, body
    assert headers["Retry-After"] == "5"
    assert json.loads(body) == {"detail": {"retry": "later"}}


@pytest.mark.parametrize("runtime", RUNTIMES)
def test_an_http_exception_without_detail_says_the_status_phrase(srv, runtime):
    status, _, body = srv.request("GET", f"/{runtime}/bare")
    assert (status, json.loads(body)) == (404, {"detail": "Not Found"})


def test_a_before_request_hook_refuses_with_its_status_and_headers_and_the_handler_does_not_run(srv):
    status, headers, body = srv.request("GET", "/guarded")
    assert status == 401, body
    assert headers["WWW-Authenticate"] == "Bearer"
    assert json.loads(body) == {"detail": "login required"}


def test_a_detail_that_is_not_json_is_the_raisers_bug_a_generic_500_with_a_traceback(srv):
    status, _, body = srv.request("GET", "/unencodable")
    assert status == 500, body
    assert json.loads(body)["detail"] == "Internal Server Error"
    [line] = srv.lines_of("/unencodable")
    assert '"level":"ERROR"' in line and "could not be encoded as JSON" in line, line
    assert "HTTPException" in line and "traceback" in line, line


def test_an_unhandled_exception_is_a_generic_500_that_keeps_its_text_out_of_the_body(srv):
    status, _, body = srv.request("GET", "/secret")
    assert status == 500
    reply = json.loads(body)
    assert reply == {"detail": "Internal Server Error", "request_id": reply["request_id"]}
    assert b"secret" not in body
    [line] = srv.lines_of("/secret")
    assert '"level":"ERROR"' in line and "secret 5b1e" in line, line


# --- CUJ-2: a client's bad body --------------------------------------------------------------


@pytest.mark.parametrize("runtime", RUNTIMES)
def test_a_client_sending_bad_json_to_req_json_gets_a_4xx(srv, runtime):
    status, _, body = srv.request(
        "POST", f"/{runtime}/json", b"{not json", {"Content-Type": "application/json"}
    )
    assert status == 400, body
    assert json.loads(body)["detail"].startswith("the body is not valid JSON: "), body
    if runtime != "process":
        answered_once_at_debug(srv, f"/{runtime}/json")


@pytest.mark.parametrize("runtime", RUNTIMES)
def test_req_json_decodes_a_json_body(srv, runtime):
    doc = {"a": [1, 2.5, "x"], "b": None}
    status, _, body = srv.request(
        "POST", f"/{runtime}/json", json.dumps(doc).encode(), {"Content-Type": "application/json"}
    )
    assert (status, json.loads(body)) == (200, {"got": doc}), body


def test_a_body_that_is_not_utf8_is_a_400_from_req_text(srv):
    status, _, body = srv.request("POST", "/text", b"\xff\xfe", {"Content-Type": "text/plain"})
    assert status == 400, body
    assert json.loads(body)["detail"].startswith("the body is not UTF-8: "), body
    answered_once_at_debug(srv, "/text")


def test_a_malformed_multipart_body_is_a_400_and_too_many_parts_a_413(srv):
    form = "multipart/form-data; boundary=b"
    status, _, body = srv.request("POST", "/upload", b"no delimiter here", {"Content-Type": form})
    assert status == 400, body
    assert "no delimiter line --b" in json.loads(body)["detail"]
    parts = b"".join(
        b'--b\r\nContent-Disposition: form-data; name="f%d"\r\n\r\nx\r\n' % i for i in range(3)
    ) + b"--b--\r\n"
    status, _, body = srv.request("POST", "/upload", parts, {"Content-Type": form})
    assert status == 413, body
    assert "max_parts=2" in json.loads(body)["detail"]


# --- the engine's own refusals ---------------------------------------------------------------


def test_the_engines_own_refusals_answer_detail(srv):
    status, _, body = srv.request("POST", "/text", b"x" * 2048, {"Content-Type": "text/plain"})
    assert (status, json.loads(body)) == (413, {"detail": "payload too large"})
    status, _, body = srv.request("GET", "/nowhere")
    assert (status, json.loads(body)) == (404, {"detail": "not found"})


# --- the type --------------------------------------------------------------------------------


def test_the_fastapi_signature_and_attributes():
    e = HTTPException(404, "gone", headers={"X-A": "1"})
    assert (e.status_code, e.detail, e.headers) == (404, "gone", {"x-a": "1"})
    assert str(e) == "404: gone"
    assert repr(e) == "HTTPException(status_code=404, detail='gone')"
    assert HTTPException(422).detail == "Unprocessable Content"
    assert HTTPException(499).detail is None  # no standard phrase
    assert HTTPException(500, None).detail == "Internal Server Error"
    assert isinstance(e, Exception)


@pytest.mark.parametrize("status", [399, 600, 200, -1])
def test_a_status_outside_4xx_and_5xx_is_refused(status):
    with pytest.raises(ValueError, match="400..=599"):
        HTTPException(status)


@pytest.mark.parametrize("headers", [{"X-A": "a\r\nSet-Cookie: x=1"}, {"X-A": "a\0"}, {"bad name": "v"}])
def test_a_header_that_could_split_the_response_is_refused(headers):
    with pytest.raises(ValueError, match="HTTPException header"):
        HTTPException(400, "x", headers=headers)


def test_multipart_error_is_an_http_exception_raised_with_its_status():
    e = MultipartError(413, "too many")
    assert isinstance(e, HTTPException) and not isinstance(e, ValueError)
    assert (e.status_code, e.detail) == (413, "too many")
