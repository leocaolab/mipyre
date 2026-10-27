"""Tests for TestClient v2 — params, cookies, redirects, OPTIONS/HEAD,
response helpers (.ok, .raise_for_status, .json).

The app is module level: TestClient serves it through sub-interpreter workers, which
rebuild it by executing this module."""

from __future__ import annotations

import signal
import subprocess
import sys

import pytest

from mipyre import Mipyre, Response
from mipyre.testing import TestClient

app = Mipyre()


@app.get("/q")
def q(req):
    return {"query": req.query}


@app.post("/login")
def login(req):
    return Response(
        body="",
        headers={"Set-Cookie": "sid=abc123; Path=/"},
    )


@app.get("/whoami")
def whoami(req):
    return {"cookie": req.headers.get("cookie", "")}


@app.get("/from")
def redir(req):
    return Response(body="", status_code=302, headers={"Location": "/to"})


@app.get("/to")
def dest(req):
    return "landed"


@app.options("/thing")
def opts(req):
    return Response(body="", status_code=204, headers={"Allow": "GET, POST"})


@app.head("/ping")
def head(req):
    return "pong"


@app.get("/")
def root(req):
    return "ok"


@app.get("/bad")
def bad(req):
    return Response(body="boom", status_code=500)


@app.get("/nums")
def nums(req):
    return [1, 2, 3]


@app.route("PATCH", "/thing")
def patch(req):
    return "patched"


# ---------------------------------------------------------------------------
# params=
# ---------------------------------------------------------------------------


def test_params_dict_is_urlencoded():
    with TestClient(app) as c:
        r = c.get("/q", params={"limit": 10, "tag": "hello world"})
        assert r.status_code == 200
        q_str = r.json()["query"]
        assert "limit=10" in q_str
        assert "tag=hello+world" in q_str or "tag=hello%20world" in q_str


def test_params_merges_with_existing_query():
    with TestClient(app) as c:
        r = c.get("/q?from=path", params={"add": "param"})
        q_str = r.json()["query"]
        assert "from=path" in q_str
        assert "add=param" in q_str


def test_params_doseq_list():
    with TestClient(app) as c:
        r = c.get("/q", params={"tag": ["a", "b"]})
        q_str = r.json()["query"]
        assert "tag=a" in q_str and "tag=b" in q_str


# ---------------------------------------------------------------------------
# Cookies
# ---------------------------------------------------------------------------


def test_cookies_persist_across_requests():
    with TestClient(app) as c:
        c.post("/login")
        r = c.get("/whoami")
        # Server-set cookie was echoed on the next request.
        assert "sid=abc123" in r.json()["cookie"]


# ---------------------------------------------------------------------------
# Redirects
# ---------------------------------------------------------------------------


def test_follows_redirects_by_default():
    with TestClient(app) as c:
        r = c.get("/from")
        # urllib followed automatically.
        assert r.status_code == 200
        assert r.text == "landed"


def test_can_disable_redirect_following():
    with TestClient(app, follow_redirects=False) as c:
        r = c.get("/from")
        assert r.status_code == 302
        loc = r.headers.get("Location") or r.headers.get("location")
        assert loc == "/to"


# ---------------------------------------------------------------------------
# OPTIONS / HEAD
# ---------------------------------------------------------------------------


def test_options_method():
    with TestClient(app) as c:
        r = c.options("/thing")
        assert r.status_code == 204
        # urllib lowercases header names in some versions; compare case-insensitively.
        allow = r.headers.get("Allow") or r.headers.get("allow")
        assert allow == "GET, POST"


def test_head_method():
    with TestClient(app) as c:
        r = c.head("/ping")
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# TestResponse helpers
# ---------------------------------------------------------------------------


def test_response_ok_true_on_2xx():
    with TestClient(app) as c:
        r = c.get("/")
        assert r.ok is True


def test_response_ok_false_on_4xx():
    from tests.apps.testclient_v2_not_found import app as not_found_app

    with TestClient(not_found_app) as c:
        r = c.get("/")
        assert r.ok is False


def test_raise_for_status_raises_on_error():
    with TestClient(app) as c:
        r = c.get("/bad")
        with pytest.raises(RuntimeError):
            r.raise_for_status()


def test_raise_for_status_noop_on_success():
    with TestClient(app) as c:
        r = c.get("/")
        r.raise_for_status()  # must not raise


def test_json_accepts_list_response():
    with TestClient(app) as c:
        r = c.get("/nums")
        assert r.json() == [1, 2, 3]


# ---------------------------------------------------------------------------
# Low-level request()
# ---------------------------------------------------------------------------


def test_request_method_accepts_arbitrary_verbs():
    with TestClient(app) as c:
        r = c.request("PATCH", "/thing")
        assert r.text == "patched"


# ---------------------------------------------------------------------------
# A stop signal that came between two servers
# ---------------------------------------------------------------------------

_BETWEEN_RUNS = """
import os
import signal
from mipyre import Mipyre
from mipyre.testing import TestClient

# The process handles SIGTERM itself, as it handles SIGINT: with the default action the signal
# between the servers would end it.
signal.signal(signal.SIGTERM, lambda signum, frame: None)
app = Mipyre(runtime="main")

@app.get("/")
def index(req):
    return "ok"

with TestClient(app) as c:
    assert c.get("/").status_code == 200
# No server runs now; the signals the first one's watcher registered still reach the process.
for signum in (signal.SIGINT, signal.SIGTERM):
    try:
        os.kill(os.getpid(), signum)
    except KeyboardInterrupt:
        pass
with TestClient(app) as c:
    print(c.get("/").status_code)
"""


def test_a_stop_signal_that_came_before_a_server_started_does_not_stop_it(tmp_path):
    # Its own process: a signal reaches every thread of the process it is sent to.
    script = tmp_path / "between_runs.py"
    script.write_text(_BETWEEN_RUNS)
    done = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, timeout=120)
    assert done.returncode == 0, done.stdout + done.stderr
    assert done.stdout.splitlines()[-1] == "200", done.stdout + done.stderr


_AFTER_A_RUN = """
import os
import signal
import sys
from mipyre import Mipyre
from mipyre.testing import TestClient

app = Mipyre(runtime="main")

@app.get("/")
def index(req):
    return "ok"

with TestClient(app) as c:
    assert c.get("/").status_code == 200
print("served", flush=True)
try:
    os.kill(os.getpid(), getattr(signal, sys.argv[1]))
    signal.pause()
    print("survived", flush=True)
except KeyboardInterrupt:
    print("KeyboardInterrupt", flush=True)
"""


@pytest.mark.parametrize(
    ("signum", "returncode", "last_line"),
    [(signal.SIGTERM, -signal.SIGTERM, "served"), (signal.SIGINT, 0, "KeyboardInterrupt")],
    ids=["sigterm", "sigint"],
)
def test_a_stop_signal_after_a_server_ran_is_handled_as_before_it(tmp_path, signum, returncode, last_line):
    # SIGTERM takes its default action and ends the process; SIGINT is Python's KeyboardInterrupt.
    script = tmp_path / "after_a_run.py"
    script.write_text(_AFTER_A_RUN)
    done = subprocess.run([sys.executable, str(script), signum.name], capture_output=True, text=True, timeout=120)
    assert done.returncode == returncode, done.stdout + done.stderr
    assert done.stdout.splitlines()[-1] == last_line, done.stdout + done.stderr


_HANDLER_SWAPPED_BETWEEN_RUNS = """
import os
import signal
import threading
from mipyre import Mipyre
from mipyre._server_run import ServerRun, Transport
from mipyre.engine import ServerFlags
from mipyre.testing import TestClient

app = Mipyre(runtime="main")

@app.get("/")
def index(req):
    return "ok"

with TestClient(app) as c:
    assert c.get("/").status_code == 200

handled = threading.Event()
signal.signal(signal.SIGTERM, lambda signum, frame: handled.set())
run = ServerRun.in_test_process(app, ServerFlags(), transport=Transport.MEMORY)
run.serve(on_serving=lambda: os.kill(os.getpid(), signal.SIGTERM))
print("stopped", handled.is_set(), flush=True)
os.kill(os.getpid(), signal.SIGTERM)
handled.wait()
print("handled", flush=True)
"""


def test_a_run_after_the_sigterm_handler_was_replaced_still_stops_on_sigterm(tmp_path):
    # The second run owns SIGTERM, so the handler installed before it is not called for the
    # signal that stopped it, and handles SIGTERM again once the run has ended.
    script = tmp_path / "handler_swapped.py"
    script.write_text(_HANDLER_SWAPPED_BETWEEN_RUNS)
    done = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, timeout=120)
    assert done.returncode == 0, done.stdout + done.stderr
    assert done.stdout.splitlines()[-2:] == ["stopped False", "handled"], done.stdout + done.stderr
