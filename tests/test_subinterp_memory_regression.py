"""Sub-interpreter memory behavior regression tests.

Covers the full leak story from 2026-04-19:

Yesterday's fixes (v1.4.5 security + correctness):
  - Cookie CRLF injection rejection
  - router.lookup case-insensitive
  - Sub-interp accept() errno backoff
  - etc. — see CHANGELOG v1.4.5

Today's fixes (hot-path leak):
  - a reference is released with its own interpreter's thread state current
  - call_handler uses PyObject_Vectorcall (was PyObject_Call)
  - build_request uses PyObject_Vectorcall (was PyObject_Call)
  - build_request does manual Py_DECREF on each arg to compensate the
    _PyObject_MakeTpCall fallback leak

Instead of restarting a server every test, this module uses one long-
lived server with many small routes that report instrumented counters.
The tests that count what requests leave behind use a second one with a
single worker (`one_interpreter`): every request, the counting one
included, is served by the same sub-interpreter, so the count before and
after is of the interpreter that did the work, and it is exact.

Why measured on a running server: the regressions are leaks — objects and blocks that outlive a
request in the worker's own interpreter, counted by that interpreter (`gc`, `sys.getallocatedblocks`).
Whether a reference is released depends on CPython's runtime, not on a value a function returns.

The thread state a cross-thread attach made and never freed (`replace_creator_tstate`, fc45a7f)
is raw malloc, which these counts don't see: `crates/subinterp/src/attach.rs` counts the
interpreter's thread states before and after N visits from other threads instead (b/441). The
12 s concurrent RSS soak that guarded it here runs before a release (`dev/release_soak.sh`).
"""

from __future__ import annotations

import contextlib
import http.client
import json
import os
import signal
import subprocess
import sys
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from tests._helpers import answers, bound_port, read_file

HOST = "127.0.0.1"
# The server binds port 0; the `server` fixture sets this to the port it bound.
_server_port: int | None = None


SERVER_SCRIPT = '''
import gc, sys, os
from mipyre import Mipyre
from mipyre.cookies import set_cookie
from mipyre.engine import Response

app = Mipyre()

# --- In-sub-interp finalizer counter (each sub-interp has its own) ------
_probe_created = [0]
_probe_finalized = [0]
class _Probe:
    __slots__ = ("x",)
    def __init__(self, x):
        self.x = x
        _probe_created[0] += 1
    def __del__(self):
        _probe_finalized[0] += 1

# --- Routes used by the memory-behavior tests ---------------------------

@app.get("/")
def index(req):
    return "ok"

@app.get("/read_headers")
def read_headers(req):
    # req.headers is a view of the request's fields; this makes the request's headers dict.
    headers = dict(req.headers.items())
    return "ok" if "host" in headers else "no host"

# What a leaking handler looks like: it keeps every request's headers dict.
_kept_headers = []

@app.get("/keep_headers")
def keep_headers(req):
    _kept_headers.append(dict(req.headers.items()))
    return "ok"

# And one that keeps every request.
_kept_requests = []

@app.get("/keep_request")
def keep_request(req):
    _kept_requests.append(req)
    return "ok"

@app.get("/census")
def census(req):
    # What this interpreter holds after a full collection: dicts that look like a
    # request's headers (2+ keys, one of them 'host'), its object allocator's live blocks,
    # and its live Request instances. Request is not GC-tracked, so gc.get_objects() never
    # lists it; every live instance holds one reference to its type, so the type's refcount
    # rises and falls with them (this census's own request included, before and after alike).
    for _ in range(2):
        gc.collect()
    header_dicts = sum(
        1 for o in gc.get_objects()
        if isinstance(o, dict) and len(o) >= 2 and "host" in o
    )
    return {
        "header_dicts": header_dicts,
        "blocks": sys.getallocatedblocks(),
        "requests": sys.getrefcount(type(req)),
    }

@app.get("/slot_attack")
def slot_attack(req):
    # Attempt to smuggle a user attribute past the Rust SlotClearer.
    # Strict __slots__ must reject this at runtime — otherwise the
    # attribute would leak past the sub-interp dealloc bug.
    from mipyre.engine import Response
    results = {}
    try:
        req.arbitrary_user_attr = "would leak"
        results["request_accepts_arbitrary_attr"] = True
    except AttributeError:
        results["request_accepts_arbitrary_attr"] = False
    resp = Response(body="ok")
    try:
        resp.arbitrary_user_attr = "would leak"
        results["response_accepts_arbitrary_attr"] = True
    except AttributeError:
        results["response_accepts_arbitrary_attr"] = False
    return results

@app.get("/probe_stats")
def probe_stats(req):
    # Does __del__ fire in a sub-interp? Create + throw away N instances,
    # force gc, then report created vs finalized.
    before_created = _probe_created[0]
    before_final = _probe_finalized[0]
    for i in range(50):
        p = _Probe(i)
        del p
    for _ in range(3):
        gc.collect()
    return {
        "created_delta": _probe_created[0] - before_created,
        "finalized_delta": _probe_finalized[0] - before_final,
    }

# --- Routes from yesterday's fixes --------------------------------------

@app.get("/cookie_ok")
def cookie_ok(req):
    return set_cookie(Response(body="ok"), "session", "abc123")

@app.get("/cookie_crlf")
def cookie_crlf(req):
    # Should raise ValueError — CRLF in cookie value. Build the value
    # with chr() to avoid escape-sequence confusion through the triple-
    # quoted outer template.
    bad_value = "bad" + chr(13) + chr(10) + "Set-Cookie: evil=1"
    try:
        set_cookie(Response(body="ok"), "x", bad_value)
        return {"rejected": False}
    except ValueError as e:
        return {"rejected": True, "err": str(e)[:60]}

@app.get("/lowercase_route_match")
def lowercase_route_match(req):
    # router.lookup is case-insensitive now
    return "matched"

if __name__ == "__main__":
    app.run(host="{HOST}", port={PORT}, workers={WORKERS})
'''


@contextlib.contextmanager
def _serving(workers: int | None) -> Iterator[int]:
    """A server running SERVER_SCRIPT with `workers` worker threads (None: the server's
    default), until the block ends; yields the port it bound."""
    script_path = f"/tmp/mipyre_regression_{os.getpid()}_{workers}.py"
    Path(script_path).write_text(
        SERVER_SCRIPT.replace("{HOST}", HOST).replace("{PORT}", "0").replace("{WORKERS}", str(workers))
    )
    env = dict(os.environ)
    log_path = script_path + ".log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, script_path],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        )
    try:
        port = bound_port(read_file(log_path), proc)
    except RuntimeError as e:
        proc.kill()
        pytest.fail(f"Mipyre server did not start:\n{e}")
    try:
        answers(f"http://{HOST}:{port}/", proc, read_file(log_path))
    except RuntimeError as e:
        pytest.fail(f"Mipyre server did not start:\n{e}")

    try:
        yield port
    finally:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (OSError, subprocess.TimeoutExpired):
                pass
        for path in (script_path, log_path):
            try:
                os.unlink(path)
            except OSError:
                pass


@pytest.fixture(scope="module")
def server():
    """One long-lived hybrid-mode server shared across this module's tests."""
    global _server_port
    with _serving(None) as port:
        _server_port = port
        yield


@pytest.fixture(scope="module")
def one_interpreter() -> Iterator[int]:
    """A server with one worker, so one sub-interpreter serves every request; yields its port."""
    with _serving(1) as port:
        yield port


def _get(path: str) -> dict:
    url = f"http://{HOST}:{_server_port}{path}"
    with urllib.request.urlopen(url, timeout=None) as r:
        data = r.read()
    try:
        return json.loads(data)
    except ValueError:
        return {"raw": data.decode("utf-8", errors="replace")}


# Requests a leak count is taken over: a handler that leaks one object per request leaves this many.
_REQUESTS = 200
# Requests to the same route before the counts, so first-use caches and free lists are filled.
_WARM_UP = 50


@dataclass(frozen=True)
class Census:
    """What the one-worker server's interpreter holds after a full collection (`/census`):
    dicts that look like a request's headers, its object allocator's live blocks
    (`sys.getallocatedblocks()`), and its live `Request` instances (their type's refcount)."""

    header_dicts: int
    blocks: int
    requests: int


def _census(port: int) -> Census:
    with urllib.request.urlopen(f"http://{HOST}:{port}/census", timeout=None) as r:
        counts = json.loads(r.read())
    return Census(header_dicts=counts["header_dicts"], blocks=counts["blocks"], requests=counts["requests"])


def _left_behind(port: int, path: str, requests: int) -> Census:
    """What `requests` serial requests to `path` leave in the one-worker server's interpreter:
    its census after them minus its census before. The route and the census are warmed up
    first, so the two counts are taken by requests alike and neither is a first call: a server
    that frees everything a request made shows zero. The requests go over one kept-alive
    connection."""
    conn = http.client.HTTPConnection(HOST, port, timeout=None)

    def serve(n: int) -> None:
        for _ in range(n):
            conn.request("GET", path)
            resp = conn.getresponse()
            resp.read()
            assert resp.status == 200, (path, resp.status)

    try:
        serve(_WARM_UP)
        _census(port)
        before = _census(port)
        serve(requests)
        after = _census(port)
    finally:
        conn.close()
    return Census(
        header_dicts=after.header_dicts - before.header_dicts,
        blocks=after.blocks - before.blocks,
        requests=after.requests - before.requests,
    )


# ─────────────────────────────────────────────────────────────────
# Yesterday's v1.4.5 security / correctness
# ─────────────────────────────────────────────────────────────────


def test_cookie_crlf_injection_rejected(server):
    """Security: Set-Cookie with CRLF in value must raise ValueError."""
    r = _get("/cookie_crlf")
    assert r["rejected"] is True, (
        "cookie value containing \\r\\n must be rejected (HTTP Response Splitting)"
    )


def test_cookie_plain_still_works(server):
    r = _get("/cookie_ok")
    assert r.get("raw") == "ok"


def test_router_case_insensitive_for_lowercase_method(server):
    """Correctness: router.lookup must normalize method (RFC 9110 §9.1)."""
    # Request with a mixed-case 'Get' should still route. urllib always
    # sends canonical upper-case 'GET', so exercise via raw socket.
    import socket

    s = socket.create_connection((HOST, _server_port), timeout=5)
    s.sendall(
        b"get /lowercase_route_match HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"Connection: close\r\n\r\n"
    )
    body = b""
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        body += chunk
    s.close()
    assert b"200 OK" in body or b"matched" in body, (
        f"lowercase 'get' must still match route, got: {body[:200]!r}"
    )


# ─────────────────────────────────────────────────────────────────
# Today's fix: _Request does not leak per-request
# ─────────────────────────────────────────────────────────────────


def test_request_response_lock_out_dynamic_attrs(server):
    """Defence in depth: the Rust SlotClearer hardcodes the seven
    __slots__ names it clears. If a user handler could do
    `request.my_thing = payload`, that attribute would bypass every
    cleanup path and leak past the sub-interp dealloc bug. Strict
    __slots__ (no __dict__) on both `_Request` and `_Response`
    physically prevents that by raising AttributeError at runtime.

    This test asserts the closed-world invariant: both classes MUST
    reject arbitrary attribute assignment. A regression here reopens
    the leak surface for user code, even though the plumbing would
    still pass the other tests.
    """
    r = _get("/slot_attack")
    assert r["request_accepts_arbitrary_attr"] is False, (
        "_Request lost its __slots__ discipline — user handlers "
        "can now stash arbitrary attributes on the request, and those "
        "will leak past the Rust SlotClearer."
    )
    assert r["response_accepts_arbitrary_attr"] is False, (
        "_Response lost its __slots__ discipline — same failure "
        "mode as the request case."
    )


def test_mipyrerequest_does_not_accumulate(one_interpreter):
    """A request does not outlive itself: after `_REQUESTS` requests the interpreter that served
    them all holds exactly as many `Request`s as before, and twice the requests leave the same
    (none). Before the Vectorcall fix every request leaked one. Until b/441 this was 2 050 serial
    requests, one connection each, over many workers, the worst of 30 probes under the baseline
    + 20."""
    once = _left_behind(one_interpreter, "/", _REQUESTS)
    assert once.requests == 0, f"{once.requests} more Requests alive after {_REQUESTS} requests"
    assert _left_behind(one_interpreter, "/", 2 * _REQUESTS).requests == once.requests


def test_left_behind_counts_the_requests_a_handler_keeps(one_interpreter):
    """The count above sees a leak: a handler that keeps every request leaves exactly one per
    request, and twice the requests twice as many."""
    once = _left_behind(one_interpreter, "/keep_request", _REQUESTS)
    assert once.requests == _REQUESTS
    assert _left_behind(one_interpreter, "/keep_request", 2 * _REQUESTS).requests == 2 * once.requests


# ─────────────────────────────────────────────────────────────────
# Known-issue tests (xfail) — lock in the bug fingerprint for future fixers
# ─────────────────────────────────────────────────────────────────


def test_subinterp_python_finalizers_fire(server):
    """Python __del__ / tp_finalize runs in sub-interp handlers.

    Historically xfail'd because Mipyre's sub-interp workers reused the
    creator-thread's tstate across OS threads, and CPython's per-OS-
    thread finalizer bookkeeping silently stopped firing. Fixed by
    `replace_creator_tstate`: each worker creates a fresh
    thread-local tstate via `PyThreadState_New(interp)` on first
    entry. Finalizers now fire normally.
    """
    stats = _get("/probe_stats")
    # 50 probes created, all immediately `del`'d. __del__ should fire
    # for all 50 after gc.collect().
    assert stats["finalized_delta"] >= stats["created_delta"] - 5


def test_headers_dicts_do_not_accumulate(one_interpreter):
    """A request's headers dict does not outlive the request.

    `req.headers` is a view of the request's fields; the route reads them into a dict, the
    one a handler holds. After `_REQUESTS` requests the interpreter that served them all
    holds exactly as many such dicts as before: each one kept past its request adds one.
    """
    left = _left_behind(one_interpreter, "/read_headers", _REQUESTS)
    assert left.header_dicts == 0, (
        f"headers dict leak — {left.header_dicts} more headers dicts alive after "
        f"{_REQUESTS} requests than before them."
    )


def test_interpreter_memory_does_not_grow_per_request(one_interpreter):
    """Serving a request leaves nothing behind in the interpreter that served it.

    After `_REQUESTS` requests the interpreter's object allocator holds exactly as many
    blocks as before them: the request, its handler's call, the response and everything
    they made are freed when the request ends. An object kept per request adds at least
    `_REQUESTS` blocks.
    """
    left = _left_behind(one_interpreter, "/", _REQUESTS)
    assert left.blocks == 0, (
        f"the interpreter holds {left.blocks} more blocks after {_REQUESTS} requests "
        f"than before them ({left.blocks / _REQUESTS:.2f} per request)."
    )


def test_left_behind_counts_what_a_handler_keeps(one_interpreter):
    """The two counts above see a leak: a handler that keeps every request's headers dict
    leaves exactly one headers dict, and at least one block, per request."""
    left = _left_behind(one_interpreter, "/keep_headers", _REQUESTS)
    assert left.header_dicts == _REQUESTS
    assert left.blocks >= _REQUESTS
