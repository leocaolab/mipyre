"""Final review, fix wave A, W4 (package & data): docs/design/code-review-final-rubric.md.

Each test names the item it covers. Server tests run a real server, either in a
subprocess on the serving path they name (`serve`, from test_review_ra: "gil", "tpc",
"pool") or in-process with `TestClient` on a `Mipyre(runtime="main")` app; both bind
ports the kernel picks.
The Postgres tests need `MIPYRE_TEST_PG_DSN` (CI and the supervisor use
postgres://hucao@localhost:5432/mipyre_test); their tables are prefixed `w4_`.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import os
import subprocess
import sys
import textwrap
import threading

import pytest

from mipyre import Mipyre, Response, Stream
from mipyre.testing import TestClient, TestServer
from tests.test_review_ra import RUN, serve

PYTHON = sys.executable
PG_DSN = os.environ.get("MIPYRE_TEST_PG_DSN")
needs_pg = pytest.mark.skipif(PG_DSN is None, reason="MIPYRE_TEST_PG_DSN not set")


def _run_script(body: str, env: dict[str, str] | None = None, timeout: float = 30):
    """A snippet in a fresh process (the tracing subscriber, the metrics switch and the
    Postgres pool are process-wide)."""
    full_env = dict(os.environ)
    full_env.update(env or {})
    return subprocess.run(
        [PYTHON, "-c", textwrap.dedent(body)],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=full_env,
    )


# ---------------------------------------------------------------------------
# F1 / G3: /mcp and @app.rpc take only the body types they decode; the engine refuses
# cross-site requests before them (docs/design/cross-site-check.md)
# ---------------------------------------------------------------------------

CSRF_SCRIPT = """
import os
from mipyre import Mipyre, rpc
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.enable_cors(allow_origins=["https://app.example.com"])

@rpc(app)
def echo(data):
    return data

from mipyre.mcp import MCPServer
mcp = MCPServer(app)

@mcp.tool()
def ping() -> str:
    return "pong"
""" + RUN

MCP_LIST = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).encode()


@pytest.fixture(scope="module", params=["gil", "tpc"])
def csrf_srv(request):
    with serve(CSRF_SCRIPT, request.param) as srv:
        yield srv


@pytest.mark.parametrize("target,body", [("/rpc/echo", b'{"a": 1}'), ("/mcp", MCP_LIST)])
def test_f1_simple_request_body_types_are_415(csrf_srv, target, body):
    for headers in ({"Content-Type": "text/plain"},
                    {"Content-Type": "application/x-www-form-urlencoded"},
                    {}):
        r = csrf_srv.post(target, body, headers=headers)
        assert r.status == 415, (headers, r.status, r.body)
        assert b"Content-Type must be" in r.body, r.body


def test_f1_rpc_cross_site_origin_is_403_with_the_reason(csrf_srv):
    r = csrf_srv.post("/rpc/echo", b'{"a": 1}', headers={
        "Content-Type": "application/json", "Origin": "https://evil.example"})
    assert r.status == 403, r.body
    assert "https://evil.example" in r.json()["detail"]


def test_f1_mcp_cross_site_origin_is_403_before_the_route(csrf_srv):
    r = csrf_srv.post("/mcp", MCP_LIST, headers={
        "Content-Type": "application/json", "Origin": "null"})
    assert r.status == 403, r.body
    assert '"null"' in r.json()["detail"], r.body


@pytest.mark.parametrize("origin", [None, "same-host", "https://app.example.com"])
def test_f1_no_origin_same_host_and_trusted_origins_are_admitted(csrf_srv, origin):
    headers = {"Content-Type": "application/json; charset=utf-8"}
    if origin == "same-host":
        headers["Origin"] = f"http://127.0.0.1:{csrf_srv.port}"
    elif origin is not None:
        headers["Origin"] = origin
    r = csrf_srv.post("/rpc/echo", b'{"a": 1}', headers=headers)
    assert r.status == 200, r.body
    assert r.json() == {"a": 1}
    r = csrf_srv.post("/mcp", MCP_LIST, headers=headers)
    assert r.status == 200, r.body
    assert [t["name"] for t in r.json()["result"]["tools"]] == ["ping"]


def test_f1_rpc_still_takes_msgpack(csrf_srv):
    msgpack = pytest.importorskip("msgpack")
    r = csrf_srv.post("/rpc/echo", msgpack.packb({"a": 2}), headers={
        "Content-Type": "application/msgpack", "Accept": "application/msgpack"})
    assert r.status == 200, r.body
    assert msgpack.unpackb(r.body) == {"a": 2}


def test_f1_allow_origins_entries_are_origins():
    app = Mipyre()
    for bad in (["not an origin"], ["ftp://x.example"], ["https://x.example/p"], ["javascript:alert(1)"], ["null"]):
        with pytest.raises(ValueError, match="not an origin"):
            app.enable_cors(allow_origins=bad)
    with pytest.raises(TypeError):
        app.enable_cors(allow_origins=[3])
    with pytest.raises(TypeError):
        app.enable_cors()
    app.enable_cors(allow_origins=["https://app.example.com:443", "http://localhost:3000"])


# ---------------------------------------------------------------------------
# F2: req.json() decodes with isojson; wide integers stay exact, floats round-trip
# ---------------------------------------------------------------------------

JSON_SCRIPT = """
import os
from mipyre import HTTPException, Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.post("/decode")
def decode(req):
    try:
        value = req.json()
    except HTTPException as e:
        return {"error": type(e.__cause__).__name__}
    return {"repr": repr(value)}
""" + RUN

WIDE = [2**64, 2**64 + 1, -(2**63) - 1, 10**40, -(10**30), 2**63 - 1, -(2**63)]
FLOATS = [0.1, 1e-300, 5e-324, 1.7976931348623157e308, 123456789.12345678, -0.0, 1.0]


@pytest.mark.parametrize("path", ["gil", "pool", "tpc"])
def test_f2_json_keeps_wide_integers_exact_and_floats_round_trip(path):
    doc = {"ints": WIDE, "floats": FLOATS, "text": "12345678901234567890123"}
    with serve(JSON_SCRIPT, path) as srv:
        r = srv.post("/decode", json.dumps(doc).encode(),
                     headers={"Content-Type": "application/json"})
        assert r.status == 200, r.body
        assert r.json()["repr"] == repr(doc)

        # No wide digit run: isojson's own path.
        narrow = {"floats": FLOATS, "a": [1, -3, 2**63 - 1], "b": None, "c": True}
        r = srv.post("/decode", json.dumps(narrow).encode())
        assert r.json()["repr"] == repr(narrow)

        for bad in (b"{bad", b"NaN", b"[1e400]", b"[12345678901234567890, NaN]"):
            r = srv.post("/decode", bad)
            assert r.json().get("error") in ("JSONDecodeError", "ValueError"), (bad, r.body)


@needs_pg
def test_f2_jsonb_column_keeps_wide_integers_exact():
    r = _run_script(f"""
        from mipyre.db import PgPool
        pool = PgPool.connect({PG_DSN!r})
        pool.execute("DROP TABLE IF EXISTS w4_json")
        pool.execute("CREATE TABLE w4_json (id int PRIMARY KEY, doc jsonb)")
        try:
            pool.execute(
                "INSERT INTO w4_json VALUES (1, $1::text::jsonb)",
                '{{"big": 18446744073709551616, "neg": -9223372036854775809, "f": 0.1}}',
            )
            doc = pool.fetch_one("SELECT doc FROM w4_json WHERE id = 1")["doc"]
            assert doc == {{"big": 2**64, "neg": -(2**63) - 1, "f": 0.1}}, doc
            assert type(doc["big"]) is int and type(doc["neg"]) is int, doc
            print("OK")
        finally:
            pool.execute("DROP TABLE w4_json")
    """)
    assert "OK" in r.stdout, (r.stdout, r.stderr)


# ---------------------------------------------------------------------------
# F5: @cached_json on a handler with path params
# ---------------------------------------------------------------------------

CACHE_SCRIPT = """
import os
from mipyre import Mipyre
from mipyre.cache import cached_json
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
CALLS = []

@app.get("/item/{item_id}")
@cached_json(ttl=60)
def item(req, item_id):
    CALLS.append(item_id)
    return {"id": item_id, "calls": len(CALLS)}

@app.get("/aitem/{item_id}")
@cached_json(ttl=60)
async def aitem(req, item_id):
    return {"id": item_id}
""" + RUN


@pytest.mark.parametrize("path", ["gil", "tpc"])
def test_f5_cached_json_with_path_params_serves_and_caches(path):
    with serve(CACHE_SCRIPT, path, workers=1) as srv:
        first = srv.get("/item/7")
        assert first.status == 200, first.body
        assert first.json() == {"id": "7", "calls": 1}
        assert srv.get("/item/7").json() == first.json()  # a hit: the handler didn't run
        assert srv.get("/item/8").json()["id"] == "8"
        if path == "gil":  # async routes under TPC are W1's item (Q1)
            r = srv.get("/aitem/9")
            assert r.status == 200, r.body
            assert r.json() == {"id": "9"}


def test_f5_a_wrapper_that_drops_the_path_params_is_a_registration_error():
    import functools

    def swallowing(fn):
        @functools.wraps(fn)
        def wrapper(req):  # can't take item_id, though fn declares it
            return fn(req)
        return wrapper

    app = Mipyre()
    with pytest.raises(TypeError, match="item_id"):
        @app.get("/x/{item_id}")
        @swallowing
        def handler(req, item_id):
            return item_id

    def passing(fn):
        @functools.wraps(fn)
        def wrapper(req, **params):
            return fn(req, **params)
        return wrapper

    @app.get("/y/{item_id}")
    @passing
    def ok(req, item_id):
        return item_id


# ---------------------------------------------------------------------------
# F7: one bounded "drive a sync or async callable" helper
# ---------------------------------------------------------------------------


def test_f7_hung_check_runs_on_one_daemon_thread_however_often_it_is_probed(monkeypatch):
    import mipyre._health as health

    monkeypatch.setattr(health, "_CHECK_TIMEOUT_S", 0.2)
    release = threading.Event()
    check = health.ReadinessCheck.of("hangs", lambda: release.wait(30))
    before = {t.ident for t in threading.enumerate()}
    try:
        for _ in range(5):
            check_results = health._run_checks([check])
            ok, results = check_results.ok, check_results.checks
            assert ok is False and results == {"hangs": {"ok": False}}
        started = [
            t for t in threading.enumerate()
            if t.ident not in before and t.name.startswith("mipyre-bounded")
        ]
        assert len(started) == 1, started
        assert started[0].daemon
    finally:
        release.set()


def test_f7_a_hung_call_does_not_block_interpreter_exit():
    r = _run_script("""
        import time
        from mipyre._bounded import BoundedCall
        try:
            BoundedCall(lambda: time.sleep(120), 0.2)()
        except TimeoutError:
            print("timed out")
    """, timeout=20)
    assert r.returncode == 0 and "timed out" in r.stdout, (r.stdout, r.stderr)


def test_f7_helper_works_from_inside_a_running_loop():
    from mipyre._bounded import BoundedCall
    from mipyre.mcp import MCPServer

    async def slow_double(x: int) -> int:
        await asyncio.sleep(0.01)
        return 2 * x

    mcp = MCPServer(Mipyre())
    mcp.tool(slow_double)

    async def main():
        direct = BoundedCall(lambda: slow_double(4), 5)()
        return direct, await mcp.answer({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                                         "params": {"name": "slow_double", "arguments": {"x": 5}}})

    direct, resp = asyncio.run(main())
    assert direct == 8
    assert resp["result"]["content"][0]["text"] == "10", resp


def test_f7_readiness_fails_only_on_raise_timeout_or_false(monkeypatch):
    import mipyre._health as health

    monkeypatch.setattr(health, "_CHECK_TIMEOUT_S", 0.3)

    async def async_false():
        return False

    def boom():
        raise RuntimeError("down")

    # "slow" hangs until released after the run: it fails by the timeout, and no thread is left sleeping (b/441 D7).
    release = threading.Event()
    checks = [health.ReadinessCheck.of(name, fn) for name, fn in [
        ("none", lambda: None), ("zero", lambda: 0), ("empty", lambda: ""),
        ("true", lambda: True), ("false", lambda: False), ("async-false", async_false),
        ("raises", boom), ("slow", release.wait),
    ]]
    try:
        check_results = health._run_checks(checks)
        ok, results = check_results.ok, check_results.checks
    finally:
        release.set()
    assert ok is False
    assert {n for n, r in results.items() if r["ok"]} == {"none", "zero", "empty", "true"}


# ---------------------------------------------------------------------------
# MCP: client field types checked before use; prompts sent as JSON
# ---------------------------------------------------------------------------


@pytest.fixture
def mcp():
    from mipyre.mcp import MCPServer

    server = MCPServer(Mipyre())

    @server.tool()
    def add(a: int, b: int) -> int:
        return a + b

    @server.resource("config://app")
    def config():
        return {"v": 1}

    @server.prompt("plan")
    def plan(goal: str):
        return {"goal": goal, "steps": [1, 2]}

    return server


def _call(server, payload):
    return server.answer(payload)


@pytest.mark.parametrize("payload,code,words", [
    ({"jsonrpc": "2.0", "id": 1, "method": ["tools/list"]}, -32600, "method must be a string"),
    ({"jsonrpc": "2.0", "id": 1, "method": {"a": 1}}, -32600, "method must be a string"),
    ({"jsonrpc": "2.0", "id": 1}, -32600, "method must be a string, got nothing"),
    ({"jsonrpc": "2.0", "id": [1], "method": "tools/list"}, -32600, "id must be"),
    ({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": ["add"]}},
     -32602, "'name' must be a string"),
    ({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {}},
     -32602, "'name' must be a string, got nothing"),
    ({"jsonrpc": "2.0", "id": 1, "method": "prompts/get", "params": {"name": {"x": 1}}},
     -32602, "'name' must be a string"),
    ({"jsonrpc": "2.0", "id": 1, "method": "resources/read", "params": {"uri": ["a"]}},
     -32602, "'uri' must be a string"),
])
def test_mcp_field_types_are_checked_before_use(mcp, payload, code, words):
    resp = _call(mcp, payload)
    assert resp["error"]["code"] == code, resp
    assert words in resp["error"]["message"], resp


def test_mcp_prompt_result_is_sent_as_json(mcp):
    resp = _call(mcp, {"jsonrpc": "2.0", "id": 1, "method": "prompts/get",
                       "params": {"name": "plan", "arguments": {"goal": "ship"}}})
    text = resp["result"]["messages"][0]["content"]["text"]
    assert json.loads(text) == {"goal": "ship", "steps": [1, 2]}, text


# ---------------------------------------------------------------------------
# Small items: RPCClient result, DbError::Connect sqlstate
# ---------------------------------------------------------------------------

def test_rpc_client_raises_for_a_2xx_reply_it_cannot_decode_and_takes_null_as_none():
    pytest.importorskip("httpx")
    from mipyre import RPCReplyError
    from mipyre import RPCClient

    app = Mipyre(runtime="main")

    @app.post("/rpc/partial")
    def partial(req):
        return Response(body=b"{not json", content_type="application/json")

    @app.post("/rpc/full")
    def full(req):
        return Response(body=b"null", content_type="application/json")

    with TestServer(app) as server, TestClient(server) as c, RPCClient(c.base_url, use_msgpack=False) as rpc:
        with pytest.raises(RPCReplyError) as raised:
            rpc.partial()
        assert (raised.value.status, raised.value.body) == (200, b"{not json")
        assert rpc.full() is None


@needs_pg
def test_db_connect_error_carries_sqlstate():
    base = PG_DSN.rsplit("/", 1)[0]
    r = _run_script(f"""
        from mipyre.db import PgPool
        try:
            # A handle connects on its first query outside a server run (b/335 D17).
            PgPool.connect({base + "/w4_no_such_database"!r}).fetch_scalar("SELECT 1")
        except ConnectionError as e:
            print("sqlstate", e.sqlstate)
    """)
    assert "sqlstate 3D000" in r.stdout, (r.stdout, r.stderr)

    r = _run_script("""
        from mipyre.db import PgPool
        try:
            PgPool.connect("postgres://nobody@127.0.0.1:1/none", acquire_timeout_secs=2).fetch_scalar("SELECT 1")
        except ConnectionError as e:
            print("sqlstate", e.sqlstate)
    """)
    assert "sqlstate None" in r.stdout, (r.stdout, r.stderr)


# ---------------------------------------------------------------------------
# G1: compression per app, validated, large bodies compressed
# ---------------------------------------------------------------------------


def test_g1_out_of_range_levels_and_no_algorithm_raise():
    from mipyre import Compression

    app = Mipyre()
    for kwargs in ({"gzip_level": 0}, {"gzip_level": 10}, {"brotli_quality": 12},
                   {"brotli_quality": -1}, {"min_size": -1}, {"gzip": False, "brotli": False}):
        with pytest.raises(ValueError):
            app.enable_compression(**kwargs)
        with pytest.raises(ValueError):
            Compression(**kwargs)
    settings = Compression(gzip_level=9, brotli_quality=11, gzip=False)
    assert (settings.gzip, settings.brotli, settings.gzip_level, settings.brotli_quality) == (
        False, True, 9, 11)


def _text_app(size: int) -> Mipyre:
    app = Mipyre(runtime="main")

    @app.get("/text", runtime="main")
    def text(req):
        return Response(body="w4 " * size, content_type="text/plain")

    return app


def test_g1_compression_is_per_app():
    compressed, plain = _text_app(1000), _text_app(1000)
    compressed.enable_compression(min_size=16, brotli=False)
    with TestClient(compressed) as a:
        on = a.get("/text", headers={"Accept-Encoding": "gzip"})
    with TestClient(plain) as b:
        off = b.get("/text", headers={"Accept-Encoding": "gzip"})
    assert on.headers.get("content-encoding") == "gzip", on.headers
    assert gzip.decompress(on.body) == b"w4 " * 1000
    assert "content-encoding" not in off.headers, off.headers
    assert off.body == b"w4 " * 1000


# ---------------------------------------------------------------------------
# Logging: one level parser, one handler class
# ---------------------------------------------------------------------------


def test_logging_level_is_typed_and_parsed_when_given():
    from mipyre import LogLevel

    app = Mipyre()
    with pytest.raises(ValueError, match="verbose"):
        app.enable_access_log(level="verbose")
    with pytest.raises(ValueError):
        Mipyre(log_config={"level": "loud"})
    app.enable_access_log(level=LogLevel.Warn)
    assert app._access_log.level == LogLevel.Warn
    app.enable_access_log(level="error")
    assert app._access_log.level == LogLevel.Error
    assert LogLevel.parse("warning") == LogLevel.Warn


def test_logging_main_root_level_comes_from_the_engine():
    r = _run_script("""
        import logging
        from mipyre.engine import LogLevel, init_logger
        from mipyre._log.bridge import install_root_handler
        from mipyre.engine import _python_log_level, emit_python_log
        init_logger(LogLevel.Warn, False, "text")
        install_root_handler(emit_python_log, _python_log_level())
        install_root_handler(emit_python_log, _python_log_level())
        root = logging.getLogger()
        names = [type(h).__qualname__ for h in root.handlers]
        print("level", root.level, "handlers", names.count("RustLogHandler"))
    """)
    assert "level 30 handlers 1" in r.stdout, (r.stdout, r.stderr)


LOG_WORKER_SCRIPT = """
import logging
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.enable_access_log(level="warn")

@app.get("/log-setup")
def log_setup(req):
    root = logging.getLogger()
    return {"level": root.level, "handlers": [type(h).__qualname__ for h in root.handlers]}
""" + RUN


def test_logging_worker_uses_the_same_handler_and_level():
    with serve(LOG_WORKER_SCRIPT, "pool") as srv:
        r = srv.get("/log-setup")
        assert r.status == 200, r.body
        assert r.json() == {"level": 30, "handlers": ["RustLogHandler"]}


# ---------------------------------------------------------------------------
# Structure and hot path
# ---------------------------------------------------------------------------


def test_request_maps_are_fresh_dicts_built_from_the_cached_parse():
    app = Mipyre(runtime="main")

    @app.get("/q/{name}", runtime="main")
    def q(req, name):
        first = req.query_params
        first["a"] = "mutated"
        params = req.params
        params["name"] = "mutated"
        all_values = req.query_params_all()
        return {
            "a": req.query_params["a"],
            "one": req.query_param("a"),
            "missing": req.query_param("zzz"),
            "all": all_values["a"],
            "name": req.params["name"],
            "fresh": first is not req.query_params,
        }

    with TestClient(app) as c:
        r = c.get("/q/bob?a=1&a=2&b=x")
    assert r.json() == {"a": "1", "one": "1", "missing": None, "all": ["1", "2"],
                        "name": "bob", "fresh": True}


def test_shared_state_reads_return_str_and_bytes():
    from mipyre import SharedState

    state = SharedState()
    state["k"] = "välue"
    assert dict(state.items())["k"] == "välue"
    state.set_bytes("raw", b"\xff\x00")
    assert state["k"] == "välue" and state.get("k") == "välue"
    assert state.get("nope", "dflt") == "dflt" and state.get("nope") is None
    assert state.get_bytes("raw") == b"\xff\x00" and state.get_bytes("nope") is None
    # A non-UTF-8 value fails every str read, the whole-map views included.
    with pytest.raises(TypeError, match="raw"):
        state["raw"]
    with pytest.raises(TypeError, match="raw"):
        state.items()


def test_sse_event_lines_split_on_every_line_ending():
    app = Mipyre(runtime="main")

    @app.get("/sse", runtime="main")
    def sse(req):
        stream = Stream()
        stream.send_event("a\r\nb\rc\nd\n", event="e", id="1")
        stream.send_event("")
        stream.close()
        return stream

    with TestClient(app) as c:
        r = c.get("/sse")
    assert r.text == (
        "id: 1\nevent: e\ndata: a\ndata: b\ndata: c\ndata: d\n\n"
        "data: \n\n"
    ), repr(r.text)


_UNCOUNTED_SERVER = """
from mipyre import Mipyre
from mipyre.testing import TestClient

app = Mipyre(runtime="main")
app.get("/")(lambda req: "ok")
with TestClient(app) as c:
    assert c.get("/").status_code == 200
    print(c.server.metrics().total_requests)
"""


def test_total_requests_is_none_when_metrics_are_off():
    env = {k: v for k, v in os.environ.items() if k != "MIPYRE_METRICS"}
    r = subprocess.run(
        [PYTHON, "-c", _UNCOUNTED_SERVER],
        capture_output=True, text=True, timeout=30, env=env,
    )
    # The server prints its banner first; the script's own line is the last.
    assert r.stdout.strip().splitlines()[-1] == "None", (r.stdout, r.stderr)


def test_routes_are_typed_records():
    from mipyre import FastRoute, Route, Runtime

    app = Mipyre()

    async def a(req, x):
        return x

    app.get("/a/{x}", a, runtime="main")
    app.add_fast_response("GET", "/health", b"ok")
    [route] = app.routes
    assert route == Route(method="GET", path="/a/{x}", handler=a, runtime=Runtime.MAIN, is_async=True,
                          body=None, query=None, in_openapi=True)
    assert app.fast_routes == [FastRoute(method="GET", path="/health", status_code=200,
                                         content_type="text/plain", body_bytes=2)]
