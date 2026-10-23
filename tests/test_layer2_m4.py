"""Layer 2, M4 (issue #6): workers run the real `mipyre` package and engine.

End-to-end tests from docs/design/real-engine-in-workers.md §9 that are new at M4. Each
starts a real server in a subprocess (a sub-interpreter permanently disables
`PyGILState_Check` in its process), stops it with SIGINT, and scans its whole log for the
PyO3 fork's panic texts.
"""
from __future__ import annotations

import collections
import concurrent.futures
import json
import os
import signal
import subprocess
import sys
import textwrap
import uuid

import httpx
import pytest

from tests.conftest import fork_panic_lines
from tests._helpers import answers, bound_port, poll_until

HERE = os.path.dirname(os.path.abspath(__file__))
PG_DSN = os.environ.get("MIPYRE_TEST_PG_DSN")

pytestmark = pytest.mark.skipif(
    sys.platform not in ("linux", "darwin"), reason="own-GIL sub-interpreters"
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


class Server:
    def __init__(self, tmp_path, source: str, extra_env: dict | None = None, argv=None):
        self._port: int | None = None
        self.script = tmp_path / "app.py"
        self.script.write_text(textwrap.dedent(source))
        self.log_path = tmp_path / "server.log"
        env = dict(os.environ)
        env["L2_PORT"] = "0"
        env.update(extra_env or {})
        cmd = argv or [sys.executable, str(self.script)]
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                cmd, stdout=log, stderr=subprocess.STDOUT, env=env,
                cwd=str(tmp_path), start_new_session=True,
            )

    def log(self) -> str:
        return self.log_path.read_text(errors="replace")

    @property
    def port(self) -> int:
        """The port the server bound (it binds port 0), from its startup line."""
        if self._port is None:
            self._port = bound_port(self.log, self.proc)
        return self._port

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def wait_up(self, path: str = "/ping") -> None:
        answers(self.base + path, self.proc, self.log)

    def wait_exit(self, timeout: float = 90) -> int:
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.kill()
            raise AssertionError(f"server did not exit in {timeout}s:\n{self.log()[-4000:]}")

    def stop(self, timeout: float = 30) -> int:
        if self.proc.poll() is None:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGINT)
        return self.wait_exit(timeout)

    def kill(self) -> None:
        if self.proc.poll() is None:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            self.proc.wait(timeout=5)


def _no_panics(server: Server) -> None:
    text = server.log()
    assert fork_panic_lines(text) == [], text[-4000:]
    assert "panicked at" not in text, text[-4000:]


RUN = """
if __name__ == "__main__":
    # The sub-interpreter count: one per TPC thread, or the pool's with MIPYRE_TPC=0.
    size = "subinterpreters" if os.environ.get("MIPYRE_TPC") == "0" else "threads"
    app.run(host="127.0.0.1", port=int(os.environ["L2_PORT"]), **{{size: {workers}}})
"""


def _app(body: str, workers: int = 2, header: str = "") -> str:
    return (
        textwrap.dedent(header) + "import os\nfrom mipyre import Mipyre\n"
        + textwrap.dedent(body) + RUN.format(workers=workers)
    )


# ---------------------------------------------------------------------------
# E2E-3a: a route table that differs in a worker fails startup with both lists
# ---------------------------------------------------------------------------


def test_route_only_in_worker_fails_startup_with_both_lists(tmp_path):
    s = Server(tmp_path, _app("""
        import mipyre.engine
        app = Mipyre()

        @app.get("/ping")
        def ping(req):
            return "ok"

        if mipyre.engine._in_worker():
            @app.get("/only-in-worker")
            def extra(req):
                return "extra"
    """))
    rc = s.wait_exit()
    log = s.log()
    assert rc != 0, log
    assert "the main interpreter registered:" in log, log[-3000:]
    assert "this worker's script registered:" in log
    assert "/only-in-worker" in log and "first difference" in log
    assert fork_panic_lines(log) == []


# ---------------------------------------------------------------------------
# E2E-1 (new cases): closure hooks now run in workers; CORS is not applied twice
# ---------------------------------------------------------------------------


def test_worker_cors_header_applied_once(tmp_path):
    # A worker's response carries the engine's CORS header exactly once (design R-1).
    s = Server(tmp_path, _app("""
        app = Mipyre()
        app.enable_cors(allow_origins="https://a.example")

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/data")
        def data(req):
            return {"ok": True}
    """))
    try:
        s.wait_up()
        r = httpx.get(s.base + "/data", headers={"Origin": "https://a.example"}, timeout=10)
    finally:
        rc = s.stop()
    assert r.status_code == 200 and r.json() == {"ok": True}
    assert r.headers.get_list("access-control-allow-origin") == ["https://a.example"]
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-4: app.state and a bare SharedState() in workers are main's map (FR-5)
# ---------------------------------------------------------------------------


def test_worker_shared_state_is_mains(tmp_path):
    s = Server(tmp_path, _app("""
        from mipyre import SharedState
        app = Mipyre()

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/n")
        def n(req):
            app.state.incr("n", 1)
            return "ok"

        @app.get("/m")
        def m(req):
            SharedState().incr("m", 1)
            return "ok"

        @app.get("/read", runtime="main")
        def read(req):
            return {"n": app.state.get("n"), "m": app.state.get("m")}
    """, workers=4))
    try:
        s.wait_up()
        with concurrent.futures.ThreadPoolExecutor(16) as pool:
            statuses = list(pool.map(
                lambda i: httpx.get(s.base + ("/n" if i % 2 else "/m"), timeout=10).status_code,
                range(800),
            ))
        read = httpx.get(s.base + "/read", timeout=10).json()
    finally:
        rc = s.stop()
    assert statuses == [200] * 800
    assert read == {"n": "400", "m": "400"}
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-5 (fetch_iter from a worker) and E2E-6 (*_async refused in a worker)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(PG_DSN is None, reason="MIPYRE_TEST_PG_DSN not set")
def test_worker_pgpool_fetch_iter_and_async_refused(tmp_path):
    s = Server(tmp_path, _app("""
        from mipyre.db import PgPool
        app = Mipyre()
        pool = PgPool.connect(os.environ["MIPYRE_TEST_PG_DSN"])

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/iter")
        def it(req):
            rows = [r["i"] for r in pool.fetch_iter("SELECT generate_series(1, $1::int) AS i", 250)]
            return {"n": len(rows), "sum": sum(rows)}

        @app.get("/async")
        async def a(req):
            return {"rows": await pool.fetch_all_async("SELECT 1 AS one")}
    """), {"MIPYRE_TEST_PG_DSN": PG_DSN})
    try:
        s.wait_up()
        results = [httpx.get(s.base + "/iter", timeout=10).json() for _ in range(20)]
        refused = httpx.get(s.base + "/async", timeout=10)
        alive = httpx.get(s.base + "/ping", timeout=10).status_code
    finally:
        rc = s.stop()
    assert results == [{"n": 250, "sum": 250 * 251 // 2}] * 20
    assert refused.status_code == 500
    assert "fetch_all_async is not available in sub-interpreter workers" in s.log()
    assert alive == 200
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-7 / E2E-7b: async pool path returns headers, runs hooks per request (FR-14)
# ---------------------------------------------------------------------------


def test_async_pool_returns_headers_and_runs_hooks(tmp_path):
    s = Server(tmp_path, _app("""
        from mipyre import Response
        app = Mipyre()

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.before_request
        def mark(req):
            return None

        @app.after_request
        def add(req, resp):
            return Response(resp.body, status_code=resp.status_code,
                            content_type=resp.content_type,
                            headers={**resp.headers, "x-after": "1"})

        @app.get("/a")
        async def a(req):
            return Response("async", headers={"x-a": "1"})

        @app.get("/dict")
        async def d(req):
            return {"k": 1}
    """), {"MIPYRE_TPC": "0"})
    try:
        s.wait_up()
        r = httpx.get(s.base + "/a", timeout=10)
        d = httpx.get(s.base + "/dict", timeout=10)
    finally:
        rc = s.stop()
    assert r.status_code == 200 and r.text == "async"
    assert r.headers["x-a"] == "1" and r.headers["x-after"] == "1"
    assert d.json() == {"k": 1} and d.headers["x-after"] == "1"
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# Requests in flight at once in the request-id test: all of them wait at the rendezvous.
_IN_FLIGHT = 32


def test_async_pool_request_ids_do_not_cross(tmp_path):
    """Request ids stay with their requests under the worst interleaving: all `_IN_FLIGHT`
    requests are in their await at once (each says so with a file, then waits for its own
    release file), and they are released in reverse order of sending, each one answered before
    the next is let go. Until b/441 the handlers slept a random 0-20 ms, a different
    interleaving each run."""
    rendezvous = tmp_path / "rendezvous"
    (rendezvous / "in").mkdir(parents=True)
    (rendezvous / "go").mkdir()
    s = Server(tmp_path, _app("""
        import asyncio
        app = Mipyre()
        app.enable_request_id_header()
        RENDEZVOUS = os.environ["RID_RENDEZVOUS"]

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/slow")
        async def slow(req):
            k = req.query_params["k"]
            open(os.path.join(RENDEZVOUS, "in", k), "x").close()
            while not os.path.exists(os.path.join(RENDEZVOUS, "go", k)):
                await asyncio.sleep(0.005)
            return "ok"
    """), {"MIPYRE_TPC": "0", "RID_RENDEZVOUS": str(rendezvous)})
    try:
        s.wait_up()

        def one(k):
            rid = uuid.uuid4().hex
            r = httpx.get(s.base + f"/slow?k={k}", headers={"X-Request-ID": rid}, timeout=None)
            return rid, r.status_code, r.headers.get("x-request-id")

        with concurrent.futures.ThreadPoolExecutor(_IN_FLIGHT) as pool:
            sent = [pool.submit(one, k) for k in range(_IN_FLIGHT)]
            poll_until(lambda: len(os.listdir(rendezvous / "in")) == _IN_FLIGHT, timeout=None)
            results = []
            for k in reversed(range(_IN_FLIGHT)):
                (rendezvous / "go" / str(k)).touch()
                results.append(sent[k].result())
    finally:
        rc = s.stop()
    assert all(status == 200 for _, status, _ in results)
    assert [(sent, got) for sent, _, got in results if sent != got] == []
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-10 surface parity, E2E-18 pyclass module, E2E-20 script as a real module
# ---------------------------------------------------------------------------

_ENGINE_NAMES = [
    "MipyreApp", "Request", "Response", "WebSocket", "SharedState", "Stream",
    "PgPool", "PgCursor", "get_gil_metrics", "init_logger", "emit_python_log", "_in_worker",
    "_worker_recv", "_worker_send", "_worker_to_response",
]


def test_worker_surface_matches_main_and_script_is_a_module(tmp_path):
    s = Server(tmp_path, _app(f"""
        import dataclasses, typing
        import mipyre, mipyre.engine
        app = Mipyre()
        NAMES = {_ENGINE_NAMES!r}

        @dataclasses.dataclass
        class Point:
            x: int
            y: "list[int]"

        def surface():
            return {{
                "all": sorted(mipyre.__all__),
                "methods": sorted(m for m in vars(mipyre.Mipyre) if not m.startswith("_")),
                "engine": [n for n in NAMES if hasattr(mipyre.engine, n)],
            }}

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/worker")
        def worker(req):
            return {{
                "surface": surface(),
                "in_worker": mipyre.engine._in_worker(),
                "type": repr(type(req)),
                "hints": sorted(typing.get_type_hints(Point)),
                "module": __name__,
            }}

        @app.get("/main", runtime="main")
        def main(req):
            return {{"surface": surface(), "in_worker": mipyre.engine._in_worker()}}

        @app.get("/boom")
        def boom(req):
            raise ValueError("boom-marker")
    """, header="from __future__ import annotations\n"),
        {"MIPYRE_LOG": "1"})
    try:
        s.wait_up()
        worker = httpx.get(s.base + "/worker", timeout=10).json()
        main = httpx.get(s.base + "/main", timeout=10).json()
        boom = httpx.get(s.base + "/boom", timeout=10)
    finally:
        # The stop is the log's fence: run() waits for every line logged before it (flush_logs, b/441 L5).
        rc = s.stop()
    # E2E-10: the same public surface in a worker as on main (FR-12).
    assert worker["in_worker"] is True and main["in_worker"] is False
    assert worker["surface"] == main["surface"]
    assert worker["surface"]["engine"] == _ENGINE_NAMES
    # E2E-18: pyclasses are mipyre.engine's (FR-18).
    assert worker["type"] == "<class 'mipyre.engine.Request'>"
    # E2E-20: `from __future__ import annotations` works, get_type_hints resolves worker
    # classes, and a handler's traceback names the script and line (FR-20).
    assert worker["hints"] == ["x", "y"] and worker["module"] == "__mipyre_worker__"
    assert boom.status_code == 500
    log = s.log()
    assert "boom-marker" in log
    assert f'File "{s.script}", line' in log or f"File \\\"{s.script}\\\", line" in log, log[-3000:]
    assert rc == 0, log[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-11: worker log records carry their worker id; main's carry none (FR-8)
# ---------------------------------------------------------------------------


def test_worker_logs_carry_worker_id(tmp_path):
    s = Server(tmp_path, _app("""
        import logging
        app = Mipyre(log_config={"level": "INFO", "format": "json", "access_log": False})
        logging.getLogger("e2e11").warning("init-line")

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/main_log", runtime="main")
        def main_log(req):
            logging.getLogger("e2e11").warning("main-line")
            return "ok"
    """, workers=4))
    try:
        s.wait_up()
        httpx.get(s.base + "/main_log", timeout=10)
    finally:
        # The stop is the log's fence (flush_logs at the end of run(), b/441 L5).
        rc = s.stop()
    records = []
    for line in s.log().splitlines():
        try:
            records.append(json.loads(line))
        except ValueError:
            pass
    fields = [r.get("fields", {}) for r in records]
    init = collections.Counter(f.get("worker") for f in fields if f.get("message") == "init-line")
    main = [f for f in fields if f.get("message") == "main-line"]
    assert init == {0: 1, 1: 1, 2: 1, 3: 1}, s.log()[-3000:]
    assert len(main) == 1 and "worker" not in main[0], main
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-13 two apps, E2E-14b mipyre never re-executed
# ---------------------------------------------------------------------------


def test_second_app_in_worker_fails_startup(tmp_path):
    s = Server(tmp_path, _app("""
        app = Mipyre()
        other = Mipyre()

        @app.get("/ping")
        def ping(req):
            return "ok"

        @other.get("/other")
        def o(req):
            return "other"
    """))
    rc = s.wait_exit()
    log = s.log()
    assert rc != 0, log
    assert "second app" in log, log[-3000:]
    assert fork_panic_lines(log) == []


def test_isolation_never_reexecutes_mipyre(tmp_path):
    pytest.importorskip("pydantic_settings")
    s = Server(tmp_path, _app("""
        import sys
        app = Mipyre(runtime="isolated")
        app.enable_request_id_header()
        ENGINE = sys.modules["mipyre.engine"]

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/settings")
        def settings(req):
            from mipyre.config import Settings  # pydantic_settings -> pydantic_core
            return {
                "same_engine": sys.modules["mipyre.engine"] is ENGINE,
                "rid": req.request_id,
                "settings": Settings.__name__,
            }
    """))
    try:
        s.wait_up()
        rids = [uuid.uuid4().hex for _ in range(4)]
        out = [httpx.get(s.base + "/settings", headers={"X-Request-ID": r}, timeout=120)
               for r in rids]
    finally:
        rc = s.stop()
    for rid, r in zip(rids, out):
        assert r.status_code == 200, s.log()[-3000:]
        assert r.headers["x-request-id"] == rid
        assert r.json() == {"same_engine": True, "rid": rid, "settings": "Settings"}
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-15: pydantic only for body= apps, and body= validates in workers
# ---------------------------------------------------------------------------


def test_pydantic_not_imported_without_model(tmp_path):
    pytest.importorskip("pydantic")
    s = Server(tmp_path, _app("""
        import sys
        app = Mipyre()

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/mods")
        def mods(req):
            return {"pydantic_core": "pydantic_core" in sys.modules,
                    "pydantic": "pydantic" in sys.modules}
    """))
    try:
        s.wait_up()
        mods = httpx.get(s.base + "/mods", timeout=10).json()
    finally:
        rc = s.stop()
    assert mods == {"pydantic_core": False, "pydantic": False}
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


@pytest.mark.parametrize("tpc", ["1", "0"])
def test_model_validates_in_workers_through_isolation(tmp_path, tpc):
    pytest.importorskip("pydantic")
    s = Server(tmp_path, _app("""
        from pydantic import BaseModel
        import mipyre.engine
        app = Mipyre(runtime="isolated")

        class Item(BaseModel):
            name: str
            qty: int

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.post("/items", body=Item)
        def create(req, item):
            return {"name": item.name, "qty": item.qty,
                    "worker": mipyre.engine._in_worker()}
    """), {"MIPYRE_TPC": tpc})
    try:
        s.wait_up()
        ok = httpx.post(s.base + "/items", content=json.dumps({"name": "a", "qty": 2}),
                        timeout=30)
        bad = httpx.post(s.base + "/items", content=json.dumps({"name": "a", "qty": "x"}),
                         timeout=30)
    finally:
        rc = s.stop(timeout=60)
    assert ok.status_code == 200, s.log()[-3000:]
    assert ok.json() == {"name": "a", "qty": 2, "worker": True}
    assert bad.status_code == 422, (bad.status_code, bad.text)
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-16 Stream from a worker, E2E-17 main-only process-wide setters
# ---------------------------------------------------------------------------


def test_stream_from_worker_is_a_loud_500(tmp_path):
    s = Server(tmp_path, _app("""
        from mipyre import Stream
        app = Mipyre()

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.get("/stream")
        def stream(req):
            return Stream()
    """), {"MIPYRE_LOG": "1"})
    try:
        s.wait_up()
        r = httpx.get(s.base + "/stream", timeout=10)
    finally:
        # The stop is the log's fence (flush_logs at the end of run(), b/441 L5).
        rc = s.stop()
    assert r.status_code == 500
    assert ("a Stream response is only served from the main interpreter, so register the "
            "route with runtime='main'") in s.log(), s.log()[-3000:]
    assert rc == 0
    _no_panics(s)


def test_worker_max_body_size_is_mains(tmp_path):
    s = Server(tmp_path, _app("""
        import mipyre.engine
        app = Mipyre(log_config={"level": "WARN"})
        app.max_body_size = 2048
        if mipyre.engine._in_worker():
            app.max_body_size = 999_999

        @app.get("/ping")
        def ping(req):
            return "ok"

        @app.post("/upload")
        def upload(req):
            return {"len": len(req.body)}
    """))
    try:
        s.wait_up()
        big = httpx.post(s.base + "/upload", content=b"x" * 4000, timeout=10)
        small = httpx.post(s.base + "/upload", content=b"x" * 100, timeout=10)
    finally:
        rc = s.stop()
    assert big.status_code == 413 and small.json() == {"len": 100}
    assert "set_max_body_size(999999) in a worker is ignored" in s.log()
    assert rc == 0
    _no_panics(s)


# ---------------------------------------------------------------------------
# E2E-19 teardown (FR-19): clean stop, failed start, abandoned worker
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tpc", ["1", "0"])
def test_graceful_stop_ends_workers_cleanly(tmp_path, tpc):
    s = Server(tmp_path, _app("""
        app = Mipyre()

        @app.get("/ping")
        def ping(req):
            return {"ok": True}

        @app.get("/a")
        async def a(req):
            return {"async": True}
    """, workers=4), {"MIPYRE_TPC": tpc})
    try:
        s.wait_up()
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            codes = list(pool.map(
                lambda i: httpx.get(s.base + ("/a" if i % 2 else "/ping"), timeout=10).status_code,
                range(200),
            ))
    finally:
        rc = s.stop()
    assert codes == [200] * 200
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


@pytest.mark.parametrize("tpc", ["1", "0"])
def test_failed_worker_start_exits_nonzero_without_abort(tmp_path, tpc):
    s = Server(tmp_path, _app("""
        import sys
        import mipyre.engine
        if mipyre.engine._in_worker() and sys.modules["__mipyre_bootstrap__"].WORKER_ID == 2:
            raise RuntimeError("worker two refuses to start")
        app = Mipyre()

        @app.get("/ping")
        def ping(req):
            return "ok"
    """, workers=4), {"MIPYRE_TPC": tpc})
    rc = s.wait_exit()
    log = s.log()
    assert rc not in (0, -signal.SIGABRT, -signal.SIGSEGV), (rc, log[-3000:])
    assert "worker two refuses to start" in log
    assert "Abort" not in log and "Fatal Python error" not in log, log[-3000:]
    assert fork_panic_lines(log) == []


# ---------------------------------------------------------------------------
# E2E-21 the CLI, E2E-22 raw-engine scripts
# ---------------------------------------------------------------------------


def test_cli_run_serves_worker_routes(tmp_path):
    (tmp_path / "cliapp.py").write_text(textwrap.dedent("""
        import mipyre.engine
        from mipyre import Mipyre
        app = Mipyre()

        @app.get("/ping")
        def ping(req):
            return {"worker": mipyre.engine._in_worker()}
    """))
    s = Server(tmp_path, "", argv=[
        sys.executable, "-m", "mipyre.cli", "run", "cliapp:app",
        "--port", "0", "--threads", "2",
    ])
    try:
        s.wait_up()
        r = httpx.get(s.base + "/ping", timeout=10).json()
    finally:
        rc = s.stop()
    assert r == {"worker": True}
    assert rc == 0, s.log()[-3000:]
    _no_panics(s)


def test_raw_engine_script_serves_unchanged(tmp_path):
    s = Server(tmp_path, """
        import os
        from mipyre.engine import MipyreApp, RuntimeChoice

        app = MipyreApp()

        def hello(req):
            return {"hello": "raw"}

        def ping(req):
            return "ok"

        app.route("GET", "/", hello, RuntimeChoice.Worker)
        app.route("GET", "/ping", ping, RuntimeChoice.Worker)

        if __name__ == "__main__":
            app.run(host="127.0.0.1", port=int(os.environ["L2_PORT"]),
                    threads=2)
    """)
    try:
        s.wait_up()
        r = httpx.get(s.base + "/", timeout=10).json()
    finally:
        rc = s.stop()
    assert r == {"hello": "raw"}
    assert rc in (0, -signal.SIGINT), s.log()[-3000:]
    _no_panics(s)
