"""A server's counts are its own (b/335 D15): two servers of one process each read what they
served from ``metrics()``, not a sum over every server the process ran."""

from mipyre import Mipyre
from mipyre.engine import ServerFlags
from mipyre.testing import TestClient

from tests._helpers import poll_until

app = Mipyre(runtime="main")
# The metrics page makes a run count its answers.
app.enable_metrics()


@app.get("/")
def index(req):
    return "ok"


def test_two_servers_in_one_process_each_count_their_own():
    with TestClient(app) as first, TestClient(app) as second:
        for _ in range(10):
            assert first.get("/").status_code == 200
        for _ in range(3):
            assert second.get("/").status_code == 200
        assert first.server.metrics().total_requests == 10
        assert second.server.metrics().total_requests == 3


def test_a_server_that_does_not_count_its_answers_says_none():
    plain = Mipyre(runtime="main")
    plain.get("/")(lambda req: "ok")
    with TestClient(plain) as c:
        assert c.get("/").status_code == 200
        assert c.server.metrics().total_requests is None
        assert c.server.metrics().dropped_requests == 0


def test_a_server_says_why_its_last_worker_retired(tmp_path, monkeypatch):
    # The first collect in any worker fails and retires it; the server keeps the reason.
    monkeypatch.setenv("RUN_METRICS_GC_MARKER", str(tmp_path / "gc-failed"))
    from tests.apps.run_metrics_gc_failure import app as failing

    # A TestClient reads no environment (b/335 M5.6): the collection settings are its config.
    with TestClient(failing, config=ServerFlags(gc_mode="count", gc_threshold=1, threads=2)) as c:
        for _ in range(4):
            c.get("/work")
        poll_until(lambda: c.server.metrics().workers_retired == 1, timeout=None, what="the retirement")
        m = c.server.metrics()
    assert m.gc_failures == 1, m
    assert "gc.collect() raised MemoryError: run-metrics-injected" in m.last_worker_retirement, m


def test_two_servers_in_one_process_each_list_their_own_degradations(tmp_path):
    # A mount whose directory is gone by the first request cannot be watched: that server serves
    # it uncached and says so; the other server, whose mount is watched, runs as configured.
    gone, kept = tmp_path / "gone", tmp_path / "kept"
    gone.mkdir()
    kept.mkdir()
    (kept / "a.txt").write_text("a")
    unwatched = Mipyre(runtime="main")
    unwatched.static("/static", str(gone))
    watched = Mipyre(runtime="main")
    watched.static("/static", str(kept))
    gone.rmdir()

    with TestClient(unwatched) as first, TestClient(watched) as second:
        first.get("/static/a.txt")
        assert second.get("/static/a.txt").text == "a"
        poll_until(lambda: first.server.metrics().degraded, timeout=None, what="the unwatched mount listed")
        [line] = first.server.metrics().degraded
        assert line.startswith(f"static_watch:{gone}: "), line
        assert second.server.metrics().degraded == []
