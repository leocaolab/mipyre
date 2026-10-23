"""runtime="process" routes: answers, failures, crashes, timeouts, admission, isolation.

One server per test module configuration, started as `python app.py` (the template is
forked from that process, as in production)."""

from __future__ import annotations

import concurrent.futures
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import pytest

from mipyre import BodyStream
from tests._helpers import bound_port, kill_session, poll_until, read_file

APP = os.path.join(os.path.dirname(__file__), "apps", "process_lane_app.py")


def launch(app_file: str, attr: str = "app") -> list[str]:
    """`mipyre run` for an app file: process routes need mipyre to start first."""
    return [sys.executable, "-m", "mipyre", "run", f"{app_file}:{attr}", "--port", "0"]


class Server:
    def __init__(self, tmp_path, **env):
        self.log_path = str(tmp_path / "server.log")
        full = dict(os.environ)
        full.update({k: str(v) for k, v in env.items()})
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                launch(APP), stdout=log, stderr=subprocess.STDOUT, start_new_session=True, env=full
            )
        self.read = read_file(self.log_path)
        self.port = bound_port(self.read, self.proc)
        # The multi-thread server (MIPYRE_TPC=0) answers its first request ~0.8 s late
        # whatever the route; the tests start after it, waited for by its answer.
        assert self.get("/hello", timeout=None)[0] == 200

    def get(self, path, timeout=10, data=None, headers=None):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, headers=headers or {}, method="POST" if data is not None else "GET"
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, dict(r.headers), r.read()
        except urllib.error.HTTPError as e:
            return e.code, dict(e.headers), e.read()

    def json(self, path, **kw):
        status, _, body = self.get(path, **kw)
        assert status == 200, (status, body, self.read()[-3000:])
        return json.loads(body)

    def stop(self):
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGINT)
            self.proc.wait(timeout=15)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            self.proc.wait(timeout=5)


@pytest.fixture
def server(tmp_path):
    s = Server(tmp_path, MIPYRE_PROCESS_WORKERS=2, MIPYRE_PROCESS_TIMEOUT=1.5)
    yield s
    s.stop()


def worker_pids(s: Server, n=8) -> set[int]:
    """The workers that answer n overlapping requests: lanes fill lowest first, so only
    requests that overlap reach every worker."""
    with concurrent.futures.ThreadPoolExecutor(n) as ex:
        return set(ex.map(lambda _: s.json("/sleep?s=0.15")["pid"], range(n)))


def test_the_ring_depth_is_the_largest_power_of_two_up_to_8_the_queue_holds():
    from mipyre._process_lane.server import ring_depth

    assert [ring_depth(q) for q in (1, 2, 3, 4, 5, 7, 8, 9, 128)] == [1, 2, 2, 4, 4, 4, 8, 8, 8]


@pytest.mark.parametrize(
    "stat, exiting",
    [
        ("1234 (mipyre-tpc-4) R 1 1 1 0 -1 4194372 0 0 0 0", True),  # PF_EXITING set: joined, not yet gone
        ("1234 (mipyre-tpc-4) S 1 1 1 0 -1 4194368 0 0 0 0", False),
        ("5 (a) b (c)) S 1 1 1 0 -1 4194368 0 0", False),  # a name with spaces and ")"
        ("5 (a) b (c)) S 1 1 1 0 -1 4 0 0", True),
    ],
)
def test_a_thread_is_exiting_by_its_kernel_flag(stat, exiting):
    from mipyre._process_lane.threads import is_exiting

    assert is_exiting(stat) is exiting


def test_a_fork_is_refused_naming_every_other_thread():
    from mipyre._process_lane.threads import ThreadedTemplateError, refusal

    assert refusal([], "the template", "fix it") is None
    error = refusal(["polars-ooc-clea", ""], "the template", "Use import.")
    assert isinstance(error, ThreadedTemplateError)
    assert str(error) == (
        "the template must be single-threaded when it forks (a fork copies only the calling thread, so a "
        "pool these threads belong to would be missing in every worker), but it runs 2 other thread(s): "
        "polars-ooc-clea, <unnamed>. Use import."
    )


def test_hello_answers_from_a_worker_process(server):
    status, headers, body = server.get("/hello")
    assert (status, body) == (200, b"hello")
    assert headers["content-type"].startswith("text/plain")
    server_pid = server.json("/server_pid")["pid"]
    pids = worker_pids(server)
    assert server_pid not in pids
    assert len(pids) == 2


def test_a_worker_gets_its_setup_from_the_template_not_from_the_environment(server):
    # The setup goes over the worker's own fd (spawned) or is inherited in memory (forked): no
    # MIPYRE_LANE_WORKER reaches the worker, nor the programs its handlers start.
    names = server.json("/mipyre-env")
    assert "MIPYRE_LANE_WORKER" not in names, names


def test_request_parts_and_response_parts_cross_the_lane(server):
    status, headers, body = server.get(
        "/echo/abc?q=xyz", data=b"\x00\x01payload" * 100, headers={"x-request-id": "rid-1"}
    )
    assert status == 201
    assert body == b"\x00\x01payload" * 100
    assert headers["content-type"] == "application/x-echo"
    assert headers["x-name"] == "abc" and headers["x-q"] == "xyz"
    assert len(headers["x-rid"]) > 0


def test_a_raising_handler_is_500_and_its_worker_lives(server):
    before = worker_pids(server)
    status, _, body = server.get("/raise")
    assert status == 500
    rid = json.loads(body)["request_id"]
    assert "7f3a" not in body.decode()  # the exception text goes only to the log
    log = poll_until(lambda: rid in server.read() and server.read(), what="error log line")
    line = next(ln for ln in log.splitlines() if rid in ln and "7f3a" in ln)
    assert "ValueError" in line
    assert worker_pids(server) == before


def test_too_many_response_headers_are_500_and_the_worker_lives(server):
    """b/927: past the lines a header map takes room for is the handler's ValueError, not a panic."""
    before = worker_pids(server)
    status, _, body = server.get("/too-many-headers")
    assert status == 500, body
    rid = json.loads(body)["request_id"]
    log = poll_until(lambda: rid in server.read() and server.read(), what="error log line")
    assert "more than 24560 lines" in next(ln for ln in log.splitlines() if rid in ln)
    assert worker_pids(server) == before


def test_os_exit_is_502_and_the_worker_is_replaced(server):
    before = worker_pids(server)
    status, _, body = server.get("/exit")
    assert status == 502, body
    rid = json.loads(body)["request_id"]
    log = poll_until(lambda: rid in server.read() and server.read(), what="502 log line")
    assert "exited with status 3" in next(ln for ln in log.splitlines() if rid in ln)
    after = replaced(server, before)
    assert len(after & before) == 1  # one replaced, one untouched
    assert server.get("/hello")[0] == 200


# A lane timeout no request reaches (10 min): in the tests that use it, only what the test does
# to a worker ends its request.
PATIENT_S = 600


def block_path(mark) -> str:
    """The /block request that writes its worker's pid to `mark`."""
    return f"/block?mark={urllib.parse.quote(str(mark))}"


def blocked_worker(mark) -> int:
    """The pid of the worker a /block request is in, once the handler has written it: waited
    for by the file, for no length of time (b/325)."""
    return int(poll_until(lambda: mark.exists() and mark.read_text(), timeout=None, what=f"{mark}"))


def test_sigkill_of_a_busy_worker_is_502_and_it_is_replaced(tmp_path):
    s = Server(tmp_path, MIPYRE_PROCESS_WORKERS=2, MIPYRE_PROCESS_TIMEOUT=PATIENT_S)
    try:
        mark = tmp_path / "busy.pid"
        with concurrent.futures.ThreadPoolExecutor(1) as ex:
            fut = ex.submit(s.get, block_path(mark), None)
            os.kill(blocked_worker(mark), signal.SIGKILL)
            status, _, body = fut.result()
        assert status == 502, body
        poll_until(lambda: len(worker_pids(s, 20)) == 2, timeout=None, what="two workers again")
    finally:
        s.stop()


def test_a_stream_from_a_process_route_is_500_naming_the_fix_and_its_worker_lives(server):
    before = worker_pids(server)
    status, _, body = server.get("/stream")
    assert status == 500, body
    rid = json.loads(body)["request_id"]
    assert "Stream" not in body.decode()  # the reason goes only to the log
    log = poll_until(lambda: rid in server.read() and server.read(), what="error log line")
    line = next(ln for ln in log.splitlines() if rid in ln)
    assert "returned a Stream" in line and "runtime='main'" in line, line
    assert worker_pids(server) == before


def test_a_handler_past_its_memory_limit_fails_alone_and_the_lanes_keep_serving(server):
    """A handler that grows until its worker's address-space limit refuses it: the request is a
    500 (MemoryError) or a 502 (the worker died of it), the other worker is untouched, and the
    next requests are answered by both lanes."""
    before = worker_pids(server)
    status, _, body = server.get("/hog", timeout=None)
    assert status in (500, 502), (status, body)
    assert server.get("/hello")[0] == 200

    def both_serving():
        pids = worker_pids(server)
        return pids if len(pids) == 2 else None

    after = poll_until(both_serving, timeout=None, what="two workers serving")
    assert len(after & before) >= 1  # the worker that did not hog is the one it was


def test_a_request_queued_behind_a_dying_worker_is_502_and_the_replacement_serves_only_later_ones(tmp_path):
    """One lane: a request waits in the lane while its worker runs a handler that is then
    killed. The running request and the waiting one are both 502: the waiting one is not run
    by the replacement, since the dead worker could have rewritten its bytes. A request sent
    after the death is answered by the replacement, and the respawn is counted with its
    latency. (Which slots the take-over gives up is pinned by the host's own test,
    `a_dead_worker_gives_up_the_request_it_ran_and_those_it_never_took`.)"""
    s = Server(tmp_path, MIPYRE_PROCESS_WORKERS=1, MIPYRE_PROCESS_TIMEOUT=PATIENT_S)
    try:
        assert s.json("/lanes")["respawns"] == 0
        mark = tmp_path / "busy.pid"
        with concurrent.futures.ThreadPoolExecutor(2) as ex:
            running = ex.submit(s.get, block_path(mark), None)
            dying = blocked_worker(mark)
            waiting = ex.submit(s.get, "/pid", None)
            assert s.json("/server_pid")["pid"] != dying
            # The kill waits until the host holds both requests, the blocked one and the one queued
            # behind it: a submit returns before its request reaches the server, so a round trip
            # sent after it does not prove it arrived (b/971).
            poll_until(lambda: s.json("/lanes")["requests_in_lanes"] == 2, timeout=None, what="/pid queued behind /block")
            os.kill(dying, signal.SIGKILL)
            status, _, body = running.result()
            waiting_status, _, waiting_body = waiting.result()
        assert status == 502, body
        assert waiting_status == 502, waiting_body
        rid = json.loads(waiting_body)["request_id"]
        log = poll_until(lambda: rid in s.read() and s.read(), what="the untaken request's log line")
        assert "died before it took the request" in next(ln for ln in log.splitlines() if rid in ln and "died" in ln)
        answer = s.json("/pid", timeout=None)
        assert answer["pid"] != dying
        stats = poll_until(lambda: (st := s.json("/lanes"))["respawn_us"] and st, timeout=None,
                           what="the respawn latency recorded")
        assert (stats["deaths"], stats["respawns"], stats["respawn_failures"]) == (1, 1, 0), stats
        assert len(stats["respawn_us"]) == 1 and stats["respawn_us"][0] > 0, stats
        assert stats["worker_pids"] == [answer["pid"]], stats
    finally:
        s.stop()


def replaced(s: Server, before: set[int]) -> set[int]:
    def fetch():
        pids = worker_pids(s)
        return pids if len(pids) == 2 and pids != before else None

    return poll_until(fetch, timeout=None, what="a replacement worker")


def test_a_handler_past_the_timeout_is_504_and_its_worker_is_replaced(server, tmp_path):
    before = worker_pids(server)
    # The handler never returns, so its answer can only be the lane's (1.5 s) timeout.
    status, _, body = server.get(block_path(tmp_path / "late.pid"), timeout=None)
    assert status == 504, body
    rid = json.loads(body)["request_id"]
    log = poll_until(lambda: rid in server.read() and server.read(), what="504 log line")
    assert "past the process-lane timeout" in next(ln for ln in log.splitlines() if rid in ln)
    after = replaced(server, before)
    assert len(after & before) == 1


def test_a_full_queue_is_503_never_a_wait(tmp_path):
    s = Server(tmp_path, MIPYRE_PROCESS_WORKERS=1, MIPYRE_PROCESS_QUEUE=8, MIPYRE_PROCESS_TIMEOUT=30)
    try:
        with concurrent.futures.ThreadPoolExecutor(40) as ex:
            results = list(ex.map(lambda _: s.get("/sleep?s=0.3", timeout=30)[0], range(40)))
        counts = {code: results.count(code) for code in set(results)}
        assert set(counts) <= {200, 503}, counts
        assert counts.get(503, 0) > 0 and counts.get(200, 0) >= 8, counts
    finally:
        s.stop()


def crash_loop_deaths_at_most(elapsed: float) -> int:
    """The most worker deaths a lane can have in `elapsed` seconds of requests that each kill
    their worker: the first 3 fork at once, then each fork waits 100 ms, doubled per crash, up
    to 5 s (the engine's `CrashBackoff::DEFAULT`)."""
    deaths, waited, wait = 3, 0.0, 0.1
    while waited + wait <= elapsed:
        waited += wait
        deaths += 1
        wait = min(wait * 2, 5.0)
    return deaths + 1  # the death the last wait ended with may be in flight


def test_a_route_that_kills_every_worker_backs_off_with_503_and_retry_after(tmp_path):
    s = Server(tmp_path, MIPYRE_PROCESS_WORKERS=1)
    try:
        started = time.monotonic()
        statuses, refused = [], None
        for _ in range(40):
            status, headers, body = s.get("/exit")
            statuses.append(status)
            if status == 503:
                refused = ({k.lower(): v for k, v in headers.items()}, json.loads(body))
                break
        elapsed = time.monotonic() - started
        # Crashes 1 and 2 fork at once; the third makes the lane wait before its next fork.
        assert statuses[:3] == [502, 502, 502], statuses
        assert refused is not None, statuses
        headers, body = refused
        assert int(headers["retry-after"]) >= 1, headers
        assert set(statuses) == {502, 503}, statuses
        stats = s.json("/lane_stats")
        assert stats["deaths"] == statuses.count(502), (stats, statuses)
        assert stats["deaths"] <= crash_loop_deaths_at_most(elapsed), (stats, elapsed)
        assert stats["respawns"] <= stats["deaths"], stats
        log = poll_until(lambda: body["request_id"] in s.read() and s.read(), what="the 503's log line")
        line = next(ln for ln in log.splitlines() if body["request_id"] in ln)
        assert "workers died without answering a request" in line and "exited with status 3" in line, line
        assert "crash-looping" in log
        # The lane forks again once its wait is over; an answer ends the crash run.
        poll_until(lambda: s.get("/hello")[0] == 200, timeout=None, what="the lane serving again")
        poll_until(lambda: "recovered" in s.read(), what="the recovery notice")
        # Counted from one again: the next two crashes fork at once.
        assert [s.get("/exit")[0] for _ in range(2)] == [502, 502]
        assert s.get("/hello")[0] == 200
    finally:
        s.stop()


def test_builtins_changed_in_one_worker_stay_in_that_worker(server):
    marker = server.json("/mark")["pid"]
    seen = {}
    for _ in range(5):
        with concurrent.futures.ThreadPoolExecutor(8) as ex:
            for r in ex.map(lambda _: server.json("/check"), range(64)):
                seen[r["pid"]] = r["mark"]
    assert seen[marker] == marker
    others = {p: m for p, m in seen.items() if p != marker}
    assert others and all(m is None for m in others.values()), seen
    assert server.json("/check_main")["mark"] is None


def test_shared_state_in_a_worker_raises_instead_of_reading_a_copy(server):
    status, _, body = server.get("/state")
    assert status == 500
    rid = json.loads(body)["request_id"]
    log = poll_until(lambda: rid in server.read() and server.read(), what="log line")
    assert "SharedState" in next(ln for ln in log.splitlines() if rid in ln)


def test_numpy_longdouble_from_int_in_a_worker(server):
    """Records what np.longdouble(int) does in a forked worker. On macOS with numpy 2.5.x
    it is expected to fail (errno left at EINVAL by a madvise in free on COW pages; fixed
    in numpy main, gh-32124). Not worked around: the outcome is printed as it is."""
    outcomes = []
    for _ in range(20):
        status, _, body = server.get("/longdouble?n=7")
        if status == 200:
            outcomes.append(("ok", json.loads(body)["value"]))
        else:
            rid = json.loads(body)["request_id"]
            log = poll_until(lambda: rid in server.read() and server.read(), what="log line")
            outcomes.append(("error", next(ln for ln in log.splitlines() if rid in ln)[-400:]))
    print("\nlongdouble outcomes:", json.dumps(outcomes[:3], indent=1), f"... ok={sum(o[0]=='ok' for o in outcomes)}/20")
    assert all(o[0] in ("ok", "error") for o in outcomes)
    for kind, value in outcomes:
        if kind == "ok":
            assert value == "7.0"


@pytest.mark.parametrize(
    "kwargs, fn_src, needle",
    [
        ({"body": BodyStream, "runtime": "process"}, "def h(req): return 1", "body=BodyStream"),
        ({"runtime": "process"}, "async def h(req): return 1", "async def"),
    ],
)
def test_unservable_process_routes_are_registration_errors(kwargs, fn_src, needle):
    from mipyre import Mipyre

    app = Mipyre()
    ns = {}
    exec(fn_src, ns)  # noqa: S102 — the handler is a parametrized source: its signature is what is under test
    with pytest.raises(ValueError, match=needle):
        app.post("/x", ns["h"], **kwargs)


def test_python_app_py_serves_process_routes_after_its_import_started_threads(tmp_path):
    """Under `python app.py` the app's first run starts the template, after the app was
    imported. This app's import ran a polars query, which started polars' threads: the
    template is a new interpreter on every platform (b/909), which the threads do not reach,
    so the route serves."""
    app = os.path.join(os.path.dirname(__file__), "apps", "process_lane_threaded_app.py")
    log_path = str(tmp_path / "server.log")
    with open(log_path, "w") as log:
        proc = subprocess.Popen([sys.executable, app], stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                                env=dict(os.environ, MIPYRE_PROCESS_WORKERS="1"))
    try:
        port = bound_port(read_file(log_path), proc)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/q", timeout=30) as r:
            assert r.status == 200 and json.loads(r.read()) == {"n": 3}
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10)


def test_a_polars_query_at_import_no_longer_matters(tmp_path):
    """Under `mipyre run` the template is forked before the app (and its polars query)
    runs, so each worker starts polars' pools itself: the route answers."""
    pytest.importorskip("polars")
    app = os.path.join(os.path.dirname(__file__), "apps", "process_lane_threaded_app.py")
    log_path = str(tmp_path / "server.log")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(launch(app), stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                                env=dict(os.environ, MIPYRE_PROCESS_WORKERS="2"))
    try:
        port = bound_port(read_file(log_path), proc)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/q", timeout=30) as r:
            assert r.status == 200 and json.loads(r.read()) == {"n": 3}
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10)


def test_a_polars_app_runs_on_process_lanes(tmp_path):
    """An app that imports polars at the top and declares runtime="process": no
    sub-interpreter is built (none of its routes runs on one), so polars is only ever
    imported by the server and the process workers."""
    pytest.importorskip("polars")
    app = os.path.join(os.path.dirname(__file__), "apps", "process_lane_polars_import_app.py")
    log_path = str(tmp_path / "server.log")
    env = dict(os.environ, MIPYRE_PROCESS_WORKERS="1")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(launch(app), stdout=log, stderr=subprocess.STDOUT, start_new_session=True, env=env)
    try:
        port = bound_port(read_file(log_path), proc)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/q", timeout=30) as r:
            assert r.status == 200
            assert json.loads(r.read()) == {"k": [1, 2], "v": [3.0, 12.0]}
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10)


def test_a_worker_stuck_in_a_handler_dies_with_its_template(tmp_path):
    """SIGKILL the template while its worker is in a handler: the worker is gone within 1 s
    (macOS: a kqueue EVFILT_PROC watch thread; Linux: PR_SET_PDEATHSIG). The worker is in its
    handler once the handler has written its pid (b/494: until then a 0.5 s sleep guessed it)."""
    s = Server(tmp_path, MIPYRE_PROCESS_WORKERS=1, MIPYRE_PROCESS_TIMEOUT=PATIENT_S)
    try:
        mark = tmp_path / "stuck.pid"
        with concurrent.futures.ThreadPoolExecutor(1) as ex:
            fut = ex.submit(s.get, block_path(mark), None)
            blocked = blocked_worker(mark)
            templates = [int(p) for p in subprocess.run(["pgrep", "-P", str(s.proc.pid)], capture_output=True, text=True).stdout.split()]
            assert len(templates) == 1, templates
            workers = [int(p) for p in subprocess.run(["pgrep", "-P", str(templates[0])], capture_output=True, text=True).stdout.split()]
            assert workers == [blocked], workers
            os.kill(templates[0], signal.SIGKILL)
            t0 = time.monotonic()

            def gone():
                try:
                    os.kill(workers[0], 0)
                    return False
                except ProcessLookupError:
                    return True

            poll_until(gone, timeout=1.0, interval=0.01, what="the worker exiting with its template")
            took = time.monotonic() - t0
            status, _, body = fut.result()
        assert status == 502, body  # its request: the worker died running it
        assert took < 1.0
    finally:
        s.stop()


def test_start_fork_serves_and_replaces_a_killed_worker(tmp_path):
    """start="fork": the template imported the app; workers are forked from it."""
    s = Server(tmp_path, MIPYRE_PROCESS_WORKERS=2, MIPYRE_PROCESS_START="fork", MIPYRE_PROCESS_TIMEOUT=1.5)
    try:
        assert s.get("/hello")[2] == b"hello"
        before = worker_pids(s)
        status, _, body = s.get("/exit")
        assert status == 502, body
        after = replaced(s, before)
        assert len(after & before) == 1
    finally:
        s.stop()


def test_start_fork_refuses_an_app_whose_import_starts_threads(tmp_path):
    """polars starts polars-ooc-cleaner at import, with no switch to stop it: under
    start="fork" the template would fork with it, so the start fails naming it."""
    pytest.importorskip("polars")
    app = os.path.join(os.path.dirname(__file__), "apps", "process_lane_polars_import_app.py")
    r = subprocess.run(launch(app), capture_output=True, text=True, timeout=120,
                       env=dict(os.environ, MIPYRE_PROCESS_WORKERS="1", MIPYRE_PROCESS_START="fork"))
    out = r.stdout + r.stderr
    assert r.returncode != 0, out[-3000:]
    assert "ThreadedTemplateError" in out and "polars-ooc-clea" in out, out[-3000:]
    assert 'start="import"' in out
    assert "Listening on" not in out or "could not start" in out


def test_a_process_lane_worker_keeps_the_seed_its_app_set_at_import(tmp_path):
    """An app that seeds `random` at import (a reproducible simulation) under `start="import"`:
    the worker's own reseed comes before the import, so the handler's first draw is the
    seeded one."""
    import random
    import textwrap

    app_file = tmp_path / "seeded_app.py"
    app_file.write_text(textwrap.dedent("""
        import random

        random.seed(1234)

        from mipyre import Mipyre

        app = Mipyre()

        @app.get("/draw", runtime="process")
        def draw(req):
            return {"draw": random.random()}
    """))
    log_path = tmp_path / "server.log"
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    env.update(MIPYRE_PROCESS_WORKERS="1", MIPYRE_PROCESS_START="import")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            launch(str(app_file)), stdout=log, stderr=subprocess.STDOUT, start_new_session=True, env=env, cwd=tmp_path
        )
    try:
        port = bound_port(read_file(str(log_path)), proc)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/draw", timeout=60) as r:
            drawn = json.loads(r.read())["draw"]
    finally:
        kill_session(proc)
    assert drawn == random.Random(1234).random(), read_file(str(log_path))()[-3000:]
