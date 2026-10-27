"""Tests for /livez and /readyz health probes."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap

import pytest

import mipyre._health as health
from mipyre import HealthProbes, Mipyre, flush_logs
from mipyre import RuntimeDeclarationError
from mipyre.testing import TestClient


def _log_lines_with(capfd, needle: str) -> list[str]:
    """The process-log lines containing `needle`. The server runs in this process and logs
    them before it answers; its log writer is a background thread, so `flush_logs()` is the
    fence: past it, every line logged before is on stderr, and stderr is read once. Read
    while the writer writes, a line can be lost: `readouterr()` reads the capture file and then
    truncates it, and a line written in between is gone."""
    flush_logs()
    return [line for line in capfd.readouterr().err.splitlines() if needle in line]


def test_livez_returns_200_always():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_livez import app

    with TestClient(app) as c:
        r = c.get("/livez")
        assert r.status_code == 200
        assert r.json() == {"status": "alive"}


def test_readyz_ok_when_no_checks():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_no_checks import app

    with TestClient(app) as c:
        r = c.get("/readyz")
        assert r.status_code == 200
        data = r.json()
        assert data == {"status": "ready", "checks": {"abandoned_workers": {"ok": True}}}


def test_readyz_ok_with_passing_checks():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_passing_checks import app

    with TestClient(app) as c:
        r = c.get("/readyz")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ready"
        assert data["checks"]["always_ok"] == {"ok": True}
        assert data["checks"]["none_also_ok"] == {"ok": True}


def test_readyz_503_on_exception():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_raising_check import app

    with TestClient(app) as c:
        r = c.get("/readyz")
        assert r.status_code == 503
        data = r.json()
        assert data["status"] == "not_ready"
        # The exception goes to the log, not to the client (D4).
        assert data["checks"]["db"] == {"ok": False}
        assert isinstance(data["request_id"], str) and data["request_id"]


def test_readyz_503_on_false_return():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_false_check import app

    with TestClient(app) as c:
        r = c.get("/readyz")
        assert r.status_code == 503
        assert r.json()["checks"]["feature_flag"]["ok"] is False


def test_readyz_aggregates_multiple_checks():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_mixed_checks import app

    with TestClient(app) as c:
        r = c.get("/readyz")
        # One failure → overall 503, but every check is reported.
        assert r.status_code == 503
        checks = r.json()["checks"]
        assert checks["ok1"]["ok"] is True
        assert checks["ok2"]["ok"] is True
        assert checks["bad"]["ok"] is False


def test_async_readiness_check_supported():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_async_checks import app

    with TestClient(app) as c:
        r = c.get("/readyz")
        assert r.status_code == 503
        data = r.json()
        assert data["checks"]["async_ok"]["ok"] is True
        # The exception goes to the log, not to the client (D4).
        assert data["checks"]["async_fail"] == {"ok": False}
        assert isinstance(data["request_id"], str) and data["request_id"]


def test_a_second_set_of_probes_on_the_same_paths_is_refused():
    app = Mipyre()
    HealthProbes(app)
    with pytest.raises(ValueError, match="already registered for GET /livez"):
        HealthProbes(app)


def test_custom_paths():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_custom_paths import app

    with TestClient(app) as c:
        assert c.get("/_alive").status_code == 200
        assert c.get("/_ready").status_code == 200
        # Default paths are NOT registered.
        assert c.get("/livez").status_code == 404


def test_check_registered_after_the_probes_still_runs(capfd):
    """You can create the probes early (e.g., in Mipyre() setup) and register
    checks later as modules load. The readyz handler closes over the
    shared list, so late appends take effect immediately. The late check's
    failure is logged with the request id, which proves it ran."""
    # Its own module: a worker serves one app per module.
    from tests.apps.health_late_check import app

    with TestClient(app) as c:
        r = c.get("/readyz")
        assert r.status_code == 503
        data = r.json()
        assert data["checks"]["late"] == {"ok": False}
        rid = data["request_id"]
        lines = _log_lines_with(capfd, "late check ran")
        assert len(lines) == 1, lines
        assert rid in lines[0], lines[0]


def test_livez_answers_past_a_hook_that_refuses_the_rest():
    """The engine answers /livez itself: a hook that asks for credentials (or an interpreter
    too busy to run one) does not make a live pod look dead."""
    from tests.apps.health_behind_auth import app

    with TestClient(app) as c:
        r = c.get("/livez")
        assert (r.status_code, r.json()) == (200, {"status": "alive"})
        assert c.get("/readyz").status_code == 401


@pytest.mark.parametrize("runtime", ["subinterp", "isolated"])
def test_readiness_probes_refuse_a_sub_interpreter_runtime(runtime):
    with pytest.raises(RuntimeDeclarationError, match="sub-interpreter"):
        HealthProbes(Mipyre(), runtime=runtime)


_HUNG_CHECK_APP = """
    import threading

    import mipyre._health

    mipyre._health._CHECK_TIMEOUT_S = 0.2  # injected: the real one is 10 s

    from mipyre import HealthProbes, Mipyre
    from mipyre.testing import TestClient

    app = Mipyre()
    probes = HealthProbes(app)
    _never = threading.Event()


    @probes.check("db")
    def db():
        _never.wait()


    if __name__ == "__main__":
        app.shutdown_timeout = 1.0  # injected: the grace period a stuck worker gets
        c = TestClient(app)
        print("readyz", c.get("/readyz").status_code, flush=True)
        try:
            c.close()
            print("closed", flush=True)
        except Exception as e:
            print("close raised", type(e).__name__, flush=True)
"""


def test_a_hung_readiness_check_does_not_stop_the_server_shutting_down(tmp_path):
    """One /readyz probe while a check hangs (a DB without a connect timeout) answers 503,
    and the server then stops cleanly: the abandoned check thread is a daemon. (In a
    sub-interpreter it could not be one; that runtime is refused above.)"""
    script = tmp_path / "app_script.py"
    script.write_text(textwrap.dedent(_HUNG_CHECK_APP))
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    env["MIPYRE_THREADS"] = "1"
    proc = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, env=env, timeout=120, cwd=tmp_path
    )
    lines = [line for line in proc.stdout.splitlines() if line.startswith(("readyz", "closed", "close raised"))]
    assert lines == ["readyz 503", "closed"], (proc.stdout, proc.returncode, proc.stderr[-2000:])
    assert proc.returncode == 0


# -- how often checks run and failures are logged: the decisions, fed times directly ----------

_PERIOD = 60.0
_DB = frozenset({"db"})


def test_a_new_failure_is_logged_at_once():
    log = health._FailureLog(_PERIOD)
    assert log.note(_DB, "r1", now=100.0) == 0
    assert log.request_id == "r1"


def test_the_same_failure_is_held_until_the_period_has_passed():
    log = health._FailureLog(_PERIOD)
    log.note(_DB, "r1", now=100.0)
    assert log.note(_DB, "r2", now=100.0 + _PERIOD - 0.001) is None
    assert log.note(_DB, "r3", now=100.0 + _PERIOD - 0.001) is None
    # A held probe cites the one whose lines say why.
    assert log.request_id == "r1"
    assert log.note(_DB, "r4", now=100.0 + _PERIOD) == 2
    assert log.request_id == "r4"


def test_a_change_in_which_checks_fail_is_logged_within_the_period():
    log = health._FailureLog(_PERIOD)
    log.note(_DB, "r1", now=100.0)
    log.note(_DB, "r2", now=101.0)
    assert log.note(frozenset({"db", "cache"}), "r3", now=102.0) == 1
    assert log.request_id == "r3"


def test_recovery_is_logged_once_and_a_later_failure_is_news_again():
    log = health._FailureLog(_PERIOD)
    log.note(_DB, "r1", now=100.0)
    log.note(_DB, "r2", now=101.0)
    assert log.note(frozenset(), "r3", now=102.0) == 1
    assert log.note(frozenset(), "r4", now=103.0) is None
    assert log.note(_DB, "r5", now=104.0) == 0


def _results(*failing: str) -> health._CheckResults:
    names = ("db", "cache")
    return health._CheckResults(
        passed={name: name not in failing for name in names},
        failures=tuple(health._CheckFailure(name, None) for name in names if name in failing),
    )


def test_an_answer_is_reused_until_its_ttl_has_passed():
    readiness = health._Readiness([], ttl=1.0, period=_PERIOD)
    answer = readiness.settle(_results(), "r1", now=100.0)
    assert answer.status == 200
    assert readiness.cached(now=100.999) is answer
    assert readiness.cached(now=101.0) is None


def test_a_held_503_cites_the_probe_whose_log_says_why():
    readiness = health._Readiness([], ttl=1.0, period=_PERIOD)
    first = json.loads(readiness.settle(_results("db"), "r1", now=100.0).body)
    later = json.loads(readiness.settle(_results("db"), "r2", now=102.0).body)
    assert first == later == {"status": "not_ready", "checks": {"db": {"ok": False}, "cache": {"ok": True}}, "request_id": "r1"}
