"""Tests for /livez and /readyz health probes."""

from __future__ import annotations



from mipyre import flush_logs
from mipyre.testing import TestClient


def _log_lines_with(capfd, needle: str) -> list[str]:
    """The process-log lines containing `needle`. The server runs in this process and logs
    them before it answers; its log writer is a background thread, so `flush_logs()` is the
    fence: past it, every line logged before is on stderr, and stderr is read once (b/497). Read
    while the writer writes, a line can be lost: `readouterr()` reads the capture file and then
    truncates it, and a line written in between is gone."""
    flush_logs()
    return [line for line in capfd.readouterr().err.splitlines() if needle in line]


def test_livez_returns_200_always():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_livez import app

    with TestClient(app, port=None) as c:
        r = c.get("/livez")
        assert r.status_code == 200
        assert r.json() == {"status": "alive"}


def test_readyz_ok_when_no_checks():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_no_checks import app

    with TestClient(app, port=None) as c:
        r = c.get("/readyz")
        assert r.status_code == 200
        data = r.json()
        assert data == {"status": "ready", "checks": {}}


def test_readyz_ok_with_passing_checks():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_passing_checks import app

    with TestClient(app, port=None) as c:
        r = c.get("/readyz")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ready"
        assert data["checks"]["always_ok"] == {"ok": True}
        assert data["checks"]["none_also_ok"] == {"ok": True}


def test_readyz_503_on_exception():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_raising_check import app

    with TestClient(app, port=None) as c:
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

    with TestClient(app, port=None) as c:
        r = c.get("/readyz")
        assert r.status_code == 503
        assert r.json()["checks"]["feature_flag"]["ok"] is False


def test_readyz_aggregates_multiple_checks():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_mixed_checks import app

    with TestClient(app, port=None) as c:
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

    with TestClient(app, port=None) as c:
        r = c.get("/readyz")
        assert r.status_code == 503
        data = r.json()
        assert data["checks"]["async_ok"]["ok"] is True
        # The exception goes to the log, not to the client (D4).
        assert data["checks"]["async_fail"] == {"ok": False}
        assert isinstance(data["request_id"], str) and data["request_id"]


def test_enable_health_probes_idempotent():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_enabled_twice import app

    with TestClient(app, port=None) as c:
        assert c.get("/livez").status_code == 200


def test_custom_paths():
    # Its own module: a worker serves one app per module.
    from tests.apps.health_custom_paths import app

    with TestClient(app, port=None) as c:
        assert c.get("/_alive").status_code == 200
        assert c.get("/_ready").status_code == 200
        # Default paths are NOT registered.
        assert c.get("/livez").status_code == 404


def test_check_registered_after_enable_still_runs(capfd):
    """You can enable probes early (e.g., in Mipyre() setup) and register
    checks later as modules load. The readyz handler closes over the
    shared list, so late appends take effect immediately. The late check's
    failure is logged with the request id, which proves it ran."""
    # Its own module: a worker serves one app per module.
    from tests.apps.health_late_check import app

    with TestClient(app, port=None) as c:
        r = c.get("/readyz")
        assert r.status_code == 503
        data = r.json()
        assert data["checks"]["late"] == {"ok": False}
        rid = data["request_id"]
        lines = _log_lines_with(capfd, "late check ran")
        assert len(lines) == 1, lines
        assert rid in lines[0], lines[0]
