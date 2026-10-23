"""Tests for X-Request-ID propagation + Prometheus /metrics."""

from __future__ import annotations

import re

import pytest

from mipyre import Mipyre, Response
from mipyre.testing import TestClient

from tests._helpers import metric_count, metric_value


# ---------------------------------------------------------------------------
# X-Request-ID
# ---------------------------------------------------------------------------


def test_request_id_minted_when_absent():
    # Its own module: a worker serves one app per module.
    from tests.apps.obs_request_id import app

    with TestClient(app) as c:
        r = c.get("/")
        assert r.status_code == 200
        rid = r.headers.get("X-Request-ID") or r.headers.get("x-request-id")
        assert rid is not None
        # UUID hex: 32 chars, no dashes (uuid4().hex).
        assert re.fullmatch(r"[0-9a-f]{32}", rid)


def test_request_id_echoed_when_client_supplies():
    # Its own module: a worker serves one app per module.
    from tests.apps.obs_request_id_echo import app

    with TestClient(app) as c:
        r = c.get("/", headers={"X-Request-ID": "trace-abc-123"})
        assert r.status_code == 200
        rid = r.headers.get("X-Request-ID") or r.headers.get("x-request-id")
        assert rid == "trace-abc-123"


def test_enable_request_id_header_idempotent():
    # Its own module: a worker serves one app per module.
    from tests.apps.obs_request_id_twice import app

    with TestClient(app) as c:
        r = c.get("/")
        rid_values = [v for k, v in r.headers.items() if k.lower() == "x-request-id"]
        # Exactly one header — not two copies from a double-install.
        assert len(rid_values) == 1


def test_request_id_header_is_on_a_500():
    """A 500 from a raising handler carries the id, the same one its body reports."""
    from mipyre import Mipyre

    app = Mipyre(runtime="main")
    app.enable_request_id_header()

    @app.get("/boom")
    def boom(req):
        raise LookupError("handler bug")

    with TestClient(app) as c:
        r = c.get("/boom")
    assert r.status_code == 500
    assert "x-request-id" in r.headers, r.headers
    assert r.headers["x-request-id"] == r.json()["request_id"]


def test_every_answer_carries_the_clients_request_id():
    """Whoever answers (the handler, a before hook, a raised exception, the engine's 404 and
    405, a fast route), the response carries the client's id, once, replacing the handler's."""
    from mipyre import Mipyre, Response

    app = Mipyre(runtime="main")
    app.enable_request_id_header()
    app.add_fast_response("GET", "/fast", "ok")

    @app.before_request
    def auth(req):
        if req.path == "/private":
            return Response(body="no", status_code=401)
        return None

    @app.get("/ok")
    def ok(req):
        return Response(body="ok", headers={"X-Request-ID": "set-by-the-handler"})

    @app.get("/private")
    def private(req):
        return "secret"

    @app.get("/boom")
    def boom(req):
        raise LookupError("handler bug")

    @app.post("/only-post")
    def only_post(req):
        return "ok"

    with TestClient(app) as c:
        for path, status in (("/ok", 200), ("/private", 401), ("/boom", 500), ("/missing", 404),
                             ("/only-post", 405), ("/fast", 200)):
            r = c.get(path, headers={"X-Request-ID": "trace-7f"})
            assert r.status_code == status, path
            values = [v for k, v in r.headers.items() if k.lower() == "x-request-id"]
            assert values == ["trace-7f"], (path, r.headers)


def test_without_the_header_enabled_no_response_carries_one():
    from mipyre import Mipyre

    app = Mipyre(runtime="main")

    @app.get("/ok")
    def ok(req):
        return "ok"

    with TestClient(app) as c:
        for path in ("/ok", "/missing"):
            assert "x-request-id" not in c.get(path, headers={"X-Request-ID": "trace-7f"}).headers


def test_enable_request_id_header_with_another_header_is_refused():
    import pytest

    from mipyre import Mipyre

    app = Mipyre(runtime="main")
    app.enable_request_id_header("X-Trace-Id")
    app.enable_request_id_header("x-trace-id")
    with pytest.raises(ValueError, match="already x-trace-id"):
        app.enable_request_id_header("X-Request-ID")


def test_custom_header_name():
    # Its own module: a worker serves one app per module.
    from tests.apps.obs_request_id_custom_header import app

    with TestClient(app) as c:
        r = c.get("/", headers={"X-Trace-Id": "custom-42"})
        assert r.headers.get("X-Trace-Id") == "custom-42" or \
               r.headers.get("x-trace-id") == "custom-42"


# ---------------------------------------------------------------------------
# Prometheus /metrics
# ---------------------------------------------------------------------------


def test_metrics_endpoint_content_type():
    # Its own module: a worker serves one app per module.
    from tests.apps.obs_metrics import app

    with TestClient(app) as c:
        r = c.get("/metrics")
        assert r.status_code == 200
        assert "text/plain" in r.headers.get("content-type", r.headers.get("Content-Type", ""))


def test_metrics_counts_requests():
    # Its own module: a worker serves one app per module.
    from tests.apps.obs_metrics_counts import app

    with TestClient(app) as c:
        # TestClient sends one GET / during startup to probe readiness;
        # snapshot the counter so we measure only what this test drives.
        base = metric_count(c.get("/metrics").text, "mipyre_http_requests_total")

        for _ in range(3):
            c.get("/")
        c.get("/boom")
        c.get("/boom")

        body = c.get("/metrics").text
        # A page shows the answers given before it: the scrape `base` came from, three /, two
        # /boom. The scrape now rendering is counted once it has gone.
        assert metric_count(body, "mipyre_http_requests_total") == base + 6
        assert metric_count(body, "mipyre_http_requests_total", status_class="5xx") == 2


def test_metrics_scrape_is_counted():
    # Its own module: a worker serves one app per module.
    from tests.apps.obs_metrics_scrape import app

    with TestClient(app) as c:
        base = metric_count(c.get("/metrics").text, "mipyre_http_requests_total")
        c.get("/")
        c.get("/metrics")
        body = c.get("/metrics").text
        # The scrape `base` came from, /, and the scrape after it.
        assert metric_count(body, "mipyre_http_requests_total") == base + 3


def test_metrics_records_latency():
    # Its own module: a worker serves one app per module.
    from tests.apps.obs_metrics_latency import app

    name = "mipyre_http_request_duration_seconds"
    with TestClient(app) as c:
        before = c.get("/metrics").text
        c.get("/slow")
        after = c.get("/metrics").text

    def grew(sample: str, **labels: str) -> float:
        return metric_value(after, sample, **labels) - metric_value(before, sample, **labels)

    # The scrape `before` came from and /slow.
    assert grew(f"{name}_count") == 2
    assert grew(f"{name}_bucket", le="+Inf") == 2
    # /slow sleeps 10 ms, so it is past the 5 ms bound; only the scrape can be within it.
    assert grew(f"{name}_bucket", le="0.005") <= 1
    assert grew(f"{name}_sum") >= 0.01


def test_metrics_counts_a_500_from_a_raising_handler():
    """A handler that raises is answered 500 by the engine; it is counted as a 5xx."""
    app = Mipyre(runtime="main")
    app.enable_metrics()

    @app.get("/ok")
    def ok(req):
        return "ok"

    @app.get("/boom")
    def boom(req):
        raise LookupError("handler bug")

    with TestClient(app) as c:
        assert c.get("/ok").status_code == 200
        assert c.get("/boom").status_code == 500
        text = c.get("/metrics").text
    counted = {cls: metric_count(text, "mipyre_http_requests_total", status_class=cls) for cls in ("2xx", "5xx")}
    assert counted == {"2xx": 1, "5xx": 1}


def test_metrics_counts_a_request_a_before_hook_answered():
    """A ``before_request`` hook that answers (401 from an auth check) skips the handler and the
    after hooks; its answer is counted all the same."""
    app = Mipyre(runtime="main")

    @app.before_request
    def auth(req):
        if req.path == "/private":
            return Response(body="no", status_code=401)
        return None

    app.enable_metrics()

    @app.get("/private")
    def private(req):
        return "secret"

    @app.get("/ok")
    def ok(req):
        return "ok"

    with TestClient(app) as c:
        base = metric_count(c.get("/metrics").text, "mipyre_http_requests_total")
        assert c.get("/ok").status_code == 200
        assert c.get("/private").status_code == 401
        text = c.get("/metrics").text
    # The scrape `base` came from, /ok and /private.
    assert metric_count(text, "mipyre_http_requests_total") == base + 3
    assert metric_count(text, "mipyre_http_requests_total", status_class="4xx", method="GET") >= 1


def test_metrics_counts_what_the_engine_answers_by_itself():
    """A 404, a 405 and a fast route are answered without Python, and each is counted."""
    app = Mipyre(runtime="main")
    app.enable_metrics()
    app.add_fast_response("GET", "/fast", "ok")

    @app.post("/only-post")
    def only_post(req):
        return "ok"

    with TestClient(app) as c:
        before = c.get("/metrics").text
        assert c.get("/missing").status_code == 404
        assert c.get("/only-post").status_code == 405
        assert c.get("/fast").status_code == 200
        after = c.get("/metrics").text

    def grew(**labels: str) -> int:
        name = "mipyre_http_requests_total"
        return metric_count(after, name, **labels) - metric_count(before, name, **labels)

    # The 2xx are /fast and the scrape `before` came from.
    assert (grew(status_class="2xx"), grew(status_class="4xx")) == (2, 2)


def test_metrics_page_is_guarded_by_the_apps_hooks():
    app = Mipyre(runtime="main")

    @app.before_request
    def auth(req):
        if req.headers.get("authorization") != "Bearer t":
            return Response(body="no", status_code=401)
        return None

    app.enable_metrics()

    with TestClient(app) as c:
        assert c.get("/metrics").status_code == 401
        r = c.get("/metrics", headers={"Authorization": "Bearer t"})
        assert r.status_code == 200
        assert "# TYPE mipyre_http_requests_total counter" in r.text


def test_each_run_shows_its_own_answers():
    app = Mipyre(runtime="main")
    app.enable_metrics()

    @app.get("/ok")
    def ok(req):
        return "ok"

    for _ in range(2):
        with TestClient(app) as c:
            base = metric_count(c.get("/metrics").text, "mipyre_http_requests_total")
            for _ in range(10):
                c.get("/ok")
            body = c.get("/metrics").text
        # The scrape `base` came from and the ten; nothing of the run before.
        assert metric_count(body, "mipyre_http_requests_total") == base + 11
        assert base == 0, "a run starts with nothing counted"


def test_enable_metrics_at_another_path_is_refused():
    app = Mipyre(runtime="main")
    app.enable_metrics()
    app.enable_metrics("/metrics")
    with pytest.raises(ValueError, match="already enabled"):
        app.enable_metrics("/other")
    with pytest.raises(ValueError, match="path parameter"):
        Mipyre(runtime="main").enable_metrics("/m/{name}")


def test_an_apps_own_route_at_the_metrics_path_fails_the_start():
    app = Mipyre(runtime="main")
    app.enable_metrics()

    @app.get("/metrics")
    def mine(req):
        return "mine"

    with pytest.raises(ValueError, match="cannot serve the metrics page"):
        with TestClient(app):
            pass


def test_metrics_exposes_the_engine_degradation_counters():
    """The engine's degradation counters (b/132) are on the same page, with HELP and TYPE, and
    the per-kind families carry their labels."""
    from tests.apps.obs_metrics_format import app

    with TestClient(app) as c:
        body = c.get("/metrics").text
    for family, kind in (
        ("mipyre_workers_serving", "gauge"),
        ("mipyre_workers_died_total", "counter"),
        ("mipyre_gc_failures_total", "counter"),
        ("mipyre_workers_retired_total", "counter"),
        ("mipyre_precompressed_fallbacks_total", "counter"),
        ("mipyre_static_unwatched_mounts_total", "counter"),
        ("mipyre_static_answers_cut_total", "counter"),
        ("mipyre_static_bytes_in_flight", "gauge"),
        ("mipyre_client_disconnects_total", "counter"),
        ("mipyre_tls_handshake_failures_total", "counter"),
        ("mipyre_bad_requests_total", "counter"),
        ("mipyre_request_head_timeouts_total", "counter"),
        ("mipyre_forwarded_for_unreadable_total", "counter"),
        ("mipyre_socket_option_failures_total", "counter"),
        ("mipyre_cpu_degraded_total", "counter"),
    ):
        assert f"# HELP {family} " in body, family
        assert f"# TYPE {family} {kind}" in body, family
    assert 'mipyre_socket_option_failures_total{option="nodelay"} ' in body
    assert 'mipyre_bad_requests_total{kind="malformed"} ' in body
    assert 'mipyre_request_head_timeouts_total{kind="first_request"} ' in body
    assert 'mipyre_cpu_degraded_total{kind="pin_failed"} ' in body
    assert re.search(r"^mipyre_workers_serving -?\d+$", body, re.MULTILINE)


def test_metrics_renders_the_drain_counters():
    """The drain counters (b/175: a drain that ran out of time, and the connections it closed)
    are on /metrics with HELP and TYPE, as counters."""
    from tests.apps.obs_metrics_format import app

    with TestClient(app) as c:
        body = c.get("/metrics").text
    for family in ("mipyre_drain_timeouts_total", "mipyre_drain_closed_connections_total"):
        assert f"# HELP {family} " in body, family
        assert f"# TYPE {family} counter" in body, family
        assert metric_count(body, family) >= 0, family


def test_metrics_renders_the_stop_watcher_failuresmetric_count():
    """The stop watchers that did not end cleanly (b/634) are on /metrics with HELP and TYPE, as a
    counter, with the engine's own count."""
    from tests.apps.obs_metrics_format import app

    with TestClient(app) as c:
        body = c.get("/metrics").text
        counted = c.server.metrics().stop_watcher_failures
    family = "mipyre_stop_watcher_failures_total"
    assert f"# HELP {family} " in body
    assert f"# TYPE {family} counter" in body
    assert metric_count(body, family) == counted


def test_metrics_renders_the_blocking_calls_leftmetric_count():
    """The blocking calls a stopping TPC thread left running (b/634) are on /metrics with HELP
    and TYPE, as a counter, with the engine's own count."""
    from tests.apps.obs_metrics_format import app

    with TestClient(app) as c:
        body = c.get("/metrics").text
        counted = c.server.metrics().blocking_calls_left
    family = "mipyre_blocking_calls_left_total"
    assert f"# HELP {family} " in body
    assert f"# TYPE {family} counter" in body
    assert metric_count(body, family) == counted


def test_metrics_shows_the_unpinned_starts_by_cause():
    """A start whose TPC threads ran unpinned because the CPUs to pin to could not be read
    (b/148) is on /metrics under its cause, with the engine's own count."""
    from tests.apps.obs_metrics_format import app

    with TestClient(app) as c:
        body = c.get("/metrics").text
        m = c.server.metrics()
    for kind, value in (
        ("affinity_unreadable", m.cpu_affinity_unreadable),
        ("topology_unreadable", m.cpu_topology_unreadable),
    ):
        assert f'mipyre_cpu_degraded_total{{kind="{kind}"}} {value}' in body, kind


def test_metrics_format_has_help_and_type_lines():
    # Its own module: a worker serves one app per module.
    from tests.apps.obs_metrics_format import app

    with TestClient(app) as c:
        body = c.get("/metrics").text
        # Every counter has HELP + TYPE directives before the value.
        assert "# HELP mipyre_http_requests_total" in body
        assert "# TYPE mipyre_http_requests_total counter" in body
        assert "# HELP mipyre_http_request_duration_seconds" in body
        assert "# TYPE mipyre_http_request_duration_seconds histogram" in body


def test_enable_metrics_idempotent():
    # Its own module: a worker serves one app per module.
    from tests.apps.obs_metrics_twice import app

    with TestClient(app) as c:
        base = metric_count(c.get("/metrics").text, "mipyre_http_requests_total")
        c.get("/")
        body = c.get("/metrics").text
        # The scrape `base` came from and /, each once: enabled twice, nothing counts twice.
        assert metric_count(body, "mipyre_http_requests_total") == base + 2


def test_custom_metrics_path():
    # Its own module: a worker serves one app per module.
    from tests.apps.obs_metrics_custom_path import app

    with TestClient(app) as c:
        c.get("/")
        r = c.get("/_/prom")
        assert r.status_code == 200
        assert "mipyre_http_requests_total" in r.text
        # Default /metrics is NOT registered.
        assert c.get("/metrics").status_code == 404
