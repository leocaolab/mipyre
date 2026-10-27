"""Tests for passive GIL contention monitoring.

The GIL watchdog was replaced with passive measurement: each request
handler records GIL acquisition wait time as a byproduct, eliminating
the active probe thread (zero observer-effect overhead).

Covers:
- get_gil_metrics() returns a Metrics snapshot with typed, named fields
- Probe count increments with real requests
- GIL hold peak tracks CPU-heavy handlers
- Total requests counter works
"""

import os

import pytest
from mipyre import Metrics, Mipyre, get_gil_metrics, reset_peaks
from mipyre.engine import RunMetrics
from mipyre.testing import TestClient


@pytest.fixture(scope="module")
def client():
    # The request count is gated by MIPYRE_METRICS=1 (default off), read when the server
    # starts (its run's counters): flip it before TestClient spawns the server thread.
    # Put back afterwards: left set, it would turn metrics on for whichever tests this
    # process runs next, and under xdist that set differs from run to run.
    before = os.environ.get("MIPYRE_METRICS")
    os.environ["MIPYRE_METRICS"] = "1"
    app = Mipyre(runtime="main")

    @app.get("/")
    def index(req):
        return {"ok": True}

    @app.get("/heavy")
    def heavy(req):
        """Simulate a handler that holds the GIL for a measurable time."""
        total = 0
        for i in range(200_000):
            total += i
        return {"total": total}

    # Main interpreter: these tests measure how long handlers hold the main GIL.
    c = TestClient(app)
    yield c
    c.close()
    if before is None:
        del os.environ["MIPYRE_METRICS"]
    else:
        os.environ["MIPYRE_METRICS"] = before


def test_metrics_shape():
    """get_gil_metrics() returns a Metrics snapshot of integer counters."""
    m = get_gil_metrics()
    assert isinstance(m, Metrics)
    for name in (
        "gil_wait_last_us",
        "gil_wait_peak_us",
        "gil_wait_count",
        "gil_wait_total_us",
        "gil_queue_length",
        "gil_hold_peak_us",
    ):
        assert isinstance(getattr(m, name), int), name
    assert m.rss_bytes is None or isinstance(m.rss_bytes, int)


def test_run_metrics_shape(client):
    """A server's own counts (``Server.metrics()``) are a RunMetrics snapshot."""
    m = client.server.metrics()
    assert isinstance(m, RunMetrics)
    assert isinstance(m.dropped_requests, int)
    assert m.total_requests is None or isinstance(m.total_requests, int)


def test_degradation_counters_are_exposed(client):
    """The degradations the engine took are counted: the process's in its snapshot, a
    server's in its own; the counters are integers, and the CPU sizing fallback is a reason or
    None."""
    m = get_gil_metrics()
    for name in (
        "precompressed_fallbacks",
        "static_unwatched_mounts",
        "static_answers_cut",
        "db_results_undeliverable",
        "log_lines_dropped",
        "log_lines_write_failed",
        "gil_congested_waits",
        "client_disconnects",
        "accept_failures",
        "accept_exhausted",
        "tls_handshake_failures",
        "malformed_requests",
        "oversized_request_heads",
        "first_request_timeouts",
        "request_head_timeouts",
        "forwarded_for_unreadable",
        "quickack_failures",
        "defer_accept_failures",
        "nodelay_failures",
    ):
        assert isinstance(getattr(m, name), int), name
    run = client.server.metrics()
    for name in (
        "workers_serving",
        "workers_died",
        "gc_failures",
        "workers_retired",
        "cpu_pin_failures",
        "cpu_qos_failures",
        "cpu_affinity_unreadable",
        "cpu_topology_unreadable",
        "static_bytes_in_flight",
    ):
        assert isinstance(getattr(run, name), int), name
    assert run.last_worker_retirement is None or isinstance(run.last_worker_retirement, str)
    assert run.cpu_sizing_fallback is None or isinstance(run.cpu_sizing_fallback, str)


def test_probe_count_increments(client):
    """After requests, passive probe count should reflect handler invocations."""
    reset_peaks()

    for _ in range(10):
        resp = client.get("/")
        assert resp.status_code == 200

    probes = get_gil_metrics().gil_wait_count
    assert probes >= 10, f"Expected >= 10 probes, got {probes}"


def test_heavy_handler_hold_peak(client):
    """CPU-heavy handler should produce higher GIL hold times."""
    reset_peaks()

    resp = client.get("/heavy")
    assert resp.status_code == 200

    hold_peak_us = get_gil_metrics().gil_hold_peak_us
    # CPU loop should hold GIL for at least a few hundred microseconds
    assert hold_peak_us > 100, f"Expected hold_peak > 100us, got {hold_peak_us}"


def test_degraded_lists_what_runs_degraded():
    """The stats carry `degraded`, a list of `key: reason`: empty while nothing is degraded, so
    a server running without its CPU pinning or RSS sampling says so there (not only in a log)."""
    degraded = get_gil_metrics().degraded
    assert isinstance(degraded, list)
    assert all(isinstance(line, str) and ": " in line for line in degraded), degraded
