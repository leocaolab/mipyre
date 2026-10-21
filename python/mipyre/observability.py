"""Observability helpers — X-Request-ID + Prometheus ``/metrics``.

Two opt-in toggles on ``Mipyre``:

    app.enable_request_id()   # guarantees X-Request-ID on every response
    app.enable_metrics()      # GET /metrics → Prometheus text format

Both ride on ``before_request`` / ``after_request`` hooks. The metrics
counters live in ``app.state``, the one map every interpreter shares: each
sub-interpreter worker has its own Python globals, so a module-level dict
would count per worker.

Metrics exposed (RED-style, no histograms):

- ``mipyre_http_requests_total`` — global request counter
- ``mipyre_http_requests_by_class_total{class="2xx|3xx|4xx|5xx"}``
- ``mipyre_http_requests_by_method_total{method="GET|POST|..."}``
- ``mipyre_http_request_duration_seconds_sum``
- ``mipyre_http_request_duration_seconds_count``

Latency is a running sum + count; the average is ``sum / count`` in the
dashboard.
"""

from __future__ import annotations

import logging
import time
from contextvars import ContextVar
from typing import TYPE_CHECKING

from mipyre.engine import Response, get_gil_metrics

if TYPE_CHECKING:
    from mipyre.app import Mipyre


_STATUS_CLASSES = ("1xx", "2xx", "3xx", "4xx", "5xx")
_TRACKED_METHODS = ("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS")

# Per-request values the before hook hands to the after hook. ContextVars, not a
# thread-local: async requests interleave on one event-loop thread, and each one runs in
# its own context, so a thread-local would hand one request's value to another.
_request_id: ContextVar[str | None] = ContextVar("mipyre_obs_request_id", default=None)
_metrics_start_ns: ContextVar[int | None] = ContextVar("mipyre_obs_metrics_start_ns", default=None)


def install_request_id(app: "Mipyre", header: str) -> None:
    from mipyre.context import ctx

    header_lower = header.lower()

    def _before(req):
        # The engine wrote the request's id once, as `req.request_id` (the client's `header`
        # value when it sent a usable one); a 5xx reports the same id. Stash it for the
        # after-hook, which echoes it without mutating the frozen req.
        rid = req.request_id
        _request_id.set(rid)
        ctx.set_request_id(rid)
        return None

    def _after(req, resp):
        rid = _request_id.get()
        if rid is None:
            return resp
        # Header names are case-insensitive but dict keys are not: drop any case variant
        # already set, or the response would carry the header twice.
        merged = {k: v for k, v in resp.headers.items() if k.lower() != header_lower}
        merged[header_lower] = rid
        return Response(
            body=resp.body,
            status_code=resp.status_code,
            content_type=resp.content_type,
            headers=merged,
        )

    app.before_request(_before)
    app.after_request(_after)


def install_metrics(app: "Mipyre", path: str) -> None:
    state = app.state

    def _before(req):
        _metrics_start_ns.set(time.monotonic_ns())
        return None

    def _after(req, resp):
        # Don't count the /metrics scrape itself — it would turn the
        # counter into a self-fulfilling load generator.
        if req.path == path:
            return resp

        # Counting must not break the request: a failure is logged, the response kept.
        try:
            status = resp.status_code
            state.incr("_m:req:total", 1)
            state.incr(f"_m:req:class:{status // 100}xx", 1)
            method = req.method.upper()
            if method in _TRACKED_METHODS:
                state.incr(f"_m:req:method:{method}", 1)

            start = _metrics_start_ns.get()
            if start is not None:
                elapsed_us = max(0, (time.monotonic_ns() - start) // 1000)
                state.incr("_m:lat:sum_us", int(elapsed_us))
                state.incr("_m:lat:count", 1)
        except Exception:
            logging.getLogger("mipyre.observability").exception(
                "metrics _after hook failed; request unaffected"
            )
        return resp

    app.before_request(_before)
    app.after_request(_after)

    def _metrics_handler(req):
        return Response(
            body=_render_prometheus(state),
            content_type="text/plain; version=0.0.4; charset=utf-8",
        )

    # runtime="main": /metrics reads cross-interpreter counters via app.state;
    # that works from any interp, but keeping the handler itself on the
    # main interp avoids dispatching a trivial scrape to a worker.
    app.get(path, runtime="main")(_metrics_handler)


class CorruptMetric(ValueError):
    """A metrics counter in ``app.state`` holds something that is not an integer."""


def _read_int(state, key: str) -> int:
    """The counter ``key``; one never incremented is 0. A value that is not an integer is
    ``CorruptMetric`` (the scrape fails, with the key and value in the log): reporting it
    as 0 would hand the monitoring a made-up number."""
    v = state.get(key)
    if v is None:
        return 0
    try:
        return int(v)
    except (ValueError, TypeError):
        raise CorruptMetric(f"metrics counter {key!r} holds {v!r}, not an integer") from None


def _render_prometheus(state) -> str:
    total = _read_int(state, "_m:req:total")

    parts: list[str] = []
    parts.append("# HELP mipyre_http_requests_total Total HTTP requests handled.")
    parts.append("# TYPE mipyre_http_requests_total counter")
    parts.append(f"mipyre_http_requests_total {total}")

    parts.append("# HELP mipyre_http_requests_by_class_total Requests by status class.")
    parts.append("# TYPE mipyre_http_requests_by_class_total counter")
    for cls in _STATUS_CLASSES:
        v = _read_int(state, f"_m:req:class:{cls}")
        parts.append(f'mipyre_http_requests_by_class_total{{class="{cls}"}} {v}')

    parts.append("# HELP mipyre_http_requests_by_method_total Requests by HTTP method.")
    parts.append("# TYPE mipyre_http_requests_by_method_total counter")
    for m in _TRACKED_METHODS:
        v = _read_int(state, f"_m:req:method:{m}")
        parts.append(f'mipyre_http_requests_by_method_total{{method="{m}"}} {v}')

    sum_us = _read_int(state, "_m:lat:sum_us")
    count = _read_int(state, "_m:lat:count")
    parts.append("# HELP mipyre_http_request_duration_seconds_sum Cumulative latency in seconds.")
    parts.append("# TYPE mipyre_http_request_duration_seconds_sum counter")
    parts.append(f"mipyre_http_request_duration_seconds_sum {sum_us / 1_000_000:.6f}")
    parts.append("# HELP mipyre_http_request_duration_seconds_count Samples in the latency sum.")
    parts.append("# TYPE mipyre_http_request_duration_seconds_count counter")
    parts.append(f"mipyre_http_request_duration_seconds_count {count}")

    parts.extend(_render_degradations(get_gil_metrics()))

    parts.append("")  # the text format ends the last line with a newline too
    return "\n".join(parts)


def _label_value(value: str) -> str:
    """A label value in the text format: backslash, double quote and newline escaped."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _render_degradations(m) -> list[str]:
    """The engine's degradation counters (``engine.Metrics``, read from the process-wide
    counters the engine keeps for the stats): what it gave up on and how often. Process-wide, so
    the same from any interpreter."""
    out: list[str] = []

    def family(name: str, kind: str, help_: str, samples: list[tuple[str, object]]) -> None:
        out.append(f"# HELP {name} {help_}")
        out.append(f"# TYPE {name} {kind}")
        for labels, value in samples:
            out.append(f"{name}{labels} {value}")

    family("mipyre_workers_serving", "gauge",
           "Sub-interpreter workers serving now.", [("", m.workers_serving)])
    family("mipyre_workers_died_total", "counter",
           "Workers that stopped serving on their own while the server ran.", [("", m.workers_died)])
    family("mipyre_gc_failures_total", "counter",
           "gc.collect() calls that raised or panicked; each retired its worker.",
           [("", m.gc_failures)])
    family("mipyre_workers_retired_total", "counter",
           "Workers retired because their gc.collect() failed: they take no more requests.",
           [("", m.workers_retired)])
    family("mipyre_precompressed_fallbacks_total", "counter",
           "Pre-compressed static files that could not be read; the original was served.",
           [("", m.precompressed_fallbacks)])
    family("mipyre_static_unwatched_mounts_total", "counter",
           "Static mounts served uncached because their file watch failed or was lost.",
           [("", m.static_unwatched_mounts)])
    family("mipyre_db_results_undeliverable_total", "counter",
           "Async database results that could not reach a closed asyncio loop.",
           [("", m.db_results_undeliverable)])
    family("mipyre_gil_congested_waits_total", "counter",
           "GIL acquisition waits past the congestion threshold (50 ms).",
           [("", m.gil_congested_waits)])
    family("mipyre_client_disconnects_total", "counter",
           "Peer-gone events (reset, aborted, closed mid-IO): the client's doing.",
           [("", m.client_disconnects)])
    family("mipyre_accept_failures_total", "counter",
           "accept() failures other than resource exhaustion or a peer going away.",
           [("", m.accept_failures)])
    family("mipyre_accept_exhausted_total", "counter",
           "accept() failures for lack of file descriptors, sockets or kernel buffers.",
           [("", m.accept_exhausted)])
    family("mipyre_tls_handshake_failures_total", "counter",
           "TLS handshakes that failed.", [("", m.tls_handshake_failures)])
    family("mipyre_bad_requests_total", "counter",
           "Connections closed because the client sent no HTTP/1 request (400) or a head past the limits "
           "(414 / 431).",
           [('{kind="malformed"}', m.malformed_requests),
            ('{kind="oversized"}', m.oversized_request_heads)])
    family("mipyre_request_head_timeouts_total", "counter",
           "Connections closed because the client sent no request head within the request-head timeout: "
           "no first request after connecting, or no next one on a kept-alive connection.",
           [('{kind="first_request"}', m.first_request_timeouts),
            ('{kind="next_request"}', m.request_head_timeouts)])
    family("mipyre_socket_option_failures_total", "counter",
           "Socket options that could not be set (the option is off on that socket).",
           [('{option="quickack"}', m.quickack_failures),
            ('{option="defer_accept"}', m.defer_accept_failures),
            ('{option="nodelay"}', m.nodelay_failures)])
    family("mipyre_cpu_degraded_total", "counter",
           "CPU placement degradations: threads not pinned, QoS not raised, topology unreadable, "
           "more TPC threads than CPUs.",
           [('{kind="pin_failed"}', m.cpu_pin_failures),
            ('{kind="qos_failed"}', m.cpu_qos_failures),
            ('{kind="affinity_unreadable"}', m.cpu_affinity_unreadable),
            ('{kind="topology_unreadable"}', m.cpu_topology_unreadable),
            ('{kind="threads_beyond_cpus"}', m.cpu_threads_beyond_cpus)])
    family("mipyre_drain_timeouts_total", "counter",
           "Drains of stopped accept loops that ran out of time.",
           [("", m.drain_timeouts)])
    family("mipyre_drain_closed_connections_total", "counter",
           "Connections still open when such a drain gave up; they were closed.",
           [("", m.drain_closed_connections)])
    family("mipyre_stop_watcher_failures_total", "counter",
           "Stop watchers (the thread that stops a TPC run on SIGINT or SIGTERM) that panicked or "
           "were still running when their run ended.",
           [("", m.stop_watcher_failures)])
    family("mipyre_blocking_calls_left_total", "counter",
           "Blocking calls (main-interpreter handlers) still running when their TPC thread's runtime "
           "was shut down, and left running.",
           [("", m.blocking_calls_left)])
    if m.cpu_sizing_fallback is not None:
        family("mipyre_cpu_sizing_fallback_info", "gauge",
               "1 when the server is sized from a fallback CPU count; the reason is the label.",
               [(f'{{reason="{_label_value(m.cpu_sizing_fallback)}"}}', 1)])
    return out
