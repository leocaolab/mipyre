"""How a failed run and an over-pinned start are reported.

- A run that fails while workers are left running keeps its error as the cause of
  `AbandonedWorkersError`.
- A start whose TPC threads outnumber the CPUs to pin them to is on /metrics.

The worker API panic is tested in `test_ffi_panic_safety.py` (fault_injection build).
"""

from __future__ import annotations

from mipyre import AbandonedWorkersError
from mipyre._server_run import run_outcome


def test_a_failed_run_is_the_cause_of_abandoned_workers_error():
    # A run that fails and leaves a worker running: the error is not lost behind
    # AbandonedWorkersError.
    failed = OSError("listener broke")
    raised = run_outcome([failed], ["w-1 (running GET /stuck)"])
    assert isinstance(raised, AbandonedWorkersError), raised
    assert raised.__cause__ is failed


def test_metrics_shows_the_starts_with_more_threads_than_cpus():
    from mipyre.testing import TestClient

    from tests.apps.obs_metrics_format import app

    with TestClient(app) as c:
        body = c.get("/metrics").text
        value = c.server.metrics().cpu_threads_beyond_cpus
    assert f'mipyre_cpu_degraded_total{{kind="threads_beyond_cpus"}} {value}' in body
