"""b/149: what the b/128 reconciliation found missing or off.

- A run that fails while workers are left running keeps its error as the cause of
  `WorkersAbandoned`.
- A start whose TPC threads outnumber the CPUs to pin them to is on /metrics.

The worker API panic is tested in `test_ffi_panic_safety.py` (fault_injection build).
"""

from __future__ import annotations

import pytest
from mipyre.app import Mipyre, WorkersAbandoned


def test_a_failed_run_is_the_cause_of_workers_abandoned(monkeypatch):
    # A run that fails and leaves a worker running: the error is not lost behind
    # WorkersAbandoned.
    app = Mipyre()
    monkeypatch.setattr(app, "_prepare", lambda settings: None)

    class Engine:
        def _take_abandoned_workers(self):
            return ["w-1 (running GET /stuck)"]

    monkeypatch.setattr(app, "_engine", Engine())

    def start(settings):
        raise OSError("listener broke")

    with pytest.raises(WorkersAbandoned) as raised:
        app._serve(None, start)
    assert isinstance(raised.value.__cause__, OSError), raised.value.__cause__


def test_metrics_shows_the_starts_with_more_threads_than_cpus():
    from mipyre.engine import get_gil_metrics
    from mipyre.testing import TestClient

    from tests.apps.obs_metrics_format import app

    with TestClient(app, port=None) as c:
        body = c.get("/metrics").text
    value = get_gil_metrics().cpu_threads_beyond_cpus
    assert f'mipyre_cpu_degraded_total{{kind="threads_beyond_cpus"}} {value}' in body
