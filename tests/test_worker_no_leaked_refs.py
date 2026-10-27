"""No Python reference is dropped without its interpreter's thread state.

The PyO3 fork refuses to release a `Py<T>` on a thread with no thread state attached,
and says so ("a Py<T> was dropped on a thread that has no Python thread state"). Worker
init that held its bootstrap and script module references past `PyEval_SaveThread` would
drop two module references there in every worker at startup.
"""
from __future__ import annotations

import sys
import urllib.error
import urllib.request

import pytest

from tests._helpers import bound_port
from tests.test_isolate_shared_ext import _get, _sigint, _start

TIMED_OUT_REQUESTS = 3

_APP = '''
import asyncio
import time
from mipyre import Mipyre, ServerFlags

REQUEST_BUDGET_S = 0.5

app = Mipyre()
app.request_timeout = REQUEST_BUDGET_S

@app.get("/t")
def t(req):
    return {"ok": 1}

@app.get("/late-sync")
def late_sync(req):
    held = [req, object(), {"k": [1, 2, 3]}]
    end = time.monotonic() + 2 * REQUEST_BUDGET_S
    while time.monotonic() < end:
        held.append(held.pop())
    return held[2]

@app.get("/stuck-async")
async def stuck_async(req):
    held = [req, object(), {"k": [1, 2, 3]}]
    await asyncio.Event().wait()
    return held

if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port={port}))
'''


@pytest.mark.skipif(sys.platform == "win32", reason="SIGINT shutdown is POSIX")
@pytest.mark.parametrize("tpc", ["1", "0"], ids=["tpc", "pool"])
def test_no_reference_dropped_without_a_thread_state(tmp_path, tpc):
    proc, log = _start(_APP, tmp_path, f"refs_{tpc}", workers=4, env_extra={"MIPYRE_TPC": tpc})
    try:
        assert _get("/t", proc, log) == {"ok": 1}
        base = f"http://127.0.0.1:{bound_port(lambda: log.read_text(errors='replace'), proc)}"
        for path in ("/late-sync", "/stuck-async"):
            assert [_status(base + path) for _ in range(TIMED_OUT_REQUESTS)] == [504] * TIMED_OUT_REQUESTS
        assert _get("/t", proc, log) == {"ok": 1}
    finally:
        rc = _sigint(proc)
    text = log.read_text(errors="replace")
    assert rc == 0, text[-3000:]
    assert "was dropped on a thread that has no Python thread state" not in text, text[-3000:]


def _status(url: str) -> int:
    try:
        with urllib.request.urlopen(url, timeout=None) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
