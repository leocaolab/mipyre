"""What runs only on macOS: the Darwin fanout TPC topology (`MIPYRE_TPC_ACCEPT=fanout`), which
the server refuses elsewhere. Every test here skips on Linux, and no other test needs to.
"""

from __future__ import annotations

import sys

import pytest

from tests.test_gc_and_pool_split import PLAIN_APP, server  # noqa: F401 — `server` is a fixture

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="the fanout topology is Darwin-only")


def test_idle_gc_on_darwin_fanout_is_startup_error(server):  # noqa: F811 — `server` is the fixture imported above
    s = server(PLAIN_APP, {"MIPYRE_GC_MODE": "idle", "MIPYRE_TPC_ACCEPT": "fanout"})
    assert s.wait_exit() != 0
    out = s.output()
    assert "idle" in out and "fanout" in out, out
