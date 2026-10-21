"""The engine leaves mimalloc's purge delay alone; the environment sets it.

mipyre sets no mimalloc option (src/alloc.rs): `purge_delay` is mimalloc's default unless
`MIMALLOC_PURGE_DELAY` (or the older `MIMALLOC_RESET_DELAY`) is in the environment, which
a long-running service sets to 0 to give closed connections' memory back at once. Each
case runs in a fresh process: the option is process-wide and read from the environment
once.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

READ = "import mipyre.engine as e; print(e._mimalloc_purge_delay())"

# `purge_delay`'s default in the mimalloc v3 the engine links (`src/options.c`).
MIMALLOC_DEFAULT_PURGE_DELAY_MS = 1000


def _purge_delay(**env_vars: str) -> int:
    env = {k: v for k, v in os.environ.items() if k not in ("MIMALLOC_PURGE_DELAY", "MIMALLOC_RESET_DELAY")}
    env.update(env_vars)
    done = subprocess.run([sys.executable, "-c", READ], env=env, capture_output=True, text=True, timeout=60)
    assert done.returncode == 0, done.stderr
    return int(done.stdout.split()[-1])


def test_purge_delay_is_mimallocs_default():
    assert _purge_delay() == MIMALLOC_DEFAULT_PURGE_DELAY_MS


@pytest.mark.parametrize("var", ["MIMALLOC_PURGE_DELAY", "MIMALLOC_RESET_DELAY"])
@pytest.mark.parametrize("value", ["0", "250"])
def test_environment_sets_purge_delay(var, value):
    assert _purge_delay(**{var: value}) == int(value)
