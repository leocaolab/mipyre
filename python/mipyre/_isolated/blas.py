"""`runtime="isolated"`, the main interpreter's side: BLAS threads.

With isolated libraries every sub-interpreter calls into the same BLAS library, whose pool
is sized to all cores by default: the server process limits it to one thread per worker
(only for apps with isolated routes), by the rule process-lane workers follow too
(`mipyre._thread_pools`).
"""

from __future__ import annotations

import logging as _logging
import os
import sys
import threading

from mipyre import _thread_pools


def _limit_blas_threads() -> str:
    """Give each worker a single-threaded BLAS unless the user sized it; return what was done, for the banner.

    Every worker calls into the same BLAS library. The variables (`_thread_pools.one_thread_each`)
    cover BLAS loaded from now on; a BLAS already loaded (numpy imported at the top of the app)
    is shrunk at run time through threadpoolctl. The variables are set in this process, the
    only place the sub-interpreters' libraries read them from, so processes the app starts
    inherit them too.
    """
    limits = _thread_pools.one_thread_each(os.environ, (_thread_pools.BLAS,))
    if not limits:
        return f"left as set by {', '.join(v for v in _thread_pools.BLAS.variables if v in os.environ)}"
    os.environ.update(limits)
    try:
        from threadpoolctl import threadpool_limits
    except ImportError:
        if "numpy" in sys.modules or "scipy" in sys.modules:
            _logging.getLogger("mipyre.app").warning(
                "numpy/scipy was imported before app.run(), so its BLAS already "
                "started a thread pool sized to all cores, and without threadpoolctl "
                "mipyre can't shrink it; N workers will contend for the cores. "
                "Fix: `pip install threadpoolctl`, or start the app with "
                "OPENBLAS_NUM_THREADS=1."
            )
            return "1 thread per worker for BLAS loaded from now on (already-loaded BLAS unchanged: no threadpoolctl)"
        return "1 thread per worker"
    threadpool_limits(limits=1, user_api="blas")
    return "1 thread per worker"


# What `_limit_blas_threads` did, once per process: BLAS is process-wide, so the first
# server with isolated sub-interpreters limits it and later ones report the same outcome.
_BLAS_LOCK = threading.Lock()
_blas_outcome: str | None = None


def blas_threads_limited() -> str:
    global _blas_outcome
    with _BLAS_LOCK:
        if _blas_outcome is None:
            _blas_outcome = _limit_blas_threads()
        return _blas_outcome
