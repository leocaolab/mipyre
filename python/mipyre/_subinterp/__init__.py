"""`runtime="isolated"` (design §14), the main interpreter's side. The worker side, the
loading adapter, is `isolation.py` in this package (run by the engine in each worker).

With isolated libraries every sub-interpreter calls into the same BLAS library, whose pool
is sized to all cores by default: the server process limits it to one thread per worker
(only for apps with isolated routes; process-lane workers get theirs from
`mipyre._process_lane.adapters`).
"""

from __future__ import annotations

import logging as _logging
import os
import sys
import threading

# Env vars BLAS libraries read for their thread-pool size. If the user set any
# of them, their choice stands and mipyre changes nothing.
_BLAS_THREAD_VARS = (
    "OPENBLAS_NUM_THREADS",
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
)


def _limit_blas_threads() -> str:
    """Give each worker a single-threaded BLAS; return what was done, for the banner.

    Every worker calls into the same BLAS library, and by default it runs its
    own thread pool sized to all cores, so N workers put N × cores threads on
    the cores. Measured on 16 cores, 4 own-GIL workers each running
    `np.linalg.inv`: 82 inversions in 25 s, against 128,873 with one BLAS
    thread. The same holds for gunicorn/uvicorn multi-process deployments.

    BLAS reads the env vars when it loads, so setting them covers libraries
    loaded later; a library already loaded (numpy imported at the top of the
    app) is changed at run time through threadpoolctl."""
    user_set = [v for v in _BLAS_THREAD_VARS if v in os.environ]
    if user_set:
        return f"left as set by {', '.join(user_set)}"
    for v in _BLAS_THREAD_VARS:
        os.environ[v] = "1"
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
