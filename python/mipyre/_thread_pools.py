"""The thread pools libraries size from environment variables, and the one rule mipyre limits
them by.

Each such pool is sized to all cores by default, so N worker processes or sub-interpreters put
N x cores threads on the cores. Measured on 16 cores, 4 own-GIL workers each running
`np.linalg.inv`: 82 inversions in 25 s, against 128,873 with one BLAS thread. A library reads
its variables when it loads, so they are set before the app is imported.
"""

from __future__ import annotations

import dataclasses
import os
from collections.abc import Mapping


@dataclasses.dataclass(frozen=True)
class LibraryPool:
    """A library's thread pool and the environment variables that size it."""

    library: str
    variables: tuple[str, ...]


# OpenBLAS reads OPENBLAS_NUM_THREADS, then OMP_NUM_THREADS; MKL reads MKL_NUM_THREADS, then
# OMP_NUM_THREADS; Apple Accelerate (numpy on macOS) reads VECLIB_MAXIMUM_THREADS.
BLAS = LibraryPool("BLAS (numpy, scipy)", ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"))
POLARS = LibraryPool("polars", ("POLARS_MAX_THREADS",))
POOLS = (BLAS, POLARS)


def one_thread_each(environ: Mapping[str, str], pools: tuple[LibraryPool, ...] = POOLS) -> dict[str, str]:
    """Return the variables to set so that each pool the user did not size runs one thread.

    A pool is the user's once they set any one of its variables: setting the others would
    overrule them, as OPENBLAS_NUM_THREADS=1 overrules a user's OMP_NUM_THREADS in OpenBLAS.
    """
    return {
        variable: "1"
        for pool in pools
        if not any(variable in environ for variable in pool.variables)
        for variable in pool.variables
    }


def describe(environ: Mapping[str, str], limits: Mapping[str, str], pools: tuple[LibraryPool, ...] = POOLS) -> str:
    """Return one startup-log phrase per pool: limited to one thread, or which of the user's variables it keeps."""
    return "; ".join(
        f"{pool.library}: 1 thread per worker"
        if pool.variables[0] in limits
        else f"{pool.library}: left as set by {', '.join(v for v in pool.variables if v in environ)}"
        for pool in pools
    )


def this_process(pools: tuple[LibraryPool, ...] = POOLS) -> tuple[dict[str, str], str]:
    """The limits for the pools this process's environment leaves unsized (`one_thread_each`),
    and the startup-log phrase that says so (`describe`)."""
    limits = one_thread_each(os.environ, pools)
    return limits, describe(os.environ, limits, pools)
