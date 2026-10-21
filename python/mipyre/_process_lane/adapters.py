"""Library adapters (design §11.3): what each library needs set, through its own public
configuration, before it is imported in a process-lane worker or template.

An adapter only sets public switches, with `setdefault` (a value the user set stands), and
says what it set. It never lets a thread through by name and never works around a check:
a library without an adapter is covered, under `start="fork"`, by the single-thread check.
"""

from __future__ import annotations

from typing import MutableMapping, Protocol


class LibraryAdapter(Protocol):
    library: str

    def prepare(self, env: MutableMapping[str, str]) -> list[str]:
        """Set the library's public switches in `env`; return what was set (`NAME=value`)."""
        ...


def _setdefault(env: MutableMapping[str, str], values: dict[str, str]) -> list[str]:
    set_now = []
    for name, value in values.items():
        if name not in env:
            env[name] = value
            set_now.append(f"{name}={value}")
    return set_now


class OpenBlasAdapter:
    """numpy / scipy / pandas' BLAS: one thread per worker (N workers otherwise run
    N x cores BLAS threads). OpenBLAS starts its pool at import and reads these then; Apple
    Accelerate (numpy on macOS) reads VECLIB_MAXIMUM_THREADS; MKL its own."""

    library = "BLAS (numpy, scipy)"

    def prepare(self, env: MutableMapping[str, str]) -> list[str]:
        return _setdefault(env, {
            "OPENBLAS_NUM_THREADS": "1",
            "OMP_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
            "VECLIB_MAXIMUM_THREADS": "1",
        })


class PolarsAdapter:
    """polars: its query thread pool sized to one per worker. (polars starts its
    `polars-ooc-cleaner` thread at import with no switch to stop it — polars-python
    on_startup.rs calls `init_ooc_cleaner()` unconditionally — so a polars app can't use
    `start="fork"`; see design §12.)"""

    library = "polars"

    def prepare(self, env: MutableMapping[str, str]) -> list[str]:
        return _setdefault(env, {"POLARS_MAX_THREADS": "1"})


ADAPTERS: tuple[LibraryAdapter, ...] = (OpenBlasAdapter(), PolarsAdapter())


def prepare_all(env: MutableMapping[str, str]) -> list[str]:
    """Runs every adapter; what they set, for the startup log."""
    done = []
    for adapter in ADAPTERS:
        done += adapter.prepare(env)
    return done
