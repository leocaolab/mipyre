"""Bootstrap run in each sub-interpreter worker before the user's script.

A worker runs the same program as the main interpreter: the user's script
imports the real `mipyre` package and its engine. This bootstrap only
prepares the interpreter for it:

- routes Python `logging` through Rust `tracing` (`mipyre.engine.emit_python_log`),
  tagged with this worker's id;
- hands cycle collection to the Rust engine's schedule (`gc.disable()`).

C extensions load as CPython loads them in a sub-interpreter: one that declares
multi-interpreter support loads, any other fails with CPython's own ImportError, which
the worker's start error explains (`mipyre._worker.imports`), unless the app has
`runtime="isolated"` routes (the isolation adapter below).

It runs as the module `__mipyre_bootstrap__`, in its own namespace. Rust sets in it
before it runs: `WORKER_ID` (this worker's index), `ISOLATION_SOURCE`: the source of
`mipyre/_isolated/isolation.py` when the app has `runtime="isolated"` routes, else None,
and `ISOLATION_FILE`: the file name that source is compiled under.
"""

# -- Python logging bridge to Rust tracing -----------------------------------

import logging as _logging

# Set in this namespace before this file runs (see the docstring), read here by name: a missing one
# is a KeyError naming it, and each has its type. `RustLogHandler`, `handler_level` and `root_level` (see
# the note below) are declared for the type checkers only; `TYPE_CHECKING` is the bare-name form they honour,
# so a worker's start imports no `typing`.
TYPE_CHECKING = False
if TYPE_CHECKING:
    from mipyre._log.bridge import RustLogHandler, handler_level, root_level
    from mipyre._isolated.isolation import _IsolatingFinder
WORKER_ID: int = globals()["WORKER_ID"]
ISOLATION_SOURCE: str | None = globals()["ISOLATION_SOURCE"]
ISOLATION_FILE: str = globals()["ISOLATION_FILE"]

# `RustLogHandler`, `handler_level` and `root_level` come from `mipyre/_log/bridge.py`, which the
# engine runs in this namespace just before this file. The handler writes to stderr
# until it is connected to the engine, at the end of this file.

_root = _logging.getLogger()
_root.handlers.clear()
_log_handler = RustLogHandler(WORKER_ID)
_root.addHandler(_log_handler)
# The handler's level is synced with Rust's filter at the end of this file, once the
# engine is imported.

# -- GC: cycle collection on the engine's schedule ----------------------------
#
# CPython's allocation-threshold trigger fires hundreds of times a second at high
# request rates, each a pause on the request path (p99 spikes). The engine runs
# `gc.collect()` on its own schedule instead (`GcMode` in src/config.rs,
# `MIPYRE_GC_MODE`); reference counting still frees non-cyclic garbage at once.
try:
    import gc as _gc
    _gc.disable()
except Exception:
    _logging.getLogger("mipyre.bootstrap").error(
        "gc.disable() failed — CPython auto-GC remains active; "
        "expect P99 tail spikes at high RPS",
        exc_info=True,
    )


# -- runtime="isolated": the sub-interpreter loading adapter ------------------
# Only when the app has runtime="isolated" routes (a compatibility option, design §14).
# Installed before the `mipyre` package is imported below, so the package's own imports
# go through it as the user's do.
if ISOLATION_SOURCE is not None:
    exec(compile(ISOLATION_SOURCE, ISOLATION_FILE, "exec"), globals())  # noqa: S102 — the isolation adapter runs in this namespace by design
    import sys as _sys

    _sys.meta_path.insert(0, _IsolatingFinder())


# -- The engine for the logging bridge -----------------------------------------
# Importing the engine imports the `mipyre` package.
from mipyre.engine import emit_python_log as _emit_python_log  # noqa: E402 — after the isolation adapter is installed
from mipyre.engine import _python_log_level  # noqa: E402 — after the isolation adapter is installed

_log_handler.connect(_emit_python_log)
# Python's level gate is the level the main interpreter's `init_logger` applied.
_log_handler.setLevel(handler_level(_python_log_level()))
_root.setLevel(root_level(_root.level, _python_log_level()))
