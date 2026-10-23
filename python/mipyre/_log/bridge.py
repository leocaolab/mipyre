"""Python ``logging`` → Rust ``tracing``: the one handler every interpreter installs.

The main interpreter imports this as ``mipyre._log.bridge``. A worker runs the same
source in its bootstrap namespace (the engine embeds it), because the bootstrap must
route logging before the ``mipyre`` package can be imported. So it imports only the
standard library.
"""

import logging
import sys

# Formats a record's exception (`logger.exception`) as its traceback. A `Handler` has no
# `formatException`; that is a `Formatter` method.
_TRACEBACK_FORMAT = logging.Formatter()


class RustLogHandler(logging.Handler):
    """Routes each record to ``mipyre.engine.emit_python_log``, tagged with where it was
    logged: a sub-interpreter worker's ``worker_id``, or a process-lane worker's ``lane``
    (neither on the main interpreter, which records neither field).

    Until ``connect(emit)`` gives it the engine's function, records go to stderr: a
    worker logs during its bootstrap, before the engine can be imported."""

    def __init__(self, worker_id, emit=None, lane=None):
        super().__init__()
        self._worker_id = worker_id
        self._lane = lane
        self._sink = emit

    def connect(self, emit):
        self._sink = emit

    def emit(self, record):
        try:
            msg = record.getMessage()
            # A local, not `record.exc_text`: the same record may go to other handlers.
            if record.exc_info:
                exc_text = record.exc_text or _TRACEBACK_FORMAT.formatException(record.exc_info)
                msg = f"{msg}\n{exc_text}"
            if self._sink is None:
                sys.stderr.write(f"{record.levelname} {record.name}: {msg}\n")
                return
            self._sink(
                record.levelno,
                record.name,
                msg,
                record.pathname or "",
                record.lineno or 0,
                self._worker_id,
                self._lane,
            )
        except Exception:  # noqa: BLE001 — handed to logging's handleError, below
            # Never crash the caller for a log line. `handleError` is logging's own
            # "emitting failed" hook: it honours `logging.raiseExceptions` and writes the
            # failing record to stderr.
            self.handleError(record)


def handler_level(engine_level):
    """Return the ``RustLogHandler``'s level for the engine's ``_python_log_level()``:
    records below what Rust logs are dropped before formatting or the FFI call. Before any
    ``init_logger`` (``None``, a raw engine app) every record goes to Rust, whose filter
    decides."""
    return logging.DEBUG if engine_level is None else engine_level


def root_level(current, engine_level):
    """Return the root logger's level once the bridge is installed: ``current`` (the
    user's), lowered so the records the handler takes reach it, never raised. A raised root
    would drop records the user's own handlers (Sentry, a file) were set up to receive."""
    return min(current, handler_level(engine_level))


def install_root_handler(emit, engine_level, lane=None):
    """Route this process's root logger to Rust through ``emit``, gated at
    ``handler_level(engine_level)``: the main interpreter's, or a process-lane worker's
    (``lane``). Adds the one ``RustLogHandler`` (not twice) and keeps the user's other
    handlers (Sentry, DataDog...)."""
    root = logging.getLogger()
    handler = next((h for h in root.handlers if isinstance(h, RustLogHandler)), None)
    if handler is None:
        handler = RustLogHandler(None, emit, lane)
        root.addHandler(handler)
    else:
        # A forked process-lane worker inherits its template's handler; its lines are its own.
        handler._lane = lane
    handler.setLevel(handler_level(engine_level))
    root.setLevel(root_level(root.level, engine_level))
