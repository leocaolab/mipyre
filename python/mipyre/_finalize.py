"""Keeps the process from finalizing while a worker thread may still be inside its
sub-interpreter: ``Py_Finalize`` would free the interpreter under it.

One registry per process, not per app: every app's abandoned workers count at exit."""

from __future__ import annotations

import atexit
import os
import sys
from typing import NoReturn

from mipyre.engine import _owned_interpreters_at_exit, flush_logs

_ABANDONED_AT_EXIT: list[str] = []


def exit_without_finalization(why: str) -> NoReturn:
    """End the process with status 1 and no finalization, after the log lines so far."""
    flush_logs()
    print(f"mipyre: {why}; exiting without finalization", file=sys.stderr, flush=True)
    sys.stdout.flush()
    os._exit(1)


def refuse_finalization_while_owned(abandoned: list[str]) -> None:
    """Make the process exit without finalizing if, at exit, threads still own interpreters.

    ``Py_Finalize`` ends every sub-interpreter left and deletes its threads' states; a thread
    still serving one (an abandoned handler in ``time.sleep``) would then restore a freed
    thread state. The check runs at exit, before the engine's own ``atexit`` hook lets
    finalization begin, and gives threads already on their way out the time ending takes."""
    if not _ABANDONED_AT_EXIT:
        atexit.register(_finalize_only_if_no_interpreter_is_owned)
    _ABANDONED_AT_EXIT.extend(abandoned)


def _finalize_only_if_no_interpreter_is_owned() -> None:
    owned = _owned_interpreters_at_exit()
    if owned:
        exit_without_finalization(
            f"{owned} sub-interpreter(s) still in use at exit, after worker(s) "
            f"{', '.join(_ABANDONED_AT_EXIT)} outlived the shutdown grace period"
        )
