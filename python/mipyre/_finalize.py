"""Keeps the process from finalizing while a worker thread may still be inside its
sub-interpreter: ``Py_Finalize`` would free the interpreter under it.

One registry per process, not per app: every app's abandoned workers count at exit."""

from __future__ import annotations

import atexit
import os
import sys
import traceback
from typing import NoReturn

from mipyre.engine import _interpreters_running_python_at_exit, flush_logs

_ABANDONED_AT_EXIT: list[str] = []


def exit_without_finalization(why: str, *, error: BaseException | None) -> NoReturn:
    """End the process with status 1 and no finalization, after the log lines so far and
    ``error``'s whole chain (causes, notes, each of a group), when the exit has one."""
    flush_logs()
    if error is not None:
        traceback.print_exception(error, file=sys.stderr)
    print(f"mipyre: {why}; exiting without finalization", file=sys.stderr, flush=True)
    sys.stdout.flush()
    os._exit(1)


def refuse_finalization_while_owned(abandoned: list[str]) -> None:
    """Make the process exit without finalizing if, at exit, threads still run Python in their
    interpreters.

    ``Py_Finalize`` ends every sub-interpreter left and deletes its threads' states; a thread
    still serving one (an abandoned handler in ``time.sleep``) would then restore a freed
    thread state. The check runs at exit, before the engine's own ``atexit`` hook lets
    finalization begin: when no thread runs Python it waits until each has ended its
    interpreter, unless one enters Python meanwhile."""
    if not _ABANDONED_AT_EXIT:
        atexit.register(_finalize_only_if_no_interpreter_runs_python)
    _ABANDONED_AT_EXIT.extend(abandoned)


def refusal_at_exit(running_python: int, abandoned: list[str]) -> str | None:
    """Why the process must exit without finalizing when ``running_python`` threads were running
    Python in their interpreters at exit; None when none was and it may finalize."""
    if not running_python:
        return None
    return (
        f"{running_python} sub-interpreter(s) still in use at exit, running Python, after worker(s) "
        f"{', '.join(abandoned)} outlived the shutdown grace period"
    )


def _finalize_only_if_no_interpreter_runs_python() -> None:
    why = refusal_at_exit(_interpreters_running_python_at_exit(), _ABANDONED_AT_EXIT)
    if why is not None:
        exit_without_finalization(why, error=None)
