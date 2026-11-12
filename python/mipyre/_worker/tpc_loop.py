"""The event loop class of a TPC thread: the server's Rust loop with the I/O methods of asyncio's unix selector loop.

The Rust base owns the scheduling: ready callbacks, timers, Tasks, Futures and the fd poller, and it alone runs a turn.
This class keeps only what asyncio's I/O code keeps on the loop object (the transports, the default executor, the child
watcher, the async generators), and inherits the standard library's code that uses them: connections, servers,
`sock_*`, pipes, subprocesses, the executor. None of the standard library's scheduler runs: it is reached only from
`run_forever`, and the Rust base's `run_forever` refuses.
"""

from __future__ import annotations

import asyncio
import weakref
from asyncio import unix_events
from collections.abc import Callable
from typing import Any

# typeshed leaves out asyncio's private names, so they are read through this.
_UNIX_EVENTS: Any = unix_events

# What `signal.set_wakeup_fd` says off the main thread, which the standard loop turns into this RuntimeError.
_NO_SIGNALS_HERE = "set_wakeup_fd only works in main thread of the main interpreter"

# The seconds a callback may run before debug mode logs it as slow: the default the standard loop's `__init__` sets,
# which `_OwnMethods.__init__` replaces.
_SLOW_CALLBACK_SECONDS = 0.1


class _OwnMethods(unix_events._UnixSelectorEventLoop):
    """Define the methods the TPC thread's loop class has of its own, in place of the standard loop's.

    It is a base of that class, after the Rust loop, so a type checker checks these against the standard loop they
    stand in for; it is never made itself.
    """

    def __init__(self) -> None:
        self._transports: weakref.WeakValueDictionary[int, Any] = (
            weakref.WeakValueDictionary()
        )
        self._default_executor = None
        self._executor_shutdown_called = False
        self._asyncgens: weakref.WeakSet[Any] = weakref.WeakSet()
        self._asyncgens_shutdown_called = False
        self._unix_server_sockets: dict[Any, Any] = {}
        self._watcher = (
            _UNIX_EVENTS._PidfdChildWatcher()
            if _UNIX_EVENTS.can_use_pidfd()
            else _UNIX_EVENTS._ThreadedChildWatcher()
        )
        self.slow_callback_duration = _SLOW_CALLBACK_SECONDS

    def add_signal_handler[*Ts](
        self, sig: int, callback: Callable[[*Ts], object], *args: *Ts
    ) -> None:
        """Raise as the standard loop does off the main thread, which a TPC thread is."""
        raise RuntimeError(_NO_SIGNALS_HERE)

    def remove_signal_handler(self, sig: int) -> bool:
        """Return False: no handler can have been added."""
        _UNIX_EVENTS._UnixSelectorEventLoop._check_signal(self, sig)
        return False


def make_loop_class(native: type) -> type:
    """Return the event loop class of a TPC thread, whose scheduling is `native`'s, the Rust loop class.

    The standard library's ``__init__`` is not called: it would make a selector, a self-pipe and a scheduler of its
    own. The class's ``__init__`` sets only the attributes its I/O code reads that the Rust base does not give.
    """
    return type("TpcEventLoop", (native, _OwnMethods), {})


async def finalize(loop: Any, timeout: float) -> None:
    """End the loop as `asyncio.run` does: cancel its Tasks other than this one and wait for them, close its unfinished
    async generators, then wait at most `timeout` seconds for its default executor's threads.

    A Task that ends in an exception other than its cancellation goes to the loop's exception handler, as with
    `asyncio.run`. A Task that ignores its cancellation holds this up until the server's deadline closes the loop.
    """
    await _cancel_all_tasks(loop)
    await loop.shutdown_asyncgens()
    await loop.shutdown_default_executor(timeout)


async def _cancel_all_tasks(loop: Any) -> None:
    """Cancel every Task of `loop` but the calling one, wait for them, and report those that raised."""
    finalizing = asyncio.current_task()
    tasks = [task for task in asyncio.all_tasks(loop) if task is not finalizing]
    if not tasks:
        return
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    for task in tasks:
        if task.cancelled() or task.exception() is None:
            continue
        loop.call_exception_handler(
            {
                "message": "unhandled exception during the TPC thread's event loop shutdown",
                "exception": task.exception(),
                "task": task,
            }
        )
