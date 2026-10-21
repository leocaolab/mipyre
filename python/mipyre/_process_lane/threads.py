"""This process's OS threads other than the calling one, by name; the single-thread rule a
fork needs (design §11.3: the fallback for libraries without an adapter)."""

from __future__ import annotations

import os
import sys


class TemplateNotSingleThreaded(RuntimeError):
    """A process about to fork runs other threads."""


def _other_threads() -> list[str]:
    """Names of this process's OS threads other than the calling one ("" for an unnamed
    one). macOS: task_threads + thread_info(THREAD_EXTENDED_INFO); Linux: /proc/self/task."""
    if sys.platform == "linux":
        me = str(threading_native_id())
        names = []
        for tid in os.listdir("/proc/self/task"):
            if tid == me:
                continue
            try:
                with open(f"/proc/self/task/{tid}/comm") as f:
                    names.append(f.read().strip())
            except FileNotFoundError:
                continue  # the thread ended while we listed
        return names
    if sys.platform == "darwin":
        return _darwin_other_threads()
    raise TemplateNotSingleThreaded(f"process lanes: counting threads is not implemented on {sys.platform}")


def threading_native_id() -> int:
    import threading

    return threading.get_native_id()


def _darwin_other_threads() -> list[str]:
    import ctypes

    libc = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
    task = ctypes.c_uint.in_dll(libc, "mach_task_self_").value
    threads = ctypes.POINTER(ctypes.c_uint)()
    count = ctypes.c_uint()
    kr = libc.task_threads(task, ctypes.byref(threads), ctypes.byref(count))
    if kr != 0:
        raise OSError(f"task_threads failed: kern_return {kr}")
    me = libc.mach_thread_self()
    THREAD_EXTENDED_INFO, COUNT = 5, 28  # struct thread_extended_info: 28 natural_t, name last
    names = []
    try:
        for i in range(count.value):
            port = threads[i]
            if port != me:
                buf = (ctypes.c_uint32 * COUNT)()
                n = ctypes.c_uint(COUNT)
                kr = libc.thread_info(port, THREAD_EXTENDED_INFO, buf, ctypes.byref(n))
                names.append(bytes(buf)[-64:].split(b"\0")[0].decode(errors="replace") if kr == 0 else f"<thread_info failed: {kr}>")
            libc.mach_port_deallocate(task, port)
    finally:
        libc.mach_port_deallocate(task, me)
        libc.vm_deallocate(task, ctypes.cast(threads, ctypes.c_void_p), count.value * 4)
    return names


def refusal(others: list[str], what: str, how_to_fix: str) -> TemplateNotSingleThreaded | None:
    """The error a fork is refused with when the process runs the threads named in ``others``
    besides the calling one ("" for an unnamed one); None when it runs none. `what` says which
    fork this is for, `how_to_fix` what the user can do."""
    if not others:
        return None
    named = ", ".join(n or "<unnamed>" for n in others)
    return TemplateNotSingleThreaded(
        f"{what} must be single-threaded when it forks (a fork copies only the calling "
        f"thread, so a pool these threads belong to would be missing in every worker), but "
        f"it runs {len(others)} other thread(s): {named}. {how_to_fix}"
    )


def require_single_threaded(what: str, how_to_fix: str) -> None:
    """Raise `refusal`'s error for this process's threads, if there is one."""
    error = refusal(_other_threads(), what, how_to_fix)
    if error is not None:
        raise error
