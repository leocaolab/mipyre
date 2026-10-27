"""This process's OS threads other than the calling one, by name; the single-thread rule a
fork needs (the fallback for libraries without an adapter)."""

from __future__ import annotations

import os
import sys
import threading


class ThreadedTemplateError(RuntimeError):
    """A process about to fork a template or worker runs other threads, which the fork would
    leave behind holding whatever locks they held."""


# The kernel's flag for a task inside its exit (`PF_EXITING`, include/linux/sched.h).
_PF_EXITING = 0x4


def is_exiting(stat: str) -> bool:
    """Whether the task whose ``/proc/<pid>/task/<tid>/stat`` line is ``stat`` is exiting: it
    will run no more user code, so it holds no lock a fork could copy held. A joined thread
    is such a task for a moment, until the kernel removes it from the list."""
    # The name, in parentheses, may itself hold spaces and parentheses: the fields follow the
    # last ")". `flags` is the 9th field, the 7th after the name.
    fields = stat[stat.rindex(")") + 1 :].split()
    return bool(int(fields[6]) & _PF_EXITING)


def _other_threads() -> list[str]:
    """Names of this process's OS threads other than the calling one ("" for an unnamed
    one), leaving out those already exiting. macOS: task_threads +
    thread_info(THREAD_EXTENDED_INFO); Linux: /proc/self/task."""
    if sys.platform == "linux":
        me = str(threading.get_native_id())
        names = []
        for tid in os.listdir("/proc/self/task"):
            if tid == me:
                continue
            try:
                with open(f"/proc/self/task/{tid}/stat") as f:
                    if is_exiting(f.read()):
                        continue
                with open(f"/proc/self/task/{tid}/comm") as f:
                    names.append(f.read().strip())
            except (FileNotFoundError, ProcessLookupError):
                continue  # the thread ended while we listed
        return names
    if sys.platform == "darwin":
        return _darwin_other_threads()
    raise NotImplementedError(f"process lanes: counting threads is not implemented on {sys.platform}")


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


def refusal(others: list[str], what: str, how_to_fix: str) -> str | None:
    """Return why a fork is refused when the process runs the threads named in ``others``
    besides the calling one ("" for an unnamed one); None when it runs none. `what` says which
    fork this is for, `how_to_fix` what the user can do."""
    if not others:
        return None
    named = ", ".join(n or "<unnamed>" for n in others)
    return (
        f"{what} must be single-threaded when it forks (a fork copies only the calling "
        f"thread, so a pool these threads belong to would be missing in every worker), but "
        f"it runs {len(others)} other thread(s): {named}. {how_to_fix}"
    )


def require_single_threaded(what: str, how_to_fix: str) -> None:
    """Raise ThreadedTemplateError with `refusal`'s reason when this process runs other
    threads, and NotImplementedError on a platform where they cannot be counted."""
    reason = refusal(_other_threads(), what, how_to_fix)
    if reason is not None:
        raise ThreadedTemplateError(reason)
