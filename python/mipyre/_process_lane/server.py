"""Process lanes, server side (``runtime="process"``): the launcher's template and the
lanes it gets. The template's own code is `template.py`, the workers' `worker.py`, how
processes start on this machine the engine's `ProcessDriver` (src/process_lane/driver.rs),
the thread pools each worker limits before import `mipyre._thread_pools`.

Start order: ``mipyre run module:app``
starts the template with the platform's driver (a fork on Linux) before any app code is
imported, while mipyre owns a process that runs no other thread, and hands it to the
server's run. Any other way in (``app.run()``, ``mipyre dev``, a test server) has imported
the app first, and the app's imports may have started threads (numpy's BLAS pool, a database
pool) that a fork would leave behind: its first server starts the template as a new
interpreter, on every platform, whatever threads the process runs. When the app has
process routes the server sends the template what a worker must import, and each run sends it
every lane's channel (its shared memory and its host's wake pipe, as fds), which the template
hands to the workers it starts for that lane without mapping it.

Every process of a server imports `mipyre` submodules from disk when it first needs them: the
launcher and a forked template or worker through the packages' `__path__`, a spawned one (macOS)
from scratch. So that they all run the launcher's build even when the install changes under a
running server, the launcher clones the `mipyre` it loaded into a run directory as it hands the
template its lanes, and it and the template point their packages there; the workers import from
there too. Only an app with process routes gets one: the run
directory lives in the user's cache directory, which a server without process routes never
writes to.
"""

from __future__ import annotations

import atexit
import dataclasses
import json
import logging
import os
import resource
import shutil
import signal
import socket
import sys
import traceback
from collections.abc import Callable, Mapping, MutableMapping
from types import ModuleType

from mipyre._clone import PrivateCopyError, claim_name, clone_tree, ensure_private_dir
from mipyre.engine import (
    BUILD_ID,
    _LaneSet,
    StartStrategy,
    _create_lanes,
    _driver_name,
    _driver_spawn_template,
    _driver_start_template,
    _start_log_writer,
    _stop_log_writer,
)

from mipyre._process_lane.threads import ThreadedTemplateError, require_single_threaded

# The `__init__.py` of the mipyre package this process imported, which is `mipyre.__file__`.
_PACKAGE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "__init__.py")

__all__ = [
    "LaneSetup", "LaneWorker", "SpentTemplate", "Template", "ThreadedTemplateError", "pin_to_run_dir",
    "start", "start_template_as_new_interpreter", "start_template_with_driver",
]


class SpentTemplate(RuntimeError):
    """The template was already dismissed or handed its lanes: it serves one start."""


class Template:
    """A server's template process and the server's end of its socket. It serves one start:
    `start` hands it its lanes, or `dismiss` ends it."""

    def __init__(self, pid: int, sock: socket.socket):
        self.pid = pid
        self.sock: socket.socket | None = sock
        # Whether this process waited for the template's exit: its pid is then no longer ours.
        self._exited = False

    def dismiss(self) -> None:
        """Tell the template it gets no lanes (the app has no process routes, or its lanes run
        already) and wait for it to exit; once it got them, or was dismissed, do nothing."""
        if self.sock is not None:
            self.sock.close()
            self.sock = None
            self._wait()

    def end(self) -> None:
        """End the template, with its workers, and wait for it to exit: the launcher's last
        step before it removes the run directory they import `mipyre` from. One that got its
        lanes is still serving them, or still starting, so it is stopped (SIGTERM: it kills its
        workers and exits); one that did not is dismissed."""
        if self.sock is not None:
            self.dismiss()
            return
        if self._exited:
            return
        # Not waited for yet, so the pid is still this template's (an exited one is a zombie).
        os.kill(self.pid, signal.SIGTERM)
        self._wait()

    def _wait(self) -> None:
        try:
            os.waitpid(self.pid, 0)
        except ChildProcessError:
            pass
        self._exited = True


# The exit status of a template or spawned worker that refuses to start because it cannot trust
# what it would run with (the pin check below): permanent, never retried.
START_REFUSED = 78


def pin_env(environ: Mapping[str, str], run_dir: str) -> list[tuple[str, str]]:
    """Return the environment a spawned process needs to import `mipyre` from `run_dir` first.

    An empty PYTHONPATH gives `run_dir` alone, not `run_dir:`, whose empty entry would put the
    current directory on `sys.path`.
    """
    rest = environ.get("PYTHONPATH")
    return [("PYTHONPATH", f"{run_dir}{os.pathsep}{rest}" if rest else run_dir)]


def unpin_env(environ: MutableMapping[str, str], run_dir: str) -> None:
    """Take `run_dir` off the front of `environ`'s PYTHONPATH, where `pin_env` put it.

    What is left is the launcher's PYTHONPATH; when nothing is left, PYTHONPATH is removed. An
    empty PYTHONPATH and none give the same `sys.path`, so the two are not told apart.
    """
    entries = environ.get("PYTHONPATH", "").split(os.pathsep)
    if entries[0] == run_dir:
        del entries[0]
    if rest := os.pathsep.join(entries):
        environ["PYTHONPATH"] = rest
    else:
        environ.pop("PYTHONPATH", None)


def is_pinned(file: str, run_dir: str) -> bool:
    """Return whether `file` lies inside `run_dir`, both resolved through their symlinks.

    An empty `run_dir` names no directory (resolving it would give the current one).
    """
    if not run_dir:
        return False
    file, run_dir = os.path.realpath(file), os.path.realpath(run_dir)
    return os.path.commonpath([file, run_dir]) == run_dir


def same_build(launcher: str | None, loaded: str) -> bool:
    """Return whether the launcher's build id (None when it passed none) is the loaded one."""
    return launcher == loaded


def package_paths(modules: Mapping[str, object]) -> list[list[str]]:
    """Return the `__path__` of each loaded `mipyre` package: where it imports its submodules from."""
    return [
        paths
        for name, module in list(modules.items())
        if (name == "mipyre" or name.startswith("mipyre.")) and (paths := getattr(module, "__path__", None)) is not None
    ]


def moved_path(path: str, src: str, clone: str) -> str:
    """Return where `path` lies in `clone`, the copy of `src`; a path outside `src` stays as it is."""
    rel = os.path.relpath(path, src)
    if rel == os.pardir or rel.startswith(os.pardir + os.sep):
        return path
    return os.path.normpath(os.path.join(clone, rel))


def pin_packages(modules: Mapping[str, object], src: str, clone: str) -> None:
    """Point each loaded `mipyre` package's `__path__` from `src`, the installed package, at `clone`.

    A submodule is imported from its package's `__path__`, so every later import of one, in this
    process and in every process forked from it, reads the clone. The list is changed in place:
    the package's spec holds the same list.
    """
    for paths in package_paths(modules):
        paths[:] = [moved_path(path, src, clone) for path in paths]


def refuse_unpinned_start(run_dir: str, launcher_build: str | None) -> None:
    """Exit with START_REFUSED unless this process imports `mipyre` from `run_dir`, in the launcher's build.

    Run at a template's or spawned worker's start, before it touches a lane: a process that
    imports another `mipyre` would read the lanes with another layout. `launcher_build` is None
    when the launcher passed none.
    """
    unpinned = [path for paths in package_paths(sys.modules) for path in paths if not is_pinned(path, run_dir)]
    if not unpinned and same_build(launcher_build, BUILD_ID):
        return
    print(
        f"mipyre: process {os.getpid()} refuses to start: it imports mipyre modules from {unpinned} "
        f"(mipyre itself from {_PACKAGE_FILE}), outside the launcher's copy in {run_dir!r}, "
        f"and loaded build {BUILD_ID!r} (the launcher runs build {launcher_build!r})",
        file=sys.stderr,
        flush=True,
    )
    os._exit(START_REFUSED)


def pin_to_run_dir(run_dir: str, launcher_build: str | None) -> None:
    """Point this process's `mipyre` packages at the copy in `run_dir`, then exit with
    START_REFUSED unless that leaves it importing only from there, in the launcher's build."""
    pin_packages(sys.modules, os.path.dirname(_PACKAGE_FILE), os.path.join(run_dir, "mipyre"))
    refuse_unpinned_start(run_dir, launcher_build)


def _claim_run_dir(src: str) -> str:
    """Clone `src`, the loaded `mipyre`, into a new run directory and return that directory.

    The run directory lives as long as this launcher holds its lock: it is removed at exit, and
    one a crashed launcher left behind is removed by the next start, which can take its lock.
    The user's cache dir, not the temp dir: the OS cleaner deletes files there that have not
    been read for days, and a run directory lives as long as the server.
    """
    from mipyre.engine import _user_cache_dir

    cache_dir = _user_cache_dir()
    if cache_dir is None:
        raise PrivateCopyError(
            "process routes need a directory of this user's for the server's copy of mipyre, in "
            "this user's cache directory, but this user has no home directory to find it under"
        )
    root = os.path.join(cache_dir, "mipyre", "run")
    try:
        ensure_private_dir(root)
    except PrivateCopyError as e:
        raise PrivateCopyError(
            f"{e}. Process routes need a directory of this user's for the server's copy of mipyre: "
            f"set {_cache_dir_variable()} to a directory this user can write (in a container, a "
            "writable volume such as an emptyDir)"
        ) from e
    _remove_stale_run_dirs(root)
    while (lock := claim_name(root, name := os.urandom(8).hex())) is None:
        pass
    run_dir = os.path.join(root, name)
    atexit.register(_remove_run_dir, root, name, lock)
    os.mkdir(run_dir, 0o700)
    clone_tree(src, os.path.join(run_dir, "mipyre"))
    return run_dir


def _cache_dir_variable() -> str:
    """The environment variable that moves the user's cache directory on this platform."""
    return "HOME" if sys.platform == "darwin" else "XDG_CACHE_HOME"


def _remove_stale_run_dirs(root: str) -> None:
    """Remove each run directory under `root` whose lock can be taken: its launcher is gone.

    One that cannot be removed is reported and left for a later start: it is another run's
    leftover, so it does not stop this one.
    """
    for entry in os.listdir(root):
        if entry.endswith(".lock") and (lock := claim_name(root, name := entry.removesuffix(".lock"))) is not None:
            try:
                _remove_run_dir(root, name, lock)
            except OSError as e:
                logging.getLogger("mipyre.run_dir").warning(
                    "could not remove the run directory %s, which an ended server left: %s; "
                    "it stays, and the next start tries again",
                    os.path.join(root, name),
                    e,
                    extra={"error_type": type(e).__name__},
                )


def _remove_run_dir(root: str, name: str, lock: int) -> None:
    """Remove the run directory `name`, then its lock file, then release the lock.

    The directory goes first, so that a directory never outlives its lock file: one without a
    lock file is never removed. The lock is released even when a removal fails.
    """
    try:
        run_dir = os.path.join(root, name)
        if os.path.isdir(run_dir):
            shutil.rmtree(run_dir)
        os.remove(os.path.join(root, name + ".lock"))
    finally:
        os.close(lock)


def start_template_with_driver(what: str, how_to_fix: str) -> Template:
    """Starts the template with this platform's driver, from this process as it is now: a fork
    on Linux, a new interpreter on macOS. Only ``mipyre run`` does, before it imports the app:
    a forked template copies only this thread, so this thread must be the process's only one,
    which only a process mipyre owns from its start can promise. The log's own writer thread
    is stopped for the fork and started again after it. `what` and `how_to_fix` name this
    start in the error another thread causes."""
    forks = _driver_name() == "fork"
    log_writer_stopped = forks and _stop_log_writer()
    try:
        if forks:
            require_single_threaded(what, how_to_fix)
        return _start_template(_driver_start_template)
    finally:
        # A forked template runs `template.main` and never returns here.
        if log_writer_stopped:
            _start_log_writer()


def start_template_as_new_interpreter() -> Template:
    """Starts the template as a new interpreter, on every platform: nothing of this process
    reaches it, so it takes whatever threads the app's imports (or a test runner) started."""
    return _start_template(_driver_spawn_template)


def _start_template(driver: Callable[[str, str, list[str], list[int], list[tuple[str, str]]], int]) -> Template:
    """Starts the template with `driver`: a fork of this process (0 here in the child) or a new
    interpreter running `mipyre._process_lane.template`. It has no run directory yet: `start`
    makes one and hands it over with the lanes, so an app without process routes needs none."""
    _raise_open_file_limit()
    env = [("_MIPYRE_BUILD_ID", BUILD_ID)]
    server_end, template_end = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    fd = template_end.fileno()
    sys.stdout.flush()
    sys.stderr.flush()
    pid = driver(sys.executable, "mipyre._process_lane.template", ["--sock", str(fd)], [fd], env)
    if pid == 0:  # a forked template
        try:
            # The launcher's own SIGTERM handling is not the template's: until `template.main`
            # sets its own, SIGTERM (the launcher ending it, `Template.end`) ends it.
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            server_end.close()
            from mipyre._process_lane import template

            template.main(template_end, BUILD_ID)
        except BaseException:  # noqa: BLE001 — the template must never return into the launcher
            traceback.print_exc()
        finally:
            os._exit(70)
    template_end.close()
    started = Template(pid, server_end)
    # The launcher ends its template and waits for it before it removes the run directory, which
    # `start` claims later and registers again after (see there). Whether or not the template got
    # its lanes: a failed start can end the launcher right after it handed them over.
    atexit.register(started.end)
    return started


def _raise_open_file_limit() -> None:
    """Raise this process's soft limit on open files to its hard limit, for the template to
    inherit: it holds two fds per lane, and launchd's soft limit of 256 runs out near 120 lanes."""
    _soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))


def ring_depth(queue_per_worker: int) -> int:
    """Return the largest power of two up to 8 that the queue holds.

    The ring must be a power of two because slots are sequence numbers mod depth, and those wrap
    at 2^32.
    """
    return 1 << (min(8, queue_per_worker).bit_length() - 1)


def start(
    template: Template,
    settings,
    routers: list[str],
    thread_limits: dict[str, str],
    target: str,
    *,
    log_level: str,
    log_format: str,
) -> _LaneSet:
    """Makes the server's lanes and hands `template` its setup: each worker sets
    `thread_limits` in its environment, logs as the server does (`log_level`, a `LogLevel`
    name, and `log_format`), then imports the app as `target` (`module:attr`)."""
    depth = ring_depth(settings.queue_per_worker)
    lanes = _create_lanes(
        settings.workers,
        slot_bytes=settings.slot_bytes,
        depth=depth,
        timeout_ms=settings.timeout_ms,
        queue_per_worker=settings.queue_per_worker,
        on_failure=settings.host_failure,
    )
    if template.sock is None:
        raise SpentTemplate(f"process-lane template (pid {template.pid}) was already dismissed or handed its lanes")
    run_dir = _claim_run_dir(os.path.dirname(_PACKAGE_FILE))
    # Registered again after the run directory's removal, so it runs before it at exit: the
    # template (or a worker it started) may still be importing `mipyre` from the directory.
    atexit.unregister(template.end)
    atexit.register(template.end)
    pin_packages(sys.modules, os.path.dirname(_PACKAGE_FILE), os.path.join(run_dir, "mipyre"))
    sock, template.sock = template.sock, None
    lanes._attach_template(template.pid, sock.detach())
    setup = LaneSetup(
        start=settings.start,
        workers=settings.workers,
        depth=depth,
        slot_bytes=settings.slot_bytes,
        worker_spin_us=settings.worker_spin_us,
        target=target,
        routers=tuple(routers),
        sys_path=tuple(sys.path),
        cwd=os.getcwd(),
        thread_limits=thread_limits,
        log_level=log_level,
        log_format=log_format,
        run_dir=run_dir,
    )
    lanes._setup_template(setup.encode())
    return lanes


@dataclasses.dataclass(frozen=True)
class LaneSetup:
    """What the server tells its template about the lanes and the app a worker serves: what
    stays the same for every run of the app (each run's command line comes in a frame of its
    own, before that run's workers start).

    The server writes it once (`start`); the template reads it off its socket and passes it,
    unchanged, to each worker it starts.
    """

    start: StartStrategy
    workers: int
    depth: int
    slot_bytes: int
    worker_spin_us: int
    target: str  # module:attr (or path.py:attr) the worker imports the app as
    routers: tuple[str, ...]  # the routers that have process routes: all a worker imports
    sys_path: tuple[str, ...]
    cwd: str
    thread_limits: dict[str, str]  # the variables each worker sets before the app is imported
    # The server's log level (a `LogLevel` name) and format: a worker's lines are the server's.
    log_level: str
    log_format: str
    run_dir: str  # holds the launcher's copy of mipyre, which the template and its workers import from

    def encode(self) -> bytes:
        """Return the setup as the JSON bytes of the template socket's setup frame."""
        return json.dumps(self._fields()).encode()

    @classmethod
    def decode(cls, data: bytes) -> "LaneSetup":
        """Return the setup `encode` wrote."""
        return cls._from_fields(json.loads(data))

    def _fields(self) -> dict:
        return {field.name: getattr(self, field.name) for field in dataclasses.fields(self)} | {"start": self.start.name}

    @classmethod
    def _from_fields(cls, fields: dict) -> "LaneSetup":
        fields["start"] = StartStrategy.parse(fields["start"])
        fields["routers"] = tuple(fields["routers"])
        fields["sys_path"] = tuple(fields["sys_path"])
        return cls(**fields)


@dataclasses.dataclass(frozen=True)
class LaneWorker:
    """One worker a template starts: the lane it serves, under which template, the start attempt
    it reports under, and its lane's two fds as the template holds them (the channel's shared
    memory, then the write end of its host's wake pipe), which the worker maps itself.
    """

    setup: LaneSetup
    lane: int
    template_pid: int
    attempt: int
    fds: tuple[int, int]
    command_line: str  # the run's, as `CommandLine.encode()` wrote it: the worker's sys.argv and flag values
    # The template's `mipyre.engine.BUILD_ID`: a spawned worker that loaded another build (the
    # files were replaced in place while the template ran) refuses to start.
    build: str

    def encode(self) -> str:
        """Return the worker as the text a spawned worker reads from its setup fd."""
        return json.dumps(
            {
                "setup": self.setup._fields(),
                "lane": self.lane,
                "template_pid": self.template_pid,
                "attempt": self.attempt,
                "fds": self.fds,
                "command_line": self.command_line,
                "build": self.build,
            }
        )

    @classmethod
    def decode(cls, text: str) -> "LaneWorker":
        """Return the worker `encode` wrote."""
        fields = json.loads(text)
        return cls(
            setup=LaneSetup._from_fields(fields["setup"]),
            lane=fields["lane"],
            template_pid=fields["template_pid"],
            attempt=fields["attempt"],
            fds=(fields["fds"][0], fields["fds"][1]),
            command_line=fields["command_line"],
            build=fields["build"],
        )


def lane_target(app: object, file: str, modules: list[ModuleType]) -> str | None:
    """The ``module:attr`` a worker imports to rebuild ``app``: of ``modules``, the one whose
    file is ``file``, and the module-level name ``app`` is bound to there; None when none is.
    ``__main__`` is named as the module ``python -m`` ran, so its package's imports resolve, or
    as its file (``path.py:attr``) when it was run as a script."""
    for module in modules:
        path = getattr(module, "__file__", None)
        if not path or os.path.abspath(path) != file:
            continue
        attr = next((name for name, value in vars(module).items() if value is app), None)
        if attr is None:
            continue
        if module.__name__ != "__main__":
            return f"{module.__name__}:{attr}"
        spec = getattr(module, "__spec__", None)
        return f"{spec.name}:{attr}" if spec is not None else f"{file}:{attr}"
    return None
