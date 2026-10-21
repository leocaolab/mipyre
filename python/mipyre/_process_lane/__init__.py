"""Process lanes, server side (``runtime="process"``): the launcher's template and the
lanes it gets. The template's own code is `template.py`, the workers' `worker.py`, how
processes start on this machine the engine's `ProcessDriver` (src/process_lane/driver.rs),
what each library needs before import `adapters.py`.

Start order (docs/process-lanes-declared-design.md §4.1): ``mipyre run module:app``
starts the template first, before any app code is imported. The server then imports the
app; when it has process routes it sends the template its lanes (the shared memory and the
wake pipes, as fds) and what a worker must import.

Every process of a server imports `mipyre` submodules from disk when it first needs them: the
launcher and a forked template or worker through the packages' `__path__`, a spawned one (macOS)
from scratch. So that they all run the launcher's build even when the install changes under a
running server, the launcher first clones the `mipyre` it loaded into a run directory and points
its packages there, and spawned processes import from there too (docs/design/shm-channel.md §2.2).
"""

from __future__ import annotations

import atexit
import dataclasses
import json
import logging
import os
import resource
import shutil
import socket
import sys
import traceback
from collections.abc import Mapping, MutableMapping

import mipyre
from mipyre._clone import _user_cache_dir, claim_name, clone_tree, ensure_private_dir
from mipyre.engine import BUILD_ID, LaneSet, StartStrategy, _driver_start_template

from mipyre._process_lane.threads import TemplateNotSingleThreaded, require_single_threaded

__all__ = ["LaneSet", "LaneSetup", "LaneWorker", "ProcessLanesNeedLauncher", "SpentTemplate", "Template", "TemplateNotSingleThreaded", "start", "start_template"]


class ProcessLanesNeedLauncher(RuntimeError):
    """An app with process routes started other than through ``mipyre run``."""

    def __init__(self, command: str):
        super().__init__(
            "this app has runtime=\"process\" routes, and their workers start from a "
            "template that must not hold any of the app's code, so mipyre has to start "
            "before the app is imported. Start it with:\n\n    " + command + "\n"
        )


class SpentTemplate(RuntimeError):
    """The template was already dismissed or handed its lanes: it serves one start."""


class Template:
    """The launcher's template process, started before the app is imported."""

    def __init__(self, pid: int, sock: socket.socket):
        self.pid = pid
        self.sock: socket.socket | None = sock

    def dismiss(self) -> None:
        """The app has no process routes: the template has nothing to do."""
        if self.sock is not None:
            self.sock.close()
            self.sock = None
            try:
                os.waitpid(self.pid, 0)
            except ChildProcessError:
                pass


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
        f"(mipyre itself from {mipyre.__file__}), outside the launcher's copy in {run_dir!r}, "
        f"and loaded build {BUILD_ID!r} (the launcher runs build {launcher_build!r})",
        file=sys.stderr,
        flush=True,
    )
    os._exit(START_REFUSED)


def _claim_run_dir(src: str) -> str:
    """Clone `src`, the loaded `mipyre`, into a new run directory and return that directory.

    The run directory lives as long as this launcher holds its lock: it is removed at exit, and
    one a crashed launcher left behind is removed by the next start, which can take its lock.
    The user's cache dir, not the temp dir: the OS cleaner deletes files there that have not
    been read for days, and a run directory lives as long as the server.
    """
    root = os.path.join(_user_cache_dir(), "mipyre", "run")
    ensure_private_dir(root)
    _remove_stale_run_dirs(root)
    while (lock := claim_name(root, name := os.urandom(8).hex())) is None:
        pass
    run_dir = os.path.join(root, name)
    atexit.register(_remove_run_dir, root, name, lock)
    os.mkdir(run_dir, 0o700)
    clone_tree(src, os.path.join(run_dir, "mipyre"))
    return run_dir


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


# Set by `mipyre run` (`start_template`), read when the app prepares its first server.
LAUNCHER_TEMPLATE: Template | None = None


def start_template() -> Template:
    """Starts the template. `mipyre run` calls this before it imports the app."""
    global LAUNCHER_TEMPLATE
    # The launcher may fork (ForkDriver): it holds Python and mipyre only, no thread.
    require_single_threaded(
        "mipyre run, before it imports the app,",
        "Something imported before the app started them (a sitecustomize, a .pth file, "
        "a PYTHONSTARTUP script).",
    )
    _raise_open_file_limit()
    src = os.path.dirname(mipyre.__file__)
    run_dir = _claim_run_dir(src)
    pin_packages(sys.modules, src, os.path.join(run_dir, "mipyre"))
    env = pin_env(os.environ, run_dir) + [("MIPYRE_BUILD_ID", BUILD_ID)]
    server_end, template_end = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    fd = template_end.fileno()
    sys.stdout.flush()
    sys.stderr.flush()
    pid = _driver_start_template(sys.executable, "mipyre._process_lane.template", ["--sock", str(fd)], [fd], env)
    if pid == 0:  # a forked template (ForkDriver)
        try:
            server_end.close()
            refuse_unpinned_start(run_dir, BUILD_ID)
            from mipyre._process_lane import template

            template.main(template_end, run_dir)
        except BaseException:  # noqa: BLE001 — the template must never return into the launcher
            traceback.print_exc()
        finally:
            os._exit(70)
    template_end.close()
    LAUNCHER_TEMPLATE = Template(pid, server_end)
    return LAUNCHER_TEMPLATE


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


def start(template: Template, settings, routers: list[str]) -> LaneSet:
    """Makes the server's lanes and sends the template its setup."""
    from mipyre import cli

    if cli.LOADED_TARGET is None:  # `mipyre run` starts the template and imports the target
        raise ProcessLanesNeedLauncher("mipyre run <module>:<app>")
    depth = ring_depth(settings.queue_per_worker)
    lanes = LaneSet(
        settings.workers,
        slot_bytes=settings.slot_bytes,
        depth=depth,
        timeout_ms=settings.timeout_ms,
        queue_per_worker=settings.queue_per_worker,
        exit_on_failure=settings.host_failure == "exit",
    )
    sock, template.sock = template.sock, None
    if sock is None:
        raise SpentTemplate(f"process-lane template (pid {template.pid}) was already dismissed or handed its lanes")
    lanes._attach_template(template.pid, sock.detach())
    setup = LaneSetup(
        start=settings.start,
        workers=settings.workers,
        depth=depth,
        slot_bytes=settings.slot_bytes,
        worker_spin_us=settings.worker_spin_us,
        target=cli.LOADED_TARGET,
        routers=tuple(routers),
        sys_path=tuple(sys.path),
        cwd=os.getcwd(),
    )
    lanes._setup_template(setup.encode())
    return lanes


@dataclasses.dataclass(frozen=True)
class LaneSetup:
    """What the server tells its template about the lanes and the app a worker serves.

    The server writes it once (`start`); the template reads it off its socket and passes it,
    unchanged, to each worker it starts.
    """

    start: StartStrategy
    workers: int
    depth: int
    slot_bytes: int
    worker_spin_us: int
    target: str  # module:attr as `mipyre run` imported it
    routers: tuple[str, ...]  # the routers that have process routes: all a worker imports
    sys_path: tuple[str, ...]
    cwd: str

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
    """One worker a template starts: the lane it serves, under which template, and the lanes'
    fds as the template holds them (the shared memory, then each lane's wake pipe). A spawned
    worker (macOS) maps the lanes from those fds; a forked one inherits the template's mapping.
    """

    setup: LaneSetup
    lane: int
    template_pid: int
    fds: tuple[int, ...]

    def encode(self) -> str:
        """Return the worker as the text of the spawned worker's MIPYRE_LANE_WORKER."""
        return json.dumps(
            {"setup": self.setup._fields(), "lane": self.lane, "template_pid": self.template_pid, "fds": self.fds}
        )

    @classmethod
    def decode(cls, text: str) -> "LaneWorker":
        """Return the worker `encode` wrote."""
        fields = json.loads(text)
        return cls(
            setup=LaneSetup._from_fields(fields["setup"]),
            lane=fields["lane"],
            template_pid=fields["template_pid"],
            fds=tuple(fields["fds"]),
        )
