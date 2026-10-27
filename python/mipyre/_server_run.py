"""One server run: from its flags settled to the server stopped and its outcome handed to the
caller. ``app.run()``, ``mipyre run`` / ``mipyre dev`` and ``TestClient`` all serve through
:class:`ServerRun`.

A run ends in this order: the server stops, and the engine keeps the process from finalizing
under any worker that outlived the shutdown grace; those workers are taken; every shutdown hook
runs; then the run raises its outcome (:func:`run_outcome`), which is the one report of how it
failed."""

from __future__ import annotations

import contextlib
import dataclasses
import enum
import errno
import sys
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Iterator, Sequence

from mipyre import flags
from mipyre._log import bridge
from mipyre._log.settings import _LogSettings
from mipyre._process_lane.settings import ProcessLanes
from mipyre._routes import Runtime, RuntimeDeclarationError
from mipyre.engine import (
    AbandonedWorker,
    Deployment,
    RunMetrics,
    ServerFlags,
    StartStrategy,
    _exit_without_finalization,
    _flags,
    _physical_cores,
    _python_log_level,
    emit_python_log,
    flush_logs,
    init_logger,
)

if TYPE_CHECKING:
    import socket

    from mipyre._process_lane.server import Template
    from mipyre.app import Mipyre
    from mipyre.engine import CommandLine, Flag, LaneRecipe, Server, _LaneSet


class Transport(enum.Enum):
    """How a server takes connections: TCP sockets (what ``run()`` serves), or in-memory
    connections made in this process (``mipyre.testing.TestClient``'s default), which reach
    the same accept loops and request pipeline with no socket."""

    TCP = "tcp"
    MEMORY = "memory"


class AbandonedWorkersError(Exception):
    """Worker threads outlived the shutdown grace period (a handler that ignores the stop)
    and may still be in their interpreters, which finalizing the process would free under
    them. ``Mipyre.run()`` prints the whole chain and exits at once without finalizing
    (``os._exit(1)``); a ``TestClient`` raises this instead, and the process exits the same
    way at exit if a thread still owns its interpreter then. Its ``__cause__`` is how the run
    ended otherwise, if it failed too."""

    def __init__(self, workers: Sequence[AbandonedWorker]):
        self.workers: tuple[AbandonedWorker, ...] = tuple(workers)
        super().__init__(_not_stopped(self.workers))


def _not_stopped(workers: Sequence[AbandonedWorker]) -> str:
    return "worker(s) " + ", ".join(str(w) for w in workers) + " did not stop within the shutdown grace period"


def run_outcome(failures: Sequence[BaseException], abandoned: Sequence[AbandonedWorker]) -> BaseException | None:
    """The one exception a run raises: none when nothing failed; the failure itself when one
    did; a ``BaseExceptionGroup`` of them when several did (an ``ExceptionGroup`` when all
    are ``Exception``s). Workers left running make it an :class:`AbandonedWorkersError`
    whose ``__cause__`` is that outcome."""
    if not failures:
        ended = None
    elif len(failures) == 1:
        ended = failures[0]
    else:
        ended = BaseExceptionGroup("server run failed", list(failures))
    if not abandoned:
        return ended
    error = AbandonedWorkersError(abandoned)
    error.__cause__ = ended
    return error


@dataclass(frozen=True)
class Launch:
    """What the launcher sets beyond the flags: whether it logs by default (``mipyre dev``), and
    the process-lane template ``mipyre run`` started before it imported the app. Without one,
    the app's first run that needs a template starts it as a new interpreter."""

    log: bool = False
    template: Template | None = None


class ServerRun:
    """One server of ``app``: prepared, started, served until stopped, and finished in the
    order the module docstring gives. ``serve()`` raises the run's outcome.

    Its flags are ``deployment`` (what the entry point read) laid over ``config`` (what the code
    gives), in the engine's ``start()``. ``script`` names the file sub-interpreter workers
    execute to rebuild the app when the app has none set; ``None`` keeps ``__main__``'s (see
    :meth:`in_test_process`). ``run_kind`` says what a flag nothing parsed is in this run's
    workers (see :meth:`in_test_process`)."""

    def __init__(
        self,
        app: Mipyre,
        deployment: Deployment,
        config: ServerFlags,
        launch: Launch = Launch(),
        transport: Transport = Transport.TCP,
        *,
        script: str | None = None,
        run_kind: flags._RunKind = flags._RunKind.SCRIPT,
    ) -> None:
        self._app = app
        self._runs = runs_of(app)
        self._deployment = deployment
        self._config = config
        self._launch = launch
        self._transport = transport
        self._script = script
        self._run_kind = run_kind
        self._prepared = False
        # The engine server once started, and whether `stop()` came first: a stop before the
        # server exists stops it as soon as it does.
        self._lock = threading.Lock()
        self._server: Server | None = None
        self._stopping = False
        # The flag values this run's workers get, held from `prepare()` to the end of the run
        # (no `flags.override` can change them meanwhile).
        self._held = contextlib.ExitStack()
        self._command_line: CommandLine | None = None

    @classmethod
    def in_test_process(cls, app: Mipyre, config: ServerFlags, *, transport: Transport) -> ServerRun:
        """A run of ``app`` in a test process (``TestClient``): only the flags ``config`` gives,
        no deployment (the test process's environment and working directory are not the
        app's, so a test's result does not depend on where it runs); workers execute the
        module that defined the app, because this process's ``__main__`` is the test runner; a
        flag nothing parsed is its declared default (the test runner's ``sys.argv`` is not the
        app's)."""
        return cls(app, Deployment(), config, transport=transport, script=app._defined_in, run_kind=flags._RunKind.TEST)

    def prepare(self) -> None:
        """Do the app's first-run work on the calling thread, before any serving thread
        exists: the process-lane template, when the app needs one and the launcher started
        none, is started here, as a new interpreter. Idempotent."""
        if self._prepared:
            return
        app = self._app
        if self._script is not None:
            app._engine.set_script_path(self._script)
        command_line = self._held.enter_context(flags._lease(self._run_kind))
        try:
            self._runs.prepare(self._deployment, self._config, self._launch, command_line)
        except BaseException:
            self._held.close()
            raise
        self._command_line = command_line
        self._prepared = True

    def serve(self, on_serving: Callable[[], None] | None = None) -> None:
        """Prepare if not yet, run the startup hooks, start the server and serve until it
        stops, then finish the run and raise its outcome (see :func:`run_outcome`).
        ``on_serving`` is called on this thread once every worker is up."""
        outcome = self.serve_to_end(on_serving)
        if outcome is not None:
            raise outcome

    def serve_to_end(self, on_serving: Callable[[], None] | None = None) -> BaseException | None:
        """``serve()``, returning the run's outcome instead of raising it: for a thread that
        serves on another's behalf (``TestClient``). The run's flag values are let go before it
        returns."""
        try:
            return self._serve_to_end(on_serving)
        finally:
            self._held.close()

    def _serve_to_end(self, on_serving: Callable[[], None] | None) -> BaseException | None:
        self.prepare()
        app = self._app
        failures: list[BaseException] = []
        ended = _failure_of(lambda: self._start_and_serve(on_serving))
        # SIGINT reaches both the engine (which drains and returns) and this thread
        # (KeyboardInterrupt here or a few bytecodes later): a graceful stop either way.
        graceful = ended is None or isinstance(ended, KeyboardInterrupt)
        if ended is not None and not graceful:
            self._note_function_defined_app(ended)
            failures.append(ended)

        abandoned = tuple(app._engine._take_abandoned_workers())

        with _sigint_ignored() if graceful else contextlib.nullcontext():
            for hook in app._shutdown_hooks:
                failed = _failure_of(hook)
                if failed is None:
                    continue
                failures.append(failed)
                # The rest still run after an error; SystemExit or a second ctrl-C ends them.
                if not isinstance(failed, Exception):
                    break
        return run_outcome(failures, abandoned)

    def stop(self) -> None:
        """Stop this run's server as SIGINT does, before or while it serves; from any thread,
        any number of times. Other servers of the same app keep serving."""
        with self._lock:
            self._stopping = True
            server = self._server
        if server is not None:
            server.shutdown()

    def metrics(self) -> RunMetrics:
        """This run's counts so far (``Server.metrics()``); the run must have started."""
        server = self._server
        if server is None:
            raise RuntimeError("the server has not started")
        return server.metrics()

    @property
    def port(self) -> int:
        """The port the server listens on (the kernel's pick for ``port=0``, once started).
        A memory-transport run has none: ``AttributeError``."""
        if self._transport is Transport.MEMORY:
            raise AttributeError(
                "an in-memory server listens on no port; "
                "serve the app with TestServer(app) for a real TCP listener"
            )
        server = self._server
        if server is None:
            raise AttributeError("the server has not bound its port yet")
        port = server.port
        assert port is not None, "a TCP server listens on a port"
        return port

    def connect(self) -> socket.socket:
        """A new in-memory connection to this run's server (memory transport): the client's
        end of a socket pair, blocking."""
        server = self._server
        if server is None:
            raise ConnectionRefusedError("the server has not started")
        return server.connect()

    def _start_and_serve(self, on_serving: Callable[[], None] | None) -> None:
        """Run the startup hooks, then start the engine server (an ``OSError`` if its port is
        taken) and serve until it stops. A ``Transport.MEMORY`` server binds nothing: it takes
        only the connections ``connect()`` makes, and the settings' address and TLS are not
        used."""
        app = self._app
        with self._runs.running_while(self):
            for hook in app._startup_hooks:
                hook()
            server = app._engine.start(
                deployment=self._deployment,
                config=self._config,
                transport=self._transport.value,
                command_line=self._command_line,
            )
            with self._lock:
                self._server = server
                if self._stopping:
                    server.shutdown()
            server.serve(on_serving)

    def _note_function_defined_app(self, error: BaseException) -> None:
        if self._app._defined_in is None and any(r.runtime.on_subinterpreters for r in self._app.routes):
            error.add_note(
                "This app was created inside a function, so sub-interpreter workers "
                "cannot rebuild it by executing its module. Define it at module level, "
                "or create it with Mipyre(runtime='main') to serve every handler on the "
                "main interpreter."
            )


def _failure_of(call: Callable[[], object]) -> BaseException | None:
    """What ``call`` raised, as a value, or ``None``: a run collects every failure of its
    serving and its hooks and raises them together at its end."""
    try:
        call()
    except BaseException as e:  # noqa: BLE001 — returned, and raised with the run's outcome
        return e
    return None


class AppRuns:
    """What the runs of one app in this process share: the app's preparation (its
    registrations sealed and logging set up, once), its process lanes once started (one
    template starts the workers of every run), and the runs running now. The app's first run
    makes it (:func:`runs_of`); it lasts as long as the app."""

    def __init__(self, app: Mipyre) -> None:
        self._app = app
        # Two servers of one app may start at once (nested TestClients), and each must find
        # the app prepared exactly once.
        self._prepare_lock = threading.Lock()
        self._prepared = False
        self._lanes: _StartedLanes | None = None
        self._running_lock = threading.Lock()
        self._running: set[ServerRun] = set()

    @property
    def process_lanes(self) -> _LaneSet | None:
        return None if self._lanes is None else self._lanes.lanes

    def stop(self) -> None:
        """Stop every run of the app that is running, as SIGTERM does (see ``Mipyre.stop``)."""
        with self._running_lock:
            running = list(self._running)
        for run in running:
            run.stop()

    @contextlib.contextmanager
    def running_while(self, run: ServerRun) -> Iterator[None]:
        with self._running_lock:
            self._running.add(run)
        try:
            yield
        finally:
            with self._running_lock:
                self._running.discard(run)

    def prepare(self, deployment: Deployment, config: ServerFlags, launch: Launch, command_line: CommandLine) -> None:
        """Get the app ready for a server. Once per app, by its first server: seal the script's
        registrations, start the process lanes and set up logging. Per server: the lanes'
        template is told this run's ``command_line``. Everything registered here exists only on
        main."""
        app = self._app
        with self._prepare_lock:
            if not self._prepared:
                app._engine._seal_registrations()
                configured = _LogSettings.resolve(app.debug, app._log_config, dev=launch.log)
                log = configured.with_access_log(app._access_log, deployment.log(None, launch.log))
                self._start_lanes(deployment, config, launch.template, log)

                app._engine.enable_request_logging(log.access_log)
                init_logger(log.level, log.access_log, log.format)
                # Main and every worker gate Python logging on the level just applied.
                bridge.install_root_handler(emit_python_log, _python_log_level())
                self._prepared = True

            if self._lanes is not None:
                self._lanes.tell_run(command_line)

    def _start_lanes(
        self, deployment: Deployment, config: ServerFlags, template: Template | None, log: _LogSettings
    ) -> None:
        """Set up the process lanes for ``runtime="process"`` routes: the template gets the
        lanes, what to import and how the server logs (``log``), for its workers' lines.
        ``template`` is the one ``mipyre run`` started before it imported the app; without one,
        it is started now as a new interpreter, which the threads the app's imports started
        (numpy's, a database pool's) cannot reach."""
        from mipyre._process_lane import server as lane_server

        app = self._app
        if not app._engine._has_process_routes() or self._lanes is not None:
            if template is not None:
                template.dismiss()
            return
        if template is None:
            template = lane_server.start_template_as_new_interpreter()
        from mipyre import _thread_pools

        lane_settings = lane_recipe(app, deployment, config)
        thread_limits, thread_pools_said = _thread_pools.this_process()
        lanes = lane_server.start(
            template,
            lane_settings,
            app._routers_by_runtime[Runtime.PROCESS],
            thread_limits,
            _lane_target(app),
            log_level=str(log.level),
            log_format=log.format,
        )
        app._engine._set_process_lanes(lanes)
        self._lanes = _StartedLanes(lanes, forks=lane_settings.start is StartStrategy.Fork)
        print(
            f"  Process lanes: {lane_settings.workers} workers (template pid {lanes.template_pid}, "
            f"timeout {lane_settings.timeout_ms} ms, queue {lane_settings.queue_per_worker}/worker; "
            f"{thread_pools_said})",
            flush=True,
        )


class _StartedLanes:
    """An app's process lanes once started. A start="fork" template imports the app once, at
    the first run it is told, and the module-level code of every worker it forks ran with that
    run's command line."""

    def __init__(self, lanes: _LaneSet, *, forks: bool) -> None:
        self.lanes = lanes
        self._forks = forks
        self._imported_with: CommandLine | None = None

    def tell_run(self, command_line: CommandLine) -> None:
        """Send the template this run's ``command_line``. A fork template's later run with other
        flag values would fork workers that saw the old ones, so that run fails here instead
        (:class:`~mipyre.flags.FlagMismatchError`)."""
        if self._forks:
            if self._imported_with is None:
                self._imported_with = command_line
            else:
                flags._require_same_values_as_fork_template(self._imported_with, command_line)
        self.lanes._template_run(command_line)


_runs_made = threading.Lock()


def runs_of(app: Mipyre) -> AppRuns:
    """Return ``app``'s :class:`AppRuns`, made by the first call."""
    with _runs_made:
        if app._runs is None:
            app._runs = AppRuns(app)
        return app._runs


def lane_recipe(app: Mipyre, deployment: Deployment, config: ServerFlags) -> LaneRecipe:
    """The process lanes' settings: the deployment (the command line, the environment,
    ``[tool.mipyre.process]``) laid over ``app``'s own ``ProcessLanes``."""
    declared = app._process_declared or ProcessLanes()
    return deployment.lanes(config._with_lanes(**dataclasses.asdict(declared)), _physical_cores())


def _lane_target(app: Mipyre) -> str:
    """The target a process-lane worker imports to rebuild ``app`` (see `lane_target`); an app
    bound to no module-level name of its file is a declaration error."""
    from mipyre._process_lane.server import lane_target

    file = app._engine.app_file()
    target = lane_target(app, file, list(sys.modules.values()))
    if target is None:
        raise RuntimeDeclarationError(
            f"this app has runtime=\"process\" routes, whose workers import the app by a "
            f"module-level name, but it is bound to no module-level name of {file}: define it "
            f"at the top level of that module (as sub-interpreter workers need too)"
        )
    return target


def run(app: Mipyre, deployment: Deployment, config: ServerFlags, launch: Launch, *, reload: bool) -> None:
    """``app.run()`` / ``mipyre run`` / ``mipyre dev``: serve ``app`` in this process until it
    stops, with the deployment its entry point read laid over the code's ``config``, and what
    the launcher adds (``launch``); ``reload`` restarts it when its files change. Workers left
    running make the process print the whole exception chain and exit with status 1 without
    finalizing; any other failure is raised."""
    from mipyre import _reload

    if reload and not _reload.is_reload_child():
        # This process only watches: it ends the way the server it ran last ended.
        status = _reload.run_with_reload(_reload.ReloadTarget.of_this_process(app._engine.app_file()))
        if status != 0:
            raise SystemExit(status)
        return

    # The log is written by a background thread the process exit does not wait for: every
    # way out of a run waits for the lines logged before it.
    try:
        ServerRun(app, deployment, config, launch).serve()
    except AbandonedWorkersError as e:
        _exit_without_finalization(str(e), e)
    except OSError as e:
        if e.errno == errno.EADDRINUSE:
            e.add_note(_port_in_use(next(f for f in _flags() if f.name == "port")))
        raise
    finally:
        flush_logs()


def _port_in_use(port: Flag) -> str:
    return (
        f"Another program listens on that port: stop it, or serve on another port with "
        f"{port.cli} (mipyre run / mipyre dev), {port.env}, {port.file} in pyproject.toml, or "
        f"{' / '.join(port.code)} in code."
    )


@contextlib.contextmanager
def _sigint_ignored() -> Iterator[None]:
    """Ignore SIGINT for the block, then put the previous handler back.

    After a graceful stop the ctrl-C that caused it is often still pending: it fires once
    the code is back in Python bytecode, which would interrupt the first shutdown hook or
    the caller after ``run()`` returns. ``signal.signal`` delivers a pending signal before it
    installs the handler, so a ``KeyboardInterrupt`` raised by it is retried; once it
    returns none is left. Off the main thread (``ValueError``) nothing is changed. The
    handler is put back so a program that goes on after ``run()`` still gets
    ``KeyboardInterrupt`` on ctrl-C."""
    import signal

    previous = None
    while True:
        try:
            previous = signal.signal(signal.SIGINT, signal.SIG_IGN)
            break
        except KeyboardInterrupt:
            continue
        except (ValueError, OSError):
            break
    try:
        yield
    finally:
        if previous is not None:
            signal.signal(signal.SIGINT, previous)
