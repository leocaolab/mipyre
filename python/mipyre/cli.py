"""Mipyre command-line interface.

Entry point for the ``mipyre`` console script. Three subcommands:

- ``mipyre run <module:attr>``   production-ish launch (no reload)
- ``mipyre dev <module:attr>``   hot-reload, access log on (logs text at INFO)
- ``mipyre routes <module:attr>`` print the registered route table

The target is a ``module:attr`` string identifying a Mipyre app, just like
gunicorn/uvicorn. ``attr`` defaults to ``app``.

Before the first ``--``: mipyre's own options and the flags the app declares
(:mod:`mipyre.flags`), parsed once after the app is imported; ``--help`` lists both. What
follows the first ``--`` is the app's own command line: mipyre does not read it, and every
interpreter that runs the app (main, each sub-interpreter, each process worker) has
``sys.argv == [<the app's file>, *those arguments]``.

Examples::

    mipyre run examples.hello:app --port 8080
    mipyre run examples.hello:app --port 8080 --db-dsn postgres://localhost/app
    mipyre run examples.hello:app --port 8080 -- --debug
    mipyre dev examples.hello
    mipyre routes examples.hello
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from mipyre import _server_run, _target, flags
from mipyre._server_run import Launch
from mipyre.engine import ConfigError, Deployment, EntryPoint, ServerFlags

if TYPE_CHECKING:
    from mipyre.app import Mipyre


def _importable_cwd() -> None:
    """Make the current directory importable — same ergonomic as uvicorn."""
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)


def _set_app_argv(target: _target.Target, app_args: list[str]) -> None:
    """``sys.argv`` becomes the app's command line before any of the app is imported, so an app
    that parses it as it is imported reads its own arguments; the workers copy main's. Finding
    the app's file imports the packages above a dotted module, so they see the app's arguments
    behind the target as given; then ``argv[0]`` becomes the file."""
    _importable_cwd()
    sys.argv = [target.text, *app_args]
    try:
        sys.argv[0] = _target.target_file(target)
    except _target.MissingTargetError as e:
        sys.exit(f"mipyre: {e}")


def _load_app(target: _target.Target) -> "Mipyre":
    """Resolve ``module[:attr]`` (or ``path/to/app.py[:attr]``) → Mipyre instance.
    Importing the module runs its top-level code, which registers routes on the app."""
    _importable_cwd()
    try:
        module, app = _target.import_target(target)
    except _target.MissingTargetError as e:
        sys.exit(f"mipyre: {e}")

    from mipyre.app import Mipyre
    if not isinstance(app, Mipyre):
        sys.exit(
            f"mipyre: {target.text} is a {type(app).__name__}, expected mipyre.Mipyre"
        )
    # Sub-interpreter workers execute the script that defines the app. Under the CLI,
    # `__main__` is this file, so point them at the app's module instead.
    module_file = getattr(module, "__file__", None)
    if module_file:
        app._engine.set_script_path(module_file)
    return app


def _cmd_run(args: argparse.Namespace, import_app: Callable[[], Mipyre]) -> None:
    deployment = _deployment(args, EntryPoint.CliRun)
    # Before the app is imported, while this process runs no other thread: the template is
    # forked from it (Linux) and holds no app code, so an app that starts threads as it is
    # imported (numpy's BLAS pool, a polars query) can still serve process routes. If the app
    # has none, its run dismisses the template.
    from mipyre._process_lane import server as lane_server

    template = lane_server.start_template_with_driver(
        "mipyre run, before it imports the app,",
        "mipyre has not imported the app yet, so code that ran before it started them: a "
        "sitecustomize, a .pth file, a PYTHONSTARTUP script. Keep that code from starting "
        "threads, or serve the app with app.run() (`python app.py`), which starts the template "
        "as a new interpreter after it imports the app.",
    )
    app = import_app()
    launch = Launch(template=template)
    _server_run.run(app, deployment, ServerFlags(), launch, reload=deployment.reload(None))


def _cmd_dev(args: argparse.Namespace, import_app: Callable[[], Mipyre]) -> None:
    deployment = _deployment(args, EntryPoint.CliDev)
    app = import_app()
    # `mipyre dev` reloads, and logs unless the deployment's `log` says otherwise.
    _server_run.run(app, deployment, ServerFlags(), Launch(log=True), reload=True)


def _cmd_routes(args: argparse.Namespace, import_app: Callable[[], Mipyre]) -> None:
    app = import_app()
    rows: list[tuple[str, str, str, str]] = []
    for route in app.routes:
        flags = [f"runtime={route.runtime}"] + (["async"] if route.is_async else [])
        if route.body is not None:
            flags.append(f"body={route.body.__name__}")
        handler = getattr(route.handler, "__qualname__", getattr(route.handler, "__name__", repr(route.handler)))
        rows.append((route.method, route.path, handler, ",".join(flags)))
    for fast in app.fast_routes:
        rows.append((fast.method, fast.path, f"<fast:{fast.body_bytes}B>", f"status={fast.status_code}"))

    if not rows:
        print("(no routes registered)")
        return

    widths = [max(len(r[i]) for r in rows) for i in range(4)]
    header = ("METHOD", "PATH", "HANDLER", "FLAGS")
    widths = [max(widths[i], len(header[i])) for i in range(4)]
    fmt = "  ".join("{:<%d}" % w for w in widths)
    print(fmt.format(*header))
    print(fmt.format(*("-" * w for w in widths)))
    for row in rows:
        print(fmt.format(*row))
    print(f"\n{len(rows)} route(s)")


class _Options:
    """mipyre's own options of one subcommand, each with the line that adds it: an app flag of
    the same name is refused naming that line."""

    def __init__(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser
        self.sites: dict[str, flags._Site] = {}

    def add(self, *names: str, **kwargs: Any) -> None:
        caller = sys._getframe(1)
        action = self.parser.add_argument(*names, **kwargs)
        self.sites[action.dest] = flags._Site(caller.f_code.co_filename, caller.f_lineno)


def _add_flags(o: _Options, *, reload: bool) -> None:
    """mipyre's settings as options, from the engine's declaration table: each takes its value
    as typed (the engine reads it, with the environment and ``pyproject.toml``); a boolean
    given bare is on, ``--allowed-hosts`` and ``--trusted-proxies`` are repeatable. ``mipyre dev``
    is the reloader itself, so it has no ``--reload``."""
    from mipyre.engine import _flags

    for flag in _flags():
        if flag.name == "reload" and not reload:
            continue
        help = f"{flag.doc} ({flag.env}; default: {flag.default})"
        if flag.shape == "bool":
            o.add(flag.cli, dest=flag.name, nargs="?", const="true", default=None, metavar="BOOL", help=help)
        elif flag.shape == "hosts":
            o.add(flag.cli, dest=flag.name, action="append", default=None, metavar="NAME", help=help)
        elif flag.shape == "proxies":
            o.add(flag.cli, dest=flag.name, action="append", default=None, metavar="ADDR", help=help)
        else:
            o.add(flag.cli, dest=flag.name, default=None, help=help)


def _add_run_flags(o: _Options) -> None:
    _add_flags(o, reload=True)


def _add_dev_flags(o: _Options) -> None:
    _add_flags(o, reload=False)


def _given_flags(args: argparse.Namespace) -> dict[str, str]:
    """The settings given on the command line, by flag name, as typed (``--allowed-hosts``'s
    and ``--trusted-proxies``'s repeats joined by commas)."""
    from mipyre.engine import _flags

    given: dict[str, str] = {}
    for flag in _flags():
        value = getattr(args, flag.name, None)
        if value is not None:
            given[flag.name] = ",".join(value) if isinstance(value, list) else value
    return given


def _deployment(args: argparse.Namespace, entry: EntryPoint) -> Deployment:
    """Read the deployment once, before anything starts: every setting that cannot be used is
    printed, with where it came from, and ends the process with status 2."""
    try:
        return Deployment.from_process(_given_flags(args), entry)
    except ConfigError as e:
        for line in str(e).splitlines():
            print(f"mipyre: {line}", file=sys.stderr)
        raise SystemExit(2) from None


def _subcommand(sub, name: str, help: str, add_flags: Callable[[_Options], None] | None) -> _Options:
    """Add subcommand ``name``. Its parser has no ``--help`` of its own: the help is given once
    the app's flags are known (``_parse_with_app_flags``), and an argument it does not know may
    be one of them."""
    p = sub.add_parser(name, help=help, add_help=False, allow_abbrev=False,
                       usage="%(prog)s target [options] [-- APP_ARGS ...]")
    p.add_argument("target", nargs="?", help="module[:attr], e.g. examples.hello:app")
    options = _Options(p)
    if add_flags is not None:
        add_flags(options)
    return options


def _full_parser(own: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Return a parser of ``own``'s options with ``--help``, to which the app's flags are added."""
    return argparse.ArgumentParser(
        prog=own.prog, usage=own.usage, description=own.description, parents=[own], allow_abbrev=False
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="mipyre",
        description="Mipyre — high-performance Python web framework powered by Rust.",
    )
    from mipyre import __version__

    parser.add_argument("--version", action="version", version=f"mipyre {__version__}")
    sub = parser.add_subparsers(dest="cmd", required=True)
    commands = {
        "run": (_subcommand(sub, "run", "start the server", _add_run_flags), _cmd_run),
        "dev": (_subcommand(sub, "dev", "start with hot-reload and the access log on (logs text at INFO)", _add_dev_flags),
                _cmd_dev),
        "routes": (_subcommand(sub, "routes", "print the route table", None), _cmd_routes),
    }

    mine, app_args = flags._split_command_line(sys.argv[1:] if argv is None else argv)
    args, rest = parser.parse_known_args(mine)
    own, command = commands[args.cmd]
    tokens = mine[mine.index(args.cmd) + 1 :]
    if args.target is None:
        _full_parser(own.parser).parse_args(tokens)  # prints the help and exits, or refuses what is unknown
        own.parser.error("the following arguments are required: target")

    flags._begin(rest, own.parser.prog, own.sites)
    try:
        target = _target.parse_target(args.target)
    except ValueError as e:
        sys.exit(f"mipyre: {e}")
    _set_app_argv(target, app_args)

    def import_app() -> Mipyre:
        app = _load_app(target)
        flags._seal(_full_parser(own.parser), tokens)
        return app

    command(args, import_app)


if __name__ == "__main__":
    main()
