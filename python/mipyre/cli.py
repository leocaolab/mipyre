"""Mipyre command-line interface.

Entry point for the ``mipyre`` console script. Three subcommands:

- ``mipyre run <module:attr>``   production-ish launch (no reload)
- ``mipyre dev <module:attr>``   hot-reload + DEBUG logging
- ``mipyre routes <module:attr>`` print the registered route table

The target is a ``module:attr`` string identifying a Mipyre app, just like
gunicorn/uvicorn. ``attr`` defaults to ``app``.

Examples::

    mipyre run examples.hello:app --port 8080
    mipyre dev examples.hello
    mipyre routes examples.hello
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import TYPE_CHECKING

from mipyre._runtime import ProcessLanes
from mipyre.app import _Launch

if TYPE_CHECKING:
    from mipyre.app import Mipyre


# The target `mipyre run` loaded (`module:attr` or `path.py:attr`): a process-lane worker
# imports the app the same way.
LOADED_TARGET: str | None = None


def _load_app(target: str) -> "Mipyre":
    """Resolve ``module[:attr]`` (or ``path/to/app.py[:attr]``) → Mipyre instance.
    Importing the module runs its top-level code, which registers routes on the app."""
    global LOADED_TARGET
    from mipyre import _runtime

    # Make the current directory importable — same ergonomic as uvicorn.
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    try:
        module_name, attr = _runtime.split_target(target)
    except ValueError as e:
        sys.exit(f"mipyre: {e}")
    try:
        module, app = _runtime.import_target(target)
    except ImportError as e:
        sys.exit(f"mipyre: cannot import {module_name!r}: {e}")
    except AttributeError as e:
        sys.exit(f"mipyre: {e}")

    from mipyre.app import Mipyre
    if not isinstance(app, Mipyre):
        sys.exit(
            f"mipyre: {target} is a {type(app).__name__}, expected mipyre.Mipyre"
        )
    # Sub-interpreter workers execute the script that defines the app. Under the CLI,
    # `__main__` is this file, so point them at the app's module instead.
    module_file = getattr(module, "__file__", None)
    if module_file:
        app._set_app_file(os.path.abspath(module_file))
    LOADED_TARGET = f"{os.path.abspath(module_name)}:{attr}" if module_name.endswith(".py") else f"{module_name}:{attr}"
    return app


def _export_runtime_flag(args: argparse.Namespace) -> None:
    """``--runtime`` becomes ``MIPYRE_RUNTIME``: every interpreter that rebuilds the app (the
    server's, each sub-interpreter re-executing the app's script, each process-lane worker)
    resolves the app's runtime as it is constructed, and the environment is what they all
    read. ``MIPYRE_RUNTIME_SOURCE`` lets an error name the flag rather than the variable."""
    if getattr(args, "runtime", None):
        os.environ["MIPYRE_RUNTIME"] = args.runtime
        os.environ["MIPYRE_RUNTIME_SOURCE"] = f"mipyre run --runtime {args.runtime}"


def _lane_flags(args: argparse.Namespace) -> ProcessLanes:
    """``--process-*``: the deployment's process-lane settings, below what the code
    declares and above the environment."""
    return ProcessLanes(
        start=args.process_start,
        workers=args.process_workers,
        timeout=args.process_timeout,
        queue_per_worker=args.process_queue,
        slot_bytes=args.process_slot_bytes,
    )


def _cmd_run(args: argparse.Namespace) -> None:
    # Normalize empty strings to None so `--tls-cert ""` is rejected, not
    # silently forwarded to app.run() as a bogus file path.
    tls_cert = args.tls_cert or None
    tls_key = args.tls_key or None
    if (tls_cert is None) != (tls_key is None):
        sys.exit("mipyre: --tls-cert and --tls-key must be specified together")
    _export_runtime_flag(args)
    # Before the app is imported: process-lane workers fork from a template that holds no
    # app code. If the app has no process routes, the template is dismissed.
    from mipyre import _process_lane

    _process_lane.start_template()
    app = _load_app(args.target)
    app._run(
        os.environ,
        _Launch(lanes=_lane_flags(args)),
        host=args.host,
        port=args.port,
        workers=args.workers,
        io_workers=args.io_workers,
        reload=False,
        tls_cert=tls_cert,
        tls_key=tls_key,
    )


def _cmd_dev(args: argparse.Namespace) -> None:
    app = _load_app(args.target)
    app._run(
        os.environ,
        # `mipyre dev` logs unless MIPYRE_LOG says otherwise.
        _Launch(log=os.environ.get("MIPYRE_LOG", "1") == "1"),
        host=args.host,
        port=args.port,
        workers=args.workers,
        reload=True,
    )


def _cmd_routes(args: argparse.Namespace) -> None:
    app = _load_app(args.target)
    rows: list[tuple[str, str, str, str]] = []
    for route in app.routes:
        flags = [f"runtime={route.runtime}"] + [
            name for name, on in (("stream", route.stream), ("async", route.is_async)) if on
        ]
        if route.model is not None:
            flags.append(f"model={route.model}")
        rows.append((route.method, route.path, route.handler, ",".join(flags)))
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


def _add_run_flags(p: argparse.ArgumentParser) -> None:
    p.add_argument("target", help="module[:attr], e.g. examples.hello:app")
    p.add_argument("--host", default=None, help="bind address (default 127.0.0.1)")
    p.add_argument("--port", type=int, default=None, help="bind port (default 8000)")
    p.add_argument("--workers", type=int, default=None, help="sub-interpreter count")
    p.add_argument("--io-workers", type=int, default=None, help="Tokio I/O thread count")
    p.add_argument("--tls-cert", default=None)
    p.add_argument("--tls-key", default=None)
    p.add_argument("--runtime", choices=("subinterp", "isolated", "process", "main"), default=None,
                   help="the app's runtime when its code names none (deployment default)")
    p.add_argument("--process-workers", type=int, default=None, help="process-lane workers")
    p.add_argument("--process-start", choices=("import", "fork"), default=None,
                   help="how process workers get the app: import it (default) or fork from a template that did")
    p.add_argument("--process-timeout", type=float, default=None, help="process handler timeout, seconds")
    p.add_argument("--process-queue", type=int, default=None, help="requests per process worker before 503")
    p.add_argument("--process-slot-bytes", type=int, default=None, help="max request / answer bytes")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="mipyre",
        description="Mipyre — high-performance Python web framework powered by Rust.",
    )
    try:
        from mipyre import __version__ as _v
    except (ImportError, AttributeError):
        _v = "dev"
    parser.add_argument("--version", action="version", version=f"mipyre {_v}")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="start the server")
    _add_run_flags(p_run)
    p_run.set_defaults(func=_cmd_run)

    p_dev = sub.add_parser("dev", help="start with hot-reload + debug logging")
    p_dev.add_argument("target", help="module[:attr]")
    p_dev.add_argument("--host", default="127.0.0.1")
    p_dev.add_argument("--port", type=int, default=None)
    p_dev.add_argument("--workers", type=int, default=None)
    p_dev.set_defaults(func=_cmd_dev)

    p_routes = sub.add_parser("routes", help="print the route table")
    p_routes.add_argument("target", help="module[:attr]")
    p_routes.set_defaults(func=_cmd_routes)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
