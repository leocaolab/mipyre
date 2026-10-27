"""Why a sub-interpreter worker could not import an extension, in words.

A C extension that does not declare multi-interpreter support fails to import in a
sub-interpreter with CPython's own ImportError (or the extension's own guard). mipyre
does not work around it: the routes that use such an extension run in a process. The
worker's start error keeps CPython's error and traceback and adds `explain`'s text: the
library, the user's line that imported it, and the two ways to fix it.
"""

from __future__ import annotations

import os
import re
import sysconfig

from mipyre._worker.extension_support import CPYTHON_REFUSAL

# What CPython and the extension-building tools raise when an extension can't load in
# a sub-interpreter, as their messages read.
MARKERS = (
    CPYTHON_REFUSAL,
    "cannot load module more than once per process",  # numpy's guard
    "do not yet support subinterpreters",  # PyO3
    "this module can only be loaded into one interpreter per process",  # Cython
    "failed to import from subinterpreter due to exception",  # CPython ran PyInit in main
)


def _chain(exc: BaseException):
    seen = set()
    link: BaseException | None = exc
    while link is not None and id(link) not in seen:
        seen.add(id(link))
        yield link
        link = link.__cause__ or link.__context__


def _offending(exc: BaseException) -> ImportError | None:
    """The ImportError in exc's cause/context chain that says the extension can't load in
    a sub-interpreter (a package may re-raise it with its own words around it)."""
    for e in _chain(exc):
        if isinstance(e, ImportError) and any(m in str(e) for m in MARKERS):
            return e
    return None


_MODULE_IN_TEXT = re.compile(r"module '?([A-Za-z_][\w.]*)'? " + re.escape(CPYTHON_REFUSAL))


def _module(exc: BaseException) -> str | None:
    """The extension module that failed: an ImportError's `name`, else the name CPython's
    message gives."""
    for e in _chain(exc):
        if isinstance(e, ImportError) and e.name:
            return e.name
    for e in _chain(exc):
        m = _MODULE_IN_TEXT.search(str(e))
        if m:
            return m.group(1)
    return None


def _library_dirs() -> tuple[str, ...]:
    paths = sysconfig.get_paths()
    dirs = {paths.get(k) for k in ("stdlib", "platstdlib", "purelib", "platlib")}
    dirs.add(os.path.dirname(os.path.abspath(__file__)))  # mipyre itself
    return tuple(os.path.abspath(d) + os.sep for d in dirs if d)


def _user_line(exc: BaseException) -> tuple[str, str] | None:
    """(`file:line`, source) of the last traceback frame in the user's code (not the
    standard library, not an installed package, not mipyre)."""
    import linecache

    library = _library_dirs()
    found = None
    tb = exc.__traceback__
    while tb is not None:
        code = tb.tb_frame.f_code
        path = code.co_filename
        if not path.startswith("<") and not os.path.abspath(path).startswith(library):
            line = linecache.getline(path, tb.tb_lineno).strip()
            found = (f"{path}:{tb.tb_lineno}", line)
        tb = tb.tb_next
    return found


def _imported_package(source: str) -> str | None:
    """The top-level package an ``import`` / ``from ... import`` line names, if one."""
    import ast

    try:
        tree = ast.parse(source.strip())
    except SyntaxError:
        return None
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            return node.names[0].name.split(".")[0]
        if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            return node.module.split(".")[0]
    return None


def explain(exc: BaseException) -> str | None:
    """The explanation for a worker whose start raised `exc`, or None if it is not an
    extension that can't load in a sub-interpreter."""
    if _offending(exc) is None:
        return None
    module = _module(exc)
    if module is None:
        return None  # CPython's text, unexplained, rather than a guessed library name
    extension = module.split(".")[0]
    where = _user_line(exc)
    # What the user wrote is what they move: `from pydantic import BaseModel` fails in
    # pydantic_core, and the fix is about pydantic.
    library = (_imported_package(where[1]) if where else None) or extension
    via = "" if library == extension else f" (it loads {module})"
    at = f"; imported at {where[0]}  {where[1]}\n" if where else ". "
    return (
        f"\n{library} can't load in a sub-interpreter{via}{at}Routes that use it need one of:\n"
        f"  1. runtime='process': they run in worker processes, where {library} loads as anywhere "
        f"else. Put them in a module of their own with router = Router(runtime='process') (named "
        f"in app.include_router('module:router')) and import {library} there, or import {library} "
        f"inside those routes' functions: a module with sub-interpreter routes is imported by every "
        f"sub-interpreter, so it must not import {library} at its top.\n"
        f"  2. runtime='main': they run on the main interpreter (one GIL; for few routes).\n"
        f"  3. runtime='isolated': each sub-interpreter loads its own copy of {library} with "
        f"CPython's multi-interpreter check off. A compatibility option, not the recommended "
        f"way: every worker holds a copy in memory, the copies share the process's signal "
        f"handlers, atexit hooks and thread pools, and {library} is not tested this way."
    )
