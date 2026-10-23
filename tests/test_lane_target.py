"""b/845 E: the target a process-lane worker imports to rebuild the app, as `lane_target`
finds it among the loaded modules (it replaced b/847's `_run_target`, which named the
`mipyre run` command an `app.run()` with process routes was refused with)."""

from __future__ import annotations

import importlib.machinery
import types

from mipyre._process_lane.server import lane_target

APP = object()
FILE = "/home/u/proj/myapp/web.py"


def _module(name: str, file: str | None, *, spec: str | None = None, **names: object) -> types.ModuleType:
    module = types.ModuleType(name)
    if file is not None:
        module.__file__ = file
    module.__spec__ = None if spec is None else importlib.machinery.ModuleSpec(spec, None)
    vars(module).update(names)
    return module


def test_an_imported_module_is_named_as_a_module():
    assert lane_target(APP, FILE, [_module("myapp.web", FILE, spec="myapp.web", api=APP)]) == "myapp.web:api"


def test_a_script_run_as_main_is_named_by_its_file():
    assert lane_target(APP, FILE, [_module("__main__", FILE, app=APP)]) == f"{FILE}:app"


def test_a_module_run_with_python_m_is_named_as_that_module():
    # Its package's relative imports then resolve in the worker.
    assert lane_target(APP, FILE, [_module("__main__", FILE, spec="myapp.web", app=APP)]) == "myapp.web:app"


def test_only_the_module_of_the_apps_file_counts():
    other = _module("other", "/home/u/proj/other.py", app=APP)
    unbound = _module("myapp.web", FILE, spec="myapp.web", app=object())
    no_file = _module("builtin_like", None, app=APP)
    assert lane_target(APP, FILE, [other, unbound, no_file]) is None
    assert lane_target(APP, FILE, [other, unbound, _module("__main__", FILE, app=APP)]) == f"{FILE}:app"
