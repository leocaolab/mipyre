"""Import targets: the ``module:attr`` strings (or ``path/to/file.py:attr``) that name an app or a
router, read and imported. ``mipyre run`` loads the app by one, and so does a process-lane worker."""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys


def split_target(target: str) -> tuple[str, str]:
    """``module[:attr]`` or ``path/to/file.py[:attr]`` → (module or path, attr)."""
    if target.count(":") > 1:
        raise ValueError(f"invalid target {target!r}: expected module:attr (exactly one ':')")
    module, _, attr = target.partition(":")
    if not module:
        raise ValueError(f"invalid target {target!r}: module name must not be empty")
    return module, attr or "app"


class MissingTargetError(Exception):
    """A ``module:attr`` target that names something that does not exist: the module (or the
    ``.py`` file) when ``attr`` is ``None``, else the attribute of the module. Anything else
    importing the module raises is the module's own error, not this."""

    def __init__(self, target: str, module: str, attr: str | None):
        self.target = target
        self.module = module
        self.attr = attr
        missing = f"no module named {module!r}" if attr is None else f"module {module!r} has no attribute {attr!r}"
        super().__init__(f"cannot import {target!r}: {missing}")


def names_target(module_name: str, missing: str) -> bool:
    """Whether a ``ModuleNotFoundError`` for ``missing``, raised importing ``module_name``,
    is that module itself or a package above it, rather than an import inside it."""
    return module_name == missing or module_name.startswith(missing + ".")


def _target_path(module_name: str) -> str | None:
    """The absolute path a ``path/to/file.py`` target names, or ``None`` for a module name."""
    if module_name.endswith(".py") or os.sep in module_name:
        return os.path.abspath(module_name)
    return None


def target_file(target: str) -> str:
    """The source file of the module ``target`` names, found without executing that module
    (the packages above a dotted module are imported, as importing it would): what
    ``sys.argv[0]`` is while the app imports. A module with no file of its own (a namespace
    package) is named by its module name. A module or file that does not exist is a
    :class:`MissingTargetError`."""
    module_name, _attr = split_target(target)
    path = _target_path(module_name)
    if path is not None:
        if not os.path.isfile(path):
            raise MissingTargetError(target, path, None)
        return path
    try:
        spec = importlib.util.find_spec(module_name)
    except ModuleNotFoundError as e:
        if e.name is None or not names_target(module_name, e.name):
            raise
        raise MissingTargetError(target, e.name, None) from None
    if spec is None:
        raise MissingTargetError(target, module_name, None)
    if spec.has_location and spec.origin is not None:
        return os.path.abspath(spec.origin)
    return module_name


def import_target(target: str):
    """The module ``target`` names, imported (a ``.py`` path is loaded under its stem's
    name), and the object its attribute names. A module, file or attribute that does not
    exist is a :class:`MissingTargetError`; whatever else importing the module raises
    propagates as it is."""
    module_name, attr = split_target(target)
    path = _target_path(module_name)
    if path is not None:
        if not os.path.isfile(path):
            raise MissingTargetError(target, path, None)
        name = os.path.splitext(os.path.basename(path))[0]
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        directory = os.path.dirname(path)
        if directory not in sys.path:
            sys.path.insert(0, directory)
        spec.loader.exec_module(module)
    else:
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as e:
            if e.name is None or not names_target(module_name, e.name):
                raise
            raise MissingTargetError(target, e.name, None) from None
    # Looked up in the module's namespace only: a module `__getattr__` is the module's code.
    namespace = vars(module)
    if attr not in namespace:
        raise MissingTargetError(target, module_name, attr)
    return module, namespace[attr]
