"""The sub-interpreter loading adapter of `runtime="isolated"` (design §14): a
compatibility option, not the recommended way. A package whose extensions can't load
shared in an own-GIL sub-interpreter is given to each worker as a private copy, loaded with
CPython's multi-interpreter check switched off for that load.

When it decides (`docs/design/isolation-finder.md`): the first time a worker resolves a
top-level name, before any of the package's code runs. `_IsolatingFinder.find_spec` reads
the package's extension files (`extension_support`): if one of them can't load shared, the
spec it returns points at this worker's copy of the package, so the import never fails
first. Submodules follow their package's `__path__`, the copy's or the original's.

Not a module: the engine runs this file in the worker bootstrap's namespace
(`__mipyre_bootstrap__`), after the logging bridge, `mipyre/_clone.py` and
`mipyre/_worker/extension_support.py` (whose names it uses) and before the `mipyre`
package is imported, and only when the app has `runtime="isolated"` routes (every
sub-interpreter of that app then has it); the bootstrap installs the finder. Costs and risks:
one copy of each isolated library per worker (memory), process-level OS state those
libraries assume is theirs alone, and a way of loading their authors don't test. Libraries
like numpy belong in `runtime="process"`.
"""

import _thread
import importlib.machinery as _machinery
import logging as _logging
import os as _os
import sys as _sys_iso
import sysconfig as _sysconfig
from dataclasses import dataclass as _dataclass

TYPE_CHECKING = False  # the bare-name form type checkers honour, so a worker imports no `typing`
if TYPE_CHECKING:  # the engine runs these files in this namespace first
    from mipyre._clone import PrivateCopyError, _remove_copy, claim_name, clone_tree, ensure_private_dir
    from mipyre._worker.extension_support import (
        ExtensionProbeError,
        MultiPhaseUnread,
        Unsupported,
        per_interpreter_support,
        probe_slot,
        static_facts,
    )

    # The server's isolate_dir, which the engine sets in this namespace before running these files.
    ISOLATE_DIR: str | None = None


def _override_check(value):
    """Set THIS interpreter's multi-interpreter extension check (-1: off, for this worker's
    private copies; 1: enforced, for shared files) and return the previous setting, for
    `_restore_check` in a `finally`.

    The setting is per interpreter and global to it, so it is only ever set around one load:
    left off, the next shared file that can't load in a sub-interpreter would load anyway."""
    import _imp

    return _imp._override_multi_interp_extensions_check(value)


def _restore_check(prev):
    import _imp

    _imp._override_multi_interp_extensions_check(prev)


class _IsoError(RuntimeError):
    """Per-worker isolation could not prepare a library's private copy."""


def _iso_root():
    """Where the per-worker clones live: the server's `isolate_dir` (`ISOLATE_DIR`, which the
    engine sets in this namespace; by default `mipyre` in this user's cache dir). Not the temp
    dir: the OS cleaner deletes files there that have not been read for days, which leaves a
    reused clone with its `.so` and without its `.py` (b/3)."""
    return ISOLATE_DIR


def _iso_worker_dir():
    """Claim a worker's private clone dir under `<root>/auto/` and return it.

    The slot is the first free `w{i}`, held for the process lifetime (the lock's fd is
    deliberately left open — closing it would release the slot). Concurrent servers therefore
    get distinct slots, while a fresh run of a single server reuses the low indices, i.e. the
    previous run's clone dirs: the same inodes, whose code signatures macOS has already verified
    (it does so on the first dlopen of an inode, seconds per worker for numpy + scipy). Held per
    open-file-description, so sibling workers in this same process also each get a distinct
    slot. Per-library freshness is the `.sig` manifest of `_iso_clone_lib`."""
    root = _iso_root()
    try:
        ensure_private_dir(root)
    except PrivateCopyError as e:
        raise _IsoError(f"{e}; set MIPYRE_ISOLATE_DIR to a directory you own") from e
    base = _os.path.join(root, "auto")
    _os.makedirs(base, exist_ok=True)
    idx = 0
    while claim_name(base, "w%d" % idx) is None:
        idx += 1
    worker_dir = _os.path.join(base, "w%d" % idx)
    _os.makedirs(worker_dir, exist_ok=True)
    return worker_dir


def _iso_clone_files(clone):
    """The files of a clone as {relative path: size} ("" for a single-file clone).
    `__pycache__` is left out: Python writes there at runtime, and losing it costs a
    recompile, not an import."""
    if not _os.path.isdir(clone) or _os.path.islink(clone):
        return {"": _os.lstat(clone).st_size}
    files = {}
    for root, dirs, names in _os.walk(clone):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for name in names:
            path = _os.path.join(root, name)
            files[_os.path.relpath(path, clone)] = _os.lstat(path).st_size
    return files


def _iso_clone_stale(clone, manifest_path, source_sig):
    """Why the existing `clone` can't be reused, as `(damaged, reason)`, or None if it
    can. Its manifest must name the current source, and every file the clone was made
    with must still be there at its size (`damaged`): something other than mipyre deleted
    or changed it (a cache cleaner; a `.py` loaded from its `.pyc` is only stat'ed, never
    read, so a missing one shows up only here)."""
    import json

    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
    except FileNotFoundError:
        return False, "it has no manifest"
    except ValueError:
        return False, "its manifest is not the current format"
    if manifest.get("source") != source_sig:
        return False, "the library changed"
    for rel, size in manifest.get("files", {}).items():
        path = _os.path.join(clone, rel) if rel else clone
        try:
            now = _os.lstat(path).st_size
        except FileNotFoundError:
            return True, f"{path} is missing"
        if now != size:
            return True, f"{path} is {now} bytes, cloned as {size}"
    return None


def _iso_clone_lib(lib, src, worker_dir, pkg2dist):
    """Clone `lib`, whose original install is `src` (a package dir or a single-file module),
    and its vendored `.libs` into `worker_dir`. Returns the cloned destination path.

    Freshness: each clone has a `<basename>.sig` manifest with its source's signature
    and the clone's files. An existing clone is reused only if the source is unchanged
    (a lib upgrade forces a re-clone) and the clone is still whole (a file deleted from
    it forces a re-clone, with a warning naming the file)."""
    import json

    def _clone(s, d, manifest_path):
        st = _os.stat(s)
        source_sig = "%d\0%d" % (st.st_mtime_ns, st.st_size)
        if _os.path.lexists(d):
            stale = _iso_clone_stale(d, manifest_path, source_sig)
            if stale is None:
                return  # up-to-date clone, inodes already verified
            damaged, why = stale
            if damaged:
                _logging.getLogger("mipyre.isolate").warning(
                    "auto-isolate: the clone of %r at %s is damaged (%s); cloning it again",
                    lib, d, why,
                )
            _remove_copy(d)
        # Clone into a temp dir and atomically rename it into place, so a
        # first-ever concurrent boot never observes a half-written copy.
        tmp = "%s.tmp-%d" % (d, _os.getpid())
        clone_tree(s, tmp)
        try:
            _os.replace(tmp, d)
        except OSError:
            if not _os.path.lexists(d):
                raise
            _remove_copy(tmp)  # lost the race to another boot — use theirs
        manifest_tmp = "%s.tmp-%d" % (manifest_path, _os.getpid())
        with open(manifest_tmp, "w") as f:
            json.dump({"source": source_sig, "files": _iso_clone_files(d)}, f)
        _os.replace(manifest_tmp, manifest_path)

    base_name = _os.path.basename(src)
    dst = _os.path.join(worker_dir, base_name)
    _clone(src, dst, _os.path.join(worker_dir, base_name + ".sig"))
    # Also clone the wheel's vendored shared libs: on Linux these live in a
    # sibling `<name>.libs/` dir (e.g. numpy.libs/ holds OpenBLAS, referenced
    # by RPATH next to the package); macOS bundles them inside `<pkg>/.dylibs/`.
    # `<name>` is the DISTRIBUTION name, which can differ from the import name
    # — e.g. `sklearn` (import) ships its libgomp in `scikit_learn.libs/`, so
    # guessing `<import_name>.libs` alone misses it. Clone every spelling.
    parent = _os.path.dirname(src)
    libs_names = {base_name}
    for dist in pkg2dist.get(base_name, []):
        libs_names.add(dist.replace("-", "_"))
    for ln in libs_names:
        vendored = _os.path.join(parent, ln + ".libs")
        if _os.path.isdir(vendored):
            vendored_dst = _os.path.join(worker_dir, _os.path.basename(vendored))
            _clone(vendored, vendored_dst, vendored_dst + ".sig")
    return dst


def _iso_pkg2dist():
    """Import name -> distribution names, for finding a package's vendored `<dist>.libs`.
    Reading it fails only on broken install metadata; a clone made without it could miss
    the libraries the package loads, so that is an error, with its cause."""
    import importlib.metadata as _md
    try:
        return _md.packages_distributions()
    except Exception as exc:
        raise _IsoError(
            "reading the installed distributions (importlib.metadata."
            f"packages_distributions) failed: {exc!r}; a per-worker copy could miss the "
            "shared libraries its package vendors, so none is made"
        ) from exc


def _iso_report(lib, dst, why):
    """Never silent: the copy's size and the reason for it go to the engine, which logs each
    isolated library once per process (the startup line, or a line of its own when a
    handler's lazy import isolates it later), with the compatibility-option caveat. The copy
    was just made, so a file in it that can't be stat'ed is an error."""
    total = 0
    if _os.path.isdir(dst) and not _os.path.islink(dst):
        for r, _d, files in _os.walk(dst):
            for f in files:
                total += _os.lstat(_os.path.join(r, f)).st_size
    else:
        total = _os.lstat(dst).st_size
    from mipyre.engine import _note_isolated

    _note_isolated(lib, total, why)


def _iso_create_here(spec):
    """Create an extension module entirely in THIS interpreter: call its
    `PyInit_*` here, and for a multi-phase one build the module from the
    returned def here. None if the binary has no `PyInit_<name>` symbol.

    CPython 3.13+ runs every extension's `PyInit_*` in the MAIN interpreter
    (import.c `import_run_extension` -> `switch_to_main_interpreter`):
    - single-phase: the module is built there and the worker gets a shallow
      copy of its dict, so its objects live on main's obmalloc heap and the
      first one the worker mutates frees main's memory into its own allocator
      (scipy's f2py `_fblas`: setattr on a fortran object -> `free(): invalid
      size`);
    - multi-phase: `PyInit_*` should only return the def, but many also call
      `import_array()` there (scipy's `_arpacklib`), which then imports numpy in
      main and stores MAIN's numpy C-API table in the binary's statics.

    Only for a private clone: nothing else loads that file, so skipping
    CPython's process-wide extension registry can't double-initialize its C
    statics. Exec slots run later through the loader's `exec_module`."""
    import sys
    # Imported on the first private clone, not by the bootstrap: a worker that loads no
    # clone never needs it, and it costs each interpreter about 0.2 MiB. Its own
    # extensions (`_ctypes`, `_struct`) are standard-library files, which the finder
    # leaves to CPython, so loading them never comes back into this function.
    import ctypes
    short = spec.name.rpartition(".")[2]
    lib = ctypes.PyDLL(spec.origin, mode=sys.getdlopenflags())
    try:
        init = getattr(lib, "PyInit_" + short)
    except AttributeError:
        return None
    # Take the result as a raw pointer. A multi-phase PyInit returns its static
    # PyModuleDef; wrapping that as an owned object (restype=py_object) decrefs it
    # when dropped. A C extension's def is immortal, so nothing happens, but PyO3's
    # starts at refcount 1: the decref frees static memory -> abort ("pointer being
    # freed was not allocated", measured with polars' _polars_runtime on 3.14).
    init.restype = ctypes.c_void_p
    ptr = init()
    if not ptr:
        raise SystemError(f"PyInit_{short} returned NULL without an exception")
    api = ctypes.pythonapi
    api.PyObject_Type.restype = ctypes.c_void_p      # new ref to the TYPE; the object is untouched
    api.PyObject_Type.argtypes = [ctypes.c_void_p]
    ob_type = api.PyObject_Type(ptr)
    api.Py_DecRef(ctypes.c_void_p(ob_type))
    if ob_type == ctypes.addressof(ctypes.c_char.in_dll(api, "PyModuleDef_Type")):
        # multi-phase: CPython's own next step, here instead of after its switch
        # to main (it also enforces Py_mod_multiple_interpreters under the
        # current override). The def stays borrowed, as CPython keeps it.
        api.PyModule_FromDefAndSpec2.restype = ctypes.py_object
        api.PyModule_FromDefAndSpec2.argtypes = [ctypes.c_void_p, ctypes.py_object, ctypes.c_int]
        return api.PyModule_FromDefAndSpec2(ptr, spec, sys.api_version)
    obj = ctypes.cast(ptr, ctypes.py_object).value  # takes its own reference
    api.Py_DecRef(ctypes.c_void_p(ptr))             # release the one PyInit returned
    api.PyModule_GetDef.restype = ctypes.c_void_p
    api.PyModule_GetDef.argtypes = [ctypes.py_object]
    api.PyState_AddModule.argtypes = [ctypes.py_object, ctypes.c_void_p]
    moddef = api.PyModule_GetDef(obj)
    # What CPython's fixup does: register it for PyState_FindModule.
    if moddef and api.PyState_AddModule(obj, moddef) != 0:
        raise ImportError(f"PyState_AddModule failed for {spec.name}")
    obj.__file__ = spec.origin
    return obj


# -- What a worker decided about a package, and why --------------------------------------------


@_dataclass(frozen=True)
class _Shared:
    """Every extension file of the package can load shared; `files` are their facts, the basis."""

    files: tuple


@_dataclass(frozen=True)
class _Copy:
    """The package loads from this worker's private copy, because of `reason` (the first of
    its files, in path order, that can't load shared)."""

    reason: "Unsupported"


def _verdict(facts):
    """The package's verdict from the facts of its extension files, in path order: a copy for
    the first file that can't load shared, else shared."""
    for file_facts in facts:
        support = per_interpreter_support(file_facts)
        if isinstance(support, Unsupported):
            return _Copy(support)
    return _Shared(tuple(facts))


# -- Why a package could not be isolated (always with the reason it needed a copy) -----------


@_dataclass(frozen=True)
class _PrivateCopyFailed:
    """The copy could not be made: the isolate dir is unusable, `cp` and the plain copy failed, or
    the installed distributions could not be read. `error` is what failed, in its words."""

    error: str
    needed: "Unsupported"

    def __str__(self):
        return f"it needs a per-worker copy ({self.needed}), and making it failed: {self.error}"


@_dataclass(frozen=True)
class _NotCopyable:
    """The package needs a copy but is no single directory or file on disk (a namespace package
    spread over several directories)."""

    where: str
    needed: "Unsupported"

    def __str__(self):
        return (
            f"it needs a per-worker copy ({self.needed}), but it is {self.where}, which can't be copied "
            "as one tree; this install can't give each worker its own copy. Use runtime='process' for "
            "the routes that use it"
        )


@_dataclass(frozen=True)
class _FileUnreadable:
    """An extension file of the package could not be read (the `OSError`, with its path, is the cause)."""

    file: str

    def __str__(self):
        return f"reading its extension file {self.file} failed"


@_dataclass(frozen=True)
class _ProbeRaised:
    """Calling a file's `PyInit_*` to read its slot raised: loading it would fail the same way."""

    file: str
    symbol: str

    def __str__(self):
        return f"calling {self.symbol} in {self.file} to read its module def failed"


@_dataclass(frozen=True)
class _SharedLoadRefused:
    """mipyre judged the package shared, and CPython's check refused one of its files: the
    judgement and CPython disagree, a mipyre bug."""

    module: str
    basis: tuple

    def __str__(self):
        said = "; ".join(str(f) for f in self.basis) or "it has no extension files"
        return (
            f"mipyre judged it could load shared ({said}), but CPython refused to load {self.module} "
            "in this sub-interpreter. mipyre's judgement is wrong for this file; please report it"
        )


class IsolationError(RuntimeError):
    """`runtime="isolated"` could not load `package` as it decided to. Not an `ImportError`, so a
    library's `except ImportError:` fallback can't hide it."""

    def __init__(self, package, reason):
        super().__init__(f"runtime='isolated' could not load {package}: {reason}")
        self.package = package
        self.reason = reason


# -- The finder and the loaders ---------------------------------------------------------------


def _is_mipyre(name):
    # A sub-interpreter worker never gets its own copy of mipyre: its engine keeps process-wide
    # state (the main-interpreter handle, the async worker registry, the logger, the Postgres
    # pool) that must exist once per process. Its engine declares per-interpreter support and
    # loads through CPython's own check.
    return name == "mipyre" or name.startswith("mipyre.")


def _stdlib_dirs():
    """(standard library dirs, site-packages dirs): the base install's `stdlib` / `platstdlib`,
    and every site dir, which can sit inside them (a system Python's `lib/python3.x/site-packages`)."""
    base = _sysconfig.get_paths(vars={"base": _sys_iso.base_prefix, "platbase": _sys_iso.base_exec_prefix})
    here = _sysconfig.get_paths()

    def dirs(*found):
        return tuple({_os.path.realpath(d) + _os.sep for d in found if d})

    return (
        dirs(base.get("stdlib"), base.get("platstdlib")),
        dirs(base.get("purelib"), base.get("platlib"), here.get("purelib"), here.get("platlib")),
    )


def _extension_files(dirs):
    """The extension files under `dirs`, by path, `__pycache__` left out."""
    suffixes = tuple(_machinery.EXTENSION_SUFFIXES)
    found = []
    for top in dirs:
        for root, subdirs, names in _os.walk(top):
            subdirs[:] = [d for d in subdirs if d != "__pycache__"]
            found.extend(_os.path.join(root, n) for n in names if n.endswith(suffixes))
    return sorted(found)


def _package_files(spec):
    """The extension files of a top-level module's install: every one under a package's
    directories, or the module's own file. None but files on disk count: a zip can't hold a
    loadable extension."""
    if spec.submodule_search_locations is not None:
        return _extension_files([d for d in spec.submodule_search_locations if _os.path.isdir(d)])
    origin = spec.origin
    if isinstance(origin, str) and _os.path.isfile(origin) and origin.endswith(tuple(_machinery.EXTENSION_SUFFIXES)):
        return [origin]
    return []


class _IsolatingFinder:
    """First on `sys.meta_path`: resolves each module through the finders after it, and decides,
    the first time a worker resolves a top-level package, whether this worker loads it from a
    private copy. What it decided, and every failure to carry that out, is kept per package, so a
    failed copy is raised again as it was, never retried (one worker's copy of one package is the
    failure domain; a restart retries)."""

    def __init__(self):
        self._verdicts = {}  # top-level name -> _Shared | _Copy | IsolationError
        self._worker_dir = None
        self._dists = None  # packages_distributions(), read for the first copy
        self._lock = _thread.allocate_lock()  # the worker dir and the distributions, read once
        self._stdlib, self._site = _stdlib_dirs()

    def find_spec(self, fullname, path, target=None):
        spec = self._next_spec(fullname, path, target)
        if spec is None or _is_mipyre(fullname):
            return spec
        if isinstance(spec.loader, type) and issubclass(spec.loader, _machinery.BuiltinImporter):
            spec.loader = _SharedBuiltinLoader(spec.loader)
            return spec
        if spec.loader is _machinery.FrozenImporter or self._in_stdlib(spec):
            return spec  # CPython's own modules go through CPython's own check
        verdict = self._verdict_of(fullname, spec) if path is None else None
        if isinstance(verdict, _Copy):
            spec = self._private_spec(fullname, spec, verdict)
        if isinstance(spec.loader, _machinery.ExtensionFileLoader):
            spec.loader = _IsolatingExtensionLoader(spec.loader, self, fullname.partition(".")[0])
        return spec

    def _next_spec(self, fullname, path, target):
        """The spec the finders after this one give, in `sys.meta_path` order: the one search."""
        finders = _sys_iso.meta_path
        start = next(i for i, f in enumerate(finders) if f is self) + 1
        for finder in finders[start:]:
            find = getattr(finder, "find_spec", None)
            if find is not None:
                spec = find(fullname, path, target)
                if spec is not None:
                    return spec
        return None

    def _in_stdlib(self, spec):
        origin = spec.origin
        if not isinstance(origin, str):
            return False
        real = _os.path.realpath(origin)
        return real.startswith(self._stdlib) and not real.startswith(self._site)

    def shared_basis(self, package):
        """The facts a shared verdict on `package` rests on (empty if none was made)."""
        verdict = self._verdicts.get(package)
        return verdict.files if isinstance(verdict, _Shared) else ()

    def _verdict_of(self, name, spec):
        verdict = self._verdicts.get(name)
        if verdict is None:
            try:
                verdict = self._decide(name, spec)
            except IsolationError as e:
                verdict = e
            self._verdicts[name] = verdict
        if isinstance(verdict, IsolationError):
            raise verdict.with_traceback(None)
        return verdict

    def _decide(self, name, spec):
        """Every file's symbol table first: one single-phase file settles it without calling any
        `PyInit_*`. Only a package whose files are all multi-phase has their slots probed."""
        facts = []
        for file in _package_files(spec):
            try:
                facts.append(static_facts(file))
            except OSError as e:
                raise IsolationError(name, _FileUnreadable(file)) from e
        verdict = _verdict([f for f in facts if not isinstance(f, MultiPhaseUnread)])
        if isinstance(verdict, _Copy):
            return verdict
        probed = []
        for file_facts in facts:
            if isinstance(file_facts, MultiPhaseUnread):
                try:
                    file_facts = probe_slot(file_facts)
                except ExtensionProbeError as e:
                    raise IsolationError(name, _ProbeRaised(e.file, e.symbol)) from e
            probed.append(file_facts)
        return _verdict(probed)

    def _private_spec(self, name, spec, verdict):
        """This worker's copy of the package (made now if it is not there or not current), as the
        spec to load; the clone dir is never on `sys.path`, so nothing reaches it but this."""
        dirs = spec.submodule_search_locations
        if dirs is not None and len(dirs) != 1:
            raise self._failed(name, _NotCopyable(f"a namespace package in {len(dirs)} directories ({', '.join(dirs)})", verdict.reason))
        src = next(iter(dirs)) if dirs is not None else spec.origin
        try:
            worker_dir, dists = self._claimed()
            dst = _iso_clone_lib(name, src, worker_dir, dists)
        except (_IsoError, PrivateCopyError, OSError) as e:
            raise self._failed(name, _PrivateCopyFailed(str(e), verdict.reason)) from e
        private = _machinery.PathFinder.find_spec(name, [worker_dir])
        if private is None:
            raise self._failed(name, _PrivateCopyFailed(f"the copy at {dst} does not resolve as {name}", verdict.reason))
        _iso_report(name, dst, str(verdict.reason))
        return private

    def _claimed(self):
        """(this worker's clone dir, the installed distributions), claimed and read once: two
        threads of a worker can make their first copies at the same time."""
        with self._lock:
            if self._worker_dir is None:
                self._worker_dir = _iso_worker_dir()
            if self._dists is None:
                self._dists = _iso_pkg2dist()
            return self._worker_dir, self._dists

    def _failed(self, name, reason):
        error = IsolationError(name, reason)
        self._verdicts[name] = error
        return error


class _IsolatingExtensionLoader:
    """Wraps an ExtensionFileLoader: a file in this worker's private copy is built here with
    CPython's check off (`_iso_create_here`); a shared file loads with the check enforced, and
    if CPython refuses it, that is an `IsolationError` naming what the judgement rested on."""

    def __init__(self, inner, finder, package):
        self._inner = inner
        self._finder = finder
        self._package = package

    def create_module(self, spec):
        private = self._is_private(spec.origin)
        prev = _override_check(-1 if private else 1)
        try:
            if private:
                mod = _iso_create_here(spec)
                return mod if mod is not None else self._inner.create_module(spec)
            try:
                return self._inner.create_module(spec)
            except ImportError as e:
                # CPython's own refusal, in its words (import.c); any other ImportError (a
                # missing library, say) is the library's to handle.
                if "does not support loading in subinterpreters" not in str(e):
                    raise
                raise IsolationError(
                    self._package, _SharedLoadRefused(spec.name, self._finder.shared_basis(self._package))
                ) from e
        finally:
            _restore_check(prev)

    def exec_module(self, module):
        prev = _override_check(-1 if self._is_private(getattr(module, "__file__", None)) else 1)
        try:
            self._inner.exec_module(module)
        finally:
            _restore_check(prev)

    def _is_private(self, path):
        wd = self._finder._worker_dir
        return bool(wd and path) and _os.path.realpath(path).startswith(_os.path.realpath(wd) + _os.sep)

    def __getattr__(self, attr):
        # get_filename / is_package / get_code / get_source / etc.
        return getattr(self._inner, attr)


class _SharedBuiltinLoader:
    """A built-in single-phase module (faulthandler, pulled in by
    sklearn -> joblib -> loky) has no file to clone, so it can only load
    shared, under the override. Known hazard: CPython runs its init in the main
    interpreter, so its objects are main's."""

    def __init__(self, inner):
        self._inner = inner

    def create_module(self, spec):
        prev = _override_check(-1)
        try:
            return self._inner.create_module(spec)
        finally:
            _restore_check(prev)

    def exec_module(self, module):
        prev = _override_check(-1)
        try:
            self._inner.exec_module(module)
        finally:
            _restore_check(prev)

    def __getattr__(self, attr):
        return getattr(self._inner, attr)
