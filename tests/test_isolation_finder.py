"""The `runtime="isolated"` finder (`python/mipyre/_isolated/isolation.py`, design `docs/design/isolation-finder.md`),
driven directly: the package verdict, when it reads what, where the copy comes from, and what a failure says.

On the main interpreter, so only up to the spec a worker would load: loading itself (CPython's check switched per
file) can only happen in a sub-interpreter, which `test_isolate.py` runs end to end."""

from __future__ import annotations

import importlib.machinery
import os
import sys
import threading
from pathlib import Path

import pytest

from tests._ext_binaries import elf, macho
from tests._helpers import isolation_adapter_ns

_SUFFIX = importlib.machinery.EXTENSION_SUFFIXES[0]
_binary = macho if sys.platform == "darwin" else elf


def _ext(directory: Path, name: str, *, single: bool) -> Path:
    """An extension file `name` the symbol-table reader takes for single- or multi-phase (it is never loaded)."""
    strtab = b"\x00_PyInit_" + name.encode() + b"\x00" + (b"_PyModule_Create2\x00" if single else b"_PyModuleDef_Init\x00")
    path = directory / f"{name}{_SUFFIX}"
    path.write_bytes(_binary(strtab))
    return path


def _package(site: Path, name: str, **exts: bool) -> Path:
    pkg = site / name
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    for ext, single in exts.items():
        _ext(pkg, ext, single=single)
    return pkg


@pytest.fixture
def adapter(tmp_path, monkeypatch):
    """(the adapter's namespace, its finder, first on sys.meta_path, a site dir on sys.path) with the clone root
    under tmp_path."""
    site = tmp_path / "site"
    site.mkdir()
    monkeypatch.syspath_prepend(str(site))
    ns = isolation_adapter_ns(tmp_path / "copies")
    finder = ns["_IsolatingFinder"]()
    monkeypatch.setattr(sys, "meta_path", [finder, *sys.meta_path])
    return ns, finder, site


_F = "/site/pkg/a.so"
_G = "/site/pkg/b.so"


@pytest.mark.parametrize(
    ("facts", "copy_for"),
    [
        (lambda ns: [], None),
        (lambda ns: [ns["MultiPhase"](_G, ns["MultipleInterpreters"].PER_INTERPRETER_GIL_SUPPORTED), ns["PlainLibrary"](_F)], None),
        (lambda ns: [ns["MultiPhase"](_F, ns["MultipleInterpreters"].SUPPORTED), ns["PlainLibrary"](_G)], 0),
        (lambda ns: [ns["PlainLibrary"](_G), ns["MultiPhase"](_F, ns["MultipleInterpreters"].ABSENT)], 1),
        (lambda ns: [ns["PlainLibrary"](_G), ns["MultiPhase"](_F, ns["MultipleInterpreters"].NOT_SUPPORTED)], 1),
        (lambda ns: [ns["PlainLibrary"](_G), ns["PlainLibrary"](_F), ns["SinglePhase"](_F)], 2),
        (lambda ns: [ns["UnknownFormat"](_F, "cut short"), ns["SinglePhase"](_G)], 0),
    ],
    ids=["none", "per-gil-and-plain", "supported", "absent", "not-supported", "single-last", "unknown-first"],
)
def test_a_package_is_copied_for_the_first_file_that_cannot_load_shared(facts, copy_for):
    """The adapter's own copies of the fact types: in a worker it holds them in its bootstrap namespace."""
    ns = isolation_adapter_ns()
    given = facts(ns)
    want = ns["_Shared"](tuple(given)) if copy_for is None else ns["_Copy"](ns["Unsupported"](given[copy_for]))
    assert ns["_verdict"](given) == want


def test_one_single_phase_file_settles_it_without_calling_any_init(adapter, monkeypatch):
    """The multi-phase file is a fake: probing it would dlopen garbage and fail. It is never probed."""
    ns, finder, site = adapter
    _package(site, "mixedpkg", a_multi=False, z_single=True)
    probed = []
    monkeypatch.setitem(ns, "probe_slot", probed.append)
    spec = importlib.machinery.PathFinder.find_spec("mixedpkg")
    verdict = finder._decide("mixedpkg", spec)
    assert verdict == ns["_Copy"](ns["Unsupported"](ns["SinglePhase"](str(site / "mixedpkg" / f"z_single{_SUFFIX}"))))
    assert probed == []


def test_all_multi_phase_files_are_probed_in_path_order(adapter, monkeypatch):
    ns, finder, site = adapter
    pkg = _package(site, "multipkg", b=False, a=False)
    probed = []

    def probe(facts):
        probed.append(facts)
        return ns["MultiPhase"](facts.file, ns["MultipleInterpreters"].PER_INTERPRETER_GIL_SUPPORTED)

    monkeypatch.setitem(ns, "probe_slot", probe)
    verdict = finder._decide("multipkg", importlib.machinery.PathFinder.find_spec("multipkg"))
    unread = ns["MultiPhaseUnread"]
    assert probed == [unread(str(pkg / f"a{_SUFFIX}")), unread(str(pkg / f"b{_SUFFIX}"))]
    assert isinstance(verdict, ns["_Shared"])


def test_a_package_judged_a_copy_resolves_to_this_workers_copy_never_on_sys_path(adapter):
    ns, finder, site = adapter
    _package(site, "copiedpkg", _ext=True)
    spec = finder.find_spec("copiedpkg", None)
    assert spec.origin.startswith(finder._worker_dir + os.sep), spec.origin
    assert finder._worker_dir not in sys.path


def test_an_old_copy_in_the_clone_dir_never_shadows_a_shared_package(adapter, tmp_path):
    """Earlier runs leave copies in the bucket. A package judged shared loads from its install, whatever is there."""
    ns, finder, site = adapter
    _package(site, "stalepkg")
    finder._claimed()
    stale = Path(finder._worker_dir) / "stalepkg"
    stale.mkdir()
    (stale / "__init__.py").write_text("STALE = True\n")
    spec = finder.find_spec("stalepkg", None)
    assert spec.origin == str(site / "stalepkg" / "__init__.py")


def test_a_namespace_package_in_two_dirs_that_needs_a_copy_says_why_it_cannot_have_one(adapter, tmp_path, monkeypatch):
    ns, finder, _site = adapter
    for d in ("one", "two"):
        (tmp_path / d / "nspkg").mkdir(parents=True)
        monkeypatch.syspath_prepend(str(tmp_path / d))
    _ext(tmp_path / "two" / "nspkg", "_core", single=True)
    with pytest.raises(ns["IsolationError"]) as raised:
        finder.find_spec("nspkg", None)
    assert not isinstance(raised.value, ImportError)
    said = str(raised.value)
    assert "runtime='isolated' could not load nspkg" in said and "a namespace package in 2 directories" in said
    assert "_core" in said and "is single-phase" in said and "runtime='process'" in said


def test_a_file_that_cannot_be_read_fails_with_its_path(adapter):
    """An extension file that is a dangling link: listed in the package, and unreadable for any user."""
    ns, finder, site = adapter
    pkg = _package(site, "lockedpkg")
    broken = pkg / f"_ext{_SUFFIX}"
    broken.symlink_to(pkg / "gone")
    with pytest.raises(ns["IsolationError"], match=f"reading its extension file {broken} failed") as raised:
        finder.find_spec("lockedpkg", None)
    assert isinstance(raised.value.__cause__, FileNotFoundError)


def test_a_copy_that_cannot_be_made_is_tried_once_and_said_again_on_every_import(adapter, tmp_path, monkeypatch):
    """Case A: an isolate dir other users can write. The failure names the dir, the fix, and why a copy was needed;
    the next import raises the same words without trying again."""
    ns, finder, site = adapter
    shared = tmp_path / "copies"
    shared.mkdir(mode=0o777)
    shared.chmod(0o777)
    _package(site, "needscopy", _ext=True)
    claims = []
    real_claim = ns["_iso_worker_dir"]
    monkeypatch.setitem(ns, "_iso_worker_dir", lambda: claims.append(1) or real_claim())
    said = []
    for _ in range(2):
        with pytest.raises(ns["IsolationError"]) as raised:
            finder.find_spec("needscopy", None)
        said.append(str(raised.value))
    assert claims == [1]
    assert said[0] == said[1]
    assert "writable by other users" in said[0] and "set MIPYRE_ISOLATE_DIR to a directory you own" in said[0]
    assert "it needs a per-worker copy" in said[0] and "is single-phase" in said[0]


def test_threads_making_their_first_copies_at_once_share_one_clone_dir(adapter):
    ns, finder, _site = adapter
    start = threading.Barrier(8)
    got = []

    def claim():
        start.wait()
        got.append(finder._claimed()[0])

    threads = [threading.Thread(target=claim) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(got)) == 1 and len(got) == 8
    assert sorted(p.name for p in Path(got[0]).parent.iterdir() if not p.name.endswith(".lock")) == ["w0"]


def test_mipyre_and_the_standard_library_are_left_to_cpython(adapter):
    ns, finder, _site = adapter
    assert finder.find_spec("mipyre", None).loader.__class__.__name__ != "_IsolatingExtensionLoader"
    assert finder.find_spec("json", None).origin.endswith(os.path.join("json", "__init__.py"))
    assert finder._verdicts == {}
