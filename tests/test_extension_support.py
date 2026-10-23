"""`mipyre._worker.extension_support`: CPython's multi-interpreter check, worked out before a load.

The judgement is fed its facts directly; the binary reader is fed binaries built here; the two ways of reading
the slot are checked against each other and against the def CPython itself holds."""

from __future__ import annotations

import ctypes
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from hypothesis import example, given
from hypothesis import strategies as st

from mipyre._worker import extension_support as es
from tests._ext_binaries import elf as _elf
from tests._ext_binaries import macho as _macho
from mipyre._worker.extension_support import (
    MultipleInterpreters,
    MultiPhase,
    MultiPhaseUnread,
    PlainLibrary,
    SinglePhase,
    Supported,
    UnknownFormat,
    Unsupported,
)

_F = "/site-packages/pkg/_ext.cpython-314-darwin.so"


@pytest.mark.parametrize(
    ("facts", "supported"),
    [
        (MultiPhase(_F, MultipleInterpreters.PER_INTERPRETER_GIL_SUPPORTED), True),
        (PlainLibrary(_F), True),
        (MultiPhase(_F, MultipleInterpreters.SUPPORTED), False),
        (MultiPhase(_F, MultipleInterpreters.NOT_SUPPORTED), False),
        (MultiPhase(_F, MultipleInterpreters.ABSENT), False),
        (SinglePhase(_F), False),
        (UnknownFormat(_F, "cut short in its string table"), False),
        (MultiPhaseUnread(_F), False),
    ],
)
def test_only_a_declared_per_interpreter_gil_or_a_plain_library_loads_shared(facts, supported):
    verdict = es.per_interpreter_support(facts)
    assert verdict == (Supported(facts) if supported else Unsupported(facts))
    if not supported:
        assert _F in str(verdict)


def test_the_reason_says_what_the_slot_says():
    absent = str(Unsupported(MultiPhase(_F, MultipleInterpreters.ABSENT)))
    assert absent == f"{_F} does not declare per-interpreter GIL support (no Py_mod_multiple_interpreters slot)"


# -- the binary reader, fed binaries built here --------------------------------------------------------------


_SINGLE = b"\x00_PyInit__ext\x00_PyModule_Create2\x00"
_MULTI = b"\x00_PyInit__ext\x00_PyModuleDef_Init\x00"
_HELPER = b"\x00_cblas_dgemm\x00_PyModule_Create2\x00"


@pytest.mark.parametrize("binary", [_elf, _macho], ids=["elf", "macho"])
@pytest.mark.parametrize(
    ("strtab", "facts"),
    [(_SINGLE, SinglePhase), (_MULTI, MultiPhaseUnread), (_HELPER, PlainLibrary)],
    ids=["single-phase", "multi-phase", "plain-library"],
)
def test_the_symbol_table_says_single_phase_multi_phase_or_plain_library(tmp_path, binary, strtab, facts):
    path = tmp_path / "_ext.cpython-314-darwin.so"
    path.write_bytes(binary(strtab))
    assert es.static_facts(str(path)) == facts(str(path))


def test_a_load_command_shorter_than_its_header_is_an_unknown_format(tmp_path):
    path = tmp_path / "_ext.so"
    path.write_bytes(_macho(_MULTI, cmdsize=4) + bytes(1 << 16))
    facts = es.static_facts(str(path))
    assert facts == UnknownFormat(str(path), "a load command of 4 bytes, less than its own 8-byte header")


def test_a_string_table_past_the_end_is_an_unknown_format(tmp_path):
    path = tmp_path / "_ext.so"
    path.write_bytes(_elf(_MULTI)[:-200])
    assert isinstance(es.static_facts(str(path)), UnknownFormat)


def test_a_file_that_is_no_binary_is_an_unknown_format(tmp_path):
    path = tmp_path / "_ext.so"
    path.write_bytes(b"not a binary")
    assert es.static_facts(str(path)) == UnknownFormat(str(path), "not ELF or Mach-O (it starts with b'not ')")


def test_a_missing_file_is_an_error_naming_it(tmp_path):
    with pytest.raises(FileNotFoundError) as raised:
        es.static_facts(str(tmp_path / "missing.so"))
    assert raised.value.filename == str(tmp_path / "missing.so")


_FAT = b"\xca\xfe\xba\xbe"


@given(
    st.binary(max_size=256)
    | st.builds(
        lambda magic, rest: magic + rest,
        st.sampled_from([b"\x7fELF\x02\x01", b"\xcf\xfa\xed\xfe", _FAT]),
        st.binary(max_size=256),
    )
)
@example(_FAT + b"\xff\xff\xff\xff" + bytes(56))  # b/892: 2**32 - 1 slices claimed, none there
def test_any_file_is_read_as_facts_and_never_runs_past_its_end(content):
    """Whatever the bytes, the string-table reader returns a table or refuses the file with the one error its caller
    turns into a fact (#1298's promise for the reader it replaced), and the facts come back as one of their kinds,
    bounded by what the file holds: a header that claims more than the file has is an unknown format, not a loop over
    what it claims (b/892)."""
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "_ext.so")
        with open(path, "wb") as f:
            f.write(content)
        try:
            table = es._dynamic_strtab(path)
        except es._Unreadable:
            table = None
        assert table is None or isinstance(table, bytes)
        assert isinstance(es.static_facts(path), (SinglePhase, MultiPhaseUnread, PlainLibrary, UnknownFormat))


# -- the slot, read both ways ----------------------------------------------------------------------------------

_BOTH_WAYS = """
import json, sys
from mipyre._worker import extension_support as es

def said(facts):
    return [type(facts).__name__, getattr(getattr(facts, "slot", None), "name", None)]

path = sys.argv[1]
unloaded = said(es.facts_of_loaded(path))
probed = said(es.facts_of_file(path))
import pyo3_mp
assert pyo3_mp.ping() == 7
print(json.dumps({"unloaded": unloaded, "probed": probed, "loaded": said(es.facts_of_loaded(path))}))
"""


@pytest.mark.parametrize(
    ("variant", "slot"),
    [("declared", "PER_INTERPRETER_GIL_SUPPORTED"), ("undeclared", "ABSENT")],
)
def test_the_slot_read_by_probing_matches_the_loaded_module(pyo3_fixture_dirs, variant, slot):
    """Each in its own process: both variants are the module `pyo3_mp`. Not loaded, the main-interpreter reader
    says the slot was not read (it loads nothing to find out); probed and loaded, both readers agree."""
    module_dir = pyo3_fixture_dirs[variant]
    (path,) = Path(module_dir).iterdir()
    env = dict(os.environ, PYTHONPATH=str(module_dir) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    done = subprocess.run([sys.executable, "-c", _BOTH_WAYS, str(path)], env=env, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr[-3000:]
    assert json.loads(done.stdout) == {
        "unloaded": ["MultiPhaseUnread", None],
        "probed": ["MultiPhase", slot],
        "loaded": ["MultiPhase", slot],
    }


def test_the_symbol_table_matches_the_def_cpython_holds():
    """For every extension module numpy and scipy load, the facts read on the main interpreter say single-phase
    exactly when CPython's def for the module has no slots (a single-phase def is built without them). scipy is a
    test dependency (pyproject.toml), so it is imported, never skipped."""
    import importlib

    for m in ("numpy", "scipy.linalg", "scipy.optimize"):
        importlib.import_module(m)
    api = ctypes.pythonapi
    api.PyModule_GetDef.restype = ctypes.c_void_p
    api.PyModule_GetDef.argtypes = [ctypes.py_object]
    slots_at = ctypes.sizeof(ctypes.c_ssize_t) + 4 * ctypes.sizeof(ctypes.c_void_p) + 4 * ctypes.sizeof(ctypes.c_void_p)
    seen_single, wrong = 0, []
    for name, mod in list(sys.modules.items()):
        f = getattr(mod, "__file__", "") or ""
        d = api.PyModule_GetDef(mod) if f.endswith(".so") else None
        if not d:
            continue
        single = not ctypes.c_void_p.from_address(d + slots_at).value
        seen_single += single
        facts = es.facts_of_loaded(f)
        if not isinstance(facts, (SinglePhase, MultiPhase)) or isinstance(facts, SinglePhase) != single:
            wrong.append((name, single, facts))
    assert seen_single, "expected scipy to load single-phase modules (f2py _fblas)"
    assert not wrong, f"facts disagree with CPython's def (name, def is single-phase, facts): {wrong}"
