"""Whether an extension file can load shared in an own-GIL sub-interpreter: CPython's own
multi-interpreter check, worked out before the load (design `docs/design/isolation-finder.md` §3.6).

CPython lets an extension load in a sub-interpreter with its own GIL only if it is multi-phase and its
`PyModuleDef` declares `Py_mod_multiple_interpreters = Py_MOD_PER_INTERPRETER_GIL_SUPPORTED`. A
single-phase extension never can; a def without the slot counts as `SUPPORTED`, which an own-GIL
interpreter refuses too.

The judgement, `per_interpreter_support`, is pure. Gathering the facts it judges is IO, with two ways
to read the slot:

- `facts_of_file(path)`, in a worker: the binary's symbol table, and for a multi-phase file one call of
  its `PyInit_*` (which only returns the def) to read the slot. `isolation.py` decides with it which
  packages a worker copies.
- `facts_of_loaded(path)`, on the main interpreter: the symbol table, and the slot from the def of the
  module already loaded from that file. It never loads a file or calls a `PyInit_*`: the main
  interpreter is the whole process's, and a check at route registration has no reason to run an
  extension's init there.

Run two ways: imported as `mipyre._worker.extension_support` on the main interpreter, and in a
`runtime="isolated"` worker by the engine, in the bootstrap's namespace before `isolation.py`, since
that runs before the `mipyre` package can be imported. So it imports only what a worker's bootstrap
already has (`logging` brings `dataclasses` and `enum`), and `ctypes` only where a slot is read.
"""

from __future__ import annotations

import enum
import logging
import os
import sys
from dataclasses import dataclass
from types import ModuleType

TYPE_CHECKING = False  # the bare-name form type checkers honour, so a worker imports no `typing`
if TYPE_CHECKING:
    from typing import BinaryIO

_PY_MOD_MULTIPLE_INTERPRETERS = 3  # the slot id, CPython's moduleobject.h


class MultipleInterpreters(enum.Enum):
    """What a multi-phase extension's def says in its `Py_mod_multiple_interpreters` slot."""

    ABSENT = "no Py_mod_multiple_interpreters slot"
    NOT_SUPPORTED = "Py_mod_multiple_interpreters = Py_MOD_MULTIPLE_INTERPRETERS_NOT_SUPPORTED"
    SUPPORTED = "Py_mod_multiple_interpreters = Py_MOD_MULTIPLE_INTERPRETERS_SUPPORTED"
    PER_INTERPRETER_GIL_SUPPORTED = "Py_mod_multiple_interpreters = Py_MOD_PER_INTERPRETER_GIL_SUPPORTED"


# The slot's values, as moduleobject.h defines them.
_SLOT_VALUES = {
    0: MultipleInterpreters.NOT_SUPPORTED,
    1: MultipleInterpreters.SUPPORTED,
    2: MultipleInterpreters.PER_INTERPRETER_GIL_SUPPORTED,
}


@dataclass(frozen=True)
class SinglePhase:
    """The file's init builds its module with `PyModule_Create2` (single-phase init)."""

    file: str

    def __str__(self) -> str:
        return f"{self.file} is single-phase (its init builds the module with PyModule_Create2)"


@dataclass(frozen=True)
class MultiPhase:
    """The file's init returns a def, and the def's `Py_mod_multiple_interpreters` slot says `slot`."""

    file: str
    slot: MultipleInterpreters

    def __str__(self) -> str:
        if self.slot is MultipleInterpreters.PER_INTERPRETER_GIL_SUPPORTED:
            return f"{self.file} declares per-interpreter GIL support"
        return f"{self.file} does not declare per-interpreter GIL support ({self.slot.value})"


@dataclass(frozen=True)
class MultiPhaseUnread:
    """The file is multi-phase by its symbol table, but no module is loaded from it in this process, so
    its slot was not read (only `facts_of_loaded` says this; it loads nothing to find out)."""

    file: str

    def __str__(self) -> str:
        return (
            f"{self.file} is multi-phase, but no module is loaded from it in this process, so its "
            "Py_mod_multiple_interpreters slot was not read"
        )


@dataclass(frozen=True)
class UnknownFormat:
    """The file is not a binary this module can read (not ELF64 little-endian or Mach-O 64, or cut short)."""

    file: str
    why: str

    def __str__(self) -> str:
        return f"{self.file} is not a binary mipyre can read ({self.why})"


@dataclass(frozen=True)
class PlainLibrary:
    """A native library a package ships, not a Python module (the file has no `PyInit_<name>`). CPython's
    check never sees it."""

    file: str

    def __str__(self) -> str:
        return f"{self.file} is not a Python module (no PyInit_{_short_name(self.file)})"


ExtensionFacts = SinglePhase | MultiPhase | MultiPhaseUnread | UnknownFormat | PlainLibrary


@dataclass(frozen=True)
class Supported:
    """CPython's check lets the file load shared in an own-GIL sub-interpreter."""

    facts: MultiPhase | PlainLibrary


@dataclass(frozen=True)
class Unsupported:
    """It does not, or it can't be told: `facts` is the reason."""

    facts: SinglePhase | MultiPhase | MultiPhaseUnread | UnknownFormat

    def __str__(self) -> str:
        return str(self.facts)


class ExtensionProbeError(RuntimeError):
    """Calling a file's `PyInit_*` to read its def failed: loading the module would fail the same way.
    The failure is the `__cause__`."""

    def __init__(self, file: str, symbol: str):
        super().__init__(f"calling {symbol} in {file} to read its module def failed")
        self.file = file
        self.symbol = symbol


def per_interpreter_support(facts: ExtensionFacts) -> Supported | Unsupported:
    """Whether CPython's multi-interpreter check lets the file these facts are about load shared in an
    own-GIL sub-interpreter."""
    if isinstance(facts, PlainLibrary):
        return Supported(facts)
    if isinstance(facts, MultiPhase) and facts.slot is MultipleInterpreters.PER_INTERPRETER_GIL_SUPPORTED:
        return Supported(facts)
    return Unsupported(facts)


# -- the facts: IO ----------------------------------------------------------------------------------


def static_facts(path: str) -> SinglePhase | MultiPhaseUnread | UnknownFormat | PlainLibrary:
    """What the file's symbol table says, without loading it. A multi-phase file is `MultiPhaseUnread`
    here: its slot is data, read by `probe_slot` or `read_loaded_slot`.

    A single-phase `PyInit_*` builds its module with `PyModule_Create2` (what the `PyModule_Create`
    macro calls); a multi-phase one only returns its def through `PyModuleDef_Init`.

    :raises OSError: the file can't be read (CPython could not load it either).
    """
    try:
        strtab = _dynamic_strtab(path)
    except _Unreadable as e:
        return UnknownFormat(path, e.why)
    if b"PyInit_" + _short_name(path).encode() + b"\x00" not in strtab:
        return PlainLibrary(path)
    if b"PyModule_Create2\x00" in strtab:
        return SinglePhase(path)
    return MultiPhaseUnread(path)


def probe_slot(facts: MultiPhaseUnread) -> MultiPhase | SinglePhase | UnknownFormat:
    """Read the slot by calling the file's `PyInit_*` in this interpreter, which for a multi-phase file
    only returns its def. For a worker only, never the main interpreter (see the module docstring).

    If the init returns a module instead, the symbol table was wrong about the file: it is single-phase,
    and its init has just run on the shared file. That is a bug in `static_facts`, said in a warning.

    :raises ExtensionProbeError: the init raised, or returned NULL.
    """
    import ctypes

    short = _short_name(facts.file)
    symbol = "PyInit_" + short
    lib = ctypes.PyDLL(facts.file, mode=sys.getdlopenflags())
    try:
        init = getattr(lib, symbol)
        # A raw pointer: a def is borrowed (PyO3's starts at refcount 1, so taking it as owned would
        # free static memory when dropped).
        init.restype = ctypes.c_void_p
        ptr = init()
    except Exception as e:  # the init's own exception, or no such symbol after all: the cause, kept
        raise ExtensionProbeError(facts.file, symbol) from e
    if not ptr:
        raise ExtensionProbeError(facts.file, symbol) from SystemError(f"{symbol} returned NULL without an exception")
    if _is_module_def(ptr):
        return _slot_of_def(facts.file, ptr)
    api = ctypes.pythonapi
    api.Py_DecRef.argtypes = [ctypes.c_void_p]
    api.Py_DecRef(ptr)  # the module the init built; this probe never uses it
    logging.getLogger("mipyre.isolate").warning(
        "%s: its symbol table names no PyModule_Create2, but %s returned a module, not a def: it is "
        "single-phase, and its init has run on the shared file. The symbol-table check is wrong for this file.",
        facts.file,
        symbol,
    )
    return SinglePhase(facts.file)


def read_loaded_slot(facts: MultiPhaseUnread) -> MultiPhase | UnknownFormat | MultiPhaseUnread:
    """Read the slot from the def of the module already loaded from the file in this interpreter
    (`facts` again when none is). Loads nothing and calls no `PyInit_*`."""
    module = _module_loaded_from(facts.file)
    if module is None:
        return facts
    import ctypes

    api = ctypes.pythonapi
    api.PyModule_GetDef.restype = ctypes.c_void_p
    api.PyModule_GetDef.argtypes = [ctypes.py_object]
    ptr = api.PyModule_GetDef(module)
    if not ptr:
        return UnknownFormat(facts.file, f"the module {module.__name__} loaded from it has no module def")
    return _slot_of_def(facts.file, ptr)


def facts_of_file(path: str) -> ExtensionFacts:
    """The file's facts, its slot read by calling its `PyInit_*` (`probe_slot`). In a worker only."""
    facts = static_facts(path)
    return probe_slot(facts) if isinstance(facts, MultiPhaseUnread) else facts


def facts_of_loaded(path: str) -> ExtensionFacts:
    """The file's facts, its slot read from the module already loaded from it (`read_loaded_slot`). Safe
    on the main interpreter."""
    facts = static_facts(path)
    return read_loaded_slot(facts) if isinstance(facts, MultiPhaseUnread) else facts


def _short_name(path: str) -> str:
    """The module name a file's `PyInit_` carries: its base name up to the first dot."""
    return os.path.basename(path).split(".", 1)[0]


def _module_loaded_from(path: str) -> ModuleType | None:
    """The module the file's `PyInit_*` made, if it is loaded. Matched by name as well as file: a
    pybind11 extension's submodules (`_core.cb`) carry the extension's `__file__` but no def."""
    real, short = os.path.realpath(path), _short_name(path)
    for module in list(sys.modules.values()):
        file = getattr(module, "__file__", None)
        if (
            isinstance(module, ModuleType)
            and module.__name__.rpartition(".")[2] == short
            and isinstance(file, str)
            and os.path.realpath(file) == real
        ):
            return module
    return None


def _is_module_def(ptr: int) -> bool:
    import ctypes

    api = ctypes.pythonapi
    api.PyObject_Type.restype = ctypes.c_void_p  # a new reference to the type; the object is untouched
    api.PyObject_Type.argtypes = [ctypes.c_void_p]
    api.Py_DecRef.argtypes = [ctypes.c_void_p]
    ob_type = api.PyObject_Type(ptr)
    api.Py_DecRef(ob_type)
    return bool(ob_type == ctypes.addressof(ctypes.c_char.in_dll(api, "PyModuleDef_Type")))


def _slot_of_def(file: str, ptr: int) -> MultiPhase | UnknownFormat:
    """The `Py_mod_multiple_interpreters` slot of the `PyModuleDef` at `ptr` (read, never owned)."""
    import ctypes

    class _Slot(ctypes.Structure):
        _fields_ = [("slot", ctypes.c_int), ("value", ctypes.c_void_p)]

    class _ModuleDef(ctypes.Structure):
        _fields_ = [
            ("ob_base", ctypes.c_byte * object.__basicsize__),  # PyObject_HEAD, as this build lays it out
            ("m_init", ctypes.c_void_p),
            ("m_index", ctypes.c_ssize_t),
            ("m_copy", ctypes.c_void_p),
            ("m_name", ctypes.c_char_p),
            ("m_doc", ctypes.c_char_p),
            ("m_size", ctypes.c_ssize_t),
            ("m_methods", ctypes.c_void_p),
            ("m_slots", ctypes.POINTER(_Slot)),
        ]

    slots = _ModuleDef.from_address(ptr).m_slots
    i = 0
    while slots and slots[i].slot != 0:
        if slots[i].slot == _PY_MOD_MULTIPLE_INTERPRETERS:
            value = slots[i].value or 0
            known = _SLOT_VALUES.get(value)
            if known is None:
                return UnknownFormat(file, f"Py_mod_multiple_interpreters is {value}, a value CPython does not know")
            return MultiPhase(file, known)
        i += 1
    return MultiPhase(file, MultipleInterpreters.ABSENT)


class _Unreadable(Exception):
    """The file is not a binary `_dynamic_strtab` can read; `why` says what it found."""

    def __init__(self, why: str):
        super().__init__(why)
        self.why = why


def _read(f: BinaryIO, n: int, what: str) -> bytes:
    data = f.read(n)
    if len(data) != n:
        raise _Unreadable(f"cut short in its {what}")
    return data


def _dynamic_strtab(path: str) -> bytes:
    """The string table of the file's dynamic symbols (ELF `.dynstr`, Mach-O `LC_SYMTAB` strings): the
    names of the symbols it imports and exports, a small slice of a file that can be hundreds of MB.

    Imports nothing: in a worker it runs while a package is being resolved, which can be in the middle
    of importing `struct`.

    :raises _Unreadable: not ELF64 little-endian or Mach-O 64, or cut short.
    :raises OSError: the file can't be read.
    """

    def le(b: bytes) -> int:
        return int.from_bytes(b, "little")

    def be(b: bytes) -> int:
        return int.from_bytes(b, "big")

    with open(path, "rb") as f:
        head = f.read(64)
        base = 0
        if head[:4] == b"\xca\xfe\xba\xbe":  # universal Mach-O: this machine's slice
            machine = os.uname().machine
            want = {"arm64": 0x0100000C, "x86_64": 0x01000007}.get(machine)
            f.seek(8)
            for _ in range(be(head[4:8])):
                arch = _read(f, 20, "fat header")  # cputype, cpusubtype, offset, size, align
                if be(arch[0:4]) == want:
                    base = be(arch[8:12])
                    break
            else:
                raise _Unreadable(f"a universal binary without a slice for {machine}")
            f.seek(base)
            head = f.read(64)
        if head[:4] == b"\xcf\xfa\xed\xfe":  # Mach-O 64, little-endian
            if len(head) < 32:
                raise _Unreadable("cut short in its Mach-O header")
            f.seek(base + 32)
            for _ in range(le(head[16:20])):  # ncmds
                cmd = _read(f, 8, "load commands")
                size = le(cmd[4:8])
                if size < 8:
                    raise _Unreadable(f"a load command of {size} bytes, less than its own 8-byte header")
                body = _read(f, size - 8, "load commands")
                if le(cmd[0:4]) == 0x2:  # LC_SYMTAB: symoff, nsyms, stroff, strsize
                    if len(body) < 16:
                        raise _Unreadable("an LC_SYMTAB shorter than its fields")
                    f.seek(base + le(body[8:12]))
                    return _read(f, le(body[12:16]), "string table")
            raise _Unreadable("a Mach-O file without LC_SYMTAB")
        if head[:4] == b"\x7fELF":
            if len(head) < 64 or head[4] != 2 or head[5] != 1:
                raise _Unreadable("an ELF file that is not 64-bit little-endian")
            shoff, shentsize, shnum = le(head[0x28:0x30]), le(head[0x3A:0x3C]), le(head[0x3C:0x3E])
            if not shoff or not shnum or shentsize < 44:
                raise _Unreadable("an ELF file without section headers")
            f.seek(shoff)
            sections = []  # (sh_type, sh_offset, sh_size, sh_link)
            for _ in range(shnum):
                sh = _read(f, shentsize, "section headers")
                sections.append((le(sh[4:8]), le(sh[24:32]), le(sh[32:40]), le(sh[40:44])))
            for sh_type, _off, _size, link in sections:
                if sh_type == 11:  # SHT_DYNSYM; sh_link = its string table
                    if link >= len(sections):
                        raise _Unreadable(f"a .dynsym linked to section {link} of {len(sections)}")
                    _t, off, size, _l = sections[link]
                    f.seek(off)
                    return _read(f, size, "string table")
            raise _Unreadable("an ELF file without .dynsym")
    raise _Unreadable(f"not ELF or Mach-O (it starts with {head[:4]!r})")
