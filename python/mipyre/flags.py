"""Command-line flags an app declares where it uses them, parsed once and seen the same in every
interpreter that runs the app.

Declare a flag at module level and read its ``.value``::

    from mipyre import flags

    DSN = flags.string("db_dsn", None, "Postgres DSN")

    @app.get("/")
    def index(req):
        return {"dsn": DSN.value}

``mipyre run app:app --db-dsn x`` parses the command line once, on the main interpreter, after
the app is imported; each sub-interpreter and each process worker gets the same values (and the
same ``sys.argv``) and never parses again. A script run with ``python app.py`` calls
:func:`parse` itself before it reads a flag. A test never parses: a server it starts
(``TestClient``, ``TestServer``) gives every flag its declared default, as an empty command line
would (``sys.argv`` is the test runner's and is never read), and :func:`override` sets other
values.

On the command line a flag is written with dashes (``db_dsn`` is ``--db-dsn``); a boolean flag
is ``--x`` / ``--no-x``, a list ``--x a,b``. What follows the first ``--`` is the app's own,
which mipyre does not read.
"""

from __future__ import annotations

import builtins
import contextlib
import enum as _enum
import inspect
import math
import os
import sys
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from mipyre.engine import CommandLine, DeclarationError

if TYPE_CHECKING:
    import argparse

T = TypeVar("T")
_F = TypeVar("_F", bound=Callable[..., Any])

_I64_MIN, _I64_MAX = -(2**63), 2**63 - 1


class FlagError(DeclarationError):
    """A flag is declared, parsed or overridden in a way that cannot work; the app's code (or
    the test's) has to change."""


class DuplicateFlagError(FlagError):
    """Two declarations take the same flag name: two of the app's (other than the same line
    executed twice, as for a script imported both as ``__main__`` and by its name), or one of
    the app's and one of ``mipyre run``'s own options. The message names both lines."""


class UnparsedFlagError(FlagError):
    """A flag's value was read before any command line was parsed: under ``mipyre run`` it is
    parsed for the app; a script calls :func:`parse` first; a test reads it while its server
    runs (its default) or sets it with :func:`override`."""


class FlagMismatchError(FlagError):
    """A worker declared a flag that main's command line does not have as declared there (a
    module only a worker imports, another default), or main declared one after its command
    line was parsed."""


class FlagOverrideError(FlagError):
    """An override that cannot take effect: an undeclared name, a value of the wrong kind, an
    override inside a worker, or one entered or left while a server holds the values."""


class _RunKind(_enum.Enum):
    """Who started a server, as the flags see it when nothing parsed a command line."""

    #: ``app.run()`` (a script that never called :func:`parse`): a flag no override sets stays
    #: unread, and reading it raises :class:`UnparsedFlagError`.
    SCRIPT = _enum.auto()
    #: ``TestClient`` / ``TestServer``: every flag no override sets is its declared default.
    TEST = _enum.auto()


class _Kind(_enum.Enum):
    """What a flag holds; the value is the word the engine's ``CommandLine`` takes."""

    STR = "str"
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"
    CHOICE = "choice"
    STRS = "strs"


@dataclass(frozen=True)
class _Site:
    """The line that declared a flag (or added one of mipyre's own options)."""

    file: str
    line: int

    def __str__(self) -> str:
        return f"{self.file}:{self.line}"


@dataclass(frozen=True)
class _Declaration:
    """One flag as its module declared it. ``default`` is held as it is stored: a float flag's
    as ``float``, a list flag's as a tuple."""

    name: str
    kind: _Kind
    choices: tuple[str, ...] | None
    default: object
    help: str
    site: _Site

    @property
    def option(self) -> str:
        return "--" + self.name.replace("_", "-")

    def claims(self) -> frozenset[str]:
        """Return the names this flag takes on the command line, with underscores (a boolean
        flag also takes ``no_<name>``)."""
        return frozenset({self.name, f"no_{self.name}"} if self.kind is _Kind.BOOL else {self.name})

    def shape(self) -> tuple[str, list[str] | None, object]:
        """Return what a worker's declaration must match: the kind, the choices, the default, as
        the engine's ``CommandLine.flag`` gives them."""
        return self.kind.value, None if self.choices is None else list(self.choices), _public(self.default)

    def is_redeclared_by(self, other: _Declaration) -> bool:
        """Return whether ``other`` is this declaration executed again: the same name, shape
        and source file."""
        return (
            self.name == other.name
            and self.shape() == other.shape()
            and os.path.realpath(self.site.file) == os.path.realpath(other.site.file)
        )

    def argparse_options(self) -> dict[str, Any]:
        """Return the keyword arguments of ``add_argument`` for this flag."""
        options: dict[str, Any] = {
            "dest": self.name,
            "default": self.default,
            "help": f"{self.help} (default: {_public(self.default)!r})",
        }
        match self.kind:
            case _Kind.STR:
                options["type"] = str
            case _Kind.INT:
                options["type"] = _integer
            case _Kind.FLOAT:
                options["type"] = _finite_float
            case _Kind.BOOL:
                import argparse

                options["action"] = argparse.BooleanOptionalAction
            case _Kind.CHOICE:
                options["choices"] = self.choices
            case _Kind.STRS:
                options["type"] = _comma_list
                options["metavar"] = "A,B,..."
        return options


def _integer(text: str) -> int:
    import argparse

    try:
        number = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid integer value: {text!r}") from None
    if not _I64_MIN <= number <= _I64_MAX:
        raise argparse.ArgumentTypeError(f"{text} is past the 64-bit integers a flag holds")
    return number


def _finite_float(text: str) -> builtins.float:
    import argparse

    try:
        number = builtins.float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid float value: {text!r}") from None
    if not math.isfinite(number):
        raise argparse.ArgumentTypeError(f"{text} is not a finite number")
    return number


def _comma_list(text: str) -> tuple[str, ...]:
    return tuple(text.split(",")) if text else ()


def _misfit(kind: _Kind, choices: tuple[str, ...] | None, value: object) -> str | None:
    """Return why ``value`` is no value of a ``kind`` flag, or ``None`` when it is one."""
    if value is None:
        return "None is no bool: a boolean flag is True or False" if kind is _Kind.BOOL else None
    match kind:
        case _Kind.STR:
            fits, noun = isinstance(value, str), "str"
        case _Kind.INT:
            fits = isinstance(value, int) and not isinstance(value, bool) and _I64_MIN <= value <= _I64_MAX
            noun = "64-bit int"
        case _Kind.FLOAT:
            fits = isinstance(value, (int, builtins.float)) and not isinstance(value, bool) and math.isfinite(value)
            noun = "finite float"
        case _Kind.BOOL:
            fits, noun = isinstance(value, bool), "bool"
        case _Kind.CHOICE:
            fits, noun = isinstance(value, str) and value in (choices or ()), f"one of {list(choices or ())}"
        case _Kind.STRS:
            fits = (
                isinstance(value, Sequence)
                and not isinstance(value, (str, bytes))
                and all(isinstance(s, str) for s in value)
            )
            noun = "sequence of str"
    return None if fits else f"{value!r} is no {noun}"


def _stored(kind: _Kind, value: object) -> object:
    """Return ``value`` (which fits ``kind``) as a flag holds it."""
    if kind is _Kind.FLOAT and isinstance(value, (int, builtins.float)):
        return builtins.float(value)
    if kind is _Kind.STRS and isinstance(value, Sequence):
        return tuple(value)
    return value


def _public(value: object) -> object:
    """Return a held value as ``.value`` gives it: a list flag's as a new list."""
    return list(value) if isinstance(value, tuple) else value


def _split_command_line(argv: Sequence[str]) -> tuple[list[str], list[str]]:
    """Split a command line at its first ``--``: the flags' part before it, the app's after it.
    A later ``--`` is the app's."""
    if "--" not in argv:
        return list(argv), []
    at = list(argv).index("--")
    return list(argv[:at]), list(argv[at + 1 :])


class _Unset(_enum.Enum):
    """A holder with no value yet: nothing was parsed or installed."""

    UNSET = _enum.auto()


class FlagHolder(Generic[T]):
    """One declared flag. Read its value with ``.value``."""

    __slots__ = ("_registry", "_declaration", "_value")

    def __init__(self, registry: _Registry, declaration: _Declaration) -> None:
        self._registry = registry
        self._declaration = declaration
        self._value: object = _Unset.UNSET

    @property
    def name(self) -> str:
        return self._declaration.name

    @property
    def value(self) -> T:
        """The flag's value: an override in effect, else the command line's, else (nothing
        parsed, a test's server running) the declared default. Otherwise
        :class:`UnparsedFlagError`."""
        for layer in reversed(self._registry.overrides):
            if self.name in layer:
                return cast(T, _public(layer[self.name]))
        if self._value is not _Unset.UNSET:
            return cast(T, _public(self._value))
        if self._registry.test_leases:
            return cast(T, _public(self._declaration.default))
        raise UnparsedFlagError(
            f"flag {self._declaration.option} (declared at {self._declaration.site}) was read before the "
            "command line was parsed: `mipyre run` parses it after it imports the app, a script calls "
            "mipyre.flags.parse() before it reads a flag, a test reads it while its server (TestClient) "
            f"runs or sets it with mipyre.flags.override({self.name}=...)"
        )

    def __repr__(self) -> str:
        return f"<FlagHolder {self._declaration.option} declared at {self._declaration.site}>"


@dataclass(frozen=True)
class _Unparsed:
    """No command line was parsed here yet (a test process, a script before ``parse()``)."""


@dataclass(frozen=True)
class _Parsing:
    """The entry split the command line and is importing the app: each declaration takes its
    value from ``rest`` as it is made. ``reserved`` is the entry's own options and their lines."""

    rest: tuple[str, ...]
    prog: str
    reserved: Mapping[str, _Site]


@dataclass(frozen=True)
class _Parsed:
    """The command line was parsed and sealed: ``argv`` is the app's ``sys.argv``."""

    argv: tuple[str, ...]
    reserved: Mapping[str, _Site]


@dataclass(frozen=True)
class _Installed:
    """A worker: its values are the copy of main's ``command_line``."""

    command_line: CommandLine


class _Registry:
    """This interpreter's flags: the declarations, where parsing stands, the overrides a test
    entered and the servers holding a copy of the values."""

    def __init__(self) -> None:
        self._holders: dict[str, FlagHolder] = {}
        self._state: _Unparsed | _Parsing | _Parsed | _Installed = _Unparsed()
        # A tuple replaced whole, so `.value` reads it without the lock.
        self.overrides: tuple[Mapping[str, object], ...] = ()
        self._leases = 0
        # Of `_leases`, those of a test's servers while nothing is parsed: meanwhile a holder
        # with no value reads as its default, as the servers' workers do.
        self.test_leases = 0
        self._lock = threading.Lock()

    def declare(self, declaration: _Declaration) -> FlagHolder:
        """Return the holder of ``declaration``, valued as where parsing stands allows."""
        for holder in self._holders.values():
            if not holder._declaration.claims() & declaration.claims():
                continue
            if holder._declaration.is_redeclared_by(declaration):
                return holder
            raise DuplicateFlagError(
                f"flag {declaration.option} declared at {declaration.site} takes a name flag "
                f"{holder._declaration.option} declared at {holder._declaration.site} already takes"
            )
        _check_reserved(declaration, self._reserved())
        holder = FlagHolder(self, declaration)
        holder._value = self._value_of(declaration)
        self._holders[declaration.name] = holder
        return holder

    def declared_names(self) -> list[str]:
        return list(self._holders)

    def _reserved(self) -> Mapping[str, _Site]:
        state = self._state
        return state.reserved if isinstance(state, (_Parsing, _Parsed)) else {}

    def _value_of(self, declaration: _Declaration) -> object:
        match self._state:
            case _Unparsed():
                return _Unset.UNSET
            case _Parsing(rest=rest, prog=prog):
                return _parse_one(declaration, rest, prog)
            case _Parsed():
                raise FlagMismatchError(
                    f"flag {declaration.option} declared at {declaration.site} after the command line was "
                    "parsed, so it can never be given: declare it at the top of a module the app imports "
                    "before it serves, not in a handler or a function"
                )
            case _Installed(command_line=command_line):
                return _installed_value(declaration, command_line)

    def begin(self, rest: Sequence[str], prog: str, reserved: Mapping[str, _Site]) -> None:
        """Start parsing: the flags declared from now on, and those declared so far, take their
        values from ``rest``. A bad value exits as argparse does (status 2)."""
        if not isinstance(self._state, _Unparsed):
            raise FlagError(f"the command line is parsed once per process, and it already was ({self._state})")
        for holder in self._holders.values():
            _check_reserved(holder._declaration, reserved)
        self._state = _Parsing(tuple(rest), prog, reserved)
        for holder in self._holders.values():
            holder._value = _parse_one(holder._declaration, self._state.rest, prog)

    def seal(self, parser: argparse.ArgumentParser, args: Sequence[str], argv: Sequence[str]) -> None:
        """Parse ``args`` once more with ``parser`` and every declared flag: an argument that is
        neither refuses the command line (status 2), ``--help`` lists both (status 0). From
        here a flag declared on this interpreter is too late; ``argv`` is the app's ``sys.argv``."""
        state = self._state
        if not isinstance(state, _Parsing):
            raise FlagError(f"the command line is sealed once, after it begins; it is {state}")
        group = parser.add_argument_group("flags the app declares")
        for holder in self._holders.values():
            group.add_argument(holder._declaration.option, **holder._declaration.argparse_options())
        parsed, unknown = parser.parse_known_args(list(args))
        if unknown:
            import shlex

            parser.error(
                f"unrecognized arguments: {shlex.join(unknown)}\n"
                "They are neither options of this command nor flags the app declares; arguments the app "
                "parses itself go after `--`."
            )
        for holder in self._holders.values():
            final = getattr(parsed, holder.name)
            if final != holder._value:
                raise AssertionError(
                    f"flag {holder._declaration.option} took {holder._value!r} when declared, and {final!r} when "
                    f"the command line {list(args)} was sealed"
                )
        self._state = _Parsed(tuple(argv), state.reserved)

    def install(self, command_line: CommandLine) -> None:
        """Make this interpreter a worker of a server whose values are ``command_line``: every
        flag declared here, now or later, is checked against it and takes its value."""
        with self._lock:
            self._state = _Installed(command_line)
            self.overrides = ()
            self._leases = 0
            self.test_leases = 0
        for holder in self._holders.values():
            holder._value = _installed_value(holder._declaration, command_line)

    @contextlib.contextmanager
    def lease(self, run_kind: _RunKind) -> Iterator[CommandLine]:
        """Hold the values the workers of a server ``run_kind`` started get, for as long as the
        server runs: overrides cannot change them meanwhile."""
        with self._lock:
            command_line = self._command_line(run_kind)
            defaulted = run_kind is _RunKind.TEST and isinstance(self._state, _Unparsed)
            self._leases += 1
            self.test_leases += int(defaulted)
        try:
            yield command_line
        finally:
            with self._lock:
                self._leases -= 1
                self.test_leases -= int(defaulted)

    def _command_line(self, run_kind: _RunKind) -> CommandLine:
        state = self._state
        layered: dict[str, object] = {}
        for layer in self.overrides:
            layered.update(layer)
        match state:
            case _Parsed(argv=argv):
                values = {name: layered.get(name, holder._value) for name, holder in self._holders.items()}
                return CommandLine(list(argv), self._flag_table(values), True)
            case _Unparsed() if run_kind is _RunKind.TEST:
                # The empty command line, parsed: every flag its default. sys.argv is the test
                # runner's; the workers get it as it is and no flag is read from it.
                values = {name: layered.get(name, h._declaration.default) for name, h in self._holders.items()}
                return CommandLine(list(sys.argv), self._flag_table(values), True)
            case _Unparsed():
                return CommandLine(list(sys.argv), self._flag_table(layered), False)
            case _Parsing() | _Installed():
                raise FlagError(
                    f"a server started while the command line is {state}: only main serves, and only after "
                    "it parsed the command line (under `mipyre run`, a server started as the app is "
                    "imported is one an `if __name__ == '__main__':` guard is missing from)"
                )

    def _flag_table(self, values: Mapping[str, object]) -> dict[str, tuple[str, list[str] | None, object, object]]:
        table = {}
        for name, value in values.items():
            kind, choices, default = self._holders[name]._declaration.shape()
            table[name] = (kind, choices, default, _public(value))
        return table

    def push(self, values: Mapping[str, object]) -> Mapping[str, object]:
        """Put ``values`` over the flags' values; return the layer, which :meth:`pop` takes."""
        if isinstance(self._state, _Installed):
            raise FlagOverrideError(
                "flags.override is for the main interpreter of a test; this interpreter is a worker, whose "
                "values are main's"
            )
        layer = {name: self._overridden(name, value) for name, value in values.items()}
        with self._lock:
            if self._leases:
                raise FlagOverrideError(
                    f"flags.override({', '.join(values)}) entered while {self._leases} server(s) hold the flag "
                    "values: enter it before the server (or TestClient) starts"
                )
            self.overrides = (*self.overrides, layer)
        return layer

    def pop(self, layer: Mapping[str, object]) -> None:
        """Take ``layer`` back off; when a server holds the values, raise after it is off."""
        with self._lock:
            at = max(i for i, entered in enumerate(self.overrides) if entered is layer)
            self.overrides = self.overrides[:at] + self.overrides[at + 1 :]
            leases = self._leases
        if leases:
            raise FlagOverrideError(
                f"flags.override({', '.join(layer)}) left while {leases} server(s) still hold its values: "
                "stop the server (or TestClient) before the override ends"
            )

    def _overridden(self, name: str, value: object) -> object:
        holder = self._holders.get(name)
        if holder is None:
            raise FlagOverrideError(f"flags.override({name}=...): no flag {name!r} is declared")
        declaration = holder._declaration
        reason = _misfit(declaration.kind, declaration.choices, value)
        if reason is not None:
            raise FlagOverrideError(
                f"flags.override({name}=...): {reason}, which flag {declaration.option} (declared at "
                f"{declaration.site}) holds"
            )
        return _stored(declaration.kind, value)


def _check_reserved(declaration: _Declaration, reserved: Mapping[str, _Site]) -> None:
    for name in sorted(declaration.claims() & reserved.keys()):
        raise DuplicateFlagError(
            f"flag {declaration.option} declared at {declaration.site} takes the name of mipyre's own option "
            f"--{name.replace('_', '-')} (added at {reserved[name]}): rename the flag"
        )


def _parse_one(declaration: _Declaration, rest: Sequence[str], prog: str) -> object:
    """Return ``declaration``'s value in ``rest``, read by a parser that knows only it; a bad
    value exits as argparse does."""
    import argparse

    parser = argparse.ArgumentParser(prog=prog, add_help=False, allow_abbrev=False)
    parser.add_argument(declaration.option, **declaration.argparse_options())
    parsed, _others = parser.parse_known_args(list(rest))
    return getattr(parsed, declaration.name)


def _installed_value(declaration: _Declaration, command_line: CommandLine) -> object:
    """Return ``declaration``'s value in a worker whose server's values are ``command_line``."""
    entry = command_line.flag(declaration.name)
    if entry is None:
        if not command_line.parsed:
            return _Unset.UNSET
        raise FlagMismatchError(
            f"flag {declaration.option} declared at {declaration.site} is not on the server's command line: "
            "main never declared it. A module only workers import (inside a handler) declares a flag too "
            "late; import it at the top of the app's module"
        )
    kind, choices, default, value = entry
    if declaration.shape() != (kind, choices, default):
        raise FlagMismatchError(
            f"flag {declaration.option} declared at {declaration.site} as {declaration.shape()} (kind, choices, "
            f"default) is {(kind, choices, default)} on main: every interpreter runs the same declaration"
        )
    return _stored(declaration.kind, value)


_REGISTRY = _Registry()


def _site_of_caller() -> _Site:
    frame = sys._getframe(2)
    return _Site(frame.f_code.co_filename, frame.f_lineno)


def _declare(
    kind: _Kind, name: str, default: object, help: str, site: _Site, choices: Sequence[str] | None = None
) -> FlagHolder:
    if not isinstance(name, str) or not name.isidentifier() or name.startswith("_") or name == "help":
        raise FlagError(
            f"flag name {name!r} at {site}: a flag is named like a Python variable, not starting with _ "
            "(and not help, the option that lists the flags)"
        )
    held: tuple[str, ...] | None = None
    if kind is _Kind.CHOICE:
        if isinstance(choices, str) or not choices or not all(isinstance(c, str) for c in choices):
            raise FlagError(f"flag --{name} at {site}: choices {choices!r} are no non-empty sequence of str")
        held = tuple(choices)
    reason = _misfit(kind, held, default)
    if reason is not None:
        raise FlagError(f"flag --{name} at {site}: its default {reason}")
    return _REGISTRY.declare(_Declaration(name, kind, held, _stored(kind, default), help, site))


def string(name: str, default: str | None, help: str) -> FlagHolder[str | None]:
    """Declare a string flag ``--name``."""
    return _declare(_Kind.STR, name, default, help, _site_of_caller())


def integer(name: str, default: int | None, help: str) -> FlagHolder[int | None]:
    """Declare a 64-bit integer flag ``--name``."""
    return _declare(_Kind.INT, name, default, help, _site_of_caller())


def float(name: str, default: builtins.float | None, help: str) -> FlagHolder[builtins.float | None]:
    """Declare a finite float flag ``--name``."""
    return _declare(_Kind.FLOAT, name, default, help, _site_of_caller())


def boolean(name: str, default: bool, help: str) -> FlagHolder[bool]:
    """Declare a boolean flag: ``--name`` sets it, ``--no-name`` clears it."""
    return _declare(_Kind.BOOL, name, default, help, _site_of_caller())


def enum(name: str, default: str | None, choices: Sequence[str], help: str) -> FlagHolder[str | None]:
    """Declare a flag ``--name`` that takes one of ``choices``."""
    return _declare(_Kind.CHOICE, name, default, help, _site_of_caller(), choices)


def strings(name: str, default: Sequence[str] | None, help: str) -> FlagHolder[list[str] | None]:
    """Declare a list flag ``--name a,b``; ``.value`` is a new list on each read."""
    return _declare(_Kind.STRS, name, default, help, _site_of_caller())


def parse(argv: Sequence[str] | None = None) -> None:
    """Parse this process's command line (``argv``, default ``sys.argv[1:]``) for a script run
    with ``python app.py``. Call it after the modules that declare flags are imported and before
    a flag is read. What follows the first ``--`` is the script's own: ``sys.argv`` becomes
    ``[sys.argv[0], *those arguments]``. An unknown argument or a bad value exits with status 2,
    ``--help`` lists the flags and exits. In a worker (a sub-interpreter executing the script) it
    does nothing: the values are main's. Under ``mipyre run``, which parses the command line
    itself, it is a :class:`FlagError`."""
    if isinstance(_REGISTRY._state, _Installed):
        return
    import argparse

    tool, app_args = _split_command_line(sys.argv[1:] if argv is None else argv)
    argv0 = sys.argv[0]
    prog = os.path.basename(argv0)
    _REGISTRY.begin(tool, prog, {})
    sys.argv = [argv0, *app_args]
    _REGISTRY.seal(argparse.ArgumentParser(prog=prog, allow_abbrev=False), tool, sys.argv)


class _Override(contextlib.ContextDecorator):
    """What :func:`override` returns."""

    def __init__(self, values: Mapping[str, object]) -> None:
        self._values = dict(values)
        self._layers: list[Mapping[str, object]] = []

    def __call__(self, func: _F) -> _F:
        if inspect.iscoroutinefunction(func):
            raise FlagOverrideError(
                f"@flags.override on async function {func.__qualname__} would end before it is awaited: use "
                "`with flags.override(...):` inside it"
            )
        return super().__call__(func)

    def __enter__(self) -> None:
        self._layers.append(_REGISTRY.push(self._values))

    def __exit__(self, *_exc: object) -> None:
        _REGISTRY.pop(self._layers.pop())


def override(**values: object) -> _Override:
    """Set flags to ``values`` for a test, as a context manager or a decorator of a (sync) test
    function; leaving restores what was there before, nested ones included. It needs no
    :func:`parse`; a flag it does not set is its declared default in the test's servers. Every
    server started inside it gets the values (main, sub-interpreters, process workers), so it is
    entered before the server (``TestClient``) starts and left after it stops: entering or
    leaving while one runs is a :class:`FlagOverrideError` (on leaving, after the values are
    restored), as is an undeclared name or a value of the wrong kind."""
    return _Override(values)


def _begin(rest: Sequence[str], prog: str, reserved: Mapping[str, _Site]) -> None:
    """``mipyre run``'s first step, before it imports the app: ``rest`` is the command line
    before ``--`` less mipyre's own options, ``reserved`` those options' names and lines."""
    _REGISTRY.begin(rest, prog, reserved)


def _seal(parser: argparse.ArgumentParser, args: Sequence[str]) -> None:
    """``mipyre run``'s last step, after it imported the app: ``parser`` (mipyre's options)
    with the app's flags parses ``args`` (the command line before ``--``); ``sys.argv`` is
    already the app's."""
    _REGISTRY.seal(parser, args, sys.argv)


def _lease(run_kind: _RunKind) -> contextlib.AbstractContextManager[CommandLine]:
    """Return the values the workers of a server ``run_kind`` started get, held until the
    server's run ends."""
    return _REGISTRY.lease(run_kind)


def _differences(before: CommandLine, now: CommandLine) -> list[str]:
    """Return what differs between two command lines: ``argv``, then each declared flag whose
    value (or presence) differs."""
    differing = ["argv"] if before.argv != now.argv else []
    names = sorted(_REGISTRY.declared_names())
    return differing + [f"--{n.replace('_', '-')}" for n in names if before.flag(n) != now.flag(n)]


def _require_same_values_as_fork_template(imported_with: CommandLine, run: CommandLine) -> None:
    """Refuse a run whose command line differs from the one a start="fork" template imported the
    app with: its workers would keep the old values wherever the app read them at import."""
    differing = _differences(imported_with, run)
    if differing:
        raise FlagMismatchError(
            f"this run's {', '.join(differing)} differ from the values the process-lane template "
            '(start="fork") imported the app with, at its first run: a fork template imports the app '
            "once, and the module-level code of every worker it forks ran with those values. Give every "
            'run the same flag values, or use separate apps, or start="import" for per-run values.'
        )


def _install(command_line: CommandLine) -> None:
    """Make this interpreter a worker of a server whose command line is ``command_line``: its
    ``sys.argv`` and its flag values. Called before the app's code runs here (and again in a
    process-lane template for each run)."""
    sys.argv = command_line.argv
    _REGISTRY.install(command_line)
