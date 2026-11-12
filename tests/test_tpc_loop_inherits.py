"""Checks that the TPC thread's event loop gives every private attribute asyncio's inherited I/O code reads.

`TpcEventLoop` inherits connections, servers, `sock_*`, pipes, subprocesses and the executor from the standard
library's unix selector loop without calling its ``__init__``. Each attribute that code reads must come from the Rust
base or from ``TpcEventLoop.__init__``. A new CPython that reads one more turns the Rust test that runs `ungiven` and
`tpc_loop_census` on the real class red (`crates/runtime/src/event_loop/tests.rs`); the tests here check the census
itself on made-up classes.
"""

from __future__ import annotations

import ast
import dis
import inspect
import textwrap
from asyncio import (
    base_events,
    base_subprocess,
    events,
    selector_events,
    sslproto,
    unix_events,
)
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

# The public methods asyncio's I/O code gives the loop, which the Rust base does not have.
INHERITED_METHODS = (
    "run_in_executor",
    "set_default_executor",
    "shutdown_default_executor",
    "getaddrinfo",
    "getnameinfo",
    "create_connection",
    "create_server",
    "create_datagram_endpoint",
    "connect_accepted_socket",
    "start_tls",
    "sendfile",
    "sock_sendfile",
    "sock_recv",
    "sock_recv_into",
    "sock_recvfrom",
    "sock_recvfrom_into",
    "sock_sendall",
    "sock_sendto",
    "sock_connect",
    "sock_accept",
    "create_unix_connection",
    "create_unix_server",
    "connect_read_pipe",
    "connect_write_pipe",
    "subprocess_exec",
    "subprocess_shell",
    "shutdown_asyncgens",
    "add_signal_handler",
    "remove_signal_handler",
)

# What else is called on the loop object without one of the methods above: the async generator hooks the server sets,
# and what the interpreter calls.
OTHER_ENTRIES = (
    "_asyncgen_firstiter_hook",
    "_asyncgen_finalizer_hook",
    "__del__",
    "__repr__",
)

# typeshed leaves out asyncio's private classes, so they are named through these.
_SELECTOR_EVENTS: Any = selector_events
_UNIX_EVENTS: Any = unix_events
_BASE_EVENTS: Any = base_events

# The classes that hold the loop and call it from outside: transports, servers, child watchers, TLS, handles.
LOOP_USERS = (
    _SELECTOR_EVENTS._SelectorSocketTransport,
    _SELECTOR_EVENTS._SelectorDatagramTransport,
    _UNIX_EVENTS._UnixReadPipeTransport,
    _UNIX_EVENTS._UnixWritePipeTransport,
    _UNIX_EVENTS._UnixSubprocessTransport,
    base_subprocess.BaseSubprocessTransport,
    base_subprocess.WriteSubprocessPipeProto,
    base_subprocess.ReadSubprocessPipeProto,
    sslproto.SSLProtocol,
    sslproto._SSLProtocolTransport,
    base_events.Server,
    _BASE_EVENTS._SendfileFallbackProtocol,
    _UNIX_EVENTS._PidfdChildWatcher,
    _UNIX_EVENTS._ThreadedChildWatcher,
    events.Handle,
    events.TimerHandle,
)

# The standard library's scheduler, which runs only from its `run_forever`; the Rust base's `run_forever` refuses.
SCHEDULER = frozenset({"_run_once", "_process_events", "_write_to_self"})

# Read only when user code builds an `events.TimerHandle` itself and cancels it, bypassing `call_later`: that is not an
# API of the loop, so the loop does not give it.
KNOWN_UNGIVEN = frozenset({"_timer_cancelled_count"})

# The names a user class reads its loop through.
LOOP_NAMES = frozenset({"_loop", "loop"})


@dataclass(frozen=True)
class LoopReach:
    """What asyncio's code reaches on a loop class: the inherited methods it runs and the instance attributes it reads."""

    methods: frozenset[str]
    attributes: frozenset[str]


def missing(reads: frozenset[str], provided: frozenset[str]) -> frozenset[str]:
    """Return the attributes read that nothing provides."""
    return reads - provided


def _tree(function: object) -> ast.AST | None:
    if not callable(function):
        return None
    try:
        source = inspect.getsource(function)
    except OSError, TypeError:
        return None
    return ast.parse(textwrap.dedent(source))


def _reads_on(tree: ast.AST, names: frozenset[str]) -> frozenset[str]:
    """Return the attributes the code reads on a variable in `names`, or on ``self.<one of names>``."""

    def holder(node: ast.expr) -> str | None:
        if isinstance(node, ast.Name):
            return node.id
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "self"
        ):
            return node.attr
        return None

    return frozenset(
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.ctx, ast.Load)
        and holder(node.value) in names
    )


def _self_reads(tree: ast.AST) -> frozenset[str]:
    """Return the attributes the code reads on ``self``, counting ``self.x += 1``, which reads before it stores."""
    updated = {
        id(node.target) for node in ast.walk(tree) if isinstance(node, ast.AugAssign)
    }
    return frozenset(
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and (isinstance(node.ctx, ast.Load) or id(node) in updated)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    )


def assigned_in_init(cls: type) -> frozenset[str]:
    """Return the attributes assigned by the ``__init__`` an instance of `cls` runs, wherever in its MRO that is.

    It is read from the bytecode, which needs no source file.
    """
    return frozenset(
        instruction.argval
        for instruction in dis.get_instructions(
            next(
                vars(klass)["__init__"]
                for klass in cls.__mro__
                if "__init__" in vars(klass)
            )
        )
        if instruction.opname == "STORE_ATTR"
    )


def loop_reads(users: tuple[type, ...]) -> frozenset[str]:
    """Return the attributes the methods of `users` read on the loop they hold."""
    return frozenset(
        name
        for user in users
        for member in vars(user).values()
        if inspect.isfunction(member) and (tree := _tree(member)) is not None
        for name in _reads_on(tree, LOOP_NAMES)
    )


def census(cls: type, native: type, entries: frozenset[str]) -> LoopReach:
    """Walk, through `cls`'s MRO, the methods reached from `entries` and what they read on ``self``.

    A name `native` has is given by it and not walked. A name no class in the MRO defines is an instance attribute.
    """
    methods: set[str] = set()
    attributes: set[str] = set()
    pending = list(entries)
    while pending:
        name = pending.pop()
        if name in methods or name in attributes:
            continue
        owner = next((klass for klass in cls.__mro__ if name in vars(klass)), None)
        if owner is None:
            attributes.add(name)
            continue
        if owner is native or owner is object:
            continue
        methods.add(name)
        member = vars(owner)[name]
        function = (
            member.fget
            if isinstance(member, property)
            else getattr(member, "__func__", member)
        )
        tree = _tree(function)
        if tree is not None:
            pending.extend(_self_reads(tree))
    return LoopReach(frozenset(methods), frozenset(attributes))


def tpc_loop_census(cls: type, native: type) -> LoopReach:
    """Return the census of the TPC thread's loop class `cls`, whose Rust base is `native`."""
    return census(
        cls,
        native,
        frozenset(INHERITED_METHODS)
        | frozenset(OTHER_ENTRIES)
        | loop_reads(LOOP_USERS),
    )


def ungiven(cls: type, native: type) -> frozenset[str]:
    """Return the attributes asyncio's code reads on `cls` that neither `native` nor ``cls.__init__`` gives."""
    return missing(
        tpc_loop_census(cls, native).attributes, assigned_in_init(cls) | KNOWN_UNGIVEN
    )


def test_an_attribute_read_and_not_provided_is_missing() -> None:
    assert missing(frozenset({"_given", "_ungiven"}), frozenset({"_given"})) == {
        "_ungiven"
    }


def test_nothing_is_missing_when_every_read_is_provided() -> None:
    assert (
        missing(frozenset({"_given"}), frozenset({"_given", "_spare"})) == frozenset()
    )


class _Native:
    def given(self) -> None: ...


class _Library:
    # Annotated only, so no class defines them and the census takes them for instance attributes.
    given: Callable[[], None]
    _read_by_helper: list[int]
    _set_by_init: int
    _counted: int
    _read_by_nobody: list[int]

    def entry(self) -> None:
        self._helper()
        self.given()

    def _helper(self) -> None:
        self._read_by_helper.append(self._set_by_init)
        self._counted += 1

    def _unreached(self) -> None:
        self._read_by_nobody.clear()


class _Sub(_Native, _Library):
    def __init__(self) -> None:
        self._set_by_init = 1


def test_the_census_follows_inherited_methods_to_what_they_read() -> None:
    found = census(_Sub, _Native, frozenset({"entry"}))
    assert found == LoopReach(
        frozenset({"entry", "_helper"}),
        frozenset({"_read_by_helper", "_set_by_init", "_counted"}),
    )
    assert missing(found.attributes, assigned_in_init(_Sub)) == {
        "_read_by_helper",
        "_counted",
    }
