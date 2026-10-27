"""Flags the app declares: declared where they are used, parsed once on
main, and the same in every interpreter that runs the app — main, each sub-interpreter, each process
worker started by import or by fork. Each end-to-end test runs a real `mipyre run`, a script, or a test
process of its own (a template forks only from a single-threaded process); the rest feed the pure parts
(declarations, the registry, the mismatch check) their inputs directly."""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import textwrap
import urllib.request

import pytest

from mipyre import flags
from mipyre.engine import CommandLine
from mipyre.flags import (
    DuplicateFlagError,
    FlagMismatchError,
    FlagOverrideError,
    UnparsedFlagError,
    _Declaration,
    _installed_value,
    _Kind,
    _misfit,
    _Registry,
    _RunKind,
    _Site,
    _split_command_line,
)
from tests._helpers import bound_port, free_port_below_ephemeral, read_file

STARTS = ["import", "fork"]

# Every kind of flag, and what each route of the app sees of them.
_FLAGS = '''
import os
import sys

from mipyre import Mipyre, flags

DSN = flags.string("db_dsn", None, "Postgres DSN")
N = flags.integer("n", 3, "how many")
RATIO = flags.float("ratio", 0.5, "a share")
ON = flags.boolean("on", False, "a switch")
TAGS = flags.strings("tags", [], "labels")
RUNTIME = flags.enum("app_runtime", "subinterp", ["subinterp", "process"], "where /default runs")
HOLDERS = (DSN, N, RATIO, ON, TAGS, RUNTIME)


def read(holder):
    try:
        return holder.value
    except flags.UnparsedFlagError:
        return "<unparsed>"


def seen():
    return {"values": {h.name: read(h) for h in HOLDERS}, "argv": sys.argv, "pid": os.getpid()}


def routes(app):
    @app.get("/main", runtime="main")
    def on_main(req):
        return seen()

    @app.get("/subinterp", runtime="subinterp")
    def on_subinterp(req):
        return seen()

    @app.get("/process", runtime="process")
    def on_process(req):
        return seen()
'''

# Under `mipyre run`: the app's runtime is a flag read at module level (CUJ-1). As a script, it
# parses its command line before that read; the sub-interpreters that execute it again do not.
_RUN_APP = _FLAGS + '''
if __name__ == "__main__":
    flags.parse()

app = Mipyre(runtime=RUNTIME.value)
routes(app)


@app.get("/default")
def on_default(req):
    return seen()


@app.get("/lazy", runtime="process")
def lazy(req):
    try:
        import lazy_flag  # declares a flag main never saw
    except flags.FlagMismatchError as e:
        return {"error": type(e).__name__, "message": str(e)}
    return {"error": None}


if __name__ == "__main__":
    app.run()
'''

# In a test process: nothing parses.
_TEST_APP = _FLAGS + '''
app = Mipyre()
routes(app)


@app.get("/override", runtime="subinterp")
def override_in_a_worker(req):
    try:
        with flags.override(db_dsn="from a worker"):
            pass
    except flags.FlagOverrideError as e:
        return {"error": type(e).__name__}
    return {"error": None}
'''

_LAZY_FLAG = '''
from mipyre import flags

LAZY = flags.string("lazy", None, "declared only where a handler imports it")
'''

_ROUTES = ("/main", "/subinterp", "/process")


def _write(tmp_path, name: str, source: str) -> str:
    path = tmp_path / name
    path.write_text(source)
    return str(path)


def _get(port: int, path: str) -> dict:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=30) as r:
        return json.loads(r.read())


def _serve(tmp_path, argv: list[str], env: dict[str, str] | None = None):
    """Start `argv` in `tmp_path` with its output in a file; return the process and its reader."""
    log_path = str(tmp_path / "server.log")
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            argv, cwd=tmp_path, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
            env={**os.environ, **(env or {})},
        )
    return proc, read_file(log_path)


def _answers(tmp_path, argv: list[str], paths: tuple[str, ...], env: dict[str, str] | None = None) -> dict[str, dict]:
    proc, output = _serve(tmp_path, argv, env)
    try:
        port = bound_port(output, proc)
        return {path: _get(port, path) for path in paths}
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGINT)
        proc.wait()


def _mipyre_run(*args: str) -> list[str]:
    return [sys.executable, "-m", "mipyre", "run", "app.py:app", *args]


EXPECTED = {"db_dsn": "x", "n": 7, "ratio": 0.25, "on": True, "tags": ["a", "b"], "app_runtime": "process"}
GIVEN = ["--db-dsn", "x", "--n", "7", "--ratio", "0.25", "--on", "--tags", "a,b", "--app-runtime", "process"]


@pytest.mark.parametrize("start", STARTS)
def test_every_interpreter_reads_the_flags_mipyre_run_parsed(tmp_path, start):
    """main, a sub-interpreter and a process worker read the same values and argv, and the
    app's runtime came from a flag read at module level."""
    app_file = _write(tmp_path, "app.py", _RUN_APP)
    seen = _answers(
        tmp_path,
        _mipyre_run("--port", "0", "--threads", "2", "--process-workers", "1", "--process-start", start, *GIVEN),
        (*_ROUTES, "/default"),
    )
    for path, answer in seen.items():
        assert answer["values"] == EXPECTED, path
        assert [os.path.realpath(answer["argv"][0]), *answer["argv"][1:]] == [os.path.realpath(app_file)], path
    assert seen["/default"]["pid"] == seen["/process"]["pid"] != seen["/main"]["pid"] == seen["/subinterp"]["pid"]


def test_a_mistyped_flag_refuses_the_command_line_before_anything_binds(tmp_path):
    """exit status 2 naming the argument, and the port is still free."""
    _write(tmp_path, "app.py", _RUN_APP)
    port = free_port_below_ephemeral()
    proc, output = _serve(tmp_path, _mipyre_run("--port", str(port), "--db-dns", "x"))
    assert proc.wait() == 2
    assert "unrecognized arguments: --db-dns x" in output(), output()
    with socket.socket() as s:
        s.bind(("127.0.0.1", port))


def test_help_lists_mipyre_s_options_and_the_app_s_flags(tmp_path):
    _write(tmp_path, "app.py", _RUN_APP)
    done = subprocess.run(_mipyre_run("--help"), cwd=tmp_path, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    for text in ("--threads", "--db-dsn DB_DSN", "Postgres DSN (default: None)", "--on, --no-on", "--tags A,B,..."):
        assert text in done.stdout, done.stdout


def test_help_without_a_target_lists_mipyre_s_options():
    done = subprocess.run([sys.executable, "-m", "mipyre", "run", "--help"], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert "--threads" in done.stdout


@pytest.mark.parametrize("start", STARTS)
def test_a_script_that_parses_its_flags_shares_them_with_every_interpreter(tmp_path, start):
    """`python app.py` with `flags.parse()`; what follows `--` is the script's."""
    _write(tmp_path, "app.py", _RUN_APP)
    seen = _answers(
        tmp_path,
        [sys.executable, "app.py", *GIVEN, "--", "--verbose"],
        _ROUTES,
        env={"MIPYRE_PORT": "0", "MIPYRE_THREADS": "2", "MIPYRE_PROCESS_WORKERS": "1", "MIPYRE_PROCESS_START": start},
    )
    for path, answer in seen.items():
        assert answer["values"] == EXPECTED, path
        assert answer["argv"] == ["app.py", "--verbose"], path


def test_a_script_that_declares_no_flag_keeps_its_own_command_line(tmp_path):
    """mipyre does not read the argv of a script that never declared a flag."""
    _write(tmp_path, "app.py", textwrap.dedent('''
        import argparse
        import sys

        from mipyre import Mipyre

        ARGS = argparse.ArgumentParser().parse_known_args()[0]
        app = Mipyre()

        @app.get("/subinterp", runtime="subinterp")
        def on_subinterp(req):
            return {"argv": sys.argv}

        if __name__ == "__main__":
            app.run()
    '''))
    seen = _answers(tmp_path, [sys.executable, "app.py", "--db-dsn", "y"], ("/subinterp",), env={"MIPYRE_PORT": "0"})
    assert seen["/subinterp"]["argv"] == ["app.py", "--db-dsn", "y"]


def test_a_script_that_never_parses_reads_no_flag_in_any_interpreter(tmp_path):
    """`python app.py` without `flags.parse()` serves, and a flag read
    on main, in a sub-interpreter or in a process worker raises UnparsedFlagError; its default is
    a test's only."""
    _write(tmp_path, "app.py", _FLAGS + '''
app = Mipyre()
routes(app)

if __name__ == "__main__":
    app.run()
''')
    seen = _answers(
        tmp_path,
        [sys.executable, "app.py"],
        _ROUTES,
        env={"MIPYRE_PORT": "0", "MIPYRE_THREADS": "2", "MIPYRE_PROCESS_WORKERS": "1"},
    )
    for path, answer in seen.items():
        assert set(answer["values"].values()) == {"<unparsed>"}, path


def test_a_flag_declared_only_where_a_worker_imports_it_fails_naming_it(tmp_path):
    """the lazy import in a process handler raises, naming the flag and its module."""
    _write(tmp_path, "app.py", _RUN_APP)
    lazy = _write(tmp_path, "lazy_flag.py", _LAZY_FLAG)
    seen = _answers(tmp_path, _mipyre_run("--port", "0", "--process-workers", "1"), ("/lazy",))
    assert seen["/lazy"]["error"] == "FlagMismatchError"
    assert "--lazy" in seen["/lazy"]["message"] and lazy in seen["/lazy"]["message"], seen


def test_a_worker_declaring_another_default_fails_to_start_naming_the_flag(tmp_path):
    """the same flag with another default in a sub-interpreter is refused, not given main's value."""
    _write(tmp_path, "app.py", textwrap.dedent('''
        from mipyre import Mipyre, flags
        from mipyre.engine import _in_worker

        N = flags.integer("n", 2 if _in_worker() else 1, "how many")
        app = Mipyre()

        @app.get("/", runtime="subinterp")
        def index(req):
            return {"n": N.value}
    '''))
    proc, output = _serve(tmp_path, _mipyre_run("--port", "0", "--threads", "1"))
    assert proc.wait() != 0
    assert "FlagMismatchError" in output() and "--n" in output(), output()


_TEST_SCRIPT = '''
import json

import app as A
from mipyre import flags
from mipyre.testing import TestClient

from mipyre.engine import ServerFlags
import os

CONFIG = ServerFlags(threads=2, process_workers=1, process_start=os.environ["CHECK_PROCESS_START"])

ROUTES = ("/main", "/subinterp", "/process")
seen = {}


def dsn(client):
    return [client.get(path).json()["values"]["db_dsn"] for path in ROUTES]


def values(client):
    return [client.get(path).json()["values"] for path in ROUTES]


def raised(call):
    try:
        call()
    except flags.FlagError as e:
        return type(e).__name__
    return None


with flags.override(db_dsn="t"), TestClient(A.app, config=CONFIG) as c:
    seen["override"] = dsn(c)
seen["after"] = raised(lambda: A.DSN.value)
with flags.override(db_dsn="second"), TestClient(A.app, config=CONFIG) as c:
    seen["second_run"] = dsn(c)
with TestClient(A.app, config=CONFIG) as c:
    seen["no_override"] = values(c)
    seen["read_while_serving"] = A.read(A.N)
    seen["enter_while_serving"] = raised(lambda: flags.override(db_dsn="x").__enter__())
    seen["override_in_a_worker"] = c.get("/override").json()["error"]

outlived = flags.override(db_dsn="outlived")
outlived.__enter__()
c = TestClient(A.app, config=CONFIG)
seen["leave_while_serving"] = raised(lambda: outlived.__exit__(None, None, None))
seen["after_leaving"] = A.read(A.DSN)
c.close()

print(json.dumps(seen))
'''


def _check(tmp_path, script: str, start: str) -> subprocess.CompletedProcess:
    _write(tmp_path, "check.py", script)
    return subprocess.run(
        [sys.executable, "check.py"], cwd=tmp_path, capture_output=True, text=True,
        env={**os.environ, "CHECK_PROCESS_START": start},
    )


_DEFAULTS: dict[str, object] = {"db_dsn": None, "n": 3, "ratio": 0.5, "on": False, "tags": [], "app_runtime": "subinterp"}


def test_a_test_s_overrides_reach_every_runtime_of_each_run(tmp_path):
    """Each run's workers get that run's overrides (process
    workers too: one template starts every run's), and every flag no override sets is its
    declared default (the test runner's argv is never read); none survive their `with`
    (outside a server an unparsed flag still raises), and none can change while a server holds
    the values. Process workers that import the app: a fork template's runs must all have the same
    values (the next test)."""
    _write(tmp_path, "app.py", _TEST_APP)
    done = _check(tmp_path, _TEST_SCRIPT, "import")
    assert done.returncode == 0, done.stderr[-4000:]
    seen = json.loads(done.stdout.strip().splitlines()[-1])
    assert seen == {
        "override": ["t", "t", "t"],
        "after": "UnparsedFlagError",
        "second_run": ["second", "second", "second"],
        "no_override": [_DEFAULTS, _DEFAULTS, _DEFAULTS],
        "read_while_serving": 3,
        "enter_while_serving": "FlagOverrideError",
        "override_in_a_worker": "FlagOverrideError",
        "leave_while_serving": "FlagOverrideError",
        "after_leaving": None,
    }


_FORK_SCRIPT = '''
import json
import os

from mipyre import flags
from mipyre.engine import ServerFlags
from mipyre.testing import TestClient

import app as A

CONFIG = ServerFlags(threads=2, process_workers=1, process_start=os.environ["CHECK_PROCESS_START"])

ROUTES = ("/main", "/subinterp", "/process")
seen = {}


def run(value):
    try:
        with flags.override(db_dsn=value), TestClient(A.app, config=CONFIG) as c:
            return [c.get(path).json()["values"]["db_dsn"] for path in ROUTES] + [c.get("/imported").json()["dsn"]]
    except flags.FlagMismatchError as e:
        return [type(e).__name__, "--db-dsn" in str(e), 'start="import"' in str(e)]


seen["first"] = run("t")
seen["same_again"] = run("t")
seen["other_values"] = run("u")
print(json.dumps(seen))
'''


def test_a_fork_template_refuses_a_run_with_values_other_than_the_ones_it_imported_with(tmp_path):
    """The fork template imports the app once: a later run with the same values is served, one
    with other values fails to start, naming the flag, instead of serving the old module-level
    values."""
    _write(tmp_path, "app.py", _TEST_APP + '''
IMPORTED_DSN = read(DSN)  # at import: a fork template's workers keep what it read


@app.get("/imported", runtime="process")
def imported(req):
    return {"dsn": IMPORTED_DSN}
''')
    done = _check(tmp_path, _FORK_SCRIPT, "fork")
    assert done.returncode == 0, done.stderr[-4000:]
    seen = json.loads(done.stdout.strip().splitlines()[-1])
    # What a worker read at import is the template's import (macOS: a new process, with the first
    # run's values) or this process's (Linux: forked after the test imported the app); either way
    # the same for every run it serves.
    assert seen["first"][:3] == ["t", "t", "t"]
    assert seen["same_again"] == seen["first"]
    assert seen["other_values"] == ["FlagMismatchError", True, True]


# --- The parts without a server ------------------------------------------------------------------------------------


def _decl(name="db_dsn", kind=_Kind.STR, default=None, choices=None, file="/srv/app.py", line=1) -> _Declaration:
    return _Declaration(name, kind, choices, default, "help", _Site(file, line))


def test_a_line_executed_twice_declares_one_flag():
    registry = _Registry()
    first = registry.declare(_decl(line=4))
    assert registry.declare(_decl(line=4)) is first


def test_two_modules_declaring_one_name_are_refused_naming_both_lines():
    registry = _Registry()
    registry.declare(_decl(file="/srv/a.py", line=4))
    with pytest.raises(DuplicateFlagError, match=r"/srv/b\.py:9 .*/srv/a\.py:4"):
        registry.declare(_decl(file="/srv/b.py", line=9))


def test_one_module_declaring_one_name_two_ways_is_refused():
    registry = _Registry()
    registry.declare(_decl(default=None))
    with pytest.raises(DuplicateFlagError):
        registry.declare(_decl(default="x"))


def test_a_boolean_flag_takes_its_negative_name_too():
    registry = _Registry()
    registry.declare(_decl(name="on", kind=_Kind.BOOL, default=False))
    with pytest.raises(DuplicateFlagError):
        registry.declare(_decl(name="no_on", file="/srv/b.py"))


def test_a_flag_taking_one_of_mipyre_s_options_names_both_lines():
    registry = _Registry()
    registry.begin([], "mipyre run", {"workers": _Site("/mipyre/cli.py", 200)})
    with pytest.raises(DuplicateFlagError, match=r"/srv/app\.py:1 .*--workers .*/mipyre/cli\.py:200"):
        registry.declare(_decl(name="workers", kind=_Kind.INT))


def test_a_flag_declared_before_the_command_line_begins_takes_its_value_when_it_does():
    registry = _Registry()
    holder = registry.declare(_decl())
    with pytest.raises(UnparsedFlagError, match="--db-dsn"):
        holder.value
    registry.begin(["--db-dsn", "x", "--other", "y"], "prog", {})
    assert holder.value == "x"


def test_sealing_refuses_what_no_one_declared():
    registry = _Registry()
    registry.begin(["--db-dsn", "x", "--other"], "prog", {})
    registry.declare(_decl())
    with pytest.raises(SystemExit) as e:
        registry.seal(argparse.ArgumentParser(prog="prog", allow_abbrev=False), ["--db-dsn", "x", "--other"], ["a"])
    assert e.value.code == 2


def test_a_flag_declared_after_the_seal_is_too_late():
    registry = _Registry()
    registry.begin([], "prog", {})
    registry.seal(argparse.ArgumentParser(prog="prog", allow_abbrev=False), [], ["a"])
    with pytest.raises(FlagMismatchError, match="after the command line was parsed"):
        registry.declare(_decl())


def test_an_override_ends_where_it_began_and_nests():
    registry = _Registry()
    holder = registry.declare(_decl())
    registry.begin(["--db-dsn", "parsed"], "prog", {})
    registry.seal(argparse.ArgumentParser(prog="prog", allow_abbrev=False), ["--db-dsn", "parsed"], ["a"])
    outer = registry.push({"db_dsn": "outer"})
    inner = registry.push({"db_dsn": "inner"})
    assert holder.value == "inner"
    registry.pop(inner)
    assert holder.value == "outer"
    registry.pop(outer)
    assert holder.value == "parsed"


def test_a_server_gets_the_overridden_values_and_holds_them():
    registry = _Registry()
    holder = registry.declare(_decl())
    layer = registry.push({"db_dsn": "t"})
    with registry.lease(_RunKind.SCRIPT) as command_line:
        assert command_line.flag("db_dsn") == ("str", None, None, "t")
        assert not command_line.parsed
        with pytest.raises(FlagOverrideError, match="entered while 1 server"):
            registry.push({"db_dsn": "u"})
        with pytest.raises(FlagOverrideError, match="left while 1 server"):
            registry.pop(layer)
    with pytest.raises(UnparsedFlagError):
        holder.value


def test_a_test_s_server_gives_every_flag_nothing_parsed_its_default():
    """The empty command line, parsed; an override still goes over it. While the server
    holds the values main reads them too, and after it stops an unparsed read raises again."""
    registry = _Registry()
    dsn = registry.declare(_decl(default="d"))
    n = registry.declare(_decl(name="n", kind=_Kind.INT, default=1))
    registry.push({"n": 7})
    with registry.lease(_RunKind.TEST) as command_line:
        assert command_line.parsed
        assert command_line.flag("db_dsn") == ("str", None, "d", "d")
        assert command_line.flag("n") == ("int", None, 1, 7)
        assert (dsn.value, n.value) == ("d", 7)
    with pytest.raises(UnparsedFlagError):
        dsn.value


def test_a_script_s_server_gives_a_flag_nothing_parsed_no_value():
    registry = _Registry()
    holder = registry.declare(_decl(default="d"))
    with registry.lease(_RunKind.SCRIPT) as command_line:
        assert not command_line.parsed
        assert command_line.flag("db_dsn") is None
        with pytest.raises(UnparsedFlagError):
            holder.value


def test_a_test_s_server_after_a_parse_gets_the_parsed_values():
    registry = _Registry()
    holder = registry.declare(_decl(default="d"))
    registry.begin(["--db-dsn", "parsed"], "prog", {})
    registry.seal(argparse.ArgumentParser(prog="prog", allow_abbrev=False), ["--db-dsn", "parsed"], ["a"])
    with registry.lease(_RunKind.TEST) as command_line:
        assert command_line.flag("db_dsn") == ("str", None, "d", "parsed")
        assert holder.value == "parsed"


@pytest.mark.parametrize("values", [{"nope": 1}, {"n": "7"}, {"n": True}, {"on": None}])
def test_an_override_of_no_flag_or_of_the_wrong_kind_is_refused(values):
    registry = _Registry()
    registry.declare(_decl(name="n", kind=_Kind.INT, default=1))
    registry.declare(_decl(name="on", kind=_Kind.BOOL, default=False))
    with pytest.raises(FlagOverrideError):
        registry.push(values)


def test_an_async_test_cannot_be_decorated_with_an_override():
    async def a_test():
        pass

    with pytest.raises(FlagOverrideError, match="async"):
        flags.override(db_dsn="t")(a_test)


@pytest.mark.parametrize(
    "kind, choices, value, fits",
    [
        (_Kind.INT, None, 2**63 - 1, True),
        (_Kind.INT, None, 2**63, False),
        (_Kind.INT, None, -(2**63), True),
        (_Kind.INT, None, -(2**63) - 1, False),
        (_Kind.INT, None, True, False),
        (_Kind.FLOAT, None, 1, True),
        (_Kind.FLOAT, None, float("inf"), False),
        (_Kind.FLOAT, None, float("nan"), False),
        (_Kind.BOOL, None, None, False),
        (_Kind.STR, None, None, True),
        (_Kind.CHOICE, ("a", "b"), "b", True),
        (_Kind.CHOICE, ("a", "b"), "c", False),
        (_Kind.STRS, None, ["a"], True),
        (_Kind.STRS, None, "a", False),
        (_Kind.STRS, None, ["a", 1], False),
    ],
)
def test_a_value_fits_its_kind_up_to_the_edge(kind, choices, value, fits):
    assert (_misfit(kind, choices, value) is None) is fits


def _line(flag_table: dict, parsed: bool = True) -> CommandLine:
    return CommandLine(["/srv/app.py"], flag_table, parsed)


def test_a_worker_takes_main_s_value_when_its_declaration_matches():
    declaration = _decl(name="n", kind=_Kind.INT, default=1)
    assert _installed_value(declaration, _line({"n": ("int", None, 1, 7)})) == 7


@pytest.mark.parametrize(
    "entry",
    [("int", None, 2, 7), ("float", None, 1.0, 7.0), ("choice", ["1"], "1", "1")],
)
def test_a_worker_whose_declaration_differs_from_main_s_is_refused(entry):
    declaration = _decl(name="n", kind=_Kind.INT, default=1)
    with pytest.raises(FlagMismatchError, match="--n"):
        _installed_value(declaration, _line({"n": entry}))


def test_a_worker_declaring_a_flag_main_has_not_is_refused_only_when_main_parsed():
    declaration = _decl()
    with pytest.raises(FlagMismatchError, match="not on the server's command line"):
        _installed_value(declaration, _line({}))
    assert _installed_value(declaration, _line({}, parsed=False)) is flags._Unset.UNSET


def test_a_command_line_reads_back_as_it_was_encoded():
    line = CommandLine(
        ["/srv/app.py", os.fsdecode(b"\xff")],
        {
            "dsn": ("str", None, None, None),
            "ratio": ("float", None, 1.0, 2.0),
            "tags": ("strs", None, [], []),
            "mode": ("choice", ["a", "b"], "a", "b"),
        },
        True,
    )
    assert CommandLine.decode(line.encode()) == line
    assert CommandLine.decode(line.encode()).argv == line.argv
    assert CommandLine.decode(line.encode()).flag("ratio") == ("float", None, 1.0, 2.0)


@pytest.mark.parametrize(
    "argv, mine, apps",
    [
        ([], [], []),
        (["run", "app:app"], ["run", "app:app"], []),
        (["--"], [], []),
        (["run", "app:app", "--"], ["run", "app:app"], []),
        (["run", "app:app", "--port", "1", "--", "--db-dsn", "x"], ["run", "app:app", "--port", "1"], ["--db-dsn", "x"]),
        (["run", "app:app", "--", "a", "--", "b"], ["run", "app:app"], ["a", "--", "b"]),
    ],
)
def test_the_command_line_splits_at_its_first_separator(argv, mine, apps):
    assert _split_command_line(argv) == (mine, apps)
