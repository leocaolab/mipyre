"""Tests for the `mipyre` CLI.

We test the three pieces we control:

- ``routes`` subcommand prints the route table.
- ``_load_app`` resolves ``module[:attr]``, errors helpfully when the
  target is missing, not a Mipyre, or the module fails to import.
- ``--version`` exits cleanly.

We don't exercise ``run``/``dev`` here — those block on the server loop
and are covered by the existing e2e/server tests.
"""

from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path

import pytest

from mipyre import Mipyre, flags
from mipyre._target import parse_target
from mipyre.cli import _load_app, main


# ---------------------------------------------------------------------------
# _load_app
# ---------------------------------------------------------------------------


def _write_app_module(tmp_path: Path, body: str = "") -> str:
    mod = tmp_path / "cli_fixture_app.py"
    mod.write_text(
        "from mipyre import BodyStream, Mipyre\n"
        "app = Mipyre()\n"
        "\n"
        "@app.get('/ping')\n"
        "def ping(req):\n"
        "    return 'pong'\n"
        "\n"
        "@app.post('/upload', runtime='main', body=BodyStream)\n"
        "def upload(req):\n"
        "    return ''\n"
        "\n"
        "app.add_fast_response('GET', '/health', b'{\"ok\":true}', content_type='application/json')\n"
        + body
    )
    sys.path.insert(0, str(tmp_path))
    return "cli_fixture_app"


@pytest.fixture(autouse=True)
def _cleanup_sys_path(monkeypatch):
    original = list(sys.path)
    mods = set(sys.modules)
    monkeypatch.setattr(sys, "argv", list(sys.argv))  # `main` sets the app's
    monkeypatch.setattr(flags, "_REGISTRY", flags._Registry())  # each `main` parses a command line once
    yield
    sys.path[:] = original
    for m in set(sys.modules) - mods:
        del sys.modules[m]


def test_load_app_default_attr(tmp_path):
    modname = _write_app_module(tmp_path)
    app = _load_app(parse_target(modname))
    assert isinstance(app, Mipyre)


def test_load_app_explicit_attr(tmp_path):
    modname = _write_app_module(tmp_path)
    app = _load_app(parse_target(f"{modname}:app"))
    assert isinstance(app, Mipyre)


def test_load_app_missing_module_exits(tmp_path):
    with pytest.raises(SystemExit) as ei:
        _load_app(parse_target("definitely_not_a_module_abc123"))
    assert "cannot import" in str(ei.value)


def test_load_app_missing_attr(tmp_path):
    modname = _write_app_module(tmp_path)
    with pytest.raises(SystemExit) as ei:
        _load_app(parse_target(f"{modname}:not_there"))
    assert "no attribute" in str(ei.value)


def test_an_import_error_inside_the_app_module_is_the_modules_own_error(tmp_path):
    # The module exists; a dependency of it does not. That is the module's failure,
    # with its traceback, not "cannot import the target".
    (tmp_path / "cli_broken_dep.py").write_text("import definitely_missing_dep_abc123\n")
    sys.path.insert(0, str(tmp_path))
    with pytest.raises(ModuleNotFoundError) as raised:
        _load_app(parse_target("cli_broken_dep:app"))
    assert raised.value.name == "definitely_missing_dep_abc123"


def test_a_missing_py_file_target_exits_with_one_line(tmp_path):
    with pytest.raises(SystemExit) as ei:
        _load_app(parse_target(str(tmp_path / "nowhere.py") + ":app"))
    assert "nowhere.py" in str(ei.value)


def test_a_module_getattr_is_not_taken_for_the_target(tmp_path):
    (tmp_path / "cli_lazy_attr.py").write_text("def __getattr__(name):\n    raise RuntimeError('computed')\n")
    sys.path.insert(0, str(tmp_path))
    with pytest.raises(SystemExit) as ei:
        _load_app(parse_target("cli_lazy_attr:app"))
    assert "no attribute 'app'" in str(ei.value)


def test_a_target_with_two_colons_exits_with_one_line():
    with pytest.raises(SystemExit) as ei:
        main(["routes", "a:b:c"])
    assert str(ei.value) == "mipyre: invalid target 'a:b:c': expected module:attr (exactly one ':')"


@pytest.mark.parametrize(
    "missing, is_target",
    [("a", True), ("a.b", True), ("a.b.c", False), ("x", False), ("a.bc", False)],
)
def test_names_target_is_the_module_or_a_package_above_it(missing, is_target):
    from mipyre._target import names_target

    assert names_target("a.b", missing) is is_target


def test_mipyre_run_of_a_missing_module_says_so_in_one_line(tmp_path):
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    done = subprocess.run(
        [sys.executable, "-m", "mipyre.cli", "run", "nosuch_module_abc123:app"],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120,
    )
    assert done.returncode == 1
    assert "mipyre: cannot import 'nosuch_module_abc123:app': no module named 'nosuch_module_abc123'" in (
        done.stderr.splitlines()
    ), done.stderr
    assert "Traceback" not in done.stderr, done.stderr


def test_mipyre_run_of_an_app_whose_import_fails_prints_its_traceback(tmp_path):
    (tmp_path / "broken_app.py").write_text("import definitely_missing_dep_abc123\n")
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    done = subprocess.run(
        [sys.executable, "-m", "mipyre.cli", "run", "broken_app:app"],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120,
    )
    assert done.returncode == 1
    assert "broken_app.py" in done.stderr, done.stderr
    assert "ModuleNotFoundError: No module named 'definitely_missing_dep_abc123'" in done.stderr, done.stderr


def test_load_app_wrong_type(tmp_path):
    mod = tmp_path / "cli_not_pyron.py"
    mod.write_text("app = 42\n")
    sys.path.insert(0, str(tmp_path))
    with pytest.raises(SystemExit) as ei:
        _load_app(parse_target("cli_not_pyron:app"))
    assert "expected mipyre.Mipyre" in str(ei.value)


# ---------------------------------------------------------------------------
# routes subcommand
# ---------------------------------------------------------------------------


def test_routes_prints_registered(tmp_path, capsys):
    modname = _write_app_module(tmp_path)
    main(["routes", modname])
    out = capsys.readouterr().out
    assert "GET" in out
    assert "/ping" in out
    assert "/upload" in out
    assert "body=BodyStream" in out  # flag rendered
    assert "runtime=main" in out
    assert "/health" in out  # fast route
    assert "<fast:" in out  # fast marker


def test_routes_empty_app(tmp_path, capsys):
    mod = tmp_path / "cli_empty.py"
    mod.write_text("from mipyre import Mipyre\napp = Mipyre()\n")
    sys.path.insert(0, str(tmp_path))
    main(["routes", "cli_empty"])
    assert "(no routes registered)" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# The app's own arguments, after `--`
# ---------------------------------------------------------------------------


def test_the_app_is_imported_with_its_file_and_the_arguments_after_the_separator(tmp_path, capsys):
    modname = _write_app_module(tmp_path, "import sys\nprint('argv', sys.argv)\n")
    main(["routes", modname, "--", "--db-dsn", "x"])
    expected = [str(tmp_path / "cli_fixture_app.py"), "--db-dsn", "x"]
    assert f"argv {expected}" in capsys.readouterr().out
    assert sys.argv == expected


def test_the_package_above_a_dotted_app_sees_the_app_s_arguments(tmp_path, capsys):
    pkg = tmp_path / "cli_argv_pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("import sys\nprint('package argv', sys.argv[1:])\n")
    (pkg / "app.py").write_text("from mipyre import Mipyre\napp = Mipyre()\n")
    sys.path.insert(0, str(tmp_path))
    main(["routes", "cli_argv_pkg.app", "--", "--db-dsn", "x"])
    assert "package argv ['--db-dsn', 'x']" in capsys.readouterr().out
    assert sys.argv == [str(pkg / "app.py"), "--db-dsn", "x"]


def test_an_unknown_argument_before_the_separator_says_where_the_app_s_go(tmp_path, capsys):
    modname = _write_app_module(tmp_path)
    with pytest.raises(SystemExit) as ei:
        main(["routes", modname, "--db-dsn", "x", "--", "--debug"])
    assert ei.value.code == 2
    err = capsys.readouterr().err
    assert "unrecognized arguments: --db-dsn x" in err
    assert "arguments the app parses itself go after `--`" in err


# ---------------------------------------------------------------------------
# Top-level argparse
# ---------------------------------------------------------------------------


def test_version_flag(capsys):
    with pytest.raises(SystemExit) as ei:
        main(["--version"])
    assert ei.value.code == 0
    out = capsys.readouterr().out
    assert out.startswith("mipyre ")


def test_no_subcommand_errors(capsys):
    with pytest.raises(SystemExit):
        main([])


# ---------------------------------------------------------------------------
# Module entry point
# ---------------------------------------------------------------------------


def test_module_entry_point_runs(tmp_path):
    """`python -m mipyre routes ...` should work without install."""
    modname = _write_app_module(tmp_path)
    env = os.environ.copy()
    env["PYTHONPATH"] = str(tmp_path) + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run(
        [sys.executable, "-m", "mipyre", "routes", modname],
        capture_output=True, text=True, env=env, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "/ping" in result.stdout
