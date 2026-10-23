"""The deployment's settings: the app's runtime (its code's alone, b/335 D9), and the process
lanes' settings the engine reads once at the entry point (b/335) and lays over the app's
``ProcessLanes``. The rules each value is held to are tested on the engine's pure resolve
(crates/pyext/src/boot/config.rs); these check what the app sees."""

from __future__ import annotations

import argparse
import dataclasses
import os

import pytest

from mipyre import Mipyre
from mipyre._process_lane.settings import ProcessLanes
from mipyre._routes import app_runtime
from mipyre._server_run import lane_recipe
from mipyre.engine import ConfigError, Deployment, EntryPoint, FailurePolicy, LaneRecipe, ServerFlags, StartStrategy


# -- the app's runtime -------------------------------------------------------------------
# An app's runtime is its code's (b/335 D9): no deployment value replaces it. What a deployment
# used to give it (MIPYRE_RUNTIME, `mipyre run --runtime`, [tool.mipyre] runtime) is gone; the
# environment variable is now an unknown one (test_review_m5), the key an unknown key (the engine's
# resolve unit tests).


def test_the_app_runtime_is_the_code_s_else_the_default():
    assert app_runtime(None, "Mipyre(...)") == "subinterp"
    assert app_runtime("process", "Mipyre(...)") == "process"


def test_tool_mipyre_that_is_not_a_table_is_an_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "pyproject.toml").write_text("[tool]\nmipyre = 3\n")
    with pytest.raises(ConfigError, match=r"\[tool.mipyre\] must be a table"):
        Deployment.from_process({}, EntryPoint.CliRun)


# -- process-lane settings --------------------------------------------------------------
# The deployment is read once at the entry point (Rust, b/335); the app's ProcessLanes is the code
# underneath it (D7: a deployment value wins). Each read here is the process's environment and
# working directory, set by monkeypatch.


def _lanes(monkeypatch, tmp_path, env=None, flags=None, declared=None, pyproject=None) -> LaneRecipe:
    monkeypatch.chdir(tmp_path)
    for name in [k for k in os.environ if k.startswith("MIPYRE_")]:
        monkeypatch.delenv(name)
    for name, value in (env or {}).items():
        monkeypatch.setenv(name, value)
    if pyproject is not None:
        (tmp_path / "pyproject.toml").write_text(pyproject)
    deployment = Deployment.from_process(flags or {}, EntryPoint.CliRun)
    code = ServerFlags()._with_lanes(**dataclasses.asdict(declared or ProcessLanes()))
    return deployment.lanes(code, 4)


def test_lane_defaults(monkeypatch, tmp_path):
    s = _lanes(monkeypatch, tmp_path)
    assert (s.workers, s.timeout_ms, s.queue_per_worker, s.slot_bytes, s.host_failure) == (
        4, 30_000, 128, 256 * 1024, FailurePolicy.Isolate
    )


def test_each_lane_setting_comes_from_flags_then_env_then_pyproject_then_code(monkeypatch, tmp_path):
    pyproject = "[tool.mipyre.process]\nworkers = 5\ntimeout = 2.5\n"
    env = {"MIPYRE_PROCESS_WORKERS": "6", "MIPYRE_PROCESS_TIMEOUT": "3"}
    flags = {"process.workers": "7", "process.timeout": "4.0"}
    code = ProcessLanes(workers=8, timeout=5.0)
    s = _lanes(monkeypatch, tmp_path, declared=code)
    assert (s.workers, s.timeout_ms) == (8, 5000)
    s = _lanes(monkeypatch, tmp_path, declared=code, pyproject=pyproject)
    assert (s.workers, s.timeout_ms) == (5, 2500)
    s = _lanes(monkeypatch, tmp_path, declared=code, env=env, pyproject=pyproject)
    assert (s.workers, s.timeout_ms) == (6, 3000)
    s = _lanes(monkeypatch, tmp_path, declared=code, flags=flags, env=env, pyproject=pyproject)
    assert (s.workers, s.timeout_ms) == (7, 4000)


def test_an_unknown_host_failure_policy_names_what_was_written(monkeypatch, tmp_path):
    with pytest.raises(ConfigError, match="MIPYRE_LANE_HOST_FAILURE: \"bogus\" is invalid"):
        _lanes(monkeypatch, tmp_path, env={"MIPYRE_LANE_HOST_FAILURE": "bogus"})
    assert _lanes(monkeypatch, tmp_path, env={"MIPYRE_LANE_HOST_FAILURE": "exit"}).host_failure == FailurePolicy.Exit


def test_the_timeout_is_in_seconds_in_the_environment_too(monkeypatch, tmp_path):
    assert _lanes(monkeypatch, tmp_path, env={"MIPYRE_PROCESS_TIMEOUT": "30"}).timeout_ms == 30_000
    assert _lanes(monkeypatch, tmp_path, env={"MIPYRE_PROCESS_TIMEOUT": "0.25"}).timeout_ms == 250


def test_a_host_failure_policy_in_pyproject_is_read_and_a_bad_one_names_the_file(monkeypatch, tmp_path):
    exit = _lanes(monkeypatch, tmp_path, pyproject='[tool.mipyre.process]\nhost_failure = "exit"\n')
    assert exit.host_failure == FailurePolicy.Exit
    with pytest.raises(ConfigError) as raised:
        _lanes(monkeypatch, tmp_path, pyproject='[tool.mipyre.process]\nhost_failure = "panic"\n')
    assert str(tmp_path / "pyproject.toml") + ":2: [tool.mipyre.process] host_failure: \"panic\" is invalid" in str(raised.value)


def test_an_unknown_start_strategy_is_an_error_with_what_was_written(monkeypatch, tmp_path):
    with pytest.raises(ConfigError, match='ProcessLanes\\(start=\\): "preload" is invalid'):
        _lanes(monkeypatch, tmp_path, declared=ProcessLanes(start="preload"))


@pytest.mark.parametrize("raw", ["0", "two"])
def test_a_lane_size_below_one_or_not_a_number_is_an_error(monkeypatch, tmp_path, raw):
    with pytest.raises(ConfigError, match=f'MIPYRE_PROCESS_QUEUE: "{raw}" is invalid for process.queue_per_worker'):
        _lanes(monkeypatch, tmp_path, env={"MIPYRE_PROCESS_QUEUE": raw})


# b/421: a bad value names the place it was written and quotes it as written.


@pytest.mark.parametrize(
    ("var", "raw"),
    [
        ("MIPYRE_PROCESS_WORKERS", "two"),
        ("MIPYRE_PROCESS_SLOT_BYTES", "0"),
        ("MIPYRE_PROCESS_TIMEOUT", "soon"),
        ("MIPYRE_PROCESS_SPIN_US", "fast"),
        ("MIPYRE_PROCESS_START", "preload"),
    ],
)
def test_a_bad_lane_variable_is_named_with_what_was_written(monkeypatch, tmp_path, var, raw):
    with pytest.raises(ConfigError) as raised:
        _lanes(monkeypatch, tmp_path, env={var: raw})
    assert f'{var}: "{raw}" is invalid' in str(raised.value)
    assert "invalid literal" not in str(raised.value)


def test_a_bad_lane_value_in_pyproject_names_the_file_and_quotes_the_seconds_written(monkeypatch, tmp_path):
    with pytest.raises(ConfigError) as raised:
        _lanes(monkeypatch, tmp_path, pyproject='[tool.mipyre.process]\ntimeout = "soon"\n')
    assert ':2: [tool.mipyre.process] timeout: "soon" is invalid' in str(raised.value)
    # Below a millisecond: the error quotes the seconds written, not the 0 ms they came to.
    with pytest.raises(ConfigError, match=r':2: \[tool.mipyre.process\] timeout: "0.0001" is invalid'):
        _lanes(monkeypatch, tmp_path, pyproject="[tool.mipyre.process]\ntimeout = 0.0001\n")


def test_a_bad_lane_value_in_the_code_or_the_flags_names_which(monkeypatch, tmp_path):
    with pytest.raises(ConfigError, match=r'ProcessLanes\(workers=\): "0" is invalid'):
        _lanes(monkeypatch, tmp_path, declared=ProcessLanes(workers=0))
    with pytest.raises(ConfigError, match=r'mipyre run --process-queue: "0" is invalid'):
        _lanes(monkeypatch, tmp_path, flags={"process.queue_per_worker": "0"})


def test_the_lanes_read_the_deployment_the_entry_point_read(monkeypatch, tmp_path):
    # b/421: not the process's os.environ at the time the lanes start.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MIPYRE_PROCESS_WORKERS", "3")
    deployment = Deployment.from_process({}, EntryPoint.CliRun)
    monkeypatch.setenv("MIPYRE_PROCESS_WORKERS", "9")
    assert lane_recipe(Mipyre(), deployment, ServerFlags()).workers == 3


def test_the_cli_process_flags_are_the_deployment_s_lane_settings(monkeypatch, tmp_path):
    from mipyre.cli import _given_flags

    args = argparse.Namespace(**{
        "process.start": "fork", "process.workers": "3", "process.timeout": "1.5",
        "process.queue_per_worker": "9", "process.slot_bytes": "4096",
    })
    s = _lanes(monkeypatch, tmp_path, flags=_given_flags(args), env={"MIPYRE_PROCESS_WORKERS": "6"})
    assert (s.start, s.workers, s.timeout_ms, s.queue_per_worker, s.slot_bytes) == (StartStrategy.Fork, 3, 1500, 9, 4096)
