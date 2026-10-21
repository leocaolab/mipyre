"""The deployment config resolvers (b/304): each takes what it reads as arguments — the
environment as a mapping, ``[tool.mipyre]`` as a ``ConfigFile``, the launcher's flags as
values — so every bad value is a call here, not a process that fails to start. The one
process that does fail to start is test_review_m5's ``mipyre run`` runtime typo."""

from __future__ import annotations

import argparse

import pytest

from mipyre import Mipyre
from mipyre._runtime import (
    ConfigFile,
    DeployedRuntime,
    LaneSettings,
    ProcessLanes,
    RuntimeDeclarationError,
    deployment_runtime,
    resolve_app_runtime,
)
from mipyre.app import _Launch, _ServeSettings
from mipyre.cli import _lane_flags


def _config(tmp_path, text: str) -> ConfigFile | None:
    (tmp_path / "pyproject.toml").write_text(text)
    return ConfigFile.load(str(tmp_path))


# -- [tool.mipyre] ----------------------------------------------------------------------


def test_no_pyproject_or_no_table_is_no_config(tmp_path):
    assert ConfigFile.load(str(tmp_path)) is None
    assert _config(tmp_path, "[project]\nname = 'x'\n") is None


def test_a_table_is_read_with_its_file(tmp_path):
    config = _config(tmp_path, '[tool.mipyre]\nruntime = "main"\n[tool.mipyre.process]\nworkers = 3\n')
    assert config is not None
    assert config.path == str(tmp_path / "pyproject.toml")
    assert config.table["runtime"] == "main"
    assert config.process() == {"workers": 3}


def test_tool_mipyre_that_is_not_a_table_is_an_error(tmp_path):
    with pytest.raises(RuntimeDeclarationError, match=r"\[tool.mipyre\] must be a table, got 3"):
        _config(tmp_path, "[tool]\nmipyre = 3\n")


def test_tool_mipyre_process_that_is_not_a_table_is_an_error(tmp_path):
    config = _config(tmp_path, '[tool.mipyre]\nprocess = "fork"\n')
    assert config is not None
    with pytest.raises(ValueError, match=r"\[tool.mipyre.process\] must be a table, got 'fork'"):
        config.process()


# -- the deployment's runtime -----------------------------------------------------------


def test_no_deployment_runtime():
    assert deployment_runtime({}, None) is None
    assert deployment_runtime({"MIPYRE_RUNTIME": ""}, None) is None


def test_a_runtime_typo_in_the_environment_names_the_variable_and_the_value():
    with pytest.raises(RuntimeDeclarationError) as err:
        deployment_runtime({"MIPYRE_RUNTIME": "subinterpter"}, None)
    assert "MIPYRE_RUNTIME" in str(err.value) and "'subinterpter'" in str(err.value)


def test_the_environment_comes_before_pyproject(tmp_path):
    config = _config(tmp_path, '[tool.mipyre]\nruntime = "main"\n')
    assert deployment_runtime({"MIPYRE_RUNTIME": "process"}, config) == DeployedRuntime(
        "process", "the environment variable MIPYRE_RUNTIME"
    )
    assert deployment_runtime({}, config) == DeployedRuntime(
        "main", f"runtime in [tool.mipyre] of {tmp_path / 'pyproject.toml'}"
    )


def test_a_runtime_typo_in_pyproject_names_the_file(tmp_path):
    config = _config(tmp_path, '[tool.mipyre]\nruntime = "gpu"\n')
    with pytest.raises(RuntimeDeclarationError, match=r"runtime in \[tool.mipyre\] of .*pyproject.toml: runtime='gpu'"):
        deployment_runtime({}, config)


def test_the_app_runtime_is_the_code_s_else_the_deployment_s_else_the_default():
    deployed = DeployedRuntime("main", "here")
    assert resolve_app_runtime(None, "Mipyre(...)", None) == "subinterp"
    assert resolve_app_runtime(None, "Mipyre(...)", deployed) == "main"
    assert resolve_app_runtime("main", "Mipyre(...)", deployed) == "main"
    assert resolve_app_runtime("process", "Mipyre(...)", None) == "process"


def test_code_and_deployment_disagreeing_is_an_error_naming_both():
    deployed = deployment_runtime({"MIPYRE_RUNTIME": "process"}, None)
    with pytest.raises(RuntimeDeclarationError) as err:
        resolve_app_runtime("subinterp", "Mipyre(...)", deployed)
    assert "Mipyre(...) declares runtime='subinterp'" in str(err.value)
    assert "the environment variable MIPYRE_RUNTIME says 'process'" in str(err.value)


def test_the_cli_flag_names_itself_as_the_source():
    deployed = deployment_runtime(
        {"MIPYRE_RUNTIME": "process", "MIPYRE_RUNTIME_SOURCE": "mipyre run --runtime process"}, None
    )
    with pytest.raises(RuntimeDeclarationError, match="mipyre run --runtime process says 'process'"):
        resolve_app_runtime("main", "Mipyre(...)", deployed)


def test_mipyre_reads_the_deployment_from_the_working_directory(tmp_path, monkeypatch):
    """The wiring: ``Mipyre()`` resolves against ``os.environ`` and ./pyproject.toml, and
    its routes inherit the result."""
    (tmp_path / "pyproject.toml").write_text('[tool.mipyre]\nruntime = "main"\n')
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MIPYRE_RUNTIME", raising=False)
    monkeypatch.delenv("MIPYRE_RUNTIME_SOURCE", raising=False)
    app = Mipyre()
    app.get("/a", lambda req: "a")
    app.get("/b", lambda req: "b", runtime="process")
    assert app.runtime == "main"
    assert [(r.path, r.runtime) for r in app.routes] == [("/a", "main"), ("/b", "process")]


# -- run()'s settings -------------------------------------------------------------------


@pytest.mark.parametrize("var", ["MIPYRE_TLS_PORTS", "MIPYRE_PLAIN_PORTS"])
@pytest.mark.parametrize("raw", ["443x", "70000", "0"])
def test_a_bad_port_list_names_the_variable_and_the_value(var, raw):
    with pytest.raises(ValueError) as err:
        _ServeSettings.resolve(env={var: raw}, port=1)
    assert var in str(err.value) and raw in str(err.value), err.value


def test_port_lists_parse():
    s = _ServeSettings.resolve(env={"MIPYRE_TLS_PORTS": "8443, 9443,", "MIPYRE_PLAIN_PORTS": "8082"}, port=1)
    assert (s.extra_tls_ports, s.extra_plain_ports) == ([8443, 9443], [8082])
    assert _ServeSettings.resolve(env={}, port=1).extra_plain_ports is None


def test_an_argument_comes_before_the_environment():
    env = {"MIPYRE_HOST": "0.0.0.0", "MIPYRE_PORT": "9000", "MIPYRE_PLAIN_PORTS": "8082"}
    s = _ServeSettings.resolve(env=env, host="127.0.0.1", port=1, extra_plain_ports=[8083])
    assert (s.host, s.port, s.extra_plain_ports) == ("127.0.0.1", 1, [8083])
    s = _ServeSettings.resolve(env=env)
    assert (s.host, s.port, s.extra_plain_ports) == ("0.0.0.0", 9000, [8082])
    assert _ServeSettings.resolve(env={}).port == 8000


def test_a_port_that_is_not_an_integer_names_the_variable():
    with pytest.raises(ValueError, match=r"MIPYRE_PORT='80a' is not a valid integer"):
        _ServeSettings.resolve(env={"MIPYRE_PORT": "80a"})


def test_logging_is_mipyre_log_or_the_launcher_s():
    assert not _ServeSettings.resolve(env={}, port=1).log
    assert not _ServeSettings.resolve(env={"MIPYRE_LOG": "0"}, port=1).log
    assert _ServeSettings.resolve(env={"MIPYRE_LOG": "1"}, port=1).log
    assert _ServeSettings.resolve(env={}, launch=_Launch(log=True), port=1).log


# -- process-lane settings --------------------------------------------------------------


def _lanes(declared=None, launched=None, env=None, config=None) -> LaneSettings:
    return LaneSettings.resolve(declared, 4, launched=launched, env=env or {}, config=config)


def test_lane_defaults():
    s = _lanes()
    assert (s.workers, s.timeout_ms, s.queue_per_worker, s.slot_bytes, s.host_failure) == (
        4, 30_000, 128, 256 * 1024, "isolate"
    )


def test_each_lane_setting_comes_from_code_then_flags_then_env_then_pyproject(tmp_path):
    config = _config(tmp_path, "[tool.mipyre.process]\nworkers = 5\ntimeout = 2.5\n")
    env = {"MIPYRE_PROCESS_WORKERS": "6", "MIPYRE_PROCESS_TIMEOUT_MS": "3000"}
    flags = ProcessLanes(workers=7, timeout=4.0)
    code = ProcessLanes(workers=8, timeout=5.0)
    assert (_lanes(config=config).workers, _lanes(config=config).timeout_ms) == (5, 2500)
    assert (_lanes(env=env, config=config).workers, _lanes(env=env, config=config).timeout_ms) == (6, 3000)
    s = _lanes(launched=flags, env=env, config=config)
    assert (s.workers, s.timeout_ms) == (7, 4000)
    s = _lanes(declared=code, launched=flags, env=env, config=config)
    assert (s.workers, s.timeout_ms) == (8, 5000)


def test_an_unknown_host_failure_policy_names_what_was_written():
    with pytest.raises(ValueError, match="host_failure must be 'isolate' or 'exit', got 'bogus'"):
        _lanes(env={"MIPYRE_LANE_HOST_FAILURE": "bogus"})
    assert _lanes(env={"MIPYRE_LANE_HOST_FAILURE": "exit"}).host_failure == "exit"


def test_an_unknown_start_strategy_is_an_error_with_what_was_written():
    with pytest.raises(ValueError, match='start="preload" is not one of "import", "fork"'):
        _lanes(declared=ProcessLanes(start="preload"))


@pytest.mark.parametrize("raw", ["0", "two"])
def test_a_lane_size_below_one_or_not_a_number_is_an_error(raw):
    with pytest.raises(ValueError, match=f"process lanes: queue_per_worker must be .*got '{raw}'"):
        _lanes(env={"MIPYRE_PROCESS_QUEUE": raw})


# b/421: a bad value names the place it was written and quotes it as written.


@pytest.mark.parametrize(
    ("env", "said"),
    [
        ({"MIPYRE_PROCESS_WORKERS": "two"}, "workers must be an integer, got 'two' (the environment variable MIPYRE_PROCESS_WORKERS)"),
        ({"MIPYRE_PROCESS_SLOT_BYTES": "0"}, "slot_bytes must be >= 1, got '0' (the environment variable MIPYRE_PROCESS_SLOT_BYTES)"),
        (
            {"MIPYRE_PROCESS_TIMEOUT_MS": "1.5"},
            "timeout must be a whole number of milliseconds, got '1.5' (the environment variable MIPYRE_PROCESS_TIMEOUT_MS)",
        ),
        (
            {"MIPYRE_PROCESS_SPIN_US": "fast"},
            "the environment variable MIPYRE_PROCESS_SPIN_US must be an integer (microseconds), got 'fast'",
        ),
        ({"MIPYRE_PROCESS_START": "preload"}, "(the environment variable MIPYRE_PROCESS_START)"),
    ],
)
def test_a_bad_lane_variable_is_named_with_what_was_written(env, said):
    with pytest.raises(ValueError) as raised:
        _lanes(env=env)
    assert said in str(raised.value)
    assert "invalid literal" not in str(raised.value)


def test_a_bad_lane_value_in_pyproject_names_the_file_and_quotes_the_seconds_written(tmp_path):
    config = _config(tmp_path, '[tool.mipyre.process]\ntimeout = "soon"\n')
    with pytest.raises(ValueError) as raised:
        _lanes(config=config)
    assert f"timeout must be a number of seconds, got 'soon' (timeout in [tool.mipyre.process] of {config.path})" in str(
        raised.value
    )
    # Below a millisecond: the error quotes the seconds written, not the 0 ms they came to.
    config = _config(tmp_path, "[tool.mipyre.process]\ntimeout = 0.0001\n")
    with pytest.raises(ValueError, match=r"timeout must be at least 1 ms, got 0\.0001 \(timeout in"):
        _lanes(config=config)


def test_a_bad_lane_value_in_the_code_or_the_flags_names_which():
    with pytest.raises(ValueError, match=r"got 0 \(Mipyre\(process=ProcessLanes\(workers=\.\.\.\)\)\)"):
        _lanes(declared=ProcessLanes(workers=0))
    with pytest.raises(ValueError, match=r"queue_per_worker must be >= 1, got 0 \(mipyre run --process-queue\)"):
        _lanes(launched=ProcessLanes(queue_per_worker=0))


def test_the_lanes_read_the_environment_the_server_was_resolved_from(tmp_path, monkeypatch):
    # b/421: not the process's os.environ, which `run()` reads only to hand it in.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MIPYRE_PROCESS_WORKERS", "9")
    settings = _ServeSettings.resolve(env={"MIPYRE_PROCESS_WORKERS": "3"}, port=1)
    assert Mipyre()._lane_settings(settings).workers == 3


def test_the_cli_process_flags_are_the_launcher_s_lane_settings():
    args = argparse.Namespace(
        process_start="fork", process_workers=3, process_timeout=1.5, process_queue=9, process_slot_bytes=4096
    )
    assert _lane_flags(args) == ProcessLanes(start="fork", workers=3, timeout=1.5, queue_per_worker=9, slot_bytes=4096)
    s = _lanes(launched=_lane_flags(args), env={"MIPYRE_PROCESS_WORKERS": "6"})
    assert (s.workers, s.timeout_ms, s.queue_per_worker, s.slot_bytes) == (3, 1500, 9, 4096)
