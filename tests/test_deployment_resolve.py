"""``Deployment.from_process`` reads this process's environment and the ``pyproject.toml`` in its
working directory once, with the command line's flags. The rules themselves (the order
of the sources, each shape) are tested on the pure ``Deployment::resolve`` in
crates/pyext/src/boot/config.rs; these check the wiring: a real environment, a real file, the
Python face."""

import json
import os
import subprocess
import sys
import textwrap

import pytest

PROBE = textwrap.dedent(
    """
    import json, sys
    from mipyre.engine import ConfigError, Deployment, EntryPoint
    try:
        d = Deployment.from_process(json.loads(sys.argv[1]), EntryPoint.CliRun)
    except ConfigError as e:
        print(json.dumps({"errors": str(e).splitlines()}))
    else:
        print(json.dumps({"summary": d.summary()}))
    """
)


def _resolve(tmp_path, env: dict[str, str], flags: dict[str, str], pyproject: str | None = None) -> dict:
    if pyproject is not None:
        (tmp_path / "pyproject.toml").write_text(pyproject)
    base = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    r = subprocess.run([sys.executable, "-c", PROBE, json.dumps(flags)], cwd=tmp_path, env={**base, **env},
                       capture_output=True, text=True, timeout=60, check=True)
    return json.loads(r.stdout)


def test_each_source_is_read_once_and_named(tmp_path):
    got = _resolve(tmp_path, {"MIPYRE_PORT": "9100"}, {"threads": "8"},
                   "[tool.mipyre]\nhost = \"0.0.0.0\"\nport = 9000\n")
    assert got == {"summary": [
        "threads=8 (mipyre run --threads)",
        "host=0.0.0.0 (" + str(tmp_path / "pyproject.toml") + ":2: [tool.mipyre] host)",
        "port=9100 (MIPYRE_PORT)",
    ]}


def test_every_error_comes_back_in_one_config_error(tmp_path):
    got = _resolve(tmp_path, {"MIPYRE_GC_MODE": "sometimes"}, {"threads": "0"},
                   "[tool.mipyre.process]\nworkers = 0\n")
    assert len(got["errors"]) == 3, got
    assert any(e.startswith("MIPYRE_GC_MODE:") for e in got["errors"]), got


def test_a_name_that_is_no_flag_is_refused():
    from mipyre.engine import Deployment, EntryPoint

    with pytest.raises(ValueError, match='"thread" is not a mipyre flag'):
        Deployment.from_process({"thread": "2"}, EntryPoint.CliRun)


def test_no_deployment_is_empty():
    from mipyre.engine import Deployment, EntryPoint

    d = Deployment()
    assert (d.entry, d.summary()) == (EntryPoint.Embedded, [])
