"""The PyO3 multi-phase fixture (tests/fixtures/pyo3_mp), built twice: the variant without the feature does not
declare per-interpreter GIL support, the one with it does (see the crate's lib.rs). CPython's own check decides on
that declaration alone, so each test's expectation follows from how its variant was built. Built by the session
fixture `pyo3_fixture_dirs` (conftest.py)."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
# A standalone crate (its own [workspace]): test asset, not a workspace package, not in any wheel.
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "pyo3_mp"
MODULE = "pyo3_mp"
_VARIANTS = {"undeclared": [], "declared": ["--features", "declare_per_interpreter"]}


def build(root_dir: Path) -> dict[str, Path]:
    """{variant: dir under `root_dir` holding the importable module}. Built with cargo (an up-to-date build costs
    next to nothing). No cargo is a failure, never a skip."""
    cargo = shutil.which("cargo")
    if cargo is None:
        pytest.fail("cargo not found: the tests build their PyO3 fixture (tests/fixtures/pyo3_mp) with cargo; "
                    "install a Rust toolchain")
    env = dict(os.environ, PYO3_PYTHON=sys.executable)
    target = Path(os.environ.get("CARGO_TARGET_DIR") or FIXTURE_DIR / "target") / "debug"
    lib_ext = ".dylib" if sys.platform == "darwin" else ".so"
    suffix = sysconfig.get_config_var("EXT_SUFFIX")
    dirs = {}
    for variant, feature_args in _VARIANTS.items():
        # Both variants build the same lib path, so each is copied out before the next build replaces it.
        cmd = [cargo, "build", "--lib", "--manifest-path", str(FIXTURE_DIR / "Cargo.toml"), *feature_args]
        built = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
        if built.returncode != 0:
            pytest.fail(f"cargo build of the PyO3 fixture ({variant}) failed:\n{built.stderr[-3000:]}")
        dest = root_dir / variant
        dest.mkdir()
        shutil.copyfile(target / f"lib{MODULE}{lib_ext}", dest / f"{MODULE}{suffix}")
        dirs[variant] = dest
    return dirs
