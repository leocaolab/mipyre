"""No hard-coded log target in the shared crates.

A shared crate is a workspace package that builds no cdylib and is not one of the server's parts
(`SERVER_PARTS`). A literal `target:` in its `src/` is refused unless it is the crate's own library
name or a path under it, read from its Cargo.toml; its other lines keep the default target, the
module path. The server's parts keep mipyre's documented targets (`mipyre::server`, `mipyre::handler`).
"""
from __future__ import annotations

import glob
import os
import re
import tomllib

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# `tracing::warn!(target: "x", ...)` / `log::info!(target: "x", ...)`, the macro's arguments
# possibly spread over several lines.
LITERAL_TARGET = re.compile(r"\btarget\s*:\s*\"([^\"]*)\"")

SERVER_PARTS = {"mipyre_process", "mipyre_runtime", "mipyre_server"}


def shared_crates(root: str) -> dict[str, str]:
    """Each workspace member whose package builds no cdylib and is not one of the server's parts:
    its directory (relative to `root`) -> its library name."""
    with open(os.path.join(root, "Cargo.toml"), "rb") as f:
        members = tomllib.load(f)["workspace"]["members"]
    shared = {}
    for pattern in members:
        for manifest_path in sorted(glob.glob(os.path.join(root, pattern, "Cargo.toml"))):
            with open(manifest_path, "rb") as f:
                manifest = tomllib.load(f)
            lib = manifest.get("lib", {})
            name = lib.get("name", manifest["package"]["name"].replace("-", "_"))
            if "cdylib" not in lib.get("crate-type", []) and name not in SERVER_PARTS:
                shared[os.path.relpath(os.path.dirname(manifest_path), root)] = name
    return shared


def literal_targets(root: str) -> dict[str, int]:
    found: dict[str, int] = {}
    for crate, own in shared_crates(root).items():
        for dirpath, _dirs, files in os.walk(os.path.join(root, crate, "src")):
            for name in files:
                if not name.endswith(".rs"):
                    continue
                path = os.path.join(dirpath, name)
                with open(path, encoding="utf-8") as f:
                    code = "\n".join(line.split("//", 1)[0] for line in f)
                count = sum(
                    1 for t in LITERAL_TARGET.findall(code) if t != own and not t.startswith(own + "::")
                )
                if count:
                    found[os.path.relpath(path, root)] = count
    return found


def test_the_shared_crates_are_found_and_the_engine_and_its_parts_are_not():
    shared = shared_crates(ROOT)
    assert "crates/tpc" in shared and "crates/subinterp" in shared, shared
    assert "crates/pyext" not in shared, shared
    assert "crates/runtime" not in shared and "crates/process" not in shared, shared


def test_no_shared_crate_hard_codes_a_log_target():
    found = literal_targets(ROOT)
    assert found == {}, f"a literal log target in a shared crate (keep the default, the module path): {found}"
