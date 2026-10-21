"""No hard-coded log target in the shared crates (b/262 (5), infra_tpc review #5, infra_http review #17).

A shared crate (`crates/*` other than the extension module) is linked by more than one product.
A `target: "<product>::server"` written into one sends that
crate's lines to whichever product's logger the string names, whoever links it. Their lines
keep the default target, the module path (`mipyre_tpc::threads`, ...), and the product decides
where those go.

The rule names no product: a literal `target:` in a shared crate's `src/` is a breach unless it
is that crate's own name (`"mipyre_subinterp"` in `mipyre_subinterp`, read from its Cargo.toml), so
it holds across a rename, and a crate renamed without its literal goes red. The shared crates are found by what they are (every workspace package
that does not build a cdylib), not by a list of names, so a crate added later is covered.
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


def shared_crates(root: str) -> dict[str, str]:
    """Each workspace member whose package builds no cdylib: its directory (relative to `root`)
    -> its library name."""
    with open(os.path.join(root, "Cargo.toml"), "rb") as f:
        members = tomllib.load(f)["workspace"]["members"]
    shared = {}
    for pattern in members:
        for manifest_path in sorted(glob.glob(os.path.join(root, pattern, "Cargo.toml"))):
            with open(manifest_path, "rb") as f:
                manifest = tomllib.load(f)
            lib = manifest.get("lib", {})
            if "cdylib" not in lib.get("crate-type", []):
                name = lib.get("name", manifest["package"]["name"].replace("-", "_"))
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


def test_the_shared_crates_are_found_and_the_extension_module_is_not_one():
    shared = shared_crates(ROOT)
    assert "crates/tpc" in shared and "crates/subinterp" in shared, shared
    assert "crates/pyext" not in shared, shared


def test_no_shared_crate_hard_codes_a_log_target():
    found = literal_targets(ROOT)
    assert found == {}, f"a literal log target in a shared crate (keep the default, the module path): {found}"
