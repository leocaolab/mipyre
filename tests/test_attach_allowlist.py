"""No bare `Python::attach` outside an audited allowlist.

Once more than one interpreter has executed the engine, a bare attach on a thread with no
Python thread state is refused by the PyO3 fork, and on a thread bound to a worker it
would run under the worker's GIL. Code that enters Python from a Rust thread goes
through `run_context::with_main` / `with_interpreter` instead. Each allowlisted site states
why it is safe; the counts are exact, so a new site fails and a removed one must be
taken off the list.

`Python::attach_current` (the PyO3 fork, tag subinterp-2026-10-14.1) has a list of its
own. It skips `attach`'s checks and its `PyGILState_Ensure` / `Release` and trusts the
caller that a thread state is current, so it is never right where `attach` would be: a
site of either kind is counted in its own list, and moving a site from one to the other
shows up in both.

Scanned: every workspace package's `crates/*/src/`, the engine's `crates/pyext/src/` among them.
Code that enters Python from a Rust thread moves between crates; a rule that read only one crate
would go green the moment a guarded site left it. The counts stay exact; an allowlist entry
follows its file to the new path.
"""
from __future__ import annotations

import glob
import os
import re
import shutil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ATTACH = re.compile(r"Python::(attach|with_gil|try_attach)\s*\(")
ATTACH_CURRENT = re.compile(r"Python::attach_current\s*\(")

# path (relative to the repo root) -> (exact count, why it is safe)
ALLOWLIST = {
    # Which interpreter a Rust thread enters is decided in `crates/subinterp`.
    "crates/subinterp/src/attach.rs": (
        1,
        "with_interpreter itself: runs only when the thread is already bound to the target "
        "interpreter, so it re-attaches that thread state",
    ),
    # mipyre_subinterp's unit tests (`#[cfg(test)] mod tests`): attaches on the test's own thread
    # to the main interpreter (`Python::initialize` first). No engine code runs in it, and no
    # thread is bound to a worker when it attaches.
    "crates/subinterp/src/interp.rs": (
        1,
        "unit tests only (`mod tests`): after a sub-interpreter's prepare panicked, main still "
        "runs Python; the interruption/end protocol's other tests run on a recording host "
        "and attach nothing",
    ),
}

# The same, for `Python::attach_current`.
ALLOWLIST_CURRENT = {
    # The one place in the whole workspace that needs an API only the PyO3 fork has, so the
    # fork tag and this entry are one fact.
    "crates/subinterp/src/interp.rs": (
        1,
        "enter (SubInterpreter::with_gil / with_unit): on the thread the worker is bound to, "
        "right after its own thread "
        "state was restored, or kept current since the last unit of work (a TPC thread); "
        "registers the attach without PyGILState_Ensure",
    ),
}


def _strip_comments(line: str) -> str:
    return line.split("//", 1)[0]


def _walk_all(root: str):
    for src in scan_roots(root):
        yield from os.walk(src)


def scan_roots(root: str) -> list[str]:
    """Every Rust tree the rule covers: each workspace package's `src/` — the engine's
    (`crates/pyext/src`) and every shared crate's."""
    roots = sorted(glob.glob(os.path.join(root, "crates", "*", "src")))
    return [p for p in roots if os.path.isdir(p)]


def attach_sites(root: str, pattern: re.Pattern = ATTACH) -> dict[str, list[int]]:
    """{relative path: [line numbers]} of `pattern`'s calls in non-comment Rust code."""
    sites: dict[str, list[int]] = {}
    for dirpath, _, files in _walk_all(root):
        for name in files:
            if not name.endswith(".rs"):
                continue
            path = os.path.join(dirpath, name)
            rel = os.path.relpath(path, root)
            with open(path, encoding="utf-8") as f:
                for n, line in enumerate(f, 1):
                    if pattern.search(_strip_comments(line)):
                        sites.setdefault(rel, []).append(n)
    return sites


def violations(root: str) -> list[str]:
    return _violations(root, ATTACH, ALLOWLIST, "bare attach", "ALLOWLIST") + _violations(
        root, ATTACH_CURRENT, ALLOWLIST_CURRENT, "attach_current", "ALLOWLIST_CURRENT"
    )


def _violations(root: str, pattern, allowlist, what: str, name: str) -> list[str]:
    out = []
    sites = attach_sites(root, pattern)
    for rel, lines in sorted(sites.items()):
        allowed = allowlist.get(rel, (0, ""))[0]
        if len(lines) != allowed:
            out.append(
                f"{rel}: {len(lines)} {what} site(s) at lines {lines}, allowlist has "
                f"{allowed}. Use run_context::main_attach / attach_to, or audit the site "
                f"and update {name} with the reason."
            )
    for rel, (allowed, _) in sorted(allowlist.items()):
        if rel not in sites:
            out.append(
                f"{rel}: in {name} for {allowed} {what} site(s) but has none; remove it"
            )
    return out


def test_no_bare_attach_outside_allowlist():
    assert violations(ROOT) == []


def test_gate_catches_a_seeded_bare_attach(tmp_path):
    """The gate fails on a new bare attach, not just passes on today's tree."""
    shutil.copytree(os.path.join(ROOT, "crates"), tmp_path / "crates")
    target = tmp_path / "crates" / "server" / "src" / "respond.rs"
    with open(target, "a", encoding="utf-8") as f:
        f.write("\n#[allow(dead_code)]\nfn seeded() {\n    pyo3::Python::attach(|_py| ());\n}\n")
    found = violations(str(tmp_path))
    assert len(found) == 1, found
    assert found[0].startswith("crates/server/src/respond.rs: 1 bare attach site(s)"), found


def test_gate_catches_a_seeded_attach_current(tmp_path):
    """The `attach_current` list is a gate too: a new site fails it."""
    shutil.copytree(os.path.join(ROOT, "crates"), tmp_path / "crates")
    target = tmp_path / "crates" / "server" / "src" / "respond.rs"
    with open(target, "a", encoding="utf-8") as f:
        f.write(
            "\n#[allow(dead_code)]\nfn seeded() {\n"
            "    unsafe { pyo3::Python::attach_current(|_py| ()) };\n}\n"
        )
    found = violations(str(tmp_path))
    assert len(found) == 1, found
    assert found[0].startswith("crates/server/src/respond.rs: 1 attach_current site(s)"), found


def test_gate_catches_a_seeded_bare_attach_in_a_shared_crate(tmp_path):
    """The shared crates are scanned, not just `src/`: a site planted in one fails."""
    shutil.copytree(os.path.join(ROOT, "crates"), tmp_path / "crates")
    target = tmp_path / "crates" / "cpu" / "src" / "plan.rs"
    with open(target, "a", encoding="utf-8") as f:
        f.write("\n#[allow(dead_code)]\nfn seeded() {\n    pyo3::Python::attach(|_py| ());\n}\n")
    found = violations(str(tmp_path))
    assert len(found) == 1, found
    assert found[0].startswith("crates/cpu/src/plan.rs: 1 bare attach site(s)"), found
