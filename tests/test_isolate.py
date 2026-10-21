"""app.isolate() — per-worker C-extension copies.

Verifies numpy loads in EVERY own-GIL sub-interpreter worker (no
"cannot load module more than once") and each worker's numpy resolves to its
own isolated copy, driven concurrently so requests spread across workers.
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import shutil
import signal
import subprocess
import sys
import sysconfig
import urllib.request
from pathlib import Path

import pytest

from tests._helpers import FINALIZE_MARK_SNIPPET, bound_port, finalized, read_file

_supported = pytest.mark.skipif(
    sys.platform not in ("linux", "darwin"),
    reason="isolate() clones via cp -c / cp --reflink (Linux/macOS)",
)

_SERVER = '''
from mipyre import Mipyre
app = Mipyre(runtime="isolated")
import numpy   # loaded by every worker at start (what app.isolate("numpy") did up front)

import os as _os
_ISO_DIR = _os.environ.get("MIPYRE_ISOLATE_DIR", "/tmp/mipyre-isolate")

@app.get("/np")
def np_op(req):
    import numpy as np
    import _interpreters
    import time
    time.sleep(0.02)  # hold the worker briefly so concurrency spreads across workers
    return {"numpy": np.__version__,
             "interp": _interpreters.get_current()[0],
             "isolated": np.__file__.startswith(_ISO_DIR)}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0)
'''


@pytest.fixture
def isolate_server(tmp_path):
    pytest.importorskip("numpy")
    script = tmp_path / "iso_server.py"
    script.write_text(_SERVER)
    copies = tmp_path / "copies"
    env = dict(
        os.environ,
        MIPYRE_WORKERS="4",
        MIPYRE_ISOLATE_DIR=str(copies),
    )
    log = tmp_path / "iso_server.log"
    with open(log, "wb") as out:
        proc = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=out, stderr=subprocess.STDOUT, env=env,
        )
    try:
        # The workers clone numpy and import it before the server listens, so once it
        # listens it answers: one request, waited for by its answer.
        url = f"http://127.0.0.1:{bound_port(read_file(str(log)), proc)}/np"
        urllib.request.urlopen(url).read()
        yield url, copies
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@_supported
def test_isolate_numpy_across_workers(isolate_server):
    url, copies = isolate_server

    def hit(_):
        r = urllib.request.urlopen(url)
        return json.loads(r.read())

    with concurrent.futures.ThreadPoolExecutor(16) as ex:
        results = list(ex.map(hit, range(200)))

    # Every response: numpy loaded (no "cannot load module more than once") and
    # resolved to an isolated per-worker copy.
    assert all("numpy" in r for r in results)
    assert all(r["isolated"] for r in results), "some worker's numpy was not isolated"

    # The decisive check: EVERY one of the 4 workers cloned its own numpy — i.e.
    # 4 independent numpy copies coexist in one process, which is exactly what
    # bare sub-interpreters cannot do (they'd hit "cannot load module more than once").
    worker_numpys = list(copies.glob("*/w*/numpy"))
    assert len(worker_numpys) == 4, f"expected 4 per-worker numpy copies, got {len(worker_numpys)}"


# -- Reactive auto-isolate (no app.isolate() call at all) ---------------------
#
# The self-healing import hook (_bootstrap._iso_import) must make an UNMODIFIED
# `import numpy` work in every own-GIL worker: the first import trips
# "does not support loading in subinterpreters", the hook reads the offending
# module out of the error, clones numpy, and retries — with zero app.isolate().

_SERVER_AUTO = '''
from mipyre import Mipyre
app = Mipyre(runtime="isolated")
# NOTE: no app.isolate(...) — isolation must happen reactively.

import os as _os
_ISO_DIR = _os.environ.get("MIPYRE_ISOLATE_DIR", "/tmp/mipyre-isolate")

# Top-level `import numpy` (the realistic way a user writes it): every worker
# runs the script body at init, so each independently trips the isolation error
# and reactively clones its own numpy — no reliance on request load spreading
# across all workers (pyre may route a burst to a single worker).
import numpy as np

@app.get("/np")
def np_op(req):
    import _interpreters
    import time
    time.sleep(0.02)
    return {"numpy": np.__version__,
             "interp": _interpreters.get_current()[0],
             "isolated": np.__file__.startswith(_ISO_DIR)}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0)
'''


@pytest.fixture
def auto_isolate_server(tmp_path):
    pytest.importorskip("numpy")
    script = tmp_path / "auto_iso_server.py"
    script.write_text(_SERVER_AUTO)
    copies = tmp_path / "copies"
    env = dict(
        os.environ,
        MIPYRE_WORKERS="4",
        MIPYRE_ISOLATE_DIR=str(copies),
    )
    log = tmp_path / "auto_iso_server.log"
    with open(log, "wb") as out:
        proc = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=out, stderr=subprocess.STDOUT, env=env,
        )
    try:
        # The workers clone numpy and import it before the server listens, so once it
        # listens it answers: one request, waited for by its answer.
        url = f"http://127.0.0.1:{bound_port(read_file(str(log)), proc)}/np"
        urllib.request.urlopen(url).read()
        yield url, copies
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@_supported
def test_auto_isolate_numpy_without_declaration(auto_isolate_server):
    url, copies = auto_isolate_server

    def hit(_):
        r = urllib.request.urlopen(url)
        return json.loads(r.read())

    with concurrent.futures.ThreadPoolExecutor(16) as ex:
        results = list(ex.map(hit, range(200)))

    # Every response: numpy loaded and resolved to an isolated per-worker copy —
    # achieved reactively, with no app.isolate("numpy") in the server script.
    assert all("numpy" in r for r in results)
    assert all(r["isolated"] for r in results), "reactive auto-isolate did not isolate numpy"

    # Reactive isolations land under the stable `auto` bucket (no declared-lib
    # signature); each of the 4 workers cloned its own numpy there.
    worker_numpys = list(copies.glob("auto/w*/numpy"))
    assert len(worker_numpys) == 4, f"expected 4 auto-isolated numpy copies, got {len(worker_numpys)}"


# ---------------------------------------------------------------------------
# Coverage for the v2.7.0 changes that manual/grill runs exercised but the
# pytest suite did not: warm restart (reused clone dir), graceful SIGINT
# shutdown (no cross-arena abort + shutdown hooks still run), and the built-in
# single-phase extension fallback (faulthandler, pulled in by sklearn→joblib→
# loky) loading shared under the transient override.
# ---------------------------------------------------------------------------


def _start_server(script_text, tmp_path, name, workers=4, isolate_dir=None, env_extra=None):
    """Write a server script (it binds port 0), launch it as a subprocess (logs to a
    file), return (proc, log_path). Caller waits for readiness via `_wait_ready`."""
    script = tmp_path / f"{name}.py"
    script.write_text(script_text.format(port=0))
    log = tmp_path / f"{name}.log"
    env = dict(os.environ, MIPYRE_WORKERS=str(workers))
    if isolate_dir is not None:
        env["MIPYRE_ISOLATE_DIR"] = str(isolate_dir)
    if env_extra:
        env.update(env_extra)
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        stdout=open(log, "wb"), stderr=subprocess.STDOUT, env=env,
    )
    return proc, log


def _wait_ready(path, proc, log):
    """GETs `path` on the server once it listens; its JSON body. Waits for the "Listening on"
    line (or the server exiting first, which fails with its output) and then for the answer,
    for no length of time (b/325)."""
    try:
        port = bound_port(read_file(str(log)), proc)
    except RuntimeError as e:
        pytest.fail(str(e))
    r = urllib.request.urlopen(f"http://127.0.0.1:{port}{path}")
    assert r.status == 200, r.status
    return json.loads(r.read())


def _stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


_SERVER_WARM = '''
from mipyre import Mipyre
app = Mipyre(runtime="isolated")
import os as _o
_D = _o.environ.get("MIPYRE_ISOLATE_DIR", "/tmp/mipyre-isolate")
import numpy as np   # top-level, no app.isolate() -> reactive auto-isolate
@app.get("/np")
def h(req):
    return {{"v": np.__version__, "isolated": np.__file__.startswith(_D)}}
if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port})
'''


@_supported
def test_auto_isolate_survives_warm_restart(tmp_path):
    """Cold run then WARM run on the SAME clone dir. The warm run reuses the
    existing per-worker clones; a regression where the clone SOURCE resolved to
    a sibling clone (worker_dir on sys.path before cloning) rmtree'd the clone
    onto itself and the retry fell back to the shared original → "cannot load
    module more than once". Both runs must serve isolated numpy."""
    pytest.importorskip("numpy")
    copies = tmp_path / "copies"
    for run in range(2):  # run 0 = cold (creates clones); run 1 = warm (reuses)
        proc, log = _start_server(_SERVER_WARM, tmp_path, f"warm{run}",
                                  workers=4, isolate_dir=copies)
        try:
            r = _wait_ready("/np", proc, log)
            assert r["isolated"], f"run {run} ({'cold' if run == 0 else 'warm'}): numpy not isolated"
        finally:
            _stop(proc)


_SERVER_SHUTDOWN = FINALIZE_MARK_SNIPPET + '''
from mipyre import Mipyre
app = Mipyre(runtime="isolated")
import os as _o
_MARK = _o.environ["SHUTDOWN_MARK"]
import numpy as np   # isolated single-phase ext -> the teardown-crash trigger
@app.on_shutdown
def _bye():
    with open(_MARK, "w") as f:
        f.write("ran")
@app.get("/np")
def h(req):
    return {{"v": np.__version__}}
if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port})
'''


@_supported
def test_graceful_sigint_no_abort_and_hooks_run(tmp_path):
    """Finalizing a worker that loaded an isolated single-phase ext used to abort on a
    cross-arena free at Py_EndInterpreter (~50% on macOS): the extension's objects were
    created by the main interpreter. Isolated extensions are now initialized inside
    their worker, so a graceful SIGINT stop runs the shutdown hooks and then finalizes
    normally (SIGINT is ignored first so it can't interrupt the hooks).
    Assert: clean exit (rc==0, not SIGABRT), hook ran, AND the process finalized (an
    `os._exit(0)` after the hooks, 5c886025's cover-up, passes the first two)."""
    pytest.importorskip("numpy")
    mark = tmp_path / "shutdown.mark"
    finalize_mark = tmp_path / "finalize.mark"
    proc, log = _start_server(_SERVER_SHUTDOWN, tmp_path, "shutdown",
                              workers=4, isolate_dir=tmp_path / "copies",
                              env_extra={"SHUTDOWN_MARK": str(mark),
                                         "FINALIZE_MARK": str(finalize_mark)})
    try:
        _wait_ready("/np", proc, log)
        proc.send_signal(signal.SIGINT)
        rc = proc.wait()
    finally:
        if proc.poll() is None:
            proc.kill()
    assert rc != -signal.SIGABRT, (
        "sub-interpreter teardown aborted (SIGABRT) on graceful shutdown:\n"
        f"{log.read_text(errors='replace')[-1800:]}"
    )
    assert rc == 0, f"expected a clean exit after normal finalization, got rc={rc}"
    assert mark.exists(), "shutdown hook did not run before the process exited"
    assert finalized(finalize_mark), (
        "the process exited without finalizing (no atexit callback ran):\n"
        f"{log.read_text(errors='replace')[-1800:]}"
    )


@pytest.mark.parametrize("ending, finalizes", [("pass", True), ("_os._exit(0)", False)])
def test_the_finalize_mark_is_written_only_by_a_finalizing_exit(tmp_path, ending, finalizes):
    """The check the graceful-stop tests use (b/402): a script that ends normally leaves the
    mark; one that ends with `os._exit(0)` (what 5c886025 put after a graceful stop) exits 0
    too, and leaves none."""
    mark = tmp_path / "finalize.mark"
    rc = subprocess.run([sys.executable, "-c", FINALIZE_MARK_SNIPPET + ending],
                        env=dict(os.environ, FINALIZE_MARK=str(mark)), timeout=60).returncode
    assert rc == 0
    assert finalized(mark) is finalizes


_SERVER_BUILTIN = '''
from mipyre import Mipyre
app = Mipyre(runtime="isolated")
import faulthandler   # built-in single-phase ext; top-level -> runs in every worker
@app.get("/fh")
def h(req):
    import faulthandler as f
    return {{"loaded": f.__name__ == "faulthandler"}}
if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port})
'''


@_supported
def test_builtin_single_phase_ext_loads_in_workers(tmp_path):
    """A built-in single-phase ext (faulthandler — pulled in transitively by
    sklearn→joblib→loky) has no clonable source, so it can't be per-worker
    copied; it must load SHARED under the transient override. Without that
    fallback, worker init died with "module faulthandler does not support
    loading in subinterpreters" (caught by the grill soak on Linux)."""
    proc, log = _start_server(_SERVER_BUILTIN, tmp_path, "builtin",
                              workers=4, isolate_dir=tmp_path / "copies")
    try:
        r = _wait_ready("/fh", proc, log)
        assert r["loaded"], "faulthandler did not load in the sub-interpreter workers"
    finally:
        _stop(proc)


# The PyO3 multi-phase fixture (tests/fixtures/pyo3_mp), built twice: the variant without the feature does not
# declare per-interpreter GIL support, the one with it does (see the crate's lib.rs). CPython's own check decides on
# that declaration alone, so each test's expectation follows from how its variant was built.
_ROOT = Path(__file__).resolve().parent.parent
# A standalone crate (its own [workspace]): test asset, not a workspace package, not in any wheel.
_FIXTURE_DIR = _ROOT / "tests" / "fixtures" / "pyo3_mp"
_FIXTURE_MODULE = "pyo3_mp"
_FIXTURE_VARIANTS = {"undeclared": [], "declared": ["--features", "declare_per_interpreter"]}


@pytest.fixture(scope="session")
def pyo3_fixture_dirs(tmp_path_factory):
    """{variant: dir holding the importable module}. Built with cargo (an up-to-date build costs next to nothing).
    No cargo is a failure, never a skip: a skip is red at the gate."""
    cargo = shutil.which("cargo")
    if cargo is None:
        pytest.fail("cargo not found: test_isolate.py builds its PyO3 fixture (tests/fixtures/pyo3_mp) with cargo; "
                    "install a Rust toolchain (the merge gate has one)")
    env = dict(os.environ, PYO3_PYTHON=sys.executable)
    target = Path(os.environ.get("CARGO_TARGET_DIR") or _FIXTURE_DIR / "target") / "debug"
    lib_ext = ".dylib" if sys.platform == "darwin" else ".so"
    suffix = sysconfig.get_config_var("EXT_SUFFIX")
    root_dir = tmp_path_factory.mktemp("pyo3_fixtures")
    dirs = {}
    for variant, feature_args in _FIXTURE_VARIANTS.items():
        # Both variants build the same lib path, so each is copied out before the next build replaces it.
        cmd = [cargo, "build", "--lib", "--manifest-path", str(_FIXTURE_DIR / "Cargo.toml"), *feature_args]
        built = subprocess.run(cmd, cwd=_ROOT, env=env, capture_output=True, text=True, check=False)
        if built.returncode != 0:
            pytest.fail(f"cargo build of the PyO3 fixture ({variant}) failed:\n{built.stderr[-3000:]}")
        dest = root_dir / variant
        dest.mkdir()
        shutil.copyfile(target / f"lib{_FIXTURE_MODULE}{lib_ext}", dest / f"{_FIXTURE_MODULE}{suffix}")
        dirs[variant] = dest
    return dirs


def _toml_section_text(path, header):
    """The raw lines of one table in a TOML file, from its header up to the next table header (for messages)."""
    out, inside = [], False
    for line in path.read_text().splitlines():
        if line.startswith("["):
            if inside:
                break
            inside = line.strip() == header
        if inside:
            out.append(line)
    return "\n".join(out)


def test_fixture_pyo3_pins_match_root():
    """tests/fixtures/pyo3_mp is a standalone crate, so it repeats the root's pyo3 fork pin ([patch.crates-io] in
    Cargo.toml) and resolves it in its own Cargo.lock. Both must name the same fork rev as the root, or the fixture
    would silently test a different PyO3 than the engine ships. Remove this once the fixture lives in the workspace
    (arc archcheck 测试夹具包 category)."""
    import tomllib
    root_toml = _ROOT / "Cargo.toml"
    fixture_toml = _FIXTURE_DIR / "Cargo.toml"
    root_patch = tomllib.loads(root_toml.read_text())["patch"]["crates-io"]
    fixture_patch = tomllib.loads(fixture_toml.read_text())["patch"]["crates-io"]
    pyo3_names = {n for n in root_patch if n.startswith("pyo3")}
    assert pyo3_names, "root Cargo.toml has no pyo3 [patch] entries to compare against"
    mine = {n: fixture_patch.get(n) for n in pyo3_names}
    theirs = {n: root_patch[n] for n in pyo3_names}
    if mine != theirs:
        header = "[patch.crates-io]"
        raise AssertionError(
            "tests/fixtures/pyo3_mp/Cargo.toml [patch] differs from the root Cargo.toml [patch] (the pyo3 fork pin "
            "must be identical; the fixture's copy is a duplicate of the root's).\n"
            f"--- root Cargo.toml ({header}) ---\n{_toml_section_text(root_toml, header)}\n"
            f"--- {fixture_toml.relative_to(_ROOT)} ({header}) ---\n{_toml_section_text(fixture_toml, header)}"
        )

    def pyo3_sources(lock_path):
        packages = tomllib.loads(lock_path.read_text()).get("package", [])
        return {p["name"]: (p["version"], p.get("source"))
                for p in packages if p["name"].startswith("pyo3") and p["name"] != "pyo3_mp"}

    root_lock = pyo3_sources(_ROOT / "Cargo.lock")
    fixture_lock = pyo3_sources(_FIXTURE_DIR / "Cargo.lock")
    assert root_lock, "root Cargo.lock has no pyo3 packages"
    if fixture_lock != root_lock:
        raise AssertionError(
            "tests/fixtures/pyo3_mp/Cargo.lock resolves pyo3 packages differently from the root Cargo.lock "
            "(name -> (version, source)):\n"
            f"--- root Cargo.lock ---\n{root_lock}\n"
            f"--- fixture Cargo.lock ---\n{fixture_lock}"
        )


_SERVER_EXT = '''
from mipyre import Mipyre
app = Mipyre(runtime="isolated")
import os as _os
_ISO_DIR = _os.environ["MIPYRE_ISOLATE_DIR"]
@app.get("/pc")
def h(req):
    import {mod} as m
    return {{"isolated": m.__file__.startswith(_ISO_DIR), "ok": m.ping() == 7}}
if __name__ == "__main__":
    app.run(host="127.0.0.1", port={{port}})
'''


def _run_multiphase(fixture_dir, tmp_path, variant):
    """Start a 2-worker isolated server that imports the fixture in each worker; return the /pc answer."""
    copies = tmp_path / "copies"
    script = _SERVER_EXT.replace("{mod}", _FIXTURE_MODULE).replace("{{port}}", "{port}")
    env_extra = {"PYTHONPATH": str(fixture_dir) + os.pathsep + os.environ.get("PYTHONPATH", "")}
    proc, log = _start_server(script, tmp_path, f"srv_{variant}", workers=2, isolate_dir=copies,
                              env_extra=env_extra)
    try:
        return _wait_ready("/pc", proc, log)
    finally:
        _stop(proc)


@_supported
def test_isolated_multiphase_ext_not_declaring_subinterp_is_cloned(pyo3_fixture_dirs, tmp_path):
    """A PyO3 multi-phase extension that does NOT declare per-interpreter GIL support is refused by CPython's own
    check in a sub-interpreter, so the worker clones it and builds the module from its private copy. The in-worker
    PyInit path (the returned def, at refcount 1, must not be taken as an owned reference) runs only here.
    Only end to end: CPython's own check and PyInit run in a real worker importing a built extension.
    """
    r = _run_multiphase(pyo3_fixture_dirs["undeclared"], tmp_path, "undeclared")
    assert r == {"isolated": True, "ok": True}, r


@_supported
def test_isolated_multiphase_ext_declaring_subinterp_loads_shared(pyo3_fixture_dirs, tmp_path):
    """The same kind of extension declaring Py_mod_multiple_interpreters = per-interpreter GIL loads shared in each
    worker (CPython's check passes, nothing to clone): isolated False, and it still works.
    Only end to end: CPython's own check and PyInit run in a real worker importing a built extension.
    """
    r = _run_multiphase(pyo3_fixture_dirs["declared"], tmp_path, "declared")
    assert r == {"isolated": False, "ok": True}, r
