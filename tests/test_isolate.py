"""runtime="isolated" — per-worker C-extension copies.

Verifies numpy loads in EVERY own-GIL sub-interpreter worker (no
"cannot load module more than once") and each worker's numpy resolves to its
own isolated copy, driven concurrently so requests spread across workers.
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import signal
import subprocess
import sys
import urllib.request

import pytest

from tests import _pyo3_fixture
from tests._helpers import FINALIZE_MARK_SNIPPET, bound_port, finalized, read_file

_supported = pytest.mark.skipif(
    sys.platform not in ("linux", "darwin"),
    reason="isolate() clones via cp -c / cp --reflink (Linux/macOS)",
)

_SERVER = '''
from mipyre import Mipyre
app = Mipyre(runtime="isolated")
import numpy   # loaded by every worker at start

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
        MIPYRE_THREADS="4",
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


# -- Automatic isolation: an unmodified `import numpy` ---------------------------
#
# The worker's finder must make an UNMODIFIED `import numpy` work in every own-GIL
# worker: the first time a worker resolves numpy it reads numpy's extension files,
# finds single-phase ones, and resolves numpy to that worker's own copy.

_SERVER_AUTO = '''
from mipyre import Mipyre
app = Mipyre(runtime="isolated")

import os as _os
_ISO_DIR = _os.environ.get("MIPYRE_ISOLATE_DIR", "/tmp/mipyre-isolate")

# Top-level `import numpy` (the realistic way a user writes it): every worker
# runs the script body at init, so each makes its own copy of numpy — no
# reliance on request load spreading across all workers (pyre may route a burst
# to a single worker).
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
        MIPYRE_THREADS="4",
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
    # achieved by the finder alone, with nothing but `import numpy` in the server script.
    assert all("numpy" in r for r in results)
    assert all(r["isolated"] for r in results), "the finder did not isolate numpy"

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
    env = dict(os.environ, MIPYRE_THREADS=str(workers))
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
import numpy as np   # top-level: each worker resolves numpy to its own copy
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


_ROOT, _FIXTURE_DIR, _FIXTURE_MODULE = _pyo3_fixture.ROOT, _pyo3_fixture.FIXTURE_DIR, _pyo3_fixture.MODULE


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


_SERVER_POLARS = '''
from mipyre import Mipyre
app = Mipyre(runtime="isolated")
import polars as pl   # polars/_plr.py tries its runtimes under `except ImportError: pass`
@app.get("/pl")
def h(req):
    return {{"ok": int(pl.Series([1, 2]).sum())}}
if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port})
'''


@_supported
def test_polars_works_in_isolated_workers_without_importing_its_runtime_first(tmp_path):
    """b/755: polars tries `_polars_runtime_32` under `except ImportError: pass`. The finder copies the runtime when
    polars first resolves it, so no ImportError ever reaches polars, and the startup line says why it was copied."""
    proc, log = _start_server(_SERVER_POLARS, tmp_path, "srv_polars", workers=2, isolate_dir=tmp_path / "copies")
    try:
        assert _wait_ready("/pl", proc, log) == {"ok": 3}
    finally:
        _stop(proc)
    text = log.read_text(errors="replace")
    assert "Polars binary is missing" not in text, text[-3000:]
    line = next(line for line in text.splitlines() if "per-worker copies of" in line)
    assert "_polars_runtime_32 (" in line and "does not declare per-interpreter GIL support" in line, line


_SERVER_FALLBACK = '''
from mipyre import Mipyre
app = Mipyre(runtime="isolated")
import os as _os
import fallbacklib
@app.get("/fb")
def h(req):
    import {mod} as m
    return {{"fallback": fallbacklib.FALLBACK, "isolated": m.__file__.startswith(_os.environ["MIPYRE_ISOLATE_DIR"])}}
if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port})
'''


@_supported
def test_a_library_that_falls_back_on_import_error_gets_its_extension_from_a_copy(pyo3_fixture_dirs, tmp_path):
    """A pure-Python library with a fallback (`try: import ext except ImportError`) around an extension that does not
    declare per-interpreter GIL support: the extension is copied before it loads, so the fallback never runs."""
    lib = tmp_path / "lib" / "fallbacklib"
    lib.mkdir(parents=True)
    (lib / "__init__.py").write_text(
        f"try:\n    import {_FIXTURE_MODULE}  # noqa: F401\n    FALLBACK = False\nexcept ImportError:\n    FALLBACK = True\n"
    )
    script = _SERVER_FALLBACK.replace("{mod}", _FIXTURE_MODULE)
    path = os.pathsep.join([str(pyo3_fixture_dirs["undeclared"]), str(tmp_path / "lib"), os.environ.get("PYTHONPATH", "")])
    proc, log = _start_server(script, tmp_path, "srv_fallback", workers=2, isolate_dir=tmp_path / "copies",
                              env_extra={"PYTHONPATH": path})
    try:
        assert _wait_ready("/fb", proc, log) == {"fallback": False, "isolated": True}
    finally:
        _stop(proc)


_SERVER_UNUSABLE_DIR = '''
from mipyre import Mipyre
app = Mipyre(runtime="isolated")
@app.get("/up")
def up(req):
    return {{"up": True}}
@app.get("/lazy")
def h(req):
    try:
        import {mod}  # noqa: F401
    except ImportError:
        return {{"caught": "ImportError"}}
    except RuntimeError as e:
        return {{"caught": type(e).__name__, "said": str(e)}}
    return {{"caught": None}}
if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port})
'''


@_supported
def test_a_copy_that_cannot_be_made_is_an_isolation_error_no_import_error_handler_hides(pyo3_fixture_dirs, tmp_path):
    # The directory is checked when the server starts (b/335 M7.1), so it turns unusable after
    # that: the copy made at the first isolated import is what fails.
    shared = tmp_path / "copies"
    script = _SERVER_UNUSABLE_DIR.replace("{mod}", _FIXTURE_MODULE)
    path = os.pathsep.join([str(pyo3_fixture_dirs["undeclared"]), os.environ.get("PYTHONPATH", "")])
    proc, log = _start_server(script, tmp_path, "srv_unusable", workers=1, isolate_dir=shared,
                              env_extra={"PYTHONPATH": path})
    try:
        assert _wait_ready("/up", proc, log) == {"up": True}
        shared.chmod(0o777)
        got = _wait_ready("/lazy", proc, log)
    finally:
        _stop(proc)
    assert got["caught"] == "IsolationError", got
    assert f"could not load {_FIXTURE_MODULE}" in got["said"] and "writable by other users" in got["said"], got
    assert "does not declare per-interpreter GIL support" in got["said"], got
