"""A worker never uses an extension from a shared file unless CPython's own
multi-interpreter check allows it; everything else comes from its private copy,
built in the worker itself.

Why: since 3.13 CPython runs every extension's `PyInit_*` in the MAIN
interpreter. A single-phase module is built there and the worker gets a shallow
copy of its dict, so its objects live on main's heap (scipy's f2py `_fblas`:
`free(): invalid size` on the first setattr). And the loader used to set the
override for shared files too, which let a lib without its own guard (orjson)
load shared into every worker; finalizing those workers aborted with "pointer
being freed was not allocated" in `remove_all_subclasses`.
"""
from __future__ import annotations

import http.client
import json
import os
import signal
import subprocess
import sys
import threading
import urllib.request

import pytest

from tests._helpers import FINALIZE_MARK_SNIPPET, bound_port, finalized, listening_ports, poll_until, set_subinterpreters

_supported = pytest.mark.skipif(
    sys.platform not in ("linux", "darwin"),
    reason="isolate() clones via cp -c / cp --reflink (Linux/macOS)",
)

def _start(script_text, tmp_path, name, workers, env_extra=None):
    """Launches the script on port 0; `_base` reads the port it bound from the log."""
    script = tmp_path / f"{name}.py"
    script.write_text(script_text.replace("{port}", "0"))
    log = tmp_path / f"{name}.log"
    env = dict(os.environ, MIPYRE_ISOLATE_DIR=str(tmp_path / "copies"), **(env_extra or {}))
    set_subinterpreters(env, str(workers))
    proc = subprocess.Popen([sys.executable, str(script)],
                            stdout=open(log, "wb"), stderr=subprocess.STDOUT, env=env)
    return proc, log


def _base(log) -> str | None:
    """The server's base URL once its startup line names the bound port."""
    ports = listening_ports(log.read_text(errors="replace"))
    return f"http://127.0.0.1:{ports[0]}" if ports else None


def _get(path, proc, log):
    """`path`'s JSON once the server answers it 200: waits for its startup line, then for the answer, for no length
    of time (workers importing numpy and scipy start when the machine lets them, b/441 T2). An HTTP error or the
    server exiting fails the test at once."""

    def read() -> str:
        return log.read_text(errors="replace")

    url = f"http://127.0.0.1:{bound_port(read, proc)}{path}"

    def answer():
        if proc.poll() is not None:
            pytest.fail(f"server exited early (rc={proc.returncode}):\n{read()[-3000:]}")
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                return (json.loads(r.read()),) if r.status == 200 else None
        except urllib.error.HTTPError as e:
            pytest.fail(f"{url} -> HTTP {e.code}:\n{read()[-3000:]}")
        except (OSError, http.client.HTTPException):
            return None  # not serving yet

    return poll_until(answer, timeout=None, what=f"{url} answering")[0]


def _sigint(proc):
    proc.send_signal(signal.SIGINT)
    try:
        return proc.wait(timeout=60)
    finally:
        if proc.poll() is None:
            proc.kill()


_SCIPY_LAZY = '''
import os
from mipyre import Mipyre
app = Mipyre(runtime="isolated")
_D = os.path.realpath(os.environ["MIPYRE_ISOLATE_DIR"])

@app.get("/sp")
def sp(req):
    # Not declared and not imported by main: the worker's import reaches the
    # shared site-packages files first.
    import scipy.optimize as so
    import scipy.linalg._fblas as fb
    return {"fun": float(so.minimize(lambda v: (v ** 2).sum(), [1.0, 2.0]).fun),
            "fblas_isolated": os.path.realpath(fb.__file__).startswith(_D)}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port})
'''


@_supported
def test_shared_single_phase_ext_is_cloned_not_loaded_shared(tmp_path):
    """Before: CPython ran scipy's single-phase init in the main interpreter,
    whose own `import_array()` then hit numpy's "cannot load module more than
    once per process" in main -> every request 500 ("failed to import from
    subinterpreter due to exception"). Now the worker's finder reads scipy's
    files before any of them loads, judges the package a copy (it has
    single-phase files), and `_fblas` is built in the worker from the copy. Finalization (no hard exit) must be clean too."""
    pytest.importorskip("scipy")
    proc, log = _start(_SCIPY_LAZY, tmp_path, "scipy_lazy", workers=2)
    try:
        r = _get("/sp", proc, log)
        assert r["fun"] < 1e-6
        assert r["fblas_isolated"], "scipy.linalg._fblas was loaded from the shared file"
    finally:
        rc = _sigint(proc)
    text = log.read_text(errors="replace")
    assert rc == 0, f"rc={rc}:\n{text[-3000:]}"
    assert "Fatal Python error" not in text, text[-3000:]


# Worker code that makes requests reach every worker (b/441): `meet()` writes the serving
# interpreter's id into `MEET_DIR`, then holds the worker until `MEET_WORKERS` ids are there.
# While a worker is held it takes no other request, so the requests in flight go to the others,
# until each worker has served one. A worker that never gets one holds the rest forever: the test
# then fails at the suite's timeout instead of passing without having tested that worker. Until
# b/441 a handler slept 20 ms to spread requests, and the tests asserted that two of the workers
# had been reached.
MEET = '''
def meet():
    import _interpreters, os, time
    here = os.environ["MEET_DIR"]
    open(os.path.join(here, str(_interpreters.get_current()[0])), "w").close()
    while len(os.listdir(here)) < int(os.environ["MEET_WORKERS"]):
        time.sleep(0.005)
'''


def reachable_workers(workers: int, *, pool: bool) -> int:
    """How many of `workers` requests can be made to reach. The pool (MIPYRE_TPC=0) hands each
    request to any idle worker. TPC (the default) pins each connection to one thread chosen by
    the kernel's SO_REUSEPORT, which balances on Linux but not on macOS (boot/run.rs): there
    every connection may land on one worker."""
    return workers if pool or sys.platform != "darwin" else 1


def meet_env(tmp_path, workers: int) -> dict[str, str]:
    """The environment `MEET` reads: a fresh directory, and how many workers to wait for."""
    here = tmp_path / "meet"
    here.mkdir()
    return {"MEET_DIR": str(here), "MEET_WORKERS": str(workers)}


def test_meet_holds_a_worker_until_every_worker_has_served(tmp_path, monkeypatch):
    """`meet`'s own input: run here (one interpreter), waiting for two. It holds while only
    its own interpreter has served, and returns once a second one (a file this test writes)
    has: the requests the tests below send can't all be answered by fewer workers than that."""
    for name, value in meet_env(tmp_path, 2).items():
        monkeypatch.setenv(name, value)
    ns: dict = {}
    exec(MEET, ns)  # noqa: S102 — the same source the workers run, run here on its own input
    met = threading.Thread(target=ns["meet"])
    met.start()
    poll_until(lambda: os.listdir(tmp_path / "meet"), timeout=None)
    assert met.is_alive(), "meet returned with one of two workers served"
    (tmp_path / "meet" / "another-worker").touch()
    met.join()


_ORJSON_TOP = '''
import os
from mipyre import Mipyre
app = Mipyre(runtime="isolated")
import orjson   # not declared; orjson has no load-once guard of its own
_D = os.path.realpath(os.environ["MIPYRE_ISOLATE_DIR"])
MEET_SRC

@app.get("/oj")
def oj(req):
    import _interpreters
    if req.query_params.get("meet"):
        meet()
    return {"interp": _interpreters.get_current()[0],
            "isolated": os.path.realpath(orjson.__file__).startswith(_D),
            "dumps": orjson.dumps({"a": 1}).decode()}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port})
'''.replace("MEET_SRC", MEET)


@_supported
def test_undeclared_ext_without_own_guard_is_isolated_in_every_worker(tmp_path):
    """orjson declares it doesn't support sub-interpreters but has no guard of
    its own, so under the old always-on override every worker loaded the shared
    file and graceful shutdown aborted. Every worker must now use its own copy
    (built in the worker: orjson's pyo3-ffi def is not immortal, which also
    covers the def's refcount), and SIGINT must finalize cleanly."""
    pytest.importorskip("orjson")
    import concurrent.futures
    reach = reachable_workers(4, pool=False)
    proc, log = _start(_ORJSON_TOP, tmp_path, "orjson_top", workers=4, env_extra=meet_env(tmp_path, reach))
    try:
        _get("/oj", proc, log)
        url = _base(log) + "/oj?meet=1"
        # More in flight than workers, so a held worker's queued requests leave others to go round.
        with concurrent.futures.ThreadPoolExecutor(64) as ex:
            rs = list(ex.map(lambda _: json.loads(urllib.request.urlopen(url, timeout=None).read()),
                             range(64)))
    finally:
        rc = _sigint(proc)
    interps = sorted({r["interp"] for r in rs})
    assert (len(interps) == 4) if reach == 4 else (len(interps) >= 1), f"requests reached interpreters {interps}"
    shared = sorted({r["interp"] for r in rs if not r["isolated"]})
    assert not shared, f"workers {shared} used the shared orjson"
    assert all(r["dumps"] == '{"a":1}' for r in rs)
    text = log.read_text(errors="replace")
    assert rc == 0, f"rc={rc}:\n{text[-3000:]}"
    assert "Fatal Python error" not in text, text[-3000:]


_FINALIZE = FINALIZE_MARK_SNIPPET + '''
import os
from mipyre import Mipyre
app = Mipyre(runtime="isolated")
import numpy as np
import scipy.linalg
import orjson

@app.get("/x")
def x(req):
    import scipy.optimize as so
    return {"r": float(so.minimize(lambda v: (v ** 2).sum(), [1.0]).fun),
            "n": float(np.linalg.svd(np.eye(4), compute_uv=False).sum()),
            "o": len(orjson.dumps([1, 2]))}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port})
'''


@_supported
def test_graceful_sigint_finalizes_isolated_workers(tmp_path):
    """run() no longer hard-exits: CPython finalizes after the engine ended
    every worker. Measured before the in-worker init: 6/6 SIGABRT at teardown
    ("pointer being freed was not allocated", dict_dealloc of main-owned
    objects); after: 0. Use the heaviest isolated set (numpy + scipy's
    single-phase modules + orjson)."""
    pytest.importorskip("scipy")
    pytest.importorskip("orjson")
    finalize_mark = tmp_path / "finalize.mark"
    proc, log = _start(_FINALIZE, tmp_path, "finalize", workers=4,
                       env_extra={"FINALIZE_MARK": str(finalize_mark)})
    try:
        for _ in range(8):
            _get("/x", proc, log)
    finally:
        rc = _sigint(proc)
    text = log.read_text(errors="replace")
    assert rc == 0, f"rc={rc}:\n{text[-3000:]}"
    assert "Fatal Python error" not in text, text[-3000:]
    assert finalized(finalize_mark), f"exited without finalizing (no atexit ran):\n{text[-3000:]}"


_ISOLATED_PLAIN = '''
from mipyre import Mipyre
app = Mipyre(runtime="isolated")

@app.get("/up")
def up(req):
    return {"up": True}

if __name__ == "__main__":
    app.run(host="127.0.0.1", port={port})
'''


@_supported
@pytest.mark.parametrize("tpc", ["1", "0"], ids=["tpc", "pool"])
def test_an_isolated_app_says_which_copies_its_workers_load_once_they_started(tmp_path, tpc):
    """Both the TPC worker server and the pool print the isolation line once their workers are up."""
    proc, log = _start(_ISOLATED_PLAIN, tmp_path, f"isolated_plain_{tpc}", workers=2, env_extra={"MIPYRE_TPC": tpc})
    try:
        assert _get("/up", proc, log) == {"up": True}
    finally:
        rc = _sigint(proc)
    text = log.read_text(errors="replace")
    assert rc == 0, f"rc={rc}:\n{text[-3000:]}"
    assert "runtime='isolated' (a compatibility option, not recommended): per-worker copies of" in text, text[-3000:]
