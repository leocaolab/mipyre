"""Fix wave A, W3 (workers / M7): docs/design/code-review-final-rubric.md (W3 row, F6),
docs/design/code-review-cced8c2-roadmap.md (M7) and the reconciliation's §4 FFI rows.

Server tests run a real server in a subprocess on the path they name:

  - "pool"  default runtime, MIPYRE_TPC=0        → the sub-interpreter pool (sync workers and
                                                     the async engine)
  - "tpc"   default runtime                        → TPC inline sub-interpreters
  - "gil"   MIPYRE_TEST_MODE=main, MIPYRE_TPC=0  → handlers on the main interpreter

Each binds port 0 and reads the port it got from its "Listening on" line: no fixed ports.

The panic test needs a `--features fault_injection` build (skipped otherwise).
"""

from __future__ import annotations

import logging
import os
import re
import signal
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from pathlib import Path

import httpx
import pytest

import mipyre.engine
from tests._helpers import bound_port, isolation_adapter_ns, poll_until, set_subinterpreters

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "crates" / "pyext" / "src"
# The sub-interpreter lifecycle moved to its own crate (b/47 S2a-5, q/5 a).
# These rules are about the code that enters an interpreter and holds its objects, so they
# follow it: every rule below that walked `src/python/*.rs` walks this too, and the counts and
# strings are unchanged.
SUBINTERP = ROOT / "crates" / "subinterp" / "src"
# The files that were the engine's `python/` module until b/51 E8 moved them into `interp/`
# (split into `runtime/` and `exchange/` by b/229)
# (which also holds the request types, errors and bridge since then): the worker code these
# checks were written for, the same files at their new paths.
# `runtime/worker.rs` and the files b/789 split it into.
WORKER = ("runtime/worker.rs", "runtime/worker/async_engine.rs", "runtime/worker/exec.rs",
          "runtime/worker/program.rs", "runtime/worker/start.rs", "runtime/worker/sync_runner.rs")
_WAS_PYTHON = ("exchange/body_stream.rs", "runtime/hook_chain.rs", "runtime/isolation.rs",
               "runtime/pool/mod.rs", "runtime/pool/serve.rs", "runtime/pool/split.rs",
               "runtime/pool/threads.rs", "runtime/pool/watchdog.rs", "runtime/request_context.rs", "runtime/rules.rs",
               "exchange/stream.rs", "exchange/websocket.rs", *WORKER,
               "runtime/worker_api.rs", "runtime/worker_app.rs")


def _worker_code() -> list:
    """Every file of the worker lifecycle: pyre's own and the shared crate's."""
    return sorted(SRC / f for f in _WAS_PYTHON) + sorted(SUBINTERP.glob("*.rs"))
ASYNC_ENGINE = ROOT / "python" / "mipyre" / "_worker" / "async_engine.py"

PATHS: dict[str, dict[str, str | None]] = {
    "pool": {"runtime": None, "MIPYRE_TPC": "0"},
    "tpc": {"runtime": None, "MIPYRE_TPC": None},
    "gil": {"runtime": "main", "MIPYRE_TPC": "0"},
}

RUN = """
if __name__ == "__main__":
    import os as _os
    app.run(host="127.0.0.1", port=0)
"""



class Server:
    """`script` (with `RUN` appended) served on `path` in a subprocess."""

    def __init__(self, script: str, path: str = "pool", workers: int = 2,
                 env: dict[str, str] | None = None, wait: bool = True):
        fd, self.script_path = tempfile.mkstemp(prefix="mipyre_w3_", suffix=".py")
        with os.fdopen(fd, "w") as f:
            f.write(textwrap.dedent(script) + RUN)
        self.log_path = self.script_path + ".log"
        full_env = dict(os.environ)
        full_env.pop("MIPYRE_TPC", None)
        full_env.pop("MIPYRE_TEST_MODE", None)
        if (runtime := PATHS[path]["runtime"]) is not None:
            full_env["MIPYRE_TEST_MODE"] = runtime
        full_env["MIPYRE_LOG"] = "1"
        if (tpc := PATHS[path]["MIPYRE_TPC"]) is not None:
            full_env["MIPYRE_TPC"] = tpc
        set_subinterpreters(full_env, str(workers))
        full_env.update(env or {})
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                [sys.executable, self.script_path], stdout=log, stderr=subprocess.STDOUT,
                env=full_env, start_new_session=True,
            )
        self.base: str | None = None  # once the server is up (`_wait_up`)
        if wait:
            self._wait_up()

    def _wait_up(self) -> None:
        """Waits for the startup line, then for the probe to get any answer, for no length of time (b/441 T2)."""
        try:
            port = bound_port(self.log, self.proc)
        except RuntimeError as e:
            raise RuntimeError(f"server did not start:\n{self.stop()[-4000:]}") from e
        base = self.base = f"http://127.0.0.1:{port}"

        def answered() -> bool:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited before answering:\n{self.stop()[-4000:]}")
            try:
                httpx.get(base + "/__w3_probe__", timeout=1)
                return True
            except httpx.TransportError:
                return False

        poll_until(answered, timeout=None, what="the server answering its probe")

    def get(self, target: str, timeout: float = 15) -> httpx.Response:
        assert self.base is not None, "the server was started with wait=False and never waited for"
        return httpx.get(self.base + target, timeout=timeout)

    def log(self) -> str:
        with open(self.log_path, errors="replace") as f:
            return f.read()

    def wait_exit(self, timeout: float = 60) -> int:
        try:
            return self.proc.wait(timeout=timeout)
        finally:
            if self.proc.poll() is None:
                os.killpg(self.proc.pid, signal.SIGKILL)

    def stop(self, timeout: float = 60) -> str:
        if self.proc.poll() is None:
            os.killpg(self.proc.pid, signal.SIGINT)
            try:
                self.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(self.proc.pid, signal.SIGKILL)
                self.proc.wait()
        return self.log()


def _code(path: Path) -> str:
    """`path` without its `//` comments."""
    return "\n".join(line.split("//", 1)[0] for line in path.read_text().splitlines())


_STRUCT = re.compile(r"\bstruct\s+(\w+)")


def _balanced(code: str, i: int) -> int:
    """The index just past the bracket group that opens at `code[i]` (`->` is no bracket)."""
    open_, close = code[i], {"{": "}", "(": ")", "<": ">"}[code[i]]
    depth, j = 0, i
    while True:
        if code[j] == open_:
            depth += 1
        elif code[j] == close and code[j - 1] != "-":
            depth -= 1
            if depth == 0:
                return j + 1
        j += 1


def _struct_fields(code: str) -> list:
    """Every field of every struct in `code` (comments already stripped), as
    `(struct, field, type)`; a tuple struct's fields are named by position."""
    fields = []
    for m in _STRUCT.finditer(code):
        i = m.end()
        while code[i].isspace():
            i += 1
        if code[i] == "<":
            i = _balanced(code, i)
        while code[i] not in "{(;":  # a `where` clause
            i += 1
        if code[i] == ";":
            continue
        body = code[i + 1:_balanced(code, i) - 1]
        body = re.sub(r"#\[[^\]]*\]", "", body)
        parts, depth, start = [], 0, 0
        for k, c in enumerate(body):
            if c in "<([{":
                depth += 1
            elif c in ">)]}" and not (c == ">" and body[k - 1] == "-"):
                depth -= 1
            elif c == "," and depth == 0:
                parts.append(body[start:k])
                start = k + 1
        parts.append(body[start:])
        for n, part in enumerate(p.strip() for p in parts):
            if not part:
                continue
            part = re.sub(r"^pub(\([^)]*\))?\s+", "", part)
            if code[i] == "{":
                name, _, ty = part.partition(":")
                fields.append((m.group(1), name.strip(), " ".join(ty.split())))
            else:
                fields.append((m.group(1), str(n), " ".join(part.split())))
    return fields


# A raw pointer to a Python object, `*mut T` / `*const T` (any path) where `T` is one of
# `pyo3::ffi`'s object structs: those laid out from `PyObject_HEAD` / `PyObject_VAR_HEAD` --
# `PyObject`, `PyVarObject`, `PyTypeObject`, `PyHeapTypeObject`, `PyLongObject`,
# `PyFloatObject`, `PyComplexObject`, `PyBytesObject`, `PyByteArrayObject`, `PyUnicodeObject`
# (`PyASCIIObject`, `PyCompactUnicodeObject`), `PyTupleObject`, `PyListObject`,
# `PyDictObject`, `PySetObject`, `PySliceObject`, `PyCellObject`, `PyCodeObject`,
# `PyFrameObject`, `PyGenObject`, `PyFunctionObject`, `PyCFunctionObject`,
# `PyCMethodObject`, the descriptor objects (`PyDescrObject`, `PyMethodDescrObject`,
# `PyMemberDescrObject`, `PyGetSetDescrObject`, `PyWrapperDescrObject`), `PyTracebackObject`,
# the exception objects (`PyBaseExceptionObject`, `PyOSErrorObject`, ...) and
# `PyWeakReference`: every `Py*Object`, plus `PyWeakReference`. Not objects, so not matched:
# `PyDictKeysObject` (a dict's key table, no object header), and the runtime's own state --
# `PyThreadState`, `PyInterpreterState`, `Py_buffer`, `PyMethodDef`, `PyModuleDef`, ...
_RAW_PY_OBJECT = re.compile(r"\*\s*(?:mut|const)\s+(?:\w+::)*(Py\w*Object|PyWeakReference)\b")
_NOT_PY_OBJECTS = {"PyDictKeysObject"}


def _holds_raw_py_object(ty: str) -> bool:
    return any(m.group(1) not in _NOT_PY_OBJECTS for m in _RAW_PY_OBJECT.finditer(ty))


def _bootstrap_ns():
    """The isolation adapter's namespace, as a worker's bootstrap holds it (`isolation_adapter_ns`)."""
    return isolation_adapter_ns()


# ---------------------------------------------------------------------------
# M7: FFI ownership (FR-19/C3), locks, unsafe impls
# ---------------------------------------------------------------------------


def test_workers_hold_py_references_not_raw_pointers():
    # FR-19/C3: the worker holds `Py<T>` and runs them through `Bound`; no raw-pointer
    # vectors with a manual DECREF, no `ended` flag, no hardcoded immortal threshold.
    for path in _worker_code():
        code = _code(path)
        assert "PyObjRef" not in code, path
        assert "Vec<*mut ffi::PyObject>" not in code, path
        assert "ended: bool" not in code, path
        assert "<< 30" not in code, path
        assert "Py_DECREF" not in code, path
        # A Python object the worker keeps is a `Py<T>`, never a raw object pointer: no struct
        # field holds one (a function's pointer parameter is borrowed for the call, not kept).
        raw = [f for f in _struct_fields(code) if _holds_raw_py_object(f[2])]
        assert not raw, f"{path}: struct fields hold raw Python object pointers: {raw}"
    # The handlers are pyre's (what a unit of work is stays with the engine): each route
    # holds its handler as a `Py` reference (`None` for a route this worker does not run).
    handler = [ty for s, name, ty in _struct_fields(_code(SRC / "runtime" / "worker" / "sync_runner.rs"))
               if (s, name) == ("InlineRoute", "handler")]
    assert handler and re.fullmatch(r"Py<PyAny>|Option<Py<PyAny>>", handler[0]), handler
    # The shared crate's worker holds its role's references in a `ManuallyDrop<R>` field (so
    # they are released by `end`, not by drop). The field's name is not the fact: only that
    # the struct owns a `ManuallyDrop<R>`.
    body = re.search(
        r"pub struct SubInterpreter<R: \w+> \{(.*?)\n\}", _code(SUBINTERP / "interp.rs"), re.S
    )
    assert body and re.search(r":\s*ManuallyDrop<R>", body.group(1)), "SubInterpreter owns no ManuallyDrop<R> field"


def test_lock_poisoning_is_not_swallowed_in_worker_code():
    for path in [SRC / "runtime" / n for n in ("worker_api.rs",
                                               "worker_app.rs", "hook_chain.rs")] + [
            SRC / f for f in WORKER] + sorted((SRC / "runtime" / "pool").glob("*.rs")) + [
            SUBINTERP / "attach.rs", SRC / "telemetry" / "leak_detect.rs"] + sorted(SUBINTERP.glob("*.rs")):
        code = _code(path)
        assert "into_inner()" not in code, f"{path}: a poisoned lock is recovered silently"
        assert ".read().ok()" not in code, path


def test_every_remaining_unsafe_impl_says_why_and_the_pool_has_none():
    for path in sorted((SRC / "runtime" / "pool").glob("*.rs")):
        assert "unsafe impl" not in _code(path), path
    for path in [*_worker_code(), SUBINTERP / "attach.rs"]:
        lines = path.read_text().splitlines()
        for n, line in enumerate(lines):
            if line.lstrip().startswith("unsafe impl"):
                above = "\n".join(lines[max(0, n - 6):n])
                assert "SAFETY:" in above, f"{path}:{n + 1}: unsafe impl without SAFETY"


def test_rebind_takes_the_error_it_reports_instead_of_clearing_it():
    for path in _worker_code():
        assert "PyErr_Clear" not in _code(path), path


def test_run_context_reports_a_missing_thread_state_as_a_value():
    code = _code(SUBINTERP / "attach.rs")
    assert 'assert!(!tstate.is_null()' not in code
    returned = re.search(r"fn ensure\(&self, main: InterpreterHandle\) -> Result<\(\), (\w+)>", code)
    assert returned, "ensure returns its failure as a value"
    error = returned.group(1)
    cases = re.search(r"enum " + error + r" \{(.*?)\n\}", code, re.S)
    assert cases, f"{error} is an enum in attach.rs"
    assert "#[from] ThreadStateCreationFailure" in cases.group(1), cases.group(1)


def test_conn_driver_renamed_and_interp_facade_gone():
    assert (SRC / "serve" / "conn.rs").exists()
    assert not (SRC / "worker.rs").exists()
    assert not (SRC / "interp" / "interp.rs").exists()
    assert not (SRC / "interp" / "ffi.rs").exists()
    # Scoped to src/ on purpose: `python::interp` and `crate::worker::` are pyre's own module
    # paths, and inside a separate crate `crate::` cannot name them at all — widening this walk
    # would add files the strings cannot appear in, not coverage.
    for path in SRC.rglob("*.rs"):
        code = _code(path)
        assert "python::interp" not in code, path
        assert "crate::worker::" not in code, path


def test_worker_app_record_lives_outside_app_rs_and_does_not_import_it():
    # The app's code was `app.rs` until b/51 E8 split it: the Python class into `pyapi/app.rs`,
    # the run it starts into `boot/run.rs`; b/72 split `pyapi/app.rs` into the files of
    # `pyapi/app/`. The module the app lives in is `crate::pyapi`.
    app = "".join(_code(p) for p in sorted((SRC / "pyapi" / "app").glob("*.rs")))
    app += _code(SRC / "boot" / "run.rs")
    # Since b/646 the record is a field of the engine module's state (`EngineModule.app`), not a
    # static.
    assert "struct WorkerApp" not in app
    assert "struct WorkerRoutes" not in app
    worker_app = _code(SRC / "runtime" / "worker_app.rs")
    assert "struct WorkerApp" in worker_app
    assert "struct WorkerRoutes" in worker_app
    for module in ("crate::pyapi", "crate::boot"):
        assert module not in worker_app
        for f in WORKER:
            assert module not in _code(SRC / f), f


def test_worker_api_passes_named_objects_not_tuples():
    code = _code(SRC / "runtime" / "worker_api.rs")
    assert "(u64, usize" not in code and "pool_id: u64" not in code
    assert "struct AsyncJob" in code
    assert "struct EngineRoutes" in _code(SRC / "runtime" / "worker" / "async_engine.rs")


def test_split_error_uses_thiserror():
    code = _code(SRC / "runtime" / "pool" / "split.rs")
    assert "impl std::fmt::Display for SplitError" not in code
    assert "thiserror::Error)]\npub(crate) enum SplitError" in code


def test_unit_tests_link_without_the_extension_module_feature():
    # `cargo test` links libpython (maturin sets PYO3_BUILD_EXTENSION_MODULE for the wheel),
    # so the unit-test binary links on macOS too.
    cargo = (ROOT / "crates" / "pyext" / "Cargo.toml").read_text()
    pyo3_dep = next(line for line in cargo.splitlines() if line.startswith("pyo3 ="))
    assert "extension-module" not in pyo3_dep
    assert "extension-module" not in (ROOT / "pyproject.toml").read_text()


# ---------------------------------------------------------------------------
# M7: the async engine
# ---------------------------------------------------------------------------

ASYNC_SCRIPT = """
import asyncio, gc, sys
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.get("/__w3_probe__")
def probe(req):
    return "up"

@app.get("/ok")
async def ok(req):
    return {"ok": True}

@app.get("/slow")
async def slow(req):
    print("w3-slow-started", flush=True)
    await asyncio.sleep(20)
    return "late"

@app.get("/kill")
async def kill(req):
    asyncio.get_running_loop().stop()
    return "stopping"

@app.get("/exit")
async def exit_(req):
    raise SystemExit("w3-exit")

@app.get("/stream")
async def stream(req):
    from mipyre import Stream
    return Stream()

@app.get("/engine")
async def engine(req):
    mod = sys.modules["__mipyre_async_engine__"]
    loops = [o for o in gc.get_objects() if isinstance(o, asyncio.AbstractEventLoop)]
    return {"timeout": mod.TASK_TIMEOUT, "loops": len(loops),
            "has_pool_id": hasattr(mod, "POOL_ID")}
"""


def test_async_engine_death_answers_its_waiters_at_once():
    srv = Server(ASYNC_SCRIPT, "pool", workers=2)
    try:
        assert srv.get("/ok").status_code == 200
        slow = {}
        t = threading.Thread(target=lambda: slow.setdefault("r", srv.get("/slow", timeout=40)))
        t.start()
        poll_until(lambda: "w3-slow-started" in srv.log(), what="/slow running")
        killed_at = time.time()
        srv.get("/kill", timeout=10)
        t.join(timeout=40)
        waited = time.time() - killed_at
    finally:
        out = srv.stop()
    # The in-flight request is answered when the engine lets go of it, not after the
    # 30 s request budget.
    assert slow["r"].status_code == 500, out[-3000:]
    assert waited < 5, (waited, out[-3000:])
    assert "the async engine dropped the request without answering it" in out
    assert "async worker stopped serving" in out
    assert "the async engine stopped: RuntimeError: Event loop stopped" in out


def test_async_engine_gets_its_budget_from_rust_and_builds_no_extra_loop():
    srv = Server(ASYNC_SCRIPT, "pool", workers=2)
    try:
        got = srv.get("/engine").json()
    finally:
        srv.stop()
    # The request budget (30 s) less the engine's margin (2 s).
    assert got["timeout"] == 28.0
    # Only the loop the engine runs on: the async worker builds none of its own.
    assert got["loops"] == 1, got
    assert got["has_pool_id"] is False
    assert "_HANDLER_TIMEOUT" not in ASYNC_ENGINE.read_text()


def test_a_non_response_on_the_async_path_is_logged_as_the_typed_error():
    srv = Server(ASYNC_SCRIPT, "pool", workers=2)
    try:
        assert srv.get("/stream").status_code == 500
    finally:
        out = srv.stop()
    failed = [line for line in out.splitlines() if '"path":"/stream"' in line]
    assert failed, out[-3000:]
    assert ('"error":"a sub-interpreter handler returned a Stream; a Stream response is only '
            "served from the main interpreter, so register the route with runtime='main'\"") in failed[0], failed[0]


# ---------------------------------------------------------------------------
# Reconcile §4: WORKER_STATES per pool; FORGOTTEN_WORKERS per run
# ---------------------------------------------------------------------------


def test_two_pool_servers_in_one_process_both_serve_async_routes():
    from mipyre.engine import ServerFlags
    from mipyre.testing import TestClient
    from tests.apps.w3_pool_a import app as app_a
    from tests.apps.w3_pool_b import app as app_b

    # A TestClient reads no environment (b/335 M5.6): the pool server is its config.
    pool = ServerFlags(tpc=False, subinterpreters=2)
    with TestClient(app_a, config=pool) as a:
        with TestClient(app_b, config=pool) as b:
            for _ in range(5):
                assert b.get("/which").json() == {"app": "b"}
                assert a.get("/which").json() == {"app": "a"}


def test_engine_has_no_process_wide_abandoned_list():
    assert not hasattr(mipyre.engine, "_forgotten_workers")
    assert mipyre.engine.MipyreApp()._take_abandoned_workers() == []


def test_a_stuck_worker_is_reported_by_the_run_that_abandoned_it():
    # A short request budget: the shutdown waits for the request's connection, which its
    # 504 ends.
    script = """
import time
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))
app.request_timeout = 1
# The shutdown's join waits this long for the stuck thread (5 s by default).
app.shutdown_timeout = 0.5

@app.get("/__w3_probe__")
def probe(req):
    return "up"

@app.get("/stuck")
def stuck(req):
    print("w3-stuck-started", flush=True)
    time.sleep(120)
    return "late"
"""
    srv = Server(script, "pool", workers=2)
    try:
        threading.Thread(target=lambda: srv.get("/stuck", timeout=200), daemon=True).start()
        poll_until(lambda: "w3-stuck-started" in srv.log(), what="/stuck running")
        out = srv.stop(timeout=120)
    finally:
        if srv.proc.poll() is None:
            os.killpg(srv.proc.pid, signal.SIGKILL)
    assert srv.proc.returncode == 1, out[-3000:]
    assert "did not stop within the shutdown grace period" in out
    assert "(running GET /stuck)" in out


# ---------------------------------------------------------------------------
# Final rubric: worker start errors reach Python as themselves
# ---------------------------------------------------------------------------

START_FAILURE = """
import sys
import os
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.get("/__w3_probe__")
def probe(req):
    return "up"

if __name__ == "__mipyre_worker__":
    {failure}

if __name__ == "__main__":
    import os, mipyre.engine
    try:
        app.run(host="127.0.0.1", port=0)
    except BaseException as e:
        cause = e.__cause__
        print("RAISED", type(e).__name__, "|", isinstance(e, ImportError), "|",
              type(cause).__name__, "|", isinstance(cause, mipyre.engine.WorkerException))
        print("MESSAGE", str(e).replace("\\n", " "))
        print("CAUSE", str(cause).replace("\\n", " "))
        sys.stdout.flush()
        os._exit(3)
"""


def _start_failure(failure: str, path: str) -> str:
    fd, script = tempfile.mkstemp(prefix="mipyre_w3_", suffix=".py")
    with os.fdopen(fd, "w") as f:
        f.write(textwrap.dedent(START_FAILURE).replace("{failure}", failure))
    env = dict(os.environ)
    env.pop("MIPYRE_TPC", None)
    if (tpc := PATHS[path]["MIPYRE_TPC"]) is not None:
        env["MIPYRE_TPC"] = tpc
    set_subinterpreters(env, "1")
    done = subprocess.run([sys.executable, script], env=env, capture_output=True, text=True,
                          timeout=120)
    out = done.stdout + done.stderr
    assert done.returncode == 3, out[-3000:]
    return out


@pytest.mark.parametrize("path", ["pool", "tpc"])
def test_a_module_missing_in_a_worker_is_a_module_not_found_error_on_main(path):
    out = _start_failure("import w3_no_such_module_anywhere", path)
    assert "RAISED ModuleNotFoundError | True | WorkerException | True" in out, out[-3000:]
    assert "No module named 'w3_no_such_module_anywhere'" in out
    # The worker's traceback is the cause's text.
    cause = next(line for line in out.splitlines() if line.startswith("CAUSE"))
    assert "Traceback (most recent call last)" in cause, cause


def test_an_absolute_import_error_gets_no_relative_import_hint():
    out = _start_failure("from os import w3_no_such_name", "pool")
    assert "RAISED ImportError | True" in out, out[-3000:]
    assert "cannot import name 'w3_no_such_name'" in out
    assert "outside any package, so a relative import" not in out


def test_any_other_start_failure_is_a_runtime_error_with_the_worker_exception_as_cause():
    out = _start_failure('raise ValueError("w3-worker-refuses")', "pool")
    assert "RAISED RuntimeError | False | WorkerException | True" in out, out[-3000:]
    assert "ValueError: w3-worker-refuses" in out


def test_a_non_str_sys_path_entry_does_not_stop_the_workers(tmp_path):
    pkgdir = tmp_path / "pathlike"
    pkgdir.mkdir()
    (pkgdir / "w3_from_pathlike.py").write_text("VALUE = 'from a pathlib.Path entry'\n")
    script = f"""
import os, pathlib, sys
sys.path.append(12345)
sys.path.append(pathlib.Path({str(pkgdir)!r}))
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.get("/__w3_probe__")
def probe(req):
    return "up"

@app.get("/value")
def value(req):
    import w3_from_pathlike
    return w3_from_pathlike.VALUE
"""
    srv = Server(script, "pool", workers=1)
    try:
        assert srv.get("/value").text == "from a pathlib.Path entry"
    finally:
        out = srv.stop()
    assert "a sys.path entry that is not a path is left out" in out


# ---------------------------------------------------------------------------
# M7: the isolation flag reaches the workers explicitly; the isolation adapter
# ---------------------------------------------------------------------------


def test_isolated_flag_reaches_the_workers_without_the_environment():
    script = """
import os, sys
from mipyre import Mipyre
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.get("/__w3_probe__")
def probe(req):
    return "up"

@app.get("/iso", runtime="isolated")
def iso(req):
    boot = sys.modules["__mipyre_bootstrap__"]
    return {"isolation": boot.ISOLATION_SOURCE is not None,
            "env": any(k.startswith("MIPYRE_ISOLAT") and k != "MIPYRE_ISOLATE_DIR" for k in os.environ)}
"""
    srv = Server(script, "pool", workers=1)
    try:
        got = srv.get("/iso").json()
    finally:
        srv.stop()
    assert got == {"isolation": True, "env": False}


def test_packages_distributions_failure_surfaces_with_its_cause(monkeypatch):
    import importlib.metadata

    def broken():
        raise KeyError("w3-broken-metadata")

    monkeypatch.setattr(importlib.metadata, "packages_distributions", broken)
    ns = _bootstrap_ns()
    with pytest.raises(RuntimeError, match="packages_distributions") as raised:
        ns["_iso_pkg2dist"]()
    assert isinstance(raised.value.__cause__, KeyError)


def _clone_twice(tmp_path, make_src, change_src):
    src = make_src(tmp_path / "site")
    worker_dir = tmp_path / "w0"
    worker_dir.mkdir()
    ns = _bootstrap_ns()
    first = ns["_iso_clone_lib"]("lib", str(src), str(worker_dir), {})
    change_src(src)
    second = ns["_iso_clone_lib"]("lib", str(src), str(worker_dir), {})
    return first, second


@pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="cp -c / --reflink")
def test_a_single_file_extension_is_recloned_after_an_upgrade(tmp_path):
    def make(site):
        site.mkdir()
        src = site / "lib.cpython-314-x.so"
        src.write_bytes(b"v1")
        return src

    def upgrade(src):
        src.write_bytes(b"version two")

    first, second = _clone_twice(tmp_path, make, upgrade)
    assert first == second
    assert Path(second).read_bytes() == b"version two"


@pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="cp -c / --reflink")
def test_a_stale_symlinked_clone_is_removed_not_followed(tmp_path):
    target = tmp_path / "elsewhere"
    target.mkdir()
    (target / "keep.txt").write_text("keep")

    def make(site):
        site.mkdir()
        src = site / "lib.cpython-314-x.so"
        src.write_bytes(b"v1")
        return src

    src = make(tmp_path / "site")
    worker_dir = tmp_path / "w0"
    worker_dir.mkdir()
    # A stale clone that is a symlink (no manifest): removed as a link.
    (worker_dir / src.name).symlink_to(target, target_is_directory=True)
    ns = _bootstrap_ns()
    dst = ns["_iso_clone_lib"]("lib", str(src), str(worker_dir), {})
    assert Path(dst).read_bytes() == b"v1"
    assert (target / "keep.txt").read_text() == "keep"


# A clone lives in the temp dir, whose cleaner deletes files it takes for unused: a
# clone missing a file must be re-cloned, not reused (it broke every worker's import of
# pydantic_core until the cache was deleted by hand).

def _package_site(site):
    pkg = site / "lib"
    (pkg / "__pycache__").mkdir(parents=True)
    (pkg / "__init__.py").write_text("from .core import x\n")
    (pkg / "core.py").write_text("x = 1\n")
    (pkg / "_ext.cpython-314-x.so").write_bytes(b"\0" * 64)
    (pkg / "__pycache__" / "core.cpython-314.pyc").write_bytes(b"pyc")
    return pkg


def _clone_package(tmp_path, pkg2dist=None):
    src = _package_site(tmp_path / "site")
    worker_dir = tmp_path / "w0"
    worker_dir.mkdir()
    ns = _bootstrap_ns()
    def clone():
        return Path(ns["_iso_clone_lib"]("lib", str(src), str(worker_dir), pkg2dist or {}))
    return src, worker_dir, clone


@pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="cp -c / --reflink")
def test_a_clone_missing_a_file_is_recloned_with_a_warning(tmp_path, caplog):
    _, _, clone = _clone_package(tmp_path)
    first = clone()
    (first / "__init__.py").unlink()  # what the temp-dir cleaner did

    with caplog.at_level(logging.WARNING, logger="mipyre.isolate"):
        second = clone()

    assert (second / "__init__.py").read_text() == "from .core import x\n"
    damaged = [r.getMessage() for r in caplog.records if "damaged" in r.getMessage()]
    assert len(damaged) == 1 and "__init__.py is missing" in damaged[0], caplog.text


@pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="cp -c / --reflink")
def test_a_whole_clone_is_reused_even_without_its_pycache(tmp_path, caplog):
    _, _, clone = _clone_package(tmp_path)
    first = clone()
    inode = (first / "_ext.cpython-314-x.so").stat().st_ino
    for pyc in (first / "__pycache__").iterdir():
        pyc.unlink()

    with caplog.at_level(logging.WARNING, logger="mipyre.isolate"):
        second = clone()

    # Reused: same inode, so macOS doesn't verify the library's signature again.
    assert (second / "_ext.cpython-314-x.so").stat().st_ino == inode
    assert "damaged" not in caplog.text


@pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="cp -c / --reflink")
def test_a_clone_with_a_source_only_manifest_is_recloned(tmp_path):
    src, worker_dir, clone = _clone_package(tmp_path)
    first = clone()
    st = src.stat()
    # The manifest before clones listed their files: the source signature alone.
    (worker_dir / "lib.sig").write_text("%d\0%d" % (st.st_mtime_ns, st.st_size))
    (first / "core.py").unlink()

    second = clone()

    assert (second / "core.py").read_text() == "x = 1\n"


@pytest.mark.skipif(sys.platform not in ("linux", "darwin"), reason="cp -c / --reflink")
def test_a_vendored_libs_clone_missing_a_file_is_recloned(tmp_path):
    src, worker_dir, clone = _clone_package(tmp_path, {"lib": ["lib-dist"]})
    vendored = src.parent / "lib_dist.libs"
    vendored.mkdir()
    (vendored / "libblas.so.3").write_bytes(b"\0" * 32)
    clone()
    (worker_dir / "lib_dist.libs" / "libblas.so.3").unlink()

    clone()

    assert (worker_dir / "lib_dist.libs" / "libblas.so.3").stat().st_size == 32


# ---------------------------------------------------------------------------
# Final rubric: one hook chain on every synchronous path
# ---------------------------------------------------------------------------

CHAIN_SCRIPT = """
import asyncio
import os
from mipyre import Mipyre, Response
from mipyre.context import ctx
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

@app.before_request
async def tag(req):
    ctx.set("tag", "set-by-async-before")
    return None

@app.before_request
def gate(req):
    if req.path == "/gated":
        return Response(body="gated", status_code=401)
    return None

@app.after_request
def wrap(req, resp):
    if req.path == "/wrapped":
        body = resp.body if isinstance(resp.body, str) else resp.body.decode()
        return Response(body="wrapped:" + body, status_code=resp.status_code)
    return None

@app.get("/__w3_probe__")
def probe(req):
    return "up"

@app.get("/gated")
def gated(req):
    return "not reached"

@app.get("/wrapped")
def wrapped(req):
    return ctx.get("tag", "missing")

@app.get("/awaited")
def awaited(req):
    async def later():
        await asyncio.sleep(0)
        return "awaited:" + ctx.get("tag", "missing")
    return later()
"""


@pytest.mark.parametrize("path", ["gil", "tpc", "pool"])
def test_the_hook_chain_is_the_same_on_every_sync_path(path):
    srv = Server(CHAIN_SCRIPT, path, workers=2)
    try:
        gated = srv.get("/gated")
        wrapped = srv.get("/wrapped")
        awaited = srv.get("/awaited")
    finally:
        srv.stop()
    assert (gated.status_code, gated.text) == (401, "gated")
    assert (wrapped.status_code, wrapped.text) == (200, "wrapped:set-by-async-before")
    assert (awaited.status_code, awaited.text) == (200, "awaited:set-by-async-before")
