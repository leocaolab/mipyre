"""Shared pytest fixtures for Mipyre integration tests.

Provides a parameterised `feature_server` fixture that spins up Mipyre in
either GIL or sub-interpreter mode on an ephemeral port, yields a base
URL, and tears down cleanly. Individual tests express their routes via
the SERVER_SCRIPT string (a template) and reuse the fixture.

The old test_all_features.py used one giant `run_feature_tests()` that
bundled 13+ assertions under one server; splitting by topic means each
file pays its own startup cost but is independently runnable and
failure-isolated.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

import pytest
from hypothesis import settings

from mipyre import Mipyre
from mipyre.testing import TestServer
from tests import _pyo3_fixture
from tests._helpers import bound_port, poll_until, read_file
from tests._test_cost import TestCost

# Hypothesis (docs/property-testing.md §5, b/749): the merge gate runs every property test on the same inputs
# (`ci`, derandomized, no example database to carry one run's finds into the next); the nightly run sets
# MIPYRE_TEST_HYPOTHESIS_PROFILE=nightly for random inputs and many more of them. A counterexample found at night is
# written into its test as an explicit `@example`, so the gate keeps running it.
settings.register_profile("ci", derandomize=True, database=None)
settings.register_profile("nightly", derandomize=False, database=None, max_examples=10_000)
settings.load_profile(os.environ.get("MIPYRE_TEST_HYPOTHESIS_PROFILE", "ci"))

HOST = "127.0.0.1"


# ---------------------------------------------------------------------------
# Parallel runs (pytest-xdist, b/156): one Postgres database per worker
# ---------------------------------------------------------------------------

# The db tests create and drop fixed table names (`mipyre_test_rows`, `w4_json`, ...) and
# one terminates a backend; under xdist the tests of one file can run on two workers at
# once. So each worker gets a database of its own on the server the DSN names,
# `<database>_<worker>` (`mipyre_gate_gw0`, ...), and the DSN in the environment is
# rewritten to it before any test module is imported (the modules read it at import, and
# the servers they start inherit it). Without xdist nothing changes: the DSN is used as
# given. The database is dropped and created fresh at the start (a run killed before its
# end leaves one behind) and dropped at the end.
#
# Made by a child Python, not this process: the Rust PgPool is one per process, and
# binding it here to the admin DSN would refuse every later connect to the worker's.

PG_DSN_ENV = "MIPYRE_TEST_PG_DSN"

_PG_ADMIN = r"""
import sys
from mipyre.db import PgPool
dsn, verb, name = sys.argv[1:4]
pool = PgPool.connect(dsn, max_connections=1)
pool.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
if verb == "create":
    pool.execute(f'CREATE DATABASE "{name}"')
"""


_WORKER_DB = pytest.StashKey[tuple[str, str]]()


class WorkerDatabaseError(RuntimeError):
    """The per-worker test database could not be created or dropped."""


def worker_dsn(dsn: str, worker: str) -> tuple[str, str]:
    """`dsn` pointed at `worker`'s own database, and that database's name."""
    parts = urllib.parse.urlsplit(dsn)
    name = f"{parts.path.lstrip('/') or 'postgres'}_{worker}"
    return urllib.parse.urlunsplit(parts._replace(path="/" + name)), name


def _pg_admin(dsn: str, verb: str, name: str) -> None:
    r = subprocess.run(
        [sys.executable, "-c", _PG_ADMIN, dsn, verb, name],
        capture_output=True, text=True, timeout=60,
    )
    if r.returncode != 0:
        raise WorkerDatabaseError(f"could not {verb} the worker database {name}:\n{r.stderr[-2000:]}")


# Test-only cargo features (b/186). A test that needs one carries `@pytest.mark.needs_feature("<name>")`, and this
# hook is the only place that decides whether it runs: the probe below reads the built extension. The gate selects
# the whole set with `-m needs_feature` in its feature build, so a feature test added later is not left out by its
# file name. In a build without the feature the test is skipped with a reason that names `--features`, which the
# default gate step defers to the feature build (dev/skip_gate.py). A name without a probe is an error.
FEATURE_PROBES = {
    "fault_injection": lambda engine: hasattr(engine, "_fault_panic"),
    "bench": lambda engine: hasattr(engine.MipyreApp, "bench_inmem"),
}


# T1 (b/441): a TestServer's close waits for its threads for the app's `shutdown_timeout`, 5 s by default,
# and raises AbandonedWorkersError past it. A test that does not set the grace itself is not testing it: its close waits
# for the stop as an event, for longer than pytest-timeout's hang guard. How long idle TPC threads take to stop under
# load is b/454; the grace's own judgement is `join_within`'s and test_b251_shutdown_timeout's. The product's
# default is unchanged (the TestServer's own behaviour is public API).
TEST_SHUTDOWN_GRACE_S = 3600.0


def _waiting_for_the_stop(init):
    default = Mipyre().shutdown_timeout

    def _start(self, app, *args, **kwargs):
        if app.shutdown_timeout == default:
            app.shutdown_timeout = TEST_SHUTDOWN_GRACE_S
        init(self, app, *args, **kwargs)

    return _start


def pytest_configure(config):
    TestServer._start = _waiting_for_the_stop(TestServer._start)
    config.addinivalue_line(
        "markers", "needs_feature(name): needs a build with the named test-only cargo feature (--features <name>)"
    )
    # Each test's requested sleep seconds and work, as JUnit properties for dev/test_time_gate.py (b/441, b/452).
    cost = TestCost()
    cost.install()
    config.pluginmanager.register(cost, "mipyre-test-cost")
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    dsn = os.environ.get(PG_DSN_ENV)
    if worker is None or dsn is None:
        return
    own_dsn, name = worker_dsn(dsn, worker)
    _pg_admin(dsn, "create", name)
    config.stash[_WORKER_DB] = (dsn, name)
    os.environ[PG_DSN_ENV] = own_dsn


def pytest_collection_modifyitems(config, items):
    import mipyre.engine as engine

    for item in items:
        for marker in item.iter_markers("needs_feature"):
            (name,) = marker.args
            if not FEATURE_PROBES[name](engine):
                item.add_marker(
                    pytest.mark.skip(reason=f"needs --features {name}: maturin develop --release --features {name}")
                )


def pytest_unconfigure(config):
    cost = config.pluginmanager.get_plugin("mipyre-test-cost")
    if cost is not None:
        cost.uninstall()
    made = config.stash.get(_WORKER_DB, None)
    if made is None:
        return
    admin_dsn, name = made
    os.environ[PG_DSN_ENV] = admin_dsn
    _pg_admin(admin_dsn, "drop", name)



@dataclass
class ServerHandle:
    base_url: str
    mode: str
    proc: subprocess.Popen

    def get(self, path: str, headers: dict | None = None) -> tuple[int, str, dict]:
        req = urllib.request.Request(self.base_url + path, headers=headers or {})
        try:
            resp = urllib.request.urlopen(req, timeout=5)
            return resp.status, resp.read().decode(), dict(resp.headers)
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode(), dict(e.headers)

    def post(
        self, path: str, body: bytes | str | None = None,
        headers: dict | None = None,
    ) -> tuple[int, str, dict]:
        data = body.encode() if isinstance(body, str) else body
        req = urllib.request.Request(
            self.base_url + path, data=data, headers=headers or {}, method="POST",
        )
        try:
            resp = urllib.request.urlopen(req, timeout=5)
            return resp.status, resp.read().decode(), dict(resp.headers)
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode(), dict(e.headers)


def _boot(script: str, mode: str) -> tuple[subprocess.Popen, int]:
    """Start a Mipyre server from a script string on a port the kernel picks. Returns
    the process handle and the bound port (from the server's startup line).
    `mode` "gil" serves every handler on the main interpreter, "subinterp" leaves the default
    runtime: the script reads $MIPYRE_TEST_MODE (the runtime, "main" or "subinterp") and passes
    it as `Mipyre(runtime=...)`, since an app's runtime is its code's (b/335 D9).
    """
    fd, path = tempfile.mkstemp(prefix="mipyre_test_", suffix=".py")
    with os.fdopen(fd, "w") as f:
        f.write(script)
    env = dict(os.environ)
    env["MIPYRE_TEST_MODE"] = "main" if mode == "gil" else "subinterp"
    env["MIPYRE_PORT"] = "0"
    # Output goes to a file, not a pipe: nobody drains a pipe while tests run, so a chatty
    # server could block on a full one, and `_teardown` scans the whole log, shutdown
    # included (Layer 2, FR-12).
    log_path = path + ".log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, path],
            stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True, env=env,
        )
    proc.mipyre_log = log_path  # type: ignore[attr-defined]  # the log rides on the Popen to the teardown that reads it
    try:
        port = bound_port(read_file(log_path), proc)
    except RuntimeError as e:
        proc.kill()
        proc.wait(timeout=5)
        raise RuntimeError(f"Mipyre server ({mode} mode) failed to start: {e}") from None
    # Then until it answers: waits for the answer or the process exiting, for no length of time (a loaded
    # machine starts a server late, b/325); a hung start is pytest-timeout's.
    errors: list[Exception] = []

    def answered() -> bool:
        if proc.poll() is not None:
            with open(log_path, errors="replace") as f:
                out = f.read()
            raise RuntimeError(
                f"Mipyre server ({mode} mode on port {port}) exited with {proc.returncode} before answering: "
                f"{errors[-1] if errors else 'no request was answered'}\nServer output:\n{out[:2000]}"
            )
        try:
            urllib.request.urlopen(f"http://{HOST}:{port}/__ping", timeout=0.5).close()
            return True
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"Mipyre server ({mode} mode on port {port}) answered /__ping with {e.code}") from e
        except OSError as e:  # refused / reset / timed out while it starts: look again
            errors.append(e)
            return False

    poll_until(answered, timeout=None, what=f"the {mode} server on port {port} answering")
    return proc, port


def _teardown(proc: subprocess.Popen) -> None:
    """Stop the server the way a user does (SIGINT → graceful shutdown), then fail if its
    log, shutdown included, contains a PyO3 fork panic."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        proc.wait(timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            pass
    log_path = getattr(proc, "mipyre_log", None)
    if log_path and os.path.exists(log_path):
        with open(log_path, errors="replace") as f:
            panics = fork_panic_lines(f.read())
        assert panics == [], "server log has PyO3 fork panics:\n" + "\n".join(panics)


def feature_server_factory(script: str):
    """Build a parametrised pytest fixture that runs `script` in both
    GIL and sub-interp mode. Scope is module to amortise startup cost
    across a whole file.

    Usage:
      from tests.conftest import feature_server_factory
      feature_server = feature_server_factory(SERVER_SCRIPT)
    """

    @pytest.fixture(scope="module", params=["gil", "subinterp"])
    def feature_server(request):
        proc, port = _boot(script, request.param)
        try:
            yield ServerHandle(
                base_url=f"http://{HOST}:{port}",
                mode=request.param,
                proc=proc,
            )
        finally:
            _teardown(proc)

    return feature_server


# ---------------------------------------------------------------------------
# Layer 2 (FR-12): fork panic-text scan for E2E server logs
# ---------------------------------------------------------------------------

# The two refusals the PyO3 fork raises once more than one interpreter has executed the
# engine and a thread with no thread state touches Python. A grep of src/ can't see
# drops, so every Layer 2 E2E scans its server log for these instead.
FORK_PANIC_TEXTS = (
    "Python::attach was called on a thread that has no Python thread state",
    "was dropped on a thread that has no Python thread state",
)


def fork_panic_lines(log_text: str) -> list[str]:
    """Lines of `log_text` that contain a fork panic text."""
    return [
        line for line in log_text.splitlines()
        if any(t in line for t in FORK_PANIC_TEXTS)
    ]


@pytest.fixture(scope="session")
def pyo3_fixture_dirs(tmp_path_factory):
    """{variant: dir holding the importable PyO3 fixture module} (`tests/_pyo3_fixture.py`)."""
    return _pyo3_fixture.build(tmp_path_factory.mktemp("pyo3_fixtures"))
