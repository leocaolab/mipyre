"""How one server run ends — the outcome it raises (`run_outcome`), what `mipyre run`
does with it, and what a `TestClient` hands back."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap

import pytest

from mipyre import AbandonedWorkersError, Mipyre
from mipyre._server_run import run_outcome
from mipyre.testing import TestClient


# ---------------------------------------------------------------------------
# run_outcome: the one exception a run raises
# ---------------------------------------------------------------------------


def test_a_run_with_no_failure_raises_nothing():
    assert run_outcome([], []) is None


def test_one_failure_is_raised_as_it_is():
    failed = PermissionError("not the owner")
    assert run_outcome([failed], []) is failed


def test_several_failures_are_one_exception_group_in_order():
    first, second = PermissionError("a"), OSError("b")
    raised = run_outcome([first, second], [])
    assert isinstance(raised, ExceptionGroup)
    assert list(raised.exceptions) == [first, second]


def test_a_base_exception_among_the_failures_makes_a_base_exception_group():
    hook_failed, exited = OSError("b"), SystemExit(3)
    raised = run_outcome([hook_failed, exited], [])
    assert type(raised) is BaseExceptionGroup
    assert list(raised.exceptions) == [hook_failed, exited]


def test_abandoned_workers_alone_are_raised_with_no_cause():
    raised = run_outcome([], ["mipyre-worker-0 (running GET /)"])
    assert isinstance(raised, AbandonedWorkersError)
    assert raised.workers == ("mipyre-worker-0 (running GET /)",)
    assert raised.__cause__ is None
    assert str(raised) == "worker(s) mipyre-worker-0 (running GET /) did not stop within the shutdown grace period"


def test_abandoned_workers_carry_the_rest_of_the_outcome_as_their_cause():
    first, second = PermissionError("a"), OSError("b")
    raised = run_outcome([first, second], ["w-1"])
    assert isinstance(raised, AbandonedWorkersError)
    assert isinstance(raised.__cause__, ExceptionGroup)
    assert list(raised.__cause__.exceptions) == [first, second]


# ---------------------------------------------------------------------------
# TestClient.close() hands back the run's outcome
# ---------------------------------------------------------------------------


def test_close_raises_a_shutdown_hook_failure_once():
    app = Mipyre(runtime="main")

    @app.get("/")
    def index(req):
        return "ok"

    @app.on_shutdown
    def closes_a_pool():
        raise RuntimeError("pool closed")

    client = TestClient(app)
    assert client.get("/").text == "ok"
    with pytest.raises(RuntimeError, match="pool closed"):
        client.close()
    client.close()


def test_a_request_after_the_server_died_names_why_it_died():
    app = Mipyre(runtime="main")

    @app.get("/")
    def index(req):
        return "ok"

    @app.on_shutdown
    def closes_a_pool():
        raise RuntimeError("pool closed")

    client = TestClient(app)
    app.stop()
    client.server._thread.join(20)
    with pytest.raises(RuntimeError, match="request failed") as raised:
        client.get("/")
    assert isinstance(raised.value.__cause__, RuntimeError), raised.value.__cause__
    assert str(raised.value.__cause__) == "pool closed"


# ---------------------------------------------------------------------------
# `mipyre run`: a failed run exits non-zero and says why
# ---------------------------------------------------------------------------


APP = """
import pathlib

from mipyre import Mipyre

app = Mipyre()
{grace}

@app.get("/")
def index(req):
    return "ok"


@app.on_shutdown
def closes_a_pool():
    raise RuntimeError("pool closed")


@app.on_shutdown
def records_it_ran():
    pathlib.Path("second-hook-ran").write_text("yes")
"""


def _run_until_serving_then_interrupt(
    tmp_path, grace: str, flags: tuple[str, ...] = ("--threads", "2")
) -> subprocess.CompletedProcess:
    """``mipyre run`` the app above with ``flags``, wait for its banner, SIGINT it, and return
    how it ended."""
    (tmp_path / "server_run_app.py").write_text(textwrap.dedent(APP.format(grace=grace)))
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(tmp_path), os.environ.get("PYTHONPATH")]))
    proc = subprocess.Popen(
        [sys.executable, "-m", "mipyre.cli", "run", "server_run_app:app", "--port", "0", *flags],
        cwd=tmp_path, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    assert proc.stdout is not None
    seen = []
    for line in proc.stdout:
        seen.append(line)
        if line.strip().startswith("Mipyre v"):
            break
    else:
        out, err = proc.communicate()
        pytest.fail(f"the server never started: {''.join(seen)}{out}{err[-4000:]}")
    proc.send_signal(signal.SIGINT)
    out, err = proc.communicate(timeout=120)
    return subprocess.CompletedProcess(proc.args, proc.returncode, "".join(seen) + out, err)


def test_mipyre_run_exits_1_with_the_traceback_of_a_shutdown_hook_that_raised(tmp_path):
    done = _run_until_serving_then_interrupt(tmp_path, grace="")
    assert done.returncode == 1, (done.returncode, done.stderr[-4000:])
    assert "RuntimeError: pool closed" in done.stderr, done.stderr[-4000:]
    assert 'raise RuntimeError("pool closed")' in done.stderr, done.stderr[-4000:]
    assert (tmp_path / "second-hook-ran").read_text() == "yes"


def test_mipyre_run_with_abandoned_workers_prints_the_whole_chain_and_exits_1(tmp_path):
    # A grace no thread can meet: every worker is still on its way out when it ends.
    done = _run_until_serving_then_interrupt(tmp_path, grace="app.shutdown_timeout = 1e-9")
    assert done.returncode == 1, (done.returncode, done.stderr[-4000:])
    assert "mipyre._server_run.AbandonedWorkersError: worker(s) " in done.stderr, done.stderr[-4000:]
    assert "RuntimeError: pool closed" in done.stderr, done.stderr[-4000:]
    assert "The above exception was the direct cause of the following exception" in done.stderr, done.stderr[-4000:]
    assert "exiting without finalization" in done.stderr, done.stderr[-4000:]
    assert (tmp_path / "second-hook-ran").read_text() == "yes"


def test_a_flag_the_server_does_not_use_stops_the_start_naming_the_flag_given(tmp_path):
    # The thread-per-core server (the default) has no sub-interpreter pool to size: a size given
    # for it is an error, named as the flag it was given as.
    (tmp_path / "server_run_app.py").write_text(textwrap.dedent(APP.format(grace="")))
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(tmp_path), os.environ.get("PYTHONPATH")]))
    done = subprocess.run(
        [sys.executable, "-m", "mipyre.cli", "run", "server_run_app:app", "--port", "0", "--subinterpreters", "3"],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120, check=False,
    )
    assert done.returncode != 0, done.stdout[-3000:]
    assert "subinterpreters is given (mipyre run --subinterpreters" in done.stderr, done.stderr[-3000:]
    assert "does not use it" in done.stderr, done.stderr[-3000:]
    assert "ServerFlags(subinterpreters=)" not in done.stderr, done.stderr[-3000:]


# ---------------------------------------------------------------------------
# app_file: the app's source file, as the engine resolves it
# ---------------------------------------------------------------------------


def test_a_named_app_file_wins_over_main_s_file(tmp_path, monkeypatch):
    monkeypatch.setattr(sys.modules["__main__"], "__file__", str(tmp_path / "main.py"), raising=False)
    app = Mipyre()
    app._engine.set_script_path(str(tmp_path / "named.py"))
    assert app._engine.app_file() == str(tmp_path / "named.py")


def test_with_nothing_named_the_app_file_is_main_s_file(tmp_path, monkeypatch):
    monkeypatch.setattr(sys.modules["__main__"], "__file__", str(tmp_path / "main.py"), raising=False)
    assert Mipyre()._engine.app_file() == str(tmp_path / "main.py")


def test_a_relative_app_file_is_taken_from_the_working_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys.modules["__main__"], "__file__", "./src/../app.py", raising=False)
    assert Mipyre()._engine.app_file() == os.path.abspath("./src/../app.py")
