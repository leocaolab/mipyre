"""How a process-lane start fails, with what really happened (b/363, b/364): a worker that
ends before it is ready, and a fork the OS refuses."""

from __future__ import annotations

import errno
import json
import os
import re
import subprocess
import sys
import textwrap

import pytest

from mipyre._process_lane.template import (
    LANE,
    MAX_FRAME_BYTES,
    RUN,
    SETUP,
    CommandFrameError,
    ForkFailureWithoutErrno,
    exited_frame,
    fit_traceback,
    fork_failure_frame,
    forked_frame,
    import_failed_frame,
    lane_fds_missing_frame,
    parse_run,
    parse_start,
    parse_words,
    split_frame,
)

APPS = os.path.join(os.path.dirname(__file__), "apps")
TEMPLATE_FRAMES = os.path.join(os.path.dirname(__file__), "fixtures", "template_frames.json")


def _golden() -> dict:
    with open(TEMPLATE_FRAMES) as f:
        return json.load(f)


def test_the_fork_failure_answer_is_the_errno_negated():
    answer = fork_failure_frame(3, 9, OSError(errno.EAGAIN, "fork: Resource temporarily unavailable"))
    assert int.from_bytes(answer[-4:], "little", signed=True) == -errno.EAGAIN


def test_a_fork_failure_without_an_errno_has_no_answer():
    cause = OSError("fork: no errno")
    with pytest.raises(ForkFailureWithoutErrno) as raised:
        fork_failure_frame(3, 9, cause)
    assert raised.value.__cause__ is cause


def test_the_template_writes_the_reports_the_server_reads():
    """The server decodes the same file (lane/tests.rs). A change here is a protocol change:
    read the diff, then change both sides and the file together."""
    golden = _golden()
    assert golden["max_frame_bytes"] == MAX_FRAME_BYTES
    for case in golden["reports"]:
        match case["kind"]:
            case "forked" if "pid" in case:
                written = forked_frame(case["lane"], case["attempt"], case["pid"])
            case "forked":
                written = fork_failure_frame(case["lane"], case["attempt"], OSError(case["errno"], case["what"]))
            case "exited":
                written = exited_frame(case["lane"], case["pid"], case["status"])
            case "import_failed":
                written = import_failed_frame(case["traceback"])
            case "lane_fds_missing":
                written = lane_fds_missing_frame(case["lane"], case["got"])
        assert written.hex() == case["hex"], case["what"]


def test_the_template_reads_the_commands_the_server_writes():
    """The server encodes the same file (lane/tests.rs)."""
    for case in _golden()["commands"]:
        frame = bytes.fromhex(case["hex"])
        tag, body, used = split_frame(frame)
        assert used == len(frame), case["what"]
        match case["kind"]:
            case "setup":
                assert (tag, json.loads(body)) == (SETUP, json.loads(case["json"])), case["what"]
            case "lane":
                assert (tag, parse_words(tag, body, 1)) == (LANE, [case["lane"]]), case["what"]
            case "start":
                assert parse_start(tag, body) == (case["lane"], case["attempt"]), case["what"]
            case "run":
                assert (tag, parse_run(body)) == (RUN, case["json"]), case["what"]


def test_a_command_frame_is_whole_only_with_its_last_byte():
    frame = bytes.fromhex(_golden()["commands"][-1]["hex"])
    assert split_frame(frame[:-1]) is None
    assert split_frame(frame[:3]) is None
    assert split_frame(frame + b"\x00")[2] == len(frame)


def test_a_command_frame_past_the_limit_or_with_no_tag_is_refused_from_its_header():
    assert split_frame(MAX_FRAME_BYTES.to_bytes(4, "little")) is None
    for n in (0, MAX_FRAME_BYTES + 1):
        with pytest.raises(CommandFrameError):
            split_frame(n.to_bytes(4, "little"))


def test_a_frame_other_than_a_start_request_is_refused_where_one_comes():
    with pytest.raises(CommandFrameError):
        parse_start(SETUP, bytes(8))
    with pytest.raises(CommandFrameError):
        parse_start(3, bytes(7))


def test_a_traceback_that_fits_is_sent_whole_and_a_longer_one_keeps_its_end_and_says_what_was_cut():
    assert fit_traceback(b"x" * 100, 100) == b"x" * 100
    text = b"x" * 150 + b"ValueError: the end"
    cut = fit_traceback(text, 100)
    assert len(cut) <= 100
    note, _, kept = cut.partition(b"\n")
    assert text.endswith(kept)
    assert note.decode() == f"[mipyre: the first {len(text) - len(kept)} bytes of this traceback were cut]"
    whole = import_failed_frame("é" * MAX_FRAME_BYTES)
    assert len(whole) <= 4 + MAX_FRAME_BYTES
    tag, body, _ = split_frame(whole)
    body.decode()  # cut on a character, not inside one


def test_a_fork_the_os_refuses_raises_its_own_errno():
    """Under RLIMIT_NPROC 1 the driver's fork fails with EAGAIN, and the OSError says so.
    RLIMIT_NPROC does not bind root, so a child started as root first becomes nobody."""
    code = textwrap.dedent(
        """
        import os, pwd, resource, sys
        from mipyre.engine import _driver_fork_worker
        if os.geteuid() == 0:
            nobody = pwd.getpwnam("nobody")
            os.setgid(nobody.pw_gid)
            os.setuid(nobody.pw_uid)
        resource.setrlimit(resource.RLIMIT_NPROC, (1, 1))
        try:
            pid = _driver_fork_worker()
        except OSError as e:
            print(e.errno, repr(e))
            sys.exit(0)
        if pid == 0:
            os._exit(0)
        print("forked", pid)
        sys.exit(1)
        """
    )
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stdout + r.stderr
    got, _, text = r.stdout.strip().partition(" ")
    assert int(got) == errno.EAGAIN, r.stdout
    assert "fork" in text, r.stdout


def test_a_worker_that_ends_before_ready_fails_the_start_with_its_pid_and_exit(tmp_path):
    app = os.path.join(APPS, "process_lane_worker_exits_app.py")
    env = dict(os.environ, MIPYRE_PROCESS_WORKERS="1", LANE_TEST_MARKER=str(tmp_path / "marker"))
    r = subprocess.run(
        [sys.executable, "-m", "mipyre", "run", f"{app}:app", "--port", "0"],
        capture_output=True, text=True, timeout=120, env=env,
    )
    out = r.stdout + r.stderr
    assert r.returncode != 0, out[-3000:]
    m = re.search(r"the process worker of lane 0 \(pid (\d+)\) ended before it was ready: exited with status 3", out)
    assert m, out[-3000:]
    assert int(m.group(1)) > 0
    assert "Listening on" not in out, out[-3000:]


def test_a_launcher_whose_app_fails_to_import_reports_that_error_alone():
    """The launcher waits for its template before it removes the run directory: a spawned
    template still starting would otherwise import `mipyre` from the install and refuse."""
    app = os.path.join(APPS, "process_lane_app_import_fails.py")
    r = subprocess.run(
        [sys.executable, "-m", "mipyre", "run", f"{app}:app", "--port", "0"],
        capture_output=True, text=True, timeout=120,
    )
    out = r.stdout + r.stderr
    assert r.returncode != 0, out[-3000:]
    assert "RuntimeError: the app's own import error" in out, out[-3000:]
    assert "refuses to start" not in out, out[-3000:]


def test_a_launcher_whose_start_fails_after_the_template_got_its_lanes_reports_that_error_alone():
    """The launcher ends its template before it removes the run directory, also once the
    template got its lanes: a spawned template still starting would otherwise import `mipyre`
    from the install, or from a half-removed copy, and say so."""
    app = os.path.join(APPS, "process_lane_startup_hook_fails.py")
    r = subprocess.run(
        [sys.executable, "-m", "mipyre", "run", f"{app}:app", "--port", "0"],
        capture_output=True, text=True, timeout=120,
    )
    out = r.stdout + r.stderr
    assert r.returncode != 0, out[-3000:]
    assert "RuntimeError: the app's own startup error" in out, out[-3000:]
    assert "refuses to start" not in out, out[-3000:]
    assert "ModuleNotFoundError" not in out, out[-3000:]


@pytest.mark.parametrize("start", ["import", "fork"])
def test_a_spawned_workers_message_gives_it_back_its_setup_lane_attempt_and_template(start):
    """The setup, and a spawned worker's message, come back as they were sent: the worker
    watches the template its message names, not its parent, which is 1 once the template died."""
    from mipyre._process_lane.server import LaneSetup, LaneWorker
    from mipyre.engine import StartStrategy

    setup = LaneSetup(
        start=StartStrategy.parse(start),
        workers=3,
        depth=8,
        slot_bytes=4096,
        worker_spin_us=50,
        target="/srv/app.py:app",
        routers=("app",),
        sys_path=("/srv", ""),
        cwd="/srv",
        thread_limits={"POLARS_MAX_THREADS": "1"},
        log_level="WARN",
        log_format="json",
        run_dir="/home/app/.cache/mipyre/run/0123456789abcdef",
    )
    sent = LaneWorker(
        setup,
        lane=2,
        template_pid=4242,
        attempt=7,
        fds=(5, 6),
        command_line='{"argv": [], "parsed": false}',
        build="mipyre-0.0.0+0123456789abcdef",
    )
    assert LaneSetup.decode(setup.encode()) == setup
    assert LaneWorker.decode(sent.encode()) == sent


def _spawned_worker(argv: list[str], setup: bytes | None) -> subprocess.CompletedProcess:
    """Runs a spawned worker's entry with `argv`; `setup`, when given, is written to the fd it
    names (`{fd}` in `argv`) and the fd closed, as the template does."""
    read_end, write_end = os.pipe()
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "mipyre._process_lane.worker", *(a.format(fd=read_end) for a in argv)],
            pass_fds=(read_end,),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    finally:
        os.close(read_end)
    with os.fdopen(write_end, "wb") as w:
        if setup is not None:
            w.write(setup)
    out, err = proc.communicate(timeout=60)
    return subprocess.CompletedProcess(proc.args, proc.returncode, out, err)


def test_a_spawned_worker_without_its_setup_fd_refuses_to_start():
    from mipyre._process_lane.server import START_REFUSED

    done = _spawned_worker([], None)
    assert done.returncode == START_REFUSED, done.stderr[-3000:]
    assert "takes --setup-fd N, got []" in done.stderr, done.stderr[-3000:]


def test_a_spawned_worker_whose_setup_is_not_one_refuses_to_start():
    from mipyre._process_lane.server import START_REFUSED

    done = _spawned_worker(["--setup-fd", "{fd}"], b"{not json")
    assert done.returncode == START_REFUSED, done.stderr[-3000:]
    assert "JSONDecodeError" in done.stderr, done.stderr[-3000:]
