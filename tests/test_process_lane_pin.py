"""The decisions that pin every process of a server to the launcher's copy of `mipyre`."""

import json
import logging
import os
import shutil
import signal
import stat
import subprocess
import sys
import types
import urllib.error
import urllib.request

import pytest

import mipyre
from mipyre import engine
from tests._helpers import bound_port, poll_until, read_file
from mipyre._clone import PrivateCopyError, claim_name, clone_tree, ensure_private_dir
from mipyre._process_lane import (
    START_REFUSED,
    _remove_stale_run_dirs,
    is_pinned,
    moved_path,
    pin_env,
    pin_packages,
    same_build,
    unpin_env,
)


@pytest.mark.parametrize(
    ("environ", "want"),
    [
        ({}, "/run/x"),
        ({"PYTHONPATH": ""}, "/run/x"),
        ({"PYTHONPATH": "a"}, f"/run/x{os.pathsep}a"),
    ],
)
def test_pin_env_puts_the_run_dir_first_and_never_adds_an_empty_entry(environ, want):
    assert pin_env(environ, "/run/x") == [("PYTHONPATH", want)]


def test_a_file_inside_the_run_dir_is_pinned(tmp_path):
    run_dir = tmp_path / "run"
    (run_dir / "mipyre").mkdir(parents=True)
    file = run_dir / "mipyre" / "__init__.py"
    file.write_text("")
    assert is_pinned(str(file), str(run_dir))


def test_a_file_in_a_sibling_with_the_same_prefix_is_not_pinned(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    sibling = tmp_path / "runx" / "mipyre"
    sibling.mkdir(parents=True)
    file = sibling / "__init__.py"
    file.write_text("")
    assert not is_pinned(str(file), str(run_dir))


def test_a_symlink_into_the_run_dir_is_judged_by_where_it_points(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "__init__.py").write_text("")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "__init__.py").write_text("")
    (run_dir / "mipyre").symlink_to(elsewhere, target_is_directory=True)
    assert not is_pinned(str(run_dir / "mipyre" / "__init__.py"), str(run_dir))
    assert is_pinned(str(tmp_path / "run" / "." / "__init__.py"), str(run_dir))


def test_the_engine_names_its_build_with_the_package_version():
    version, _, tree = engine.BUILD_ID.partition("+")
    assert version and len(tree) == 16


@pytest.mark.parametrize(
    ("pythonpath", "want"),
    [
        (None, None),
        ("", None),
        ("a", "a"),
        (f"a{os.pathsep}b", f"a{os.pathsep}b"),
    ],
)
def test_unpin_env_gives_back_the_launchers_entries_and_no_empty_pythonpath(pythonpath, want):
    """An empty PYTHONPATH and none give the same `sys.path`, so both come back as none."""
    environ = {} if pythonpath is None else {"PYTHONPATH": pythonpath}
    pinned = dict(pin_env(environ, "/run/x"))
    unpin_env(pinned, "/run/x")
    assert pinned.get("PYTHONPATH") == want


def test_unpin_env_leaves_a_path_that_does_not_start_with_the_run_dir():
    environ = {"PYTHONPATH": f"a{os.pathsep}/run/x"}
    unpin_env(environ, "/run/x")
    assert environ == {"PYTHONPATH": f"a{os.pathsep}/run/x"}


def test_an_empty_run_dir_pins_nothing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "__init__.py").write_text("")
    assert not is_pinned(str(tmp_path / "__init__.py"), "")


@pytest.mark.parametrize(
    ("path", "want"),
    [
        ("/venv/mipyre", "/run/x/mipyre"),
        ("/venv/mipyre/_process_lane", "/run/x/mipyre/_process_lane"),
        ("/venv/mipyrex", "/venv/mipyrex"),
        ("/elsewhere/mipyre", "/elsewhere/mipyre"),
    ],
)
def test_a_path_moves_into_the_clone_only_from_inside_the_package(path, want):
    assert moved_path(path, "/venv/mipyre", "/run/x/mipyre") == want


def test_pinning_repoints_each_loaded_mipyre_package_in_place_and_nothing_else():
    paths = ["/venv/mipyre"]
    modules = {
        "mipyre": types.SimpleNamespace(__path__=paths),
        "mipyre._process_lane": types.SimpleNamespace(__path__=["/venv/mipyre/_process_lane"]),
        "mipyre.engine": types.SimpleNamespace(),
        "mipyre.blocked": None,
        "mipyrex": types.SimpleNamespace(__path__=["/venv/mipyre"]),
    }
    pin_packages(modules, "/venv/mipyre", "/run/x/mipyre")
    assert modules["mipyre"].__path__ is paths and paths == ["/run/x/mipyre"]
    assert modules["mipyre._process_lane"].__path__ == ["/run/x/mipyre/_process_lane"]
    assert modules["mipyrex"].__path__ == ["/venv/mipyre"]


def test_the_same_build_id_is_the_same_build():
    assert same_build("2.10.0+0123456789abcdef", "2.10.0+0123456789abcdef")


def test_a_build_id_one_character_off_or_missing_is_another_build():
    assert not same_build("2.10.0+0123456789abcdef", "2.10.0+0123456789abcdee")
    assert not same_build(None, "2.10.0+0123456789abcdef")


def test_a_held_name_cannot_be_claimed_until_its_lock_is_released(tmp_path):
    first = claim_name(str(tmp_path), "n")
    assert first is not None
    try:
        assert claim_name(str(tmp_path), "n") is None
    finally:
        os.close(first)
    again = claim_name(str(tmp_path), "n")
    assert again is not None
    os.close(again)
    assert (tmp_path / "n.lock").exists()


def test_a_dir_others_can_write_is_not_private(tmp_path):
    shared = tmp_path / "shared"
    shared.mkdir()
    shared.chmod(0o777)
    with pytest.raises(PrivateCopyError, match="writable by other users"):
        ensure_private_dir(str(shared))


def test_a_symlink_is_not_a_private_dir(tmp_path):
    (tmp_path / "real").mkdir(mode=0o700)
    (tmp_path / "link").symlink_to(tmp_path / "real", target_is_directory=True)
    with pytest.raises(PrivateCopyError, match="not a directory"):
        ensure_private_dir(str(tmp_path / "link"))


def test_a_new_private_dir_is_owner_only(tmp_path):
    ensure_private_dir(str(tmp_path / "a" / "b"))
    assert stat.S_IMODE((tmp_path / "a" / "b").stat().st_mode) == 0o700


def test_a_clone_does_not_see_later_changes_to_its_source(tmp_path):
    src = tmp_path / "pkg"
    src.mkdir()
    (src / "in_place.py").write_text("VALUE = 1\n")
    (src / "replaced.py").write_text("VALUE = 1\n")
    clone_tree(str(src), str(tmp_path / "clone"))

    with open(src / "in_place.py", "r+") as f:
        f.write("VALUE = 2\n")
    (src / "replaced.new").write_text("VALUE = 2\n")
    os.replace(src / "replaced.new", src / "replaced.py")

    assert (tmp_path / "clone" / "in_place.py").read_text() == "VALUE = 1\n"
    assert (tmp_path / "clone" / "replaced.py").read_text() == "VALUE = 1\n"


def test_a_clone_keeps_its_sources_mtime_so_their_cached_bytecode_stays_valid(tmp_path):
    src = tmp_path / "pkg"
    src.mkdir()
    (src / "mod.py").write_text("VALUE = 1\n")
    os.utime(src / "mod.py", (1_000_000_000, 1_000_000_000))
    clone_tree(str(src), str(tmp_path / "clone"))
    assert (tmp_path / "clone" / "mod.py").stat().st_mtime == 1_000_000_000


def test_a_copy_that_cannot_be_made_either_way_warns_raises_and_leaves_nothing(tmp_path, caplog):
    # An in-process server started by an earlier test leaves the root logger at its level (ERROR).
    caplog.set_level(logging.WARNING, logger="mipyre.clone")
    src = tmp_path / "pkg"
    src.mkdir()
    (src / "__init__.py").write_text("")
    parent = tmp_path / "locked"
    parent.mkdir()
    parent.chmod(0o500)
    try:
        with pytest.raises(PrivateCopyError, match="Permission denied") as raised:
            clone_tree(str(src), str(parent / "clone"))
        assert isinstance(raised.value.__cause__, OSError)
        assert "making a plain copy instead" in caplog.text
        assert not (parent / "clone").exists()
    finally:
        parent.chmod(0o700)


def test_only_run_dirs_whose_lock_is_free_are_removed(tmp_path):
    root = tmp_path / "run"
    root.mkdir()
    for name in ("stale", "live"):
        (root / name / "mipyre").mkdir(parents=True)
        (root / f"{name}.lock").write_text("")
    (root / "unlocked" / "mipyre").mkdir(parents=True)
    live = claim_name(str(root), "live")
    try:
        _remove_stale_run_dirs(str(root))
        assert sorted(os.listdir(root)) == ["live", "live.lock", "unlocked"]
    finally:
        os.close(live)



def test_a_stale_run_dir_that_cannot_be_removed_is_reported_and_the_sweep_goes_on(tmp_path, caplog):
    """A run directory an ended server left that cannot be removed is that one directory's
    problem: the sweep warns naming it, releases its lock, and removes the next one."""
    root = tmp_path / "run"
    stuck = root / "stuck" / "mipyre"
    stuck.mkdir(parents=True)
    (stuck / "module.py").write_text("")
    (root / "stuck.lock").write_text("")
    (root / "gone" / "mipyre").mkdir(parents=True)
    (root / "gone.lock").write_text("")
    stuck.chmod(0o500)  # its file cannot be unlinked, so rmtree fails
    try:
        with caplog.at_level(logging.WARNING, logger="mipyre.run_dir"):
            _remove_stale_run_dirs(str(root))
        assert sorted(os.listdir(root)) == ["stuck", "stuck.lock"]
        assert str(root / "stuck") in caplog.text
        lock = claim_name(str(root), "stuck")
        assert lock is not None, "the failed removal kept the lock"
        os.close(lock)
    finally:
        stuck.chmod(0o700)

_CLAIM_AND_SWEEP = """
import os, sys
from mipyre._clone import claim_name
from mipyre._process_lane import _remove_run_dir, _remove_stale_run_dirs

root = sys.argv[1]
for _ in range(200):
    while (lock := claim_name(root, name := os.urandom(8).hex())) is None:
        pass
    os.mkdir(os.path.join(root, name))
    _remove_stale_run_dirs(root)
    _remove_run_dir(root, name, lock)
"""


def test_launchers_claiming_and_sweeping_run_dirs_at_once_never_trip_over_each_other(tmp_path):
    """A sweeper may delete a lock file between another launcher's open and its lock; that one claims anew."""
    procs = [
        subprocess.Popen([sys.executable, "-c", _CLAIM_AND_SWEEP, str(tmp_path)], stderr=subprocess.PIPE, text=True)
        for _ in range(8)
    ]
    failures = [err for p in procs if (err := p.communicate(timeout=60)[1]) or p.returncode != 0]
    assert failures == []
    assert os.listdir(tmp_path) == []


_PROBE_APP = '''
import os

from mipyre import Mipyre

app = Mipyre()


@app.get("/probe", runtime="process")
def probe(req):
    from mipyre import _pin_probe

    return {"value": _pin_probe.VALUE, "file": _pin_probe.__file__, "pid": os.getpid()}


@app.get("/die", runtime="process")
def die(req):
    os._exit(1)
'''


def _get(port, path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=30) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def test_workers_keep_the_launchers_mipyre_while_the_install_changes(tmp_path):
    """A worker, spawned or forked, imports a `mipyre` module it had not loaded from the launcher's copy."""
    site = tmp_path / "site"
    shutil.copytree(os.path.dirname(mipyre.__file__), site / "mipyre", ignore=shutil.ignore_patterns("__pycache__"))
    probe = site / "mipyre" / "_pin_probe.py"
    probe.write_text('VALUE = "old"\n')
    (tmp_path / "app.py").write_text(_PROBE_APP)
    home = tmp_path / "home"
    env = {k: v for k, v in os.environ.items() if k != "XDG_CACHE_HOME"}
    env.update(PYTHONPATH=str(site), HOME=str(home), MIPYRE_PROCESS_WORKERS="1")
    log_path = tmp_path / "server.log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "mipyre", "run", "app.py:app", "--port", "0"],
            cwd=tmp_path, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
    run_root = home / ("Library/Caches" if sys.platform == "darwin" else ".cache") / "mipyre" / "run"
    try:
        port = bound_port(read_file(str(log_path)), proc)

        def answer():
            status, body = _get(port, "/probe")
            return json.loads(body) if status == 200 else None

        first = poll_until(answer, timeout=None, what="the first worker's answer")
        assert first["value"] == "old"
        assert is_pinned(first["file"], str(run_root))

        def replaced_after(change, before):
            change()
            assert _get(port, "/die")[0] == 502
            return poll_until(lambda: (a := answer()) and a["pid"] != before["pid"] and a, timeout=None, what="a replacement worker")

        def in_place():
            # A different size: a same-size edit in the same second would leave the cached .pyc valid.
            with open(probe, "r+") as f:
                f.write('VALUE = "new, a different size"\n')

        def write_new_and_rename():
            (site / "mipyre" / "_pin_probe.next").write_text('VALUE = "newer"\n')
            os.replace(site / "mipyre" / "_pin_probe.next", probe)

        second = replaced_after(in_place, first)
        third = replaced_after(write_new_and_rename, second)
        assert (second["value"], third["value"]) == ("old", "old")
    finally:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        proc.wait(timeout=30)
    assert os.listdir(run_root) == [], log_path.read_text()[-3000:]


def _spawned_worker(pythonpath, build_id):
    env = {k: v for k, v in os.environ.items() if k != "MIPYRE_BUILD_ID"}
    env["PYTHONPATH"] = pythonpath
    if build_id is not None:
        env["MIPYRE_BUILD_ID"] = build_id
    return subprocess.run(
        [sys.executable, "-m", "mipyre._process_lane.worker"], env=env, capture_output=True, text=True, timeout=60
    )


def test_a_spawned_worker_that_imported_another_mipyre_refuses_to_start(tmp_path):
    elsewhere = tmp_path / "not-the-run-dir"
    elsewhere.mkdir()
    done = _spawned_worker(str(elsewhere), engine.BUILD_ID)
    assert done.returncode == START_REFUSED, done.stderr
    assert mipyre.__file__ in done.stderr and str(elsewhere) in done.stderr


def test_a_spawned_worker_of_another_build_refuses_to_start(tmp_path):
    run_dir = tmp_path / "run"
    shutil.copytree(os.path.dirname(mipyre.__file__), run_dir / "mipyre", ignore=shutil.ignore_patterns("__pycache__"))
    done = _spawned_worker(str(run_dir), "0.0.0+0000000000000000")
    assert done.returncode == START_REFUSED, done.stderr
    assert "0.0.0+0000000000000000" in done.stderr and engine.BUILD_ID in done.stderr
