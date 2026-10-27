"""The process-lane template: started by the launcher before the app
is imported; on the server's request it starts one worker per lane, and reaps them.

The launcher below is the one place that matches on `StartStrategy`:

- Import: the template holds no app code; each worker imports the app itself
  (driver: a fork of this bare template, or a new process on macOS);
- Fork: the template imports the app (with the first run's command line), checks it is still
  single-threaded, then forks each worker.

Each run's command line (`sys.argv` and the flag values) comes in a run frame before that run's
start requests: an import worker installs it before it imports the app, the fork template
installs it before it forks the run's workers.

`python -m mipyre._process_lane.template --sock FD` is the entry of a template the
SpawnDriver started; the ForkDriver's template comes in through `main` in the forked launcher.
Either starts before the launcher has a copy of `mipyre` to pin it to: the setup names that
copy's run directory, and the template points its `mipyre` packages there before it imports
anything else, or refuses to go on.
"""

from __future__ import annotations

import os
import selectors
import signal
import socket
import sys
import traceback
from typing import NoReturn

from mipyre.engine import (
    BUILD_ID,
    StartStrategy,
    _driver_fork_worker,
    _driver_name,
    _driver_start_import_worker,
)

from mipyre._process_lane import worker
from mipyre._process_lane.server import LaneSetup, LaneWorker, pin_env, pin_to_run_dir
from mipyre._process_lane.threads import require_single_threaded


class ForkFailureWithoutErrno(RuntimeError):
    """The driver's OSError for a failed start carried no errno: the template has no true
    answer for the server (its answer is the errno), so it ends instead of inventing one."""


class CommandFrameError(RuntimeError):
    """The server sent bytes that are no command frame the template takes: the template ends."""


# The template socket's frames (crates/process/src/template.rs is the server's side): a 4-byte
# little-endian length, then that many bytes, the first of which is the tag. Both directions
# number their tags in one space.
FRAME_HEADER_BYTES = 4
MAX_FRAME_BYTES = 64 * 1024
SETUP, LANE, START, FORKED, EXITED, IMPORT_FAILED, LANE_FDS_MISSING, RUN = range(1, 9)
# A lane's frame carries the lane's shared memory and its wake-pipe write end.
LANE_FDS = 2


def frame(tag: int, body: bytes) -> bytes:
    """The frame of `tag` with `body`."""
    return (1 + len(body)).to_bytes(FRAME_HEADER_BYTES, "little") + bytes([tag]) + body


def _words(*values: int, signed: tuple[bool, ...]) -> bytes:
    return b"".join(v.to_bytes(4, "little", signed=s) for v, s in zip(values, signed, strict=True))


def forked_frame(lane: int, attempt: int, pid: int) -> bytes:
    """The answer to start request `attempt` for `lane` that started worker `pid`. The attempt
    lets the server tell an answer that comes after its deadline from the one it waits on."""
    return frame(FORKED, _words(lane, attempt, pid, signed=(False, False, True)))


def fork_failure_frame(lane: int, attempt: int, e: OSError) -> bytes:
    """The answer to start request `attempt` for `lane` that failed with `e`: the errno negated
    in place of a pid (the server reads it back as the OS error it was)."""
    if e.errno is None:
        raise ForkFailureWithoutErrno(f"the worker start failed with no errno: {e!r}") from e
    return forked_frame(lane, attempt, -e.errno)


def exited_frame(lane: int, pid: int, status: int) -> bytes:
    """The report that worker `pid` of `lane` was reaped with wait status `status`."""
    return frame(EXITED, _words(lane, pid, status, signed=(False, True, True)))


def import_failed_frame(traceback_text: str) -> bytes:
    """The report that the app failed to import here (start="fork"), with `traceback_text` cut
    to fit a frame; a cut one says how many bytes it lost."""
    return frame(IMPORT_FAILED, fit_traceback(traceback_text.encode("utf-8", "replace"), MAX_FRAME_BYTES - 1))


def fit_traceback(text: bytes, budget: int) -> bytes:
    """`text`, or as much of its end (where the exception is) as fits `budget` bytes after a note
    of how many bytes of its start were cut."""
    if len(text) <= budget:
        return text
    longest_note = f"[mipyre: the first {len(text)} bytes of this traceback were cut]\n".encode()
    kept = text[len(text) - (budget - len(longest_note)) :].decode("utf-8", "ignore").encode()
    return f"[mipyre: the first {len(text) - len(kept)} bytes of this traceback were cut]\n".encode() + kept


def lane_fds_missing_frame(lane: int, got: int) -> bytes:
    """The report that `lane`'s frame came with `got` fds, not LANE_FDS."""
    return frame(LANE_FDS_MISSING, _words(lane, got, signed=(False, False)))


def frame_length(head: bytes | bytearray) -> int:
    """The length a frame's header gives, refused past MAX_FRAME_BYTES or with no room for a tag
    before anything is read for it."""
    n = int.from_bytes(head[:FRAME_HEADER_BYTES], "little")
    if not 1 <= n <= MAX_FRAME_BYTES:
        raise CommandFrameError(f"a frame of {n} bytes from the server: frames are 1 to {MAX_FRAME_BYTES}")
    return n


def split_frame(buf: bytes | bytearray) -> tuple[int, bytes, int] | None:
    """The first frame in `buf` as (tag, body, bytes it takes), or None while it is not whole."""
    if len(buf) < FRAME_HEADER_BYTES:
        return None
    n = frame_length(buf)
    if len(buf) < FRAME_HEADER_BYTES + n:
        return None
    return buf[FRAME_HEADER_BYTES], bytes(buf[FRAME_HEADER_BYTES + 1 : FRAME_HEADER_BYTES + n]), FRAME_HEADER_BYTES + n


def parse_words(tag: int, body: bytes, count: int) -> list[int]:
    """The `count` unsigned 4-byte words of a frame of `tag`."""
    if len(body) != 4 * count:
        raise CommandFrameError(f"a frame with tag {tag} and a {len(body)}-byte body, which takes {4 * count}")
    return [int.from_bytes(body[i : i + 4], "little") for i in range(0, len(body), 4)]


def parse_start(tag: int, body: bytes) -> tuple[int, int]:
    """The lane and the attempt of a start frame."""
    if tag != START:
        raise CommandFrameError(f"a frame with tag {tag} where a start request (tag {START}) comes")
    lane, attempt = parse_words(tag, body, 2)
    return lane, attempt


def recv_commands(sock: socket.socket, n: int = 4096) -> bytes | None:
    """The next bytes of the server's commands, at most `n`, or None when the server's end is closed.

    A closed end reads as EOF, or on Linux as ECONNRESET: an AF_UNIX stream reports the
    peer's close as a reset when frames this template wrote were still unread on the server's
    side (a server that stops kills its workers and exits without reading the reports of
    their deaths). Both say the server is gone; nothing else resets a socket pair.
    """
    try:
        chunk = sock.recv(n)
    except ConnectionResetError:
        return None
    return chunk or None


def send_report(sock: socket.socket, report: bytes) -> bool:
    """Write `report` to the server; False when the server's end is closed (EPIPE): the server
    is gone, as when `recv_commands` gives None: a stopping server exits while the deaths of
    the workers it killed are being reported."""
    try:
        sock.sendall(report)
    except BrokenPipeError:
        return False
    return True


def parse_run(body: bytes) -> str:
    """The command line of a run frame, as `CommandLine.encode()` wrote it."""
    try:
        return body.decode("utf-8")
    except UnicodeDecodeError as e:
        raise CommandFrameError(f"a run frame whose command line is not UTF-8: {body[:64]!r}") from e


def _read_exact(sock: socket.socket, n: int) -> bytes | None:
    """`n` bytes from `sock`, or None when the server closed it first."""
    buf = b""
    while len(buf) < n:
        more = recv_commands(sock, n - len(buf))
        if more is None:
            return None
        buf += more
    return buf


def _recv_setup(sock: socket.socket) -> LaneSetup | None:
    """The server's setup frame, or None when the server closed the socket first."""
    command = _recv_command(sock)
    if command is None:
        return None
    tag, body, fds, _cut = command
    _refuse_fds(tag, fds)
    if tag != SETUP:
        raise CommandFrameError(f"a frame with tag {tag} where the setup (tag {SETUP}) comes")
    return LaneSetup.decode(body)


# Received fds are close-on-exec on Linux, which has the flag; macOS has none. A worker gets
# them either way: a fork keeps every fd, and the spawn driver hands a new process the ones it
# passes.
if sys.platform == "linux":
    _RECV_FDS_FLAGS = socket.MSG_CMSG_CLOEXEC
else:
    _RECV_FDS_FLAGS = 0


def _recv_command(sock: socket.socket) -> tuple[int, bytes, list[int], bool] | None:
    """The server's next command frame: its tag, its body, the fds that came on its first byte,
    and whether the kernel cut them short; None when the server closed the socket first. Read to
    the byte: a read into the next frame would drop the fds on its first byte."""
    try:
        data, fds, flags, _addr = socket.recv_fds(sock, FRAME_HEADER_BYTES, LANE_FDS, _RECV_FDS_FLAGS)
    except ConnectionResetError:
        return None  # the server's end is closed, as `recv_commands` reads it
    head = data and data + (_read_exact(sock, FRAME_HEADER_BYTES - len(data)) or b"")
    if len(head) < FRAME_HEADER_BYTES:
        _close(fds)
        return None
    try:
        n = frame_length(head)
    except CommandFrameError:
        _close(fds)
        raise
    rest = _read_exact(sock, n)
    if rest is None:
        _close(fds)
        return None
    return rest[0], rest[1:], fds, bool(flags & socket.MSG_CTRUNC)


def _refuse_fds(tag: int, fds: list[int]) -> None:
    """Close `fds`, which came on a frame with `tag`, and refuse them: only a lane's frame carries fds."""
    _close(fds)
    if fds:
        raise CommandFrameError(f"{len(fds)} fds on a frame with tag {tag}: only a lane's frame carries fds")


def _close(fds) -> None:
    for fd in fds:
        os.close(fd)


def spawn_import_worker(one: LaneWorker, run_dir: str) -> int:
    """Spawns a `start="import"` worker and hands it `one` over a socketpair it inherits
    (`--setup-fd N`), written after the spawn so a setup larger than the socket's buffer never
    holds up the template; returns its pid.

    A worker that dies before it read its setup is told by its exit status, which the template
    reports as for any worker: the setup it was not there to read is not an error of its own.
    """
    ours, theirs = socket.socketpair()
    with ours:
        with theirs:
            pid = _driver_start_import_worker(
                sys.executable,
                "mipyre._process_lane.worker",
                ["--setup-fd", str(theirs.fileno())],
                [*one.fds, theirs.fileno()],
                pin_env(os.environ, run_dir),
            )
        try:
            ours.sendall(one.encode().encode())
        except (BrokenPipeError, ConnectionResetError):
            pass  # the worker already exited; its status says why (see above)
    return pid


def main(sock: socket.socket, launcher_build: str | None) -> None:
    """Serve the server's worker requests on `sock` until it goes away.

    The setup's run directory holds the launcher's copy of `mipyre`, in build `launcher_build`:
    the template imports from it from then on, and each spawned worker is pinned to it.

    One loop is the socket's only writer: it answers start requests and, woken through the
    signal wakeup fd, reaps workers and reports them. SIGCHLD itself does nothing, so no frame
    is ever written from inside a signal handler, half-way through another.
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # the server stops the template, not a Ctrl-C
    children: dict[int, int] = {}

    def stop(_signum=None, _frame=None, status: int = 0) -> NoReturn:
        # The template's workers go with it: a worker stuck in a handler never looks at
        # its parent, so nobody else would end it.
        for pid in list(children):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        os._exit(status)

    # Before it waits for its setup: the launcher may end it at any point (`Template.end`).
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGHUP, stop)
    setup = _recv_setup(sock)
    if setup is None:
        os._exit(0)  # the app has no process routes, or the server went away
    pin_to_run_dir(setup.run_dir, launcher_build)
    # Each lane's channel for the current run, as its frame brought it: the shared memory and the
    # write end of its host's wake pipe. The template maps none of them; it hands a lane's pair to
    # each worker it starts for the lane, and closes a run's pair when the next run's comes.
    lane_fds: dict[int, tuple[int, int]] = {}
    template_pid = os.getpid()

    wake_read, wake_write = os.pipe()
    os.set_blocking(wake_read, False)
    os.set_blocking(wake_write, False)
    signal.set_wakeup_fd(wake_write, warn_on_full_buffer=False)
    signal.signal(signal.SIGCHLD, lambda _signum, _frame: None)
    selector = selectors.DefaultSelector()
    selector.register(sock, selectors.EVENT_READ)
    selector.register(wake_read, selectors.EVENT_READ)
    # What a forked worker closes: the template's own, never its workers'. A fork inherits the
    # Linux epoll fd; a kqueue (macOS) is not inherited.
    template_fds = (wake_read, wake_write) + ((selector.fileno(),) if sys.platform == "linux" else ())

    def reap() -> None:
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return
            if pid == 0:
                return
            lane = children.pop(pid, None)
            if lane is not None and not send_report(sock, exited_frame(lane, pid, status)):
                stop()

    # The latest run frame's command line; the import of start="fork" happens at the first.
    run_command_line: str | None = None
    forked_app = None

    def take_run(command_line: str) -> None:
        nonlocal run_command_line, forked_app
        run_command_line = command_line
        if setup.start is not StartStrategy.Fork:
            return
        try:
            worker.install_command_line(command_line)
            if forked_app is None:
                forked_app = worker.load_app(setup)
                require_single_threaded(
                    'the process-lane template (start="fork")',
                    'Use start="import" (the default) for this app: its workers import the app '
                    "themselves, so nothing it starts at import is ever forked.",
                )
        except BaseException:  # noqa: BLE001 — reported to the server, which fails the start
            sock.sendall(import_failed_frame(traceback.format_exc()))
            os._exit(70)

    def fds_to_close(lane: int) -> tuple[int, ...]:
        """What a worker of `lane` closes: the template's own fds and the other lanes'."""
        others = tuple(fd for other, pair in lane_fds.items() if other != lane for fd in pair)
        return template_fds + others

    def start_worker(lane: int, attempt: int, command_line: str) -> int:
        one = LaneWorker(setup, lane, template_pid, attempt, lane_fds[lane], command_line, BUILD_ID)
        match setup.start:
            case StartStrategy.Import if _driver_name() == "spawn":
                pid = spawn_import_worker(one, setup.run_dir)
            case StartStrategy.Import:
                pid = _driver_start_import_worker(sys.executable, "mipyre._process_lane.worker", [], [], [])
                if pid == 0:
                    worker.run(one, sock, fds_to_close(lane), lambda lanes: worker.import_app(lanes, one))
            case StartStrategy.Fork:
                pid = _driver_fork_worker()
                if pid == 0:
                    worker.run(one, sock, fds_to_close(lane), lambda _lanes: forked_app)
        return pid

    def take_lane(lane: int, fds: list[int], cut: bool) -> None:
        """Keep `lane`'s channel for this run in place of the last run's; a frame short of its
        fds is reported to the server, and the template ends: it cannot serve that lane."""
        if not 0 <= lane < setup.workers:
            _close(fds)
            raise CommandFrameError(f"a frame for lane {lane}: the server has {setup.workers}")
        if cut or len(fds) != LANE_FDS:
            _close(fds)
            send_report(sock, lane_fds_missing_frame(lane, len(fds)))
            stop(status=70)
        _close(lane_fds.pop(lane, ()))
        lane_fds[lane] = (fds[0], fds[1])

    def start(lane: int, attempt: int) -> None:
        if run_command_line is None:
            raise CommandFrameError(f"a start request for lane {lane} before any run frame")
        if lane not in lane_fds:
            raise CommandFrameError(f"a start request for lane {lane}, whose channel never came")
        sys.stdout.flush()
        sys.stderr.flush()
        try:
            pid = start_worker(lane, attempt, run_command_line)
        except OSError as e:
            print(f"mipyre: the template could not start a worker for lane {lane}: {e}", file=sys.stderr, flush=True)
            if not send_report(sock, fork_failure_frame(lane, attempt, e)):
                stop()
            return
        children[pid] = lane
        if not send_report(sock, forked_frame(lane, attempt, pid)):
            stop()

    while True:
        for key, _events in selector.select():
            if key.fileobj is sock:
                command = _recv_command(sock)
                if command is None:
                    stop()  # the server is gone: so are its workers
                tag, body, fds, cut = command
                if tag == LANE:
                    take_lane(parse_words(tag, body, 1)[0], fds, cut)
                    continue
                _refuse_fds(tag, fds)
                if tag == RUN:
                    take_run(parse_run(body))
                else:
                    start(*parse_start(tag, body))
            else:
                try:
                    while os.read(wake_read, 512):
                        pass
                except BlockingIOError:
                    pass
                reap()


if __name__ == "__main__":
    fd = int(sys.argv[sys.argv.index("--sock") + 1])
    try:
        main(socket.socket(fileno=fd), os.environ.get("_MIPYRE_BUILD_ID"))
    except BaseException:  # noqa: BLE001 — printed; the process then exits 70 (the finally below)
        traceback.print_exc()
    finally:
        os._exit(70)
