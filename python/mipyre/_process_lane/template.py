"""The process-lane template (design §4.1, §11.4): started by the launcher before the app
is imported; on the server's request it starts one worker per lane, and reaps them.

The launcher below is the one place that matches on `StartStrategy`:

- Import: the template holds no app code; each worker gets the adapters and the app itself
  (driver: a fork of this bare template, or a new process on macOS while b/32 lasts);
- Fork: the template runs the adapters, imports the app, checks it is still
  single-threaded, then forks each worker.

`python -m mipyre._process_lane.template --sock FD` is the entry of a template the
SpawnDriver started, from the launcher's pinned copy of `mipyre` (the first PYTHONPATH entry);
the ForkDriver's template comes in through `main` in the forked launcher, whose `mipyre`
packages already import from that copy.
"""

from __future__ import annotations

import os
import selectors
import signal
import socket
import sys
import traceback

from mipyre.engine import (
    LaneSet,
    StartStrategy,
    _driver_fork_worker,
    _driver_start_import_worker,
)

from mipyre._process_lane import LaneSetup, LaneWorker, adapters, pin_env, refuse_unpinned_start, unpin_env, worker
from mipyre._process_lane.threads import require_single_threaded


class ForkFailureWithoutErrno(RuntimeError):
    """The driver's OSError for a failed start carried no errno: the template has no true
    answer for the server (its answer is the errno), so it ends instead of inventing one."""


class CommandFrameError(RuntimeError):
    """The server sent bytes that are no command frame the template takes: the template ends."""


# The template socket's frames (crates/pyext/src/lane/template.rs is the server's side): a 4-byte
# little-endian length, then that many bytes, the first of which is the tag. Both directions
# number their tags in one space.
FRAME_HEADER_BYTES = 4
MAX_FRAME_BYTES = 64 * 1024
SETUP, LANE, START, FORKED, EXITED, IMPORT_FAILED, LANE_FDS_MISSING = range(1, 8)
# A lane's frame carries the lane's shared memory and its wake-pipe write end.
LANE_FDS = 2
LANE_FRAME_BYTES = FRAME_HEADER_BYTES + 1 + 4


def frame(tag: int, body: bytes) -> bytes:
    """The frame of `tag` with `body`."""
    return (1 + len(body)).to_bytes(4, "little") + bytes([tag]) + body


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


def split_frame(buf: bytes | bytearray) -> tuple[int, bytes, int] | None:
    """The first frame in `buf` as (tag, body, bytes it takes), or None while it is not whole.
    A length past MAX_FRAME_BYTES, or one with no tag, is refused from the header alone."""
    if len(buf) < FRAME_HEADER_BYTES:
        return None
    n = int.from_bytes(buf[:FRAME_HEADER_BYTES], "little")
    if not 1 <= n <= MAX_FRAME_BYTES:
        raise CommandFrameError(f"a frame of {n} bytes from the server: frames are 1 to {MAX_FRAME_BYTES}")
    if len(buf) < FRAME_HEADER_BYTES + n:
        return None
    return buf[FRAME_HEADER_BYTES], bytes(buf[FRAME_HEADER_BYTES + 1 : FRAME_HEADER_BYTES + n]), FRAME_HEADER_BYTES + n


def whole_frame(buf: bytes) -> tuple[int, bytes]:
    """The tag and the body of `buf`, which is one whole frame."""
    whole = split_frame(buf)
    if whole is None or whole[2] != len(buf):
        raise CommandFrameError(f"{len(buf)} bytes from the server that are not one frame: {buf[:64].hex()}")
    return whole[0], whole[1]


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


def _read_exact(sock: socket.socket, n: int) -> bytes | None:
    """`n` bytes from `sock`, or None when the server closed it first."""
    buf = b""
    while len(buf) < n:
        more = sock.recv(n - len(buf))
        if not more:
            return None
        buf += more
    return buf


def _recv_setup(sock: socket.socket) -> LaneSetup | None:
    """The server's setup frame, or None when the server closed the socket first. Read to the
    byte: each lane's frame follows with its fds, which a read past the setup would drop."""
    head = _read_exact(sock, FRAME_HEADER_BYTES)
    if head is None:
        return None
    rest = _read_exact(sock, int.from_bytes(head, "little"))
    if rest is None:
        return None
    tag, body = whole_frame(head + rest)
    if tag != SETUP:
        raise CommandFrameError(f"a frame with tag {tag} where the setup (tag {SETUP}) comes")
    return LaneSetup.decode(body)


# Received fds are close-on-exec until a worker is handed them (Linux has the flag; elsewhere the
# spawn driver clears the flag on the fds it passes, and a fork keeps them either way).
if sys.platform == "linux":
    _RECV_FDS_FLAGS = socket.MSG_CMSG_CLOEXEC
else:
    _RECV_FDS_FLAGS = 0


def _recv_lane(sock: socket.socket, lane: int) -> tuple[list[int], bool] | None:
    """Lane `lane`'s frame: the fds that came on it, and whether the kernel cut them short;
    None when the server closed the socket first."""
    data, fds, flags, _addr = socket.recv_fds(sock, LANE_FRAME_BYTES, LANE_FDS, _RECV_FDS_FLAGS)
    if not data:
        return None
    rest = _read_exact(sock, LANE_FRAME_BYTES - len(data))
    if rest is None:
        return None
    tag, body = whole_frame(data + rest)
    if tag != LANE or parse_words(tag, body, 1) != [lane]:
        raise CommandFrameError(f"a frame with tag {tag} and body {body.hex()} where lane {lane}'s (tag {LANE}) comes")
    return fds, bool(flags & socket.MSG_CTRUNC)


def _recv_lanes(sock: socket.socket, workers: int) -> list[int] | None:
    """The fds the lanes are mapped with: the shared memory, then each lane's wake-pipe write end.
    Each lane's frame carries the shared memory again; one copy is kept. A frame short of its
    fds is reported to the server, and the template ends: it cannot serve that lane."""
    shm = None
    wakes = []
    for lane in range(workers):
        received = _recv_lane(sock, lane)
        if received is None:
            return None
        fds, cut = received
        if cut or len(fds) != LANE_FDS:
            sock.sendall(lane_fds_missing_frame(lane, len(fds)))
            os._exit(70)
        if shm is None:
            shm = fds[0]
        else:
            os.close(fds[0])
        wakes.append(fds[1])
    if shm is None:
        raise CommandFrameError("a setup for no lanes: the server makes at least one")
    return [shm, *wakes]


def main(sock: socket.socket, run_dir: str) -> None:
    """Serve the server's worker requests on `sock` until it goes away.

    `run_dir` holds the launcher's pinned copy of `mipyre`: each spawned worker is pinned to it
    again, because a spawned template took it off its own PYTHONPATH.

    One loop is the socket's only writer: it answers start requests and, woken through the
    signal wakeup fd, reaps workers and reports them. SIGCHLD itself does nothing, so no frame
    is ever written from inside a signal handler, half-way through another.
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # the server stops the template, not a Ctrl-C
    setup = _recv_setup(sock)
    if setup is None:
        os._exit(0)  # the app has no process routes, or the server went away
    fds = _recv_lanes(sock, setup.workers)
    if fds is None:
        os._exit(0)  # the server went away
    lanes = LaneSet._attach(setup.workers, setup.depth, setup.slot_bytes, setup.worker_spin_us, fds)
    template_pid = os.getpid()
    children: dict[int, int] = {}

    def stop(_signum=None, _frame=None):
        # The template's workers go with it: a worker stuck in a handler never looks at
        # its parent, so nobody else would end it.
        for pid in list(children):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        os._exit(0)

    wake_read, wake_write = os.pipe()
    os.set_blocking(wake_read, False)
    os.set_blocking(wake_write, False)
    signal.set_wakeup_fd(wake_write, warn_on_full_buffer=False)
    signal.signal(signal.SIGCHLD, lambda _signum, _frame: None)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGHUP, stop)
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
            if lane is not None:
                sock.sendall(exited_frame(lane, pid, status))

    def lane_worker(lane: int) -> LaneWorker:
        return LaneWorker(setup, lane, template_pid, tuple(fds))

    match setup.start:
        case StartStrategy.Import:

            def start_worker(lane: int) -> int:
                one = lane_worker(lane)
                env = [("MIPYRE_LANE_WORKER", one.encode())] + pin_env(os.environ, run_dir)
                pid = _driver_start_import_worker(sys.executable, "mipyre._process_lane.worker", [], fds, env)
                if pid == 0:
                    worker.run(lanes, one, sock, template_fds, lambda: worker.import_app(lanes, one))
                return pid

        case StartStrategy.Fork:
            try:
                adapters.prepare_all(os.environ)
                app = worker.load_app(setup)
                require_single_threaded(
                    'the process-lane template (start="fork")',
                    'Use start="import" (the default) for this app: its workers import the app '
                    "themselves, so nothing it starts at import is ever forked.",
                )
            except BaseException:  # noqa: BLE001 — reported to the server, which fails the start
                sock.sendall(import_failed_frame(traceback.format_exc()))
                os._exit(70)

            def start_worker(lane: int) -> int:
                pid = _driver_fork_worker()
                if pid == 0:
                    worker.run(lanes, lane_worker(lane), sock, template_fds, lambda: app)
                return pid

    def start(lane: int, attempt: int) -> None:
        sys.stdout.flush()
        sys.stderr.flush()
        try:
            pid = start_worker(lane)
        except OSError as e:
            print(f"mipyre: the template could not start a worker for lane {lane}: {e}", file=sys.stderr, flush=True)
            sock.sendall(fork_failure_frame(lane, attempt, e))
            return
        children[pid] = lane
        sock.sendall(forked_frame(lane, attempt, pid))

    received = bytearray()
    while True:
        for key, _events in selector.select():
            if key.fileobj is sock:
                chunk = sock.recv(4096)
                if not chunk:
                    stop()  # the server is gone: so are its workers
                received += chunk
                while (whole := split_frame(received)) is not None:
                    tag, body, used = whole
                    del received[:used]
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
    run_dir = os.environ.get("PYTHONPATH", "").split(os.pathsep)[0]
    refuse_unpinned_start(run_dir, os.environ.get("MIPYRE_BUILD_ID"))
    unpin_env(os.environ, run_dir)
    try:
        main(socket.socket(fileno=fd), run_dir)
    except BaseException:  # noqa: BLE001 — printed; the process then exits 70 (the finally below)
        traceback.print_exc()
    finally:
        os._exit(70)
