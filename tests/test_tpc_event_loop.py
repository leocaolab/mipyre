"""`async def` routes on the TPC thread's own event loop, run against a real server.

Each test starts its own server with the settings it needs (threads, budget, per-thread limit), so
an answer does not depend on which thread the kernel hands a connection to.
"""

import concurrent.futures
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

import pytest

from tests._helpers import answers, bound_port, kill_session, poll_until, read_file

APP = r"""
import asyncio, contextvars, os, socket, time
from mipyre import Mipyre, ServerFlags

MARKS = os.environ["MARKS"]
app = Mipyre()
app.request_timeout = float(os.environ.get("BUDGET", "1"))
if "SHUTDOWN" in os.environ:
    app.shutdown_timeout = float(os.environ["SHUTDOWN"])

def mark(name):
    open(os.path.join(MARKS, name), "w").close()

@app.get("/ping")
def ping(req):
    return "pong"

@app.get("/sleep/{ms}")
async def sleep(req):
    await asyncio.sleep(int(req.params["ms"]) / 1000)
    return "slept"

@app.get("/finishing")
async def finishing(req):
    mark(f"finishing-{time.monotonic_ns()}")
    await asyncio.sleep(0.3)
    return "slept"

@app.get("/slow")
async def slow(req):
    mark(f"slow-{time.monotonic_ns()}")
    try:
        await asyncio.sleep(30)
    except asyncio.CancelledError:
        mark("cancelled")
        raise
    return "never"

@app.get("/past-budget")
async def past_budget(req):
    mark("entered")
    try:
        await asyncio.sleep(1.5)
    except asyncio.CancelledError:
        mark("cancelled")
        raise
    mark("finished")
    return "late"

@app.get("/cpu")
async def cpu(req):
    end = time.monotonic() + 1.5
    while time.monotonic() < end:
        pass
    return "burnt"

@app.get("/hold")
async def hold(req):
    mark("entered")
    while not os.path.exists(os.path.join(MARKS, "gate")):
        await asyncio.sleep(0.005)
    return "held"

@app.post("/hold-body")
async def hold_body(req):
    mark("entered")
    while not os.path.exists(os.path.join(MARKS, "gate")):
        await asyncio.sleep(0.005)
    return str(len(req.body))

@app.post("/echo")
async def echo(req):
    await asyncio.sleep(0)
    return str(len(req.body))

@app.get("/outlive")
async def outlive(req):
    mark("entered")
    await asyncio.sleep(0.5)
    mark("outlived")
    return "late"

request_id = contextvars.ContextVar("request_id", default=None)

@app.get("/features")
async def features(req):
    request_id.set(req.query.removeprefix("id="))
    async def child(n):
        await asyncio.sleep(0.01)
        return (n, request_id.get())
    results = await asyncio.gather(child(1), child(2))
    task = asyncio.create_task(child(3))
    async with asyncio.timeout(1):
        third = await task
    try:
        async with asyncio.timeout(0.01):
            await asyncio.sleep(1)
        timed_out = False
    except TimeoutError:
        timed_out = True
    loop = asyncio.get_running_loop()
    return {"gather": results, "task": third, "timeout": timed_out,
            "loop": type(loop).__name__, "running": loop.is_running()}

@app.get("/connect")
async def connect(req):
    async def answer(reader, writer):
        writer.write((await reader.readline()).upper())
        await writer.drain()
        writer.close()
    server = await asyncio.start_server(answer, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"hello\n")
    await writer.drain()
    line = await reader.readline()
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    return line.decode().strip()

@app.get("/to-thread")
async def to_thread(req):
    def blocking():
        time.sleep(0.05)
        return "from a thread"
    return await asyncio.to_thread(blocking)

@app.get("/reader")
async def reader(req):
    a, b = socket.socketpair()
    a.setblocking(False)
    loop = asyncio.get_running_loop()
    got = loop.create_future()
    loop.add_reader(a.fileno(), lambda: got.set_result(a.recv(100)))
    loop.call_later(0.02, b.send, b"ping")
    data = await got
    loop.remove_reader(a.fileno())
    a.close(); b.close()
    return data

GENERATORS = []

@app.get("/open-generator")
async def open_generator(req):
    async def counting():
        try:
            yield 1
            yield 2
        finally:
            mark("generator-closed")
    generator = counting()
    GENERATORS.append(generator)
    return str(await generator.__anext__())

SPAWNED = set()

async def spawned(name):
    mark(f"{name}-started")
    try:
        await asyncio.sleep(30)
    except asyncio.CancelledError:
        mark(f"{name}-cancelled")
        raise
    finally:
        mark(f"{name}-finally")

@app.get("/spawn")
async def spawn(req):
    SPAWNED.add(asyncio.create_task(spawned("kept")))
    return "spawned"

@app.get("/spawn-unreferenced")
async def spawn_unreferenced(req):
    asyncio.create_task(spawned("unreferenced"))
    return "spawned"

async def stubborn():
    mark("stubborn-started")
    while True:
        try:
            await asyncio.sleep(30)
            return
        except asyncio.CancelledError:
            mark("stubborn-cancelled")

@app.get("/spawn-stubborn")
async def spawn_stubborn(req):
    SPAWNED.add(asyncio.create_task(stubborn()))
    return "spawned"

@app.get("/linger")
async def linger(req):
    mark("linger-started")
    try:
        await asyncio.sleep(30)
    except asyncio.CancelledError:
        mark("linger-cancelled")
        await asyncio.sleep(0.1)
        raise
    return "never"

@app.post("/late")
async def late(req):
    mark("late-started")
    try:
        await asyncio.sleep(30)
    except asyncio.CancelledError:
        mark("late-cancelled")
        raise
    return "never"

@app.get("/def-asyncio-run")
def def_asyncio_run(req):
    async def inner():
        await asyncio.sleep(0)
        return "ran"
    return asyncio.run(inner())

if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0))
"""

SYNC_ONLY_APP = r"""
from mipyre import Mipyre, ServerFlags
app = Mipyre()

@app.get("/ping")
def ping(req):
    return "pong"

if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0))
"""

# Past this many body bytes a request needs a large seat (`ADMISSION_SKIP_BYTES`).
SMALL_BODY_LINE = 64 * 1024


class Server:
    def __init__(self, tmp_path, script=APP, **env):
        self.marks = tmp_path / "marks"
        self.marks.mkdir()
        path = tmp_path / "app.py"
        path.write_text(script)
        self.log = str(path) + ".log"
        full = dict(os.environ, MARKS=str(self.marks), **env)
        full.pop("MIPYRE_TPC", None)
        with open(self.log, "w") as log:
            self.proc = subprocess.Popen(
                [sys.executable, str(path)],
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=full,
            )
        self.base = f"http://127.0.0.1:{bound_port(read_file(self.log), self.proc)}"
        answers(self.base + "/ping", self.proc, read_file(self.log))

    def get(self, path, data=None, timeout=10):
        try:
            with urllib.request.urlopen(
                self.base + path, data=data, timeout=timeout
            ) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def address(self):
        host, port = self.base.removeprefix("http://").split(":")
        return host, int(port)

    def terminate(self):
        os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
        return self.proc.wait(timeout=15)

    def stop(self):
        try:
            if self.proc.poll() is None:
                self.terminate()
        finally:
            kill_session(self.proc)
        return read_file(self.log)()

    def threads(self):
        return sorted(
            open(f"/proc/{self.proc.pid}/task/{t}/comm").read().strip()
            for t in os.listdir(f"/proc/{self.proc.pid}/task")
        )


@pytest.fixture
def server(tmp_path, request):
    env = getattr(request, "param", {})
    s = Server(tmp_path, **env)
    yield s
    s.stop()


def half_sent_post(server, path):
    """Open a connection, send a POST to `path` with half of its 10-byte body, and return the
    connection once the server has had time to take it and seat the request."""
    host, port = server.address()
    conn = socket.create_connection((host, port), timeout=15)
    conn.sendall(
        f"POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Length: 10\r\n"
        "Connection: close\r\n\r\n".encode()
        + b"y" * 5
    )
    time.sleep(0.3)
    return conn


def rest_and_answer(conn):
    """Send the other half of a `half_sent_post` body and return all the server answers."""
    conn.sendall(b"y" * 5)
    answer = b""
    try:
        while chunk := conn.recv(4096):
            answer += chunk
    except OSError:
        pass
    conn.close()
    return answer


def chunked_status(server, path, size):
    """Send a chunked body of `size` bytes, declaring no length, and return the response's status."""
    host, port = server.address()
    with socket.create_connection((host, port), timeout=10) as conn:
        conn.sendall(
            f"POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nTransfer-Encoding: chunked\r\n\r\n".encode()
        )
        try:
            conn.sendall(f"{size:x}\r\n".encode() + b"x" * size + b"\r\n0\r\n\r\n")
        except OSError:
            pass
        return int(conn.recv(64).split(b" ")[1])


@pytest.mark.parametrize("server", [{"MIPYRE_THREADS": "1"}], indirect=True)
def test_sleeping_handlers_overlap_on_one_thread(server):
    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(100) as pool:
        got = list(pool.map(lambda _: server.get("/sleep/300"), range(100)))
    took = time.monotonic() - start
    assert all(g == (200, b"slept") for g in got), got[:3]
    assert took < 1.5, took


@pytest.mark.parametrize("server", [{"MIPYRE_THREADS": "1"}], indirect=True)
def test_a_handler_past_its_budget_gets_504_on_time_and_its_task_runs_on(server):
    pinged = []

    def ping_while_it_awaits():
        poll_until(lambda: (server.marks / "entered").exists() or None, timeout=5)
        pinged.append(server.get("/ping"))

    t = threading.Thread(target=ping_while_it_awaits)
    t.start()
    start = time.monotonic()
    status, _ = server.get("/past-budget")
    took = time.monotonic() - start
    t.join()
    assert status == 504
    assert 0.9 < took < 1.4, took
    assert pinged == [(200, b"pong")]
    poll_until(lambda: (server.marks / "finished").exists() or None, timeout=5)
    assert not (server.marks / "cancelled").exists()


@pytest.mark.parametrize("server", [{"MIPYRE_THREADS": "1"}], indirect=True)
def test_a_cpu_bound_handler_past_its_budget_gets_504_once_it_returns(server):
    start = time.monotonic()
    status, _ = server.get("/cpu")
    assert status == 504
    assert time.monotonic() - start >= 1.5


@pytest.mark.parametrize(
    "server",
    [{"MIPYRE_THREADS": "1", "MIPYRE_ASYNC_QUEUE_CAPACITY": "1"}],
    indirect=True,
)
def test_a_request_past_the_threads_limit_gets_503(server):
    first = {}
    t = threading.Thread(target=lambda: first.update(answer=server.get("/hold")))
    t.start()
    poll_until(lambda: (server.marks / "entered").exists() or None, timeout=5)
    assert server.get("/hold")[0] == 503
    assert server.get("/ping") == (200, b"pong")
    (server.marks / "gate").touch()
    t.join()
    assert first["answer"] == (200, b"held")
    assert server.get("/sleep/1") == (200, b"slept")


@pytest.mark.parametrize(
    "server",
    [{"MIPYRE_THREADS": "1", "MIPYRE_ASYNC_QUEUE_CAPACITY": "16"}],
    indirect=True,
)
def test_a_large_body_past_the_large_seats_gets_503_declared_or_not(server):
    large = b"x" * (SMALL_BODY_LINE + 1)
    first = {}
    t = threading.Thread(
        target=lambda: first.update(answer=server.get("/hold-body", data=large))
    )
    t.start()
    poll_until(lambda: (server.marks / "entered").exists() or None, timeout=5)
    assert server.get("/echo", data=large)[0] == 503
    assert chunked_status(server, "/echo", SMALL_BODY_LINE + 1) == 503
    assert server.get("/echo", data=b"x" * SMALL_BODY_LINE) == (
        200,
        str(SMALL_BODY_LINE).encode(),
    )
    (server.marks / "gate").touch()
    t.join()
    assert first["answer"] == (200, str(len(large)).encode())
    assert server.get("/echo", data=large) == (200, str(len(large)).encode())


@pytest.mark.skipif(sys.platform != "linux", reason="reads thread names from /proc/<pid>/task, which is Linux's")
@pytest.mark.parametrize("server", [{"MIPYRE_THREADS": "2"}], indirect=True)
def test_def_and_async_def_mix_on_the_tpc_threads_alone(server):
    with concurrent.futures.ThreadPoolExecutor(32) as pool:
        got = list(
            pool.map(
                lambda i: server.get("/ping" if i % 2 else "/sleep/20"), range(200)
            )
        )
    assert all(g[0] == 200 for g in got)
    assert server.get("/def-asyncio-run") == (200, b"ran")
    names = server.threads()
    assert not any("async" in n for n in names), names
    assert sum(n.startswith("mipyre-tpc") for n in names) == 2, names


def test_an_app_without_async_def_serves_as_before(tmp_path):
    s = Server(tmp_path, script=SYNC_ONLY_APP, MIPYRE_THREADS="2")
    try:
        assert s.get("/ping") == (200, b"pong")
    finally:
        s.stop()


@pytest.mark.parametrize("server", [{"MIPYRE_THREADS": "1"}], indirect=True)
def test_gather_timeout_create_task_and_contextvars_work(server):
    status, body = server.get("/features?id=r7")
    assert status == 200, body
    assert json.loads(body) == {
        "gather": [[1, "r7"], [2, "r7"]],
        "task": [3, "r7"],
        "timeout": True,
        "loop": "TpcEventLoop",
        "running": True,
    }


@pytest.mark.parametrize("server", [{"MIPYRE_THREADS": "1"}], indirect=True)
def test_a_handler_serves_and_connects_to_a_socket_and_runs_a_thread(server):
    assert server.get("/connect") == (200, b"HELLO")
    assert server.get("/to-thread") == (200, b"from a thread")


@pytest.mark.parametrize("server", [{"MIPYRE_THREADS": "1"}], indirect=True)
def test_add_reader_calls_back_when_the_fd_is_readable(server):
    assert server.get("/reader") == (200, b"ping")


@pytest.mark.parametrize("server", [{"MIPYRE_THREADS": "1"}], indirect=True)
def test_a_client_that_goes_away_does_not_cancel_the_task(server):
    host, port = server.address()
    c = socket.create_connection((host, port))
    c.sendall(f"GET /outlive HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n".encode())
    poll_until(lambda: (server.marks / "entered").exists() or None, timeout=5)
    c.close()
    poll_until(lambda: (server.marks / "outlived").exists() or None, timeout=5)
    assert not (server.marks / "cancelled").exists()


@pytest.mark.parametrize(
    "server", [{"MIPYRE_THREADS": "1", "BUDGET": "5"}], indirect=True
)
def test_the_stop_lets_requests_finish_cancels_the_rest_and_closes_open_async_generators(
    server,
):
    assert server.get("/open-generator") == (200, b"1")
    with concurrent.futures.ThreadPoolExecutor(20) as pool:
        finishing = [pool.submit(server.get, "/finishing", None, 10) for _ in range(10)]
        hanging = [pool.submit(server.get, "/slow", None, 10) for _ in range(10)]
        poll_until(lambda: len(list(server.marks.iterdir())) == 20 or None, timeout=5)
        code = server.terminate()
        finished = [f.result() for f in finishing]
        for f in hanging:
            try:
                f.result()
            except OSError:
                pass
    log = read_file(server.log)()
    assert code == 0, log[-2000:]
    assert finished == [(200, b"slept")] * 10
    assert (server.marks / "cancelled").exists(), log[-2000:]
    assert (server.marks / "generator-closed").exists(), log[-2000:]
    assert "panicked" not in log, log[-2000:]


@pytest.mark.parametrize(
    "server", [{"MIPYRE_THREADS": "1", "BUDGET": "5", "SHUTDOWN": "3"}], indirect=True
)
def test_the_stop_cancels_the_tasks_handlers_spawned_and_runs_their_cleanup(server):
    assert server.get("/spawn") == (200, b"spawned")
    assert server.get("/spawn-unreferenced") == (200, b"spawned")
    started = {"kept-started", "unreferenced-started"}
    poll_until(
        lambda: started <= {p.name for p in server.marks.iterdir()} or None, timeout=5
    )
    code = server.terminate()
    log = read_file(server.log)()
    marks = {p.name for p in server.marks.iterdir()}
    assert code == 0, log[-2000:]
    for name in ("kept", "unreferenced"):
        assert {f"{name}-cancelled", f"{name}-finally"} <= marks, (marks, log[-2000:])


@pytest.mark.parametrize(
    "server", [{"MIPYRE_THREADS": "1", "BUDGET": "10", "SHUTDOWN": "5"}], indirect=True
)
def test_a_request_still_sending_its_body_when_the_stop_begins_is_answered(server):
    host, port = server.address()
    with socket.create_connection((host, port), timeout=10) as conn:
        conn.sendall(
            f"POST /echo HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Length: 10\r\n"
            "Connection: close\r\n\r\n".encode()
            + b"y" * 5
        )
        time.sleep(0.2)
        os.killpg(os.getpgid(server.proc.pid), signal.SIGTERM)
        # The thread has no live Task while the body is unread: its loop must stay open all the same.
        time.sleep(0.5)
        conn.sendall(b"y" * 5)
        answer = b""
        while chunk := conn.recv(4096):
            answer += chunk
    code = server.proc.wait(timeout=15)
    log = read_file(server.log)()
    assert answer.startswith(b"HTTP/1.1 200"), (answer, log[-2000:])
    assert answer.endswith(b"\r\n\r\n10"), answer
    assert code == 0, log[-2000:]


@pytest.mark.parametrize(
    "server", [{"MIPYRE_THREADS": "1", "BUDGET": "20", "SHUTDOWN": "5"}], indirect=True
)
def test_a_request_whose_body_completes_after_the_stops_cancel_gets_503_and_the_finalizers_run(server):
    assert server.get("/spawn") == (200, b"spawned")
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        lingering = pool.submit(server.get, "/linger", None, 15)
        poll_until(
            lambda: {"kept-started", "linger-started"} <= {p.name for p in server.marks.iterdir()} or None,
            timeout=5,
        )
        conn = half_sent_post(server, "/late")
        os.killpg(os.getpgid(server.proc.pid), signal.SIGTERM)
        poll_until(lambda: (server.marks / "linger-cancelled").exists() or None, timeout=10)
        answer = rest_and_answer(conn)
        code = server.proc.wait(timeout=15)
        try:
            lingering.result()
        except OSError:
            pass
    log = read_file(server.log)()
    marks = {p.name for p in server.marks.iterdir()}
    assert answer.startswith(b"HTTP/1.1 503"), (answer, log[-2000:])
    assert b"server event loop closed" in answer, answer
    assert "late-started" not in marks, (marks, log[-2000:])
    assert {"kept-cancelled", "kept-finally"} <= marks, (marks, log[-2000:])
    assert code == 0, log[-2000:]


@pytest.mark.parametrize(
    "server", [{"MIPYRE_THREADS": "1", "BUDGET": "20", "SHUTDOWN": "5"}], indirect=True
)
def test_a_request_whose_body_completes_while_the_finalizers_run_gets_503(server):
    assert server.get("/spawn-stubborn") == (200, b"spawned")
    poll_until(lambda: (server.marks / "stubborn-started").exists() or None, timeout=5)
    conn = half_sent_post(server, "/late")
    os.killpg(os.getpgid(server.proc.pid), signal.SIGTERM)
    # No request Task is live, so the finalizers start at the cancel time; the stubborn Task holds
    # them until the stop's deadline.
    poll_until(lambda: (server.marks / "stubborn-cancelled").exists() or None, timeout=10)
    answer = rest_and_answer(conn)
    code = server.proc.wait(timeout=15)
    log = read_file(server.log)()
    marks = {p.name for p in server.marks.iterdir()}
    assert answer.startswith(b"HTTP/1.1 503"), (answer, log[-2000:])
    assert "late-started" not in marks, (marks, log[-2000:])
    assert code == 0, log[-2000:]


@pytest.mark.parametrize(
    "server", [{"MIPYRE_THREADS": "1", "BUDGET": "5", "SHUTDOWN": "4"}], indirect=True
)
def test_a_spawned_task_that_ignores_its_cancel_holds_the_finalizers_to_the_deadline_and_it_is_logged(server):
    assert server.get("/open-generator") == (200, b"1")
    assert server.get("/spawn-stubborn") == (200, b"spawned")
    poll_until(lambda: (server.marks / "stubborn-started").exists() or None, timeout=5)
    started = time.monotonic()
    code = server.terminate()
    took = time.monotonic() - started
    log = read_file(server.log)()
    assert code == 0, log[-2000:]
    assert took < 4.5, f"the stop overran its 4 s shutdown_timeout: {took:.2f}s"
    assert (server.marks / "stubborn-cancelled").exists(), log[-2000:]
    cut_off = [line for line in log.splitlines() if "before its finalizers ended" in line]
    assert len(cut_off) == 1, log[-2000:]
    assert '"level":"WARN"' in cut_off[0], cut_off[0]

