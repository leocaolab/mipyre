"""TestClient runs the production path.

- TestClient served on the main interpreter (`mode="default"`) while production
  (`Mipyre.run()` with no mode) serves through sub-interpreter workers.
- Its port-retry loop re-ran all of `run()`: `/mcp` was registered again (the router
  rejects the duplicate, masking the real EADDRINUSE) and startup hooks ran again.
- `close()` did nothing, so every client leaked a running server.

The app is module level on purpose: sub-interpreter workers rebuild it by executing this
file, as they execute a production app's script.
"""

from __future__ import annotations

import errno
import os
import socket
import stat
import subprocess
import sys
import threading

import pytest

import mipyre.engine
from mipyre import Mipyre
from mipyre.engine import ServerFlags
from mipyre.mcp import MCPServer
from mipyre.testing import TestClient, TestServer

from tests._helpers import poll_until

HOST = "127.0.0.1"

# Its runtime is MIPYRE_TEST_MODE's: the default in this process, "main" where a test runs it so.
app = Mipyre(runtime=os.environ.get("MIPYRE_TEST_MODE"))

# The main interpreter's record of hook runs, in order.
LIFECYCLE: list[str] = []


@app.on_startup
def _started():
    LIFECYCLE.append("startup")


@app.on_shutdown
def _stopped():
    LIFECYCLE.append("shutdown")


mcp = MCPServer(app)

@mcp.tool()
def echo(text: str) -> str:
    return text


@app.get("/where")
def where(req):
    return {"in_worker": mipyre.engine._in_worker()}


#: Every start builds its workers by executing this file; two keep the tests quick. A test server
#: reads no environment, so the size is its config.
TWO_WORKERS = ServerFlags(threads=2)


def _os_threads_down_to(limit: int) -> int:
    """The OS thread count, once it is at most `limit`.

    close() joins every thread it started, but a joined thread's /proc entry can outlast the
    join for a moment. This waits for the count, for no length of time: how long an
    exited thread stays listed is the kernel's, not a fact of the test. A thread left running
    never goes, and the test then fails at the suite's timeout, stopped here.
    """
    return poll_until(lambda: [n] if (n := _os_thread_count()) <= limit else None, timeout=None)[0]


def _os_thread_count() -> int:
    if sys.platform.startswith("linux"):
        return len(os.listdir("/proc/self/task"))
    # macOS: one line per thread after the header.
    out = subprocess.run(
        ["ps", "-M", "-p", str(os.getpid())], capture_output=True, text=True, check=True
    ).stdout
    return len(out.strip().splitlines()) - 1


def _ports_this_process_listens_on() -> set[int]:
    """The TCP ports a socket of this process listens on, from the kernel's own tables: a server
    left behind is a socket of this process, and another process binding the same port number
    since (an xdist worker) is not."""
    if sys.platform.startswith("linux"):
        sockets = set()
        for fd in os.listdir("/proc/self/fd"):
            try:
                target = os.readlink(f"/proc/self/fd/{fd}")
            except FileNotFoundError:  # closed since the listing (the listing's own descriptor)
                continue
            if target.startswith("socket:["):
                sockets.add(target[len("socket:["):-1])
        ports = set()
        for table in ("/proc/net/tcp", "/proc/net/tcp6"):
            with open(table) as rows:
                next(rows)  # the header
                for row in rows:
                    # local_address (HEX_IP:HEX_PORT), st (0A = LISTEN), inode: fields 1, 3, 9.
                    fields = row.split()
                    if fields[3] == "0A" and fields[9] in sockets:
                        ports.add(int(fields[1].rsplit(":", 1)[1], 16))
        return ports
    # macOS: lsof lists this process's listening TCP sockets, one `n<address>:<port>` line each;
    # it exits 1 when there is none.
    out = subprocess.run(
        ["lsof", "-nP", "-a", "-p", str(os.getpid()), "-iTCP", "-sTCP:LISTEN", "-Fn"],
        capture_output=True, text=True, check=False,
    )
    assert out.returncode in (0, 1), out.stdout + out.stderr
    return {int(line.rsplit(":", 1)[1]) for line in out.stdout.splitlines() if line.startswith("n")}


def _sockets_of_this_process() -> list[tuple[socket.AddressFamily, socket.SocketKind, object]]:
    """Each socket descriptor of this process as (family, type, local address), read with
    getsockname on a duplicate of it.

    The kernel answers for the socket in whatever state it is in. /proc/net/tcp would not do:
    it lists listening and connected sockets, and a socket that is bound but never listened on
    is in neither table, though it still holds the port number against a new bind.
    """
    found = []
    for name in os.listdir("/dev/fd"):
        fd = int(name)
        try:
            if not stat.S_ISSOCK(os.fstat(fd).st_mode):
                continue
            dup = os.dup(fd)
        except OSError as e:
            if e.errno != errno.EBADF:  # closed since the listing (the listing's own descriptor)
                raise
            continue
        with socket.socket(fileno=dup) as s:
            found.append((s.family, s.type, s.getsockname()))
    return found


def _tcp_ports_bound(sockets) -> set[int]:
    """The TCP port numbers the given sockets (family, type, local address) are bound to.

    A TCP socket bound to a port holds it whether it listens, is connected, or neither; one not
    bound yet reads port 0. A UDP or Unix socket holds no TCP port."""
    return {
        address[1]
        for family, kind, address in sockets
        if family in (socket.AF_INET, socket.AF_INET6)
        and kind == socket.SOCK_STREAM
        and address[1] != 0
    }


def _tcp_ports_this_process_binds() -> set[int]:
    return _tcp_ports_bound(_sockets_of_this_process())


def _serve_once() -> int:
    with TestServer(app, config=TWO_WORKERS) as server, TestClient(server) as c:
        assert c.get("/where").status_code == 200
        return c.port


# ---------------------------------------------------------------------------
# 1. The default is the production mode
# ---------------------------------------------------------------------------


def test_default_serves_through_a_sub_interpreter():
    with TestClient(app, config=TWO_WORKERS) as c:
        assert c.get("/where").json() == {"in_worker": True}


def test_mode_gil_serves_on_the_main_interpreter():
    # The same module-level app, on the main interpreter: the app passes MIPYRE_TEST_MODE as its
    # runtime when it is built, so it is built in a process run with it set to main.
    script = (
        "from tests.test_testclient_production_path import app\n"
        "from mipyre.testing import TestClient\n"
        "with TestClient(app) as c:\n"
        "    print('WHERE', c.get('/where').json())\n"
    )
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = dict(os.environ, MIPYRE_TEST_MODE="main")
    r = subprocess.run([sys.executable, "-c", script], cwd=root, env=env,
                       capture_output=True, text=True, timeout=120, check=False)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "WHERE {'in_worker': False}" in r.stdout, r.stdout + r.stderr


def test_an_app_built_in_a_function_names_why_workers_cannot_serve_it(tmp_path, monkeypatch):
    # The workers rebuild an app with no module of its own by running `__main__.__file__`.
    # Named here, not inherited: under `python -m pytest` that is pytest's `__main__.py`, but
    # an xdist worker's `__main__` (execnet's) has no `__file__` at all. Either way
    # the script defines no app, which is the situation this test is about.
    script = tmp_path / "main_without_the_app.py"
    script.write_text("")
    monkeypatch.setattr(sys.modules["__main__"], "__file__", str(script), raising=False)

    def make():
        inner = Mipyre()

        @inner.get("/")
        def index(req):
            return "hi"

        return inner

    with pytest.raises(RuntimeError) as err:
        TestClient(make(), config=TWO_WORKERS)
    text = "\n".join([str(err.value), *getattr(err.value, "__notes__", ())])
    # The engine's own error, plus what to do about it (as a note on it).
    assert "registered no routes in the worker" in text, text
    assert "created inside a function" in text, text
    assert "Mipyre(runtime='main')" in text, text



def test_an_app_under_python_c_names_why_there_is_no_script_for_the_workers(tmp_path):
    # `python -c` leaves `__main__.__file__` unset, so the workers have no script to rebuild
    # the app from. The error says so with what `__main__` is, not a bare
    # AttributeError.
    script = (
        "import mipyre.engine\n"
        "from mipyre import Mipyre\n"
        "from mipyre.testing import TestClient\n"
        "def make():\n"
        "    inner = Mipyre()\n"
        "    @inner.get('/')\n"
        "    def index(req):\n"
        "        return 'hi'\n"
        "    return inner\n"
        "try:\n"
        "    TestClient(make())\n"
        "except mipyre.engine.MissingAppFileError as err:\n"
        "    print('RAISED', type(err).__name__, isinstance(err, mipyre.DeclarationError))\n"
        "    print(err)\n"
    )
    r = subprocess.run([sys.executable, "-c", script], cwd=tmp_path,
                       capture_output=True, text=True, timeout=120, check=False)
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    assert "RAISED MissingAppFileError True" in r.stdout, out
    assert "rebuild the app by executing its script" in r.stdout, out
    assert "__main__ is <module '__main__'" in r.stdout, out
    assert "which has no __file__" in r.stdout, out
    assert "sys.argv[0] is '-c'" in r.stdout, out
    assert "Mipyre(runtime='main')" in r.stdout, out
    assert "AttributeError" not in out, out


# ---------------------------------------------------------------------------
# 2. A retried start repeats only bind/serve
# ---------------------------------------------------------------------------


def test_a_failed_start_then_a_start_prepares_once():
    # A bind that fails is an OSError from the start (a start binds before serving); the
    # next start on the same app does not prepare it again, and each start runs the
    # startup and shutdown hooks once.
    taken = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    taken.bind((HOST, 0))
    taken.listen()
    busy = taken.getsockname()[1]
    start = len(LIFECYCLE)
    try:
        with pytest.raises(OSError):
            with TestServer(app, port=busy, config=TWO_WORKERS):
                pass
    finally:
        taken.close()
    assert LIFECYCLE[start:] == ["startup", "shutdown"]

    with TestServer(app, config=TWO_WORKERS) as server, TestClient(server) as c:
        assert c.port != busy
        tools = c.post(
            "/mcp", body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
        ).json()
        assert [t["name"] for t in tools["result"]["tools"]] == ["echo"]
    assert LIFECYCLE[start:] == ["startup", "shutdown", "startup", "shutdown"]


def test_each_start_runs_startup_and_shutdown_once():
    start = len(LIFECYCLE)
    _serve_once()
    _serve_once()
    assert LIFECYCLE[start:] == ["startup", "shutdown", "startup", "shutdown"]


# ---------------------------------------------------------------------------
# 3. close() stops the server
# ---------------------------------------------------------------------------


def test_close_frees_the_port():
    """Once closed, no socket of this process listens on the client's port, and none is bound to
    it in any other state either: a socket bound and never listened on, or a connection the
    server accepted, would be left behind as well.

    Both are facts about this process's own sockets. A refused connect there, or
    binding the same port number again, was another process's to break, once an xdist worker
    bound that number."""
    port = _serve_once()

    assert port not in _ports_this_process_listens_on()
    assert port not in _tcp_ports_this_process_binds()


def test_close_is_idempotent():
    c = TestClient(app, config=TWO_WORKERS)
    c.close()
    c.close()


def test_many_clients_leak_no_threads_or_servers():
    """Counts the process's OS threads and its listening sockets after five clients: a thread or
    a server left behind is a fact of the process and the kernel, not of any value a function
    returns. Then ten more clients bring the thread count back to where it was as well (a
    thread left per client would keep it 5, then 15, above), and leave none of their ports
    listened on either. The ports are this process's sockets: a connection refused on a
    closed client's port was another process's to break, once an xdist worker bound the same
    number."""
    _serve_once()  # first-start one-offs (logger, signal driver) settle here
    py_threads = threading.active_count()
    os_threads = _os_thread_count()

    ports = [_serve_once() for _ in range(5)]

    assert threading.active_count() == py_threads
    # A thread from before the baseline may finish meanwhile; none may be added.
    assert _os_threads_down_to(os_threads) <= os_threads
    assert _ports_this_process_listens_on() & set(ports) == set()

    ports += [_serve_once() for _ in range(10)]
    assert threading.active_count() == py_threads
    assert _os_threads_down_to(os_threads) <= os_threads
    assert _ports_this_process_listens_on() & set(ports) == set()


def test_the_listening_ports_see_a_listener_left_open():
    """This test's own server socket: listed while it listens, gone once it is closed (the
    judgment of test_close_frees_the_port and test_many_clients_leak_no_threads_or_servers)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, 0))
        s.listen()
        port = s.getsockname()[1]
        assert port in _ports_this_process_listens_on()
    assert port not in _ports_this_process_listens_on()


def test_the_bound_ports_see_a_socket_bound_but_not_listening():
    """This test's own socket: bound and never listened on, it holds the port (the judgment of
    test_close_frees_the_port); listening, still; closed, no longer."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, 0))
        port = s.getsockname()[1]
        assert port in _tcp_ports_this_process_binds()
        assert port not in _ports_this_process_listens_on()
        s.listen()
        assert port in _tcp_ports_this_process_binds()
    assert port not in _tcp_ports_this_process_binds()


def test_the_bound_ports_count_tcp_sockets_bound_to_a_port_in_any_state():
    inet, inet6, unix = socket.AF_INET, socket.AF_INET6, socket.AF_UNIX
    stream, dgram = socket.SOCK_STREAM, socket.SOCK_DGRAM
    assert _tcp_ports_bound([(inet, stream, ("127.0.0.1", 8000))]) == {8000}
    assert _tcp_ports_bound([(inet6, stream, ("::1", 8001, 0, 0))]) == {8001}
    # Port 1, the lowest a bound socket can read; port 0, a socket not bound yet.
    assert _tcp_ports_bound([(inet, stream, ("0.0.0.0", 1))]) == {1}
    assert _tcp_ports_bound([(inet, stream, ("0.0.0.0", 0))]) == set()
    # The same number held by UDP is not a TCP port; a Unix socket has no port.
    assert _tcp_ports_bound([(inet, dgram, ("127.0.0.1", 8000))]) == set()
    assert _tcp_ports_bound([(unix, stream, "")]) == set()


def test_the_thread_count_sees_a_thread_left_running():
    """This test's own leak: threads that keep running are in the count, and it goes down by
    them only once they end. (Measured down from the count while they run: a thread an earlier
    test joined may still be listed when they start, and leave meanwhile.)"""
    release = threading.Event()
    left = [threading.Thread(target=release.wait) for _ in range(3)]
    for t in left:
        t.start()
    try:
        running = _os_thread_count()
    finally:
        release.set()
        for t in left:
            t.join()
    assert _os_threads_down_to(running - len(left)) <= running - len(left)


def test_mcp_route_is_registered_once():
    _serve_once()
    _serve_once()
    mcp = [r for r in app.routes if r.path == "/mcp"]
    assert len(mcp) == 1, mcp
