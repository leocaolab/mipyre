"""b/272: guards for infra_http review fixes that had none.

- N2 (b/212), the engine's side: a listener that becomes unusable stops the whole server, and
  `serve()` raises naming it, as an OSError, on the TPC server and on the multi-thread one
  (`MIPYRE_TPC=0`). The infra side (`listener.rs::an_unusable_listener_stops_the_loop_at_once`)
  only shows the accept loop returns.
"""

from __future__ import annotations

import os
import socket
import sys
import threading
import urllib.error
import urllib.request

import pytest

import mipyre.engine

from tests._helpers import poll_until

HOST = "127.0.0.1"


# ---------------------------------------------------------------------------
# N2: an unusable listener stops the server
# ---------------------------------------------------------------------------


def _answers(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://{HOST}:{port}/", timeout=2):
            return True
    except urllib.error.HTTPError:
        return True  # an answer (the app has no routes: 404)
    except OSError:
        return False


def _shut_down_listeners(port: int) -> int:
    """`shutdown()` every listening socket of this process on `port`; returns how many.

    On Linux a listening socket that is shut down answers `accept` with EINVAL: the listener is
    no longer listening, which is what `classify` calls `ListenerBroken`. The fds are the
    server's own (this process); each is reached through a dup, and shutdown acts on the socket,
    not the fd, so the server's fd is left open and only stops listening.
    """
    shut = 0
    for name in os.listdir("/proc/self/fd"):
        try:
            s = socket.socket(fileno=os.dup(int(name)))
        except OSError:
            continue  # not a socket (or the fd listdir itself used, gone by now)
        with s:
            if s.family not in (socket.AF_INET, socket.AF_INET6) or s.type != socket.SOCK_STREAM:
                continue
            if not s.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN):
                continue
            if s.getsockname()[1] != port:
                continue
            s.shutdown(socket.SHUT_RDWR)
            shut += 1
    return shut


@pytest.mark.skipif(sys.platform != "linux", reason="shutdown(2) on a listening socket breaks accept on Linux")
# One per accept loop that reads a broken listener's stop: TPC's GIL-mode and sub-interpreter
# loops (serve/tpc/), and the multi-thread server's one loop, which both modes share (so one mode
# of it). Which errno is which failure, and that a broken listener stops, are tcp::listener's
# tests (mipyre_transport).
@pytest.mark.parametrize(
    ("tpc", "mode"),
    [("1", "gil"), ("1", "subinterp"), ("0", "gil")],
    ids=["tpc-gil", "tpc-subinterp", "multi_thread"],
)
def test_an_unusable_listener_stops_the_server_and_serve_raises(monkeypatch, tpc, mode):
    """A real listening socket, shut down under the running server: the fact is Linux's accept(2)
    answering EINVAL on it, which no function here produces. Which errno is which failure is a pure
    test in tcp::listener; this one checks each accept loop stops on the real one."""
    monkeypatch.setenv("MIPYRE_TPC", tpc)
    monkeypatch.setenv("MIPYRE_WORKERS", "2")
    monkeypatch.setenv("MIPYRE_IO_WORKERS", "2")
    engine = mipyre.engine.MipyreApp()
    # The sub-interpreter server names the script its workers would run; under xdist `__main__`
    # has none. No route runs on a sub-interpreter here, so no worker runs it.
    engine.set_script_path(os.path.abspath(__file__))
    server = engine.start(host=HOST, port=0, mode=mode)
    port = server.port
    raised: list[BaseException] = []

    def serve() -> None:
        try:
            server.serve()
        except BaseException as e:  # noqa: BLE001 — handed to the test thread, which asserts on it
            raised.append(e)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    poll_until(lambda: _answers(port) or not thread.is_alive(), timeout=20, what="the server answering")
    assert thread.is_alive(), f"serve() returned before the listener broke: {raised}"

    assert _shut_down_listeners(port) >= 1
    thread.join(20)
    alive = thread.is_alive()
    if alive:
        server.shutdown()
        thread.join(20)
    assert not alive, "the server kept running with no listener to accept from"
    assert len(raised) == 1, raised
    assert "accept loop stopped" in str(raised[0]) and "unusable" in str(raised[0]), raised[0]
    # Both servers raise the same condition as the same exception, with the same text (b/280).
    assert isinstance(raised[0], OSError), raised
    assert str(raised[0]).startswith("the server stopped: accept loop stopped: the listener is unusable"), raised[0]

