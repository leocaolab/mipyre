"""A request refused before its body is read still gets its answer.

The server answers 413 from the ``Content-Length`` alone, then closes. Closed at once, the
client, still writing the body, met ``EPIPE`` on a Unix socket and a reset on TCP, and never
read the 413. The body here is larger than any socket buffer, so it is still being written
when the answer goes out, on every run.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator

import pytest

from mipyre import Mipyre
from mipyre.testing import TestClient, TestServer

MAX_BODY_SIZE = 1000
# Larger than a socket buffer holds on Linux or macOS, TCP or Unix.
UNREAD_BODY_SIZE = 5 * 1024 * 1024

# Module level: runtime="main" serves it on the main interpreter, so nothing re-executes this file.
_app = Mipyre(runtime="main")
_app.max_body_size = MAX_BODY_SIZE


@_app.post("/len")
def _len(req):
    return {"len": len(req.body)}


@contextlib.contextmanager
def _client(transport: str) -> Iterator[TestClient]:
    """A client of an in-memory server ("memory") or of a server on a TCP port ("tcp")."""
    if transport == "memory":
        with TestClient(_app) as c:
            yield c
    else:
        with TestServer(_app) as server, TestClient(server) as c:
            yield c


@pytest.mark.parametrize("transport", ["memory", "tcp"])
def test_a_body_refused_by_its_length_gets_its_413_while_the_client_is_still_sending(transport):
    with _client(transport) as c:
        assert c.post("/len", body=b"x" * UNREAD_BODY_SIZE).status_code == 413
        assert c.post("/len", body=b"x" * MAX_BODY_SIZE).json() == {"len": MAX_BODY_SIZE}
