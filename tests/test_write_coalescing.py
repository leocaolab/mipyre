"""Write coalescing (docs/write-coalescing.md) on the production serving path, over raw
sockets: a response is never held for the client's next request, pipelined responses come
back whole and in order, and nothing kept is lost when the connection closes. Plain TCP
on the sub-interpreter TPC path, and TLS on the TPC main-interpreter path.

How many writes a batch takes is checked where the writes can be counted: the Rust tests
in `src/server/coalesce/tests.rs` (16 pipelined requests: one write, against hyper's 16).
"""

import shutil
import socket
import ssl
import subprocess
import threading

import pytest

from mipyre import Mipyre, Response, Stream
from mipyre.testing import TestClient
from tests._helpers import poll_until

BIG = 200 * 1024

app = Mipyre()


@app.get("/echo/{name}")
def echo(req):
    return Response(req.params["name"], content_type="text/plain")


@app.get("/big")
def big(req):
    return Response(b"x" * BIG, content_type="application/octet-stream")


# Set by the test once it has read the first event: the stream sends its second only then.
SECOND_EVENT = threading.Event()


@app.get("/sse", runtime="main")
def sse(req):
    stream = Stream()
    stream.send("data: first\n\n")

    def later():
        SECOND_EVENT.wait()
        stream.send("data: second\n\n")
        stream.close()

    threading.Thread(target=later, daemon=True).start()
    return stream


@pytest.fixture(scope="module")
def client():
    with TestClient(app, transport="tcp") as c:
        yield c


def _get(path, extra=""):
    return f"GET {path} HTTP/1.1\r\nHost: t\r\n{extra}\r\n".encode()


class _Reader:
    """Parses HTTP/1.1 responses off a socket (Content-Length or chunked bodies).

    Reads block until the bytes arrive or the server closes the connection (EOFError): what
    the tests check is which bytes arrive and in what order, never how fast (b/325). How
    soon a response is written is the Rust tests' (`crates/pyext/src/serve/coalesce_tests.rs`,
    on a paused clock)."""

    def __init__(self, sock):
        self.sock = sock
        self.sock.settimeout(None)
        self.buf = b""

    def _fill(self):
        chunk = self.sock.recv(65536)
        if not chunk:
            raise EOFError(f"connection closed; buffered {self.buf[:200]!r}")
        self.buf += chunk

    def _until(self, sep):
        while sep not in self.buf:
            self._fill()
        i = self.buf.index(sep)
        out, self.buf = self.buf[:i], self.buf[i + len(sep):]
        return out

    def _exactly(self, n):
        while len(self.buf) < n:
            self._fill()
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def response(self):
        """(status, headers, body) of the next response."""
        head = self._until(b"\r\n\r\n").decode("latin-1")
        status_line, *lines = head.split("\r\n")
        headers = {k.strip().lower(): v.strip() for k, v in (line.split(":", 1) for line in lines)}
        if "content-length" in headers:
            body = self._exactly(int(headers["content-length"]))
        else:
            assert headers.get("transfer-encoding") == "chunked", head
            body = b""
            while True:
                size = int(self._until(b"\r\n"), 16)
                body += self._exactly(size)
                self._exactly(2)
                if size == 0:
                    break
        return int(status_line.split()[1]), headers, body

    def raw_until(self, needle):
        """Reads until `needle` has arrived; returns everything read so far."""
        while needle not in self.buf:
            self._fill()
        return self.buf


def _connect(port):
    sock = socket.create_connection(("127.0.0.1", port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return sock


# ── plain TCP, sub-interpreter TPC path ──


def test_a_response_followed_by_half_a_request_arrives_at_once(client):
    with _connect(client.port) as sock:
        reader = _Reader(sock)
        sock.sendall(_get("/echo/a") + b"GET /echo/b HT")
        # The first response arrives while the second request is still half-sent: it was not
        # held for it. A held one would come out only when the server's header-read timeout
        # closed the connection, and then the second request below could not be answered.
        status, _, body = reader.response()
        assert (status, body) == (200, b"a")
        sock.sendall(b"TP/1.1\r\nHost: t\r\n\r\n")
        assert reader.response()[2] == b"b"


def test_headers_sent_a_few_bytes_at_a_time_complete(client):
    # A smoke test over a real socket. Every cut point and pause is covered in Rust, on an
    # in-memory pipe and a paused clock (`requests_cut_anywhere_are_answered_as_if_sent_whole`
    # in crates/pyext/src/serve/coalesce_tests.rs); here the kernel may join the pieces.
    request = _get("/echo/slow")
    with _connect(client.port) as sock:
        for i in range(0, len(request), 2):
            sock.sendall(request[i:i + 2])
        status, _, body = _Reader(sock).response()
        assert (status, body) == (200, b"slow")


def test_sixteen_pipelined_requests_come_back_whole_and_in_order(client):
    with _connect(client.port) as sock:
        sock.sendall(b"".join(_get(f"/echo/{i}") for i in range(16)))
        reader = _Reader(sock)
        bodies = [reader.response()[2] for _ in range(16)]
        assert bodies == [str(i).encode() for i in range(16)]


def test_connection_close_in_a_batch_loses_no_response(client):
    with _connect(client.port) as sock:
        sock.sendall(_get("/echo/1") + _get("/echo/2") + _get("/echo/3", "Connection: close\r\n"))
        reader = _Reader(sock)
        bodies = [reader.response()[2] for _ in range(3)]
        assert bodies == [b"1", b"2", b"3"]
        assert sock.recv(1) == b""  # and then the server closes


def test_a_large_response_between_small_ones(client):
    with _connect(client.port) as sock:
        sock.sendall(_get("/echo/a") + _get("/big") + _get("/echo/c"))
        reader = _Reader(sock)
        first, second, third = (reader.response()[2] for _ in range(3))
        assert first == b"a"
        assert second == b"x" * BIG
        assert third == b"c"


def test_a_streamed_response_is_sent_chunk_by_chunk_with_a_request_queued_behind(client):
    with _connect(client.port) as sock:
        reader = _Reader(sock)
        sock.sendall(_get("/sse") + _get("/echo/after"))
        seen = reader.raw_until(b"data: first")
        # The first event is out while the stream waits for the second, which it sends only
        # once the test has read the first.
        assert b"data: second" not in seen
        SECOND_EVENT.set()
        status, _, body = reader.response()
        assert status == 200 and body == b"data: first\n\ndata: second\n\n"
        assert reader.response()[2] == b"after"


# ── TLS, TPC main-interpreter path ──


@pytest.fixture(scope="module")
def tls_port(tmp_path_factory):
    if shutil.which("openssl") is None:
        pytest.skip("openssl CLI not available")
    d = tmp_path_factory.mktemp("coalesce_tls")
    cert, key = d / "cert.pem", d / "key.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(key),
         "-out", str(cert), "-days", "1", "-subj", "/CN=localhost"],
        check=True, capture_output=True,
    )
    tls_app = Mipyre(runtime="main")

    @tls_app.get("/echo/{name}")
    def tls_echo(req):
        return Response(req.params["name"], content_type="text/plain")

    t = threading.Thread(
        target=lambda: tls_app.run(
            host="127.0.0.1", port=0, tls_cert=str(cert), tls_key=str(key)
        ),
        daemon=True,
    )
    t.start()
    servers = poll_until(lambda: list(tls_app._servers), timeout=None, what="TLS bind")
    port = servers[0].port
    poll_until(lambda: _tls_answers(port), timeout=None, what="TLS serving")
    yield port
    for srv in servers:
        srv.shutdown()
    t.join(10)


def _tls_context():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _tls_connect(port):
    return _tls_context().wrap_socket(_connect(port), server_hostname="localhost")


def _tls_answers(port):
    try:
        with _tls_connect(port) as sock:
            sock.sendall(_get("/echo/up"))
            return _Reader(sock).response()[2] == b"up"
    except (OSError, EOFError):  # not listening yet, or closed while starting
        return False


def test_tls_a_response_followed_by_half_a_request_arrives_at_once(tls_port):
    with _tls_connect(tls_port) as sock:
        reader = _Reader(sock)
        sock.sendall(_get("/echo/a") + b"GET /echo/b HT")
        # The first response arrives while the second request is still half-sent: it was not
        # held for it. A held one would come out only when the server's header-read timeout
        # closed the connection, and then the second request below could not be answered.
        status, _, body = reader.response()
        assert (status, body) == (200, b"a")
        sock.sendall(b"TP/1.1\r\nHost: t\r\n\r\n")
        assert reader.response()[2] == b"b"


def test_tls_sixteen_pipelined_requests_come_back_whole_and_in_order(tls_port):
    with _tls_connect(tls_port) as sock:
        sock.sendall(b"".join(_get(f"/echo/{i}") for i in range(16)))
        reader = _Reader(sock)
        bodies = [reader.response()[2] for _ in range(16)]
        assert bodies == [str(i).encode() for i in range(16)]
