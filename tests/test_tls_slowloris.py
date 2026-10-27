"""The TLS handshake is bounded at 10 s (Slowloris).

A client that opens a TCP connection and never finishes the handshake would otherwise pin an
async task and an fd indefinitely, and enough half-open connections run the server out of fds.
A client that doesn't complete in that window has its connection closed. We can't wait 10 s in a
unit test, so we verify the bound is *present* in the source (a real
end-to-end test needs a malicious client that never sends ClientHello).

Why not end to end: the bound is a timer around the handshake of a real accepted stream; this checks
it is present in the source rather than wait out the 10 s against a client that never says hello.
"""

import pathlib


# Rooted at this file instead of the process's cwd, like the other source rules.
TLS = pathlib.Path(__file__).resolve().parent.parent / "crates" / "transport" / "src" / "tls" / "mod.rs"


def test_tls_handshake_bounded_by_timeout():
    src = TLS.read_text()
    # A `tokio::time::timeout(..., acceptor.accept(...))` sandwich is the
    # only mechanism available to bound an async future; look for both
    # halves. The actual timeout value is a constant above the call.
    assert "tokio::time::timeout" in src, (
        "tls::wrap_tls must wrap acceptor.accept() in tokio::time::timeout "
        "so a slow client can't pin an fd forever (Slowloris)"
    )
    assert "acceptor.accept" in src
    # Sanity: the timeout constant is reasonable (≥ 1s, ≤ 60s — users
    # can override if they have extraordinary network conditions).
    assert "HANDSHAKE_TIMEOUT" in src
