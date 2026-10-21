"""b/256: the call sites of the b/128 error-handling fixes, end to end.

The mechanisms (the log gate, the degradation counters, the error kinds) have unit tests beside
them; these tests drive the places that use them in a real server process:

- b/133: a JSON serializer that does not return bytes is `SerializerUnavailable` (the
  environment), not `NotJsonSerializable` (the value).

Each server is a subprocess of its own: the JSON serializer is per process, and another test in
this pytest process may already have replaced it. (b/132's worker counts are the run's own,
unit-tested beside `LiveWorkers` and `Retirement`, b/297; b/131's DB runtime that is not kept when
its build fails is unit-tested beside `db::BuildSlot`, b/302.)
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import urllib.error
import urllib.request

import pytest

from tests._helpers import bound_port, kill_session, lines_with, read_file

HOST = "127.0.0.1"


class Server:
    """`script` served in a subprocess of its own, its output in `tmp_path`."""

    def __init__(self, tmp_path, script: str, env: dict[str, str]):
        path = tmp_path / "app.py"
        path.write_text(textwrap.dedent(script))
        self.log_path = str(tmp_path / "server.log")
        full_env = dict(os.environ)
        full_env.pop("MIPYRE_TPC", None)
        full_env["MIPYRE_LOG"] = "1"
        full_env.update(env)
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                [sys.executable, str(path)], stdout=log, stderr=subprocess.STDOUT,
                env=full_env, start_new_session=True,
            )
        self.output = read_file(self.log_path)
        self.port = bound_port(self.output, self.proc)

    def get(self, path: str) -> tuple[int, str]:
        """`path` on a connection of its own (the kernel picks the thread that takes it)."""
        try:
            with urllib.request.urlopen(f"http://{HOST}:{self.port}{path}", timeout=5) as resp:
                return resp.status, resp.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()
        except (urllib.error.URLError, ConnectionError) as e:
            # A TPC thread whose worker retired closes its listeners: a connection queued on one
            # is refused or reset once. Status 0 says so; the caller asks again.
            return 0, repr(e)

    def close(self) -> None:
        kill_session(self.proc)


@pytest.fixture
def serve(tmp_path):
    started: list[Server] = []

    def start(script: str, env: dict[str, str] | None = None) -> Server:
        s = Server(tmp_path, script, env or {})
        started.append(s)
        return s

    yield start
    for s in started:
        s.close()


# ---------------------------------------------------------------------------
# b/133: SerializerUnavailable
# ---------------------------------------------------------------------------

BROKEN_SERIALIZER_APP = """
    import isojson

    # The serializer loads, but what it returns is not bytes: the environment is broken, not
    # the value the handler returned.
    isojson.dumps = lambda obj, default=None: "not bytes"

    from mipyre import Mipyre

    app = Mipyre(runtime="main")

    @app.get("/dict")
    def a_dict(req):
        return {"a": 1}

    app.run(host="127.0.0.1", port=0)
"""


def test_a_serializer_that_returns_no_bytes_is_unavailable_not_the_values_fault(serve):
    s = serve(BROKEN_SERIALIZER_APP)
    status, body = s.get("/dict")
    assert status == 500, body
    lines = lines_with(s.output, "request failed")
    assert len(lines) == 1, s.output()[-3000:]
    assert '"level":"ERROR"' in lines[0], lines[0]
    assert "the JSON serializer is unavailable" in lines[0], lines[0]
    assert "could not be serialized as JSON" not in lines[0], lines[0]
