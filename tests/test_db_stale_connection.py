"""A pooled connection the server closed while it sat idle (b/18).

The pool does not ping a connection before handing it out (`ping_on_acquire=False`,
the default), so the first query on a connection the server has ended fails, and it
fails with the server's own error, not a summary. The pool then drops that connection
(sqlx pings it on release, which fails) and the next query runs on a new one; nothing
retries the failed query. With `ping_on_acquire=True` the dead connection is found
before it is handed out and no query fails.

Each case runs in its own process: a process has one pool, and the cases need pools
with one connection and different settings. Skipped unless `MIPYRE_TEST_PG_DSN` is set.
"""

import json
import os
import subprocess
import sys

import pytest

PG_DSN = os.environ.get("MIPYRE_TEST_PG_DSN")

pytestmark = pytest.mark.skipif(PG_DSN is None, reason="MIPYRE_TEST_PG_DSN not set")

# Opens a one-connection pool, has another process end that connection's backend with
# pg_terminate_backend, then runs two queries and reports what each did.
_SCRIPT = r"""
import json, subprocess, sys
from mipyre.db import PgPool

dsn, ping = sys.argv[1], sys.argv[2] == "1"
pool = PgPool.connect(dsn, max_connections=1, ping_on_acquire=ping)
pid = pool.fetch_scalar("SELECT pg_backend_pid()")
killer = (
    "import sys; from mipyre.db import PgPool; "
    "p = PgPool.connect(sys.argv[1], max_connections=1); "
    "print(p.fetch_scalar('SELECT pg_terminate_backend($1)', int(sys.argv[2])))"
)
killed = subprocess.run(
    [sys.executable, "-c", killer, dsn, str(pid)], capture_output=True, text=True, check=True
).stdout.strip()

def attempt():
    try:
        return {"ok": pool.fetch_scalar("SELECT pg_backend_pid()")}
    except Exception as e:
        return {"error": type(e).__name__, "message": str(e), "sqlstate": e.sqlstate}

first = attempt()
second = attempt()
print(json.dumps({"pid": pid, "killed": killed, "first": first, "second": second}))
"""


def _run(ping: bool) -> dict:
    assert PG_DSN is not None, "the module is skipped without MIPYRE_TEST_PG_DSN"
    done = subprocess.run(
        [sys.executable, "-c", _SCRIPT, PG_DSN, "1" if ping else "0"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_the_first_query_on_a_terminated_connection_fails_with_the_servers_error():
    report = _run(ping=False)
    assert report["killed"] == "True"
    first = report["first"]
    assert first["error"] == "DatabaseError"
    # 57P01 admin_shutdown, with the server's message as it sent it.
    assert first["sqlstate"] == "57P01"
    assert "terminating connection due to administrator command" in first["message"]
    # The pool dropped the dead connection: the next query runs on a new backend.
    second = report["second"]
    assert "ok" in second, second
    assert second["ok"] != report["pid"]


def test_ping_on_acquire_replaces_a_terminated_connection_before_the_query():
    report = _run(ping=True)
    assert report["killed"] == "True"
    assert "ok" in report["first"], report["first"]
    assert report["first"]["ok"] != report["pid"]
    assert "ok" in report["second"], report["second"]
