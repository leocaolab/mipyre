"""End-to-end Postgres tests.

Skipped entirely unless `MIPYRE_TEST_PG_DSN` is set. To run locally::

    docker run --rm -d --name mipyre-pg -p 5433:5432 \\
        -e POSTGRES_PASSWORD=mipyre -e POSTGRES_DB=mipyretest postgres:17-alpine

    export MIPYRE_TEST_PG_DSN="postgres://postgres:mipyre@127.0.0.1:5433/mipyretest"
    pytest tests/test_db_pg.py

Outside a server run each `PgPool.connect()` handle opens a pool of its own on its first
query; in a run, every handle to one DSN shares the run's pool. The tests here use a
module-scoped fixture that sets up a schema once.
"""

import datetime
import json
import os
import subprocess
import sys
import uuid

import pytest

from mipyre import Mipyre
from mipyre.db import PgPool
from mipyre.testing import TestClient


PG_DSN = os.environ.get("MIPYRE_TEST_PG_DSN")

pytestmark = pytest.mark.skipif(
    PG_DSN is None,
    reason="MIPYRE_TEST_PG_DSN not set — skipping Postgres integration tests",
)


@pytest.fixture(scope="module")
def pool():
    # Outside a server run this handle opens a pool of its own on its first query.
    p = PgPool.connect(PG_DSN)
    # Fresh schema per test module.
    p.execute("DROP TABLE IF EXISTS mipyre_test_rows")
    p.execute("""
        CREATE TABLE mipyre_test_rows (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER,
            flag BOOLEAN DEFAULT false,
            meta JSONB,
            bin BYTEA
        )
    """)
    yield p
    p.execute("DROP TABLE IF EXISTS mipyre_test_rows")


# ---------------------------------------------------------------------------
# Raw pool API
# ---------------------------------------------------------------------------

def test_execute_returns_rows_affected(pool):
    n = pool.execute(
        "INSERT INTO mipyre_test_rows (name, value) VALUES ($1, $2)",
        "alice", 42,
    )
    assert n == 1


def test_fetch_one_returns_dict(pool):
    pool.execute("INSERT INTO mipyre_test_rows (name, value) VALUES ($1, $2)", "bob", 7)
    row = pool.fetch_one("SELECT name, value FROM mipyre_test_rows WHERE name = $1", "bob")
    assert row == {"name": "bob", "value": 7}


def test_fetch_one_none_on_no_match(pool):
    row = pool.fetch_one("SELECT name FROM mipyre_test_rows WHERE name = $1", "nobody")
    assert row is None


def test_fetch_all_returns_list(pool):
    pool.execute("DELETE FROM mipyre_test_rows")
    for i in range(3):
        pool.execute("INSERT INTO mipyre_test_rows (name, value) VALUES ($1, $2)", f"n{i}", i)
    rows = pool.fetch_all("SELECT name, value FROM mipyre_test_rows ORDER BY value")
    assert [(r["name"], r["value"]) for r in rows] == [("n0", 0), ("n1", 1), ("n2", 2)]


def test_fetch_scalar(pool):
    count = pool.fetch_scalar("SELECT COUNT(*) FROM mipyre_test_rows")
    assert isinstance(count, int)
    assert count >= 0


def test_null_values(pool):
    pool.execute("INSERT INTO mipyre_test_rows (name, value) VALUES ($1, $2)", "nullish", None)
    row = pool.fetch_one("SELECT name, value FROM mipyre_test_rows WHERE name = $1", "nullish")
    assert row == {"name": "nullish", "value": None}


def test_bool_and_json(pool):
    pool.execute(
        "INSERT INTO mipyre_test_rows (name, flag, meta) VALUES ($1, $2, $3)",
        "mix", True, {"a": 1, "b": [1, 2, 3]},
    )
    row = pool.fetch_one(
        "SELECT flag, meta FROM mipyre_test_rows WHERE name = $1", "mix"
    )
    assert row["flag"] is True
    assert row["meta"] == {"a": 1, "b": [1, 2, 3]}


def test_bytes_roundtrip(pool):
    blob = bytes(range(256))
    pool.execute(
        "INSERT INTO mipyre_test_rows (name, bin) VALUES ($1, $2)", "blob", blob
    )
    row = pool.fetch_one("SELECT bin FROM mipyre_test_rows WHERE name = $1", "blob")
    assert row["bin"] == blob


# ---------------------------------------------------------------------------
# Handler integration
# ---------------------------------------------------------------------------

def test_handler_can_query(pool):
    # Pre-seed data
    pool.execute("DELETE FROM mipyre_test_rows")
    pool.execute("INSERT INTO mipyre_test_rows (name, value) VALUES ($1, $2)", "carol", 100)

    # Module level (workers rebuild the app by executing its module).
    from tests.apps.db_pg_query import app

    with TestClient(app) as c:
        resp = c.get("/users/carol")
        assert resp.status_code == 200
        assert resp.json() == {"name": "carol", "value": 100}

        resp = c.get("/users/nobody")
        assert resp.json() == {"error": "not found"}


def test_unsupported_param_type_raises(pool):
    with pytest.raises(ValueError, match="unsupported parameter type"):
        pool.fetch_one("SELECT $1::text", object())


def test_connect_is_idempotent(pool):
    """Calling connect() again with the same DSN in one run returns a handle to the same pool
    (b/335 D17: the run's pool for the DSN). One connection, so both see one backend."""
    # A DSN of its own: the module's pool already fixed this DSN's size.
    dsn = PG_DSN + ("&" if "?" in PG_DSN else "?") + "application_name=idempotent"
    one = PgPool.connect(dsn, max_connections=1)
    again = PgPool.connect(dsn)
    app = Mipyre(runtime="main")

    @app.get("/backends")
    def backends(req):
        return [one.fetch_scalar("SELECT pg_backend_pid()"), again.fetch_scalar("SELECT pg_backend_pid()")]

    with TestClient(app) as c:
        first, second = c.get("/backends").json()
    assert first == second


# Holds a session advisory lock in a process of its own (outside the run under test) until its
# stdin closes; it says "locked" once it holds it.
_LOCK_HOLDER_SCRIPT = r"""
import sys
from mipyre.db import PgPool

holder = PgPool.connect(sys.argv[1], max_connections=1)
holder.execute("SELECT pg_advisory_lock($1)", int(sys.argv[2]))
print("locked", flush=True)
sys.stdin.read()
"""


def test_an_async_query_in_flight_when_a_run_takes_its_handle_still_answers():
    """b/968: a handle that queried before a run opened a pool of its own; when the run seals it
    binds the handle to the run's pool (b/335 D17), and a query already running on the old pool
    still gets its answer. The query waits on a lock held across the seal, so it is in flight
    then; before the fix the seal dropped the old pool's runtime and the query failed with
    "the runtime shut down"."""
    import asyncio
    import concurrent.futures
    import threading

    key = uuid.uuid4().int >> 65
    holder = subprocess.Popen(
        [sys.executable, "-c", _LOCK_HOLDER_SCRIPT, PG_DSN, str(key)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    )
    try:
        assert holder.stdout.readline() == "locked\n"
        held = PgPool.connect(PG_DSN)
        issued = threading.Event()

        async def held_query():
            answer = held.fetch_scalar_async(
                "SELECT $2::text FROM (SELECT pg_advisory_xact_lock_shared($1)) AS waited", key, "answered"
            )
            issued.set()
            return await answer

        with concurrent.futures.ThreadPoolExecutor(1) as waiter:
            outcome = waiter.submit(asyncio.run, held_query())
            assert issued.wait(30)
            app = Mipyre(runtime="main")
            app.get("/")(lambda req: "up")

            with TestClient(app):
                holder.stdin.close()
                assert outcome.result(30) == "answered"
    finally:
        holder.kill()
        holder.wait(30)


# ---------------------------------------------------------------------------
# Async API (v2 — await-ready methods for async def handlers)
# ---------------------------------------------------------------------------

def test_async_api(pool):
    """The *_async methods return awaitables that cooperate with asyncio."""
    import asyncio

    async def run():
        val = await pool.fetch_scalar_async("SELECT 1")
        assert val == 1

        one = await pool.fetch_one_async("SELECT $1::int AS x", 42)
        assert one == {"x": 42}

        # None on no match.
        none_row = await pool.fetch_one_async(
            "SELECT id FROM mipyre_test_rows WHERE id = $1", 999999
        )
        assert none_row is None

        rows = await pool.fetch_all_async(
            "SELECT 1 AS a UNION SELECT 2 ORDER BY a"
        )
        assert [r["a"] for r in rows] == [1, 2]

    asyncio.run(run())


def test_async_execute(pool):
    """execute_async returns the rows-affected count."""
    import asyncio

    async def run():
        pool.execute("DELETE FROM mipyre_test_rows WHERE name = $1", "async_tmp")
        n = await pool.execute_async(
            "INSERT INTO mipyre_test_rows (name, value) VALUES ($1, $2)",
            "async_tmp", 123,
        )
        assert n == 1
        # Clean up.
        pool.execute("DELETE FROM mipyre_test_rows WHERE name = $1", "async_tmp")

    asyncio.run(run())


def test_unknown_column_types_do_not_explode(pool):
    """Regression for the DB-decode-fallback bug: the old `_ =>` fallback
    in `column_to_py` forced `<String as Decode>::decode` on the raw binary
    value, which failed for every non-text Postgres type (UUID, TIMESTAMP,
    INET, interval, …). Queries touching those types blew up with
    PyRuntimeError.

    Columns are now decoded by their Postgres type: UUID and timestamps
    become `uuid.UUID` / `datetime.datetime`, and a type with no decoder
    (INET) is always returned as its raw wire bytes, never guessed text.
    """
    # UUID — decoded to uuid.UUID.
    row = pool.fetch_one("SELECT gen_random_uuid() AS u")
    assert row is not None
    assert isinstance(row["u"], uuid.UUID)

    # TIMESTAMPTZ — decoded to datetime.datetime.
    row = pool.fetch_one("SELECT now() AS t")
    assert row is not None
    assert isinstance(row["t"], datetime.datetime)

    # INET — no decoder: raw wire bytes.
    row = pool.fetch_one("SELECT '10.0.0.1'::inet AS ip")
    assert row is not None
    assert isinstance(row["ip"], bytes)


def test_uuid_text_cast_roundtrip(pool):
    """Workaround documented in the fallback comment: cast to text
    server-side for a clean string column in the Python dict."""
    row = pool.fetch_one("SELECT gen_random_uuid()::text AS u")
    assert isinstance(row["u"], str)
    # Rough UUID shape check: 8-4-4-4-12 hex + dashes.
    assert len(row["u"]) == 36
    assert row["u"].count("-") == 4


def test_fetch_iter_basic(pool):
    """Cursor yields rows one at a time, preserving value + type."""
    rows = list(pool.fetch_iter(
        "SELECT $1::int AS x, $2::text AS y UNION ALL SELECT $3, $4",
        1, "a", 2, "b",
    ))
    assert rows == [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]


def test_fetch_iter_large_result_set_bounded_memory(pool):
    """Streaming 50k rows must not spike resident memory. We can't
    assert exact RSS in a pytest, but we can (a) verify the iterator
    completes and (b) verify the process didn't crash — which fetch_all
    on 50k-row queries has historically been close to doing on
    memory-constrained boxes because of the O(2N) peak."""
    count = 0
    checksum = 0
    for row in pool.fetch_iter(
        "SELECT i, i * 2 AS doubled FROM generate_series(1, 50000) i"
    ):
        count += 1
        checksum += row["doubled"]
    assert count == 50000
    assert checksum == sum(i * 2 for i in range(1, 50001))


def test_fetch_iter_early_break_cleans_up(pool):
    """Breaking out of the iteration mid-stream must not leak the
    Postgres connection. Run many cursors with early break in sequence;
    if connection cleanup is busted the pool (size 4) runs out after
    a handful of iterations."""
    for _ in range(50):
        seen = 0
        for _row in pool.fetch_iter(
            "SELECT i FROM generate_series(1, 1000000) i"
        ):
            seen += 1
            if seen == 3:
                break
        assert seen == 3
    # If we got here the pool never starved.


def test_fetch_iter_propagates_sql_error(pool):
    """SQL errors surface as RuntimeError on the first __next__ that
    sees the error (sqlx discovers the error during streaming)."""
    import pytest
    with pytest.raises(RuntimeError):
        # Division by zero raises during execution (not planning).
        list(pool.fetch_iter("SELECT 1/0"))


def test_fetch_iter_empty_result(pool):
    assert list(pool.fetch_iter("SELECT 1 WHERE FALSE")) == []


def test_fetch_iter_via_to_list(pool):
    """to_list is the eager-drain shortcut; equivalent to list(cursor)."""
    rows = pool.fetch_iter(
        "SELECT i FROM generate_series(1, 5) i"
    ).to_list()
    assert [r["i"] for r in rows] == [1, 2, 3, 4, 5]


# Arrive at a rendezvous of `n` queries and wait for the rest: count this query into the sequence
# `seq` (nextval is not transactional, so the other sessions see it at once), then wait until it
# reaches `n`; returns the count seen then. `give_up_s` ends the wait in Postgres (a query must not
# outlive the test); it is not what is checked, the count is.
_RENDEZVOUS = """
CREATE OR REPLACE FUNCTION mipyre_test_rendezvous(seq text, n int, give_up_s float8)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    seen bigint;
    since timestamptz := clock_timestamp();
BEGIN
    PERFORM nextval(seq::regclass);
    LOOP
        EXECUTE format('SELECT last_value FROM %I', seq) INTO seen;
        EXIT WHEN seen >= n OR clock_timestamp() > since + make_interval(secs => give_up_s);
        PERFORM pg_sleep(0.005);
    END LOOP;
    RETURN seen;
END $$
"""


@pytest.fixture
def rendezvous(pool):
    """A fresh rendezvous sequence (its name), with the function that meets on it."""
    pool.execute(_RENDEZVOUS)
    seq = f"mipyre_test_rdv_{uuid.uuid4().hex}"
    pool.execute(f"CREATE SEQUENCE {seq}")
    yield seq
    pool.execute(f"DROP SEQUENCE IF EXISTS {seq}")


def test_async_concurrent_queries(pool, rendezvous):
    """Four async queries on the pool's tokio runtime are in Postgres at once: each waits for
    the other three to arrive and answers how many had. Until b/441 this was four
    `pg_sleep(0.1)` under 0.3 s, which three at a time also passed."""
    import asyncio

    async def run():
        return await asyncio.gather(*[
            pool.fetch_scalar_async("SELECT mipyre_test_rendezvous($1, 4, 60)", rendezvous)
            for _ in range(4)
        ])

    assert asyncio.run(run()) == [4] * 4


def test_the_rendezvous_sees_queries_run_one_at_a_time(pool, rendezvous):
    """The rendezvous's own input: the same four queries awaited one after another. Each sees
    only those before it and itself, and gives up waiting for the rest (after 0.2 s)."""
    import asyncio

    async def run():
        return [
            await pool.fetch_scalar_async("SELECT mipyre_test_rendezvous($1, 4, 0.2)", rendezvous)
            for _ in range(4)
        ]

    assert asyncio.run(run()) == [1, 2, 3, 4]


# ---------------------------------------------------------------------------
# A query that finds every pooled connection held (b/373)
# ---------------------------------------------------------------------------
#
# Whoever holds the pool's connections, a query waiting for one gives up at
# `acquire_timeout_secs` with a `DatabaseError` whose `sqlstate` is None (the server never saw
# it), and the wait takes nothing: once the holder lets go of it (its result read to the end),
# the same one connection serves the next query, so the pool is back where it was, with no
# connection leaked or opened twice. A cursor dropped mid-result closes its connection instead
# (b/462); that case is `test_cursor_connection_returns_after_a_full_read_and_closes_after_a_drop`.
#
# The error's mapping is unit-tested in `crates/pyext/src/db/error.rs`. The case runs in its own
# process: a process has one pool, and this one needs a pool of one connection (this module's
# `pool` fixture is not used).

# A cursor holds the pool's one connection: its driver task streams more rows than the
# cursor's channel holds (8), so after the first row it waits on the channel with the
# connection checked out, until the cursor has read the rest.
_FULL_POOL_SCRIPT = r"""
import json, sys, time
from mipyre.db import PgPool

pool = PgPool.connect(sys.argv[1], max_connections=1, acquire_timeout_secs=1)
pid = pool.fetch_scalar("SELECT pg_backend_pid()")

cursor = pool.fetch_iter("SELECT n FROM generate_series(1, 100000) AS n")
first = next(cursor)["n"]
started = time.monotonic()
try:
    pool.fetch_scalar("SELECT 1")
    timed_out = {"ok": True}
except Exception as e:
    timed_out = {"error": type(e).__name__, "mro": [c.__name__ for c in type(e).__mro__],
                 "message": str(e), "sqlstate": e.sqlstate}
waited = time.monotonic() - started

for _ in cursor:
    pass
after = pool.fetch_scalar("SELECT pg_backend_pid()")
print(json.dumps({"pid": pid, "first": first, "timed_out": timed_out, "waited": waited,
                  "after": after}))
"""


def test_a_full_pool_times_out_without_sqlstate_and_the_connection_comes_back():
    done = subprocess.run(
        [sys.executable, "-c", _FULL_POOL_SCRIPT, PG_DSN],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert done.returncode == 0, done.stderr
    report = json.loads(done.stdout.strip().splitlines()[-1])
    assert report["first"] == 1

    timed_out = report["timed_out"]
    assert timed_out.get("error") == "DatabaseError", timed_out
    assert "ConnectionError" not in timed_out["mro"], timed_out
    assert timed_out["sqlstate"] is None
    assert timed_out["message"].startswith("fetch_scalar: "), timed_out
    assert "timed out" in timed_out["message"], timed_out
    # It waited for the acquire timeout (1 s), not for the holder.
    assert report["waited"] >= 1.0, report

    # The one connection came back: the next query runs on the same backend.
    assert report["after"] == report["pid"], report


# ---------------------------------------------------------------------------
# What a cursor's connection does when the cursor ends (b/462)
# ---------------------------------------------------------------------------

# A one-connection pool in its own process (a process has one pool, and this one must
# have a single connection, so the query after the cursor needs the cursor's own). The
# abandoned query never ends: before b/462 the pool read the rest of a dropped cursor's
# result before handing the connection out again, so the next query waited until the
# pool's acquire timeout. Then it waits for the abandoned backend to be gone from the
# server (an event, polled; the subprocess timeout is the only bound).
_CURSOR_RELEASE_SCRIPT = r"""
import json, sys, time
from mipyre.db import PgPool

pool = PgPool.connect(sys.argv[1], max_connections=1)

read = list(pool.fetch_iter("SELECT pg_backend_pid() AS pid, generate_series(1, 3) AS i"))
after_read = pool.fetch_scalar("SELECT pg_backend_pid()")

failed = None
try:
    list(pool.fetch_iter("SELECT pg_backend_pid() AS pid, 1 / (3 - generate_series(1, 5)) AS i"))
except Exception as e:
    failed = e.sqlstate
after_error = pool.fetch_scalar("SELECT pg_backend_pid()")

cursor = pool.fetch_iter(
    "SELECT pg_backend_pid() AS pid, generate_series(1, 9223372036854775807) AS i"
)
abandoned = next(cursor)["pid"]
del cursor
after_drop = pool.fetch_scalar("SELECT pg_backend_pid()")
while pool.fetch_scalar("SELECT count(*) FROM pg_stat_activity WHERE pid = $1", abandoned):
    time.sleep(0.01)

print(json.dumps({
    "read": [r["pid"] for r in read],
    "after_read": after_read,
    "failed": failed,
    "after_error": after_error,
    "abandoned": abandoned,
    "after_drop": after_drop,
}))
"""


def _cursor_release_report() -> dict:
    assert PG_DSN is not None, "the module is skipped without MIPYRE_TEST_PG_DSN"
    done = subprocess.run(
        [sys.executable, "-c", _CURSOR_RELEASE_SCRIPT, PG_DSN],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_cursor_connection_returns_after_a_full_read_and_closes_after_a_drop():
    report = _cursor_release_report()
    # Read to the end: the same connection serves the next query.
    first = report["read"][0]
    assert report["read"] == [first] * 3
    assert report["after_read"] == first
    # The server's error ended the statement: the connection is kept too.
    assert report["failed"] == "22012"  # division_by_zero
    assert report["after_error"] == first
    # Dropped mid-result: the next query got a connection (a new one, the pool's only
    # slot came back), and the backend that ran the endless query is gone.
    assert report["abandoned"] == first
    assert report["after_drop"] != report["abandoned"]
