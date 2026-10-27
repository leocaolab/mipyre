"""`PgPool.*_async` result delivery: results, errors, concurrency,
cancellation and a closed loop.

Each case runs in a fresh interpreter so a stuck delivery shows up as a timeout, not as a
hung pytest process. Skipped unless `MIPYRE_TEST_PG_DSN` is set.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import pytest

from tests._helpers import read_file, text_appears

PG_DSN = os.environ.get("MIPYRE_TEST_PG_DSN")

pytestmark = pytest.mark.skipif(
    PG_DSN is None,
    reason="MIPYRE_TEST_PG_DSN not set — skipping Postgres integration tests",
)

PRELUDE = """
import asyncio, sys, time
from mipyre.db import PgPool
pool = PgPool.connect(sys.argv[1])
"""


def _run(body: str, timeout: float = 60) -> str:
    assert PG_DSN is not None, "the module is skipped without MIPYRE_TEST_PG_DSN"
    code = PRELUDE + textwrap.dedent(body)
    out = subprocess.run(
        [sys.executable, "-c", code, PG_DSN],
        capture_output=True, text=True, timeout=timeout,
        env=dict(os.environ, RUST_BACKTRACE="0", MIPYRE_LOG="1"),
    )
    text = out.stdout + out.stderr
    assert out.returncode == 0, text[-4000:]
    assert [line for line in text.splitlines() if "panicked at" in line] == [], text[-4000:]
    return text


def test_results_of_every_method():
    out = _run("""
        async def main():
            assert await pool.fetch_one_async("SELECT 1 AS a, $1::text AS b", "x") == {"a": 1, "b": "x"}
            assert await pool.fetch_one_async("SELECT 1 WHERE false") is None
            assert await pool.fetch_all_async("SELECT generate_series(1, 3) AS i") == [
                {"i": 1}, {"i": 2}, {"i": 3}]
            assert await pool.fetch_scalar_async("SELECT 41 + $1::int", 1) == 42
            # A pooled connection per statement: a TEMP table would be invisible to the next one.
            await pool.execute_async("DROP TABLE IF EXISTS m1_async_t")
            await pool.execute_async("CREATE TABLE m1_async_t (x int)")
            assert await pool.execute_async("INSERT INTO m1_async_t VALUES (1), (2)") == 2
            await pool.execute_async("DROP TABLE m1_async_t")
            print("RESULTS_OK")
        asyncio.run(main())
    """)
    assert "RESULTS_OK" in out


def test_error_becomes_runtime_error_with_the_database_message():
    out = _run("""
        async def main():
            try:
                await pool.fetch_all_async("SELECT * FROM no_such_table_m1")
            except RuntimeError as e:
                assert "no_such_table_m1" in str(e), str(e)
                print("ERROR_OK")
        asyncio.run(main())
    """)
    assert "ERROR_OK" in out


def test_many_concurrent_awaits():
    out = _run("""
        async def main():
            rows = await asyncio.gather(*(
                pool.fetch_scalar_async("SELECT $1::int * 2", i) for i in range(200)))
            assert rows == [i * 2 for i in range(200)], rows[:5]
            print("CONCURRENT_OK")
        asyncio.run(main())
    """)
    assert "CONCURRENT_OK" in out


def test_no_running_loop_is_an_error_not_a_hang():
    out = _run("""
        try:
            pool.fetch_all_async("SELECT 1")
        except RuntimeError as e:
            assert "running event loop" in str(e), str(e)
            print("NO_LOOP_OK")
    """)
    assert "NO_LOOP_OK" in out


# The advisory lock the in-flight query of the cancel case waits on. Per database, and each xdist worker has its own.
_CANCEL_LOCK = 4410802

_CANCEL_IN_FLIGHT = PRELUDE + """
import gc, weakref
key = int(sys.argv[2])
WAITING = (
    "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND objid::bigint = $1::bigint"
    " AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
)

async def main():
    task = asyncio.ensure_future(
        pool.fetch_one_async("SELECT 1 FROM (SELECT pg_advisory_xact_lock($1::bigint)) AS got", key))
    # In flight: the server has the query, waiting on the lock the holder process has.
    while await pool.fetch_scalar_async(WAITING, key) != 1:
        await asyncio.sleep(0.005)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert task.cancelled()
    # The aborted query released its connection: the pool still serves.
    assert await pool.fetch_scalar_async("SELECT 7") == 7
    # The query's task holds the future until it has delivered to it. It goes while the lock is
    # still held, so the query cannot have finished: the cancel aborted it. A query left running
    # would deliver only once the holder ends, after this.
    gone = weakref.ref(task)
    del task
    while gone() is not None:
        gc.collect()
        await asyncio.sleep(0.005)
    print("CANCEL_OK", flush=True)

errors = []
loop = asyncio.new_event_loop()
loop.set_exception_handler(lambda l, ctx: errors.append(ctx.get("message")))
loop.run_until_complete(main())
loop.close()
assert errors == [], errors
"""


def test_cancel_while_query_in_flight(tmp_path):
    """Cancelling the awaiting task cancels it and aborts the query's task; the late result
    must not be set on the cancelled future, and the pool keeps working.

    The query waits on an advisory lock another process holds until the case is over: it is in
    flight while the server lists it waiting, and it cannot finish before the cancel or before
    its task lets go of the future, so both are seen while the query could not have ended."""
    env = dict(os.environ, RUST_BACKTRACE="0", MIPYRE_LOG="1")
    key = str(_CANCEL_LOCK)
    holder_out = tmp_path / "holder.log"
    with open(holder_out, "w") as h_log:
        holder = subprocess.Popen([sys.executable, "-c", _LOCK_HOLDER, PG_DSN, key], stdin=subprocess.PIPE,
                                  stdout=h_log, stderr=subprocess.STDOUT, text=True, env=env)
        try:
            text_appears(read_file(str(holder_out)), "LOCKED", holder)
            out = subprocess.run([sys.executable, "-c", _CANCEL_IN_FLIGHT, PG_DSN, key],
                                 capture_output=True, text=True, env=env, timeout=None)
            holder.communicate("\n")
        finally:
            if holder.poll() is None:
                holder.kill()
                holder.wait()
    text = out.stdout + out.stderr
    assert out.returncode == 0, text[-4000:]
    assert [line for line in text.splitlines() if "panicked at" in line] == [], text[-4000:]
    assert "CANCEL_OK" in text
    assert "InvalidStateError" not in text, text[-2000:]


# The advisory lock the closed-loop case's query waits on. Per database, and each xdist worker has its own.
_CLOSED_LOOP_LOCK = 4410801

_LOCK_HOLDER = """
import sys
from mipyre.db import PgPool
pool = PgPool.connect(sys.argv[1], max_connections=1)
pool.fetch_one("SELECT 1 FROM (SELECT pg_advisory_lock($1::bigint)) AS held", int(sys.argv[2]))
print("LOCKED", flush=True)
sys.stdin.readline()   # the lock goes with this session when the process ends
"""

_CLOSED_LOOP = PRELUDE + """
from mipyre.engine import init_logger
init_logger("WARN", False, "text")
loop = asyncio.new_event_loop()
async def start():
    # Waits on the lock the holder process has: its result comes only once the test ends the holder.
    return pool.fetch_one_async("SELECT 1 FROM (SELECT pg_advisory_xact_lock($1::bigint)) AS got", int(sys.argv[2]))
fut = loop.run_until_complete(start())
loop.close()
print("LOOP_CLOSED", flush=True)
sys.stdin.readline()   # the test has seen the dropped result's log line
async def after():
    return await pool.fetch_scalar_async("SELECT 9")
assert asyncio.run(after()) == 9
print("CLOSED_LOOP_OK", flush=True)
"""


def test_closed_loop_drops_the_result_and_says_so(tmp_path):
    """The loop is closed before the result arrives: delivery can't reach it, the process
    neither crashes nor hangs, the failure is logged, and later queries on a new loop work.

    The query waits on an advisory lock another process holds, and that process ends only after
    the loop is closed: the result arrives after the loop is gone by construction, not by a sleep
    outlasting a `pg_sleep`, which on a fast machine could deliver first."""
    env = dict(os.environ, RUST_BACKTRACE="0", MIPYRE_LOG="1")
    key = str(_CLOSED_LOOP_LOCK)
    holder_out, out_path = tmp_path / "holder.log", tmp_path / "closed_loop.log"
    with open(holder_out, "w") as h_log, open(out_path, "w") as c_log:
        holder = subprocess.Popen([sys.executable, "-c", _LOCK_HOLDER, PG_DSN, key], stdin=subprocess.PIPE,
                                  stdout=h_log, stderr=subprocess.STDOUT, text=True, env=env)
        child = None
        try:
            text_appears(read_file(str(holder_out)), "LOCKED", holder)
            child = subprocess.Popen([sys.executable, "-c", _CLOSED_LOOP, PG_DSN, key], stdin=subprocess.PIPE,
                                     stdout=c_log, stderr=subprocess.STDOUT, text=True, env=env)
            text_appears(read_file(str(out_path)), "LOOP_CLOSED", child)
            holder.communicate("\n")   # the lock is released: the result arrives now
            text_appears(read_file(str(out_path)), "the asyncio loop can't be reached", child)
            child.communicate("\n")
        finally:
            for proc in (holder, child):
                if proc is not None and proc.poll() is None:
                    proc.kill()
                    proc.wait()
    out = out_path.read_text(errors="replace")
    assert child.returncode == 0, out[-4000:]
    assert [line for line in out.splitlines() if "panicked at" in line] == [], out[-4000:]
    assert "CLOSED_LOOP_OK" in out, out[-3000:]
