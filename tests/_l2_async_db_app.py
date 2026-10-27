"""`PgPool.*_async` on main while workers execute the
real engine.

Like `_l2_main_side_app.py`, every worker imports the real `mipyre.engine`, so more
than one interpreter has executed it and the PyO3 fork refuses a bare foreign-thread attach.
A `*_async` future resolved with such an attach on a Tokio thread would never complete. Two
callers are driven here:

- `/adb`: an async `runtime="main"` route awaiting `fetch_all_async`;
- a thread on the main interpreter running its own asyncio loop that awaits
  `fetch_all_async` in a loop; `/bg` reports its successes and failures.
"""
import os
from typing import Any

import mipyre.engine as _engine
from mipyre import Mipyre, ServerFlags

IN_WORKER = _engine._in_worker()

app = Mipyre()

if not IN_WORKER:
    import asyncio
    import threading

    from mipyre.db import PgPool

    pool = PgPool.connect(os.environ["MIPYRE_TEST_PG_DSN"])
    background: dict[str, Any] = {"ok": 0, "errors": []}

    def _background() -> None:
        async def run() -> None:
            n = 0
            while True:
                n += 1
                try:
                    rows = await pool.fetch_all_async("SELECT $1::int AS n", n)
                    assert rows == [{"n": n}], rows
                    background["ok"] += 1
                except Exception as e:  # noqa: BLE001 — reported through /bg
                    background["errors"].append(repr(e))
                await asyncio.sleep(0.001)

        asyncio.run(run())

    threading.Thread(target=_background, daemon=True).start()


@app.get("/w")
def worker_route(req):
    # Served by a worker, which runs the real engine.
    return {"real_engine": _engine._in_worker() and hasattr(_engine, "_worker_take")}


@app.get("/adb", runtime="main")
async def async_db(req):
    rows = await pool.fetch_all_async("SELECT 2 AS two, $1::text AS s", "x")
    return {"rows": rows}


@app.get("/bg", runtime="main")
def background_state(req):
    return {"ok": background["ok"], "errors": background["errors"][:5]}


if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=int(os.environ["L2_PORT"])))
