"""A runtime="main" route that queries the process-wide pool — for
tests/test_db_pg.py::test_handler_can_query, whose `pool` fixture creates the table.
Workers execute this module too, so it only connects (idempotent) and registers."""

import os

from mipyre import Mipyre
from mipyre.db import PgPool

pool = PgPool.connect(os.environ["MIPYRE_TEST_PG_DSN"])

app = Mipyre()


@app.get("/")
def root(req):
    return "ok"


@app.get("/users/{name}", runtime="main")
def get_user(req):
    return pool.fetch_one(
        "SELECT name, value FROM mipyre_test_rows WHERE name = $1",
        req.params["name"],
    ) or {"error": "not found"}
