"""Postgres support for Mipyre handlers.

Thin Python-side re-export of the Rust `PgPool` class. Connect once at startup (module
level), then call `pool.fetch_one(...)`, `.fetch_all(...)`, `.fetch_scalar(...)`,
`.execute(...)` from a handler on the main interpreter or a sub-interpreter worker. A
`runtime="process"` route runs in a worker process of its own, apart from the server's pools:
there every `PgPool` call raises `RuntimeError`. The query runs on a dedicated DB runtime while
the calling thread waits with its GIL released, so other workers make progress. The `*_async`
variants run on the main interpreter (`runtime="main"` routes); in a worker they raise
`NotImplementedError`.

The pools belong to the server run. `PgPool.connect()` connects nothing: it
returns a handle and records what it asked for and which module asked. When a server starts it
opens one pool per DSN from every handle the main interpreter still holds that its own app's
modules made (the module that created the app, its script, its routers), and binds them to it;
another app's handles are left alone; it closes the pools
when it stops. Two calls for one DSN that name different pool settings (`max_connections`,
`acquire_timeout_secs`, `runtime_threads`, `max_lifetime_secs`, `idle_timeout_secs`,
`ping_on_acquire`) stop the start with `ValueError`, every one named at once; a setting left out
takes what another call named. Another DSN (a read replica, a second database) gets a pool of
its own. A worker's `connect()` binds to its run's pool for the DSN, raising `ValueError` if it
names a setting that pool does not have. `runtime_threads` sizes the run's one DB runtime,
which every pool's protocol work and socket I/O runs on; the default is one thread per 4 CPUs
the process may use, at least 2. One handle is one run's at a time: two servers of one app serving at
once that would share a handle stop the second's start.

Outside any server run (a script, a migration) a handle opens a pool of its own on its first
query, raising `ConnectionError` there if it cannot connect, and closes it when dropped.

Connections that died while idle
--------------------------------

The pool does not test a connection before handing it out (`ping_on_acquire=False`):
that test is a round trip to the server on every query, one of three (test, query,
and the test sqlx runs when the connection goes back, which can't be turned off).
Without it a query takes two. The price: when the server or the network ended a pooled
connection while it sat idle (a Postgres restart, `pg_terminate_backend`, a failover,
a NAT or load balancer dropping an idle flow), the first query that gets that
connection fails. It raises `DatabaseError` carrying the error as it came: the
server's message and `.sqlstate` when the server said why (`57P01` "terminating
connection due to administrator command"), or the socket error when it just went away
(`.sqlstate` None). Mipyre does not retry it: the statement may have run, and only
the caller knows whether running it again is safe. The pool closes that connection
and the next query gets a new one. After a server restart, every connection idle at
the time fails this way once.

To make that rarer, connections are closed before they get old or stale:
`max_lifetime_secs` (default 1800, 30 min, sqlx's default) and `idle_timeout_secs`
(default 300, 5 min: what asyncpg does, and under the ~350 s after which NATs and
cloud load balancers commonly drop idle flows). 0 means never.

To go back to testing every connection before use (no query ever gets a dead
connection that the test would have caught, one more round trip per query)::

    pool = PgPool.connect(dsn, ping_on_acquire=True)

Example::

    from mipyre import HTTPException, Mipyre
    from mipyre.db import PgPool

    app = Mipyre()
    pool = PgPool.connect("postgres://localhost/mydb", max_connections=20)

    @app.get("/users/{id}", runtime="main")
    def get_user(req):
        row = pool.fetch_one(
            "SELECT id, name, email FROM users WHERE id = $1",
            int(req.params["id"]),
        )
        if row is None:
            raise HTTPException(404, "no such user")
        return row

Parameters are encoded as the type the statement declares for them (the
server infers it from the SQL), so the same SQL behaves the same whatever
values came first, and ``None`` is a NULL of the right type. Python ↔ Postgres:

    bool                        bool
    int                         int2 / int4 / int8 (range-checked); float4/8, numeric
    float                       float4 / float8; numeric
    str                         text / varchar / char / name, enum labels
    bytes                       bytea
    dict / list                 json / jsonb
    decimal.Decimal             numeric (NaN and ±Infinity included)
    uuid.UUID                   uuid
    datetime.date               date
    datetime.datetime (naive)   timestamp
    datetime.datetime (aware)   timestamptz (read back in UTC)

A value the declared type cannot take (wrong type, out of range, not encodable)
raises ``ParamError``, a subclass of both ``TypeError`` and ``ValueError``,
before the query is sent. An int past 64 bits can be a ``numeric``; for an
integer column it is out of range. A column of any other type — arrays, inet, interval,
citext, … — reads back as its raw binary wire ``bytes``; cast it in SQL
(``col::text``) to get text. An ambiguous parameter such as ``SELECT $1`` is
text; cast it (``$1::int``) to send another type.

A failed query raises ``DatabaseError`` (a ``RuntimeError``) carrying the
server's SQLSTATE in ``.sqlstate`` (``None`` if the server never reported one,
e.g. a pool timeout). Integrity violations raise ``IntegrityError``, duplicate
keys its subclass ``UniqueViolation`` (SQLSTATE 23505).

For large result sets, use `pool.fetch_iter(sql, ...)` to get a
streaming cursor — O(1) memory, rows yielded one at a time; it works in any
interpreter. Returning a streaming *response* (`EventSourceResponse`, a `Stream`) needs
`runtime="main"`, so an export-style handler looks like this:

    @app.get("/export", runtime="main")
    def export(req):
        rows = pool.fetch_iter("SELECT * FROM transactions")
        return EventSourceResponse(ServerSentEvent(json.dumps(row)) for row in rows)

Not supported: transactions; mapping rows to Pydantic models.
"""

from .engine import DatabaseError, IntegrityError, ParamError, PgCursor, PgPool, UniqueViolation

__all__ = ["DatabaseError", "IntegrityError", "ParamError", "PgCursor", "PgPool", "UniqueViolation"]
