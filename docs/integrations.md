# Integrations

This page shows how to use mipyre with databases, caches, outbound HTTP, AWS, MCP and RPC clients, and a reverse proxy, and in which runtime each one works. Every code block on this page ran on a developer machine against real services before it was written down, and every output block is what that run printed. Where something did not work, the error is quoted as it came out, under [Not supported yet](#not-supported-yet).

Monitoring, logs, metrics and tracing are on their own page: [Observability](observability.md). Running on Kubernetes is in [Kubernetes](kubernetes.md).

## How the tests were run

Each app on this page was started three times, once per runtime: with the app's `Mipyre(runtime=...)` set to `subinterp`, `process` and `main` in turn, which sets the runtime of every route that doesn't declare its own (see the [fortune codelab](codelabs/fortune.md), step 9). Each run was served with:

```bash
mipyre run app:app --threads 2 --process-workers 2 --port 18000
```

The services ran in Docker on the same machine:

| Service | Image |
|---|---|
| PostgreSQL primary and a streaming replica | `postgres:17.6` |
| Redis | `redis:7.4.5` |
| MySQL | `mysql:8.4.6` |
| nginx | `nginx:1.29.1` |
| AWS (S3, SQS, DynamoDB, Secrets Manager, CloudWatch) | `localstack/localstack:4.7.0` |

Python 3.14.7, on macOS (arm64) for the local runs and on Linux (`python:3.14.0-slim-bookworm`, arm64) where a section says so. Client libraries: redis-py 8.1.0, PyMySQL 1.2.3, mysqlclient 2.3.0, SQLAlchemy 2.1.5 with pg8000 1.31.5, httpx 0.28.1, boto3 1.43.116, mcp 2.3.0, msgpack 1.2.3, betterproto 2.0.0b7.

## Results

| System | `subinterp` | `process` | `main` |
|---|---|---|---|
| PostgreSQL, `PgPool`, primary + read replica | yes | no: `PgPool` raises `RuntimeError` | yes |
| `PgPool` `*_async` methods | no: `NotImplementedError` | no: `async def` refused at startup | yes |
| Redis (redis-py) | yes | yes | yes |
| MySQL, PyMySQL | yes | yes | yes |
| MySQL, mysqlclient | no: can't load in a sub-interpreter | yes | yes |
| SQLAlchemy 2.1 | no: can't load in a sub-interpreter | yes | yes |
| httpx, sync `Client` | yes | yes | yes |
| httpx, `AsyncClient` in an `async def` handler | yes | no: `async def` refused at startup | yes |
| boto3: S3, SQS, DynamoDB, Secrets Manager | yes | yes | yes |
| MCP client (official `mcp` SDK) to `MCPServer` | yes¹ | yes¹ | yes |
| `RPCClient`, JSON and MsgPack | yes¹ | yes¹ | yes |
| RPC with a Protobuf request body | yes¹ | yes¹ | yes |
| `RPCClient` with Protobuf | no: `RPCClient` has no Protobuf mode | no | no |
| Real client address behind nginx | with `trusted_proxies`; without it, `req.client_ip` is the proxy | the same | the same |

¹ The MCP endpoint and RPC methods always run on the main interpreter, whatever the app's runtime; the app's other routes follow `Mipyre(runtime=...)`.

Tracing (`traceparent`, `X-Amzn-Trace-Id`, OpenTelemetry), Prometheus, log collectors and CloudWatch are in the results table of [Observability](observability.md#results).

## One client per worker

A sub-interpreter worker executes your app's module itself, and so does each process worker. A client you create at module level therefore exists once per worker: each sub-interpreter and each worker process builds its own connection pool, and no client object is ever shared between two interpreters. You don't need to do anything for this, and you can't share one client between workers.

Within one worker, the client is used by one thread at a time on `subinterp` (each sub-interpreter belongs to one server thread) but by several threads at once on `main` (the main interpreter answers requests on several bridge threads). So use clients that are thread-safe (redis-py, httpx, boto3 clients, SQLAlchemy engines, `PgPool`), or keep one connection per thread with `threading.local()` for drivers that aren't (PyMySQL, mysqlclient).

This is boto3 on Linux, 32 concurrent requests to an app with 4 server threads and 4 process workers, printing which interpreter answered and the `id()` of its S3 client (the app is in [AWS with boto3](#aws-with-boto3)):

```text
== runtime=subinterp
distinct (pid, interp, s3 client id): [(10, 1, 270941267854064), (10, 2, 270941236396784), (10, 3, 270941002629872), (10, 4, 270940971172592)]
== runtime=process
distinct (pid, interp, s3 client id): [(15, 0, 272757075193408), (16, 0, 272757075291376), (17, 0, 272757075193072), (18, 0, 272757075291376)]
== runtime=main
distinct (pid, interp, s3 client id): [(10, 0, 251594975976752)]
```

Four sub-interpreters in one process, four clients; four worker processes, one client each (two processes can print the same `id()`, since each has its own address space); one client on `main`. On macOS every connection in a test like this one tends to land on the same server thread, so a local run shows a single sub-interpreter.

## PostgreSQL with `PgPool`

`PgPool` is mipyre's own PostgreSQL driver. A second DSN, such as a read replica, gets a pool of its own:

```python
import os
import _interpreters

from mipyre import Mipyre
from mipyre.db import PgPool

app = Mipyre()
primary = PgPool.connect("postgres://postgres:devex@127.0.0.1:55432/devex", max_connections=4)
replica = PgPool.connect("postgres://postgres:devex@127.0.0.1:55433/devex", max_connections=4)


@app.get("/check")
def check(req):
    primary.execute("INSERT INTO items VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = $2", 100, "from-primary")
    return {
        "primary_in_recovery": primary.fetch_scalar("SELECT pg_is_in_recovery()"),
        "replica_in_recovery": replica.fetch_scalar("SELECT pg_is_in_recovery()"),
        "replica_rows": replica.fetch_all("SELECT id, name FROM items WHERE id <= $1 ORDER BY id", 3),
        "pid": os.getpid(),
        "interp": _interpreters.get_current()[0],
    }


@app.get("/check_async", runtime="main")
async def check_async(req):
    return {"replica_rows": await replica.fetch_all_async("SELECT id, name FROM items ORDER BY id LIMIT 2")}
```

`GET /check` on `subinterp` and on `main`:

```text
200 {"primary_in_recovery":false,"replica_in_recovery":true,"replica_rows":[{"id":1,"name":"alpha"},{"id":2,"name":"beta"},{"id":3,"name":"gamma"}],"pid":97422,"interp":1}
```

`GET /check_async` answers on every runtime, because the route declares `runtime="main"`:

```text
200 {"replica_rows":[{"id":1,"name":"alpha"},{"id":2,"name":"beta"}]}
```

Every interpreter that connects to the same DSN shares that DSN's pool, so the pool is one per process and DSN, not one per worker. Sync calls work on `subinterp` and `main`; the `*_async` calls need `main`. Routing rules:

- Sync `PgPool` calls: put the route on `subinterp` (the default) or `main`.
- `*_async` calls: declare the route `runtime="main"`.
- `runtime="process"`: `PgPool` can't be used. See [Not supported yet](#not-supported-yet).

## Redis

redis-py is pure Python and works in every runtime. Create the client at module level; it is thread-safe and keeps its own pool:

```python
import os
import _interpreters

import redis

from mipyre import Mipyre

app = Mipyre()
client = redis.Redis(host="127.0.0.1", port=56379)


@app.get("/check")
def check(req):
    n = client.incr("devex:hits")
    client.set("devex:last", f"pid={os.getpid()}")
    return {"hits": n, "last": client.get("devex:last").decode(), "pid": os.getpid(),
            "interp": _interpreters.get_current()[0], "client_id": id(client)}
```

```text
subinterp  200 {"hits":1,"last":"pid=78178","pid":78178,"interp":1,"client_id":4418077088}
process    200 {"hits":18,"last":"pid=80989","pid":80989,"interp":0,"client_id":4371334544}
main       200 {"hits":35,"last":"pid=81098","pid":81098,"interp":0,"client_id":4365066192}
```

## MySQL

Two drivers were tested. **PyMySQL** is pure Python and works in every runtime. **mysqlclient** (`MySQLdb`) is a C extension that can't load in a sub-interpreter; it works on `process` and `main`. Neither driver's connections may be shared between threads, so keep one per thread:

```python
import os
import threading
import _interpreters

import pymysql

from mipyre import Mipyre

app = Mipyre()
_local = threading.local()


def conn():
    c = getattr(_local, "conn", None)
    if c is None:
        c = _local.conn = pymysql.connect(host="127.0.0.1", port=53306, user="root", password="devex", database="devex", autocommit=True)
    return c


@app.get("/check")
def check(req):
    with conn().cursor() as cur:
        cur.execute("SELECT VERSION(), %s + 1", (41,))
        version, answer = cur.fetchone()
    return {"server": version, "answer": answer, "pid": os.getpid(), "interp": _interpreters.get_current()[0]}
```

```text
subinterp  200 {"server":"8.4.6","answer":42,"pid":88896,"interp":1}
process    200 {"server":"8.4.6","answer":42,"pid":88902,"interp":0}
main       200 {"server":"8.4.6","answer":42,"pid":88904,"interp":0}
```

With mysqlclient, replace the import with `import MySQLdb` and the connect call with `MySQLdb.connect(host=..., port=..., user=..., password=..., database=..., autocommit=True)`. On `process` and `main` it answers the same way. On `subinterp` the server doesn't start; the error is under [Not supported yet](#not-supported-yet). The usual fix is to put the MySQL routes in a router of their own with `runtime="process"`, as the error message suggests.

## SQLAlchemy

SQLAlchemy 2.1 ships compiled (Cython) modules that can't load in a sub-interpreter, so it works on `process` and `main`. This app uses the pure-Python pg8000 driver, so that only SQLAlchemy itself is being tested:

```python
import os
import _interpreters

import sqlalchemy
from sqlalchemy import create_engine, text

from mipyre import Mipyre

app = Mipyre()
engine = create_engine("postgresql+pg8000://postgres:devex@127.0.0.1:55432/devex", pool_size=4)


@app.get("/check")
def check(req):
    with engine.connect() as c:
        rows = c.execute(text("SELECT id, name FROM items WHERE id <= :n ORDER BY id"), {"n": 3}).all()
    return {"sqlalchemy": sqlalchemy.__version__, "rows": [list(r) for r in rows],
            "pid": os.getpid(), "interp": _interpreters.get_current()[0]}
```

```text
process  200 {"sqlalchemy":"2.1.5","rows":[[1,"alpha"],[2,"beta"],[3,"gamma"]],"pid":87294,"interp":0}
main     200 {"sqlalchemy":"2.1.5","rows":[[1,"alpha"],[2,"beta"],[3,"gamma"]],"pid":87297,"interp":0}
```

Setting `DISABLE_SQLALCHEMY_CEXT_RUNTIME=1` doesn't help on `subinterp`: the same `ImportError` is raised, because the compiled module is still imported.

## Outbound HTTP with httpx

A module-level `httpx.Client` works in every runtime. `httpx.AsyncClient` works in an `async def` handler, which runs on `subinterp` or `main`:

```python
import os
import _interpreters

import httpx

from mipyre import Mipyre

app = Mipyre()
client = httpx.Client(timeout=5)
TARGET = "http://127.0.0.1:59090/-/ready"


@app.get("/check")
def check(req):
    r = client.get(TARGET)
    return {"status": r.status_code, "body": r.text.strip(), "pid": os.getpid(),
            "interp": _interpreters.get_current()[0]}


@app.get("/check_async", runtime="main")
async def check_async(req):
    async with httpx.AsyncClient(timeout=5) as c:
        r = await c.get(TARGET)
    return {"status": r.status_code, "body": r.text.strip()}
```

```text
subinterp  GET /check: 200 {"status":200,"body":"Prometheus Server is Ready.","pid":87300,"interp":2}
subinterp  GET /check_async: 200 {"status":200,"body":"Prometheus Server is Ready."}
process    GET /check: 200 {"status":200,"body":"Prometheus Server is Ready.","pid":97449,"interp":0}
main       GET /check: 200 {"status":200,"body":"Prometheus Server is Ready.","pid":97451,"interp":0}
```

Without `runtime="main"` on the `async def` route, an app whose `Mipyre(runtime=...)` is `"process"` doesn't start; see [Not supported yet](#not-supported-yet).

Don't call your own server from a handler with a blocking client. With the default thread-per-core server, the handler blocks its thread, and the connection it opens can be accepted by that same thread; the call then times out (`httpx.ReadTimeout: timed out` after the client's 5 s).

## AWS with boto3

boto3 and botocore are pure Python and work in every runtime. A boto3 `Session` isn't thread-safe, but the clients made from it are; build one session and its clients at module level, which gives each worker its own (see [One client per worker](#one-client-per-worker)). Tested against LocalStack:

```python
import json
import os
import threading
import _interpreters

import boto3

from mipyre import Mipyre

app = Mipyre()
ENDPOINT = os.environ.get("AWS_ENDPOINT", "http://127.0.0.1:54566")
session = boto3.Session(aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1")
s3 = session.client("s3", endpoint_url=ENDPOINT)
sqs = session.client("sqs", endpoint_url=ENDPOINT)
ddb = session.client("dynamodb", endpoint_url=ENDPOINT)
secrets = session.client("secretsmanager", endpoint_url=ENDPOINT)
QUEUE_URL = sqs.get_queue_url(QueueName="devex")["QueueUrl"]


@app.get("/check")
def check(req):
    me = f"{os.getpid()}-{_interpreters.get_current()[0]}-{threading.get_ident()}"
    s3.put_object(Bucket="devex", Key=f"k/{me}", Body=b"hello")
    body = s3.get_object(Bucket="devex", Key=f"k/{me}")["Body"].read().decode()
    sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=me)
    ddb.put_item(TableName="devex", Item={"id": {"S": me}, "n": {"N": "1"}})
    item = ddb.get_item(TableName="devex", Key={"id": {"S": me}})["Item"]
    secret = json.loads(secrets.get_secret_value(SecretId="devex/db")["SecretString"])
    return {"s3": body, "sqs_sent": True, "dynamodb": item["n"]["N"], "secret_user": secret["user"],
            "pid": os.getpid(), "interp": _interpreters.get_current()[0], "s3_client_id": id(s3)}
```

```text
subinterp  200 {"s3":"hello","sqs_sent":true,"dynamodb":"1","secret_user":"app","pid":94126,"interp":1,"s3_client_id":4595960192}
process    200 {"s3":"hello","sqs_sent":true,"dynamodb":"1","secret_user":"app","pid":94139,"interp":0,"s3_client_id":4413541072}
main       200 {"s3":"hello","sqs_sent":true,"dynamodb":"1","secret_user":"app","pid":94143,"interp":0,"s3_client_id":4405636672}
```

The bucket, queue, table and secret were created beforehand with boto3. In production, leave out `endpoint_url` and the keys, and let boto3 find credentials the usual way (environment, IAM role, IRSA); only LocalStack was tested here.

Sending logs and metrics to CloudWatch is in [Observability](observability.md#cloudwatch-logs-and-metrics).

### Running LocalStack

```yaml
services:
  localstack:
    image: localstack/localstack:4.7.0
    environment:
      SERVICES: s3,sqs,dynamodb,secretsmanager,logs,cloudwatch
    ports: ["127.0.0.1:54566:4566"]
```

To read CloudWatch metrics back from LocalStack 4.7, use boto3 1.37 or earlier: newer botocore talks to CloudWatch in a protocol LocalStack 4.7 rejects, with `InternalError ... Operation detection failed. Missing Action in request for query-protocol service ServiceModel(cloudwatch)`. S3, SQS, DynamoDB, Secrets Manager and CloudWatch Logs work with the current boto3.

## MCP clients

`MCPServer(app)` serves the Model Context Protocol at `/mcp`, always on the main interpreter. The official Python SDK's streamable HTTP client connects to it:

```python
from mipyre import Mipyre
from mipyre.mcp import MCPServer

app = Mipyre()
mcp = MCPServer(app)


@mcp.tool(description="Add two numbers")
def add(a: int, b: int) -> int:
    return a + b
```

```python
import asyncio

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


async def main():
    async with streamable_http_client("http://127.0.0.1:18000/mcp") as streams, ClientSession(streams[0], streams[1]) as s:
        init = await s.initialize()
        print("initialize:", init.server_info.name, "protocol", init.protocol_version)
        print("tools/list:", [t.name for t in (await s.list_tools()).tools])
        result = await s.call_tool("add", {"a": 2, "b": 3})
        print("tools/call add(2, 3):", [c.text for c in result.content])


asyncio.run(main())
```

Output, the same with the app on each of the three runtimes:

```text
initialize: mipyre-mcp protocol 2024-11-05
tools/list: ['add']
tools/call add(2, 3): ['5']
```

## RPC

`@rpc(app)` serves the function at `POST /rpc/<its name>` (`@rpc(app, "name")` names it), always on the main interpreter. `RPCClient` calls it as a method, in JSON or MsgPack. A method with `proto_model=` also accepts a Protobuf request body (decoded with `proto_model().parse(body)`, the betterproto API); the reply is JSON or MsgPack, never Protobuf.

```python
from dataclasses import dataclass

import betterproto

from mipyre import Mipyre, rpc

app = Mipyre()


@dataclass
class AddRequest(betterproto.Message):
    a: int = betterproto.int64_field(1)
    b: int = betterproto.int64_field(2)


@rpc(app)
def add(data):
    return {"sum": data["a"] + data["b"]}


@rpc(app, proto_model=AddRequest)
def add_proto(data):
    return {"sum": data.a + data.b}
```

The client, with the app's module (`rpc_app.py`) importable:

```python
import httpx

from mipyre import RPCClient
from rpc_app import AddRequest

for use_msgpack in (False, True):
    with RPCClient("http://127.0.0.1:18000", use_msgpack=use_msgpack) as client:
        print(f"RPCClient(use_msgpack={use_msgpack}).add(a=2, b=3):", client.add(a=2, b=3))

r = httpx.post("http://127.0.0.1:18000/rpc/add_proto", content=bytes(AddRequest(a=2, b=3)),
               headers={"content-type": "application/x-protobuf"})
print(r.status_code, r.text, r.headers["content-type"])
```

Output, the same with the app on each of the three runtimes:

```text
RPCClient(use_msgpack=False).add(a=2, b=3): {'sum': 5}
RPCClient(use_msgpack=True).add(a=2, b=3): {'sum': 5}
200 {"sum":5} application/json
```

`RPCClient` needs `httpx`, and MsgPack needs `msgpack`.

## Behind nginx

`req.client_ip` is the address of the TCP peer. Behind a reverse proxy, that is the proxy, until the proxy is listed in `trusted_proxies` (below). Unlisted, `X-Forwarded-For` and `Forwarded` reach the handler as ordinary headers. With this nginx configuration in front of the app and no `trusted_proxies`:

```nginx
server {
    listen 80;
    location / {
        proxy_pass http://mipyre.devex:8000;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

nginx sends the upstream's name (`mipyre.devex:8000`) as `Host`, so start the server with `--allowed-host mipyre.devex` (or `MIPYRE_ALLOWED_HOSTS`); otherwise every proxied request is answered `421 Misdirected Request`.

and a handler returning `{"client_ip": req.client_ip, "x_forwarded_for": req.headers.get("x-forwarded-for")}`, a client at `10.244.0.52` got this through nginx (at `10.244.0.62`) and directly, on Kubernetes:

```text
through nginx  {"client_ip":"10.244.0.62","x_forwarded_for":"10.244.0.52"}
directly       {"client_ip":"10.244.0.52","x_forwarded_for":null}
```

The local runs, with nginx in Docker, gave the same picture on all three runtimes: `"client_ip":"127.0.0.1"` (Docker's port forwarder) and `"x_forwarded_for":"172.18.0.1"`.

To have the client's address in `req.client_ip`, list the proxy's address or block: `MIPYRE_TRUSTED_PROXIES=10.244.0.0/16` (or `run(trusted_proxies=[...])`, `mipyre run --trusted-proxy ADDR`, repeatable). A request whose peer is in the list takes its client from `X-Forwarded-For`, read from the right: each listed proxy's entry is skipped, and the first address that is not a listed proxy is the client. With the configuration above that is the `$remote_addr` nginx appended; whatever the client sent in its own `X-Forwarded-For` sits left of it and is never read. A request from an address not in the list keeps the peer as its client, whatever headers it sends, so a client that reaches the port directly cannot claim an address.

Only `X-Forwarded-For` is read, since it is the header nginx, HAProxy, Envoy and the cloud load balancers write by default; `Forwarded` (RFC 7239) is not. If the proxy writes an entry that is not a bare IP address where the client is looked for (`unknown`, an address with a port), the header is ignored, the proxy is the client, and `mipyre_forwarded_for_unreadable_total` counts it; a log line once a minute shows the header's value. The access log's `client_ip` is the TCP peer, the proxy, whatever `trusted_proxies` says.

## Async handlers and the runtimes

`async def` handlers run on `subinterp` (on a per-thread event loop) and on `main`. On `process`, a route with an `async def` handler stops the app at import:

```text
ValueError: GET /check_async: runtime='process' does not run `async def` handlers; use a `def` handler
```

So an app whose default runtime is `"process"` declares its `async def` routes `runtime="main"`, as the examples above do.

## Not supported yet

Each entry gives the error as mipyre or the library printed it (paths shortened to `.../`).

**mysqlclient on `subinterp`.** The server doesn't start:

```text
worker 0: .../mysqlclient_app.py raised while the worker started: ImportError: module MySQLdb._mysql does not support loading in subinterpreters
MySQLdb can't load in a sub-interpreter; imported at .../mysqlclient_app.py:5  import MySQLdb
Routes that use it need one of:
  1. runtime='process': ...
  2. runtime='main': ...
  3. runtime='isolated': ...
```

Use PyMySQL, or put the routes on `process` or `main`.

**SQLAlchemy 2.1 on `subinterp`.** The server doesn't start:

```text
worker 0: .../sqlalchemy_app.py raised while the worker started: ImportError: module sqlalchemy.util._collections_cy does not support loading in subinterpreters
sqlalchemy can't load in a sub-interpreter; imported at .../sqlalchemy_app.py:4  import sqlalchemy
```

`DISABLE_SQLALCHEMY_CEXT_RUNTIME=1` gives the same error. Use `process` or `main`, or `PgPool` on `subinterp`.

**`PgPool` on `process`.** Each query on a process route answers 500, and the log has:

```text
in the process-lane worker: the handler raised RuntimeError: PgPool is not available in a runtime='process' route: the route runs in a worker process of its own, where PgPool would be a copy apart from the server's, silently out of step with it. Use it from a runtime='subinterp' or runtime='main' route.
```

`app.state` raises the same error there. A process route that needs PostgreSQL can open its own connection with a regular driver (pg8000 and SQLAlchemy both work on `process`), or the query can move to a `subinterp` route.

**`PgPool` `*_async` methods outside `main`.** On `subinterp`, the request answers 500 and the log has `NotImplementedError: fetch_all_async is not available in sub-interpreter workers; use fetch_all (sync) or route with runtime='main'`.

**`async def` handlers on `process`.** See [Async handlers and the runtimes](#async-handlers-and-the-runtimes).

**`RPCClient` with Protobuf.** `RPCClient` sends JSON or MsgPack only, and RPC replies are never Protobuf. Send a Protobuf request with any HTTP client, as in [RPC](#rpc).

**FastAPI, ASGI and WSGI apps.** Not yet; planned. mipyre serves its own `Mipyre` app only: an ASGI app (FastAPI, Starlette) or a WSGI app (Flask, Django) can't be mounted in it.

## Run the services yourself

A Compose file for the services on this page:

```yaml
services:
  pg-primary:
    image: postgres:17.6
    environment: {POSTGRES_PASSWORD: devex, POSTGRES_DB: devex}
    command: ["postgres", "-c", "wal_level=replica", "-c", "max_wal_senders=4", "-c", "hot_standby=on"]
    ports: ["127.0.0.1:55432:5432"]
  redis:
    image: redis:7.4.5
    ports: ["127.0.0.1:56379:6379"]
  mysql:
    image: mysql:8.4.6
    environment: {MYSQL_ROOT_PASSWORD: devex, MYSQL_DATABASE: devex}
    ports: ["127.0.0.1:53306:3306"]
  localstack:
    image: localstack/localstack:4.7.0
    environment: {SERVICES: "s3,sqs,dynamodb,secretsmanager,logs,cloudwatch"}
    ports: ["127.0.0.1:54566:4566"]
```

The read replica in the PostgreSQL section was a second `postgres:17.6` container started from `pg_basebackup -h pg-primary -U postgres -D "$PGDATA" -R -X stream`, with `host replication all all scram-sha-256` added to the primary's `pg_hba.conf`, and the table the examples read was created on the primary with `CREATE TABLE items (id int PRIMARY KEY, name text); INSERT INTO items VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');`.
