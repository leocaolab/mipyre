# Observability

This page covers what mipyre tells you about itself: the metrics endpoint, the logs and the access log, request ids and trace headers, the health probes, and how to send all of it to Prometheus, a log collector, CloudWatch and OpenTelemetry. As on the [Integrations](integrations.md) page, each example ran against the real system, once per runtime (the app's `Mipyre(runtime=...)` set to `subinterp`, `process` and `main` in turn, served with `mipyre run app:app --threads 2 --process-workers 2`), and each output block is what that run printed.

Tested with Prometheus 3.5.0 (`prom/prometheus:v3.5.0`), Vector 0.49.0 (`timberio/vector:0.49.0-debian`), the OpenTelemetry Collector 0.137.0 (`otel/opentelemetry-collector:0.137.0`) with opentelemetry-sdk 1.45.1, and LocalStack 4.7.0 for CloudWatch.

## Results

| What | `subinterp` | `process` | `main` |
|---|---|---|---|
| Prometheus scrapes `/metrics` | yes | yes | yes |
| JSON server and access log lines reach the collector | yes | yes | yes |
| A handler's `logging` lines reach the collector as JSON | yes | yes | yes |
| `X-Request-ID`, `traceparent` or `X-Amzn-Trace-Id` taken as the request id and written on every response | yes | yes | yes |
| The trace header forwarded by the handler on an outbound call | yes | yes | yes |
| OpenTelemetry SDK, `BatchSpanProcessor` | no: the worker doesn't start | yes | yes |
| OpenTelemetry SDK, `SimpleSpanProcessor` | yes | yes | yes |
| `/livez`, `/readyz` | yes | yes | yes |
| Access log sampling | yes | yes | yes |
| Logs to CloudWatch Logs, metrics to CloudWatch, through Vector | yes | yes | yes |

## Metrics

`app.enable_metrics()` serves the Prometheus text format at `GET /metrics` (another path with `enable_metrics(path=...)`). The engine renders the page; it is an ordinary `runtime="main"` route, so the app's `before_request` hooks run for it and can guard it. This is the whole output of a server that answered one `GET /hello` and one `GET /nope` (404), without the `# HELP` lines:

```text
# TYPE mipyre_http_requests_total counter
mipyre_http_requests_total{method="GET",status_class="2xx"} 1
mipyre_http_requests_total{method="GET",status_class="4xx"} 1
# TYPE mipyre_http_request_duration_seconds histogram
mipyre_http_request_duration_seconds_bucket{le="0.0001"} 1
mipyre_http_request_duration_seconds_bucket{le="0.00025"} 2
mipyre_http_request_duration_seconds_bucket{le="0.0005"} 2
mipyre_http_request_duration_seconds_bucket{le="0.001"} 2
mipyre_http_request_duration_seconds_bucket{le="0.0025"} 2
mipyre_http_request_duration_seconds_bucket{le="0.005"} 2
mipyre_http_request_duration_seconds_bucket{le="0.01"} 2
mipyre_http_request_duration_seconds_bucket{le="0.025"} 2
mipyre_http_request_duration_seconds_bucket{le="0.05"} 2
mipyre_http_request_duration_seconds_bucket{le="0.1"} 2
mipyre_http_request_duration_seconds_bucket{le="0.25"} 2
mipyre_http_request_duration_seconds_bucket{le="0.5"} 2
mipyre_http_request_duration_seconds_bucket{le="1"} 2
mipyre_http_request_duration_seconds_bucket{le="2.5"} 2
mipyre_http_request_duration_seconds_bucket{le="5"} 2
mipyre_http_request_duration_seconds_bucket{le="10"} 2
mipyre_http_request_duration_seconds_bucket{le="+Inf"} 2
mipyre_http_request_duration_seconds_sum 0.000235
mipyre_http_request_duration_seconds_count 2
# TYPE mipyre_http_requests_refused_total counter
mipyre_http_requests_refused_total 0
# TYPE mipyre_workers_serving gauge
mipyre_workers_serving 2
# TYPE mipyre_workers_died_total counter
mipyre_workers_died_total{runtime="subinterp"} 0
mipyre_workers_died_total{runtime="process"} 0
# TYPE mipyre_gc_failures_total counter
mipyre_gc_failures_total 0
# TYPE mipyre_workers_retired_total counter
mipyre_workers_retired_total 0
# TYPE mipyre_cpu_degraded_total counter
mipyre_cpu_degraded_total{kind="pin_failed"} 0
mipyre_cpu_degraded_total{kind="qos_failed"} 0
mipyre_cpu_degraded_total{kind="affinity_unreadable"} 0
mipyre_cpu_degraded_total{kind="topology_unreadable"} 0
mipyre_cpu_degraded_total{kind="threads_beyond_cpus"} 0
# TYPE mipyre_drain_timeouts_total counter
mipyre_drain_timeouts_total 0
# TYPE mipyre_drain_closed_connections_total counter
mipyre_drain_closed_connections_total 0
# TYPE mipyre_stop_watcher_failures_total counter
mipyre_stop_watcher_failures_total 0
# TYPE mipyre_blocking_calls_left_total counter
mipyre_blocking_calls_left_total 0
# TYPE mipyre_validation_failures_total counter
mipyre_validation_failures_total 0
# TYPE mipyre_precompressed_fallbacks_total counter
mipyre_precompressed_fallbacks_total 0
# TYPE mipyre_static_unwatched_mounts_total counter
mipyre_static_unwatched_mounts_total 0
# TYPE mipyre_static_answers_cut_total counter
mipyre_static_answers_cut_total 0
# TYPE mipyre_static_bytes_in_flight gauge
mipyre_static_bytes_in_flight 0
# TYPE mipyre_db_results_undeliverable_total counter
mipyre_db_results_undeliverable_total 0
# TYPE mipyre_gil_congested_waits_total counter
mipyre_gil_congested_waits_total 0
# TYPE mipyre_client_disconnects_total counter
mipyre_client_disconnects_total 0
# TYPE mipyre_accept_failures_total counter
mipyre_accept_failures_total 0
# TYPE mipyre_accept_exhausted_total counter
mipyre_accept_exhausted_total 0
# TYPE mipyre_tls_handshake_failures_total counter
mipyre_tls_handshake_failures_total 0
# TYPE mipyre_bad_requests_total counter
mipyre_bad_requests_total{kind="malformed"} 0
mipyre_bad_requests_total{kind="oversized"} 0
# TYPE mipyre_request_head_timeouts_total counter
mipyre_request_head_timeouts_total{kind="first_request"} 0
mipyre_request_head_timeouts_total{kind="next_request"} 0
# TYPE mipyre_forwarded_for_unreadable_total counter
mipyre_forwarded_for_unreadable_total 0
# TYPE mipyre_socket_option_failures_total counter
mipyre_socket_option_failures_total{option="quickack"} 0
mipyre_socket_option_failures_total{option="defer_accept"} 0
mipyre_socket_option_failures_total{option="nodelay"} 0
```

Two groups:

- **Request counters** (`mipyre_http_*`): every response the server gave, by method and status class (`mipyre_http_requests_total`), and a histogram of the time from a request's arrival to its response's head (`mipyre_http_request_duration_seconds`; a streamed body's sending is not in it). The engine counts each response as it leaves, whoever answered it: a handler, a `before_request` hook, a raised exception's 500, or the engine's own 404 or 503. `mipyre_http_requests_refused_total` counts the requests the server answered by itself instead of the handler (503 overloaded or shutting down, 504 past the request budget, 502 a process-lane worker died). `mipyre_validation_failures_total` counts the 422s answered because a `body=` model or `query=` string failed validation. The counts are the server's run: a restart starts them from zero.
- **Server counters** (everything else): the server's own events, process-wide: sub-interpreter workers serving and lost, accept failures, malformed or oversized request heads, request-head timeouts, unreadable `X-Forwarded-For` headers from a trusted proxy, TLS handshake failures, socket options that couldn't be set, CPU pinning that failed, drains that ran out of time. Each `# HELP` line says what one counts.

Every runtime is counted the same way, `runtime="process"` routes included, and so are `/livez`, `/readyz` and the scrapes of `/metrics` themselves (a scrape is counted as it leaves, so the next one shows it). `mipyre_workers_died_total` counts the workers that stopped on their own, by runtime: `runtime="subinterp"` for sub-interpreter workers, `runtime="process"` for worker processes that crashed, were killed or timed out, each of which the server replaces.

### Prometheus

Point a scrape job at the server. Locally, with Prometheus in Docker and the server on the host:

```yaml
global:
  scrape_interval: 2s
scrape_configs:
  - job_name: mipyre
    static_configs:
      - targets: ["host.docker.internal:18000"]
```

The server must listen on an address the container can reach and accept the name Prometheus sends as `Host`: `mipyre run app:app --host 0.0.0.0 --allowed-host host.docker.internal`; without `--allowed-host` every scrape is answered `421`. After the test's requests on `subinterp` (20 to `/hello`, a few others, a `GET /nope` and its own start-up poll, both 404), with Prometheus's scrapes counted too:

```bash
curl -s 'localhost:59090/api/v1/query' --data-urlencode 'query=sum by (status_class) (mipyre_http_requests_total)'
```

```text
[["2xx", "31"], ["4xx", "2"]]
```

(the test printed the `status_class` label and value of each series). On Kubernetes, scrape each pod rather than the Service, so each pod's counters stay separate; see [Kubernetes](kubernetes.md#prometheus).

## Logs

The server writes one JSON object per line to standard output. By default it logs errors only, with no access log; `app.enable_access_log()` turns the access log on and raises the level to INFO. The other settings (`log_config`, `MIPYRE_LOG=1`, `debug=True`) are in the [fortune codelab](codelabs/fortune.md), step 10. Every run on this page used `app.enable_access_log()` and the default JSON format.

Every line has `timestamp`, `level`, `fields` (with `message`) and `target`. The targets seen in the runs:

| `target` | What |
|---|---|
| `mipyre::server` | startup, shutdown, worker and degradation events |
| `mipyre::access` | the access log: one line per response, with `request_id` and `client_ip` |
| `mipyre::handler` | a request that failed: `request_id`, `method`, `path`, `error`, `traceback` |
| `mipyre::app` | your code's `logging` calls, with `logger`, `file`, `line`; `worker` on a sub-interpreter, `lane` in a `runtime="process"` worker |

Lines as they came out of a `subinterp` run with `app.enable_request_id_header()` (a request with `X-Request-ID: abc-123`, a `logging.getLogger("devex").warning(...)` in the handler, and the access line; then a `GET /nope`):

```text
{"timestamp":"2026-10-22T00:01:46.892043Z","level":"WARN","fields":{"message":"handled request_id=abc-123","worker":5,"logger":"devex","file":".../obs_app.py","line":9},"target":"mipyre::app"}
{"timestamp":"2026-10-22T00:01:46.892064Z","level":"INFO","fields":{"message":"Request handled","method":"GET","path":"/hello","status":200,"latency_us":124,"source":"tpc-inline","request_id":"abc-123","client_ip":"127.0.0.1"},"target":"mipyre::access"}
{"timestamp":"2026-10-22T00:01:46.900025Z","level":"INFO","fields":{"message":"Request handled","method":"GET","path":"/nope","status":404,"latency_us":2,"source":"rust","request_id":"7699cd7fd48e4f9e0000000000000001","client_ip":"127.0.0.1"},"target":"mipyre::access"}
```

Every access line has `request_id`, the id the response's request-id header and the handler's `req.request_id` carry (the client's own with `enable_request_id_header()`, such as a `traceparent`, else one the server made), so a request's access line, its handler's lines and a 5xx's `request failed` line are found by one id. `client_ip` is the address of the connection's peer: behind a proxy or load balancer, that is the proxy; the client's own address is in the header the proxy adds (`X-Forwarded-For`), which the access line doesn't read.

and a failed request:

```text
{"timestamp":"2026-10-21T12:38:07.582751Z","level":"ERROR","fields":{"message":"request failed","request_id":"bd30be90c28ff87b0000000000000001","method":"GET","path":"/check_async","error":"the request's async task raised NotImplementedError: fetch_all_async is not available in sub-interpreter workers; use fetch_all (sync) or route with runtime='main'","traceback":"Traceback (most recent call last):\n ..."},"target":"mipyre::handler"}
```

A `runtime="process"` worker that dies is replaced, and each death is one `WARN` line on `mipyre::server` with the lane, the pid, how the process ended and how long it served. After `kill -9` of a worker on a `process` run (macOS, where the OS names the signal `Killed: 9`):

```text
{"timestamp":"2026-10-21T21:11:30.234381Z","level":"WARN","fields":{"message":"process lane 1: worker 24707 died (killed by signal 9 (Killed: 9)) after serving 106.9ms; the lane forks a replacement","lane":1,"pid":24707,"exit":"killed by signal 9 (Killed: 9)","lived_ms":106},"target":"mipyre::server"}
```

A lane whose workers keep dying without answering waits longer between replacements and logs an `ERROR` that it is crash-looping.

A request that fails `body=` or `query=` validation is answered 422 and counted in `mipyre_validation_failures_total`; its warning line (target `mipyre::app`, logger `mipyre.validation`) goes out at most once a minute for the whole server, so a client sending bad bodies can't flood the log. The first reads `a request failed validation with 1 error(s) (0 more since the last line)`; the next, a minute later at the earliest, says how many 422s went unlogged in between. On `runtime="process"` routes the validation runs in the worker process, so those 422s are in the `4xx` of `mipyre_http_requests_total` but not in `mipyre_validation_failures_total`.

The startup banner (`Mipyre v… [TPC mode, …]`, `Listening on …`) is also printed as plain text lines; a collector that parses JSON drops them, and the same facts are in the `Mipyre started` JSON line.

### Access log sampling

```python
app.enable_access_log(sample=10, always_log_status=400)
```

logs about 1 in 10 responses, and every response with status 400 or higher. 200 requests to a 200 route and 20 to a missing path gave these access lines by status:

```text
subinterp  {404: 23, 200: 20}
process    {404: 22, 200: 20}
main       {404: 22, 200: 20}
```

The 404 count includes the test's own few startup polls.

### Sending logs to a collector

Run the server with its output going where the collector reads it: a file, journald, or the container runtime's log on Kubernetes. Vector, reading a file and parsing each line as JSON:

```yaml
sources:
  mipyre:
    type: file
    include: ["/logs/runs/*.log"]
    read_from: beginning
transforms:
  parsed:
    type: remap
    inputs: ["mipyre"]
    drop_on_abort: true
    source: |
      run = .file
      parsed, err = parse_json(.message)
      if err != null { abort }
      . = object(parsed) ?? {"value": parsed}
      .run = run
sinks:
  collected:
    type: file
    inputs: ["parsed"]
    path: /logs/collected.ndjson
    encoding: {codec: json}
```

An access line as Vector wrote it, from a `subinterp` run:

```text
{'latency_us': 251, 'message': 'Request handled', 'method': 'GET', 'path': '/hello', 'source': 'tpc-inline', 'status': 200}
```

(the `fields` object of the collected line; this run predates the access line's `request_id` and `client_ip`, which a current line also has, as above). `drop_on_abort` drops the non-JSON lines described above; route them to another sink instead if you need them.

## Request ids and trace headers

Every request has an id, `req.request_id`. The server makes one up (32 hex digits) unless `app.enable_request_id_header(header)` is on and the client sent a usable value in that header: 1 to 128 characters of letters, digits and `- _ . : / + = ; @ ~`. Either way, a 5xx response, its log line and the access line carry the same id. With `enable_request_id_header()` the engine also writes the id in that header on every response as it leaves, whoever answered: a handler, a `before_request` hook, a raised exception's 500, the engine's own 404 or 503. A value the handler set in that header is replaced.

```python
app = Mipyre()
app.enable_request_id_header("traceparent")   # or "X-Request-ID" (the default), "X-Amzn-Trace-Id", ...


@app.get("/hello")
def hello(req):
    return {"request_id": req.request_id}
```

All three headers were tested on all three runtimes, with the same result:

```text
GET /hello (traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01): 200 {"request_id":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", ...}
response header traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
GET /hello (X-Amzn-Trace-Id: Root=1-67891233-abcdef012345678912345678;Parent=53995c3f42cd8ad8;Sampled=1): 200 {"request_id":"Root=1-67891233-abcdef012345678912345678;Parent=53995c3f42cd8ad8;Sampled=1", ...}
response header X-Amzn-Trace-Id: Root=1-67891233-abcdef012345678912345678;Parent=53995c3f42cd8ad8;Sampled=1
GET /hello (X-Request-ID: devex-client-id-1): 200 {"request_id":"devex-client-id-1", ...}
response header X-Request-ID: devex-client-id-1
GET /nope: 404, response header X-Request-ID: 6b1745d120965b9c0000000000000005
```

Without the header, `req.request_id` is the server's own, such as `6b1745d120965b9c0000000000000001`, and the response carries that one.

Writing `traceparent` back unchanged means the response carries the caller's span id, not one of yours; for real distributed tracing use OpenTelemetry (below) and treat the request id as a correlation id.

**Forwarding.** mipyre doesn't add trace headers to outbound calls. Forward the one you got:

```python
import httpx

client = httpx.Client(timeout=5)
TRACE_HEADER = "traceparent"


@app.get("/propagate")
def propagate(req):
    incoming = req.headers.get(TRACE_HEADER.lower())
    r = client.get(ECHO_URL, headers={TRACE_HEADER: incoming} if incoming else {})
    return {"incoming": incoming, "downstream_saw": r.json().get(TRACE_HEADER.lower())}
```

with `ECHO_URL` a server that answers with the headers it received:

```text
200 {"incoming":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","downstream_saw":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
```

## OpenTelemetry

The OpenTelemetry SDK runs inside handlers like any library. Set it up at module level, so each worker has its own provider, and continue the caller's trace from the request headers:

```python
import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

from mipyre import Mipyre

app = Mipyre()
provider = TracerProvider(resource=Resource.create({"service.name": "mipyre-devex"}))
provider.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter(endpoint="http://127.0.0.1:54318/v1/traces")))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("devex")


@app.get("/check")
def check(req):
    with tracer.start_as_current_span("GET /check", context=extract(dict(req.headers.items()))) as span:
        ctx = span.get_span_context()
        return {"trace_id": format(ctx.trace_id, "032x"), "span_id": format(ctx.span_id, "016x"), "pid": os.getpid()}
```

A request with `traceparent: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01` answered, on each runtime, with the caller's trace id and a new span id:

```text
200 {"trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"072cefde3627c036","pid":97760}
```

and the collector (debug exporter) printed `Trace ID       : 0af7651916cd43dd8448eb211c80319c` for each span.

**Choose the span processor by runtime.** The usual `BatchSpanProcessor` exports from a daemon thread, and a sub-interpreter refuses to start one. With it, on `subinterp` the server doesn't start:

```text
worker 0: .../otel_app.py raised while the worker started: RuntimeError: daemon threads are disabled in this (sub)interpreter
```

`SimpleSpanProcessor`, as above, exports on the request's own thread when the span ends (one HTTP request to the collector per span, inside the request's time), and works on every runtime. `BatchSpanProcessor` works on `process` and `main`.

**Flushing at shutdown.** `@app.on_shutdown` runs once, on the main interpreter of the server process, not in each sub-interpreter or worker process:

```text
subinterp: requests answered in (pid 4833, interp 1); on_shutdown ran in pid 4833 interp 0
process:   requests answered in pid 4840;           on_shutdown ran in pid 4837 interp 0
```

So `provider.shutdown()` in an `on_shutdown` hook doesn't flush a worker's provider. With `BatchSpanProcessor` on `process`, spans still in the batch when the server stops are lost: a run that stopped the server right after its requests delivered none of them to the collector, and one that waited 2 s first delivered them. `SimpleSpanProcessor` has nothing to lose.

## Health probes

```python
from mipyre import HealthProbes

probes = HealthProbes(app)


@probes.check("db")
def database_ready():
    pool.fetch_scalar("SELECT 1")
```

`GET /livez` answers `200 {"status": "alive"}` without entering Python. `GET /readyz` runs every readiness check, on the main interpreter, and answers `200 {"status": "ready", "checks": {...}}`, one entry per check, when all pass. With a check that raises:

```python
@probes.check("db")
def db_ready():
    raise ConnectionError("db at 10.0.0.9:5432 refused the connection")
```

three probes in a row answered, on each runtime:

```text
GET /livez: 200 {"status": "alive"}
GET /readyz #1: 503 {"status": "not_ready", "checks": {"db": {"ok": false}}, "request_id": "178b6f0e74f708670000000000000000"}
GET /readyz #2: 503 {"status": "not_ready", "checks": {"db": {"ok": false}}, "request_id": "178b6f0e74f708670000000000000000"}
GET /readyz #3: 503 {"status": "not_ready", "checks": {"db": {"ok": false}}, "request_id": "178b6f0e74f708670000000000000000"}
```

The reason is not in the response, which anyone who can reach the port can read. It is in the log, once, under the `request_id` the response gives:

```text
{"timestamp":"2026-10-21T13:17:00.269219Z","level":"ERROR","fields":{"message":"readiness check 'db' raised (request_id=178b6f0e74f708670000000000000000; 0 failing probes not logged before)\nTraceback (most recent call last):\n ... ConnectionError: db at 10.0.0.9:5432 refused the connection\n", ...}
```

Probes that come quickly get the same answer and the same `request_id` without running the checks again.

Probe settings for Kubernetes are in [Kubernetes](kubernetes.md#deployment-service-and-probes).

## CloudWatch Logs and metrics

Vector can send the same parsed log lines to CloudWatch Logs, and the `/metrics` counters to CloudWatch metrics, with no code in the app. Add to the Vector configuration above (start the server with `--allowed-host host.docker.internal`, as for Prometheus; here pointed at LocalStack; on AWS, leave out `endpoint` and `auth` and give Vector an IAM role):

```yaml
sources:
  scraped:
    type: prometheus_scrape
    endpoints: ["http://host.docker.internal:18000/metrics"]
    scrape_interval_secs: 2
sinks:
  cloudwatch:
    type: aws_cloudwatch_logs
    inputs: ["parsed"]
    endpoint: http://localstack:4566
    region: us-east-1
    group_name: mipyre
    stream_name: devex
    create_missing_group: true
    create_missing_stream: true
    encoding: {codec: json}
    auth:
      access_key_id: test
      secret_access_key: test
  cloudwatch_metrics:
    type: aws_cloudwatch_metrics
    inputs: ["scraped"]
    endpoint: http://localstack:4566
    region: us-east-1
    default_namespace: mipyre
    auth:
      access_key_id: test
      secret_access_key: test
```

Read back with boto3 after the runs on all three runtimes:

```python
import json
from datetime import UTC, datetime, timedelta

import boto3

session = boto3.Session(aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1")
logs = session.client("logs", endpoint_url="http://127.0.0.1:54566")
cloudwatch = session.client("cloudwatch", endpoint_url="http://127.0.0.1:54566")

streams = logs.describe_log_streams(logGroupName="mipyre")["logStreams"]
print("log streams:", [s["logStreamName"] for s in streams])
events = [json.loads(e["message"]) for e in logs.filter_log_events(logGroupName="mipyre")["events"]]
access = [e["fields"] for e in events if e.get("target") == "mipyre::access"]
print("events in CloudWatch Logs:", len(events), "of which access lines:", len(access), "first:", access[:1])

names = sorted({m["MetricName"] for m in cloudwatch.list_metrics(Namespace="mipyre")["Metrics"]})
print("CloudWatch metrics in namespace mipyre:", len(names), [n for n in names if n.startswith("mipyre_http")])
series = cloudwatch.list_metrics(Namespace="mipyre", MetricName="mipyre_http_requests_total")["Metrics"]
for dimensions in sorted((m["Dimensions"] for m in series), key=str):
    stats = cloudwatch.get_metric_statistics(
        Namespace="mipyre", MetricName="mipyre_http_requests_total", Dimensions=dimensions,
        StartTime=datetime.now(UTC) - timedelta(hours=2), EndTime=datetime.now(UTC), Period=7200, Statistics=["Sum", "SampleCount"],
    )["Datapoints"]
    labels = {d["Name"]: d["Value"] for d in dimensions}
    print("mipyre_http_requests_total", labels, [{k: p[k] for k in ("Sum", "SampleCount")} for p in stats])
```

```text
log streams: ['devex']
events in CloudWatch Logs: 518 of which access lines: 317 first: [{'latency_us': 167, 'message': 'Request handled', 'method': 'GET', 'path': '/__devex_not_a_route', 'source': 'rust', 'status': 404}]
CloudWatch metrics in namespace mipyre: 22 ['mipyre_http_requests_refused_total', 'mipyre_http_requests_total']
mipyre_http_requests_total {'method': 'GET', 'status_class': '2xx'} [{'Sum': 36.0, 'SampleCount': 18.0}]
mipyre_http_requests_total {'method': 'GET', 'status_class': '4xx'} [{'Sum': 0.0, 'SampleCount': 26.0}]
```

The access line read back predates the line's `request_id` and `client_ip` fields. The labels become dimensions, so a query names them. The latency histogram doesn't reach CloudWatch through this sink: of the `mipyre_http_*` metrics, only the two counters do.

This read-back used boto3 1.37: with LocalStack 4.7, newer boto3 versions fail on `ListMetrics` (see [Integrations](integrations.md#running-localstack)). Vector's own `PutMetricData` and `PutLogEvents` calls all succeeded (`AWS cloudwatch.PutMetricData => 200` in LocalStack's log).
