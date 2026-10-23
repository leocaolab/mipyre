# Kubernetes

This page shows how to build an image, deploy it with probes, roll it out without failed requests, size it with CPU limits, and scrape and collect it, and what happens when a process worker runs out of memory or is killed. Everything here ran on a local [kind](https://kind.sigs.k8s.io/) cluster (kind 0.33.0, Kubernetes 1.37.0, one node with 4 CPUs) on 2026-10-21, and each output block is what the run printed.

## The app

```python
import os
import time

from mipyre import HealthProbes, Mipyre

app = Mipyre()
app.enable_metrics()
HealthProbes(app)
app.enable_request_id_header()
app.enable_access_log(sample=100, always_log_status=400)


@app.get("/")
def index(req):
    return {"pod": os.environ.get("HOSTNAME"), "client_ip": req.client_ip,
            "x_forwarded_for": req.headers.get("x-forwarded-for")}


@app.get("/slow")
def slow(req):
    time.sleep(float(req.query_param("s") or "1"))
    return {"slept": True}


@app.get("/cpu-quota")
def cpu_quota(req):
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            quota = f.read().strip()
    except OSError as e:
        quota = repr(e)
    return {"cpu.max": quota, "os.cpu_count": os.cpu_count(), "affinity": len(os.sched_getaffinity(0))}


@app.get("/grow", runtime="process")
def grow(req):
    hog = bytearray(int(req.query_param("mb") or "64") * 1024 * 1024)
    for i in range(0, len(hog), 4096):
        hog[i] = 1
    return {"held_mb": len(hog) // (1024 * 1024), "pid": os.getpid()}


@app.get("/whoami", runtime="process")
def whoami(req):
    return {"pid": os.getpid()}
```

## The image

The image used for this page (with the `COPY` path of the app shortened here) built mipyre from source in a first stage and installed the wheel in a slim Python 3.14 image:

```dockerfile
FROM python:3.14.0-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential curl ca-certificates pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.1
ENV PATH=/root/.cargo/bin:$PATH
RUN pip install --no-cache-dir maturin==1.9.6
WORKDIR /src
COPY . .
RUN maturin build --release --out /wheels

FROM python:3.14.0-slim-bookworm
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels
WORKDIR /app
COPY app.py /app/app.py
RUN useradd --uid 10001 --create-home app
USER 10001
EXPOSE 8000
CMD ["mipyre", "run", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```

With mipyre from PyPI, the first stage isn't needed: install it with `pip install mipyre` in the second stage instead of the wheel (not what was run here). The image was 249 MB.

```bash
docker build -t mipyre-devex:dev .
kind create cluster --name mipyre-devex
kind load docker-image mipyre-devex:dev --name mipyre-devex
```

**An app with `runtime="process"` routes needs a writable cache directory** (a home, or `XDG_CACHE_HOME`). As it starts the process workers, the server copies the mipyre package into `$XDG_CACHE_HOME/mipyre/run` (default `~/.cache/mipyre/run`), so that the workers keep running the version the server started with even if the installed package changes under it. An app without process routes writes nothing there and needs neither. This page's app has process routes. The first image made the user with `useradd --uid 10001 app`, which creates no home directory, and the pods went into `CrashLoopBackOff` with (the error now also names the variable to set):

```text
mipyre._clone.PrivateCopyError: making the private directory /home/app/.cache/mipyre/run failed: [Errno 13] Permission denied: '/home/app'
```

`useradd --create-home` fixed it. With `securityContext: {readOnlyRootFilesystem: true}` the pods fail the same way:

```text
mipyre._clone.PrivateCopyError: making the private directory /home/app/.cache/mipyre/run failed: [Errno 30] Read-only file system: '/home/app/.cache'
```

and start with an `emptyDir` for the cache: add the `env` entry and `volumeMounts` to the container of the Deployment below, and `volumes` to its pod spec:

```yaml
          env:
            - {name: XDG_CACHE_HOME, value: /cache}
          volumeMounts: [{name: cache, mountPath: /cache}]
      volumes: [{name: cache, emptyDir: {}}]
```

## Deployment, Service and probes

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: devex
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mipyre
  namespace: devex
spec:
  replicas: 2
  selector:
    matchLabels: {app: mipyre}
  strategy:
    type: RollingUpdate
    rollingUpdate: {maxUnavailable: 0, maxSurge: 1}
  template:
    metadata:
      labels: {app: mipyre}
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8000"
        prometheus.io/path: /metrics
    spec:
      terminationGracePeriodSeconds: 30
      containers:
        - name: mipyre
          image: mipyre-devex:dev
          imagePullPolicy: Never
          ports: [{containerPort: 8000, name: http}]
          env:
            - {name: MIPYRE_PROCESS_WORKERS, value: "2"}
          resources:
            requests: {cpu: "1", memory: 256Mi}
            limits: {cpu: "2", memory: 512Mi}
          # Keep serving while the endpoint removal reaches every node's proxy, then SIGTERM.
          lifecycle:
            preStop:
              sleep: {seconds: 5}
          livenessProbe:
            httpGet: {path: /livez, port: http}
            periodSeconds: 5
          readinessProbe:
            httpGet: {path: /readyz, port: http}
            periodSeconds: 2
            failureThreshold: 1
---
apiVersion: v1
kind: Service
metadata:
  name: mipyre
  namespace: devex
spec:
  selector: {app: mipyre}
  ports: [{port: 8000, targetPort: http, name: http}]
```

`/livez` is answered by the server without entering Python, so a pod whose handlers are all busy still passes it; `/readyz` runs the readiness checks (see [Observability](observability.md#health-probes)). After `kubectl apply`, both pods were `Running` and ready within 3 s of starting. From inside a pod:

```text
/livez {"status": "alive"}
/readyz {"status": "ready", "checks": {}}
```

## Rolling updates

On `SIGTERM` the server stops accepting connections, finishes the requests in flight and exits. The test sent requests to the Service from two client pods for 60 s while the Deployment was restarted twice (`kubectl rollout restart`):

- **new connection per request**: 4 threads, 50 requests per second each;
- **keep-alive**: 4 threads, 200 requests per second each, each thread on one connection, opening a new one after a failure.

A failure is counted by where it happened: opening the connection, or a request on an open connection. There were no non-2xx responses in any run.

| Run | New connection per request | Keep-alive |
|---|---|---|
| without `preStop` | 11 132 × 200; connect: 11 `ConnectionRefusedError`, 9 `TimeoutError` | 47 982 × 200; connect: 11 `ConnectionRefusedError`; request: 11 `RemoteDisconnected` |
| with `preStop: sleep 5 s`, first run | 12 004 × 200; no failures | 47 993 × 200; request: 11 `RemoteDisconnected` |
| with `preStop: sleep 5 s`, second run | 12 004 × 200; no failures | 47 993 × 200; request: 11 `RemoteDisconnected` |

Without `preStop`, the pod gets `SIGTERM` while the Service still sends it new connections, which are refused or time out; this is how Kubernetes works, and the `preStop` sleep is the usual fix. With it, the only failures left were on kept-alive connections: when a stopping server closes an idle connection just as the client sends its next request on it, the client sees `RemoteDisconnected: Remote end closed connection without response`, about 5 times per restart at this load. A client that retries idempotent requests on that error recovers; Python's `http.client`, used here, doesn't retry. The full output of a run:

```text
loadgen-new: {"mode": "new", "statuses": {"200": 12004}, "errors": {}}
loadgen-keepalive: {"mode": "keepalive", "statuses": {"200": 47993}, "errors": {"request: RemoteDisconnected: Remote end closed connection without response": 11}}
```

## CPU limits and worker counts

The server reads the container's CPU quota (cgroup `cpu.max`) and sizes both its threads and the process workers from it. With `limits.cpu: "2"` on the 4-CPU node, and without `MIPYRE_THREADS` or `MIPYRE_PROCESS_WORKERS` set:

```text
  Process lanes: 2 workers (template pid 15, timeout 30000 ms, queue 128/worker; ...)
  TPC threads: 2 (CPUs: 2, pinned)
```

and `/cpu-quota` answered `{"cpu.max":"200000 100000","os.cpu_count":4,"affinity":1}`: `os.cpu_count()` still says 4, and each server thread is pinned to one CPU. With the CPU limit removed (`cpu.max` `max 100000`), the same image started 4 of each:

```text
  Process lanes: 4 workers (template pid 15, ...)
  TPC threads: 4 (CPUs: 4, pinned)
```

Set `MIPYRE_THREADS` and `MIPYRE_PROCESS_WORKERS` to override either.

## Prometheus

Scrape each pod, not the Service, so each pod's counters are their own series. With the pod annotations of the Deployment above, a Prometheus in the cluster (with a ServiceAccount allowed to `get`, `list` and `watch` pods) found both pods:

```yaml
global: {scrape_interval: 5s}
scrape_configs:
  - job_name: pods
    kubernetes_sd_configs: [{role: pod, namespaces: {names: [devex]}}]
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: "true"
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
        target_label: __metrics_path__
      - source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_port]
        regex: ([^:]+)(?::\d+)?;(\d+)
        replacement: $1:$2
        target_label: __address__
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
```

```text
mipyre-7674896dd7-hvhhf up
mipyre-7674896dd7-zc27c up
{"metric":{"__name__":"mipyre_http_requests_total","instance":"10.244.0.5:8000","job":"pods","method":"GET","pod":"mipyre-7674896dd7-hvhhf","status_class":"2xx"},"value":[1792609398.895,"50"]}
{"metric":{"__name__":"mipyre_http_requests_total","instance":"10.244.0.6:8000","job":"pods","method":"GET","pod":"mipyre-7674896dd7-zc27c","status_class":"2xx"},"value":[1792609398.895,"48"]}
```

Nothing but the kubelet and Prometheus had called the fresh pods: every response is counted, so these are their `/readyz` (every 2 s) and `/livez` (every 5 s) probes and Prometheus's scrapes of `/metrics` (see [Observability](observability.md#metrics)).

## Collecting logs

The server logs JSON lines to standard output, which the container runtime writes to the node's `/var/log/pods`. A Vector DaemonSet (`timberio/vector:0.49.0-debian`, with a ServiceAccount allowed to `get`, `list` and `watch` pods, namespaces and nodes, and `/var/log` mounted from the host) collected them with:

```yaml
sources:
  pods:
    type: kubernetes_logs
    extra_label_selector: app=mipyre
transforms:
  parsed:
    type: remap
    inputs: [pods]
    drop_on_abort: true
    source: |
      pod = .kubernetes.pod_name
      parsed, err = parse_json(.message)
      if err != null { abort }
      . = object(parsed) ?? {"value": parsed}
      .pod = pod
sinks:
  out:
    type: console
    inputs: [parsed]
    encoding: {codec: json}
```

Over the half hour of these tests it collected `{'mipyre::server': 232, 'mipyre::access': 10729}` lines (the access log sampled 1 in 100), for example:

```text
{"fields": {"latency_us": 418, "message": "Request handled", "method": "GET", "source": "tpc-main-bridge", "path": "/readyz", "status": 200}, "level": "INFO", "pod": "mipyre-6c6f9dd6f8-b2pm8", "target": "mipyre::access", "timestamp": "2026-10-21T12:52:36.243004Z"}
```

Lines that aren't JSON are dropped by this configuration: the startup banner (see [Observability](observability.md#logs)). The `logging` lines of `runtime="process"` handlers are JSON lines of the server's log, with `lane`. On a node without L3 cache information in sysfs (as in kind), each start also logs a `WARN` `could not read the L3 domains; pinning as one domain`.

## Client addresses

Through the Service, `req.client_ip` is the client pod's address (kube-proxy keeps the source address). Behind an ingress or any other proxy it is the proxy's; see [Integrations](integrations.md#behind-nginx).

## When a process worker dies

**Killed.** `kill -9` on one of the two process workers, from inside the pod, while a client in another pod kept calling `/whoami` (a `runtime="process"` route):

```text
/whoami (200, '{"pid":18}', 0.01)
/whoami (200, '{"pid":17}', 0.0)
/whoami (200, '{"pid":18}', 0.0)
killing 17
/whoami (200, '{"pid":33}', 0.01)
/whoami (200, '{"pid":18}', 0.0)
/whoami (200, '{"pid":18}', 0.0)
/ (200, '{"pod":"mipyre-b659ff84c-7prc6","client_ip":"10.244.0.52","x_forwarded_for":null}', 0.0)
```

No request failed, and a new worker (pid 33) took the dead one's place; the pod didn't restart. This run predates the death's record: the server now logs each one once, a `WARN` line with the lane, the pid and how the worker ended, and counts it in `mipyre_workers_died_total{runtime="process"}` ([Observability](observability.md)).

**Out of memory.** With `limits.memory: 512Mi`, a request to `/grow?mb=700` made one process worker allocate 700 MiB:

```text
/whoami (200, '{"pid":17}', 0.01)
/whoami (200, '{"pid":17}', 0.0)
/grow?mb=100 (200, '{"held_mb":100,"pid":17}', 0.02)
/grow?mb=700 ('EXC', "RemoteDisconnected('Remote end closed connection without response')", 0.06)
/whoami ('EXC', "URLError(ConnectionRefusedError(111, 'Connection refused'))", 0.0)
/ ('EXC', "URLError(ConnectionRefusedError(111, 'Connection refused'))", 0.0)
sleep:5
/whoami ('EXC', "URLError(ConnectionRefusedError(111, 'Connection refused'))", 0.0)
/ ('EXC', "URLError(ConnectionRefusedError(111, 'Connection refused'))", 0.0)
sleep:10
/whoami (200, '{"pid":17}', 0.0)
/ (200, '{"pod":"mipyre-b659ff84c-7prc6","client_ip":"10.244.0.52","x_forwarded_for":null}', 0.0)
```

The kernel's out-of-memory killer killed every process in the container, not just the worker that grew (from the node's `dmesg`):

```text
Memory cgroup out of memory: Killed process 115470 (mipyre) total-vm:1816608kB, anon-rss:456960kB, ...
Memory cgroup out of memory: Killed process 115448 (mipyre) total-vm:1692232kB, anon-rss:52940kB, ...
Memory cgroup out of memory: Killed process 115468 (mipyre) total-vm:1096932kB, anon-rss:17308kB, ...
Memory cgroup out of memory: Killed process 115471 (mipyre) total-vm:1099812kB, anon-rss:18696kB, ...
```

Kubernetes then restarted the container: the pod's restart count went up by one, and the pod refused connections until the new container was up: in two runs it answered again after 5 s and after somewhere between 5 and 16 s. Every request in flight on that pod failed, including those on other routes. A process worker therefore isolates a crash (as `kill -9` showed) but not a memory blow-up: size `limits.memory` for all workers at their peak, and keep memory-hungry work bounded inside the handler. mipyre has no per-worker memory limit.
