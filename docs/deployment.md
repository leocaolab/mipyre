# Deployment

How to run a mipyre app as a service: starting it, stopping it cleanly, health probes, TLS and HTTP/2, and systemd socket activation.

## Starting the server

Use `mipyre run` in production, bound to the interface your load balancer reaches:

```bash
mipyre run app:app --host 0.0.0.0 --port 8000
```

`python app.py` serves the same app, process routes included; `mipyre run` takes the deployment's settings as flags. The target is `module:attr` or `path/to/file.py:attr`; `attr` defaults to `app`. The flags the app declares take their values here too (`mipyre run app:app --db-dsn x`), and arguments it parses itself go after `--` ([configuration.md](configuration.md#configuring-your-own-app)).

The server prints what it started with, here on a 4-CPU Linux machine:

```text
  Mipyre v2.0.0 [TPC mode, hybrid-inline]
  Listening on http://0.0.0.0:8000
  TPC threads: 4 (CPUs: 4, pinned)
  Routes: 1 sub-interp + 1 GIL + 0 process + 0 async
```

An app with process routes also prints a `Process lanes:` line with the workers, the handler timeout and the queue size.

Sizes, TLS and every other setting: [configuration.md](configuration.md).

## Stopping it: graceful shutdown

`SIGTERM` (what systemd and Kubernetes send) and `SIGINT` (Ctrl-C) stop the server the same way:

1. It stops accepting connections and prints `Shutting down gracefully...`.
2. The requests in flight finish and are answered.
3. The `@app.on_shutdown` hooks run, every one of them.
4. The process exits with status 0. If a hook raised, `app.run()` raises its exception (several as one `ExceptionGroup`) once the others have run, and `mipyre run` prints its traceback and exits with status 1. A hook whose failure is to be ignored catches it itself.

`app.shutdown_timeout` (default 5 s) bounds how long the shutdown waits for each of the app's threads. A connection still open on a thread-per-core thread gets that time less 1 s to finish (half of it when it is under 2 s), then is closed. A thread still busy after `shutdown_timeout`, for example a handler that ignores the shutdown, is reported by name, and the process exits without waiting for it.

```python
import time
from mipyre import Mipyre

app = Mipyre()
app.shutdown_timeout = 10


@app.get("/slow")
def slow(req):
    time.sleep(2)
    return "done"


@app.on_shutdown
def bye():
    print("on_shutdown ran", flush=True)
```

A `SIGTERM` sent while `/slow` is running: the client still gets `200 done`, then the log shows

```text
  Shutting down gracefully...
on_shutdown ran
```

Give the service manager a stop timeout longer than `shutdown_timeout` (systemd's `TimeoutStopSec=`, Kubernetes' `terminationGracePeriodSeconds`).

## Health probes

```python
from mipyre import HealthProbes, Mipyre

app = Mipyre()
probes = HealthProbes(app)


@probes.check("db")
def db_ready():
    return True          # raise, return False, or take longer than 10 s to fail
```

- `GET /livez` always answers `200 {"status": "alive"}`. The engine answers it without entering Python or running `before_request` hooks, so a busy interpreter or a hook that asks for credentials doesn't make the process look dead. Use it as the liveness probe.
- `GET /readyz` runs every `@probes.check` and answers `200 {"status": "ready", "checks": {...}}` when all pass, or `503 {"status": "not_ready", ...}` when one fails. Use it as the readiness probe. Why a check failed is never in the body (the probe is unauthenticated): it goes to the log, under the request id the body gives.
- Checks run on the main interpreter by default, or in a worker process with `HealthProbes(app, runtime="process")`; not in a sub-interpreter. A probe within a second of the last one gets that run's result, so probing often doesn't multiply the checks.

The paths are `livez_path=` and `readyz_path=`.

## TLS and HTTP/2

Give the server a PEM certificate chain and key. It then serves HTTPS on its port, with HTTP/2 negotiated over ALPN:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=localhost" -keyout key.pem -out cert.pem
mipyre run app:app --tls-cert cert.pem --tls-key key.pem --port 8443
```

```bash
curl -s -k --http2 -I https://localhost:8443/livez | head -1
```

```text
HTTP/2 200
```

`MIPYRE_TLS_CERT` and `MIPYRE_TLS_KEY` do the same. Set both or neither: one without the other stops the start.

A plain-HTTP port also serves HTTP/2 to a client that speaks it from the start (prior knowledge, `curl --http2-prior-knowledge`). To serve TLS and plain HTTP side by side, list more ports: `run(extra_tls_ports=[...])` / `MIPYRE_TLS_PORTS` (the main `port` then serves plain HTTP), and `run(extra_plain_ports=[...])` / `MIPYRE_PLAIN_PORTS`.

## systemd

### A plain service

```ini
# /etc/systemd/system/app.service
[Unit]
Description=app
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=/srv/app
ExecStart=/srv/app/.venv/bin/mipyre run app:app --host 0.0.0.0 --port 8000
Restart=on-failure
TimeoutStopSec=15

[Install]
WantedBy=multi-user.target
```

`systemctl stop app` sends `SIGTERM`, which shuts the server down gracefully. With `Restart=on-failure`, a server that ends because of a failure (which exits non-zero) is restarted.

### Socket activation

When systemd starts the server with sockets from a `.socket` unit (`Accept=no`), the server serves those sockets and binds none of its own. With no sockets handed over, it binds `host` and `port` as usual. Unix only.

```ini
# /etc/systemd/system/app.socket
[Socket]
ListenStream=0.0.0.0:8000
Accept=no
Backlog=8192

[Install]
WantedBy=sockets.target
```

```ini
# /etc/systemd/system/app.service
[Service]
WorkingDirectory=/srv/app
ExecStart=/srv/app/.venv/bin/mipyre run app:app
Sockets=app.socket
```

At startup the server reads the two variables systemd sets, the way `sd_listen_fds(3)` does: `LISTEN_PID` must be this process's pid, and `LISTEN_FDS` is how many descriptors were handed over, from descriptor 3 on. Then:

- The start says which sockets it serves:

  ```text
    Serving the sockets systemd handed over (LISTEN_FDS)
  ```

- `host`, `port` and the extra ports are not used: giving any of them, in code or in the deployment, stops the start, naming it. List every address in the `.socket` unit instead:

  ```text
  mipyre.engine.ConfigError: port is given (MIPYRE_PORT), but a server serving the sockets systemd handed over (LISTEN_FDS) does not use it: it is used only by a server that binds its own sockets: the .socket unit names the addresses, so list every one there
  ```

- `tls_cert` / `tls_key`, when given, apply to every handed socket.
- Each handed socket must be a listening TCP socket. A Unix-domain socket (`ListenStream=/path`), a datagram socket, or a connection (`Accept=yes`) stops the start, saying which.
- A malformed variable (`LISTEN_PID=abc`) stops the start, naming the variable and its value.
- The handed descriptors are made close-on-exec, so processes the server starts don't inherit them.

One socket per address is shared by every accept loop of the server. For the kernel to spread connections over the loops, as it does when the server binds its own `SO_REUSEPORT` sockets, hand over one socket per accept loop: one `.socket` unit per loop, each with the same `ListenStream=` and `ReusePort=yes`, all named in the service's `Sockets=`.

## Containers and cloud

Kubernetes, AWS services, proxies, databases and caches: [integrations.md](integrations.md).
