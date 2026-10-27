# Troubleshooting

What the common first-day errors look like, what each means, and how to fix it. Every message below was produced by a fresh install on macOS arm64 with Python 3.14.

## Reading a failed start

`python app.py` that fails to start raises, with a traceback that runs through mipyre's own modules. Read the last line:

```text
Traceback (most recent call last):
  ...
OSError: [Errno 48] bind for 127.0.0.1:8000 failed: Address already in use (os error 48)
```

`mipyre run app:app` prints the same error as one line, `mipyre: ...`.

## Installing

| You see | What it means | Fix |
|---|---|---|
| `ERROR: Could not find a version that satisfies the requirement mipyre (from versions: none)` and `No matching distribution found for mipyre` | pip found no wheel for this Python or this platform; it does not say which | check `python -V` is 3.14 (not 3.13, not the free-threaded 3.14t), and that the platform is Linux x86_64 or macOS arm64 ([limitations.md](limitations.md#platforms)) |
| `... is not a supported wheel on this platform` | a downloaded wheel built for another Python or platform | the same check |

## Starting

| You see | What it means | Fix |
|---|---|---|
| `OSError: [Errno 48] bind for 127.0.0.1:8000 failed: Address already in use` (`Errno 98` on Linux) | another process listens on that address and port | `lsof -iTCP:8000 -sTCP:LISTEN` names it; stop it, or choose another port: `app.run(ServerFlags(port=8001))`, `MIPYRE_PORT=8001`, `mipyre run app:app --port 8001` |
| `mipyre.engine.ConfigError: MIPYRE_THREADS: "0" is invalid for threads: expected a positive whole number` | a setting's value is out of range; the message names the setting and where the value came from (here the environment) | fix that value ([configuration.md](configuration.md#every-server-setting)) |
| `mipyre.flags.FlagError: app.run() was called while mipyre run imported the app's module, ...` (or `while a runtime="process" worker imported ...`) | `app.run()` runs when the module is imported, and the module was imported by `mipyre run` or by a `runtime="process"` worker | put `app.run()` under `if __name__ == "__main__":` |
| `ImportError: worker 0: app.py raised while the worker started: ...` ending with `numpy can't load in a sub-interpreter; imported at app.py:1` and a numbered list of fixes | a module the sub-interpreters import imports a C extension that can't load there | follow the listed fixes; [runtimes.md](runtimes.md) explains each |

On macOS a port held on every address (`*:8000`, as `python -m http.server 8000` holds it) does not stop mipyre binding `127.0.0.1:8000`: the start succeeds, requests to `127.0.0.1` reach mipyre, and a client that resolves `localhost` to `::1` reaches the other server. Linux refuses the bind with `Errno 98`.

## TLS

Every TLS error stops the start.

| You see | What it means | Fix |
|---|---|---|
| `ConfigError: tls_cert (MIPYRE_TLS_CERT) is given without tls_key: give both, or neither` | only one of the pair is set | set both, or neither |
| `ConfigError: tls_cert (MIPYRE_TLS_CERT): cannot open "/etc/ssl/cert.pem": No such file or directory (os error 2)` | the certificate path doesn't exist; a relative path is read from the directory the server starts in | fix the path |
| `ConfigError: tls_cert (MIPYRE_TLS_CERT): no PEM certificate in "key.pem"` | the file given as the certificate holds no `BEGIN CERTIFICATE` block: not a PEM file, or the certificate and key are swapped | give the certificate chain as `tls_cert` and the private key as `tls_key` |
| `ConfigError: tls_key (MIPYRE_TLS_KEY): the key in "b.key" is not the private key of the certificate in "a.crt"` | the private key is not the certificate's key | give the key the certificate was made with |

## Serving

| You see | What it means | Fix |
|---|---|---|
| a `500` with `{"detail":"Internal Server Error","request_id":"..."}` | the handler raised | the server's `ERROR` line on target `mipyre::handler` with the same `request_id` has the exception and its traceback |
| a `500` whose log line's `error` is an `ImportError` ending in `does not support loading in subinterpreters` | a `subinterp` route imports, inside its function, a C extension that can't load in a sub-interpreter | declare that route `runtime="process"` or `runtime="main"` ([runtimes.md](runtimes.md)) |
| `logging.getLogger(...).info(...)` in a handler prints nothing | the default level is `WARN` | `Mipyre(log_config={"level": "INFO"})`; `"format": "text"` for lines a person reads; `MIPYRE_LOG=1` also turns the access log on ([observability.md](observability.md#logs)) |
| `<frozen site>:101: RuntimeWarning: Unexpected value in sys.prefix` once per worker at start | the virtual environment's `python` was started by a relative path such as `../venv/bin/python`; CPython warns in each sub-interpreter it creates | start it by its absolute path, or activate the environment and run `python` |

## Stopping

Ctrl-C (`SIGINT`) and `SIGTERM` stop the server the same way: it prints `Shutting down gracefully...`, finishes the requests in flight, runs the shutdown hooks, stops the `runtime="process"` workers and exits with status 0 ([deployment.md](deployment.md#stopping-it-graceful-shutdown)).
