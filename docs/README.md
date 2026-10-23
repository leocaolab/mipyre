# mipyre documentation

## Codelabs

Step-by-step tutorials.

- [Fortune app](codelabs/fortune.md): a PostgreSQL-backed web service with JSON and HTML routes, a pydantic-validated write, an async query and a numpy route, then configuration, tests with `TestServer` and `TestClient`, and production.
- [AI agent server](codelabs/ai-agent.md): MCP tools and a resource with `MCPServer`, a pydantic chat route, the answer streamed as Server-Sent Events, and session memory shared by every interpreter.
- [Realtime](codelabs/realtime.md): a WebSocket echo server that refuses other sites, and a Server-Sent Events stream that stops when its client leaves.
- [Trading API](codelabs/trading.md): numpy and pandas analytics on worker processes, a pydantic order route, MsgPack RPC for other services, and quotes over a WebSocket.

## Reference

| Page | What it covers |
|---|---|
| [Runtimes](runtimes.md) | `subinterp`, `process`, `main` and `isolated`: where a route runs, how to choose, and what each can't do |
| [Configuration](configuration.md) | every setting: in code, environment variable, command-line flag, `pyproject.toml`, and its default; the limits on the request path |
| [Testing](testing.md) | integration tests of your handlers with `TestClient` and `TestServer`: every runtime, pytest fixtures, `flags.override`, failures, `pytest -n` |
| [Deployment](deployment.md) | starting and stopping the server, health probes, TLS and HTTP/2, systemd and socket activation |
| [Compatibility](compatibility.md) | which libraries load in a sub-interpreter, how to check one, and the PyO3 fork mipyre is built with |
| [Integrations](integrations.md) | mipyre with proxies, databases, caches, Kubernetes and cloud services |
| [Observability](observability.md) | logs, metrics and tracing |
| [Limitations](limitations.md) | platforms, per-runtime limits, missing command-line flags, and known issues |

## Elsewhere in the repository

- [`examples/`](../examples/README.md): runnable apps.
- [`benchmarks/public/`](../benchmarks/public/): the runtime-mode benchmark, its harness and its results.
