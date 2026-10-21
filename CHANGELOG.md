# Changelog

All notable changes to mipyre are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/): within 1.x, the public API does not break.

## [1.0.0] — 2026-10-21

The first public release.

### Added

- An HTTP/1.1 and HTTP/2 server written in Rust (tokio, hyper, rustls), one thread per physical core by default, with a multi-thread server as the alternative (`MIPYRE_TPC=0`).
- Python handlers running in parallel in sub-interpreters that each own their GIL, inside one process; `def` and `async def` handlers.
- Per-route runtimes: `subinterp` (default), `process` (worker processes over shared memory, for C extensions that can't load in a sub-interpreter), `main` (the main interpreter) and `isolated`; declared on a route, a `Router`, the app or the deployment.
- Routing with path and query parameters, `Router` modules, a fallback route and prebuilt constant responses.
- Body and query validation with pydantic (`model=`, `query=`), answered with `422` and the reasons; an OpenAPI 3.1 document.
- Hooks (`before_request`, `after_request`, `on_startup`, `on_shutdown`), CORS, cookies, CSRF checks, redirects.
- gzip and brotli compression; static files with ETag and precompressed variants.
- Multipart uploads, streaming request bodies, Server-Sent Events, WebSocket.
- RPC over JSON, MsgPack or Protobuf; an MCP endpoint.
- A PostgreSQL pool, a per-worker JSON cache, and state shared by every worker (`app.state`).
- Health probes, Prometheus metrics, JSON or text logs, an access log with sampling.
- Limits on body size, timeouts, slow uploads and clients that stop reading; bounded queues answered with `503`.
- TLS, systemd socket activation, graceful shutdown, hot reload, the `mipyre` command (`run`, `dev`, `routes`) and an in-memory `TestClient`.

### Requirements

- Python 3.14 (the build with the GIL). Wheels for Linux x86_64 and macOS arm64.
