# Build realtime endpoints with mipyre

A step-by-step guide to pushing data to clients with mipyre: a WebSocket echo server that refuses connections from other sites, and a Server-Sent Events stream that stops when its client leaves.

## Before you begin

### What you'll build

Two small apps, each with a browser page:

- `websocket_demo.py`: `GET /` serves a page that talks to `/ws`, a WebSocket that echoes text and binary messages and refuses pages from other sites.
- `sse_demo.py`: `GET /` serves a page that reads `/stream`, a stream of tokens sent as Server-Sent Events while `/fast` keeps answering on the sub-interpreters.

The finished apps are [`examples/websocket_demo.py`](../../examples/websocket_demo.py) and [`examples/sse_demo.py`](../../examples/sse_demo.py).

### What you'll learn

- Where WebSocket handlers and streaming routes run, and why.
- How to read text and binary WebSocket messages, and what to do when a client leaves or stops reading.
- Which pages may open your WebSocket, and how to let your own front end on another site in.
- What mipyre's WebSocket limits do.
- How to stream Server-Sent Events with `Stream`.

### What you'll need

- macOS on Apple silicon or Linux x86_64.
- Python 3.14: the regular build, not the free-threaded one.
- `curl`, and a browser if you want to see the pages.

Estimated time: 20 minutes.

## 1. Set up the project

**Goal:** a virtual environment with mipyre, and the `websockets` package as a client to test with.

```bash
mkdir realtime && cd realtime
python3.14 -m venv .venv
source .venv/bin/activate
pip install mipyre websockets
mipyre --version
```

**Checkpoint:**

```text
mipyre 3.0.0
```

The server doesn't need `websockets`; the test clients in this guide do.

## 2. Serve a WebSocket echo

**Goal:** a WebSocket that echoes every message back, and a page that uses it.

Create `websocket_demo.py`:

```python
from mipyre import Mipyre, Response

app = Mipyre()

PAGE = """<!DOCTYPE html>
<html><body>
<h1>Mipyre WebSocket Demo</h1>
<input id="msg" type="text" placeholder="Type a message...">
<button onclick="send()">Send</button>
<pre id="log"></pre>
<script>
const ws = new WebSocket(`ws://${location.host}/ws`);
const log = document.getElementById('log');
ws.onmessage = (e) => { log.textContent += e.data + '\\n'; };
ws.onopen = () => { log.textContent += '[connected]\\n'; };
ws.onclose = () => { log.textContent += '[disconnected]\\n'; };
function send() {
    const msg = document.getElementById('msg');
    ws.send(msg.value);
    msg.value = '';
}
</script>
</body></html>"""


@app.get("/")
def index(req):
    return Response(PAGE, content_type="text/html; charset=utf-8")


@app.websocket("/ws")
async def echo(ws):
    try:
        async for msg in ws:
            if isinstance(msg, bytes):
                await ws.send_bytes(msg)
            else:
                await ws.send(f"echo: {msg}")
    except ConnectionError:
        pass  # the client left


if __name__ == "__main__":
    app.run()
```

A `str` body goes out as `text/plain` unless you name the content type, so `index` returns a `Response` with `text/html`; a browser would show the page's source otherwise.

A WebSocket handler is an `async def`: it gets the connection and owns it until it returns, and every receive and send is awaited.

- `async for msg in ws` yields each message, `str` for a text frame and `bytes` for a binary one, until the connection ends. `await ws.recv_message()` reads one message the same way and returns `None` once the connection has ended; `recv()` and `recv_bytes()` read one kind only and raise `TypeError` if the next message is the other kind, so a client sending a binary frame to a `recv()` loop ends the handler with an error in the log.
- `await ws.send(...)` and `await ws.send_bytes(...)` queue a message. While a client that isn't reading has let the send buffer fill, the await lasts until there is room. They raise `ConnectionError` once the connection is closed, which this handler takes as the end of the conversation.

A WebSocket route runs where the app's runtime says, like any other route: on the default `subinterp` runtime each connection is a task on one of the sub-interpreter workers' event loops, so handlers on different workers run in parallel and a connection waiting for a message holds no thread. Module globals are per worker, as for every `subinterp` route; share state through `app.state`. `@app.websocket("/ws", runtime="main")` runs the handler on the main interpreter's event loop instead, where every connection sees the same globals and shares one GIL; `runtime="process"` is refused. Write the `before_request` hooks of an app with WebSocket routes as `async def`: a `def` hook that blocks holds up every connection on its event loop.

Start the server:

```bash
python websocket_demo.py
```

In a second terminal, check the page:

```bash
curl -s -i localhost:8000/ | head -2
```

```text
HTTP/1.1 200 OK
content-type: text/html; charset=utf-8
```

Open `http://127.0.0.1:8000/` in a browser and send a message, or use a Python client. Create `ws_client.py`:

```python
import sys

from websockets.sync.client import connect

origin = sys.argv[1] if len(sys.argv) > 1 else None
with connect("ws://127.0.0.1:8000/ws", origin=origin) as ws:
    ws.send("hello")
    print(ws.recv())
    ws.send(b"\x00\x01\x02")
    print(ws.recv())
```

Run it:

```bash
python ws_client.py
```

**Checkpoint:** the text comes back prefixed, the bytes as they were:

```text
echo: hello
b'\x00\x01\x02'
```

Leave the server running for the next step.

## 3. See WebSockets from other sites refused

**Goal:** know which pages may open `/ws`, and how to let a front end on another site in.

The same-origin policy doesn't apply to WebSockets: a page on any site can open a WebSocket to your server, and the browser sends your site's cookies with the upgrade request. A WebSocket that trusted those cookies could then be used by that page as the logged-in user. The browser does send the page's origin in the `Origin` header, and mipyre checks it on every upgrade before your handler runs, as it does for every `POST`, `PUT`, `PATCH` and `DELETE`. It opens the WebSocket for:

- a page this server served: its `Origin` names the host and port the request was sent to;
- a page of a site listed in `app.enable_cors(allow_origins=[...])`;
- a client that sends no `Origin`, which is not a browser, such as `ws_client.py` without an argument.

Any other page gets `403`, and your handler doesn't run. Connect as this server's own page would, then as a page on another site would:

```bash
python ws_client.py http://127.0.0.1:8000
python ws_client.py https://attacker.example 2>&1 | tail -1
```

**Checkpoint:** the server's own origin is admitted, the other site gets `403`:

```text
echo: hello
b'\x00\x01\x02'
websockets.exceptions.InvalidStatus: server rejected WebSocket connection: HTTP 403
```

The page at `http://127.0.0.1:8000/` works, because its origin is the server's own. When your front end is served from another site, say `https://app.example.com`, list it after `app = Mipyre()`:

```python
app.enable_cors(allow_origins=["https://app.example.com"])
```

Pages from that site may then open `/ws` too, and read your other routes' responses; every other site's pages still get `403`.

## 4. Know the WebSocket limits

**Goal:** know what happens to a client that sends too much.

Two app attributes bound what WebSocket clients can take:

| Attribute | Default | Past it |
|---|---|---|
| `max_websocket_message_size` | 1 MiB | the connection is closed with code `1009` (message too big) |
| `max_websocket_connections` | 1024 | the upgrade is answered `503` |

Set them after `app = Mipyre()`, for example `app.max_websocket_message_size = 64 * 1024`.

A client that goes quiet is checked as well: after `app.websocket_ping_interval` (20 s) without a frame from it the server sends a ping, and a client that sends nothing within `app.websocket_ping_timeout` (20 s) after it is closed with `1011` and counted in `mipyre_websocket_keepalive_timeouts_total`. A browser or the `websockets` client answers the ping by itself, so an idle but live connection stays open. A client that stops reading what the server sends is cut off after 10 s without progress, as an HTTP client is.

With the server from step 3 running, create `big_message.py`, which sends a 2 MB message:

```python
from websockets.sync.client import connect

with connect("ws://127.0.0.1:8000/ws", max_size=None) as ws:
    ws.send("x" * 2_000_000)
    ws.recv()
```

```bash
python big_message.py 2>&1 | tail -1
```

**Checkpoint:**

```text
websockets.exceptions.ConnectionClosedError: received 1009 (message too big) Message too long: 2000000 > 1048576; then sent 1009 (message too big) Message too long: 2000000 > 1048576
```

The server closed the connection without handing the message to the handler. Stop the server.

## 5. Stream Server-Sent Events

**Goal:** push a stream of tokens over plain HTTP, the way a chat model's answer arrives, while the other routes keep answering.

Server-Sent Events are one-way: the server keeps a response open and writes events to it. A browser reads them with `EventSource`, and any HTTP client can. Create `sse_demo.py`:

```python
import time
from collections.abc import Iterator

from mipyre import EventSourceResponse, Mipyre, Response, ServerSentEvent

app = Mipyre()

TOKENS = "Hello! I am Mipyre, a Python web framework whose HTTP server is written in Rust.".split()
TOKEN_DELAY_SECONDS = 0.05

PAGE = """<!DOCTYPE html>
<html><body>
<h1>Mipyre SSE Demo</h1>
<pre id="output"></pre>
<script>
const source = new EventSource('/stream');
const output = document.getElementById('output');
source.onmessage = (e) => { output.textContent += e.data + ' '; };
source.addEventListener('done', () => {
    output.textContent += '\\n[DONE]';
    source.close();
});
</script>
</body></html>"""


@app.get("/")
def index(req):
    return Response(PAGE, content_type="text/html; charset=utf-8")


def token_events() -> Iterator[ServerSentEvent]:
    for n, token in enumerate(TOKENS):
        yield ServerSentEvent(token, id=str(n))
        time.sleep(TOKEN_DELAY_SECONDS)
    yield ServerSentEvent("[DONE]", event="done")


@app.get("/stream", runtime="main")
def stream_tokens(req):
    return EventSourceResponse(token_events())


@app.get("/fast")
def fast(req):
    return "fast route still works"


if __name__ == "__main__":
    app.run()
```

`EventSourceResponse` returns a `Stream`, which sends the headers at once and the body as it's written, as `text/event-stream`. Each `ServerSentEvent(data, event=..., id=...)` the generator yields is one event: an `id:` and an `event:` line if you pass them, and a `data:` line for each line of `data`. The response ends when the generator does.

Where each route runs:

- `/stream` declares `runtime="main"` because a `Stream` works only on the main interpreter. The handler returns at once and a thread of its own reads the generator, so the stream doesn't hold one of the main interpreter's workers while it sleeps between tokens. At most 256 generators are read at once; a request for one more is answered `503`.
- `/` and `/fast` are pure Python on the default `subinterp` runtime, answered in parallel on every core while streams are open.

When the client disconnects, the generator is closed at its `yield`, so it stops producing tokens for nobody. While a slow client lets the buffer fill, the generator is not read until the client takes more. To write a `Stream` yourself, `send_event` raises `BlockingIOError` when the buffer is full; wait for room with `stream.wait_writable()` (`await stream.wait_writable_async()` in an `async def` producer) and send it again.

Stop the WebSocket server if it is still running, and start this one:

```bash
python sse_demo.py
```

The banner counts `/stream` as a route on the main interpreter:

```text
  Routes: 2 sub-interp + 1 main + 0 process + 0 async
```

In the second terminal:

```bash
curl -s -i -N localhost:8000/stream | head -14
curl -s localhost:8000/fast; echo
```

**Checkpoint:** the events arrive one every 50 ms; the first ones are:

```text
HTTP/1.1 200 OK
cache-control: no-cache
content-type: text/event-stream
connection: keep-alive
server: Mipyre/3.0.0
transfer-encoding: chunked
date: ...

id: 0
data: Hello!

id: 1
data: I

fast route still works
```

`-N` turns off curl's buffering, so each event shows as it arrives; `head` closes the connection after 14 lines, and the server's thread stops at its next `send_event`. Open `http://127.0.0.1:8000/` in a browser to see the page print the tokens. If the connection drops, `EventSource` reconnects by itself and sends the last `id` it saw in a `Last-Event-ID` header, which a real app can use to resume.

Stop the server with Ctrl-C.

## What you've built

- A WebSocket echo server that reads text and binary messages, ends cleanly when a client leaves, refuses pages from other sites, and is bounded by mipyre's message size and connection limits and its keepalive.
- A Server-Sent Events stream that stops when its client leaves, beside routes that keep answering in parallel.
- Each route on the runtime it needs:

  | Route | Runtime | Why |
  |---|---|---|
  | `GET /`, `GET /fast`, `/ws` | `subinterp` | pure Python: parallel on every core; a WebSocket connection is a task on a worker's event loop |
  | `GET /stream` | `main` | `Stream` works only on the main interpreter |

The [AI agent codelab](ai-agent.md) streams a model's answer and remembers each session; the [trading codelab](trading.md) adds a WebSocket price feed to an API that runs numpy and pandas in worker processes.
