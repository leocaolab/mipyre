"""Server-Sent Events: a stream of tokens, the way a chat model's answer arrives.

Run:
  python examples/sse_demo.py

Open http://127.0.0.1:8000/ in a browser, or:
  curl -N http://127.0.0.1:8000/stream
"""

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
